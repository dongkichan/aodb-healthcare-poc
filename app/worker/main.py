"""Outbox relay and stream consumers in one process.

The relay moves committed outbox rows to the Redis stream and marks them published. Delivery is
at least once: if the relay stops between the XADD and its commit, the rows are published again,
and the idempotent projections absorb the duplicates. Each projection reads the stream through
its own consumer group and acknowledges an entry only after applying it.
"""
import os
import threading
import time
import traceback

import psycopg
import redis

from .projections import PROJECTIONS, decode, rds

STREAM = "poc:events"
POLL_SECONDS = 0.05


def relay(stop: threading.Event) -> None:
    r = rds()
    conn = psycopg.connect(os.environ["PG_WORKER_DSN"])
    while not stop.is_set():
        with conn.transaction():
            rows = conn.execute(
                "SELECT outbox_id, event_type, payload::text, extract(epoch FROM created_at) FROM outbox_event "
                "WHERE published_at IS NULL ORDER BY outbox_id LIMIT 200 FOR UPDATE SKIP LOCKED").fetchall()
            if rows:
                pipe = r.pipeline(transaction=False)
                for oid, etype, payload, created in rows:
                    pipe.xadd(STREAM, {"outbox_id": oid, "type": etype, "payload": payload, "created": f"{created:.6f}"},
                              maxlen=200000, approximate=True)
                pipe.execute()
                conn.execute("UPDATE outbox_event SET published_at = clock_timestamp() WHERE outbox_id = ANY(%s)",
                             [[row[0] for row in rows]])
        if not rows:
            time.sleep(POLL_SECONDS)


def ensure_group(r: redis.Redis, group: str) -> None:
    try:
        r.xgroup_create(STREAM, group, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def consume(group: str, stop: threading.Event) -> None:
    r = rds()
    apply = PROJECTIONS[group]
    ensure_group(r, group)
    start = "0"  # first re-read this consumer's unacknowledged entries, then new ones
    while not stop.is_set():
        try:
            resp = r.xreadgroup(group, f"{group}-1", {STREAM: start}, count=100, block=1000)
        except redis.ResponseError as e:
            if "NOGROUP" in str(e):  # the stream or group was lost (for example, Redis was flushed)
                ensure_group(r, group)
                start = "0"
                continue
            raise
        entries = resp[0][1] if resp else []
        if not entries:
            start = ">"
            continue
        for msg_id, fields in entries:
            oid, payload = decode(fields)
            applied = apply(oid, payload)
            lag_ms = (time.time() - float(fields["created"])) * 1000
            pipe = r.pipeline(transaction=False)
            pipe.lpush(f"metrics:lag:{group}", f"{lag_ms:.2f}")
            pipe.ltrim(f"metrics:lag:{group}", 0, 49999)
            if not applied:
                pipe.incr(f"metrics:skipped:{group}")
            pipe.xack(STREAM, group, msg_id)
            pipe.execute()


def supervise(name, target, *args) -> threading.Thread:
    def run():
        while True:
            try:
                target(*args)
                return
            except Exception:
                traceback.print_exc()
                time.sleep(1)
    t = threading.Thread(target=run, name=name, daemon=True)
    t.start()
    return t


def main() -> None:
    stop = threading.Event()
    supervise("relay", relay, stop)
    for group in PROJECTIONS:
        supervise(group, consume, group, stop)
    print("worker started: relay + " + ", ".join(PROJECTIONS), flush=True)
    while True:
        rds().set("worker:heartbeat", str(time.time()), ex=10)
        time.sleep(2)


if __name__ == "__main__":
    main()

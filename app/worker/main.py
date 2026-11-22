"""Outbox relay and stream consumers in one process.

The relay moves committed outbox rows to the Redis stream and marks them published. Delivery is
at least once: if the relay stops between the XADD and its commit, the rows are published again,
and the idempotent projections absorb the duplicates. Each projection reads the stream through
its own consumer group and acknowledges an entry only after applying it.

Robustness:
- every thread writes its own heartbeat; the container health check requires all four;
- an entry that fails 5 times is parked in dead:<group> and acknowledged, so one bad entry cannot
  stop a projection;
- a consumer pauses while pause:<group> is set (used by the Neo4j rebuild);
- the stream is not trimmed: trimming by length could drop entries a slow group has not read.
"""
import json
import os
import threading
import time
import traceback

import psycopg
import redis

from .projections import PROJECTIONS, decode, rds

STREAM = "poc:events"
POLL_SECONDS = 0.05
MAX_ATTEMPTS = 5
THREADS = ("relay",) + tuple(PROJECTIONS)


def beat(name: str, last: list) -> None:
    """Heartbeat every 2 s per thread; a Redis hiccup must not kill the thread."""
    now = time.time()
    if now - last[0] >= 2:
        try:
            rds().set(f"worker:alive:{name}", str(now), ex=15)
            last[0] = now
        except redis.RedisError:
            pass


def relay(stop: threading.Event) -> None:
    r = rds()
    conn = psycopg.connect(os.environ["PG_WORKER_DSN"])
    last = [0.0]
    while not stop.is_set():
        beat("relay", last)
        with conn.transaction():
            rows = conn.execute(
                "SELECT outbox_id, event_type, payload::text, extract(epoch FROM created_at) FROM outbox_event "
                "WHERE published_at IS NULL ORDER BY outbox_id LIMIT 200 FOR UPDATE SKIP LOCKED").fetchall()
            if rows:
                pipe = r.pipeline(transaction=False)
                for oid, etype, payload, created in rows:
                    pipe.xadd(STREAM, {"outbox_id": oid, "type": etype, "payload": payload, "created": f"{created:.6f}"})
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


def handle(r: redis.Redis, group: str, apply, msg_id: str, fields: dict) -> bool:
    """Applies one entry. Returns False if it failed and should be retried later."""
    try:
        oid, payload = decode(fields)
        applied = apply(oid, payload)
        lag_ms = (time.time() - float(fields["created"])) * 1000
    except Exception as exc:  # noqa: BLE001 - any failure counts toward parking the entry
        attempts = r.hincrby(f"fail:{group}", msg_id, 1)
        print(f"{group}: entry {msg_id} failed (attempt {attempts}): {exc!r}", flush=True)
        if attempts < MAX_ATTEMPTS:
            return False
        r.lpush(f"dead:{group}", json.dumps({"id": msg_id, "fields": fields, "error": repr(exc)}))
        r.hdel(f"fail:{group}", msg_id)
        r.xack(STREAM, group, msg_id)
        return True
    pipe = r.pipeline(transaction=False)
    pipe.lpush(f"metrics:lag:{group}", f"{lag_ms:.2f}")
    pipe.ltrim(f"metrics:lag:{group}", 0, 49999)
    if applied is False:
        pipe.incr(f"metrics:skipped:{group}")
    elif applied is None:
        pipe.incr(f"metrics:missing:{group}")
    pipe.hdel(f"fail:{group}", msg_id)
    pipe.xack(STREAM, group, msg_id)
    pipe.execute()
    return True


def consume(group: str, stop: threading.Event) -> None:
    r = rds()
    apply = PROJECTIONS[group]
    ensure_group(r, group)
    start = "0"  # first re-read this consumer's unacknowledged entries, then new ones
    last = [0.0]
    while not stop.is_set():
        beat(group, last)
        if r.exists(f"pause:{group}"):
            r.set(f"paused:{group}", "1", ex=5)
            time.sleep(0.2)
            continue
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
            if not handle(r, group, apply, msg_id, fields):
                start = "0"   # retry the pending entry on the next pass
                time.sleep(1)
                break


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
        time.sleep(5)


if __name__ == "__main__":
    main()

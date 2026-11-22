"""Rebuilds the derived stores from PostgreSQL, and replays the outbox.

  python -m worker.rebuild redis       queues and status cache from open requests
  python -m worker.rebuild neo4j       whole graph from the approved rules and PostgreSQL
  python -m worker.rebuild replay 60   republish outbox events from the last 60 minutes

Each command prints one JSON line with what it did and how long it took.
"""
import json
import os
import sys
import time

import psycopg

from seed import data as d
from seed.load import load_neo4j

from .projections import graph, rds


def _worker_conn():
    return psycopg.connect(os.environ["PG_WORKER_DSN"])


def rebuild_redis() -> dict:
    t0 = time.perf_counter()
    r = rds()
    removed = 0
    for pattern in ("queue:*", "req:*:status"):
        batch = list(r.scan_iter(match=pattern, count=1000))
        for i in range(0, len(batch), 500):
            removed += r.delete(*batch[i:i + 500])
    with _worker_conn() as conn:
        rows = conn.execute(
            "SELECT r.request_id, c.name_code, w.name, r.status, extract(epoch FROM r.due_at), "
            "(SELECT max(o.outbox_id) FROM outbox_event o WHERE o.aggregate_id = r.request_id::text) "
            "FROM request r JOIN client c USING (client_id) JOIN workflow_type w USING (workflow_type_id) "
            "WHERE r.status <> 'completed'").fetchall()
    pipe = r.pipeline(transaction=False)
    for rid, client, wf, status, due, oid in rows:
        pipe.hset(f"req:{rid}:status", mapping={"oid": oid or 0, "status": status})
        pipe.zadd(f"queue:{client}:{wf}", {rid: float(due)})
    pipe.execute()
    return {"rebuilt": "redis", "keys_removed": removed, "open_requests": len(rows),
            "seconds": round(time.perf_counter() - t0, 3)}


def rebuild_neo4j() -> dict:
    t0 = time.perf_counter()
    with graph().session() as s:
        s.run("MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 5000 ROWS").consume()
    with _worker_conn() as conn:
        total = conn.execute("SELECT count(*) FROM guidance_chunk").fetchone()[0]
        rows = conn.execute(
            "SELECT r.request_id, c.name_code, w.name, r.status, "
            "(SELECT max(o.outbox_id) FROM outbox_event o WHERE o.aggregate_id = r.request_id::text) "
            "FROM request r JOIN client c USING (client_id) JOIN workflow_type w USING (workflow_type_id)").fetchall()
    stats = load_neo4j(total)  # rules, roles, staff links, and guidance sections
    reqs = [{"id": rid, "client": c, "type": wf, "wf": f"{c}/{wf}", "status": st, "oid": oid or 0,
             "outcome": "completed" if st == "completed" else None} for rid, c, wf, st, oid in rows]
    with graph().session() as s:
        for i in range(0, len(reqs), 2000):
            s.run("""UNWIND $rows AS row
                     MATCH (w:WorkflowType {id: row.wf})
                     MERGE (r:Request {id: row.id})
                     SET r.client = row.client, r.type = row.type, r.status = row.status,
                         r.last_outbox_id = row.oid, r.outcome = row.outcome
                     MERGE (r)-[:OF_TYPE]->(w)""", rows=reqs[i:i + 2000]).consume()
    return {"rebuilt": "neo4j", "graph_nodes": stats["graph_nodes"] + len(reqs), "requests": len(reqs),
            "seconds": round(time.perf_counter() - t0, 3)}


def replay(minutes: int) -> dict:
    with _worker_conn() as conn:
        n = conn.execute("UPDATE outbox_event SET published_at = NULL "
                         "WHERE created_at > now() - make_interval(mins => %s)", [minutes]).rowcount
    return {"replayed_events": n, "minutes": minutes}


if __name__ == "__main__":
    what = sys.argv[1]
    if what == "redis":
        print(json.dumps(rebuild_redis()))
    elif what == "neo4j":
        print(json.dumps(rebuild_neo4j()))
    elif what == "replay":
        print(json.dumps(replay(int(sys.argv[2]))))
    else:
        raise SystemExit(__doc__)

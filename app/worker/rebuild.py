"""Rebuilds the derived stores from PostgreSQL, and replays the outbox.

  python -m worker.rebuild redis       queues and status cache from the latest state of every request
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

from .projections import graph, rds, redis_apply


def _worker_conn():
    return psycopg.connect(os.environ["PG_WORKER_DSN"])


def _snapshot(conn):
    """Every request with its latest outbox ID (the version the consumers compare against)."""
    return conn.execute(
        "SELECT r.request_id, c.name_code, w.name, r.status, extract(epoch FROM r.due_at), "
        "coalesce((SELECT max(o.outbox_id) FROM outbox_event o WHERE o.aggregate_id = r.request_id::text), 0) "
        "FROM request r JOIN client c USING (client_id) JOIN workflow_type w USING (workflow_type_id)").fetchall()


def rebuild_redis() -> dict:
    """Re-applies the latest state of every request through the same guarded script the consumer
    uses, so a consumer that applied a newer event during the rebuild is never overwritten. No
    keys are deleted first."""
    t0 = time.perf_counter()
    with _worker_conn() as conn:
        rows = _snapshot(conn)
    applied = sum(redis_apply(oid, st, rid, c, wf, float(due), mode="rebuild") for rid, c, wf, st, due, oid in rows)
    return {"rebuilt": "redis", "requests_applied": applied, "open_requests": sum(1 for r in rows if r[3] != "completed"),
            "seconds": round(time.perf_counter() - t0, 3)}


def rebuild_neo4j(group: str = "proj-neo4j", wait_seconds: float = 10) -> dict:
    """Pauses the Neo4j consumer, rebuilds the whole graph from the approved rules and PostgreSQL,
    then resumes the consumer. While the graph is empty, new requests get 422 from the API."""
    r = rds()
    r.set(f"pause:{group}", "1", ex=300)
    deadline = time.time() + wait_seconds
    while not r.exists(f"paused:{group}") and time.time() < deadline:
        time.sleep(0.1)
    try:
        t0 = time.perf_counter()
        with graph().session() as s:
            s.run("MATCH (n) CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 5000 ROWS").consume()
        with _worker_conn() as conn:
            total = conn.execute("SELECT count(*) FROM guidance_chunk").fetchone()[0]
            rows = _snapshot(conn)
        stats = load_neo4j(total)  # rules, roles, staff links, and guidance sections
        reqs = [{"id": rid, "client": c, "type": wf, "wf": f"{c}/{wf}", "status": st, "oid": oid,
                 "outcome": "completed" if st == "completed" else None} for rid, c, wf, st, due, oid in rows]
        with graph().session() as s:
            for i in range(0, len(reqs), 2000):
                s.run("""UNWIND $rows AS row
                         MATCH (w:WorkflowType {id: row.wf})
                         MERGE (r:Request {id: row.id})
                           ON CREATE SET r.client = row.client, r.type = row.type, r.last_outbox_id = -1
                         WITH r, row, w WHERE r.last_outbox_id < row.oid
                         SET r.status = row.status, r.last_outbox_id = row.oid, r.outcome = row.outcome
                         MERGE (r)-[:OF_TYPE]->(w)""", rows=reqs[i:i + 2000]).consume()
        seconds = round(time.perf_counter() - t0, 3)
    finally:
        r.delete(f"pause:{group}", f"paused:{group}")
    return {"rebuilt": "neo4j", "graph_nodes": stats["graph_nodes"] + len(reqs), "requests": len(reqs),
            "seconds": seconds}


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

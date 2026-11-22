"""Scripted demo for the 3-5 minute video: one request from sign-in to audited completion.

  docker compose run --rm tests python -m demo.run            (pauses between steps)
  docker compose run --rm tests python -m demo.run --no-pause (prints everything at once)

Every value printed comes from the running stack. Synthetic data only.
"""
import json
import os
import sys
import time
import uuid

import httpx
import psycopg
import redis
from neo4j import GraphDatabase
from pymongo import MongoClient

from seed.data import user_uuid

API = os.environ.get("API_URL", "http://api:8000")
PAUSE = "--no-pause" not in sys.argv
http = httpx.Client(base_url=API, timeout=30)
tokens = {}


def step(n, title):
    if PAUSE:
        input("\n[press Enter for the next step]")
    print(f"\n=== Step {n}. {title} ===", flush=True)


def call(method, url, handle, **kw):
    if handle not in tokens:
        tokens[handle] = http.post("/dev/token", json={"handle": handle}).json()["access_token"]
    t0 = time.perf_counter()
    r = http.request(method, url, headers={"Authorization": f"Bearer {tokens[handle]}"}, **kw)
    print(f"{handle:>10} {method} {url} -> {r.status_code} ({(time.perf_counter() - t0) * 1000:.0f} ms)")
    return r


def show(obj, keys=None):
    if keys:
        obj = {k: obj[k] for k in keys if k in obj}
    print("           " + json.dumps(obj, default=str)[:400])


def main():
    admin = psycopg.connect(os.environ["PG_ADMIN_DSN"], autocommit=True)
    mongo = MongoClient(os.environ["MONGO_URL"]).get_database("poc")
    graph = GraphDatabase.driver(os.environ["NEO4J_URL"], auth=("neo4j", os.environ["NEO4J_PASSWORD"]))
    rds = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)

    step(1, "Sign in (simulated identity provider): role and clients come from PostgreSQL")
    t = http.post("/dev/token", json={"handle": "staff-a"}).json()
    tokens["staff-a"] = t["access_token"]
    show(t, ["handle", "role", "clients"])

    step(2, "Register a request: Neo4j rules -> PostgreSQL transaction -> MongoDB work package")
    ref = f"DEMO-{uuid.uuid4().hex[:6]}"
    r = call("POST", "/requests", "staff-a", json={
        "client": "CL-01", "workflow": "appointment_change", "external_ref": ref,
        "summary": "Caller asked to move the visit to a morning slot and confirm by phone.",
        "fields": {"client_reference": ref, "requested_window": "AM", "callback_confirmed": True}})
    body = r.json()
    rid = body["request_id"]
    show(body, ["request_id", "status", "required_fields", "missing_fields", "work_package_id", "timings_ms"])
    row = admin.execute("SELECT request_id, client_id, external_ref, status FROM poc.request WHERE request_id = %s", [rid]).fetchone()
    print(f"  PostgreSQL request row: {row}")
    wp = mongo.work_packages.find_one({"request_id": rid}, {"_id": 1, "fields": 1, "version": 1, "status": 1})
    print(f"  MongoDB work package:   {wp}")

    step(3, "A missing required field holds the request (Neo4j rule for this client)")
    r = call("POST", "/requests", "staff-a", json={
        "client": "CL-01", "workflow": "appointment_change", "external_ref": f"DEMO-{uuid.uuid4().hex[:6]}",
        "summary": "Caller wants a new slot.", "fields": {"client_reference": "R", "requested_window": "PM"}})
    show(r.json(), ["status", "missing_fields"])

    step(4, "Guidance search: client-filtered vector search in pgvector + graph expansion in Neo4j")
    r = call("GET", "/guidance/search", "staff-a", params={"request": rid})
    s = r.json()
    for p in s["results"]:
        print(f"  {p['section_id']:<14} {p['client']} distance {p['distance']:.3f}  {(p['text'] or '')[:70]}")
    show(s["graph"], ["required_fields", "roles", "referenced_sections"])
    show(s["timings_ms"])

    step(5, "Another client's data: refused at the gateway, and invisible in the database")
    call("GET", "/requests", "staff-b", params={"client": "CL-03"})
    with psycopg.connect(os.environ["PG_DSN"]) as app:
        app.execute("SELECT set_config('app.user_id', %s, true)", [user_uuid("staff-b")])
        n = app.execute("SELECT count(*) FROM poc.request WHERE client_id = 3").fetchone()[0]
    total = admin.execute("SELECT count(*) FROM poc.request WHERE client_id = 3").fetchone()[0]
    print(f"  as staff-b (CL-01 only), CL-03 rows visible: {n}   (rows that exist: {total})")

    step(6, "Draft, then review by someone else (self-review is refused)")
    d = call("POST", f"/requests/{rid}/drafts", "staff-a").json()
    print(f"  draft {d['draft_id']} cites {[x['section_id'] for x in d['sources']]}")
    call("POST", f"/drafts/{d['draft_id']}/review", "staff-a", json={"decision": "accepted", "reason": "self"})
    r = call("POST", f"/drafts/{d['draft_id']}/review", "staff-b", json={"decision": "accepted", "reason": "matches guidance"})
    show(r.json(), ["decision", "status"])

    step(7, "Complete: status, outcome, client action, audit, and outbox in one transaction")
    key = uuid.uuid4().hex
    call("POST", f"/requests/{rid}/complete", "staff-a", json={"outcome": "slot moved and confirmed", "idempotency_key": key})
    r = call("POST", f"/requests/{rid}/complete", "staff-a", json={"outcome": "slot moved and confirmed", "idempotency_key": key})
    show(r.json(), ["status", "replayed"])

    step(8, "Audit trail: staff refused, manager allowed, DPO checks the hash chain")
    call("GET", "/audit", "staff-a", params={"request": rid})
    r = call("GET", "/audit", "manager-1", params={"request": rid})
    for e in r.json()["events"]:
        print(f"  #{e['audit_id']:<6} {e['occurred_at'][11:23]} {e['actor']:<8} {e['action']:<8} {e['object_ref']}  fields={e['changed_fields']}")
    show(call("GET", "/audit/verify", "dpo-1").json())
    with admin.transaction(force_rollback=True):
        target = admin.execute("SELECT max(audit_id) - 3 FROM poc.audit_event").fetchone()[0]
        admin.execute("UPDATE poc.audit_event SET action = 'read_audit' WHERE audit_id = %s", [target])
        ok, _, bad = admin.execute("SELECT * FROM poc.audit_verify()").fetchone()
        print(f"  superuser edits event #{target} -> chain ok: {ok}, first broken event: #{bad}   (rolled back)")

    step(9, "Outbox -> Redis stream -> projections in MongoDB, Neo4j, and the Redis queue")
    time.sleep(1)
    with graph.session() as gs:
        node = gs.run("MATCH (r:Request {id: $id}) RETURN r.status AS status, r.client AS client, r.type AS type",
                      id=rid).single()
    print(f"  Neo4j Request node:      {dict(node) if node else None}")
    print(f"  MongoDB work package:    status = {mongo.work_packages.find_one({'request_id': rid})['status']}")
    print(f"  Redis status cache:      {rds.hgetall(f'req:{rid}:status')}")
    print(f"  Redis queue CL-01:       request {rid} still queued? {rds.zscore('queue:CL-01:appointment_change', rid) is not None}")
    groups = rds.xinfo_groups("poc:events")
    print("  Stream consumer groups:  " + ", ".join(f"{g['name']} (pending {g['pending']}, lag {g['lag']})" for g in groups))
    print("\nDemo complete.")


if __name__ == "__main__":
    main()

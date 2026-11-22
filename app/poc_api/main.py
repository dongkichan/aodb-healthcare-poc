"""Proof-of-concept API: a subset of the REST endpoints in the proposed data architecture.

Every call passes the gateway checks in security.py. Every PostgreSQL statement runs inside
user_tx(), so row-level security sees the signed-in staff member. Synthetic data only.
"""
import os
import time
import uuid
from typing import Literal, Optional

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException
from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field
from pymongo.errors import DuplicateKeyError, PyMongoError

from .security import User, current_user, issue_token, require_client, require_role
from .stores import embedder, mongo_db, neo4j_driver, pg_pool, redis_client, user_tx

app = FastAPI(title="AODB proof-of-concept API", version="1.0.0",
              description="Healthcare administrative support platform. Synthetic data only.")

FAULTS = os.environ.get("POC_FAULT_INJECTION") == "1"


@app.on_event("startup")
def warm_up() -> None:
    embedder().encode("warm up")  # load the embedding model before the first request
# default: the planner chooses, with pgvector iterative index scans on, so a filtered HNSW scan keeps
# reading the index until it has enough rows for the client and workflow.
# planner: the planner chooses, iterative scans off (the search as first designed).
# exact / hnsw / hnsw_iterative: force one access path, for the measurements.
SEARCH_MODES = ("default", "planner", "exact", "hnsw", "hnsw_iterative")


def ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000, 2)


def maybe_fail(point: str, fault: Optional[str]) -> None:
    """Test hook: raises inside the transaction so the tests can check that nothing is kept."""
    if FAULTS and fault == point:
        raise RuntimeError(f"injected fault at {point}")


def write_audit(conn, user: User, client_id: int, action: str, object_ref: str, changed=None) -> None:
    # audit_id and prev_hash are assigned by the hash-chain trigger.
    conn.execute("INSERT INTO audit_event (actor_id, actor_role, client_id, action, object_ref, changed_fields) "
                 "VALUES (%s, %s, %s, %s, %s, %s)", [user.user_id, user.role, client_id, action, object_ref, changed])


def write_outbox(conn, rid: int, event_type: str) -> None:
    """Outbox row in the same transaction as the change. The payload carries IDs, codes, status,
    and due time only, never request content."""
    client_id, payload = conn.execute(
        "SELECT r.client_id, json_build_object('request_id', r.request_id, 'client', c.name_code, 'workflow', w.name, "
        "'status', r.status, 'due', extract(epoch FROM r.due_at)) FROM request r JOIN client c USING (client_id) "
        "JOIN workflow_type w USING (workflow_type_id) WHERE r.request_id = %s", [rid]).fetchone()
    conn.execute("INSERT INTO outbox_event (client_id, aggregate_id, event_type, payload, created_at) "
                 "VALUES (%s, %s, %s, %s, clock_timestamp())", [client_id, str(rid), event_type, Jsonb(payload)])


def write_status(conn, rid: int, client_id: int, old: Optional[str], new: str, user: User) -> None:
    conn.execute("INSERT INTO request_event (request_id, client_id, from_status, to_status, actor_id) "
                 "VALUES (%s, %s, %s, %s, %s)", [rid, client_id, old, new, user.user_id])


# ---------------------------------------------------------------- health and sign-in

@app.get("/health")
def health():
    with pg_pool().connection() as conn:
        conn.execute("SELECT 1")
    mongo_db().command("ping")
    neo4j_driver().verify_connectivity()
    redis_client().ping()
    return {"status": "ok"}


class TokenRequest(BaseModel):
    handle: str = Field(min_length=1, max_length=40)


@app.post("/dev/token")
def dev_token(body: TokenRequest):
    """Development sign-in for synthetic users. Production signs in through OpenID Connect."""
    if os.environ.get("POC_DEV_TOKENS") != "1":
        raise HTTPException(404)
    return issue_token(body.handle)


# ---------------------------------------------------------------- requests

class NewRequest(BaseModel):
    client: str
    workflow: str
    external_ref: str = Field(min_length=1, max_length=64)
    summary: str = Field(min_length=1, max_length=500)
    fields: dict = {}


@app.post("/requests", status_code=201)
def create_request(body: NewRequest, user: User = Depends(current_user)):
    require_role(user, "staff", "team_lead", "client_coordinator")
    require_client(user, body.client)
    timings = {}

    # 1. Neo4j: the fields this client requires for this workflow.
    t0 = time.perf_counter()
    with neo4j_driver().session() as s:
        rec = s.run("MATCH (:Client {code: $c})-[:USES]->(w:WorkflowType {name: $w}) "
                    "OPTIONAL MATCH (w)-[:REQUIRES]->(f:RequiredField) RETURN collect(f.name) AS fields",
                    c=body.client, w=body.workflow).single()
    timings["neo4j"] = ms(t0)
    if rec is None:
        raise HTTPException(422, "unknown workflow for this client")
    required = sorted(rec["fields"])
    missing = [f for f in required if body.fields.get(f) in (None, "", False)]
    status = "held" if missing else "open"

    # 2. PostgreSQL: request, first status event, audit event, and outbox row in one transaction.
    t0 = time.perf_counter()
    try:
        with user_tx(user.user_id) as conn:
            row = conn.execute("SELECT w.workflow_type_id, w.client_id, w.target_hours FROM workflow_type w "
                               "JOIN client c USING (client_id) WHERE c.name_code = %s AND w.name = %s",
                               [body.client, body.workflow]).fetchone()
            if row is None:
                raise HTTPException(422, "unknown workflow for this client")
            wf_id, client_id, hours = row
            rid = conn.execute(
                "INSERT INTO request (client_id, workflow_type_id, external_ref, summary, status, owner_id, due_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, now() + make_interval(hours => %s)) RETURNING request_id",
                [client_id, wf_id, body.external_ref, body.summary, status, user.user_id, hours]).fetchone()[0]
            write_status(conn, rid, client_id, None, status, user)
            write_audit(conn, user, client_id, "create", f"request:{rid}", ["external_ref", "summary", "status"])
            write_outbox(conn, rid, "request.created")
    except psycopg.errors.UniqueViolation:
        raise HTTPException(409, "this client reference already has an open request")
    timings["postgres"] = ms(t0)

    # 3. MongoDB: the work package, shaped by this client's checklist (retried if it fails).
    t0 = time.perf_counter()
    wp_id = f"WP-{rid:06d}"
    doc = {"_id": wp_id, "request_id": rid, "client_code": body.client, "workflow": body.workflow,
           "fields": body.fields, "missing_fields": missing, "guidance_refs": [], "version": 1,
           "status": "held" if missing else "in_progress"}
    for attempt in range(3):
        try:
            mongo_db().work_packages.insert_one(doc)
            break
        except DuplicateKeyError:
            break
        except PyMongoError:
            if attempt == 2:
                raise
            time.sleep(0.2)
    timings["mongodb"] = ms(t0)
    return {"request_id": rid, "status": status, "required_fields": required, "missing_fields": missing,
            "work_package_id": wp_id, "timings_ms": timings}


@app.get("/requests")
def list_requests(client: str, status: Optional[str] = None, user: User = Depends(current_user)):
    require_role(user, "staff", "team_lead", "client_coordinator", "manager", "dpo")
    require_client(user, client)
    with user_tx(user.user_id) as conn:
        rows = conn.execute(
            "SELECT r.request_id, c.name_code, w.name, r.external_ref, r.summary, r.status, r.due_at "
            "FROM request r JOIN client c USING (client_id) JOIN workflow_type w USING (workflow_type_id) "
            "WHERE c.name_code = %s AND (%s::text IS NULL OR r.status = %s) ORDER BY r.due_at LIMIT 100",
            [client, status, status]).fetchall()
        client_id = conn.execute("SELECT client_id FROM client WHERE name_code = %s", [client]).fetchone()[0]
        # Client references and summaries are Restricted, so the read itself is logged.
        write_audit(conn, user, client_id, "read_restricted", f"requests?client={client}&status={status or 'any'}")
    return {"client": client, "requests": [
        {"request_id": r[0], "client": r[1], "workflow": r[2], "external_ref": r[3], "summary": r[4],
         "status": r[5], "due_at": r[6].isoformat()} for r in rows]}


# ---------------------------------------------------------------- guidance search (vector + graph)

def run_search(conn, rid: int, mode: str = "default", k: int = 5) -> dict:
    timings = {}
    req = conn.execute("SELECT r.client_id, c.name_code, w.name, r.summary, r.status FROM request r "
                       "JOIN client c USING (client_id) JOIN workflow_type w USING (workflow_type_id) "
                       "WHERE r.request_id = %s", [rid]).fetchone()
    if req is None:  # also the answer for another client's request: row-level security hides it
        raise HTTPException(404, "request not found")
    client_id, code, workflow, summary, status = req
    wp = mongo_db().work_packages.find_one({"request_id": rid}, {"missing_fields": 1}) or {}
    query = f"{workflow.replace('_', ' ')}: {summary}"
    if wp.get("missing_fields"):
        query += " Missing: " + ", ".join(wp["missing_fields"]) + "."

    t0 = time.perf_counter()
    vec = embedder().encode(query, normalize_embeddings=True)
    timings["embed"] = ms(t0)

    # Filter by client and workflow first, then rank by cosine distance.
    t0 = time.perf_counter()
    if mode == "exact":
        conn.execute("SET LOCAL enable_indexscan = off")
    elif mode in ("hnsw", "hnsw_iterative"):
        conn.execute("SET LOCAL enable_seqscan = off")
    if mode in ("default", "hnsw_iterative"):
        conn.execute("SET LOCAL hnsw.iterative_scan = strict_order")
    rows = conn.execute(
        "SELECT section_id, guidance_id, version, workflow_type, embedding <=> %s AS distance FROM guidance_chunk "
        "WHERE client_id = %s AND workflow_type = %s ORDER BY embedding <=> %s LIMIT %s",
        [vec, client_id, workflow, vec, k]).fetchall()
    timings["pgvector"] = ms(t0)
    sids = [r[0] for r in rows]

    # Neo4j: one- or two-hop expansion from the matched sections to the rules that apply.
    t0 = time.perf_counter()
    with neo4j_driver().session() as s:
        g = s.run("""
            MATCH (c:Client {code: $client})-[:USES]->(w:WorkflowType {name: $wf})
            OPTIONAL MATCH (w)-[:REQUIRES]->(f:RequiredField)
            WITH c, w, collect(DISTINCT f.name) AS fields
            OPTIONAL MATCH (r:Role)-[:MAY_PERFORM]->(w)
            WITH c, w, fields, collect(DISTINCT r.code) AS roles
            OPTIONAL MATCH (g:GuidanceSection)-[:APPLIES_TO]->(w) WHERE g.id IN $sids
            OPTIONAL MATCH (g)-[:REFERS_TO*1..2]->(g2:GuidanceSection)-[:APPLIES_TO]->(:WorkflowType)<-[:USES]-(c)
            RETURN fields, roles, collect(DISTINCT g2.id) AS referenced""",
                  client=code, wf=workflow, sids=sids).single()
    timings["neo4j"] = ms(t0)
    referenced = sorted(g["referenced"])

    # MongoDB: passage text for the matched and referenced sections.
    t0 = time.perf_counter()
    wanted = set(sids) | set(referenced)
    texts = {}
    for doc in mongo_db().guidance.find({"sections.section_id": {"$in": list(wanted)}}, {"sections": 1}):
        for sec in doc["sections"]:
            if sec["section_id"] in wanted:
                texts[sec["section_id"]] = sec["text"]
    timings["mongodb"] = ms(t0)

    return {
        "request_id": rid, "client": code, "workflow": workflow, "status": status, "mode": mode,
        "results": [{"section_id": r[0], "guidance_id": r[1], "version": r[2], "client": code, "workflow": r[3],
                     "distance": round(float(r[4]), 4), "text": texts.get(r[0])} for r in rows],
        "graph": {"required_fields": sorted(g["fields"]), "roles": sorted(g["roles"]),
                  "referenced_sections": referenced,
                  "referenced_text": {s: texts.get(s) for s in referenced if s not in sids}},
        "timings_ms": timings, "_client_id": client_id,
    }


@app.get("/guidance/search")
def guidance_search(request: int, mode: str = "default", user: User = Depends(current_user)):
    require_role(user, "staff", "team_lead")
    if mode not in SEARCH_MODES:
        raise HTTPException(422, f"mode must be one of {SEARCH_MODES}")
    with user_tx(user.user_id) as conn:
        found = run_search(conn, request, mode)
    found.pop("_client_id")
    return found


# ---------------------------------------------------------------- drafts and review

@app.post("/requests/{rid}/drafts", status_code=201)
def create_draft(rid: int, user: User = Depends(current_user)):
    """Assembles a proposed note from the retrieved passages. No language model is called:
    the approved AI environment is not available to the prototype."""
    require_role(user, "staff")
    with user_tx(user.user_id) as conn:
        found = run_search(conn, rid)
        if found["status"] != "open":
            raise HTTPException(409, f"request is {found['status']}; only open requests can be drafted")
        used = found["results"][:3]
        if not used:
            raise HTTPException(422, "a draft must cite at least one approved guidance passage")
        client_id = found["_client_id"]
        draft_id = f"DR-{rid:06d}-{uuid.uuid4().hex[:6]}"
        sources = [{"section_id": p["section_id"], "version": p["version"]} for p in used]
        text = "Proposed note, assembled from approved guidance:\n" + "\n".join(
            f"- {p['text']} [{p['section_id']} v{p['version']}]" for p in used)
        conn.execute("UPDATE request SET status = 'in_review' WHERE request_id = %s", [rid])
        write_status(conn, rid, client_id, "open", "in_review", user)
        write_audit(conn, user, client_id, "draft", f"request:{rid}/draft:{draft_id}", ["status"])
        write_outbox(conn, rid, "request.in_review")
        mongo_db().drafts.insert_one({"_id": draft_id, "request_id": rid, "client_code": found["client"],
                                      "prepared_by": user.user_id, "sources": sources, "text": text,
                                      "status": "proposed"})
    return {"draft_id": draft_id, "request_id": rid, "sources": sources, "text": text}


class Review(BaseModel):
    decision: Literal["accepted", "rejected"]
    reason: str = Field(min_length=1, max_length=300)


@app.post("/drafts/{draft_id}/review")
def review_draft(draft_id: str, body: Review, user: User = Depends(current_user)):
    require_role(user, "staff", "team_lead")
    draft = mongo_db().drafts.find_one({"_id": draft_id})
    if draft is None or draft["client_code"] not in user.clients:
        raise HTTPException(404, "draft not found")
    if draft["prepared_by"] == user.user_id:
        raise HTTPException(403, "the reviewer must not be the person who prepared the draft")
    rid = draft["request_id"]
    with user_tx(user.user_id) as conn:
        row = conn.execute("SELECT client_id, status FROM request WHERE request_id = %s FOR UPDATE", [rid]).fetchone()
        if row is None:
            raise HTTPException(404, "request not found")
        client_id, status = row
        if status != "in_review":
            raise HTTPException(409, f"request is {status}, not in review")
        conn.execute("INSERT INTO review_decision (request_id, client_id, draft_ref, reviewer_id, decision, reason) "
                     "VALUES (%s, %s, %s, %s, %s, %s)", [rid, client_id, draft_id, user.user_id, body.decision, body.reason])
        new = "approved" if body.decision == "accepted" else "open"
        conn.execute("UPDATE request SET status = %s WHERE request_id = %s", [new, rid])
        write_status(conn, rid, client_id, status, new, user)
        write_audit(conn, user, client_id, "review", f"request:{rid}/draft:{draft_id}", ["status", "decision"])
        write_outbox(conn, rid, "request.reviewed")
    mongo_db().drafts.update_one({"_id": draft_id}, {"$set": {"status": body.decision}})
    return {"draft_id": draft_id, "request_id": rid, "decision": body.decision, "status": new}


# ---------------------------------------------------------------- completion

class Completion(BaseModel):
    outcome: str = Field(min_length=1, max_length=200)
    idempotency_key: str = Field(min_length=8, max_length=64)
    channel: Literal["staff_direct", "client_connector"] = "staff_direct"


@app.post("/requests/{rid}/complete")
def complete_request(rid: int, body: Completion, user: User = Depends(current_user),
                     x_poc_fault: Optional[str] = Header(default=None)):
    """Status, outcome, client action, status event, audit event, and outbox row commit together."""
    require_role(user, "staff")
    with user_tx(user.user_id) as conn:
        row = conn.execute("SELECT client_id, status FROM request WHERE request_id = %s FOR UPDATE", [rid]).fetchone()
        if row is None:
            raise HTTPException(404, "request not found")
        client_id, status = row
        prior = conn.execute("SELECT request_id FROM client_action WHERE idempotency_key = %s",
                             [body.idempotency_key]).fetchone()
        if prior:
            if prior[0] == rid:
                return {"request_id": rid, "status": "completed", "replayed": True}
            raise HTTPException(409, "idempotency key already used for another request")
        if status != "approved":
            raise HTTPException(409, f"request is {status}; completion needs an accepted review decision")
        last = conn.execute("SELECT decision FROM review_decision WHERE request_id = %s "
                            "ORDER BY decided_at DESC, decision_id DESC LIMIT 1", [rid]).fetchone()
        if last is None or last[0] != "accepted":
            raise HTTPException(409, "completion needs an accepted review decision")
        conn.execute("INSERT INTO client_action (request_id, client_id, idempotency_key, channel, status) "
                     "VALUES (%s, %s, %s, %s, 'done')", [rid, client_id, body.idempotency_key, body.channel])
        conn.execute("UPDATE request SET status = 'completed', outcome = %s WHERE request_id = %s", [body.outcome, rid])
        write_status(conn, rid, client_id, "approved", "completed", user)
        write_audit(conn, user, client_id, "complete", f"request:{rid}", ["status", "outcome"])
        maybe_fail("after_audit", x_poc_fault)
        write_outbox(conn, rid, "request.completed")
        maybe_fail("before_commit", x_poc_fault)
    return {"request_id": rid, "status": "completed", "replayed": False}


# ---------------------------------------------------------------- audit trail

@app.get("/audit")
def audit_trail(request: int, user: User = Depends(current_user)):
    require_role(user, "team_lead", "manager", "dpo")
    with user_tx(user.user_id) as conn:
        rows = conn.execute(
            "SELECT audit_id, occurred_at, handle_of(actor_id), actor_role, action, object_ref, changed_fields, client_id "
            "FROM audit_event WHERE object_ref = %s OR object_ref LIKE %s ORDER BY audit_id",
            [f"request:{request}", f"request:{request}/%"]).fetchall()
        if not rows:
            raise HTTPException(404, "no audit events visible for this request")
        write_audit(conn, user, rows[0][7], "read_audit", f"audit?request={request}")
    return {"request_id": request, "events": [
        {"audit_id": r[0], "occurred_at": r[1].isoformat(), "actor": r[2], "actor_role": r[3], "action": r[4],
         "object_ref": r[5], "changed_fields": r[6]} for r in rows]}


@app.get("/audit/verify")
def audit_verify(user: User = Depends(current_user)):
    require_role(user, "dpo")
    with user_tx(user.user_id) as conn:
        ok, checked, first_bad = conn.execute("SELECT * FROM audit_verify()").fetchone()
    return {"ok": ok, "events_checked": checked, "first_bad_event": first_bad}

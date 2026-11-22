"""C4: outbox -> Redis stream -> idempotent consumers keep MongoDB, Neo4j, and the Redis queue in
step with PostgreSQL; a duplicate or older event changes nothing; Redis and Neo4j can be rebuilt
from PostgreSQL, and recent events can be replayed from the outbox."""
import json
import time
import uuid

import conftest
from conftest import approved_request, auth, create_request
from worker.rebuild import rebuild_neo4j, rebuild_redis, replay

STREAM = "poc:events"
GROUPS = ("proj-mongo", "proj-neo4j", "proj-redis")
QUEUE = "queue:CL-01:appointment_change"


def wait_until(check, timeout=20.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        value = check()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError("condition not reached in time")


def drain(admin, rds):
    """Wait until every outbox row is published and every consumer group has caught up."""
    def done():
        if admin.execute("SELECT count(*) FROM poc.outbox_event WHERE published_at IS NULL").fetchone()[0]:
            return False
        groups = rds.xinfo_groups(STREAM)
        return len(groups) == 3 and all(g["pending"] == 0 and g["lag"] == 0 for g in groups)
    wait_until(done)


def node_status(graph, rid):
    with graph.session() as s:
        rec = s.run("MATCH (r:Request {id: $id}) RETURN r.status AS status", id=rid).single()
    return rec["status"] if rec else None


def complete(api, rid):
    r = api.post(f"/requests/{rid}/complete", headers=auth(api, "staff-a"),
                 json={"outcome": "slot moved and confirmed", "idempotency_key": uuid.uuid4().hex})
    assert r.status_code == 200, r.text


def test_events_reach_every_projection(api, graph, mongo, rds):
    rid = approved_request(api)
    wait_until(lambda: node_status(graph, rid) == "approved")
    wait_until(lambda: rds.zscore(QUEUE, rid) is not None)
    complete(api, rid)
    wait_until(lambda: node_status(graph, rid) == "completed")
    wait_until(lambda: mongo.work_packages.find_one({"request_id": rid})["status"] == "completed")
    wait_until(lambda: rds.zscore(QUEUE, rid) is None)
    assert rds.hget(f"req:{rid}:status", "status") == "completed"


def test_duplicate_and_older_events_change_nothing(api, admin, graph, mongo, rds):
    rid = approved_request(api)
    complete(api, rid)
    drain(admin, rds)
    mine = [f for _, f in rds.xrevrange(STREAM, count=5000) if json.loads(f["payload"])["request_id"] == rid]
    assert [f["type"] for f in reversed(mine)][-1] == "request.completed"
    before = {g: int(rds.get(f"metrics:skipped:{g}") or 0) for g in GROUPS}
    for f in reversed(mine):          # publish every event again, oldest first
        rds.xadd(STREAM, f)
    drain(admin, rds)
    after = {g: int(rds.get(f"metrics:skipped:{g}") or 0) for g in GROUPS}
    n = len(mine)
    assert after["proj-neo4j"] - before["proj-neo4j"] == n
    assert after["proj-redis"] - before["proj-redis"] == n
    assert after["proj-mongo"] - before["proj-mongo"] == n - 1   # the API itself writes the created state
    assert node_status(graph, rid) == "completed"
    assert mongo.work_packages.find_one({"request_id": rid})["status"] == "completed"
    assert rds.zscore(QUEUE, rid) is None


def _open_ids(admin):
    return {r[0] for r in admin.execute(
        "SELECT r.request_id FROM poc.request r JOIN poc.workflow_type w USING (workflow_type_id) "
        "WHERE r.client_id = 1 AND w.name = 'appointment_change' AND r.status <> 'completed'").fetchall()}


def test_redis_rebuild_matches_postgres(api, admin, rds):
    create_request(api, "staff-a", "CL-01")
    drain(admin, rds)
    rds.delete(QUEUE)
    result = rebuild_redis()
    assert result["open_requests"] >= 1
    assert {int(x) for x in rds.zrange(QUEUE, 0, -1)} == _open_ids(admin)


def test_redis_loss_means_sign_in_again_then_recovery(api, admin, graph, rds):
    rid = create_request(api, "staff-a", "CL-01").json()["request_id"]
    old = auth(api, "staff-a")
    rds.flushall()                                  # sessions, queues, stream, and groups are gone
    assert api.get("/requests", params={"client": "CL-01"}, headers=old).status_code == 401
    conftest._tokens.clear()
    assert api.get("/requests", params={"client": "CL-01"}, headers=auth(api, "staff-a")).status_code == 200
    replay(10)                                      # republish the last 10 minutes from the outbox
    rebuild_redis()
    drain(admin, rds)
    assert {int(x) for x in rds.zrange(QUEUE, 0, -1)} == _open_ids(admin)
    assert node_status(graph, rid) == "open"
    new = create_request(api, "staff-a", "CL-01").json()["request_id"]
    wait_until(lambda: rds.zscore(QUEUE, new) is not None)   # the consumers recovered


def test_neo4j_rebuild_restores_rules_and_requests(api, admin, graph, rds):
    rid = approved_request(api)
    complete(api, rid)
    drain(admin, rds)
    result = rebuild_neo4j()
    assert result["requests"] == admin.execute("SELECT count(*) FROM poc.request").fetchone()[0]
    assert node_status(graph, rid) == "completed"
    with graph.session() as s:
        fields = s.run("MATCH (:Client {code: 'CL-01'})-[:USES]->(:WorkflowType {name: 'appointment_change'})"
                       "-[:REQUIRES]->(f) RETURN collect(f.name) AS f").single()["f"]
    assert set(fields) == {"client_reference", "requested_window", "callback_confirmed"}

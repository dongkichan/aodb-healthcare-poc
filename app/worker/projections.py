"""The three projections fed by the outbox stream. Each one is idempotent: it applies an event
only if the event's outbox_id is newer than the last one applied to that request, so a duplicate
or a late, older event changes nothing.

Each apply function returns True (applied), False (a duplicate or older event, skipped), or None
(the target document was missing, which is counted separately and never mistaken for a duplicate).
"""
import json
import os
from functools import lru_cache

import redis
from neo4j import GraphDatabase
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

WP_STATUS = {"held": "held", "open": "in_progress", "in_review": "in_review", "approved": "approved",
             "completed": "completed"}


@lru_cache
def rds() -> redis.Redis:
    return redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


@lru_cache
def mdb():
    return MongoClient(os.environ["MONGO_URL"]).get_database("poc", write_concern=WriteConcern(w="majority"))


@lru_cache
def graph():
    return GraphDatabase.driver(os.environ["NEO4J_URL"], auth=("neo4j", os.environ["NEO4J_PASSWORD"]))


def apply_mongo(oid: int, p: dict):
    """Mirror the request status onto its work package (keyed by request ID, guarded by outbox ID)."""
    wps = mdb().work_packages
    if p.get("_event") == "request.created":
        # The API writes the package itself. If that write never landed, leave an empty package
        # that staff must complete; $setOnInsert never overwrites a package the API wrote.
        wps.update_one({"_id": f"WP-{p['request_id']:06d}"},
                       {"$setOnInsert": {"request_id": p["request_id"], "client_code": p["client"],
                                         "workflow": p["workflow"], "fields": {}, "missing_fields": ["(re-enter)"],
                                         "guidance_refs": [], "version": 0, "status": "needs_fields"}},
                       upsert=True)
        return True
    res = wps.update_one(
        {"request_id": p["request_id"], "$or": [{"last_outbox_id": {"$exists": False}}, {"last_outbox_id": {"$lt": oid}}]},
        {"$set": {"status": WP_STATUS[p["status"]], "last_outbox_id": oid}})
    if res.modified_count == 1:
        return True
    return False if wps.count_documents({"request_id": p["request_id"]}, limit=1) else None


def apply_neo4j(oid: int, p: dict):
    """Request node with ID, client, type, and outcome only; no request content."""
    with graph().session() as s:
        rec = s.run("""
            OPTIONAL MATCH (w:WorkflowType {id: $wf})
            WITH w WHERE w IS NOT NULL
            MERGE (r:Request {id: $rid})
              ON CREATE SET r.client = $client, r.type = $workflow, r.last_outbox_id = 0
            MERGE (r)-[:OF_TYPE]->(w)
            WITH r, r.last_outbox_id < $oid AS newer
            FOREACH (_ IN CASE WHEN newer THEN [1] ELSE [] END |
              SET r.status = $status, r.last_outbox_id = $oid,
                  r.outcome = CASE WHEN $status = 'completed' THEN 'completed' ELSE null END)
            RETURN newer""",
                    wf=f"{p['client']}/{p['workflow']}", rid=p["request_id"], client=p["client"],
                    workflow=p["workflow"], status=p["status"], oid=oid).single()
    if rec is None:
        return None  # the workflow rule is missing from the graph
    return bool(rec["newer"])


# Status cache and work queue in one atomic step, guarded by the outbox ID.
# ARGV[5] = 'rebuild' re-applies an event whose ID equals the stored one (rebuilds replay the
# latest state); consumers skip it.
_REDIS_APPLY = """
local last = tonumber(redis.call('HGET', KEYS[1], 'oid') or '0')
local incoming = tonumber(ARGV[1])
if incoming < last or (incoming == last and ARGV[5] ~= 'rebuild') then return 0 end
redis.call('HSET', KEYS[1], 'oid', ARGV[1], 'status', ARGV[2])
if ARGV[2] == 'completed' then
  redis.call('ZREM', KEYS[2], ARGV[3])
else
  redis.call('ZADD', KEYS[2], ARGV[4], ARGV[3])
end
return 1
"""


def redis_apply(oid: int, status: str, rid: int, client: str, workflow: str, due: float, mode: str = "consume") -> bool:
    keys = [f"req:{rid}:status", f"queue:{client}:{workflow}"]
    return rds().eval(_REDIS_APPLY, 2, *keys, oid, status, rid, due, mode) == 1


def apply_redis(oid: int, p: dict):
    return redis_apply(oid, p["status"], p["request_id"], p["client"], p["workflow"], p["due"])


PROJECTIONS = {"proj-mongo": apply_mongo, "proj-neo4j": apply_neo4j, "proj-redis": apply_redis}


def decode(fields: dict) -> tuple:
    p = json.loads(fields["payload"])
    p["_event"] = fields["type"]
    return int(fields["outbox_id"]), p

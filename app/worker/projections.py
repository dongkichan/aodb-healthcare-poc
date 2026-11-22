"""The three projections fed by the outbox stream. Each one is idempotent: it applies an event
only if the event's outbox_id is newer than the last one applied to that request, so a duplicate
or a late, older event changes nothing."""
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


def apply_mongo(oid: int, p: dict) -> bool:
    """Mirror the request status onto its work package (keyed by request ID, guarded by outbox ID)."""
    if p["status"] in ("held", "open") and p.get("_event") == "request.created":
        return True  # the API created the work package itself
    res = mdb().work_packages.update_one(
        {"request_id": p["request_id"], "$or": [{"last_outbox_id": {"$exists": False}}, {"last_outbox_id": {"$lt": oid}}]},
        {"$set": {"status": WP_STATUS[p["status"]], "last_outbox_id": oid}})
    return res.modified_count == 1


def apply_neo4j(oid: int, p: dict) -> bool:
    """Request node with ID, client, type, and outcome only; no request content."""
    with graph().session() as s:
        n = s.run("""
            MATCH (w:WorkflowType {id: $wf})
            MERGE (r:Request {id: $rid})
              ON CREATE SET r.client = $client, r.type = $workflow, r.last_outbox_id = 0
            MERGE (r)-[:OF_TYPE]->(w)
            WITH r WHERE r.last_outbox_id < $oid
            SET r.status = $status, r.last_outbox_id = $oid,
                r.outcome = CASE WHEN $status = 'completed' THEN 'completed' ELSE null END
            RETURN count(r) AS n""",
                  wf=f"{p['client']}/{p['workflow']}", rid=p["request_id"], client=p["client"],
                  workflow=p["workflow"], status=p["status"], oid=oid).single()["n"]
    return n == 1


# Status cache and work queue in one atomic step, guarded by the outbox ID.
_REDIS_APPLY = """
local last = tonumber(redis.call('HGET', KEYS[1], 'oid') or '0')
if tonumber(ARGV[1]) <= last then return 0 end
redis.call('HSET', KEYS[1], 'oid', ARGV[1], 'status', ARGV[2])
if ARGV[2] == 'completed' then
  redis.call('ZREM', KEYS[2], ARGV[3])
else
  redis.call('ZADD', KEYS[2], ARGV[4], ARGV[3])
end
return 1
"""


def apply_redis(oid: int, p: dict) -> bool:
    r = rds()
    keys = [f"req:{p['request_id']}:status", f"queue:{p['client']}:{p['workflow']}"]
    return r.eval(_REDIS_APPLY, 2, *keys, oid, p["status"], p["request_id"], p["due"]) == 1


PROJECTIONS = {"proj-mongo": apply_mongo, "proj-neo4j": apply_neo4j, "proj-redis": apply_redis}


def decode(fields: dict) -> tuple:
    p = json.loads(fields["payload"])
    p["_event"] = fields["type"]
    return int(fields["outbox_id"]), p

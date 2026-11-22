"""Shared helpers. The tests run against the live stack (docker compose run --rm tests)."""
import os
import uuid

import httpx
import psycopg
import pytest
import redis
from neo4j import GraphDatabase
from pymongo import MongoClient

from seed.data import user_uuid

API = os.environ.get("API_URL", "http://localhost:8000")

COMPLETE_FIELDS = {
    ("CL-01", "appointment_change"): {"client_reference": "REF-1", "requested_window": "AM", "callback_confirmed": True},
    ("CL-02", "appointment_change"): {"client_reference": "REF-2", "requested_window": "PM", "preferred_channel": "text"},
    ("CL-03", "appointment_change"): {"client_reference": "REF-3", "requested_window": "AM"},
    ("CL-01", "records_request"): {"client_reference": "REF-4", "requester_type": "patient",
                                    "delivery_method": "portal", "consent_on_file": True},
}
SUMMARY = {
    "appointment_change": "Caller asked to move the visit to a morning slot and confirm by phone.",
    "records_request": "Requester asked for a copy of visit records through the portal.",
}

_tokens = {}


@pytest.fixture(scope="session")
def api():
    with httpx.Client(base_url=API, timeout=30) as c:
        yield c


def auth(api, handle):
    if handle not in _tokens:
        r = api.post("/dev/token", json={"handle": handle})
        assert r.status_code == 200, r.text
        _tokens[handle] = {"Authorization": f"Bearer {r.json()['access_token']}"}
    return _tokens[handle]


def new_ref():
    return f"T-{uuid.uuid4().hex[:12]}"


def create_request(api, handle, client, workflow="appointment_change", fields=None, ref=None):
    body = {"client": client, "workflow": workflow, "external_ref": ref or new_ref(),
            "summary": SUMMARY[workflow],
            "fields": COMPLETE_FIELDS[(client, workflow)] if fields is None else fields}
    return api.post("/requests", json=body, headers=auth(api, handle))


def approved_request(api, preparer="staff-a", reviewer="staff-b", client="CL-01"):
    """A request taken through draft and accepted review, ready to complete."""
    r = create_request(api, preparer, client)
    assert r.status_code == 201, r.text
    rid = r.json()["request_id"]
    d = api.post(f"/requests/{rid}/drafts", headers=auth(api, preparer))
    assert d.status_code == 201, d.text
    v = api.post(f"/drafts/{d.json()['draft_id']}/review", json={"decision": "accepted", "reason": "checked against guidance"},
                 headers=auth(api, reviewer))
    assert v.status_code == 200, v.text
    return rid


@pytest.fixture
def admin():
    """Superuser connection: bypasses row-level security, used only to check what was stored."""
    with psycopg.connect(os.environ["PG_ADMIN_DSN"], autocommit=True) as conn:
        yield conn


@pytest.fixture
def app_conn():
    """A raw app_rw connection, the same login the API uses."""
    with psycopg.connect(os.environ["PG_DSN"]) as conn:
        yield conn


def as_user(conn, handle):
    conn.execute("SELECT set_config('app.user_id', %s, true)", [user_uuid(handle)])


@pytest.fixture(scope="session")
def mongo():
    return MongoClient(os.environ["MONGO_URL"]).get_database("poc")


@pytest.fixture(scope="session")
def graph():
    d = GraphDatabase.driver(os.environ["NEO4J_URL"], auth=("neo4j", os.environ["NEO4J_PASSWORD"]))
    yield d
    d.close()


@pytest.fixture(scope="session")
def rds():
    return redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)

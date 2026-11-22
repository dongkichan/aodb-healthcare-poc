"""C2: completion commits status, outcome, audit, and outbox together or not at all,
and it is refused without an accepted review decision."""
import uuid

import pytest

from conftest import approved_request, auth, create_request


def _complete(api, rid, handle="staff-a", key=None, fault=None):
    headers = dict(auth(api, handle))
    if fault:
        headers["X-PoC-Fault"] = fault
    return api.post(f"/requests/{rid}/complete", headers=headers,
                    json={"outcome": "slot moved and confirmed", "idempotency_key": key or uuid.uuid4().hex})


def _counts(admin, rid):
    return admin.execute(
        """SELECT (SELECT status FROM poc.request WHERE request_id = %(r)s),
                  (SELECT count(*) FROM poc.request_event WHERE request_id = %(r)s AND to_status = 'completed'),
                  (SELECT count(*) FROM poc.audit_event WHERE object_ref = 'request:' || %(r)s AND action = 'complete'),
                  (SELECT count(*) FROM poc.outbox_event WHERE aggregate_id = %(r)s::text AND event_type = 'request.completed'),
                  (SELECT count(*) FROM poc.client_action WHERE request_id = %(r)s)""", {"r": rid}).fetchone()


def test_completion_refused_without_accepted_review(api, admin):
    rid = create_request(api, "staff-a", "CL-01").json()["request_id"]
    r = _complete(api, rid)
    assert r.status_code == 409
    assert _counts(admin, rid) == ("open", 0, 0, 0, 0)


def test_preparer_cannot_review_own_draft(api):
    rid = create_request(api, "staff-a", "CL-01").json()["request_id"]
    draft = api.post(f"/requests/{rid}/drafts", headers=auth(api, "staff-a")).json()
    r = api.post(f"/drafts/{draft['draft_id']}/review", json={"decision": "accepted", "reason": "self"},
                 headers=auth(api, "staff-a"))
    assert r.status_code == 403


def test_completion_commits_all_rows_together(api, admin):
    rid = approved_request(api)
    assert _complete(api, rid).status_code == 200
    assert _counts(admin, rid) == ("completed", 1, 1, 1, 1)


@pytest.mark.parametrize("fault", ["after_audit", "before_commit"])
def test_failure_inside_the_transaction_leaves_nothing(api, admin, fault):
    rid = approved_request(api)
    r = _complete(api, rid, fault=fault)
    assert r.status_code == 500
    assert _counts(admin, rid) == ("approved", 0, 0, 0, 0)
    ok, _, first_bad = admin.execute("SELECT * FROM poc.audit_verify()").fetchone()
    assert ok and first_bad is None   # the chain head rolled back with the transaction


def test_retry_with_same_key_is_not_repeated(api, admin):
    rid = approved_request(api)
    key = uuid.uuid4().hex
    assert _complete(api, rid, key=key).status_code == 200
    again = _complete(api, rid, key=key)
    assert again.status_code == 200 and again.json()["replayed"] is True
    assert _counts(admin, rid) == ("completed", 1, 1, 1, 1)


def test_team_lead_cannot_complete(api):
    rid = approved_request(api)
    assert _complete(api, rid, handle="lead-a").status_code == 403


def test_rejected_draft_cannot_be_accepted_later(api):
    rid = create_request(api, "staff-a", "CL-01").json()["request_id"]
    d1 = api.post(f"/requests/{rid}/drafts", headers=auth(api, "staff-a")).json()["draft_id"]
    r = api.post(f"/drafts/{d1}/review", json={"decision": "rejected", "reason": "wrong slot"}, headers=auth(api, "staff-b"))
    assert r.status_code == 200 and r.json()["status"] == "open"
    d2 = api.post(f"/requests/{rid}/drafts", headers=auth(api, "staff-a")).json()["draft_id"]
    late = api.post(f"/drafts/{d1}/review", json={"decision": "accepted", "reason": "late"}, headers=auth(api, "staff-b"))
    assert late.status_code == 409
    ok = api.post(f"/drafts/{d2}/review", json={"decision": "accepted", "reason": "fixed"}, headers=auth(api, "staff-b"))
    assert ok.status_code == 200 and ok.json()["status"] == "approved"


def test_key_used_on_another_clients_request_is_refused_cleanly(api):
    key = uuid.uuid4().hex
    assert _complete(api, approved_request(api), key=key).status_code == 200
    other = approved_request(api, preparer="staff-c", reviewer="lead-c", client="CL-03")
    r = _complete(api, other, handle="staff-c", key=key)
    assert r.status_code == 409

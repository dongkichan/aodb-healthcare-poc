"""C3: the audit trail is append-only, its hash chain exposes tampering even under concurrent
writes, and every read of Restricted data is logged."""
from concurrent.futures import ThreadPoolExecutor

import psycopg
import pytest

from conftest import approved_request, as_user, auth, create_request
from seed.data import user_uuid


def test_app_login_cannot_update_or_delete_audit_events(app_conn):
    for stmt in ["UPDATE poc.audit_event SET action = 'create'", "DELETE FROM poc.audit_event"]:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with app_conn.transaction():
                as_user(app_conn, "dpo-1")
                app_conn.execute(stmt)


def test_staff_cannot_read_the_audit_trail(api, app_conn):
    rid = create_request(api, "staff-a", "CL-01").json()["request_id"]
    assert api.get("/audit", params={"request": rid}, headers=auth(api, "staff-a")).status_code == 403
    with app_conn.transaction():
        as_user(app_conn, "staff-a")
        assert app_conn.execute("SELECT count(*) FROM poc.audit_event").fetchone()[0] == 0


def test_reads_of_restricted_data_are_logged(api, admin):
    actor = user_uuid("staff-a")
    q = "SELECT count(*) FROM poc.audit_event WHERE actor_id = %s AND action = 'read_restricted'"
    before = admin.execute(q, [actor]).fetchone()[0]
    assert api.get("/requests", params={"client": "CL-01"}, headers=auth(api, "staff-a")).status_code == 200
    assert admin.execute(q, [actor]).fetchone()[0] == before + 1


def test_manager_reads_the_trail_in_order(api):
    rid = approved_request(api)
    r = api.get("/audit", params={"request": rid}, headers=auth(api, "manager-1"))
    assert r.status_code == 200
    actions = [e["action"] for e in r.json()["events"]]
    assert actions == ["create", "draft", "review"]
    assert [e["actor"] for e in r.json()["events"]] == ["staff-a", "staff-a", "staff-b"]


def test_chain_holds_under_concurrent_writers(api, admin):
    with ThreadPoolExecutor(max_workers=25) as pool:
        codes = list(pool.map(lambda _: create_request(api, "staff-a", "CL-01").status_code, range(50)))
    assert codes == [201] * 50
    ok, checked, first_bad = admin.execute("SELECT * FROM poc.audit_verify()").fetchone()
    assert ok, f"chain broken at {first_bad}"
    assert admin.execute("SELECT max(audit_id) = count(*) FROM poc.audit_event").fetchone()[0]


def test_edit_by_a_superuser_is_detected(admin):
    with admin.transaction(force_rollback=True):
        target = admin.execute("SELECT audit_id FROM poc.audit_event ORDER BY audit_id OFFSET 2 LIMIT 1").fetchone()[0]
        admin.execute("UPDATE poc.audit_event SET action = 'read_audit' WHERE audit_id = %s", [target])
        ok, _, first_bad = admin.execute("SELECT * FROM poc.audit_verify()").fetchone()
        assert not ok and first_bad == target + 1


def test_removing_the_latest_event_is_detected(admin):
    with admin.transaction(force_rollback=True):
        admin.execute("DELETE FROM poc.audit_event WHERE audit_id = (SELECT max(audit_id) FROM poc.audit_event)")
        ok, _, _ = admin.execute("SELECT * FROM poc.audit_verify()").fetchone()
        assert not ok


def test_only_the_dpo_runs_the_chain_check(api):
    assert api.get("/audit/verify", headers=auth(api, "manager-1")).status_code == 403
    r = api.get("/audit/verify", headers=auth(api, "dpo-1"))
    assert r.status_code == 200 and r.json()["ok"] is True

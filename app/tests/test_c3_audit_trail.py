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


def test_edit_without_repairing_the_chain_is_detected(admin):
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


def test_owner_who_repairs_the_chain_is_not_detected(admin):
    """Documented limit: the hash is unkeyed and the head row lives in the same database, so the
    owner or a superuser can edit an event and recompute the later hashes and the head. Anchoring
    the head hash outside the database (Part 6, Section 6.5.2) closes this gap."""
    with admin.transaction(force_rollback=True):
        last = admin.execute("SELECT max(audit_id) FROM poc.audit_event").fetchone()[0]
        target = last - 3
        admin.execute("UPDATE poc.audit_event SET action = 'read_audit' WHERE audit_id = %s", [target])
        for aid in range(target + 1, last + 1):
            admin.execute("UPDATE poc.audit_event e SET prev_hash = poc.audit_row_hash(p) FROM poc.audit_event p "
                          "WHERE p.audit_id = %s AND e.audit_id = %s", [aid - 1, aid])
        admin.execute("UPDATE poc.audit_chain_head SET last_hash = (SELECT poc.audit_row_hash(e) "
                      "FROM poc.audit_event e WHERE e.audit_id = %s)", [last])
        ok, _, _ = admin.execute("SELECT * FROM poc.audit_verify()").fetchone()
        assert ok


def test_only_the_dpo_may_run_the_chain_check_in_the_database(app_conn):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        with app_conn.transaction():
            as_user(app_conn, "manager-1")
            app_conn.execute("SELECT * FROM poc.audit_verify()")


def test_audit_event_role_must_match_the_users_role(app_conn):
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        with app_conn.transaction():
            as_user(app_conn, "staff-a")
            app_conn.execute("INSERT INTO poc.audit_event (actor_id, actor_role, client_id, action, object_ref) "
                             "VALUES (%s, 'dpo', 1, 'read_audit', 'spoof')", [user_uuid("staff-a")])

"""C1: row-level security confines every query to the user's assigned clients.

Claimed in the data architecture (relational model) and the access-control section: it holds
only if the app role does not own the tables, cannot bypass row security, and sets the user
per transaction, so a pooled connection never carries one user's setting to another.
"""
from concurrent.futures import ThreadPoolExecutor

from conftest import as_user, auth, create_request

CLIENT_TABLES = ["client", "workflow_type", "request", "request_event", "review_decision",
                 "client_action", "outbox_event", "guidance_chunk", "audit_event"]


def test_app_login_is_not_owner_and_cannot_bypass(admin):
    sup, bypass = admin.execute("SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = 'app_rw'").fetchone()
    assert (sup, bypass) == (False, False)
    rows = admin.execute("SELECT c.relname, pg_get_userbyid(c.relowner), c.relrowsecurity FROM pg_class c "
                         "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'poc' AND c.relkind = 'r'").fetchall()
    owners = {name: owner for name, owner, _ in rows}
    assert set(owners.values()) == {"poc_owner"}
    rls = {name: on for name, _, on in rows}
    assert all(rls[t] for t in CLIENT_TABLES)


def test_gateway_refuses_unassigned_client(api):
    assert api.get("/requests", params={"client": "CL-03"}, headers=auth(api, "staff-b")).status_code == 403
    assert create_request(api, "staff-b", "CL-03").status_code == 403


def test_database_returns_no_rows_for_unassigned_client(api, admin, app_conn):
    assert create_request(api, "staff-c", "CL-03").status_code == 201
    assert create_request(api, "staff-b", "CL-01").status_code == 201
    assert admin.execute("SELECT count(*) FROM poc.request WHERE client_id = 3").fetchone()[0] > 0
    with app_conn.transaction():
        as_user(app_conn, "staff-b")  # assigned CL-01 only
        assert app_conn.execute("SELECT count(*) FROM poc.request WHERE client_id = 3").fetchone()[0] == 0
        assert app_conn.execute("SELECT count(*) FROM poc.request WHERE client_id = 1").fetchone()[0] > 0


def test_request_of_another_client_is_not_found_by_id(api):
    rid = create_request(api, "staff-c", "CL-03").json()["request_id"]
    r = api.get("/guidance/search", params={"request": rid}, headers=auth(api, "staff-b"))
    assert r.status_code == 404


def test_no_user_set_sees_nothing(app_conn):
    with app_conn.transaction():
        assert app_conn.execute("SELECT count(*) FROM poc.request").fetchone()[0] == 0
        assert app_conn.execute("SELECT count(*) FROM poc.guidance_chunk").fetchone()[0] == 0


def test_transaction_setting_ends_with_the_transaction(api, app_conn):
    assert create_request(api, "staff-a", "CL-01").status_code == 201
    with app_conn.transaction():
        as_user(app_conn, "staff-a")
        assert app_conn.execute("SELECT count(*) FROM poc.request").fetchone()[0] > 0
    with app_conn.transaction():
        assert app_conn.execute("SELECT app_user_id()").fetchone()[0] is None
        assert app_conn.execute("SELECT count(*) FROM poc.request").fetchone()[0] == 0


def test_session_setting_would_leak_to_the_next_transaction(api, app_conn):
    """Negative control: why the API uses the transaction-scoped form."""
    assert create_request(api, "staff-a", "CL-01").status_code == 201
    with app_conn.transaction():
        uid = app_conn.execute("SELECT user_id_for_handle('staff-a')").fetchone()[0]
        app_conn.execute("SELECT set_config('app.user_id', %s, false)", [str(uid)])
    with app_conn.transaction():
        leaked = app_conn.execute("SELECT count(*) FROM poc.request").fetchone()[0]
        app_conn.execute("RESET app.user_id")
    assert leaked > 0


def test_concurrent_users_on_one_pool_never_mix(api):
    assert create_request(api, "staff-b", "CL-01").status_code == 201
    assert create_request(api, "staff-c", "CL-03").status_code == 201
    calls = [("staff-b", "CL-01"), ("staff-c", "CL-03")] * 30

    def run(call):
        handle, client = call
        r = api.get("/requests", params={"client": client}, headers=auth(api, handle))
        return r.status_code, {row["client"] for row in r.json()["requests"]}, len(r.json()["requests"]), client

    with ThreadPoolExecutor(max_workers=20) as pool:
        results = list(pool.map(run, calls))
    for status, clients, n, expected in results:
        assert status == 200
        assert n > 0                  # a leaked setting from the other user would hide these rows
        assert clients == {expected}


def test_unassigned_role_has_no_request_access(api):
    r = api.get("/requests", params={"client": "CL-01"}, headers=auth(api, "custodian-1"))
    assert r.status_code == 403

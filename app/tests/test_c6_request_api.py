"""C6: POST /requests checks required fields in Neo4j, commits to PostgreSQL, and creates the
MongoDB work package in one call."""
from conftest import auth, create_request, new_ref


def test_create_request_writes_three_stores(api, admin, mongo):
    r = create_request(api, "staff-a", "CL-01")
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["status"] == "open" and body["missing_fields"] == []
    assert set(body["timings_ms"]) == {"neo4j", "postgres", "mongodb"}
    rid = body["request_id"]
    assert admin.execute("SELECT status FROM poc.request WHERE request_id = %s", [rid]).fetchone()[0] == "open"
    assert admin.execute("SELECT count(*) FROM poc.outbox_event WHERE aggregate_id = %s::text", [rid]).fetchone()[0] == 1
    wp = mongo.work_packages.find_one({"request_id": rid})
    assert wp["_id"] == body["work_package_id"] and wp["version"] == 1
    assert wp["fields"]["callback_confirmed"] is True


def test_missing_required_field_holds_the_request(api):
    fields = {"client_reference": "REF-9", "requested_window": "AM"}  # CL-01 also requires callback_confirmed
    r = create_request(api, "staff-a", "CL-01", fields=fields)
    assert r.status_code == 201
    assert r.json()["status"] == "held" and r.json()["missing_fields"] == ["callback_confirmed"]


def test_required_fields_differ_by_client(api):
    fields = {"client_reference": "REF-9", "requested_window": "AM", "callback_confirmed": True}
    r = create_request(api, "staff-a", "CL-02", fields=fields)  # CL-02 requires preferred_channel instead
    assert r.json()["missing_fields"] == ["preferred_channel"]


def test_duplicate_open_reference_is_rejected(api):
    ref = new_ref()
    assert create_request(api, "staff-a", "CL-01", ref=ref).status_code == 201
    assert create_request(api, "staff-a", "CL-01", ref=ref).status_code == 409


def test_unknown_workflow_is_rejected(api):
    r = api.post("/requests", headers=auth(api, "staff-a"),
                 json={"client": "CL-01", "workflow": "billing_dispute", "external_ref": new_ref(),
                       "summary": "x", "fields": {}})
    assert r.status_code == 422

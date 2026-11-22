"""C5: a client-filtered vector search returns five of that client's passages and never another
client's, and the one- or two-hop graph expansion adds the rules that apply."""
import pytest

from conftest import auth, create_request, new_ref

CASES = [("staff-a", "CL-01"), ("staff-a", "CL-02"), ("staff-c", "CL-03")]


def _search(api, handle, client, **params):
    rid = create_request(api, handle, client).json()["request_id"]
    r = api.get("/guidance/search", params={"request": rid, **params}, headers=auth(api, handle))
    assert r.status_code == 200, r.text
    return r.json()


@pytest.mark.parametrize("handle,client", CASES)
def test_results_come_only_from_the_requests_client(api, handle, client):
    body = _search(api, handle, client)
    assert body["results"], "no passages returned"
    assert {p["client"] for p in body["results"]} == {client}
    # The label comes from the passage row; the section ID carries its client's number independently.
    assert all(p["section_id"][3:5] == client[-2:] for p in body["results"])
    assert {p["workflow"] for p in body["results"]} == {"appointment_change"}


@pytest.mark.parametrize("handle,client", CASES)
def test_search_returns_five_passages(api, handle, client):
    assert len(_search(api, handle, client)["results"]) == 5


def test_near_duplicate_passage_of_another_client_is_excluded(api):
    ids = [p["section_id"] for p in _search(api, "staff-a", "CL-01")["results"]]
    assert "GD-0101#s1" in ids          # this client's callback passage
    assert "GD-0201#s1" not in ids      # CL-02's near-identical passage


def test_graph_expansion_adds_rules_for_this_client(api):
    body = _search(api, "staff-a", "CL-01")
    g = body["graph"]
    assert set(g["required_fields"]) == {"client_reference", "requested_window", "callback_confirmed"}
    assert "staff" in g["roles"]
    assert g["referenced_sections"] and all(s.startswith("GD-01") for s in g["referenced_sections"])


def _misfiled(api):
    body = {"client": "CL-01", "workflow": "appointment_change", "external_ref": new_ref(),
            "summary": "Requester wants a copy of visit records through the portal; consent form not on file.",
            "fields": {"client_reference": "REF-7", "requested_window": "AM", "callback_confirmed": True}}
    return api.post("/requests", json=body, headers=auth(api, "staff-a")).json()["request_id"]


@pytest.mark.parametrize("mode", ["default", "hnsw_iterative"])
def test_misfiled_request_still_gets_five_passages(api, mode):
    """A request filed under one workflow whose text is about another. The nearest passages overall
    then belong to the other workflow, so a filtered HNSW scan without iterative scans can return
    fewer than five, or none (pgvector README, "Filtering"). hnsw_iterative forces the index path,
    so this case is exercised at any corpus size; default is the API's own setting."""
    rid = _misfiled(api)
    r = api.get("/guidance/search", params={"request": rid, "mode": mode}, headers=auth(api, "staff-a"))
    assert r.status_code == 200
    results = r.json()["results"]
    assert len(results) == 5
    assert {p["client"] for p in results} == {"CL-01"} and {p["workflow"] for p in results} == {"appointment_change"}

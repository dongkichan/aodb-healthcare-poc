"""C5: a client-filtered vector search returns five of that client's passages and never another
client's, and the one- or two-hop graph expansion adds the rules that apply."""
import pytest

from conftest import auth, create_request

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

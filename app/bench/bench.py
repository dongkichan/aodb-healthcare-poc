"""Measures the proof of concept and writes evidence/metrics.json (merged by label).

  python -m bench.bench 1k     run against the current corpus and store the results under "1k"

Latency is wall time measured by the client (this container) unless a field says "server".
Every number in the report must come from this file.
"""
import json
import os
import platform
import statistics
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import psycopg
from pgvector.psycopg import register_vector

from seed.data import WORKFLOWS, user_uuid
from worker.projections import rds
from worker.rebuild import rebuild_neo4j, rebuild_redis

API = os.environ.get("API_URL", "http://api:8000")
OUT = Path(os.environ.get("EVIDENCE_DIR", "/srv/evidence")) / "metrics.json"
N = int(os.environ.get("BENCH_N", "200"))
GROUPS = ("proj-mongo", "proj-neo4j", "proj-redis")
FIELDS = {"client_reference": "REF-B", "requested_window": "AM", "callback_confirmed": True}

# Synthetic queries for the vector experiment: 10 per workflow, worded differently from the passages.
QUERIES = {
    "appointment_change": [
        "caller wants to move the visit to next week", "patient asks for an earlier morning slot",
        "reschedule because the caller has a conflict", "confirm the new time by phone",
        "no open slots in the window the caller wants", "same-day change requested by the caller",
        "caller prefers a text message confirmation", "move the appointment and update the client system",
        "caller cancelled and wants a new date", "check the callback number before changing the slot"],
    "referral_follow_up": [
        "follow up on a specialist referral", "nobody answered the referral call",
        "referral needs an authorization number", "caller declined the referral",
        "send the written notice after missed calls", "check the referral type before calling",
        "follow-up date is tomorrow", "log each call attempt for the referral",
        "authorization for the referral has expired", "client wants the referral visit rescheduled"],
    "records_request": [
        "requester wants a copy of visit records", "consent form is not on file",
        "send records through the portal", "another provider asked for records",
        "legal representative requests records", "confirm the delivery method",
        "partial release of records requested", "signature missing on the request",
        "client must confirm delivery of records", "records desk should handle this request"],
}


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p / 100 * len(s) + 0.5)) - 1))
    return round(s[k], 2)


def summary(values):
    return {"n": len(values), "p50_ms": pct(values, 50), "p95_ms": pct(values, 95),
            "max_ms": round(max(values), 2) if values else None,
            "mean_ms": round(statistics.fmean(values), 2) if values else None}


class Client:
    def __init__(self):
        self.http = httpx.Client(base_url=API, timeout=60)
        self.tokens = {}

    def h(self, handle):
        if handle not in self.tokens:
            r = self.http.post("/dev/token", json={"handle": handle})
            r.raise_for_status()
            self.tokens[handle] = {"Authorization": f"Bearer {r.json()['access_token']}"}
        return self.tokens[handle]

    def timed(self, method, url, handle, **kw):
        t0 = time.perf_counter()
        r = self.http.request(method, url, headers=self.h(handle), **kw)
        dt = (time.perf_counter() - t0) * 1000
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {url} -> {r.status_code} {r.text[:200]}")
        return r, dt

    def new_request(self, handle="staff-a"):
        return self.timed("POST", "/requests", handle, json={
            "client": "CL-01", "workflow": "appointment_change", "external_ref": f"B-{uuid.uuid4().hex[:12]}",
            "summary": "Caller asked to move the visit to a morning slot and confirm by phone.", "fields": FIELDS})


def environment():
    with psycopg.connect(os.environ["PG_ADMIN_DSN"]) as conn:
        pg_version = conn.execute("SHOW server_version").fetchone()[0]
        vec = conn.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'").fetchone()[0]
        counts = conn.execute("SELECT (SELECT count(*) FROM poc.guidance_chunk), (SELECT count(*) FROM poc.request), "
                              "(SELECT count(*) FROM poc.audit_event)").fetchone()
    return {"host": os.environ.get("HOST_HARDWARE", "not recorded"), "container_arch": platform.machine(),
            "container_cpus": os.cpu_count(), "postgres": pg_version, "pgvector": vec,
            "guidance_passages": counts[0], "requests_before_run": counts[1], "audit_events_before_run": counts[2]}


def endpoint_latency(c: Client) -> dict:
    for _ in range(20):  # warm-up
        r, _ = c.new_request()
        c.timed("GET", "/guidance/search", "staff-a", params={"request": r.json()["request_id"]})
    times = {k: [] for k in ["POST /requests", "GET /requests", "GET /guidance/search", "POST /requests/{id}/drafts",
                             "POST /drafts/{id}/review", "POST /requests/{id}/complete", "GET /audit"]}
    server = {"create": {"neo4j": [], "postgres": [], "mongodb": []},
              "search": {"embed": [], "pgvector": [], "neo4j": [], "mongodb": []}}
    flow = []
    for _ in range(N):
        r, dt = c.new_request(); times["POST /requests"].append(dt); rid = r.json()["request_id"]
        for k, v in r.json()["timings_ms"].items():
            server["create"][k].append(v)
        _, dt2 = c.timed("GET", "/requests", "staff-a", params={"client": "CL-01", "status": "open"})
        times["GET /requests"].append(dt2)
        s, dt3 = c.timed("GET", "/guidance/search", "staff-a", params={"request": rid}); times["GET /guidance/search"].append(dt3)
        for k, v in s.json()["timings_ms"].items():
            server["search"][k].append(v)
        d, dt4 = c.timed("POST", f"/requests/{rid}/drafts", "staff-a"); times["POST /requests/{id}/drafts"].append(dt4)
        _, dt5 = c.timed("POST", f"/drafts/{d.json()['draft_id']}/review", "staff-b",
                         json={"decision": "accepted", "reason": "checked"}); times["POST /drafts/{id}/review"].append(dt5)
        _, dt6 = c.timed("POST", f"/requests/{rid}/complete", "staff-a",
                         json={"outcome": "done", "idempotency_key": uuid.uuid4().hex})
        times["POST /requests/{id}/complete"].append(dt6)
        _, dt7 = c.timed("GET", "/audit", "manager-1", params={"request": rid}); times["GET /audit"].append(dt7)
        flow.append(dt + dt4 + dt5 + dt6)
    return {"endpoints": {k: summary(v) for k, v in times.items()},
            "server_side": {g: {k: summary(v) for k, v in d.items()} for g, d in server.items()},
            "flow_create_draft_review_complete": summary(flow)}


def concurrency(c: Client, workers=10, per_worker=20) -> dict:
    # A second staff member, so the run stays under the gateway limit of 1,200 calls per user per minute.
    def one(_):
        return c.new_request("staff-b")[1]
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        lat = list(pool.map(one, range(workers * per_worker)))
    wall = time.perf_counter() - t0
    return {"workers": workers, "requests": len(lat), "wall_seconds": round(wall, 2),
            "throughput_per_second": round(len(lat) / wall, 1), "latency": summary(lat)}


def outbox_lag() -> dict:
    r = rds()
    deadline = time.time() + 30
    with psycopg.connect(os.environ["PG_ADMIN_DSN"]) as conn:
        while time.time() < deadline:
            if conn.execute("SELECT count(*) FROM poc.outbox_event WHERE published_at IS NULL").fetchone()[0] == 0 and \
               all(g["pending"] == 0 and g["lag"] == 0 for g in r.xinfo_groups("poc:events")):
                break
            time.sleep(0.2)
    return {g: summary([float(x) for x in r.lrange(f"metrics:lag:{g}", 0, -1)]) for g in GROUPS}


def plan_shape(conn, sql, params) -> str:
    plan = conn.execute("EXPLAIN (FORMAT JSON) " + sql, params).fetchone()[0][0]["Plan"]
    parts = []
    while plan:
        parts.append(plan["Node Type"] + (f" ({plan['Index Name']})" if plan.get("Index Name") else ""))
        # Follow the main input; skip the InitPlan that evaluates the row-level-security helper.
        children = [p for p in plan.get("Plans", []) if p.get("Parent Relationship") not in ("InitPlan", "SubPlan")]
        plan = children[0] if children else None
    return " > ".join(parts)


def vector_experiment() -> dict:
    """Aligned: the query is about the workflow the filter selects (the normal case).
    Mismatched: the query is about a different workflow, as with a misfiled request. The filter
    then matches few of the query's nearest neighbours, which is the case the pgvector README warns about."""
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    vecs = {wf: model.encode(QUERIES[wf], normalize_embeddings=True) for wf in WORKFLOWS}
    aligned = [(cid, wf, v) for wf in WORKFLOWS for cid in (1, 2, 3) for v in vecs[wf]]
    mismatched = [(cid, wf, v) for wf in WORKFLOWS for other in WORKFLOWS if other != wf
                  for cid in (1, 2, 3) for v in vecs[other]]
    return {"aligned": run_modes(aligned), "mismatched": run_modes(mismatched)}


def run_modes(cases) -> dict:
    sql = ("SELECT section_id, client_id FROM guidance_chunk WHERE client_id = %s AND workflow_type = %s "
           "ORDER BY embedding <=> %s LIMIT 5")
    settings = {"exact": ["SET LOCAL enable_indexscan = off"],
                "planner": [],
                "api_default": ["SET LOCAL hnsw.iterative_scan = strict_order"],
                "hnsw": ["SET LOCAL enable_seqscan = off"],
                "hnsw_iterative": ["SET LOCAL enable_seqscan = off", "SET LOCAL hnsw.iterative_scan = strict_order"]}
    out, truth = {}, {}
    # app_rw, so row-level security applies as in the API. prepare_threshold=None: without it psycopg
    # prepares the statement after 5 runs and PostgreSQL may reuse a cached generic plan that ignores
    # the per-transaction settings below (this made every mode measure the exact plan in an earlier run).
    with psycopg.connect(os.environ["PG_DSN"], prepare_threshold=None) as conn:
        register_vector(conn)
        for mode in ["exact", "planner", "api_default", "hnsw", "hnsw_iterative"]:
            lat, counts, recall, leaks, shape = [], [], [], 0, None
            for i, (cid, wf, v) in enumerate(cases):
                with conn.transaction():
                    conn.execute("SELECT set_config('app.user_id', %s, true)", [user_uuid("manager-1")])
                    for s in settings[mode]:
                        conn.execute(s)
                    if shape is None:
                        shape = plan_shape(conn, sql, [cid, wf, v])
                    t0 = time.perf_counter()
                    rows = conn.execute(sql, [cid, wf, v]).fetchall()
                    lat.append((time.perf_counter() - t0) * 1000)
                ids = [r[0] for r in rows]
                leaks += sum(1 for r in rows if r[1] != cid)
                counts.append(len(ids))
                if mode == "exact":
                    truth[i] = set(ids)
                else:
                    recall.append(len(set(ids) & truth[i]) / max(1, len(truth[i])))
            out[mode] = {"plan": shape, "queries": len(cases), "latency": summary(lat),
                         "mean_results": round(statistics.fmean(counts), 2),
                         "queries_with_fewer_than_5": sum(1 for n in counts if n < 5),
                         "recall_at_5_vs_exact": round(statistics.fmean(recall), 4) if recall else 1.0,
                         "results_from_another_client": leaks}
    return out


def chain_check() -> dict:
    with psycopg.connect(os.environ["PG_ADMIN_DSN"]) as conn:
        t0 = time.perf_counter()
        ok, checked, first_bad = conn.execute("SELECT * FROM poc.audit_verify()").fetchone()
        return {"ok": ok, "events_checked": checked, "seconds": round(time.perf_counter() - t0, 3)}


def main(label: str) -> None:
    started = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    env = environment()
    for g in GROUPS:
        rds().delete(f"metrics:lag:{g}")
    c = Client()
    result = {"started": started, "environment": env}
    print(f"[{label}] endpoint latency, N={N}", flush=True)
    result.update(endpoint_latency(c))
    print(f"[{label}] concurrency", flush=True)
    result["concurrency_post_requests"] = concurrency(c)
    print(f"[{label}] outbox lag", flush=True)
    result["outbox_lag_ms"] = outbox_lag()
    print(f"[{label}] vector experiment", flush=True)
    result["vector_search"] = vector_experiment()
    print(f"[{label}] rebuilds and chain check", flush=True)
    result["rebuild_redis"] = rebuild_redis()
    result["rebuild_neo4j"] = rebuild_neo4j()
    result["audit_chain_check"] = chain_check()
    merged = json.loads(OUT.read_text()) if OUT.exists() else {}
    merged[label] = result
    OUT.write_text(json.dumps(merged, indent=2))
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "run")

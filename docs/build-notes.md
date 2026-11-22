# Build notes

What happened while building this proof of concept, in order, and what changed because of it.
Machine: Apple M5 (arm64), 10 cores, 16 GB; Docker Desktop 28.5.1 with 10 CPUs and 7.7 GiB.

## Tests first
- Against a stub API with only `/health`, 33 of the first 36 tests failed. The 3 that passed were schema checks (the API login does not own the tables, a session with no user sees no rows, the API login cannot change the audit trail).
- With the worker stopped, all 5 streaming (C4) tests failed; with it running they passed.
- The misfiled-request search test failed (0 passages) against the first API at 10,000 passages and passed after iterative index scans were turned on.

## Problems met
1. The role-setup shell script would not run from the mounted folder under Docker Desktop ("bad interpreter: Permission denied"); it became a psql script that reads the password with `\getenv`.
2. A policy written as `client_id = ANY ((SELECT app_user_clients()))` failed ("operator does not exist: integer = integer[]"); a cast to `int[]` fixed it.
3. At 1,000 passages the planner used an exact scan for the filtered vector query, so the HNSW filtering risk was not exercised; the benchmark also runs at 10,000 passages.
4. The secret scan blocked the first commit because the example settings held shared passwords; `scripts/init-env.sh` now writes random values per machine.
5. With automatic IDs, concurrent transactions take audit IDs in one order and commit in another, so the hash chain would not follow the IDs; the next ID and the previous hash are now taken under a lock on one head row (audit writes queue on it).
6. The gateway's rate limit (1,200 calls per user per minute) stopped the benchmark; the concurrency run uses a second staff account.
7. The first vector measurements reused a cached generic plan: psycopg prepares a statement after repeated runs, and in our runs the reused plan ignored per-transaction planner settings, so every mode measured exact search. `prepare_threshold=None` is set in the API pool and the benchmark.
8. With that fixed, plain filtered HNSW search returned fewer than five passages for most misfiled-style queries at 10,000 passages (for one records question, the index produced 57 candidates and the filter removed all of them). The API now sets `hnsw.iterative_scan = strict_order` for every search.

## Changes after an independent code review
- A test now exercises the API's own `user_tx` on a one-connection pool, so a leaked user setting would fail it; the earlier concurrency test could not detect one.
- The audit claim states its limit: the owner or a superuser can edit or delete events and repair the chain; a test documents it. `audit_verify()` is now DPO-only in the database, an event's role must match the user's role, and the hash encodes the changed-field list as JSON.
- Search results carry each passage's own client; the work-package write is an idempotent upsert, with a worker fallback; drafts lock the request, and only the latest proposed draft can be reviewed; a reused idempotency key returns 409.
- Rebuilds no longer race the consumers: Redis rebuilds go through the same guarded script, and the Neo4j rebuild pauses its consumer. The worker has per-thread heartbeats, a restart policy, and parks an entry that fails 5 times. The stream is no longer trimmed by length.
- The worker login reads only the request columns it needs; the API login may update only status and outcome. The development sign-in and the fault switch are off unless `.env` turns them on.

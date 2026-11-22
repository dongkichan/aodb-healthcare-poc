# Proof of concept: healthcare administrative support platform

Coursework for Advanced Organization of Databases (AODB Group 1). This repository is the proof of concept for a proposed data architecture: a platform that helps a healthcare business process outsourcing (BPO) provider handle administrative requests for its clients. It uses **synthetic data only**. There are no patient records, no real organizations, and no real people in it.

The proof of concept tests six claims from the design that we were not sure would work. Each claim has automated tests that were run red (failing) before the code that makes them pass was written, and every number in [`evidence/metrics.json`](evidence/metrics.json) was measured on the stack in this repository.

## What it shows

| Claim | What is tested | Tests |
|---|---|---|
| C1 Row-level security | Every query by the API login returns only the signed-in user's assigned clients: 403 at the gateway, 0 rows in the database, no leaks between users sharing pooled connections | `app/tests/test_c1_row_level_security.py` |
| C2 All-or-nothing completion | Status, outcome, client action, audit event, and outbox row commit together or not at all; no completion without an accepted review; a retried completion runs once | `app/tests/test_c2_completion.py` |
| C3 Append-only audit trail | The API login cannot change or delete audit events; a hash chain exposes edits and deletions; reads of restricted data are logged; the chain holds under 50 concurrent writers | `app/tests/test_c3_audit_trail.py` |
| C4 Outbox and stream | Committed events reach MongoDB, Neo4j, and the Redis queue through a Redis stream; duplicates and late events change nothing; Redis and Neo4j can be rebuilt from PostgreSQL | `app/tests/test_c4_outbox_stream.py` |
| C5 Client-filtered vector search | Five passages from the request's own client, never another client's, including for a misfiled request | `app/tests/test_c5_guidance_search.py` |
| C6 Request API across stores | One call checks required fields in Neo4j, commits to PostgreSQL, and creates the MongoDB work package | `app/tests/test_c6_request_api.py` |

## Architecture

| Component | Image | Role |
|---|---|---|
| PostgreSQL 17.12 + pgvector 0.8.7 | `pgvector/pgvector:0.8.7-pg17` | System of record: requests, status events, review decisions, client actions, audit trail, outbox; guidance embeddings (HNSW index) |
| MongoDB 8.0.38 | `mongo:8.0.38` | Work packages, guidance documents, drafts (single-member replica set, "majority" writes) |
| Neo4j 5.26.33 Community | `neo4j:5.26.33-community` | Rules: clients, workflows, required fields, roles, guidance links; request nodes |
| Redis 7.4.12 | `redis:7.4.12-alpine` | Sessions, rate limits, work queues, status cache, event stream |
| API (FastAPI) | built from `app/` | REST endpoints, gateway checks |
| Worker | built from `app/` | Outbox relay and three stream consumers |

The API and worker log in to PostgreSQL with their own roles. Neither owns the tables or can bypass row-level security; see `db/postgres/`.

## Run it

You need Docker Desktop (or Docker Engine with Compose v2) with about 8 GB of memory and 6 GB of free disk. The first build downloads the images and the embedding model (about 2 GB) once; after that nothing is downloaded at run time.

```bash
./scripts/init-env.sh                       # writes .env with random local passwords
docker compose up -d --build --wait         # PostgreSQL, MongoDB, Neo4j, Redis, seed, API, worker
docker compose run --rm tests               # all claim tests
docker compose run --rm tests python -m demo.run   # the scripted demo (press Enter between steps)
```

On Windows without Bash, copy `.env.example` to `.env` and fill each value with any long random string.

- API and interactive docs: http://localhost:8000/docs (sign in with `POST /dev/token` and a handle such as `staff-a`)
- Neo4j Browser: http://localhost:7474 (user `neo4j`, password from `.env`)
- Postman: import `postman/aodb-poc.postman_collection.json`

Benchmarks (they rewrite `evidence/metrics.json`):

```bash
docker compose run --rm bench python -m bench.bench 1k
docker compose run --rm -e GUIDANCE_TOTAL=10000 seed    # grow the corpus to 10,000 passages
docker compose run --rm bench python -m bench.bench 10k
```

Stop with `docker compose down`; add `-v` to delete the data.

## Synthetic users

| Handle | Role | Clients |
|---|---|---|
| staff-a | staff | CL-01, CL-02 |
| staff-b | staff | CL-01 |
| staff-c | staff | CL-03 |
| lead-a, lead-c | team lead | CL-01 and CL-02; CL-03 |
| coord-1 | client coordinator | CL-01 |
| manager-1 | manager | all |
| custodian-1 | IT custodian | none |
| dpo-1 | data protection officer | all |

## Evidence

- `evidence/metrics.json`: latency, throughput, outbox lag, vector search experiment, rebuild times (1,000 and 10,000 passages)
- `evidence/pytest-1k.txt`, `evidence/pytest-10k.txt`: test runs
- `evidence/demo-output.txt`: one run of the scripted demo
- `evidence/cli/`: PostgreSQL, MongoDB, and Redis command-line output, and the EXPLAIN ANALYZE comparison for filtered vector search

## Limits of the prototype

- Sign-in is simulated. `POST /dev/token` issues a signed token for a synthetic handle; the proposed design signs in through the organization's identity provider with OpenID Connect.
- Inside the Compose network, MongoDB and Redis run without authentication or TLS, and no data are encrypted at rest. Only the API (port 8000) and Neo4j Browser are published, and only on localhost.
- MongoDB runs as a one-member replica set and Neo4j as one Community instance; the design calls for three-member and three-primary clusters.
- Drafts are assembled from retrieved passages without a language model, because the approved AI environment is not available to the prototype.
- Measurements come from one laptop and one run per corpus size. They show behaviour, not production capacity.

"""Loads the synthetic seed data into PostgreSQL, MongoDB, and Neo4j.

Safe to run again: reference rows are inserted only when missing, and only passages that are
not yet embedded are embedded. Raising GUIDANCE_TOTAL (for example to 10000) adds passages.
"""
import json
import os
import time
from collections import defaultdict

import psycopg
from neo4j import GraphDatabase
from pgvector.psycopg import register_vector
from pymongo import MongoClient, ReplaceOne
from pymongo.write_concern import WriteConcern

from . import data as d

CLIENT_ID = {code: cid for cid, code in d.CLIENTS}


def load_postgres(total: int) -> dict:
    stats = {}
    with psycopg.connect(os.environ["PG_ADMIN_DSN"]) as pg:
        register_vector(pg)
        with pg.transaction(), pg.cursor() as cur:
            cur.executemany("INSERT INTO poc.client (client_id, name_code) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            d.CLIENTS)
            cur.executemany("INSERT INTO poc.role VALUES (%s, %s) ON CONFLICT DO NOTHING", d.ROLES)
            for handle, role, clients in d.USERS:
                uid = d.user_uuid(handle)
                cur.execute("INSERT INTO poc.staff_user VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (uid, handle, role))
                for code in clients:
                    cur.execute("INSERT INTO poc.staff_client_assignment (user_id, client_id) VALUES (%s, %s) "
                                "ON CONFLICT DO NOTHING", (uid, CLIENT_ID[code]))
            for cid, _ in d.CLIENTS:
                for wf in d.WORKFLOWS:
                    cur.execute("INSERT INTO poc.workflow_type VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                (d.workflow_id(cid, wf), cid, wf, d.TARGET_HOURS[wf]))
        existing = {r[0] for r in pg.execute("SELECT section_id FROM poc.guidance_chunk").fetchall()}
        missing = [p for p in d.all_passages(total) if p[0] not in existing]
        stats["passages_embedded_now"] = len(missing)
        if missing:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
            t0 = time.perf_counter()
            vectors = model.encode([p[3] for p in missing], batch_size=64, normalize_embeddings=True)
            stats["embedding_seconds"] = round(time.perf_counter() - t0, 2)
            with pg.transaction(), pg.cursor() as cur:
                cur.executemany(
                    "INSERT INTO poc.guidance_chunk (guidance_id, section_id, version, client_id, workflow_type, "
                    "content_hash, embedding) VALUES (%s, %s, 1, %s, %s, %s, %s)",
                    [(d.guidance_id(sid), sid, CLIENT_ID[client], wf, d.content_hash(text), vec)
                     for (sid, client, wf, text, _), vec in zip(missing, vectors)])
        stats["guidance_chunks"] = pg.execute("SELECT count(*) FROM poc.guidance_chunk").fetchone()[0]
    return stats


def load_mongo(total: int) -> dict:
    client = MongoClient(os.environ["MONGO_URL"])
    db = client.get_database("poc", write_concern=WriteConcern(w="majority"))
    docs = defaultdict(lambda: {"sections": []})
    for sid, code, wf, text, _ in d.all_passages(total):
        doc = docs[d.guidance_id(sid)]
        doc.update({"_id": d.guidance_id(sid), "client_code": code, "workflow": wf, "version": 1, "status": "approved"})
        doc["sections"].append({"section_id": sid, "text": text, "content_hash": d.content_hash(text)})
    db.guidance.bulk_write([ReplaceOne({"_id": k}, v, upsert=True) for k, v in docs.items()])
    db.guidance.create_index("sections.section_id")
    db.work_packages.create_index("request_id", unique=True)
    db.drafts.create_index("request_id")
    return {"guidance_documents": db.guidance.count_documents({})}


def load_neo4j(total: int) -> dict:
    driver = GraphDatabase.driver(os.environ["NEO4J_URL"], auth=("neo4j", os.environ["NEO4J_PASSWORD"]))
    with driver.session() as s:
        for stmt in [
            "CREATE CONSTRAINT client_code IF NOT EXISTS FOR (n:Client) REQUIRE n.code IS UNIQUE",
            "CREATE CONSTRAINT workflow_id IF NOT EXISTS FOR (n:WorkflowType) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT field_name IF NOT EXISTS FOR (n:RequiredField) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT role_code IF NOT EXISTS FOR (n:Role) REQUIRE n.code IS UNIQUE",
            "CREATE CONSTRAINT staff_handle IF NOT EXISTS FOR (n:Staff) REQUIRE n.handle IS UNIQUE",
            "CREATE CONSTRAINT section_id IF NOT EXISTS FOR (n:GuidanceSection) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT request_id IF NOT EXISTS FOR (n:Request) REQUIRE n.id IS UNIQUE",
        ]:
            s.run(stmt)
        rules = [{"client": c, "workflow": w, "id": f"{c}/{w}", "fields": f} for (c, w), f in d.REQUIRED.items()]
        s.run("""UNWIND $rules AS r
                 MERGE (c:Client {code: r.client})
                 MERGE (w:WorkflowType {id: r.id}) SET w.name = r.workflow
                 MERGE (c)-[:USES]->(w)
                 WITH r, w UNWIND r.fields AS f
                 MERGE (rf:RequiredField {name: f})
                 MERGE (w)-[:REQUIRES]->(rf)""", rules=rules)
        perms = [{"role": role, "id": f"{c}/{w}"} for role, wfs in d.MAY_PERFORM.items()
                 for _, c in d.CLIENTS for w in wfs]
        s.run("""UNWIND $perms AS p
                 MERGE (r:Role {code: p.role})
                 WITH r, p MATCH (w:WorkflowType {id: p.id})
                 MERGE (r)-[:MAY_PERFORM]->(w)""", perms=perms)
        staff = [{"handle": h, "role": role, "client": c} for h, role, cs in d.USERS for c in cs]
        s.run("""UNWIND $staff AS st
                 MERGE (s:Staff {handle: st.handle}) SET s.role = st.role
                 WITH s, st MATCH (c:Client {code: st.client})
                 MERGE (s)-[:ASSIGNED_TO]->(c)""", staff=staff)
        sections = [{"id": sid, "version": 1, "wf": f"{c}/{w}"} for sid, c, w, _, _ in d.all_passages(total)]
        for i in range(0, len(sections), 2000):
            s.run("""UNWIND $rows AS row
                     MERGE (g:GuidanceSection {id: row.id}) SET g.version = row.version
                     WITH g, row MATCH (w:WorkflowType {id: row.wf})
                     MERGE (g)-[:APPLIES_TO]->(w)""", rows=sections[i:i + 2000])
        refs = [{"src": sid, "dst": dst} for sid, _, _, _, dsts in d.HANDWRITTEN for dst in dsts]
        s.run("""UNWIND $refs AS r
                 MATCH (a:GuidanceSection {id: r.src}), (b:GuidanceSection {id: r.dst})
                 MERGE (a)-[:REFERS_TO]->(b)""", refs=refs)
        nodes = s.run("MATCH (n) RETURN count(n) AS n").single()["n"]
    driver.close()
    return {"graph_nodes": nodes}


def main() -> None:
    total = int(os.environ.get("GUIDANCE_TOTAL", "1000"))
    t0 = time.perf_counter()
    stats = {"guidance_total": total}
    stats.update(load_postgres(total))
    stats.update(load_mongo(total))
    stats.update(load_neo4j(total))
    stats["seed_seconds"] = round(time.perf_counter() - t0, 2)
    print("SEED " + json.dumps(stats), flush=True)


if __name__ == "__main__":
    main()

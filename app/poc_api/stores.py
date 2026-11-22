"""Connections to the four stores, opened once per process."""
import os
from contextlib import contextmanager
from functools import lru_cache

import redis
from neo4j import GraphDatabase
from pgvector.psycopg import register_vector
from psycopg_pool import ConnectionPool
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern


def _configure(conn):
    register_vector(conn)


@lru_cache
def pg_pool() -> ConnectionPool:
    # The API logs in as app_rw only: not the table owner and unable to bypass row-level security.
    return ConnectionPool(os.environ["PG_DSN"], min_size=2, max_size=10, configure=_configure, open=True)


@contextmanager
def user_tx(user_id: str):
    """A PostgreSQL transaction on behalf of one signed-in staff member.

    set_config(..., true) scopes app.user_id to this transaction, so the setting ends at commit
    or rollback and never reaches the next borrower of the pooled connection.
    """
    with pg_pool().connection() as conn:
        with conn.transaction():
            conn.execute("SELECT set_config('app.user_id', %s, true)", [user_id])
            yield conn


@lru_cache
def mongo_db():
    client = MongoClient(os.environ["MONGO_URL"])
    return client.get_database("poc", write_concern=WriteConcern(w="majority"))


@lru_cache
def neo4j_driver():
    return GraphDatabase.driver(os.environ["NEO4J_URL"], auth=("neo4j", os.environ["NEO4J_PASSWORD"]))


@lru_cache
def redis_client() -> redis.Redis:
    return redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


@lru_cache
def embedder():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

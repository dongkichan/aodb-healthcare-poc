"""The gateway checks: token, session, rate limit, role, and assigned client.

Sign-in is simulated. /dev/token issues a signed token for a synthetic handle, standing in for
the organization's identity provider (OpenID Connect in the proposed design). The role and the
assigned clients always come from PostgreSQL, never from the token.
"""
import json
import os
import time
import uuid
from dataclasses import dataclass

import jwt
from fastapi import Header, HTTPException

from .stores import redis_client, user_tx

SESSION_SECONDS = 3600
RATE_LIMIT_PER_MINUTE = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "1200"))


@dataclass(frozen=True)
class User:
    user_id: str
    handle: str
    role: str
    clients: tuple  # client codes, e.g. ("CL-01", "CL-02")


def issue_token(handle: str) -> dict:
    with user_tx("") as conn:
        row = conn.execute("SELECT user_id_for_handle(%s)", [handle]).fetchone()
    if not row or row[0] is None:
        raise HTTPException(404, "unknown handle")
    user_id = str(row[0])
    with user_tx(user_id) as conn:
        role, codes = conn.execute("SELECT app_role(), app_user_client_codes()").fetchone()
    jti = uuid.uuid4().hex
    session = {"user_id": user_id, "handle": handle, "role": role, "clients": codes}
    redis_client().set(f"session:{jti}", json.dumps(session), ex=SESSION_SECONDS)
    token = jwt.encode({"sub": user_id, "jti": jti, "exp": int(time.time()) + SESSION_SECONDS},
                       os.environ["JWT_SECRET"], algorithm="HS256")
    return {"access_token": token, "token_type": "bearer", **session}


def current_user(authorization: str = Header(default="")) -> User:
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "sign in required")
    try:
        claims = jwt.decode(authorization[7:], os.environ["JWT_SECRET"], algorithms=["HS256"])
    except jwt.PyJWTError:
        raise HTTPException(401, "invalid or expired token")
    r = redis_client()
    raw = r.get(f"session:{claims['jti']}")
    if raw is None:
        # Sessions live only in Redis; if Redis loses them, staff sign in again.
        raise HTTPException(401, "session expired; sign in again")
    s = json.loads(raw)
    key = f"rate:{s['user_id']}:{int(time.time() // 60)}"
    calls = r.incr(key)
    if calls == 1:
        r.expire(key, 60)
    if calls > RATE_LIMIT_PER_MINUTE:
        raise HTTPException(429, "rate limit reached")
    return User(s["user_id"], s["handle"], s["role"], tuple(s["clients"]))


def require_role(user: User, *roles: str) -> None:
    if user.role not in roles:
        raise HTTPException(403, f"role {user.role} may not use this function")


def require_client(user: User, client_code: str) -> None:
    if client_code not in user.clients:
        raise HTTPException(403, f"client {client_code} is not assigned to {user.handle}")

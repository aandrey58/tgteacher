"""MCP access control: local bearer credentials or external OAuth introspection."""
import hmac
import os
import time
from urllib.parse import urlsplit

import httpx
from mcp.server.auth.provider import AccessToken
from mcp.server.auth.middleware.auth_context import get_access_token

READ = "content:read"
EDIT = "content:edit"
PUBLISH = "content:publish"
LEARNER_READ = "learner:read"
LEARNER_EDIT = "learner:edit"


class StaticTokenVerifier:
    def __init__(self, reader, editor, publisher, resource="http://127.0.0.1:8000/mcp"):
        self.resource = resource
        tokens = [reader, editor, publisher]
        if any(not t or len(t) < 32 or not t.isascii() or t.startswith("tgtl_") for t in tokens) or len(set(tokens)) != 3:
            raise ValueError("Configure three distinct ASCII bearer tokens of at least 32 characters")
        self.tokens = [
            (reader, "reader", [READ]),
            (editor, "editor", [READ, EDIT]),
            (publisher, "publisher", [READ, EDIT, PUBLISH]),
        ]

    async def verify_token(self, token: str):
        if not token.isascii():
            return None
        for secret, client_id, scopes in self.tokens:
            if hmac.compare_digest(token, secret):
                return AccessToken(token=token, client_id=client_id, scopes=scopes, resource=self.resource)
        return None


class IntrospectionTokenVerifier:
    """RFC 7662 resource server; requires an independently operated OAuth server."""
    def __init__(self, url, client_id, secret, resource):
        if urlsplit(url).scheme != "https" or not client_id or not secret:
            raise ValueError("HTTPS introspection URL and service credentials required")
        self.url, self.client_id, self.secret, self.resource = url, client_id, secret, resource

    async def verify_token(self, token: str):
        try:
            async with httpx.AsyncClient(timeout=5, follow_redirects=False) as client:
                response = await client.post(self.url, data={"token": token, "token_type_hint": "access_token"},
                                             auth=(self.client_id, self.secret))
                response.raise_for_status()
                data = response.json()
            audience = data.get("aud", [])
            audience = [audience] if isinstance(audience, str) else audience
            if not isinstance(audience, list) or not all(isinstance(a,str) for a in audience):
                return None
            exp = data.get("exp")
            if data.get("active") is not True or self.resource not in audience:
                return None
            if type(exp) is not int or exp <= time.time():
                return None
            raw_scope = data.get("scope", "")
            if not isinstance(raw_scope,str):
                return None
            scopes = [s for s in raw_scope.split() if s in (READ, EDIT, PUBLISH)]
            if READ not in scopes:
                return None
            return AccessToken(token=token, client_id=str(data.get("client_id") or "oauth"),
                               scopes=scopes, expires_at=exp, resource=self.resource)
        except (httpx.HTTPError, ValueError, TypeError, AttributeError):
            return None


def verifier_from_env(resource):
    mode = os.getenv("TGTEACHER_MCP_AUTH_MODE", "static")
    if mode == "static":
        return StaticTokenVerifier(*(os.getenv("TGTEACHER_MCP_" + key + "_TOKEN", "")
                                     for key in ("READ", "EDIT", "PUBLISH")), resource=resource)
    if mode == "oauth":
        return IntrospectionTokenVerifier(os.environ["TGTEACHER_MCP_INTROSPECTION_URL"],
                                          os.environ["TGTEACHER_MCP_CLIENT_ID"],
                                          os.environ["TGTEACHER_MCP_CLIENT_SECRET"], resource)
    raise ValueError("Unknown MCP authentication mode")


def require_scope(scope):
    token = get_access_token()
    if not token or scope not in token.scopes:
        raise PermissionError("Insufficient permission: " + scope)


class LearnerTokenVerifier:
    """Separate Telegram-linked credentials from the existing content roles."""
    def __init__(self, content_verifier, resource):
        self.content_verifier = content_verifier
        self.resource = resource

    async def verify_token(self, token: str):
        if not token.startswith("tgtl_"):
            return await self.content_verifier.verify_token(token)
        from tgteacher_bot.db.srs_repo import verify_token
        try:
            row = await verify_token(token)
        except Exception:
            return None
        if not row:
            return None
        return AccessToken(token=token,client_id=f"learner:{row['user_id']}",
                           scopes=[LEARNER_READ,LEARNER_EDIT],expires_at=int(row["expires_at"].timestamp()),resource=self.resource)


def require_learner(write=False):
    require_scope(LEARNER_EDIT if write else LEARNER_READ)
    token = get_access_token()
    prefix, sep, uid = token.client_id.partition(":")
    if prefix != "learner" or not sep or not uid.isascii() or not uid.isdecimal():
        raise PermissionError("Learner identity unavailable")
    return int(uid)

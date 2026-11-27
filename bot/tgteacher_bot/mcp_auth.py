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


def _https_url(value, *, query_allowed=False):
    if not isinstance(value, str) or not value or any(ord(c) <= 32 or ord(c) == 127 for c in value):
        raise ValueError("Invalid OAuth URL")
    parsed = urlsplit(value)
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.fragment or (parsed.query and not query_allowed)):
        raise ValueError("OAuth requires an unambiguous HTTPS URL")
    _ = parsed.port
    return value


def _identity(value):
    return isinstance(value, str) and 0 < len(value) <= 512 and all(c.isprintable() and not c.isspace() for c in value)


class IntrospectionTokenVerifier:
    """Strict content-only RFC 7662 contract for an independently operated issuer."""
    def __init__(self, url, client_id, secret, resource, issuer):
        self.url = _https_url(url, query_allowed=True)
        self.resource = _https_url(resource)
        self.issuer = _https_url(issuer)
        if not _identity(client_id) or not isinstance(secret, str) or not secret:
            raise ValueError("Introspection service credentials required")
        self.client_id, self.secret = client_id, secret

    async def verify_token(self, token: str):
        if not isinstance(token, str) or not 1 <= len(token) <= 8192 or not token.isascii() or any(not 33 <= ord(c) <= 126 for c in token) or token.startswith("tgtl_"):
            return None
        try:
            async with httpx.AsyncClient(timeout=5, follow_redirects=False, trust_env=False) as client:
                response = await client.post(self.url, data={"token": token, "token_type_hint": "access_token"},
                                             auth=(self.client_id, self.secret))
                response.raise_for_status()
                if len(response.content) > 65536:
                    return None
                data = response.json()
            if not isinstance(data, dict) or data.get("active") is not True:
                return None
            if data.get("iss") != self.issuer or not _identity(data.get("sub")) or not _identity(data.get("client_id")):
                return None
            audience = data.get("aud", [])
            audience = [audience] if isinstance(audience, str) else audience
            if not isinstance(audience, list) or not all(isinstance(a, str) for a in audience) or self.resource not in audience:
                return None
            now = time.time()
            exp, nbf = data.get("exp"), data.get("nbf")
            if type(exp) is not int or exp <= now or exp > 253402300799:
                return None
            if "nbf" in data and (type(nbf) is not int or nbf > now or nbf >= exp):
                return None
            if "token_type" in data and (not isinstance(data["token_type"], str) or data["token_type"].lower() != "bearer"):
                return None
            raw_scope = data.get("scope", "")
            if not isinstance(raw_scope, str):
                return None
            granted = set(raw_scope.split())
            # Learner scopes and identity claims must never cross the content boundary.
            if granted & {LEARNER_READ, LEARNER_EDIT} or READ not in granted or (PUBLISH in granted and EDIT not in granted):
                return None
            scopes = [s for s in (READ, EDIT, PUBLISH) if s in granted]
            return AccessToken(token=token, client_id="oauth:" + data["client_id"],
                               scopes=scopes, expires_at=exp, resource=self.resource)
        except (httpx.HTTPError, ValueError, TypeError, AttributeError, OverflowError):
            return None


def verifier_from_env(resource):
    mode = os.getenv("TGTEACHER_MCP_AUTH_MODE", "static")
    if mode == "static":
        return StaticTokenVerifier(*(os.getenv("TGTEACHER_MCP_" + key + "_TOKEN", "")
                                     for key in ("READ", "EDIT", "PUBLISH")), resource=resource)
    if mode == "oauth":
        return IntrospectionTokenVerifier(os.environ["TGTEACHER_MCP_INTROSPECTION_URL"],
                                          os.environ["TGTEACHER_MCP_CLIENT_ID"],
                                          os.environ["TGTEACHER_MCP_CLIENT_SECRET"], resource,
                                          os.environ["TGTEACHER_MCP_ISSUER_URL"])
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
        if not isinstance(token, str):
            return None
        if not token.startswith("tgtl_"):
            return await self.content_verifier.verify_token(token)
        from tgteacher_bot.db.srs_repo import verify_token
        try:
            row = await verify_token(token)
        except Exception:
            return None
        if not row or type(row["user_id"]) is not int or row["user_id"] <= 0 or row["expires_at"].timestamp() <= time.time():
            return None
        return AccessToken(token=token,client_id=f"learner:{row['user_id']}",
                           scopes=[LEARNER_READ,LEARNER_EDIT],expires_at=int(row["expires_at"].timestamp()),resource=self.resource)


def require_learner(write=False):
    require_scope(LEARNER_EDIT if write else LEARNER_READ)
    token = get_access_token()
    prefix, sep, uid = token.client_id.partition(":")
    if (not token.token.startswith("tgtl_") or prefix != "learner" or not sep or not uid.isascii()
            or not uid.isdecimal() or len(uid) > 19 or not 0 < int(uid) <= 9223372036854775807):
        raise PermissionError("Learner identity unavailable")
    return int(uid)

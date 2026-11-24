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


class StaticTokenVerifier:
    def __init__(self, reader, editor, publisher, resource="http://127.0.0.1:8000/mcp"):
        self.resource = resource
        tokens = [reader, editor, publisher]
        if any(not t or len(t) < 32 or not t.isascii() for t in tokens) or len(set(tokens)) != 3:
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
            exp = data.get("exp")
            if data.get("active") is not True or self.resource not in audience:
                return None
            if not isinstance(exp, int) or exp <= time.time():
                return None
            scopes = data.get("scope", "").split()
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

import asyncio
import os
import sys
import unittest
import uuid
import time
import httpx
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))

from starlette.testclient import TestClient
from tgteacher_bot.mcp_auth import StaticTokenVerifier, IntrospectionTokenVerifier, READ, EDIT, PUBLISH
from tgteacher_bot.mcp_server import build_server
from tgteacher_bot.db import drafts_repo as drafts

READER, EDITOR, PUBLISHER = ("r" * 32, "e" * 32, "p" * 32)
SOURCE = '#FAMILY "MCP travel"\n#WORD ticket\n#TRANSLATION билет\n'
MEDIA = SOURCE + '#STAGE5\n#TASK\n#IMAGE ticket.jpg\n#ANSWER ticket\n'


class AuthTests(unittest.IsolatedAsyncioTestCase):
    async def test_distinct_roles(self):
        verifier = StaticTokenVerifier(READER, EDITOR, PUBLISHER)
        self.assertEqual((await verifier.verify_token(READER)).scopes, [READ])
        self.assertEqual((await verifier.verify_token(EDITOR)).scopes, [READ, EDIT])
        self.assertEqual((await verifier.verify_token(PUBLISHER)).scopes, [READ, EDIT, PUBLISH])
        self.assertIsNone(await verifier.verify_token("bad"))
        self.assertIsNone(await verifier.verify_token("невалидный"))

    def test_invalid_configuration(self):
        with self.assertRaises(ValueError):
            StaticTokenVerifier(READER, READER, PUBLISHER)
        with self.assertRaises(ValueError):
            StaticTokenVerifier("", EDITOR, PUBLISHER)


    async def test_external_oauth_audience_expiry_and_scopes(self):
        resource = "https://teacher.example/mcp"
        verifier = IntrospectionTokenVerifier("https://auth.example/introspect", "client", "secret", resource)
        valid = {"active": True, "aud": [resource], "exp": int(time.time()) + 600,
                 "scope": READ + " " + EDIT, "client_id": "test"}
        cases = [(valid, True), ({**valid, "aud": ["https://other.example/mcp"]}, False),
                 ({**valid, "exp": 1}, False), ({**valid, "active": False}, False),
                 ({**valid, "scope": EDIT}, False)]
        for data, allowed in cases:
            with self.subTest(data=data):
                response = httpx.Response(200, json=data, request=httpx.Request("POST", verifier.url))
                with patch("httpx.AsyncClient.post", new=AsyncMock(return_value=response)):
                    token = await verifier.verify_token("opaque-token")
                self.assertEqual(token is not None, allowed)
                if token:
                    self.assertEqual(token.scopes, [READ, EDIT])
                    self.assertEqual(str(token.resource), resource)


class HTTPTests(unittest.TestCase):
    def setUp(self):
        server = build_server(StaticTokenVerifier(READER, EDITOR, PUBLISHER))
        self.client = TestClient(server.streamable_http_app(), base_url="http://127.0.0.1:8000")
        self.client.__enter__()
        self.addCleanup(self.client.__exit__, None, None, None)

    def rpc(self, method, params=None, token=READER, origin=None):
        headers = {"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-06-18"}
        if token:
            headers["Authorization"] = "Bearer " + token
        if origin:
            headers["Origin"] = origin
        return self.client.post("/mcp", headers=headers, json={
            "jsonrpc": "2.0", "id": 1, "method": method, "params": params or {},
        })

    def call(self, name, args, token):
        r = self.rpc("tools/call", {"name": name, "arguments": args}, token)
        self.assertEqual(r.status_code, 200, r.text)
        return r.json()["result"]

    def test_auth_and_discovery(self):
        self.assertEqual(self.rpc("tools/list", token=None).status_code, 401)
        self.assertEqual(self.rpc("tools/list", token="wrong").status_code, 401)
        result = self.rpc("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                        "clientInfo": {"name": "test", "version": "1"}})
        self.assertEqual(result.status_code, 200, result.text)
        result = self.rpc("tools/list").json()["result"]
        names = {tool["name"] for tool in result["tools"]}
        self.assertEqual(len(names), 10)
        self.assertIn("publish_draft", names)
        self.assertIn("search_words", names)
        template = self.call("get_content_format", {}, READER)
        self.assertFalse(template.get("isError", False), template)
        self.assertIn("#FAMILY", str(template))

    def test_permissions_and_confirmation(self):
        args = {"draft_id": str(uuid.uuid4()), "expected_revision": 1, "confirm": True}
        with patch.object(drafts, "publish_draft", new=AsyncMock(return_value={"status": "published"})) as publish:
            self.assertTrue(self.call("publish_draft", args, EDITOR)["isError"])
            self.assertTrue(self.call("publish_draft", {**args, "confirm": False}, PUBLISHER)["isError"])
            publish.assert_not_awaited()
            self.assertFalse(self.call("publish_draft", args, PUBLISHER).get("isError", False))
            publish.assert_awaited_once()
        with patch.object(drafts, "create_draft", new=AsyncMock(return_value={"status": "draft"})) as create:
            args = {"draft_id": str(uuid.uuid4()), "family_txt": SOURCE}
            self.assertTrue(self.call("create_draft", args, READER)["isError"])
            create.assert_not_awaited()
            self.assertFalse(self.call("create_draft", args, EDITOR).get("isError", False))
            create.assert_awaited_once()

    @unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"), "requires isolated PostgreSQL")
    def test_http_publish_and_search_real_database(self):
        import asyncpg
        from tgteacher_bot.db import user_repo, families_repo
        from tgteacher_bot import mcp_server
        dsn = os.environ["TGTEACHER_TEST_DSN"]
        schema = "test_" + uuid.uuid4().hex

        async def setup():
            admin = await asyncpg.connect(dsn)
            try:
                await admin.execute(f'CREATE SCHEMA "{schema}"')
                pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4,
                                                  server_settings={"search_path": schema})
                return admin, pool
            except BaseException:
                await admin.close()
                raise

        admin, pool = self.client.portal.call(setup)

        async def cleanup():
            await pool.close()
            try:
                await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
            finally:
                await admin.close()

        self.addCleanup(self.client.portal.call, cleanup)
        for module in (user_repo, families_repo, drafts, mcp_server):
            p = patch.object(module, "get_pool", new=AsyncMock(return_value=pool))
            p.start()
            self.addCleanup(p.stop)
        self.client.portal.call(user_repo.init_db_pg)
        ident = str(uuid.uuid4())
        def invoke(name, args, token):
            result = self.call(name, args, token)
            self.assertFalse(result.get("isError", False), result)
            return result["structuredContent"]

        draft = invoke("create_draft", {"draft_id": ident, "family_txt": SOURCE}, EDITOR)
        self.assertEqual(draft["status"], "draft")
        self.assertEqual(invoke("list_families", {}, READER)["items"], [])
        self.assertTrue(invoke("validate_draft", {"draft_id": ident}, READER)["publishable"])
        published = invoke("publish_draft", {"draft_id": ident, "expected_revision": 1, "confirm": True}, PUBLISHER)
        words = invoke("search_words", {"query": "ticket"}, READER)["items"]
        self.assertEqual(words[0]["family_id"], published["family_id"])
        family = invoke("get_family", {"family_id": published["family_id"]}, READER)
        self.assertEqual(family["words"][0]["word"], "ticket")
        self.assertEqual(len(invoke("list_families", {}, READER)["items"]), 1)

    def test_foreign_origin_rejected(self):
        self.assertNotEqual(self.rpc("tools/list", origin="https://evil.example").status_code, 200)


class PreviewTests(unittest.TestCase):
    def test_validation(self):
        self.assertTrue(drafts.preview(SOURCE)["publishable"])
        self.assertFalse(drafts.preview("invalid")["valid"])
        self.assertFalse(drafts.preview(MEDIA)["publishable"])

    def test_size_and_markup(self):
        with self.assertRaises(ValueError):
            drafts.check_source("x" * (drafts.MAX_DRAFT_BYTES + 1))
        self.assertFalse(drafts.preview(SOURCE + "#EXAMPLE <b>bad</b>")["valid"])


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"), "requires isolated PostgreSQL")
class DraftDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncpg
        from tgteacher_bot.db import user_repo, families_repo
        self.schema = "test_" + uuid.uuid4().hex
        dsn = os.environ["TGTEACHER_TEST_DSN"]
        self.admin = await asyncpg.connect(dsn)
        self.addAsyncCleanup(self.admin.close)
        await self.admin.execute(f'CREATE SCHEMA "{self.schema}"')
        self.addAsyncCleanup(self.admin.execute, f'DROP SCHEMA "{self.schema}" CASCADE')
        self.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4, server_settings={"search_path": self.schema})
        self.addAsyncCleanup(self.pool.close)
        for module in (user_repo, families_repo, drafts):
            p = patch.object(module, "get_pool", new=AsyncMock(return_value=self.pool))
            p.start()
            self.addCleanup(p.stop)
        await user_repo.init_db_pg()
        self.id = str(uuid.uuid4())

    async def test_draft_revision_and_publication(self):
        first = await drafts.create_draft(self.id, SOURCE)
        self.assertEqual(first["revision"], 1)
        self.assertEqual(await drafts.create_draft(self.id, SOURCE), first)
        with self.assertRaises(drafts.DraftConflict):
            await drafts.create_draft(self.id, SOURCE + "#HINT other")
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM families"), 0)
        updated = await drafts.update_draft(self.id, 1, SOURCE + "#HINT travel")
        self.assertEqual(updated["revision"], 2)
        with self.assertRaises(drafts.DraftConflict):
            await drafts.publish_draft(self.id, 1)
        self.assertTrue((await drafts.validate_draft(self.id))["publishable"])
        a, b = await asyncio.gather(drafts.publish_draft(self.id, 2), drafts.publish_draft(self.id, 2))
        self.assertEqual(a["family_id"], b["family_id"])
        self.assertEqual(a["status"], "published")
        self.assertEqual(await drafts.get_published_source(a["family_id"]), SOURCE + "#HINT travel")
        with self.assertRaises(drafts.DraftConflict):
            await drafts.update_draft(self.id, 2, SOURCE)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM families"), 1)
            await conn.execute("DELETE FROM families WHERE id=$1", a["family_id"])
        self.assertEqual((await drafts.publish_draft(self.id, 2))["status"], "published_deleted")

    async def test_invalid_and_media_never_publish(self):
        for text in ("invalid", MEDIA):
            ident = str(uuid.uuid4())
            await drafts.create_draft(ident, text)
            with self.assertRaises(ValueError):
                await drafts.publish_draft(ident, 1)
            self.assertEqual((await drafts.get_draft(ident))["status"], "draft")
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM families"), 0)

    async def test_duplicate_and_database_rollback(self):
        import asyncpg
        await drafts.create_draft(self.id, SOURCE)
        async with self.pool.acquire() as conn:
            await conn.execute("ALTER TABLE content_drafts ADD CONSTRAINT fail_publish CHECK (published_at IS NULL)")
        with self.assertRaises(asyncpg.CheckViolationError):
            await drafts.publish_draft(self.id, 1)
        self.assertEqual((await drafts.get_draft(self.id))["status"], "draft")
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM families"), 0)
            await conn.execute("ALTER TABLE content_drafts DROP CONSTRAINT fail_publish")
        await drafts.publish_draft(self.id, 1)
        other = str(uuid.uuid4())
        await drafts.create_draft(other, SOURCE)
        self.assertFalse((await drafts.validate_draft(other))["publishable"])
        with self.assertRaises(ValueError):
            await drafts.publish_draft(other, 1)

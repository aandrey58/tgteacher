import asyncio
import re
import json
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
        self.assertEqual(len(names), 43)
        roadmap = (ROOT / "docs" / "MCP_V1_ROADMAP.md").read_text(encoding="utf-8")
        rows = re.findall(r"^\| \d+ \| `(\w+)` \| .*? \| (implemented|planned) \|", roadmap, re.M)
        self.assertEqual(len(rows), 45)
        self.assertEqual(len({name for name, _ in rows}), 45)
        self.assertEqual(names, {name for name, status in rows if status == "implemented"})
        self.assertIn("publish_draft", names)
        self.assertIn("archive_family", names)
        self.assertIn("restore_family", names)
        self.assertIn("search_words", names)
        template = self.call("get_content_format", {}, READER)
        self.assertFalse(template.get("isError", False), template)
        self.assertIn("#FAMILY", str(template))

    def test_content_roles_cannot_read_learner_data(self):
        for token in (READER,EDITOR,PUBLISHER):
            for name in ("get_learning_progress","get_learning_statistics","get_weak_words","get_due_reviews"):
                self.assertTrue(self.call(name,{},token)["isError"])

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
        from tgteacher_bot.db import user_repo, families_repo, content_versions
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
        for module in (user_repo, families_repo, drafts, mcp_server, content_versions):
            p = patch.object(module, "get_pool", new=AsyncMock(return_value=pool))
            p.start()
            self.addCleanup(p.stop)
        self.client.portal.call(user_repo.init_db_pg)
        ident = str(uuid.uuid4())
        def invoke(name, args, token):
            result = self.call(name, args, token)
            self.assertFalse(result.get("isError", False), result)
            if "structuredContent" in result:
                return result["structuredContent"]
            self.assertEqual(result["content"][0]["type"], "text")
            return json.loads(result["content"][0]["text"])

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
        archive_args = {"family_id": published["family_id"], "confirm": True}
        self.assertTrue(self.call("archive_family", archive_args, EDITOR)["isError"])
        self.assertTrue(self.call("archive_family", {**archive_args, "confirm": False}, PUBLISHER)["isError"])
        archived = invoke("archive_family", archive_args, PUBLISHER)
        self.assertTrue(archived["archived"])
        self.assertEqual(invoke("list_families", {}, READER)["items"], [])
        self.assertEqual(len(invoke("list_families", {"status": "archived"}, READER)["items"]), 1)
        self.assertEqual(invoke("search_words", {"query": "ticket"}, READER)["items"], [])
        self.assertEqual(len(invoke("search_words", {"query": "ticket", "include_archived": True}, READER)["items"]), 1)
        self.assertTrue(invoke("get_family", {"family_id": published["family_id"]}, READER)["archived_at"])
        self.assertEqual(invoke("publish_draft", {"draft_id": ident, "expected_revision": 1, "confirm": True}, PUBLISHER)["status"], "archived")
        self.assertTrue(self.call("restore_family", archive_args, EDITOR)["isError"])
        restored = invoke("restore_family", archive_args, PUBLISHER)
        self.assertFalse(restored["archived"])
        self.assertEqual(restored["family_id"], published["family_id"])
        self.assertEqual(len(invoke("list_families", {}, READER)["items"]), 1)
        edit_id = str(uuid.uuid4())
        args = {"family_id": published["family_id"], "draft_id": edit_id, "expected_version": 1}
        self.assertTrue(self.call("create_edit_draft", args, READER)["isError"])
        edit = invoke("create_edit_draft", args, EDITOR)
        self.assertEqual(invoke("list_edit_drafts", {"family_id": published["family_id"]}, READER)["items"][0]["draft_id"], edit_id)
        edit["content"]["words"][0]["translation"] = "проездной билет"
        edit = invoke("update_edit_draft", {"draft_id": edit_id, "expected_revision": 1, "content": edit["content"]}, EDITOR)
        preview = invoke("preview_edit", {"draft_id": edit_id}, READER)
        self.assertTrue(preview["valid"])
        args = {"draft_id": edit_id, "expected_revision": edit["revision"], "confirm": True}
        self.assertTrue(self.call("publish_edit", args, EDITOR)["isError"])
        self.assertTrue(self.call("publish_edit", {**args, "confirm": False}, PUBLISHER)["isError"])
        result = invoke("publish_edit", args, PUBLISHER)
        self.assertEqual(result["content_version"], 2)
        family = invoke("get_family", {"family_id": published["family_id"]}, READER)
        self.assertEqual(family["words"][0]["translation"], "проездной билет")
        self.assertEqual(family["words"][0]["task_id"], edit["content"]["words"][0]["task_id"])
        self.assertEqual(len(invoke("list_family_versions", {"family_id": published["family_id"]}, READER)["items"]), 2)
        restored = invoke("rollback_family", {"family_id": published["family_id"], "version": 1, "expected_version": 2, "confirm": True}, PUBLISHER)
        self.assertEqual(restored["content_version"], 3)
        new_id = str(uuid.uuid4())
        invoke("create_draft", {"draft_id": new_id, "family_txt": SOURCE.replace("MCP travel", "MCP new draft")}, EDITOR)
        args = {"draft_id": new_id, "draft_kind": "new", "stage": 1, "expected_revision": 1}
        added = invoke("add_tasks", {**args, "tasks": [{"word": "hat", "translation": "шляпа"}]}, EDITOR)
        self.assertEqual(added["task_count"], 2)
        self.assertEqual(invoke("delete_draft", {"draft_id": new_id, "expected_revision": 2, "confirm": True}, EDITOR)["status"], "deleted")
        fresh_edit = str(uuid.uuid4())
        invoke("create_edit_draft", {"family_id": published["family_id"], "draft_id": fresh_edit, "expected_version": 3}, EDITOR)
        args = {"draft_id": fresh_edit, "draft_kind": "edit", "stage": 1}
        added = invoke("add_tasks", {**args, "expected_revision": 1, "tasks": [{"word": "hat", "translation": "шляпа"}]}, EDITOR)
        self.assertEqual(added["task_count"], 2)
        invoke("update_task", {**args, "expected_revision": 2, "task_index": 1, "patch": {"translation": "головной убор"}}, EDITOR)
        invoke("reorder_tasks", {**args, "expected_revision": 3, "order": [1, 0]}, EDITOR)
        invoke("archive_task", {**args, "expected_revision": 4, "task_index": 1, "confirm": True}, EDITOR)
        self.assertEqual(invoke("delete_edit_draft", {"draft_id": fresh_edit, "expected_revision": 5, "confirm": True}, EDITOR)["status"], "deleted")
        self.assertEqual(invoke("get_family", {"family_id": published["family_id"]}, READER)["words"][0]["word"], "ticket")



    def test_granular_tools_permissions(self):
        from tgteacher_bot.db import content_operations as ops, content_versions as versions
        base = {"draft_id": str(uuid.uuid4()), "expected_revision": 1}
        common = {**base, "draft_kind": "edit", "stage": 1}
        cases = [
            (drafts, "delete_draft", {**base, "confirm": True}),
            (versions, "delete_edit_draft", {**base, "confirm": True}),
            (ops, "add_tasks", {**common, "tasks": [{"word": "apple", "translation": "яблоко"}]}),
            (ops, "update_task", {**common, "task_index": 0, "patch": {"translation": "билет"}}),
            (ops, "archive_task", {**common, "task_index": 0, "confirm": True}),
            (ops, "reorder_tasks", {**common, "order": [0]}),
        ]
        for module, name, args in cases:
            with self.subTest(tool=name), patch.object(module, name, new=AsyncMock(return_value={"revision": 2})) as call:
                self.assertTrue(self.call(name, args, READER)["isError"])
                if "confirm" in args:
                    self.assertTrue(self.call(name, {**args, "confirm": False}, EDITOR)["isError"])
                call.assert_not_awaited()
                self.assertFalse(self.call(name, args, EDITOR).get("isError", False))
                call.assert_awaited_once()

    def test_tts_permissions_and_approval(self):
        from tgteacher_bot.db import tts_repo as tts
        item = {"provider": "openai", "voice": "alloy", "text": "Hello"}
        for name, args in (
            ("generate_audio", {**item, "max_cost_microusd": 100, "confirm": True}),
            ("batch_generate_audio", {"items": [item], "max_cost_microusd": 100, "confirm": True}),
        ):
            with patch.object(tts, "generate_batch", new=AsyncMock(return_value={"items": []})) as generate:
                self.assertTrue(self.call(name, args, READER)["isError"])
                self.assertTrue(self.call(name, {**args, "confirm": False}, EDITOR)["isError"])
                generate.assert_not_awaited()
                self.assertFalse(self.call(name, args, EDITOR).get("isError", False))
                generate.assert_awaited_once()
        with patch.object(tts, "attach_audio", new=AsyncMock(return_value={"revision": 2})) as attach:
            args = {"draft_kind": "edit", "draft_id": str(uuid.uuid4()), "expected_revision": 1, "task_index": 0, "asset_id": "a"*64}
            self.assertTrue(self.call("attach_audio", args, READER)["isError"])
            attach.assert_not_awaited()
            self.assertFalse(self.call("attach_audio", args, EDITOR).get("isError", False))
            attach.assert_awaited_once()

    def test_image_permissions_and_estimates(self):
        from tgteacher_bot.db import image_repo, media_repo
        item = {"provider": "openrouter", "model": "vendor/model", "prompt": "apple"}
        for name, args in (("generate_image", item), ("batch_generate_images", {"items": [item]})):
            with patch.object(image_repo, "generate_batch", new=AsyncMock(return_value={"items": []})) as generate:
                self.assertFalse(self.call(name, args, READER).get("isError", False))
                self.assertTrue(self.call(name, {**args, "estimate_only": False, "confirm": True}, READER)["isError"])
                self.assertTrue(self.call(name, {**args, "estimate_only": False}, EDITOR)["isError"])
                self.assertFalse(self.call(name, {**args, "estimate_only": False, "confirm": True, "max_cost_microusd": 100}, EDITOR).get("isError", False))
                self.assertEqual(generate.await_count, 2)
        base = {"draft_kind":"edit","draft_id":str(uuid.uuid4()),"expected_revision":1,"task_index":0}
        for name,args in (("attach_image",{**base,"asset_id":"a"*64}),("detach_media",{**base,"kind":"image","confirm":True})):
            with patch.object(media_repo,name,new=AsyncMock(return_value={"revision":2})) as call:
                self.assertTrue(self.call(name,args,READER)["isError"])
                if "confirm" in args:
                    self.assertTrue(self.call(name,{**args,"confirm":False},EDITOR)["isError"])
                call.assert_not_awaited()
                self.assertFalse(self.call(name,args,EDITOR).get("isError",False))
                call.assert_awaited_once()
        with patch.object(media_repo,"list_media",new=AsyncMock(return_value={"items":[]})):
            self.assertFalse(self.call("list_media",{},READER).get("isError",False))
        with patch.object(media_repo,"get_media",new=AsyncMock(return_value={"kind":"image"})):
            self.assertFalse(self.call("get_media",{"kind":"image","asset_id":"a"*64},READER).get("isError",False))

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

    async def test_archive_preserves_every_progress_table_and_task(self):
        from tgteacher_bot.db import families_repo as families, user_repo as users
        from tgteacher_bot.utils.content_validation import DuplicateFamilyError
        await drafts.create_draft(self.id, SOURCE + '#STAGE2\n#TASK\n#SENTENCE Show your ___.\n#ANSWER ticket\n#CHOICES ticket; bag; hat; coat\n')
        published = await drafts.publish_draft(self.id, 1)
        fid = published["family_id"]
        uid = 1234
        await users.start_family_progress_pg(uid, fid)
        await users.set_task_done_pg(uid, fid, 1, 0)
        await users.set_family_stage_done_pg(uid, fid, 1)
        await users.set_family_finished_pg(uid, fid)
        await users.increment_family_completion_count_pg(uid, fid)
        await users.set_stage2_answer_pg(uid, fid, 0, "ticket", True)
        await users.set_last_opened_family_place_pg(uid, fid, 1, 0)
        await users.set_user_stage_state_pg(uid, fid, 1, {"task_idx": 0})
        await users.set_current_family_idx_pg(uid, fid)
        tables = ["user_family_progress", "user_task_progress",
                  "user_last_opened_family_place", "user_stage_state", "user_current_family"]

        async def snapshot():
            async with self.pool.acquire() as conn:
                return {table: [dict(r) for r in await conn.fetch(
                    f"SELECT * FROM {table} WHERE family_idx=$1 ORDER BY 1,2", fid
                )] for table in tables}

        before = await snapshot()
        self.assertTrue(all(before.values()))
        words = await families.get_stage1_words_pg(fid)
        archived = await families.delete_family_from_pg(fid)
        self.assertTrue(archived["archived"])
        self.assertEqual(await families.set_family_archived_pg(fid, True), archived)
        self.assertEqual(await snapshot(), before)
        self.assertEqual(await families.get_all_families_meta_pg(), [])
        self.assertIsNone(await families.get_family_data_pg(fid))
        self.assertTrue((await families.get_family_data_pg(fid, True))["archived_at"])
        self.assertEqual(await users.get_started_families_ids_pg(uid), [fid])
        self.assertEqual(await families.get_stage1_words_pg(fid), words)
        self.assertEqual((await drafts.get_draft(self.id))["status"], "archived")
        self.assertFalse((await drafts.validate_draft(self.id))["publishable"])
        # Scheduled state cleanup must not delete archived lesson state.
        await users.clean_inactive_user_states_pg(-1)
        self.assertEqual(await snapshot(), before)
        with self.assertRaises(DuplicateFamilyError):
            await families.save_family_to_pg({"name": "MCP travel", "words": [{"word": "ticket", "translation": "билет"}]})
        restored = await families.set_family_archived_pg(fid, False)
        self.assertFalse(restored["archived"])
        self.assertEqual(restored["family_id"], fid)
        self.assertEqual(await snapshot(), before)
        self.assertEqual((await families.get_all_families_meta_pg())[0]["id"], fid)
        self.assertEqual((await drafts.get_draft(self.id))["status"], "published")
        self.assertEqual(await families.get_stage1_words_pg(fid), words)
        self.assertEqual(await families.set_family_archived_pg(fid, False), restored)
        with self.assertRaises(ValueError):
            await families.set_family_archived_pg(999999, True)

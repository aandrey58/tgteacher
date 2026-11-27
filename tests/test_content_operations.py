import asyncio
import os
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))

from tgteacher_bot.db import content_operations as ops, content_versions as versions, drafts_repo as drafts, families_repo as families, user_repo as users
from tgteacher_bot.utils.family_parser import parse_family_text

SOURCE = '#FAMILY "Granular travel"\n#WORD ticket\n#TRANSLATION билет\n#WORD hat\n#TRANSLATION шляпа\n'
BAG = {"word": "bag", "translation": "сумка"}


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"), "requires PostgreSQL")
class ContentOperationsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncpg
        dsn = os.environ["TGTEACHER_TEST_DSN"]
        self.schema = "test_" + uuid.uuid4().hex
        admin = await asyncpg.connect(dsn)
        self.addAsyncCleanup(admin.close)
        await admin.execute(f'CREATE SCHEMA "{self.schema}"')
        self.addAsyncCleanup(admin.execute, f'DROP SCHEMA "{self.schema}" CASCADE')
        self.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4, server_settings={"search_path": self.schema})
        self.addAsyncCleanup(self.pool.close)
        for module in (users, families, versions, drafts):
            p = patch.object(module, "get_pool", new=AsyncMock(return_value=self.pool))
            p.start()
            self.addCleanup(p.stop)
        await users.init_db_pg()
        self.id = str(uuid.uuid4())

    async def edit(self):
        fid = await families.save_family_to_pg(parse_family_text(SOURCE))
        return fid, await versions.create_edit_draft(fid, self.id, 1)

    async def test_new_draft_all_operations(self):
        await drafts.create_draft(self.id, SOURCE)
        r = await ops.add_tasks("new", self.id, 1, 1, [BAG], 1)
        self.assertEqual(r["revision"], 2)
        self.assertEqual(r["task_count"], 3)
        await ops.update_task("new", self.id, 2, 1, 1, {"translation": "багажная сумка"})
        await ops.reorder_tasks("new", self.id, 3, 1, [2, 1, 0])
        await ops.archive_task("new", self.id, 4, 1, 2)
        raw = await drafts.get_draft(self.id)
        self.assertEqual(raw["revision"], 5)
        words = parse_family_text(raw["family_txt"])["words"]
        self.assertEqual([w["word"] for w in words], ["hat", "bag"])
        self.assertEqual(words[1]["translation"], "багажная сумка")
        self.assertTrue((await drafts.validate_draft(self.id))["publishable"])
        published = await drafts.publish_draft(self.id, 5)
        self.assertEqual(len((await versions.get_current_content(published["family_id"]))["words"]), 2)
        with self.assertRaises(drafts.DraftConflict):
            await drafts.delete_draft(self.id, 5)

    async def test_edit_operations_preserve_ids_and_progress(self):
        fid, d = await self.edit()
        first, second = [w["task_id"] for w in d["content"]["words"]]
        for pos in (0, 1):
            await users.set_task_done_pg(7, fid, 1, pos)
        await ops.add_tasks("edit", self.id, 1, 1, [BAG])
        await ops.update_task("edit", self.id, 2, 1, 1, {"translation": "головной убор"})
        r = await ops.reorder_tasks("edit", self.id, 3, 1, [1, 2, 0])
        self.assertEqual([t["task_id"] for t in r["task_refs"]], [second, None, first])
        await ops.archive_task("edit", self.id, 4, 1, 2)
        current = await versions.get_current_content(fid)
        self.assertEqual(current["content_version"], 1)
        self.assertEqual([w["task_id"] for w in current["words"]], [first, second])
        self.assertEqual((await users.get_completed_tasks_pg([7], fid, 1))[7], {0, 1})
        report = await versions.preview_edit(self.id)
        self.assertEqual(report["changes"]["words"]["archived_ids"], [first])
        self.assertEqual(report["changes"]["words"]["affected_learners"], 1)
        await versions.publish_edit(self.id, 5)
        current = await versions.get_current_content(fid)
        self.assertEqual(current["words"][0]["task_id"], second)
        self.assertEqual((await users.get_completed_tasks_pg([7], fid, 1))[7], {0})
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM user_task_progress WHERE family_idx=$1", fid), 2)
            self.assertTrue(await conn.fetchval("SELECT archived_at IS NOT NULL FROM stage1_words WHERE id=$1", first))
        await versions.rollback_family(fid, 1, 2)
        self.assertEqual((await users.get_completed_tasks_pg([7], fid, 1))[7], {0, 1})
        with self.assertRaises(versions.VersionConflict):
            await versions.delete_edit_draft(self.id, 5)

    async def test_validation_and_revision_conflicts_are_atomic(self):
        fid, original = await self.edit()
        bad = [
            ops.add_tasks("edit", self.id, 1, 1, [{**BAG, "task_id": 123}]),
            ops.add_tasks("edit", self.id, 1, 1, [BAG], -1),
            ops.update_task("edit", self.id, 1, 1, 0, {"task_id": 123}),
            ops.update_task("edit", self.id, 1, 1, 0, {"translation": ""}),
            ops.update_task("edit", self.id, 1, 1, 9, {"hint": "test"}),
            ops.reorder_tasks("edit", self.id, 1, 1, [0, 0]),
            ops.reorder_tasks("edit", self.id, 1, 1, [0]),
            ops.archive_task("edit", self.id, 1, 9, 0),
            ops.add_tasks("edit", self.id, 0, 1, [BAG]),
        ]
        for operation in bad:
            with self.assertRaises(ValueError):
                await operation
        self.assertEqual(await versions.get_edit_draft(self.id), original)
        await ops.archive_task("edit", self.id, 1, 1, 0)
        with self.assertRaises(ValueError):
            await ops.archive_task("edit", self.id, 2, 1, 0)
        self.assertEqual((await versions.get_edit_draft(self.id))["revision"], 2)

    async def test_concurrent_granular_writes(self):
        await drafts.create_draft(self.id, SOURCE)
        results = await asyncio.gather(
            ops.add_tasks("new", self.id, 1, 1, [BAG]),
            ops.update_task("new", self.id, 1, 1, 0, {"hint": "travel"}),
            return_exceptions=True)
        self.assertEqual(sum(isinstance(r, dict) for r in results), 1)
        self.assertEqual(sum(isinstance(r, drafts.DraftConflict) for r in results), 1)
        self.assertEqual((await drafts.get_draft(self.id))["revision"], 2)
        fid = await families.save_family_to_pg({**parse_family_text(SOURCE), "name": "Other group"})
        await versions.create_edit_draft(fid, self.id, 1)
        results = await asyncio.gather(
            ops.add_tasks("edit", self.id, 1, 1, [BAG]),
            ops.reorder_tasks("edit", self.id, 1, 1, [1, 0]),
            return_exceptions=True)
        self.assertEqual(sum(isinstance(r, dict) for r in results), 1)
        self.assertEqual(sum(isinstance(r, versions.VersionConflict) for r in results), 1)

    async def test_deleted_drafts_cannot_be_reused_or_published(self):
        await drafts.create_draft(self.id, SOURCE)
        with self.assertRaises(drafts.DraftConflict):
            await drafts.delete_draft(self.id, 2)
        deleted = await drafts.delete_draft(self.id, 1)
        self.assertEqual(deleted["status"], "deleted")
        self.assertEqual(await drafts.delete_draft(self.id, 1), deleted)
        self.assertEqual(await drafts.list_drafts(), [])
        self.assertFalse((await drafts.validate_draft(self.id))["publishable"])
        for op in (drafts.create_draft(self.id, SOURCE), drafts.publish_draft(self.id, 2),
                   drafts.update_draft(self.id, 2, SOURCE), ops.add_tasks("new", self.id, 2, 1, [BAG])):
            with self.assertRaises(drafts.DraftConflict):
                await op
        fid, original = await self.edit()
        with self.assertRaises(versions.VersionConflict):
            await versions.delete_edit_draft(self.id, 2)
        deleted = await versions.delete_edit_draft(self.id, 1)
        self.assertEqual(await versions.delete_edit_draft(self.id, 1), deleted)
        self.assertEqual((await versions.get_edit_draft(self.id))["status"], "deleted")
        self.assertEqual((await versions.list_edit_drafts(fid))["items"], [])
        for op in (versions.create_edit_draft(fid, self.id, 1), versions.publish_edit(self.id, 2),
                   versions.update_edit_draft(self.id, 2, original["content"]), versions.preview_edit(self.id),
                   ops.add_tasks("edit", self.id, 2, 1, [BAG])):
            with self.assertRaises(versions.VersionConflict):
                await op
        self.assertEqual((await versions.get_current_content(fid))["content_version"], 1)

    async def test_delete_publish_race(self):
        await drafts.create_draft(self.id, SOURCE)
        results = await asyncio.gather(drafts.delete_draft(self.id, 1), drafts.publish_draft(self.id, 1), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, dict) for r in results), 1)
        self.assertEqual(sum(isinstance(r, drafts.DraftConflict) for r in results), 1)
        status = (await drafts.get_draft(self.id))["status"]
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM families"), int(status == "published"))
        fid = await families.save_family_to_pg({**parse_family_text(SOURCE), "name": "Race edit"})
        await versions.create_edit_draft(fid, self.id, 1)
        await ops.add_tasks("edit", self.id, 1, 1, [BAG])
        results = await asyncio.gather(versions.delete_edit_draft(self.id, 2), versions.publish_edit(self.id, 2), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, dict) for r in results), 1)
        self.assertEqual(sum(isinstance(r, versions.VersionConflict) for r in results), 1)
        status = (await versions.get_edit_draft(self.id))["status"]
        self.assertEqual((await versions.get_current_content(fid))["content_version"], 2 if status == "published" else 1)

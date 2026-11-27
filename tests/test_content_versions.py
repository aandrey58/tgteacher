import asyncio
import copy
import os
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))
from tgteacher_bot.db import content_versions as versions, content_schema, families_repo as families, user_repo as users
from tgteacher_bot.core.learning_session import SESSION, versioned_markup, callback_matches
from telegram import InlineKeyboardButton
from tgteacher_bot.utils.family_renderer import render_family_txt
from tgteacher_bot.utils.family_parser import parse_family_text


def sample():
    return {"name": "Versioned travel", "target": "FREE", "words": [
        {"word": "apple", "translation": "яблоко"}, {"word": "banana", "translation": "банан"},
        {"word": "orange", "translation": "апельсин"}],
        "stage2_tasks": [{"sentence": "Show your ___.", "answer": "ticket", "choices": ["ticket", "hat", "coat", "bag"]}]}


class CallbackTests(unittest.TestCase):
    def test_versioned_keyboard(self):
        token = SESSION.set((12, 3))
        try:
            keyboard = versioned_markup([[InlineKeyboardButton("next", callback_data="stage2_next"),
                                          InlineKeyboardButton("menu", callback_data="main_menu")]])
            data = keyboard.inline_keyboard[0][0].callback_data
            self.assertEqual(data, "stage2_next~12.3")
            self.assertEqual(keyboard.inline_keyboard[0][1].callback_data, "main_menu")
            self.assertTrue(callback_matches(data, 12, 3))
            self.assertFalse(callback_matches(data, 12, 4))
            self.assertFalse(callback_matches(data, 11, 3))
            self.assertFalse(callback_matches("stage2_next", 12, 3))
            self.assertTrue(callback_matches("stage2_next", 12, 1))
        finally:
            SESSION.reset(token)


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"), "requires PostgreSQL")
class VersionTests(unittest.IsolatedAsyncioTestCase):
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
        for module in (users, families, versions):
            p = patch.object(module, "get_pool", new=AsyncMock(return_value=self.pool))
            p.start()
            self.addCleanup(p.stop)
        await users.init_db_pg()
        self.fid = await families.save_family_to_pg(sample())

    async def draft(self):
        meta = await versions.get_current_content(self.fid)
        return await versions.create_edit_draft(self.fid, str(uuid.uuid4()), meta["content_version"])

    async def test_reorder_archive_replace_and_rollback(self):
        fid, uid = self.fid, 42
        await users.start_family_progress_pg(uid, fid)
        await users.set_task_done_pg(uid, fid, 1, 0)
        await users.set_task_done_pg(uid, fid, 1, 1)
        await users.set_stage2_answer_pg(uid, fid, 0, "ticket", True)
        await users.set_family_finished_pg(uid, fid)
        await users.set_user_stage_state_pg(uid, fid, 1, {"tasks_order": [2, 0, 1]})
        await users.set_last_opened_family_place_pg(uid, fid, 1, 1)
        draft = await self.draft()
        original = copy.deepcopy(draft["content"])
        apple, banana, orange = original["words"]
        content = draft["content"]
        content["name"] = "Travel revised"
        content["words"] = [copy.deepcopy(banana), copy.deepcopy(orange), {"word": "pear", "translation": "груша"}]
        content["words"][0]["translation"] = "банан (фрукт)"
        content["stage2_tasks"][0]["answer"] = "bag"
        updated = await versions.update_edit_draft(draft["draft_id"], 1, content)
        impact = await versions.preview_edit(draft["draft_id"])
        self.assertIn(apple["task_id"], impact["changes"]["words"]["archived_ids"])
        self.assertIn(banana["task_id"], impact["changes"]["words"]["preserved_ids"])
        self.assertEqual(len(impact["changes"]["stage2_tasks"]["replaced_ids"]), 1)
        self.assertEqual((await users.get_completed_tasks_pg([uid], fid, 1))[uid], {0, 1})
        results = await asyncio.gather(*(versions.publish_edit(draft["draft_id"], updated["revision"]) for _ in range(2)))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0]["content_version"], 2)
        current = await versions.get_current_content(fid)
        self.assertEqual([w["word"] for w in current["words"]], ["banana", "orange", "pear"])
        self.assertEqual(current["words"][0]["task_id"], banana["task_id"])
        self.assertEqual((await users.get_completed_tasks_pg([uid], fid, 1))[uid], {0})
        self.assertIsNone(await users.get_stage2_answer_pg(uid, fid, 0))
        self.assertEqual((await users.get_all_user_completed_tasks_counts(uid, [fid]))[fid][1], 1)
        self.assertEqual(await users.get_family_completion_count_pg(uid, fid), 1)
        self.assertIsNone(await users.get_user_stage_state_pg(uid, fid, 1))
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM user_task_progress WHERE family_idx=$1", fid), 3)
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM family_version_learner_history WHERE family_id=$1", fid), 1)
            self.assertEqual(await conn.fetchval("SELECT jsonb_array_length(task_progress) FROM family_version_learner_history WHERE family_id=$1 AND version=1", fid), 3)
            self.assertTrue(await conn.fetchval("SELECT archived_at IS NOT NULL FROM stage1_words WHERE id=$1", apple["task_id"]))
        await users.set_task_done_pg(uid, fid, 1, 2)
        restored = await versions.rollback_family(fid, 1, 2)
        self.assertEqual(restored["content_version"], 3)
        self.assertEqual((await versions.get_version(fid, 1))["content"], original)
        self.assertEqual((await versions.get_version(fid, 3))["content"], original)
        self.assertEqual((await users.get_completed_tasks_pg([uid], fid, 1))[uid], {0, 1})
        self.assertEqual(await users.get_stage2_answer_pg(uid, fid, 0), ("ticket", True))
        self.assertEqual(await users.get_family_completion_count_pg(uid, fid), 1)
        # Forward rollback restores the newer pear's learning result too.
        await versions.rollback_family(fid, 2, 3)
        self.assertEqual((await users.get_completed_tasks_pg([uid], fid, 1))[uid], {0, 2})

    async def test_stale_and_foreign_ids(self):
        a, b = await self.draft(), await self.draft()
        a["content"]["words"][0]["task_id"] = 99999999
        await versions.update_edit_draft(a["draft_id"], 1, a["content"])
        with self.assertRaises(ValueError):
            await versions.publish_edit(a["draft_id"], 2)
        b["content"]["description"] = "New description"
        await versions.update_edit_draft(b["draft_id"], 1, b["content"])
        await versions.publish_edit(b["draft_id"], 2)
        with self.assertRaises(versions.VersionConflict):
            await versions.publish_edit(a["draft_id"], 2)
        self.assertEqual(len(await versions.list_versions(self.fid)), 2)

    async def test_transaction_rollback_after_tasks_written(self):
        import asyncpg
        d = await self.draft()
        d["content"]["words"].pop()
        await versions.update_edit_draft(d["draft_id"], 1, d["content"])
        async with self.pool.acquire() as conn:
            await conn.execute("ALTER TABLE family_versions ADD CONSTRAINT fail_version CHECK (version=1)")
        with self.assertRaises(asyncpg.CheckViolationError):
            await versions.publish_edit(d["draft_id"], 2)
        self.assertEqual(len((await versions.get_current_content(self.fid))["words"]), 3)
        self.assertEqual((await versions.get_current_content(self.fid))["content_version"], 1)
        self.assertIsNone((await versions.get_edit_draft(d["draft_id"]))["published_version"])

    async def test_migration_preserves_valid_and_unmapped_legacy_progress(self):
        fid = self.fid
        async with self.pool.acquire() as conn:
            # Reconstruct the pre-3.2 positional schema in this isolated database.
            await conn.execute("""
                DROP VIEW active_user_task_progress;
                ALTER TABLE user_task_progress DROP CONSTRAINT user_task_progress_pkey;
                ALTER TABLE user_task_progress DROP COLUMN task_id;
                ALTER TABLE user_task_progress ADD PRIMARY KEY(user_id,family_idx,stage_num,task_idx);
                DELETE FROM content_schema_migrations WHERE name='stable_progress_v1';
            """)
            await conn.execute("INSERT INTO user_task_progress(user_id,family_idx,stage_num,task_idx,is_done) VALUES(1,$1,1,1,1),(1,$1,1,99,1)", fid)
            await content_schema.migrate_content(conn)
            await content_schema.migrate_content(conn)
            expected = await conn.fetchval("SELECT id FROM stage1_words WHERE family_id=$1 AND word_order=1", fid)
            self.assertEqual(await conn.fetchval("SELECT task_id FROM user_task_progress WHERE family_idx=$1", fid), expected)
            self.assertEqual(await conn.fetchval("SELECT task_idx FROM legacy_unmapped_progress WHERE family_idx=$1", fid), 99)
        self.assertEqual((await users.get_completed_tasks_pg([1], fid, 1))[1], {1})


    async def test_metadata_edit_preserves_position_and_state(self):
        await users.set_user_stage_state_pg(77, self.fid, 1, {"family_idx": self.fid, "tasks_order": [2, 0, 1], "word_idx": 1})
        await users.set_last_opened_family_place_pg(77, self.fid, 1, 1)
        d = await self.draft()
        d["content"]["description"] = "Metadata only"
        await versions.update_edit_draft(d["draft_id"], 1, d["content"])
        await versions.publish_edit(d["draft_id"], 2)
        self.assertEqual((await users.get_user_stage_state_pg(77, self.fid, 1))["tasks_order"], [2,0,1])
        self.assertEqual((await users.get_user_stage_state_pg(77, self.fid, 1))["content_version"], 2)
        self.assertEqual(await users.get_last_opened_family_place_pg(77, self.fid), (1, 1))

    async def test_all_eight_stages_use_stable_ids(self):
        from test_content_management import sample as all_stages
        data = all_stages()
        data["name"] = "Eight stages"
        fid = await families.save_family_to_pg(data)
        for stage in range(1, 9):
            await users.set_task_done_pg(33, fid, stage, 0)
        expected = (await versions.get_current_content(fid))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT stage_num, task_id FROM user_task_progress WHERE family_idx=$1 ORDER BY stage_num", fid)
        self.assertEqual([r["task_id"] for r in rows], [expected[content_schema.stage_key(n)][0]["task_id"] for n in range(1,9)])
        self.assertEqual(len(rows), 8)
        text = render_family_txt(expected)
        parsed = parse_family_text(text)
        self.assertEqual(parsed["name"], data["name"])
        for stage in range(1,9):
            self.assertEqual(len(parsed[content_schema.stage_key(stage)]), 1)

    async def test_invalid_export_delimiters_are_rejected(self):
        d = await self.draft()
        d["content"]["stage2_tasks"][0]["choices"][1] = "hat; cap"
        with self.assertRaises(ValueError):
            await versions.update_edit_draft(d["draft_id"], 1, d["content"])


    async def test_rollback_restores_completed_version_without_new_completion(self):
        uid, fid = 90, self.fid
        for pos in range(3):
            await users.set_task_done_pg(uid, fid, 1, pos)
        await users.set_stage2_answer_pg(uid, fid, 0, "ticket", True)
        await users.set_family_stage_done_pg(uid, fid, 1)
        await users.set_family_stage_done_pg(uid, fid, 2)
        await users.set_family_finished_pg(uid, fid)
        d = await self.draft()
        d["content"]["words"].append({"word": "pear", "translation": "груша"})
        await versions.update_edit_draft(d["draft_id"], 1, d["content"])
        await versions.publish_edit(d["draft_id"], 2)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT finished FROM user_family_progress WHERE user_id=$1 AND family_idx=$2", uid, fid), 0)
        await versions.rollback_family(fid, 1, 2)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT finished FROM user_family_progress WHERE user_id=$1 AND family_idx=$2", uid, fid), 1)
        self.assertEqual(await users.get_family_completion_count_pg(uid, fid), 1)

    async def test_learning_dispatch_blocks_edit_and_rejects_stale_callback(self):
        from telegram import CallbackQuery, Update, User
        from telegram.ext import Application
        from tgteacher_bot.core import learning_session as session
        app = Application.builder().application_class(session.LearningApplication).token("123:TEST").build()
        uid, fid = 321, self.fid
        app._user_data[uid]["current_family_idx"] = fid

        def callback():
            return Update(1, callback_query=CallbackQuery(
                id="query", from_user=User(uid, "Learner", False), chat_instance="chat", data=f"stage1_next~{fid}.1"))

        d = await self.draft()
        d["content"]["description"] = "Revision two"
        await versions.update_edit_draft(d["draft_id"], 1, d["content"])
        entered, release = asyncio.Event(), asyncio.Event()
        seen = []
        async def dispatch(_, update):
            seen.append(update.callback_query.data)
            entered.set()
            await release.wait()
        with patch.object(session, "get_pool", new=AsyncMock(return_value=self.pool)), patch.object(Application, "process_update", new=dispatch), patch.object(CallbackQuery, "answer", new=AsyncMock()) as answer:
            learner = asyncio.create_task(app.process_update(callback()))
            try:
                await asyncio.wait_for(entered.wait(), 3)
                publication = asyncio.create_task(versions.publish_edit(d["draft_id"], 2))
                await asyncio.sleep(0.05)
                self.assertFalse(publication.done())
            finally:
                release.set()
                await learner
            await asyncio.wait_for(publication, 3)
            await app.process_update(callback())
            answer.assert_awaited_once()
            self.assertEqual(seen, ["stage1_next"])


    async def test_busy_writer_rollback_and_retry(self):
        from tgteacher_bot.db import catalog_lock
        draft = await self.draft()
        draft["content"]["description"] = "Retry publication"
        await versions.update_edit_draft(draft["draft_id"],1,draft["content"])
        async with self.pool.acquire() as guard:
            async with guard.transaction():
                await guard.execute("SELECT pg_advisory_xact_lock_shared(847231,1)")
                with patch.object(catalog_lock,"WRITER_WAIT_MS",100):
                    with self.assertRaises(catalog_lock.CatalogBusy):
                        await asyncio.wait_for(versions.publish_edit(draft["draft_id"],2),3)
                self.assertEqual((await versions.get_current_content(self.fid))["content_version"],1)
                self.assertIsNone((await versions.get_edit_draft(draft["draft_id"]))["published_version"])
        self.assertEqual((await versions.publish_edit(draft["draft_id"],2))["content_version"],2)

    async def test_busy_catalog_and_stale_reply_do_not_hold_guard(self):
        from telegram import CallbackQuery, Update, User
        from telegram.ext import Application
        from tgteacher_bot.core import learning_session as session
        app = Application.builder().application_class(session.LearningApplication).token("123:TEST").build()
        uid,fid = 321,self.fid
        app._user_data[uid]["current_family_idx"] = fid
        def callback(version):
            return Update(1,callback_query=CallbackQuery(id="query",from_user=User(uid,"Learner",False),
                          chat_instance="chat",data=f"stage1_next~{fid}.{version}"))
        entered,release = asyncio.Event(),asyncio.Event()
        async def answer(*args,**kwargs):
            entered.set()
            await release.wait()
        with patch.object(session,"get_pool",new=AsyncMock(return_value=self.pool)), patch.object(Application,"process_update",new=AsyncMock()) as dispatch, patch.object(CallbackQuery,"answer",new=AsyncMock(side_effect=answer)):
            async with self.pool.acquire() as writer:
                async with writer.transaction():
                    await writer.execute("SELECT pg_advisory_xact_lock(847231,1)")
                    task = asyncio.create_task(app.process_update(callback(1)))
                    try:
                        await asyncio.wait_for(entered.wait(),3)
                        dispatch.assert_not_awaited()
                    finally:
                        release.set()
                        await task
            entered.clear()
            release.clear()
            task = asyncio.create_task(app.process_update(callback(99)))
            try:
                await asyncio.wait_for(entered.wait(),3)
                async with self.pool.acquire() as writer:
                    async with writer.transaction():
                        self.assertTrue(await writer.fetchval("SELECT pg_try_advisory_xact_lock(847231,1)"))
                dispatch.assert_not_awaited()
            finally:
                release.set()
                await task
            valid=callback(1)
            await app.process_update(valid)
            dispatch.assert_awaited_once()
            self.assertEqual(valid.callback_query.data,f"stage1_next~{fid}.1")
            self.assertIsNone(session.SESSION.get())

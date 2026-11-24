import asyncio
import os
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bot"))
from fsrs import Card, Rating
from tgteacher_bot.db import srs_repo as srs, srs_schema, content_versions as versions, families_repo as families, user_repo as users
from tgteacher_bot.mcp_auth import StaticTokenVerifier, LearnerTokenVerifier, LEARNER_READ
from tgteacher_bot.handlers.user import reviews
from types import SimpleNamespace

SOURCE = {"name":"Review travel","target":"FREE","words":[{"word":"ticket","translation":"билет"},{"word":"bag","translation":"сумка"}],
          "stage2_tasks":[{"sentence":"Show your ___.","answer":"ticket","choices":["ticket","hat","bag","coat"]}]}


class SchedulerTests(unittest.TestCase):
    def test_real_fsrs_ratings(self):
        now = datetime.now(timezone.utc)
        card = Card(card_id=1,due=now)
        again,log = srs.SCHEDULER.review_card(card,Rating.Again,review_datetime=now)
        easy,_ = srs.SCHEDULER.review_card(card,Rating.Easy,review_datetime=now)
        self.assertGreater(easy.due,again.due)
        self.assertGreater(again.due,now)
        self.assertEqual(Card.from_json(easy.to_json()).due,easy.due)
        self.assertEqual(int(log.rating),1)


class TelegramTests(unittest.IsolatedAsyncioTestCase):
    async def test_review_private_and_reveal_ui(self):
        message = SimpleNamespace(reply_text=AsyncMock())
        user = SimpleNamespace(id=17)
        context = SimpleNamespace(user_data={"stage5":{"awaiting_input":True}})
        update = SimpleNamespace(effective_chat=SimpleNamespace(id=17,type="private"),effective_user=user,
                                 effective_message=message,callback_query=None)
        nonce = uuid.uuid4().hex
        with patch.object(srs,"next_review",new=AsyncMock(return_value={"nonce":nonce,"word":"ticket","family_name":"Travel"})) as next_card:
            await reviews.start_review(update,context)
            next_card.assert_awaited_once_with(17)
            self.assertNotIn("stage5",context.user_data)
            args,kwargs = message.reply_text.await_args
            self.assertIn("ticket",args[0])
            self.assertNotIn("билет",args[0])
            self.assertEqual(kwargs["reply_markup"].inline_keyboard[0][0].callback_data,"srs_reveal_"+nonce)
        query = SimpleNamespace(data="srs_reveal_"+nonce,from_user=user,answer=AsyncMock(),edit_message_text=AsyncMock())
        update.callback_query=query
        with patch.object(srs,"reveal_review",new=AsyncMock(return_value={"word":"ticket","translation":"билет"})) as reveal:
            await reviews.reveal(update,context)
            reveal.assert_awaited_once_with(17,nonce)
            args,kwargs = query.edit_message_text.await_args
            self.assertIn("билет",args[0])
            self.assertEqual(kwargs["reply_markup"].inline_keyboard[1][1].callback_data,f"srs_rate_{nonce}_4")
        update.effective_chat.type="group"
        with patch.object(srs,"issue_token",new=AsyncMock()) as issue:
            await reviews.issue_token(update,context)
            issue.assert_not_awaited()


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"),"requires PostgreSQL")
class SRSDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncpg
        dsn = os.environ["TGTEACHER_TEST_DSN"]
        schema = "test_" + uuid.uuid4().hex
        admin = await asyncpg.connect(dsn)
        self.addAsyncCleanup(admin.close)
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        self.addAsyncCleanup(admin.execute,f'DROP SCHEMA "{schema}" CASCADE')
        self.pool = await asyncpg.create_pool(dsn,min_size=1,max_size=4,server_settings={"search_path":schema})
        self.addAsyncCleanup(self.pool.close)
        for module in (srs,versions,families,users):
            p = patch.object(module,"get_pool",new=AsyncMock(return_value=self.pool))
            p.start()
            self.addCleanup(p.stop)
        await users.init_db_pg()
        self.fid = await families.save_family_to_pg(SOURCE)
        self.uid = 11
        await users.add_or_update_user_pg(11,"one","One","")
        await users.add_or_update_user_pg(22,"two","Two","")

    async def enroll(self,uid=11,pos=0):
        await users.set_task_done_pg(uid,self.fid,1,pos)

    async def test_enrollment_history_and_isolation(self):
        await self.enroll()
        await self.enroll()
        await self.enroll(22,1)
        await users.set_stage2_answer_pg(11,self.fid,0,"hat",False)
        await users.set_stage2_answer_pg(11,self.fid,0,"ticket",True)
        self.assertEqual([r["word"] for r in (await srs.due_reviews(11))["items"]],["ticket"])
        self.assertEqual([r["word"] for r in (await srs.due_reviews(22))["items"]],["bag"])
        stats = await srs.learning_statistics(11)
        self.assertEqual(stats["lesson_attempts"],2)
        self.assertEqual(stats["correct_lesson_attempts"],1)
        self.assertEqual((await srs.learning_statistics(22))["lesson_attempts"],0)
        self.assertEqual((await srs.learning_progress(11))["items"][0]["completed_tasks"],2)
        self.assertEqual((await srs.learning_progress(22))["items"][0]["completed_tasks"],1)
        self.assertEqual((await srs.weak_words(11))["items"],[])

    async def test_ticket_replay_wrong_user_and_real_fsrs(self):
        await self.enroll()
        t = await srs.next_review(11)
        self.assertNotIn("translation",t)
        for uid in (11,22):
            with self.assertRaises(ValueError):
                await srs.rate_review(uid,t["nonce"],1)
        with self.assertRaises(ValueError):
            await srs.reveal_review(22,t["nonce"])
        self.assertEqual((await srs.reveal_review(11,t["nonce"]))["translation"],"билет")
        results = await asyncio.gather(srs.rate_review(11,t["nonce"],1),srs.rate_review(11,t["nonce"],1),return_exceptions=True)
        self.assertEqual(sum(isinstance(r,dict) for r in results),1)
        self.assertEqual(sum(isinstance(r,ValueError) for r in results),1)
        self.assertEqual((await srs.learning_statistics(11))["review_ratings"],{"1":1})
        self.assertEqual((await srs.weak_words(11))["items"][0]["word"],"ticket")
        self.assertEqual((await srs.due_reviews(11))["items"],[])
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT card,due FROM srs_cards WHERE user_id=11")
            self.assertEqual(Card.from_json(row["card"]).due,row["due"])
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM srs_reviews WHERE user_id=11"),1)

    async def test_version_fence_archive_restore_and_access(self):
        await self.enroll()
        t = await srs.next_review(11)
        await srs.reveal_review(11,t["nonce"])
        eid = str(uuid.uuid4())
        draft = await versions.create_edit_draft(self.fid,eid,1)
        old_id = draft["content"]["words"][0]["task_id"]
        draft["content"]["words"].reverse()
        await versions.update_edit_draft(eid,1,draft["content"])
        await versions.publish_edit(eid,2)
        with self.assertRaises(ValueError):
            await srs.rate_review(11,t["nonce"],3)
        self.assertEqual((await srs.due_reviews(11))["items"][0]["task_id"],old_id)
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE families SET target='VIP' WHERE id=$1",self.fid)
        self.assertEqual((await srs.due_reviews(11))["items"],[])
        async with self.pool.acquire() as conn:
            await conn.execute("UPDATE users SET is_subscribed=true,subscription_until=now()+interval '1 day' WHERE user_id=11")
        self.assertIsNotNone(await srs.next_review(11))
        await families.set_family_archived_pg(self.fid,True)
        self.assertEqual((await srs.due_reviews(11))["items"],[])
        await families.set_family_archived_pg(self.fid,False)
        self.assertEqual((await srs.due_reviews(11))["items"][0]["task_id"],old_id)

    async def test_legacy_backfill_without_invented_reviews(self):
        # Emulate pre-SRS rows, then run migration twice.
        async with self.pool.acquire() as conn:
            await conn.execute("DROP TRIGGER srs_learning_event ON user_task_progress")
            await conn.execute("DELETE FROM content_schema_migrations WHERE name='srs_v1'")
        await self.enroll()
        await users.set_stage2_answer_pg(11,self.fid,0,"hat",False)
        async with self.pool.acquire() as conn:
            await srs_schema.init_srs(conn)
            await srs_schema.init_srs(conn)
            self.assertIsNone(await conn.fetchval("SELECT card FROM srs_cards WHERE user_id=11"))
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM srs_reviews"),0)
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM learner_answer_events WHERE answered_at IS NULL"),1)
        stats = await srs.learning_statistics(11)
        self.assertEqual(stats["legacy_answer_snapshots"],1)
        self.assertEqual(stats["lesson_attempts"],0)
        self.assertEqual(stats["due_cards"],1)

    async def test_credential_rotation_expiry_and_account_deletion(self):
        verifier = LearnerTokenVerifier(StaticTokenVerifier("r"*32,"e"*32,"p"*32),"https://teacher.example/mcp")
        raw = await srs.issue_token(11)
        token = await verifier.verify_token(raw)
        self.assertEqual(token.scopes,[LEARNER_READ])
        self.assertEqual(token.client_id,"learner:11")
        newer = await srs.issue_token(11)
        self.assertIsNone(await verifier.verify_token(raw))
        self.assertIsNotNone(await verifier.verify_token(newer))
        async with self.pool.acquire() as conn:
            self.assertNotEqual(await conn.fetchval("SELECT token_hash FROM learner_tokens WHERE user_id=11"),newer)
            await conn.execute("UPDATE learner_tokens SET expires_at=now()-interval '1 second' WHERE user_id=11")
        self.assertIsNone(await verifier.verify_token(newer))
        raw = await srs.issue_token(11)
        await srs.revoke_token(11)
        self.assertIsNone(await verifier.verify_token(raw))
        raw = await srs.issue_token(11)
        await self.enroll()
        t = await srs.next_review(11)
        await srs.reveal_review(11,t["nonce"])
        await srs.rate_review(11,t["nonce"],4)
        await users.set_stage2_answer_pg(11,self.fid,0,"ticket",True)
        await self.enroll(22,1)
        await users.delete_user_and_progress_pg(11)
        self.assertIsNone(await verifier.verify_token(raw))
        async with self.pool.acquire() as conn:
            for table in ("srs_cards","srs_reviews","srs_review_tickets","learner_answer_events"):
                self.assertEqual(await conn.fetchval(f"SELECT count(*) FROM {table} WHERE user_id=11"),0)
        self.assertEqual((await srs.due_reviews(22))["items"][0]["word"],"bag")

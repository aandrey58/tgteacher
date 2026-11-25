"""Isolated synthetic Telegram events and PostgreSQL Stars lifecycle tests."""
import asyncio
import os
import sys
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace as Obj
from unittest.mock import AsyncMock,patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"bot"))
from telegram.ext import ApplicationHandlerStop
from tgteacher_bot.db import stars_repo as repo,user_repo as users,web_operations as ops
from tgteacher_bot.handlers.user import stars

class HandlerTests(unittest.IsolatedAsyncioTestCase):
    async def test_precheckout_and_disabled_settlement(self):
        q=Obj(id="q1",from_user=Obj(id=11),invoice_payload="vipxtr:"+str(uuid.uuid4()),currency="XTR",total_amount=250,answer=AsyncMock())
        with patch.dict(os.environ,{"TGTEACHER_STARS_ENABLED":"1"}),patch.object(repo,"authorize",new=AsyncMock()) as auth:
            with self.assertRaises(ApplicationHandlerStop):
                await stars.pre_checkout(Obj(pre_checkout_query=q),None)
            auth.assert_awaited_once_with(11,q.invoice_payload,"XTR",250,"q1")
            q.answer.assert_awaited_once_with(ok=True)
        q.answer.reset_mock()
        with patch.dict(os.environ,{"TGTEACHER_STARS_ENABLED":"0"}),patch.object(repo,"authorize",new=AsyncMock()) as auth:
            with self.assertRaises(ApplicationHandlerStop):
                await stars.pre_checkout(Obj(pre_checkout_query=q),None)
            auth.assert_not_awaited()
            self.assertFalse(q.answer.await_args.kwargs["ok"])
            payment=Obj(invoice_payload=q.invoice_payload,currency="XTR",total_amount=250,telegram_payment_charge_id="charge")
            m=Obj(successful_payment=payment,reply_text=AsyncMock())
            update=Obj(message=m,effective_user=Obj(id=11),effective_chat=Obj(type="private",id=11))
            with patch.object(repo,"settle",new=AsyncMock(return_value={"state":"paid"})) as settle:
                with self.assertRaises(ApplicationHandlerStop):
                    await stars.successful_payment(update,None)
                settle.assert_awaited_once_with(11,q.invoice_payload,"XTR",250,"charge")

    async def test_invoice_is_xtr_and_private(self):
        payload="vipxtr:"+str(uuid.uuid4())
        q=Obj(from_user=Obj(id=11),data="stars_buy_3_250",answer=AsyncMock(),edit_message_text=AsyncMock())
        update=Obj(callback_query=q,effective_user=q.from_user,effective_chat=Obj(id=11,type="private"))
        ctx=Obj(bot=Obj(send_invoice=AsyncMock()))
        with patch.dict(os.environ,{"TGTEACHER_STARS_ENABLED":"1"}),patch.object(repo,"prepare",new=AsyncMock(return_value=payload)):
            await stars.choose(update,ctx)
            kw=ctx.bot.send_invoice.await_args.kwargs
            self.assertEqual((kw["currency"],kw["provider_token"],kw["payload"]),("XTR","",payload))
            self.assertEqual(len(kw["prices"]),1)
            self.assertEqual(kw["prices"][0].amount,250)
            update.effective_chat.type="group"
            await stars.choose(update,ctx)
            ctx.bot.send_invoice.assert_awaited_once()

@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"),"requires PostgreSQL")
class DatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncpg
        dsn=os.environ["TGTEACHER_TEST_DSN"]
        schema="test_"+uuid.uuid4().hex
        admin=await asyncpg.connect(dsn)
        self.addAsyncCleanup(admin.close)
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        self.addAsyncCleanup(admin.execute,f'DROP SCHEMA "{schema}" CASCADE')
        self.pool=await asyncpg.create_pool(dsn,min_size=1,max_size=8,server_settings={"search_path":schema})
        self.addAsyncCleanup(self.pool.close)
        for module in (repo,users,ops):
            p=patch.object(module,"get_pool",new=AsyncMock(return_value=self.pool))
            p.start()
            self.addCleanup(p.stop)
        await users.init_db_pg()
        async with self.pool.acquire() as c:
            await c.execute("INSERT INTO users(user_id) VALUES(11),(22)")
        await repo.set_tariff(3,250)

    async def snapshot(self,uid=11):
        async with self.pool.acquire() as c:
            return dict(await c.fetchrow("SELECT is_subscribed,subscription_count,subscription_until FROM users WHERE user_id=$1",uid))

    async def authorized(self,uid=11):
        payload=await repo.prepare(uid,3,250)
        await repo.authorize(uid,payload,"XTR",250,uuid.uuid4().hex)
        return payload

    async def test_wrong_tariff_owner_currency_and_checkout_replay(self):
        with self.assertRaises(repo.StarsConflict):
            await repo.prepare(11,3,1)
        p=await repo.prepare(11,3,250)
        for uid,currency,amount in ((22,"XTR",250),(11,"RUB",250),(11,"XTR",249)):
            with self.assertRaises(repo.StarsConflict):
                await repo.authorize(uid,p,currency,amount,"q1")
        await repo.set_tariff(3,300)
        with self.assertRaises(repo.StarsConflict):
            await repo.authorize(11,p,"XTR",250,"q1")
        await repo.set_tariff(3,250)
        await repo.authorize(11,p,"XTR",250,"q1")
        await repo.authorize(11,p,"XTR",250,"q1")
        with self.assertRaises(repo.StarsConflict):
            await repo.authorize(11,p,"XTR",250,"q2")

    async def test_concurrent_settlement_charge_uniqueness_and_report(self):
        p=await self.authorized()
        before=await self.snapshot()
        result=await asyncio.gather(*(repo.settle(11,p,"XTR",250,"charge-1") for _ in range(8)))
        self.assertTrue(all(r["state"]=="paid" for r in result))
        after=await self.snapshot()
        self.assertEqual(after["subscription_count"],(before["subscription_count"] or 0)+3)
        with self.assertRaises(repo.StarsConflict):
            await repo.settle(11,p,"XTR",250,"charge-2")
        another=await self.authorized()
        with self.assertRaises(Exception):
            await repo.settle(11,another,"XTR",250,"charge-1")
        self.assertEqual(await self.snapshot(),after)
        rows=await ops.reports(kind="payments")
        self.assertEqual(len(rows),1)
        self.assertEqual((rows[0]["currency"],rows[0]["amount_stars"],rows[0]["amount_kopecks"]),("XTR",250,None))
        async with self.pool.acquire() as c:
            self.assertEqual(await c.fetchval("SELECT count(*) FROM payments"),0)

    async def test_refund_before_or_after_credit(self):
        p=await self.authorized()
        before=await self.snapshot()
        await repo.observe_refund(11,p,"XTR",250,"charge-a")
        self.assertEqual((await repo.settle(11,p,"XTR",250,"charge-a"))["state"],"refunded")
        self.assertEqual(await self.snapshot(),before)
        p=await self.authorized()
        await repo.settle(11,p,"XTR",250,"charge-b")
        after=await self.snapshot()
        for _ in range(2):
            await repo.observe_refund(11,p,"XTR",250,"charge-b")
        self.assertEqual(await self.snapshot(),after)
        with self.assertRaises(repo.StarsConflict):
            await repo.observe_refund(22,p,"XTR",250,"charge-b")

    async def test_transaction_rollback_and_retry(self):
        p=await self.authorized()
        before=await self.snapshot()
        async with self.pool.acquire() as c:
            await c.execute("ALTER TABLE stars_orders ADD CONSTRAINT synthetic_fault CHECK(state<>'paid')")
        with self.assertRaises(Exception):
            await repo.settle(11,p,"XTR",250,"charge-fault")
        self.assertEqual(await self.snapshot(),before)
        async with self.pool.acquire() as c:
            await c.execute("ALTER TABLE stars_orders DROP CONSTRAINT synthetic_fault")
        self.assertEqual((await repo.settle(11,p,"XTR",250,"charge-fault"))["state"],"paid")

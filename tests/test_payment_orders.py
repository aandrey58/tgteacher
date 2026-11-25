"""Payment regression: isolated Postgres and synthetic YooKassa SDK responses only."""
import asyncio
import os
import sys
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"bot"))
from tgteacher_bot.db import payment_orders as orders, user_repo as users, web_operations as ops
from tgteacher_bot.services.payments import yookassa_payment as yoo

ENV={"YOOKASSA_SHOP_ID":"ci-shop","YOOKASSA_SECRET_KEY":"ci-only",
     "YOOKASSA_RETURN_URL":"https://example.test/return",
     "TGTEACHER_YOOKASSA_CREATE":"1","TGTEACHER_YOOKASSA_RECEIPT_MODE":"external"}

def remote(row,state="pending",pid=None,**override):
    values={"id":str(pid or uuid.uuid4()),"status":state,"paid":state=="succeeded",
            "amount":SimpleNamespace(value=f"{row['amount']//100}.{row['amount']%100:02d}",currency="RUB"),
            "metadata":{"order_id":str(row["id"]),"user_id":str(row["user_id"]),"months":str(row["months"]),"payment_type":"subscription"},
            "confirmation":SimpleNamespace(confirmation_url="https://example.test/pay")}
    values.update(override)
    return SimpleNamespace(**values)

class PaymentUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_creation_disabled_and_private_callback(self):
        with patch.dict(os.environ,{**ENV,"TGTEACHER_YOOKASSA_CREATE":"0"}):
            service=yoo.YooKassaPayment()
            with patch.object(orders,"claim",new=AsyncMock()) as claim,patch.object(yoo.Payment,"create") as sdk:
                with self.assertRaises(orders.PaymentConflict):
                    await service.create_payment(11,uuid.uuid4())
                claim.assert_not_awaited()
                sdk.assert_not_called()
        user=SimpleNamespace(id=11)
        query=SimpleNamespace(from_user=user,answer=AsyncMock(),edit_message_text=AsyncMock(),data="confirm_payment_"+str(uuid.uuid4()))
        update=SimpleNamespace(callback_query=query,effective_user=user,effective_chat=SimpleNamespace(id=-1,type="group"))
        with patch.object(yoo,"get_yookassa_payment") as service:
            await yoo.create_payment_callback(update,None)
            service.assert_not_called()
        query.answer.assert_awaited_once()
        query.edit_message_text.assert_not_awaited()

    async def test_old_price_callback_cannot_create_payment(self):
        user=SimpleNamespace(id=11)
        query=SimpleNamespace(from_user=user,answer=AsyncMock(),edit_message_text=AsyncMock(),data="confirm_payment_3_300")
        update=SimpleNamespace(callback_query=query,effective_user=user,effective_chat=SimpleNamespace(id=11,type="private"))
        with patch.object(yoo,"get_yookassa_payment") as service:
            await yoo.create_payment_callback(update,None)
            service.assert_not_called()
        query.edit_message_text.assert_awaited_once()

@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"),"requires PostgreSQL")
class PaymentDatabaseTests(unittest.IsolatedAsyncioTestCase):
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
        for module in (orders,users,ops):
            patcher=patch.object(module,"get_pool",new=AsyncMock(return_value=self.pool))
            patcher.start()
            self.addCleanup(patcher.stop)
        patcher=patch.dict(os.environ,ENV)
        patcher.start()
        self.addCleanup(patcher.stop)
        await users.init_db_pg()
        async with self.pool.acquire() as c:
            await c.execute("INSERT INTO users(user_id) VALUES(11),(22)")
            await c.execute("INSERT INTO payment_options(months,amount) VALUES(3,300) ON CONFLICT(months) DO UPDATE SET amount=excluded.amount")
        self.service=yoo.YooKassaPayment()

    async def snapshot(self,uid=11):
        async with self.pool.acquire() as c:
            return dict(await c.fetchrow("SELECT subscription_until,subscription_count,is_subscribed FROM users WHERE user_id=$1",uid))

    async def make_pending(self,uid=11):
        oid=await orders.prepare(uid,3,30000)
        row=await orders.get_order(uid,oid)
        payment=remote(row)
        with patch.object(yoo.Payment,"create",return_value=payment) as sdk:
            result=await self.service.create_payment(uid,oid)
            sdk.assert_called_once()
            args=sdk.call_args.args
            self.assertEqual(args[1],str(oid))
            self.assertEqual(args[0]["amount"],{"value":"300.00","currency":"RUB"})
            self.assertEqual(args[0]["metadata"]["order_id"],str(oid))
            self.assertNotIn("receipt",args[0])
        self.assertEqual(result["state"],"pending")
        return oid,result

    async def test_tariff_and_owner_before_provider(self):
        with self.assertRaises(orders.PaymentConflict):
            await orders.prepare(11,3,100)
        oid=await orders.prepare(11,3,30000)
        async with self.pool.acquire() as c:
            await c.execute("UPDATE payment_options SET amount=400 WHERE months=3")
        with patch.object(yoo.Payment,"create") as sdk,patch.object(yoo.Payment,"find_one") as lookup:
            with self.assertRaises(orders.PaymentConflict):
                await self.service.create_payment(22,oid)
            with self.assertRaises(orders.PaymentConflict):
                await self.service.check_payment_status(22,oid)
            with self.assertRaises(orders.PaymentConflict):
                await self.service.create_payment(11,oid)
            sdk.assert_not_called()
            lookup.assert_not_called()
        self.assertEqual((await orders.get_order(11,oid))["state"],"prepared")

    async def test_concurrent_creation_and_unknown_blocks_new_order(self):
        oid=await orders.prepare(11,3,30000)
        row=await orders.get_order(11,oid)
        payment=remote(row)
        with patch.object(yoo.Payment,"create",return_value=payment) as sdk:
            await asyncio.gather(*(self.service.create_payment(11,oid) for _ in range(6)))
            sdk.assert_called_once()
        self.assertEqual((await orders.get_order(11,oid))["state"],"pending")
        other=await orders.prepare(11,3,30000)
        with patch.object(yoo.Payment,"create") as sdk:
            with self.assertRaises(orders.PaymentConflict):
                await self.service.create_payment(11,other)
            sdk.assert_not_called()
        uid=22
        unknown=await orders.prepare(uid,3,30000)
        with patch.object(yoo.Payment,"create",side_effect=TimeoutError("synthetic timeout")) as sdk:
            with self.assertRaises(TimeoutError):
                await self.service.create_payment(uid,unknown)
            self.assertEqual((await self.service.create_payment(uid,unknown))["state"],"uncertain")
            fresh=await orders.prepare(uid,3,30000)
            with self.assertRaises(orders.PaymentConflict):
                await self.service.create_payment(uid,fresh)
            sdk.assert_called_once()

    async def test_provider_mismatch_and_idempotent_settlement(self):
        oid,row=await self.make_pending()
        initial=await self.snapshot()
        pid=row["provider_id"]
        variants=[
            remote(row,"succeeded",pid,paid=False),
            remote(row,"succeeded",pid,amount=SimpleNamespace(value="299.99",currency="RUB")),
            remote(row,"succeeded",pid,amount=SimpleNamespace(value="300.00",currency="USD")),
            remote(row,"succeeded",pid,metadata={"order_id":str(oid),"user_id":"22","months":"3","payment_type":"subscription"}),
            remote(row,"succeeded",pid,metadata={"order_id":str(oid),"user_id":"11","months":"3","payment_type":"gift"}),
            remote(row,"succeeded",uuid.uuid4()),
        ]
        for wrong in variants:
            with patch.object(yoo.Payment,"find_one",return_value=wrong):
                with self.assertRaises(orders.PaymentConflict):
                    await self.service.check_payment_status(11,oid)
            self.assertEqual(await self.snapshot(),initial)
        paid=remote(row,"succeeded",pid)
        with patch.object(yoo.Payment,"find_one",return_value=paid):
            results=await asyncio.gather(*(self.service.check_payment_status(11,oid) for _ in range(8)))
        self.assertTrue(all(x["state"]=="succeeded" for x in results))
        after=await self.snapshot()
        self.assertTrue(after["is_subscribed"])
        self.assertEqual(after["subscription_count"],(initial["subscription_count"] or 0)+3)
        async with self.pool.acquire() as c:
            self.assertEqual(await c.fetchval("SELECT count(*) FROM payments WHERE payment_id=$1",pid),1)
            self.assertEqual(await c.fetchval("SELECT result_subscription_until FROM payments WHERE payment_id=$1",pid),after["subscription_until"])
        with patch.object(yoo.Payment,"find_one") as sdk:
            self.assertEqual((await self.service.check_payment_status(11,oid))["state"],"succeeded")
            sdk.assert_not_called()
        self.assertEqual(await self.snapshot(),after)

    async def test_settlement_rollback_and_recovery(self):
        oid,row=await self.make_pending()
        initial=await self.snapshot()
        paid=remote(row,"succeeded",row["provider_id"])
        async with self.pool.acquire() as c:
            await c.execute("ALTER TABLE payments ADD CONSTRAINT ci_fault CHECK(status<>'succeeded')")
        with patch.object(yoo.Payment,"find_one",return_value=paid):
            with self.assertRaises(Exception):
                await self.service.check_payment_status(11,oid)
            self.assertEqual(await self.snapshot(),initial)
            self.assertEqual((await orders.get_order(11,oid))["state"],"pending")
            async with self.pool.acquire() as c:
                self.assertEqual(await c.fetchval("SELECT count(*) FROM payments"),0)
                await c.execute("ALTER TABLE payments DROP CONSTRAINT ci_fault")
            self.assertEqual((await self.service.check_payment_status(11,oid))["state"],"succeeded")

    async def test_expired_permanent_and_legacy_helper(self):
        async with self.pool.acquire() as c:
            await c.execute("UPDATE users SET is_subscribed=true,subscription_until=now()-interval '1 day' WHERE user_id=11")
            await c.execute("UPDATE users SET is_subscribed=true,subscription_until=NULL WHERE user_id=22")
        self.assertFalse((await users.get_user_subscription_info_pg(11))["is_subscribed"])
        self.assertTrue((await users.get_user_subscription_info_pg(22))["is_subscribed"])
        await users.extend_user_subscription_pg(33,1)
        self.assertTrue((await users.get_user_subscription_info_pg(33))["is_subscribed"])
        await ops.subscription([22],days=30,operation_id=str(uuid.uuid4()))
        self.assertIsNone((await self.snapshot(22))["subscription_until"])
        oid,row=await self.make_pending(22)
        with patch.object(yoo.Payment,"find_one",return_value=remote(row,"succeeded",row["provider_id"])):
            self.assertEqual((await self.service.check_payment_status(22,oid))["state"],"succeeded")
        self.assertIsNone((await self.snapshot(22))["subscription_until"])

    async def test_legacy_payment_requires_operator_not_automatic_grant(self):
        old=str(uuid.uuid4())
        async with self.pool.acquire() as c:
            await c.execute("INSERT INTO payments(payment_id,user_id,months,amount,status,processed) VALUES($1,11,3,30000,'succeeded',true)",old)
        before=await self.snapshot()
        with patch.object(yoo.Payment,"find_one") as sdk:
            with self.assertRaises(orders.PaymentConflict):
                await self.service.check_payment_status(11,old)
            sdk.assert_not_called()
        self.assertEqual(await self.snapshot(),before)

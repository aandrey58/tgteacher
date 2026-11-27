"""Regression coverage for pool startup, shutdown and real concurrent DB access."""
import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bot"))
from tgteacher_bot.db import pool


def fake_pool():
    obj = MagicMock()
    obj.close = AsyncMock()
    return obj


class PoolLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.addAsyncCleanup(pool.close_pool)

    async def test_parallel_initialization_single_pool(self):
        instance = fake_pool()
        with patch.object(pool.asyncpg, "create_pool", new=AsyncMock(return_value=instance)) as create:
            results = await asyncio.gather(*(pool.get_pool() for _ in range(50)))
            self.assertTrue(all(result is instance for result in results))
            create.assert_awaited_once()
            self.assertEqual(create.await_args.kwargs["max_size"], 15)
            await asyncio.gather(*(pool.close_pool() for _ in range(20)))
            instance.close.assert_awaited_once()

    async def test_cancelled_waiter_does_not_cancel_initialization(self):
        entered, release = asyncio.Event(), asyncio.Event()
        instance = fake_pool()
        async def create():
            entered.set()
            await release.wait()
            return instance
        async def factory_async(**kwargs):
            return await create()
        with patch.object(pool.asyncpg, "create_pool", new=AsyncMock(side_effect=factory_async)) as factory:
            first = asyncio.create_task(pool.get_pool())
            await entered.wait()
            second = asyncio.create_task(pool.get_pool())
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            release.set()
            self.assertIs(await second, instance)
            factory.assert_awaited_once()

    async def test_initialization_failure_can_retry(self):
        instance = fake_pool()
        with patch.object(pool.asyncpg, "create_pool", new=AsyncMock(side_effect=[OSError("offline"), instance])) as create:
            with self.assertRaises(OSError):
                await pool.get_pool()
            self.assertIs(await pool.get_pool(), instance)
            self.assertEqual(create.await_count, 2)

    async def test_shutdown_fences_inflight_init(self):
        entered, release = asyncio.Event(), asyncio.Event()
        instance, replacement = fake_pool(), fake_pool()
        async def create(**kwargs):
            entered.set()
            await release.wait()
            return instance
        with patch.object(pool.asyncpg, "create_pool", new=AsyncMock(side_effect=create)):
            pending = asyncio.create_task(pool.get_pool())
            await entered.wait()
            closing = asyncio.create_task(pool.close_pool())
            await asyncio.sleep(0)
            with self.assertRaisesRegex(RuntimeError, "shutting down"):
                await pool.get_pool()
            release.set()
            with self.assertRaisesRegex(RuntimeError, "shut down"):
                await pending
            await closing
            instance.close.assert_awaited_once()
        with patch.object(pool.asyncpg, "create_pool", new=AsyncMock(return_value=replacement)):
            self.assertIs(await pool.get_pool(), replacement)

    async def test_timeout_and_cancelled_close_waiter(self):
        instance = fake_pool()
        entered = asyncio.Event()
        async def stalled():
            entered.set()
            await asyncio.Event().wait()
        instance.close.side_effect = stalled
        with patch.object(pool.asyncpg,"create_pool",new=AsyncMock(return_value=instance)), patch.object(pool,"CLOSE_TIMEOUT",0.05):
            await pool.get_pool()
            first = asyncio.create_task(pool.close_pool())
            await entered.wait()
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            await pool.close_pool()
            instance.terminate.assert_called_once()
            instance.close.assert_awaited_once()

    async def test_cross_loop_live_pool_rejected(self):
        instance = fake_pool()
        with patch.object(pool.asyncpg,"create_pool",new=AsyncMock(return_value=instance)):
            await pool.get_pool()
            def other_loop():
                return asyncio.run(pool.get_pool())
            with self.assertRaisesRegex(RuntimeError,"another event loop"):
                await asyncio.to_thread(other_loop)


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"), "requires PostgreSQL")
class RealPoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_parallel_pool_queries_and_reopen(self):
        import asyncpg
        original = asyncpg.create_pool
        dsn = os.environ["TGTEACHER_TEST_DSN"]
        async def create(**kwargs):
            for key in ("user","password","database","host","port"):
                kwargs.pop(key)
            return await original(dsn, **kwargs)
        self.addAsyncCleanup(pool.close_pool)
        with patch.object(pool.asyncpg,"create_pool",new=AsyncMock(side_effect=create)) as factory:
            async def query():
                db = await pool.get_pool()
                async with db.acquire() as conn:
                    return db, await conn.fetchval("SELECT 1")
            results = await asyncio.gather(*(query() for _ in range(40)))
            self.assertTrue(all(db is results[0][0] and value == 1 for db,value in results))
            factory.assert_awaited_once()
            await pool.close_pool()
            self.assertTrue(results[0][0].is_closing())
            replacement = await pool.get_pool()
            self.assertIsNot(replacement,results[0][0])
            self.assertEqual(factory.await_count,2)

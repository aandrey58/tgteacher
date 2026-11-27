"""One asyncpg pool per process/event loop, with cancellation-safe lifecycle."""
import asyncio
import logging
import os

import asyncpg

logger = logging.getLogger(__name__)
_pool = None
_owner_loop = None
_init_task = None
_close_task = None
_generation = 0
CLOSE_TIMEOUT = 15


def _check_loop():
    global _owner_loop
    loop = asyncio.get_running_loop()
    if _owner_loop is not loop:
        if _pool is not None or _init_task is not None or _close_task is not None:
            raise RuntimeError("Database pool belongs to another event loop; close it first")
        _owner_loop = loop
    return loop


def _observe_failure(task):
    # A caller may be cancelled while the shared background operation finishes.
    # Retrieve its exception without changing the outcome for remaining waiters.
    if not task.cancelled():
        task.exception()


async def _initialize():
    global _pool, _init_task
    try:
        _pool = await asyncpg.create_pool(
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DB", "tgteacher"),
            host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            min_size=1,
            max_size=15,
            timeout=10,
            command_timeout=60,
            server_settings={"application_name": "tgteacher_" + os.getenv("TGTEACHER_DB_SERVICE", "legacy")},
        )
        return _pool
    finally:
        _init_task = None


async def get_pool():
    global _init_task
    loop = _check_loop()
    if _close_task is not None:
        raise RuntimeError("Database pool is shutting down")
    if _pool is not None:
        return _pool
    generation = _generation
    # No await between checking and publishing the task: all callers on this
    # event loop await the same initialization, not independently created pools.
    if _init_task is None:
        _init_task = loop.create_task(_initialize())
        _init_task.add_done_callback(_observe_failure)
    result = await asyncio.shield(_init_task)
    if generation != _generation or _close_task is not None:
        raise RuntimeError("Database pool was shut down during initialization")
    return result


async def _shutdown():
    global _pool, _close_task
    try:
        if _init_task is not None:
            try:
                await asyncio.shield(_init_task)
            except Exception:
                # Initialization failure is delivered to its callers; nothing to close.
                logger.warning("Database initialization failed during shutdown")
        current = _pool
        if current is None:
            return
        try:
            await asyncio.wait_for(current.close(), timeout=CLOSE_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning("Database pool close timed out; terminating connections")
            current.terminate()
        except BaseException:
            current.terminate()
            raise
        finally:
            _pool = None
    finally:
        _close_task = None


async def close_pool():
    global _close_task, _generation
    loop = _check_loop()
    if _close_task is None:
        _generation += 1
        _close_task = loop.create_task(_shutdown())
        _close_task.add_done_callback(_observe_failure)
    await asyncio.shield(_close_task)

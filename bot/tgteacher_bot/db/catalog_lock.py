"""Bounded catalog writer acquisition; preserve the existing transaction fence."""
import asyncpg

WRITER_WAIT_MS = 5000


class CatalogBusy(ValueError):
    """Retry the operation later; the catalog transaction was not committed."""


async def lock_catalog_writer(conn):
    previous = await conn.fetchval("SHOW lock_timeout")
    await conn.execute("SELECT set_config('lock_timeout',$1,true)", str(WRITER_WAIT_MS)+"ms")
    try:
        await conn.execute("SELECT pg_advisory_xact_lock(847231, 1)")
    except asyncpg.LockNotAvailableError as exc:
        # The transaction is aborted; let its owning context roll back.
        raise CatalogBusy("Учебный каталог занят. Повторите операцию через несколько секунд.") from exc
    await conn.execute("SELECT set_config('lock_timeout',$1,true)", previous)

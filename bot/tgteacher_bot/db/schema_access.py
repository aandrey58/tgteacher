"""Runtime schema fence. Only the offline migration process performs DDL."""
import os
from tgteacher_bot.db.pool import get_pool

VERSION = 1
ROLES = {
    "bot": "tgteacher_bot_runtime",
    "mcp": "tgteacher_mcp_runtime",
    "admin": "tgteacher_admin_runtime",
}


def verify_only():
    mode = os.getenv("TGTEACHER_DB_SCHEMA_MODE", "legacy")
    if mode not in ("legacy", "verify"):
        raise RuntimeError("Invalid database schema mode")
    return mode == "verify"


async def verify_schema():
    role = ROLES.get(os.getenv("TGTEACHER_DB_SERVICE"))
    if not role:
        raise RuntimeError("Missing runtime database service")
    pool = await get_pool()
    async with pool.acquire() as c:
        actual = await c.fetchval("SELECT current_user")
        version = await c.fetchval("SELECT version FROM runtime_schema_release WHERE singleton")
        if actual != role or version != VERSION:
            raise RuntimeError("Runtime database role or schema version mismatch; run offline migration")

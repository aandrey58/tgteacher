"""Actual PostgreSQL runtime credential/permission smoke, executed in each container."""
import asyncio
import os
import asyncpg
from tgteacher_bot.db import pool, user_repo
from tgteacher_bot.db.schema_access import ROLES

async def main():
    service = os.environ["TGTEACHER_DB_SERVICE"]
    await user_repo.init_db_pg()
    db = await pool.get_pool()
    async with db.acquire() as c:
        assert await c.fetchval("SELECT current_user") == ROLES[service]
        assert not await c.fetchval("SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls FROM pg_roles WHERE rolname=current_user")
        await c.fetchval("SELECT count(*) FROM families")
        assert await c.fetchval("SELECT 1 FROM content_schema_migrations WHERE name='srs_v1'")
        forbidden = ["CREATE TABLE public.runtime_ddl_probe(id integer)", "CREATE TEMP TABLE runtime_temp_probe(id integer)", "UPDATE runtime_schema_release SET version=0", "DELETE FROM content_schema_migrations WHERE false"]
        if service != "admin":
            forbidden.append("SELECT count(*) FROM web_admin_accounts")
        if service == "mcp":
            forbidden.extend(["SELECT count(*) FROM payments","SELECT count(*) FROM stars_orders","SELECT count(*) FROM web_support","UPDATE users SET is_subscribed=true WHERE false"])
        if service == "admin":
            forbidden.append("UPDATE stars_orders SET state='paid' WHERE false")
        for statement in forbidden:
            try:
                async with c.transaction():
                    await c.execute(statement)
                    raise AssertionError("Unexpected granted permission: "+statement)
            except asyncpg.InsufficientPrivilegeError:
                pass
    if service == "bot":
        from tgteacher_bot.services.legacy.system_snapshots import init_system_snapshots_pg
        await init_system_snapshots_pg()
        await user_repo.add_or_update_user_pg(900000099,"ci-probe","CI","Probe")
        await user_repo.delete_user_and_progress_pg(900000099)
    await db.close()
    print(service+": permission checks passed")

if __name__ == "__main__":
    asyncio.run(main())

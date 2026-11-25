"""Offline PostgreSQL role provisioning. Run only on a dedicated TGTeacher database."""
import asyncio
import base64
import hashlib
import hmac
import os
import secrets

from tgteacher_bot.db import user_repo, pool
from tgteacher_bot.db.schema_access import VERSION, ROLES, verify_only
from tgteacher_bot.web_admin import init_web

# Explicit, fail-closed table allowlist. Content publishing legitimately mutates
# learner progress, and the bot retains legacy administration during cutover.
CONTENT = set("families stage1_words stage2_tasks stage3_tasks stage4_tasks stage5_tasks stage6_tasks stage7_tasks stage8_tasks family_versions family_edit_drafts content_drafts family_audio_versions family_image_versions audio_assets image_assets tts_requests image_requests".split())
PROGRESS = set("user_family_progress user_task_progress user_last_opened_family_place user_stage_state user_current_family family_version_learner_history legacy_unmapped_progress srs_cards srs_reviews learner_answer_events srs_review_tickets learner_preferences".split())
LEARNERS = {"users", "deleted_learner_markers", "learner_tokens"}
OPERATIONS = set("web_support web_campaigns web_deliveries web_message_attachments web_admin_list_change web_system_control web_subscription_extensions payment_options stars_tariffs".split())
FINANCE = {"payments", "payment_orders", "payment_refund_observations", "stars_orders"}
AUTH = {"web_admin_accounts", "web_admin_sessions", "web_admin_audit"}
VIEWS = {"active_task_positions", "active_user_task_progress"}
COMMON_READ = VIEWS | {"runtime_schema_release"}
WRITES = {
    "bot": CONTENT | PROGRESS | LEARNERS | OPERATIONS | FINANCE | {"system_snapshots"},
    "admin": CONTENT | PROGRESS | LEARNERS | OPERATIONS | AUTH | {"payments"},
    "mcp": CONTENT | PROGRESS,
}
READS = {
    "bot": COMMON_READ,
    "admin": COMMON_READ | (FINANCE - {"payments"}) | {"system_snapshots"},
    "mcp": COMMON_READ | {"users", "learner_tokens"},
}


def ident(value):
    return '"' + value.replace('"', '""') + '"'


def scram(password):
    # Send a SCRAM verifier, never the cleartext password in a SQL statement.
    salt = secrets.token_bytes(16)
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 4096)
    client = hmac.digest(salted, b"Client Key", "sha256")
    server = hmac.digest(salted, b"Server Key", "sha256")
    b64 = lambda value: base64.b64encode(value).decode("ascii")
    return "SCRAM-SHA-256$4096:" + b64(salt) + "$" + b64(hashlib.sha256(client).digest()) + ":" + b64(server)


async def provision():
    if verify_only():
        raise RuntimeError("Migration must use the offline owner connection")
    passwords = {s: os.environ["TGTEACHER_"+s.upper()+"_DB_PASSWORD"] for s in ROLES}
    if any(len(p)<24 or not p.isascii() or any(ord(ch)<33 or ord(ch)>126 for ch in p) for p in passwords.values()):
        raise RuntimeError("Use distinct printable ASCII runtime passwords, at least 24 characters")
    if len(set(passwords.values()) | {os.getenv("POSTGRES_PASSWORD")}) != 4:
        raise RuntimeError("Runtime and owner credentials must differ")
    db = await pool.get_pool()
    # Refuse accidental provisioning from a runtime or non-superuser role.
    async with db.acquire() as c:
        if not await c.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname=current_user"):
            raise RuntimeError("Offline provisioning requires the database administrator")
        if await c.fetchval("SELECT current_schema()") != "public":
            raise RuntimeError("Expected dedicated public schema")
    await user_repo.init_db_pg()
    async with db.acquire() as c:
        async with c.transaction():
            await c.execute("SELECT pg_advisory_xact_lock(847231,1)")
            await c.execute("SELECT pg_advisory_xact_lock(847231,8)")
            await init_web(c)
            await c.execute("""
                CREATE TABLE IF NOT EXISTS system_snapshots (
                    id BIGSERIAL PRIMARY KEY, ts TIMESTAMPTZ NOT NULL DEFAULT NOW(), data JSONB NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_system_snapshots_ts ON system_snapshots(ts DESC);
                CREATE TABLE IF NOT EXISTS runtime_schema_release (
                    singleton BOOLEAN PRIMARY KEY CHECK(singleton), version INTEGER NOT NULL
                );
            """)
            database = ident(await c.fetchval("SELECT current_database()"))
            await c.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
            await c.execute("REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC")
            await c.execute("REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM PUBLIC")
            await c.execute(f"REVOKE TEMPORARY ON DATABASE {database} FROM PUBLIC")
            for service, name in ROLES.items():
                role = ident(name)
                existing = await c.fetchrow("SELECT oid FROM pg_roles WHERE rolname=$1",name)
                if existing:
                    oid = existing["oid"]
                    if await c.fetchval("SELECT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=$1 OR roleid=$1)",oid):
                        raise RuntimeError("Runtime role has unexpected memberships")
                    if await c.fetchval("SELECT EXISTS(SELECT 1 FROM pg_class WHERE relowner=$1 UNION ALL SELECT 1 FROM pg_namespace WHERE nspowner=$1 UNION ALL SELECT 1 FROM pg_database WHERE datdba=$1)",oid):
                        raise RuntimeError("Runtime role unexpectedly owns database objects")
                else:
                    await c.execute(f"CREATE ROLE {role} NOLOGIN")
                verifier = scram(passwords[service])
                await c.execute(f"ALTER ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD '{verifier}'")
                await c.execute(f"REVOKE ALL ON DATABASE {database} FROM {role}")
                await c.execute(f"GRANT CONNECT ON DATABASE {database} TO {role}")
                await c.execute(f"REVOKE ALL ON SCHEMA public FROM {role}")
                await c.execute(f"GRANT USAGE ON SCHEMA public TO {role}")
                await c.execute(f"REVOKE ALL ON ALL TABLES IN SCHEMA public FROM {role}")
                await c.execute(f"REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {role}")
                for table in sorted(WRITES[service]):
                    await c.execute(f"GRANT SELECT,INSERT,UPDATE,DELETE ON public.{ident(table)} TO {role}")
                for table in sorted(READS[service]):
                    await c.execute(f"GRANT SELECT ON public.{ident(table)} TO {role}")
                sequences = await c.fetch("""
                    SELECT DISTINCT seq.relname FROM pg_class seq
                    JOIN pg_depend d ON d.objid=seq.oid AND d.classid='pg_class'::regclass
                    JOIN pg_class tab ON tab.oid=d.refobjid AND d.refclassid='pg_class'::regclass
                    JOIN pg_namespace n ON n.oid=seq.relnamespace
                    WHERE seq.relkind='S' AND n.nspname='public' AND d.deptype IN ('a','i')
                      AND tab.relname=ANY($1::text[])
                """, sorted(WRITES[service]))
                for seq in sequences:
                    await c.execute(f"GRANT USAGE ON SEQUENCE public.{ident(seq['relname'])} TO {role}")
            await c.execute("INSERT INTO runtime_schema_release(singleton,version) VALUES(true,$1) ON CONFLICT(singleton) DO UPDATE SET version=excluded.version",VERSION)


async def main():
    try:
        await provision()
        print("Runtime roles and schema release prepared.")
    finally:
        if pool._pool is not None:
            await pool._pool.close()


if __name__ == "__main__":
    asyncio.run(main())

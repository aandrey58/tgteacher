"""One-time OAuth-to-Telegram linking. Only the private bot confirms ownership."""
import hashlib
import os
import re
import secrets

from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.db.srs_repo import lock_user

CODE = re.compile(r"[A-Za-z0-9_-]{32}")


def enabled():
    return os.getenv("TGTEACHER_LEARNER_OAUTH_ENABLED", "0") == "1"


def digest(code):
    return hashlib.sha256(code.encode("ascii")).hexdigest()


async def lock_subject(conn, issuer, subject):
    await conn.execute("SELECT pg_advisory_xact_lock(847233,hashtext($1))", issuer+"\n"+subject)


async def resolve(issuer, subject):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT user_id FROM learner_oauth_links WHERE issuer=$1 AND subject=$2",
            issuer, subject)


async def begin(issuer, subject):
    code = secrets.token_urlsafe(24)
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM learner_oauth_challenges WHERE expires_at<=now()")
            await lock_subject(conn, issuer, subject)
            if await conn.fetchval("SELECT 1 FROM learner_oauth_links WHERE issuer=$1 AND subject=$2", issuer, subject):
                raise ValueError("Already linked; unlink in the Telegram bot first")
            await conn.execute("""
                INSERT INTO learner_oauth_challenges(issuer,subject,code_hash,expires_at)
                VALUES($1,$2,$3,now()+interval '10 minutes')
                ON CONFLICT(issuer,subject) DO UPDATE SET code_hash=excluded.code_hash,expires_at=excluded.expires_at
                """, issuer, subject, digest(code))
    return code


async def confirm(uid, code):
    if not isinstance(code, str) or not CODE.fullmatch(code):
        return False
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await lock_user(conn, uid)
            if not await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", uid):
                return False
            candidate = await conn.fetchrow("SELECT issuer,subject FROM learner_oauth_challenges WHERE code_hash=$1 AND expires_at>now()", digest(code))
            if not candidate:
                return False
            await lock_subject(conn, candidate["issuer"], candidate["subject"])
            challenge = await conn.fetchrow("""
                DELETE FROM learner_oauth_challenges WHERE code_hash=$1 AND expires_at>now()
                RETURNING issuer,subject
                """, digest(code))
            if not challenge:
                return False
            # One Telegram account and one issuer/subject per link; never replace an existing link.
            inserted = await conn.fetchval("""
                INSERT INTO learner_oauth_links(issuer,subject,user_id) VALUES($1,$2,$3)
                ON CONFLICT DO NOTHING RETURNING user_id
                """, challenge["issuer"], challenge["subject"], uid)
            return inserted == uid


async def unlink(uid):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await lock_user(conn, uid)
            link = await conn.fetchrow("SELECT issuer,subject FROM learner_oauth_links WHERE user_id=$1", uid)
            if link:
                await lock_subject(conn, link["issuer"], link["subject"])
                await conn.execute("DELETE FROM learner_oauth_links WHERE user_id=$1", uid)
                await conn.execute("DELETE FROM learner_oauth_challenges WHERE issuer=$1 AND subject=$2", link["issuer"], link["subject"])
            return bool(link)

"""Individual web identities; independent of Telegram and MCP credentials."""
import asyncio
import hashlib
import hmac
import re
import secrets

ROLES={"owner","editor","support"}
ITERATIONS=600_000
EDITOR_PAGES={"dashboard","groups","drafts","reviews","media","tts","images","budgets"}
EDITOR_READ={"family","draft","edit_draft","validate","preview","versions","version","media","voices","tts_models","estimate_audio","parse_source","render_source","estimate_image"}
EDITOR_WRITE={"create_draft","update_draft","create_edit","update_edit","delete_draft","delete_edit","publish","publish_edit","archive","rollback","attach_audio","attach_image","detach","bulk_archive","generate_audio","generate_image"}
SUPPORT_ACTIONS={"support","campaigns","create_campaign","confirm_campaign","cancel_campaign","close_question"}
DUMMY_HASH="pbkdf2-sha256:"+"0"*32+":"+"0"*64


def valid_username(value):
    if not isinstance(value,str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{2,63}",value):
        raise ValueError("Invalid username")
    return value.lower()


def encode_password(password):
    if not isinstance(password,str) or not 16<=len(password)<=256:
        raise ValueError("Password must have 16–256 characters")
    salt=secrets.token_bytes(16)
    key=hashlib.pbkdf2_hmac("sha256",password.encode("utf-8"),salt,ITERATIONS)
    return f"pbkdf2-sha256:{salt.hex()}:{key.hex()}"


def verify_password(password,encoded):
    if not isinstance(password,str) or len(password)>256:
        return False
    try:
        scheme,salt,key=encoded.split(":")
        if scheme!="pbkdf2-sha256" or len(salt)!=32 or len(key)!=64:
            return False
        derived=hashlib.pbkdf2_hmac("sha256",password.encode("utf-8"),bytes.fromhex(salt),ITERATIONS)
        return hmac.compare_digest(derived,bytes.fromhex(key))
    except (ValueError,TypeError,UnicodeError):
        return False


def page_allowed(role,page):
    return role=="owner" or role=="editor" and page in EDITOR_PAGES or role=="support" and page=="images"


def route_allowed(role,route):
    return role=="owner" or role=="editor" and route in {"asset","export_zip","template","import_zip","upload_media"} or role=="support" and route in {"asset","upload_attachment"}


def action_allowed(role,name):
    return role=="owner" or role=="editor" and name in EDITOR_READ|EDITOR_WRITE|{"change_own_password"} or role=="support" and name in SUPPORT_ACTIONS|{"change_own_password"}


async def initialize(c):
    await c.execute("""
        CREATE TABLE IF NOT EXISTS web_admin_accounts (
            id BIGSERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('owner','editor','support')),
            enabled BOOLEAN NOT NULL DEFAULT true, revision BIGINT NOT NULL DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        ALTER TABLE web_admin_sessions ADD COLUMN IF NOT EXISTS account_id BIGINT REFERENCES web_admin_accounts(id) ON DELETE CASCADE;
        ALTER TABLE web_admin_sessions ADD COLUMN IF NOT EXISTS account_revision BIGINT;
        ALTER TABLE web_admin_audit ADD COLUMN IF NOT EXISTS actor TEXT;
        ALTER TABLE web_admin_audit ADD COLUMN IF NOT EXISTS actor_role TEXT;
        CREATE INDEX IF NOT EXISTS web_admin_sessions_account_idx ON web_admin_sessions(account_id);
    """)


async def accounts(pool,offset=0):
    if type(offset) is not int or not 0<=offset<=10000:
        raise ValueError("Invalid offset")
    async with pool.acquire() as c:
        rows=await c.fetch("SELECT id,username,role,enabled,revision,created_at::text AS created_at FROM web_admin_accounts ORDER BY id LIMIT 51 OFFSET $1",offset)
    return {"items":[dict(r) for r in rows[:50]],"next":len(rows)>50}


async def create(pool,username,password,role):
    username=valid_username(username)
    if username=="recovery" or not isinstance(role,str) or role not in ROLES:
        raise ValueError("Invalid role or reserved username")
    hashed=await asyncio.to_thread(encode_password,password)
    async with pool.acquire() as c:
        async with c.transaction():
            await c.execute("SELECT pg_advisory_xact_lock(847231,96)")
            if await c.fetchval("SELECT count(*) FROM web_admin_accounts")>=500:
                raise ValueError("Account limit")
            row=await c.fetchrow("""INSERT INTO web_admin_accounts(username,password_hash,role) VALUES($1,$2,$3)
                ON CONFLICT(username) DO NOTHING RETURNING id,username,role,enabled,revision""",username,hashed,role)
            if not row:
                raise ValueError("Username exists")
            return dict(row)


async def update(pool,account_id,expected_revision,role,enabled,password=None):
    if type(account_id) is not int or account_id<1 or type(expected_revision) is not int or expected_revision<1 or not isinstance(role,str) or role not in ROLES or type(enabled) is not bool:
        raise ValueError("Invalid account update")
    hashed=await asyncio.to_thread(encode_password,password) if password is not None else None
    async with pool.acquire() as c:
        async with c.transaction():
            await c.execute("SELECT pg_advisory_xact_lock(847231,96)")
            row=await c.fetchrow("SELECT role,enabled,revision FROM web_admin_accounts WHERE id=$1 FOR UPDATE",account_id)
            if not row or row["revision"]!=expected_revision:
                raise ValueError("Account revision conflict")
            if row["role"]=="owner" and row["enabled"] and (role!="owner" or not enabled):
                if await c.fetchval("SELECT count(*) FROM web_admin_accounts WHERE role='owner' AND enabled")<=1:
                    raise ValueError("Cannot disable last owner")
            updated=await c.fetchrow("""UPDATE web_admin_accounts SET role=$2,enabled=$3,password_hash=coalesce($4,password_hash),revision=revision+1
                WHERE id=$1 RETURNING id,username,role,enabled,revision""",account_id,role,enabled,hashed)
            await c.execute("DELETE FROM web_admin_sessions WHERE account_id=$1",account_id)
            return dict(updated)


async def own_password(pool,actor,current_password,new_password):
    if actor["id"] is None:
        raise ValueError("Recovery key is managed on server")
    async with pool.acquire() as c:
        row=await c.fetchrow("SELECT password_hash,role,enabled,revision FROM web_admin_accounts WHERE id=$1",actor["id"])
    if not row or not row["enabled"] or row["revision"]!=actor["revision"] or not await asyncio.to_thread(verify_password,current_password,row["password_hash"]):
        raise ValueError("Invalid current password")
    return await update(pool,actor["id"],row["revision"],row["role"],True,new_password)

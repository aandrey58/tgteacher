"""Immutable image assets, request cache and conservative budget ledger."""
import asyncio
import hashlib
import json
import re

from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.services.image_providers import configured_models, daily_limit, normalize_image

MANAGED = re.compile(r"img-([0-9a-f]{64})\.jpg\Z")


async def init_images(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS image_assets (
            asset_id TEXT PRIMARY KEY, jpeg BYTEA NOT NULL,
            width INTEGER NOT NULL CHECK(width BETWEEN 16 AND 1536),
            height INTEGER NOT NULL CHECK(height BETWEEN 16 AND 1536),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CHECK(length(asset_id)=64), CHECK(octet_length(jpeg) BETWEEN 1 AND 5242880)
        );
        CREATE TABLE IF NOT EXISTS image_requests (
            cache_key TEXT PRIMARY KEY, provider TEXT NOT NULL, model TEXT NOT NULL, size TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('pending','ready','failed')),
            reserved_microusd BIGINT NOT NULL CHECK(reserved_microusd>0),
            reserved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            asset_id TEXT REFERENCES image_assets(asset_id),
            CHECK ((status='ready')=(asset_id IS NOT NULL))
        );
        CREATE INDEX IF NOT EXISTS image_requests_reserved_idx ON image_requests(reserved_at);
        CREATE TABLE IF NOT EXISTS family_image_versions (
            family_id INTEGER NOT NULL, version INTEGER NOT NULL, task_id INTEGER NOT NULL,
            asset_id TEXT NOT NULL REFERENCES image_assets(asset_id),
            PRIMARY KEY(family_id,version,task_id),
            FOREIGN KEY(family_id,version) REFERENCES family_versions(family_id,version) ON DELETE CASCADE
        );
    """)


def asset_filename(asset_id):
    if not isinstance(asset_id, str) or not re.fullmatch(r"[0-9a-f]{64}", asset_id):
        raise ValueError("Invalid image asset ID")
    return f"img-{asset_id}.jpg"


def managed_id(filename):
    match = MANAGED.fullmatch(filename or "")
    return match.group(1) if match else None


async def image_exists(conn, filename):
    ident = managed_id(filename)
    return bool(ident and await conn.fetchval("SELECT 1 FROM image_assets WHERE asset_id=$1", ident))


async def get_image_bytes(filename):
    ident = managed_id(filename)
    if not ident:
        return None
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT jpeg FROM image_assets WHERE asset_id=$1", ident)


async def record_image_links(conn, family_id, version, content):
    for task in content.get("stage5_tasks", []):
        ident = managed_id(task.get("image"))
        if ident and await image_exists(conn, task["image"]):
            await conn.execute("""
                INSERT INTO family_image_versions(family_id,version,task_id,asset_id)
                VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING
            """, family_id, version, task["task_id"], ident)


def model_profiles():
    return [{"provider": p.name, "model": p.model, "size": p.size,
             "surface": p.surface, "microusd_per_image": p.cost_microusd}
            for p in configured_models().values()]


def prepare(items):
    if not isinstance(items, list) or not 1 <= len(items) <= 10:
        raise ValueError("Supply 1–10 image requests")
    models = configured_models()
    result = []
    for item in items:
        if not isinstance(item, dict) or set(item) != {"provider", "model", "prompt"} or any(not isinstance(v,str) for v in item.values()):
            raise ValueError("Each request needs provider, model and prompt")
        p = models.get((item["provider"], item["model"]))
        prompt = item["prompt"].strip()
        if not p or not 1 <= len(prompt.encode()) <= 4000 or any(ord(c)<32 and c not in "\n\t" for c in prompt):
            raise ValueError("Model must be configured; prompt must contain 1–4000 UTF-8 bytes")
        key = hashlib.sha256(json.dumps(["image-v1",p.name,p.model,p.surface,p.size,prompt,"jpeg"],ensure_ascii=False).encode()).hexdigest()
        result.append((key,p,prompt))
    return result


async def generate_batch(items, max_cost_microusd, estimate_only=False):
    prepared = prepare(items)
    if type(max_cost_microusd) is not int or not 0 <= max_cost_microusd <= 1_000_000_000:
        raise ValueError("Invalid approved maximum cost")
    unique = {key:(key,p,prompt) for key,p,prompt in prepared}
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(847231, 5)")
            existing = {r["cache_key"]: r for r in await conn.fetch(
                "SELECT cache_key,status,asset_id,reserved_microusd FROM image_requests WHERE cache_key=ANY($1::text[])",list(unique))}
            cost = sum(p.cost_microusd for key,p,_ in unique.values() if key not in existing)
            reservation_time=await conn.fetchval("SELECT statement_timestamp()")
            spent = int(await conn.fetchval("""SELECT coalesce(sum(reserved_microusd),0) FROM image_requests
                WHERE reserved_at >= date_trunc('day',$1::timestamptz AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
                AND reserved_at < (date_trunc('day',$1::timestamptz AT TIME ZONE 'UTC') + interval '1 day') AT TIME ZONE 'UTC'""",reservation_time))
            limit = daily_limit()
            if estimate_only:
                return {"estimated_microusd": cost, "daily_reserved_microusd": spent, "daily_limit_microusd": limit,
                        "within_budget": cost == 0 or spent + cost <= limit,
                        "items": [{"cache_key": k, "status": existing[k]["status"] if k in existing else "new"} for k,_,_ in prepared]}
            if cost > max_cost_microusd or (cost and spent+cost > limit):
                raise ValueError("Image generation exceeds approved or daily budget")
            for key,p,_ in unique.values():
                if key not in existing:
                    await conn.execute("""INSERT INTO image_requests(cache_key,provider,model,size,status,reserved_microusd,reserved_at)
                        VALUES($1,$2,$3,$4,'pending',$5,$6)""",key,p.name,p.model,p.size,p.cost_microusd,reservation_time)
    semaphore = asyncio.Semaphore(2)

    async def run(entry):
        key,p,prompt = entry
        cached = key in existing
        if cached:
            row = existing[key]
        else:
            async with semaphore:
                try:
                    async with asyncio.timeout(180):
                        raw = await p.generate(prompt)
                        data,w,h = await asyncio.to_thread(normalize_image,raw)
                    ident = hashlib.sha256(data).hexdigest()
                    async with pool.acquire() as conn:
                        async with conn.transaction():
                            await conn.execute("INSERT INTO image_assets(asset_id,jpeg,width,height) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",ident,data,w,h)
                            await conn.execute("UPDATE image_requests SET status='ready',asset_id=$2 WHERE cache_key=$1 AND status='pending'",key,ident)
                    row = {"status":"ready","asset_id":ident,"reserved_microusd":p.cost_microusd}
                except Exception:
                    async with pool.acquire() as conn:
                        await conn.execute("UPDATE image_requests SET status='failed' WHERE cache_key=$1 AND status='pending'",key)
                        row = await conn.fetchrow("SELECT status,asset_id,reserved_microusd FROM image_requests WHERE cache_key=$1",key)
        ident = row["asset_id"]
        return key, {"cache_key":key,"status":row["status"],"asset_id":ident,
                     "filename":asset_filename(ident) if ident else None,"cached":cached,
                     "reserved_microusd":row["reserved_microusd"]}
    results = dict(await asyncio.gather(*(run(e) for e in unique.values())))
    return {"reserved_microusd":cost,"items":[results[k] for k,_,_ in prepared]}

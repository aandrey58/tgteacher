"""Durable TTS cache, conservative spend reservations and versioned audio links."""
import asyncio
import hashlib
import json
import re

from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.services.tts_providers import configured_providers, daily_limit, check_mp3, MAX_TEXT_BYTES, discover_catalog, GATEWAYS, KEYS

MANAGED = re.compile(r"tts-([0-9a-f]{64})\.mp3\Z")
MAX_BATCH = 10


async def init_tts(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS audio_assets (
            asset_id TEXT PRIMARY KEY, mp3 BYTEA NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CHECK (length(asset_id)=64), CHECK (octet_length(mp3) BETWEEN 128 AND 5242880)
        );
        CREATE TABLE IF NOT EXISTS tts_requests (
            cache_key TEXT PRIMARY KEY, provider TEXT NOT NULL, model TEXT NOT NULL,
            voice TEXT NOT NULL, status TEXT NOT NULL CHECK(status IN ('pending','ready','failed')),
            reserved_microusd BIGINT NOT NULL CHECK(reserved_microusd>0),
            reserved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            asset_id TEXT REFERENCES audio_assets(asset_id),
            CHECK ((status='ready')=(asset_id IS NOT NULL))
        );
        CREATE INDEX IF NOT EXISTS tts_requests_reserved_idx ON tts_requests(reserved_at);
        CREATE TABLE IF NOT EXISTS family_audio_versions (
            family_id INTEGER NOT NULL, version INTEGER NOT NULL, task_id INTEGER NOT NULL,
            asset_id TEXT NOT NULL REFERENCES audio_assets(asset_id),
            PRIMARY KEY(family_id,version,task_id),
            FOREIGN KEY(family_id,version) REFERENCES family_versions(family_id,version) ON DELETE CASCADE
        );
    """)


def asset_filename(asset_id):
    if not isinstance(asset_id, str) or not re.fullmatch(r"[0-9a-f]{64}", asset_id):
        raise ValueError("Invalid audio asset ID")
    return f"tts-{asset_id}.mp3"


def managed_id(filename):
    match = MANAGED.fullmatch(filename or "")
    return match.group(1) if match else None


async def audio_exists(conn, filename):
    ident = managed_id(filename)
    return bool(ident and await conn.fetchval("SELECT 1 FROM audio_assets WHERE asset_id=$1", ident))


async def get_audio_bytes(filename):
    ident = managed_id(filename)
    if not ident:
        return None
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT mp3 FROM audio_assets WHERE asset_id=$1", ident)


async def record_audio_links(conn, family_id, version, content):
    for task in content.get("stage7_tasks", []):
        ident = managed_id(task.get("audio"))
        if ident and await audio_exists(conn, task["audio"]):
            await conn.execute("""
                INSERT INTO family_audio_versions(family_id,version,task_id,asset_id)
                VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING
            """, family_id, version, task["task_id"], ident)


async def validate_audio_files(conn, content, legacy_directory=None):
    from tgteacher_bot.utils.content_validation import validate_family, ContentValidationError
    # Structure is validated even when media is stored outside the family folder.
    validate_family(content)
    if content.get("stage5_tasks") and legacy_directory is None:
        raise ContentValidationError("Image attachment is not available without a legacy media folder")
    legacy = {**content, "stage7_tasks": []}
    for task in content.get("stage7_tasks", []):
        if not await audio_exists(conn, task["audio"]):
            if legacy_directory is None:
                raise ContentValidationError("Missing generated audio asset: " + task["audio"])
            legacy["stage7_tasks"].append(task)
    if legacy_directory is not None:
        validate_family(legacy, media_dir=legacy_directory)


def list_providers():
    return {"providers": [{"provider": p.name, "model": p.model, "voices": len(p.voices),
                           "rate_microusd_per_utf8_byte": p.rate_microusd_per_byte, "source_format": p.source_format}
                          for p in configured_providers().values()],
            "daily_limit_microusd": daily_limit(), "max_batch": MAX_BATCH, "max_text_bytes": MAX_TEXT_BYTES}


def _select(providers, provider, model=None):
    matches = [p for p in providers.values() if p.name == provider and (model is None or p.model == model)]
    if len(matches) != 1:
        raise ValueError("Select one explicitly configured provider and model")
    return matches[0]


def list_voices(provider, model=None):
    p = _select(configured_providers(), provider, model)
    return {"provider": provider, "model": p.model, "voices": list(p.voices)}


async def discover_providers():
    import os
    result = list_providers()
    enabled = {(p["provider"], p["model"]) for p in result["providers"]}
    catalogs = []
    for name in GATEWAYS:
        if not os.getenv(KEYS[name], "").strip():
            continue
        try:
            rows = await discover_catalog(name)
            catalogs.append({"provider": name, "models": [{**r, "generation_configured": (name, r["model"]) in enabled} for r in rows]})
        except Exception:
            catalogs.append({"provider": name, "error": "Catalog unavailable; configured profiles remain listed"})
    return {**result, "catalogs": catalogs}


async def discover_voices(provider, model=None):
    profiles = [p for p in configured_providers().values() if p.name == provider and (model is None or p.model == model)]
    if provider in GATEWAYS:
        if model is None and len(profiles) == 1:
            model = profiles[0].model
        if model is None:
            raise ValueError("Specify a model")
        try:
            row = next((r for r in await discover_catalog(provider) if r["model"] == model), None)
        except Exception:
            row = None
        if row and row["voices"]:
            if profiles and profiles[0].voices:
                row = {**row, "voices": [v for v in row["voices"] if v in profiles[0].voices]}
            return {"provider": provider, **row, "generation_configured": bool(profiles)}
    return list_voices(provider, model)


async def _prepare(items):
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_BATCH:
        raise ValueError("Supply 1–10 audio requests")
    providers = configured_providers()
    prepared = []
    for item in items:
        if not isinstance(item, dict) or not {"provider", "voice", "text"} <= set(item) or set(item) - {"provider", "voice", "text", "model"}:
            raise ValueError("Each request needs provider, voice and text")
        provider, voice, text = item["provider"], item["voice"], item["text"]
        if not all(isinstance(v, str) for v in (provider, voice, text)):
            raise ValueError("Provider, voice and text must be strings")
        model = item.get("model")
        if model is not None and not isinstance(model, str):
            raise ValueError("Model must be a string")
        p = _select(providers, provider, model)
        if p.voices:
            voices = p.voices
        elif provider in GATEWAYS:
            row = next((r for r in await discover_catalog(provider) if r["model"] == p.model), None)
            if not row or (row["formats"] and p.source_format not in row["formats"]):
                raise ValueError("Model or output format unavailable")
            voices = row["voices"]
        else:
            voices = ()
        if voice not in voices:
            raise ValueError("Voice is not available or configured")
        text = text.strip()
        size = len(text.encode("utf-8"))
        if not 1 <= size <= MAX_TEXT_BYTES or any(ord(c) < 32 and c not in "\n\t" for c in text):
            raise ValueError("Text must contain 1–2000 UTF-8 bytes without control characters")
        key = hashlib.sha256(json.dumps(["tts-v1", p.name, p.model, voice, text, "mp3"] if p.source_format == "mp3" else ["tts-v2", p.name, p.model, voice, text, p.source_format, p.pcm_rate, p.pcm_channels, "mp3"], ensure_ascii=False).encode()).hexdigest()
        prepared.append((key, p, voice, text, size * p.rate_microusd_per_byte))
    return prepared


async def _existing(conn, prepared):
    keys = list({entry[0] for entry in prepared})
    return {r["cache_key"]: r for r in await conn.fetch("SELECT cache_key,status,asset_id,reserved_microusd FROM tts_requests WHERE cache_key=ANY($1::text[])", keys)}


async def _spent(conn):
    return int(await conn.fetchval("""
        SELECT coalesce(sum(reserved_microusd),0) FROM tts_requests
        WHERE reserved_at >= date_trunc('day', statement_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
    """))


def _cost(prepared, existing):
    return sum({key: cost for key, _, _, _, cost in prepared if key not in existing}.values())


async def estimate(items):
    prepared = await _prepare(items)
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await _existing(conn, prepared)
        spent = await _spent(conn)
    cost = _cost(prepared, existing)
    return {"estimated_microusd": cost, "daily_reserved_microusd": spent,
            "daily_limit_microusd": daily_limit(), "within_budget": spent + cost <= daily_limit(),
            "items": [{"cache_key": key, "status": existing[key]["status"] if key in existing else "new",
                       "asset_id": existing[key]["asset_id"] if key in existing else None}
                      for key, *_ in prepared]}


def _result(key, row, cached):
    ident = row["asset_id"]
    return {"cache_key": key, "status": row["status"], "asset_id": ident,
            "filename": asset_filename(ident) if ident else None,
            "cached": cached, "reserved_microusd": row["reserved_microusd"]}


async def generate_batch(items, max_cost_microusd):
    prepared = await _prepare(items)
    if type(max_cost_microusd) is not int or not 0 <= max_cost_microusd <= 1000000000:
        raise ValueError("Invalid maximum approved cost")
    pool = await get_pool()
    unique = {entry[0]: entry for entry in prepared}
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Serialize reservations across all workers, not the network calls.
            await conn.execute("SELECT pg_advisory_xact_lock(847231, 4)")
            existing = await _existing(conn, prepared)
            cost = _cost(prepared, existing)
            if cost > max_cost_microusd:
                raise ValueError("Estimated cost exceeds approved maximum")
            if cost and await _spent(conn) + cost > daily_limit():
                raise ValueError("Daily TTS budget exceeded")
            for key, p, voice, _, price in unique.values():
                if key not in existing:
                    await conn.execute("""
                        INSERT INTO tts_requests(cache_key,provider,model,voice,status,reserved_microusd,reserved_at)
                        VALUES($1,$2,$3,$4,'pending',$5,statement_timestamp())
                    """, key, p.name, p.model, voice, price)
    semaphore = asyncio.Semaphore(3)

    async def run(entry):
        key, p, voice, text, price = entry
        if key in existing:
            return key, _result(key, existing[key], True)
        async with semaphore:
            try:
                async with asyncio.timeout(125):
                    data = check_mp3(await p.synthesize(text, voice))
                ident = hashlib.sha256(data).hexdigest()
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        await conn.execute("INSERT INTO audio_assets(asset_id,mp3) VALUES($1,$2) ON CONFLICT DO NOTHING", ident, data)
                        await conn.execute("UPDATE tts_requests SET status='ready',asset_id=$2 WHERE cache_key=$1 AND status='pending'", key, ident)
                row = {"status": "ready", "asset_id": ident, "reserved_microusd": price}
            except Exception:
                # Upstream may have billed a failed request. Keep the reservation
                # and never automatically retry ambiguous/failed generations.
                async with pool.acquire() as conn:
                    await conn.execute("UPDATE tts_requests SET status='failed' WHERE cache_key=$1 AND status='pending'", key)
                    row = await conn.fetchrow("SELECT status,asset_id,reserved_microusd FROM tts_requests WHERE cache_key=$1", key)
            return key, _result(key, row, False)

    results = dict(await asyncio.gather(*(run(entry) for entry in unique.values())))
    return {"reserved_microusd": cost, "items": [results[key] for key, *_ in prepared]}


async def attach_audio(draft_kind, draft_id, expected_revision, task_index, asset_id, expected_task_id=None):
    from tgteacher_bot.db import content_operations as ops, drafts_repo as drafts, content_versions as versions
    from tgteacher_bot.utils.family_parser import parse_family_text
    filename = asset_filename(asset_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        if not await audio_exists(conn, filename):
            raise ValueError("Audio asset not found")
    if draft_kind == "edit":
        draft = await versions.get_edit_draft(draft_id)
        tasks = draft["content"]["stage7_tasks"]
    elif draft_kind == "new":
        draft = await drafts.get_draft(draft_id)
        tasks = parse_family_text(draft["family_txt"])["stage7_tasks"]
    else:
        raise ValueError("draft_kind must be new or edit")
    if draft["status"] != "draft" or draft["revision"] != expected_revision:
        raise ValueError("Draft deleted, published or revision changed")
    if type(task_index) is not int or not 0 <= task_index < len(tasks):
        raise ValueError("Stage 7 task index out of range")
    if tasks[task_index].get("task_id") != expected_task_id:
        raise ValueError("Task identity changed; supply its permanent ID, or null for a new task")
    result = await ops.update_task(draft_kind, draft_id, expected_revision, 7, task_index, {"audio": filename})
    return {**result, "asset_id": asset_id, "filename": filename}

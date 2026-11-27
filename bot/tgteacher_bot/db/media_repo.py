"""Shared image/audio catalog, revision-fenced attachment and publication validation."""
import re

from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.db import image_repo as images, tts_repo as audio
from tgteacher_bot.utils.content_validation import validate_family, ContentValidationError

DETACHED = {"image": "__detached__.jpg", "audio": "__detached__.mp3"}
KINDS = {"image": (5, images), "audio": (7, audio)}


async def validate_media(conn, content, legacy_directory=None):
    validate_family(content)
    legacy = {**content, "stage5_tasks": [], "stage7_tasks": []}
    for kind, (stage, module) in KINDS.items():
        key = f"stage{stage}_tasks"
        exists = images.image_exists if kind == "image" else audio.audio_exists
        for task in content.get(key, []):
            name = task[kind]
            if name == DETACHED[kind]:
                raise ContentValidationError("Detached media must be replaced: " + kind)
            if await exists(conn, name):
                continue
            if legacy_directory is None or module.managed_id(name):
                raise ContentValidationError("Missing managed or legacy media: " + name)
            legacy[key].append(task)
    if legacy_directory is not None:
        validate_family(legacy, media_dir=legacy_directory)


async def record_media_links(conn, family_id, version, content):
    await audio.record_audio_links(conn, family_id, version, content)
    await images.record_image_links(conn, family_id, version, content)


async def _task(draft_kind, draft_id, expected_revision, stage, task_index, expected_task_id):
    from tgteacher_bot.db import drafts_repo as drafts, content_versions as versions
    from tgteacher_bot.utils.family_parser import parse_family_text
    if draft_kind == "edit":
        draft = await versions.get_edit_draft(draft_id)
        content = draft["content"]
    elif draft_kind == "new":
        draft = await drafts.get_draft(draft_id)
        content = parse_family_text(draft["family_txt"])
    else:
        raise ValueError("draft_kind must be new or edit")
    if draft["status"] != "draft" or draft["revision"] != expected_revision:
        raise ValueError("Draft deleted, published or revision changed")
    tasks = content[f"stage{stage}_tasks"]
    if type(task_index) is not int or not 0 <= task_index < len(tasks):
        raise ValueError("Task index out of range")
    task = tasks[task_index]
    if task.get("task_id") != expected_task_id:
        raise ValueError("Task identity changed; supply permanent ID or null for new task")
    return task


async def attach_image(draft_kind, draft_id, expected_revision, task_index, asset_id, expected_task_id=None):
    from tgteacher_bot.db import content_operations as ops
    filename = images.asset_filename(asset_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        if not await images.image_exists(conn, filename):
            raise ValueError("Image asset not found")
    await _task(draft_kind,draft_id,expected_revision,5,task_index,expected_task_id)
    result = await ops.update_task(draft_kind,draft_id,expected_revision,5,task_index,{"image":filename})
    return {**result,"asset_id":asset_id,"filename":filename}


async def detach_media(kind, draft_kind, draft_id, expected_revision, task_index, expected_task_id=None):
    from tgteacher_bot.db import content_operations as ops
    if kind not in KINDS:
        raise ValueError("Invalid media kind")
    stage,_ = KINDS[kind]
    task = await _task(draft_kind,draft_id,expected_revision,stage,task_index,expected_task_id)
    if task[kind] == DETACHED[kind]:
        raise ValueError("Media already detached")
    result = await ops.update_task(draft_kind,draft_id,expected_revision,stage,task_index,{kind:DETACHED[kind]})
    return {**result,"detached_filename":task[kind],"requires_replacement":True}


async def list_media(kind="all", limit=30, offset=0):
    if kind not in ("all","image","audio") or not 1 <= limit <= 100 or offset < 0:
        raise ValueError("Invalid media kind or pagination")
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT * FROM (
                SELECT 'image' AS kind,asset_id,octet_length(jpeg) AS size_bytes,width,height,created_at FROM image_assets
                UNION ALL
                SELECT 'audio',asset_id,octet_length(mp3),NULL::integer,NULL::integer,created_at FROM audio_assets
            ) m WHERE ($1='all' OR kind=$1)
            ORDER BY created_at DESC,kind,asset_id LIMIT $2 OFFSET $3
        """,kind,limit+1,offset)
    return {"items":[_asset(r) for r in rows[:limit]],"next_offset":offset+limit if len(rows)>limit else None,
            "image_models":images.model_profiles(),"image_daily_limit_microusd":images.daily_limit()}


def _asset(row):
    return {**dict(row),"created_at":row["created_at"].isoformat(),
            "filename":KINDS[row["kind"]][1].asset_filename(row["asset_id"])}


async def get_media(kind, asset_id, limit=30, offset=0):
    if kind not in KINDS or not isinstance(asset_id,str) or not re.fullmatch(r"[0-9a-f]{64}",asset_id) or not 1 <= limit <= 100 or offset < 0:
        raise ValueError("Invalid asset identity or pagination")
    pool = await get_pool()
    # Identifiers are hard-coded, never derived from arbitrary caller input.
    table, blob, history, requests = ("image_assets","jpeg","family_image_versions","image_requests") if kind=="image" else ("audio_assets","mp3","family_audio_versions","tts_requests")
    dimensions = "width,height" if kind=="image" else "NULL::integer AS width,NULL::integer AS height"
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT $2::text AS kind,asset_id,octet_length({blob}) AS size_bytes,{dimensions},created_at FROM {table} WHERE asset_id=$1",asset_id,kind)
        if not row:
            raise ValueError("Media asset not found")
        links = await conn.fetch(f"SELECT family_id,version,task_id FROM {history} WHERE asset_id=$1 ORDER BY family_id,version,task_id LIMIT $2 OFFSET $3",asset_id,limit+1,offset)
        provenance = await conn.fetch(f"SELECT provider,model,reserved_microusd,cache_key FROM {requests} WHERE asset_id=$1 ORDER BY cache_key LIMIT 100",asset_id)
    return {**_asset(row),"stage":KINDS[kind][0],"versions":[dict(r) for r in links[:limit]],
            "next_offset":offset+limit if len(links)>limit else None,"generations":[dict(r) for r in provenance]}

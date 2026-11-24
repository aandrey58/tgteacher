"""Versioned published content. Exercise row IDs survive reorder, archive and rollback."""
import json
import uuid
from pathlib import Path

from tgteacher_bot.db.content_schema import STAGES, JSON_FIELDS, stage_key
from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.utils.content_validation import validate_family, family_identity, DuplicateFamilyError
from tgteacher_bot.core.paths import families_dir


class VersionConflict(ValueError):
    pass


def decode(value):
    return json.loads(value) if isinstance(value, str) else value


async def read_content(conn, family_id):
    meta = await conn.fetchrow("SELECT id, name, description, target, folder_name, archived_at, content_version FROM families WHERE id=$1", family_id)
    if not meta:
        raise ValueError("Family not found")
    content = {key: meta[key] for key in ("name", "description", "target")}
    content["target"] = content["target"] or "VIP+FREE"
    for stage, (table, order, fields) in STAGES.items():
        rows = await conn.fetch(f"SELECT id, {', '.join(fields)} FROM {table} WHERE family_id=$1 AND archived_at IS NULL ORDER BY {order}, id", family_id)
        content[stage_key(stage)] = [
            {"task_id": row["id"], **{k: (decode(row[k]) or [] if k in JSON_FIELDS else row[k]) for k in fields}}
            for row in rows
        ]
    return meta, content


async def record_initial_version(conn, family_id):
    meta, content = await read_content(conn, family_id)
    await conn.execute("""
        INSERT INTO family_versions(family_id, version, content) VALUES($1,$2,$3::jsonb)
        ON CONFLICT(family_id,version) DO NOTHING
    """, family_id, meta["content_version"], json.dumps(content, ensure_ascii=False))


async def get_current_content(family_id):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            meta, content = await read_content(conn, family_id)
            return {"id": family_id, "content_version": meta["content_version"],
                    "archived_at": meta["archived_at"].isoformat() if meta["archived_at"] else None, **content}


def clean_content(content):
    if not isinstance(content, dict) or len(json.dumps(content, ensure_ascii=False).encode()) > 256 * 1024:
        raise ValueError("Content must be a JSON object of at most 256 KiB")
    allowed = {"name", "description", "target", *(stage_key(n) for n in STAGES)}
    if set(content) - allowed:
        raise ValueError("Unexpected content fields; use only name, description, target and stage arrays")
    result = {key: content.get(key) for key in ("name", "description", "target")}
    result["target"] = result["target"] or "VIP+FREE"
    if result["description"] is not None and not isinstance(result["description"], str):
        raise ValueError("Description must be text")
    for stage, (_, _, fields) in STAGES.items():
        key = stage_key(stage)
        entries = content.get(key, [])
        if not isinstance(entries, list) or len(entries) > 500:
            raise ValueError(f"{key} must contain at most 500 entries")
        seen = set()
        result[key] = []
        for entry in entries:
            if not isinstance(entry, dict) or set(entry) - {"task_id", *fields}:
                raise ValueError(f"Invalid fields in {key}")
            item = {"task_id": entry.get("task_id")}
            ident = item["task_id"]
            if ident is not None:
                if type(ident) is not int or ident <= 0 or ident in seen:
                    raise ValueError(f"Invalid or duplicate task_id in {key}")
                seen.add(ident)
            for field in fields:
                value = entry.get(field, [] if field in JSON_FIELDS else None)
                if field in JSON_FIELDS:
                    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
                        raise ValueError(f"{field} must be an array of strings")
                elif value is not None and not isinstance(value, str):
                    raise ValueError(f"{field} must be text")
                item[field] = value
            result[key].append(item)
    if len(result["stage8_tasks"]) > 1:
        raise ValueError("The current Telegram stage 8 supports only one text task")
    for value in _strings(result):
        if any(c in value for c in ("<", ">", "\x00", "\r", "\n")):
            raise ValueError("Use plain single-line text without HTML; stage 8 uses literal backslash-n")
    if '"' in (result["name"] or ""):
        raise ValueError("Family name cannot contain double quotes")
    validate_family(result)
    return result


def _strings(obj):
    if isinstance(obj, str):
        yield obj
    elif isinstance(obj, dict):
        for v in obj.values():
            yield from _strings(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _strings(v)


# Only non-assessment fields may change in place. Everything else gets a
# fresh exercise row ID, so a changed answer can never inherit a correct result.
COSMETIC = {1: {"translation", "example", "example_translation", "hint"},
            **{n: {"explanation"} for n in range(2, 8)}, 8: {"explanations"}}


def plan_edit(old, new):
    plan = {}
    for stage, (_, _, fields) in STAGES.items():
        key = stage_key(stage)
        before = {entry["task_id"]: entry for entry in old[key]}
        keep, replace, add = [], [], []
        used = set()
        for position, item in enumerate(new[key]):
            ident = item["task_id"]
            if ident is None:
                add.append(position)
                continue
            if ident not in before:
                raise ValueError(f"Task {ident} does not belong to the current {key}")
            used.add(ident)
            if any(item[k] != before[ident][k] for k in fields if k not in COSMETIC[stage]):
                replace.append(ident)
            else:
                keep.append(ident)
        plan[key] = {"preserved_ids": keep, "replaced_ids": replace, "new_count": len(add),
                     "archived_ids": [ident for ident in before if ident not in used or ident in replace],
                     "order_changed": [item["task_id"] for item in old[key]] != [item["task_id"] for item in new[key]]}
    return plan


async def _validate_candidate(conn, meta, old, content):
    plan = plan_edit(old, content)
    identity = family_identity(content["name"], content["target"])
    rows = await conn.fetch("SELECT name,target FROM families WHERE id<>$1", meta["id"])
    if any(family_identity(r["name"], r["target"]) == identity for r in rows):
        raise DuplicateFamilyError("Group name and access tier already reserved")
    if content["stage5_tasks"] or content["stage7_tasks"]:
        folder = meta["folder_name"]
        if not folder or Path(folder).name != folder or folder in (".", ".."):
            raise ValueError("No media directory for this group")
        root = families_dir().resolve()
        path = (root / folder).resolve()
        if path.parent != root:
            raise ValueError("Invalid media directory")
        validate_family(content, media_dir=path)
    return plan


def _draft(row):
    return {"draft_id": str(row["id"]), "family_id": row["family_id"],
            "base_version": row["base_version"], "revision": row["revision"],
            "published_version": row["published_version"], "content": decode(row["content"])}


async def create_edit_draft(family_id, draft_id, expected_version):
    pool = await get_pool()
    ident = uuid.UUID(draft_id)
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(847231, 1)")
            previous = await conn.fetchrow("SELECT * FROM family_edit_drafts WHERE id=$1", ident)
            if previous:
                if previous["family_id"] != family_id or previous["base_version"] != expected_version:
                    raise VersionConflict("Draft UUID already used")
                return _draft(previous)
            meta, content = await read_content(conn, family_id)
            if meta["content_version"] != expected_version:
                raise VersionConflict("Family version changed")
            await record_initial_version(conn, family_id)
            row = await conn.fetchrow("""
                INSERT INTO family_edit_drafts(id,family_id,base_version,content)
                VALUES($1,$2,$3,$4::jsonb) RETURNING *
            """, ident, family_id, expected_version, json.dumps(content, ensure_ascii=False))
            return _draft(row)


async def get_edit_draft(draft_id):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM family_edit_drafts WHERE id=$1", uuid.UUID(draft_id))
        if not row:
            raise ValueError("Edit draft not found")
        return _draft(row)


async def update_edit_draft(draft_id, expected_revision, content):
    content = clean_content(content)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE family_edit_drafts SET content=$3::jsonb, revision=revision+1
            WHERE id=$1 AND revision=$2 AND published_version IS NULL RETURNING *
        """, uuid.UUID(draft_id), expected_revision, json.dumps(content, ensure_ascii=False))
        if not row:
            raise VersionConflict("Draft missing, published or revision changed")
        return _draft(row)


async def preview_edit(draft_id):
    draft = await get_edit_draft(draft_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            meta, old = await read_content(conn, draft["family_id"])
            if draft["published_version"] is not None or meta["content_version"] != draft["base_version"]:
                raise VersionConflict("Draft published or based on stale version")
            candidate = clean_content(draft["content"])
            plan = await _validate_candidate(conn, meta, old, candidate)
    return {"draft_id": draft_id, "revision": draft["revision"], "base_version": draft["base_version"],
            "valid": True, "changes": plan, "metadata_changed": any(old[k] != candidate[k] for k in ("name","description","target"))}


async def _snapshot_learners(conn, fid, version):
    await conn.execute("""
        INSERT INTO family_version_learner_history(family_id,version,user_id,progress,session_states,last_place)
        SELECT $1,$2,u.user_id,
          (SELECT to_jsonb(p) FROM user_family_progress p WHERE p.family_idx=$1 AND p.user_id=u.user_id),
          (SELECT jsonb_agg(to_jsonb(s) ORDER BY s.stage_num) FROM user_stage_state s WHERE s.family_idx=$1 AND s.user_id=u.user_id),
          (SELECT to_jsonb(l) FROM user_last_opened_family_place l WHERE l.family_idx=$1 AND l.user_id=u.user_id)
        FROM (
            SELECT user_id FROM user_family_progress WHERE family_idx=$1
            UNION SELECT user_id FROM user_task_progress WHERE family_idx=$1
            UNION SELECT user_id FROM user_stage_state WHERE family_idx=$1
            UNION SELECT user_id FROM user_last_opened_family_place WHERE family_idx=$1
        ) u ON CONFLICT DO NOTHING
    """, fid, version)


async def _write_tasks(conn, fid, stage, entries, reuse):
    table, order, fields = STAGES[stage]
    await conn.execute(f"UPDATE {table} SET archived_at=coalesce(archived_at,now()) WHERE family_id=$1 AND archived_at IS NULL", fid)
    for pos, item in enumerate(entries):
        ident = item["task_id"]
        values = [json.dumps(item[k], ensure_ascii=False) if k in JSON_FIELDS else item[k] for k in fields]
        if ident in reuse:
            assignments = ", ".join(f"{k}=${i+4}" + ("::jsonb" if k in JSON_FIELDS else "") for i,k in enumerate(fields))
            status = await conn.execute(f"UPDATE {table} SET {order}=$3, archived_at=NULL, {assignments} WHERE id=$1 AND family_id=$2", ident, fid, pos, *values)
            if status != "UPDATE 1":
                raise VersionConflict("Historical task identity missing")
        else:
            placeholders = ", ".join(f"${i+3}" + ("::jsonb" if k in JSON_FIELDS else "") for i,k in enumerate(fields))
            await conn.execute(f"INSERT INTO {table}(family_id,{order},{', '.join(fields)}) VALUES($1,$2,{placeholders})", fid, pos, *values)


async def _apply(conn, meta, old, content, plan=None, restored_from=None):
    fid, version = meta["id"], meta["content_version"]
    await record_initial_version(conn, fid)
    await _snapshot_learners(conn, fid, version)
    task_changes = any(old[stage_key(n)] != content[stage_key(n)] for n in STAGES)
    changed_sets = []
    for stage in STAGES:
        key = stage_key(stage)
        if old[key] == content[key]:
            continue
        reuse = set(plan[key]["preserved_ids"]) if plan is not None else {item["task_id"] for item in content[key]}
        await _write_tasks(conn, fid, stage, content[key], reuse)
        if set(item["task_id"] for item in old[key]) != reuse or (plan is not None and plan[key]["new_count"]):
            changed_sets.append(stage)
    await conn.execute("UPDATE families SET name=$2,description=$3,target=$4,content_version=content_version+1 WHERE id=$1",
                       fid, content["name"], content["description"], content["target"])
    if task_changes:
        # Shuffled display indices and old answer buttons cannot be reused.
        # Keep all task results and completion counts, archive old transient states.
        await conn.execute("DELETE FROM user_stage_state WHERE family_idx=$1", fid)
        for stage in changed_sets:
            await conn.execute(f"""
                UPDATE user_family_progress p SET stage{stage}_done=CASE
                    WHEN EXISTS (SELECT 1 FROM active_task_positions t WHERE t.family_idx=$1 AND t.stage_num=$2)
                     AND NOT EXISTS (SELECT 1 FROM active_task_positions t WHERE t.family_idx=$1 AND t.stage_num=$2
                       AND NOT EXISTS (SELECT 1 FROM user_task_progress a WHERE a.user_id=p.user_id
                         AND a.family_idx=t.family_idx AND a.stage_num=t.stage_num AND a.task_id=t.task_id AND a.is_done=1))
                    THEN 1 ELSE 0 END
                WHERE p.family_idx=$1
            """, fid, stage)
        if changed_sets:
            await conn.execute("""
                UPDATE user_family_progress p SET finished=0 WHERE family_idx=$1 AND EXISTS (
                    SELECT 1 FROM active_task_positions t WHERE t.family_idx=$1 AND NOT EXISTS (
                        SELECT 1 FROM user_task_progress a WHERE a.user_id=p.user_id AND a.family_idx=$1
                        AND a.stage_num=t.stage_num AND a.task_id=t.task_id AND a.is_done=1))
            """, fid)
        await conn.execute("""
            UPDATE user_last_opened_family_place l SET task_idx=0, stage_num=coalesce((
                SELECT min(t.stage_num) FROM active_task_positions t WHERE t.family_idx=$1 AND NOT EXISTS (
                    SELECT 1 FROM user_task_progress a WHERE a.user_id=l.user_id AND a.family_idx=$1
                    AND a.stage_num=t.stage_num AND a.task_id=t.task_id AND a.is_done=1)), 1)
            WHERE family_idx=$1
        """, fid)
    _, snapshot = await read_content(conn, fid)
    await conn.execute("""
        INSERT INTO family_versions(family_id,version,content,restored_from) VALUES($1,$2,$3::jsonb,$4)
    """, fid, version+1, json.dumps(snapshot, ensure_ascii=False), restored_from)
    return {"family_id": fid, "content_version": version+1, "restored_from": restored_from}


async def publish_edit(draft_id, expected_revision):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(847231, 1)")
            row = await conn.fetchrow("SELECT * FROM family_edit_drafts WHERE id=$1 FOR UPDATE", uuid.UUID(draft_id))
            if not row or row["revision"] != expected_revision:
                raise VersionConflict("Draft missing or revision changed")
            if row["published_version"] is not None:
                return {"family_id": row["family_id"], "content_version": row["published_version"], "restored_from": None}
            meta, old = await read_content(conn, row["family_id"])
            if meta["content_version"] != row["base_version"]:
                raise VersionConflict("Family version changed")
            content = clean_content(decode(row["content"]))
            plan = await _validate_candidate(conn, meta, old, content)
            result = await _apply(conn, meta, old, content, plan)
            await conn.execute("UPDATE family_edit_drafts SET published_version=$2 WHERE id=$1", row["id"], result["content_version"])
            return result


async def list_versions(family_id):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT version, created_at::text, restored_from FROM family_versions WHERE family_id=$1 ORDER BY version DESC", family_id)
        return [dict(r) for r in rows]


async def get_version(family_id, version):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT content, restored_from FROM family_versions WHERE family_id=$1 AND version=$2", family_id, version)
        if not row:
            raise ValueError("Version not found")
        return {"family_id": family_id, "version": version, "restored_from": row["restored_from"], "content": decode(row["content"])}


async def rollback_family(family_id, version, expected_version):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(847231, 1)")
            meta, old = await read_content(conn, family_id)
            if meta["content_version"] != expected_version:
                raise VersionConflict("Family version changed")
            snapshot = await conn.fetchval("SELECT content FROM family_versions WHERE family_id=$1 AND version=$2", family_id, version)
            if snapshot is None or version >= expected_version:
                raise ValueError("Select an earlier existing version")
            content = decode(snapshot)
            identity = family_identity(content["name"], content["target"])
            rows = await conn.fetch("SELECT name,target FROM families WHERE id<>$1", family_id)
            if any(family_identity(r["name"],r["target"]) == identity for r in rows):
                raise DuplicateFamilyError("Historical name and access tier are already reserved")
            return await _apply(conn, meta, old, content, restored_from=version)

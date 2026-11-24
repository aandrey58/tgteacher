"""Durable, revisioned lesson drafts. Published catalog rows are immutable here."""
import uuid
from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.db.tts_repo import validate_audio_files
from tgteacher_bot.db.families_repo import insert_family_on_connection
from tgteacher_bot.utils.family_parser import parse_family_text
from tgteacher_bot.utils.content_validation import validate_family, ContentValidationError, family_identity

MAX_DRAFT_BYTES = 128 * 1024


class DraftConflict(ValueError):
    pass


async def init_drafts(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS content_drafts (
            id UUID PRIMARY KEY,
            family_txt TEXT NOT NULL,
            revision INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            published_at TIMESTAMPTZ,
            family_id INTEGER REFERENCES families(id) ON DELETE SET NULL
        )
    """)
    await conn.execute("ALTER TABLE content_drafts ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ")


def check_source(source):
    if not isinstance(source, str) or not source.strip() or len(source.encode("utf-8")) > MAX_DRAFT_BYTES:
        raise ValueError("Draft must contain 1–131072 UTF-8 bytes")
    if "\x00" in source:
        raise ValueError("NUL is not allowed")
    return source


def parse_publishable(source):
    family = validate_family(parse_family_text(check_source(source)))
    # The current Telegram handlers interpolate content into HTML messages.
    if "<" in source or ">" in source:
        raise ContentValidationError("HTML markup is not supported in MCP lesson text")
    return family


def preview(source):
    try:
        family = parse_publishable(source)
    except (ValueError, IndexError, TypeError, KeyError) as exc:
        return {"valid": False, "publishable": False, "errors": [str(exc)]}
    media = bool(family.get("stage5_tasks") or family.get("stage7_tasks"))
    return {
        "valid": True, "publishable": not media,
        "errors": ["Media must be verified against stored assets"] if media else [],
        "name": family["name"], "target": family["target"],
        "counts": {"1": len(family["words"]), **{str(i): len(family[f"stage{i}_tasks"]) for i in range(2, 9)}},
    }


def serialize(row, include_text=True):
    result = {
        "draft_id": str(row["id"]), "revision": row["revision"],
        "status": "deleted" if row.get("deleted_at") else (("archived" if row.get("archived_at") else "published") if row["family_id"] is not None else "published_deleted")
                  if row["published_at"] else "draft",
        "family_id": row["family_id"],
        "updated_at": row["updated_at"].isoformat(),
    }
    if include_text:
        result["family_txt"] = row["family_txt"]
    return result


async def create_draft(draft_id, source):
    ident = uuid.UUID(draft_id)
    source = check_source(source)
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO content_drafts (id, family_txt) VALUES ($1, $2)
                ON CONFLICT (id) DO NOTHING
            """, ident, source)
            row = await conn.fetchrow("SELECT d.*, (SELECT archived_at FROM families WHERE id=d.family_id) AS archived_at FROM content_drafts d WHERE d.id=$1", ident)
            if row["deleted_at"] is not None or row["family_txt"] != source:
                raise DraftConflict("Draft ID already used with different content")
            return serialize(row)


async def get_draft(draft_id):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT d.*, (SELECT archived_at FROM families WHERE id=d.family_id) AS archived_at FROM content_drafts d WHERE d.id=$1", uuid.UUID(draft_id))
        if not row:
            raise ValueError("Draft not found")
        return serialize(row)


async def list_drafts(limit=30, offset=0):
    if not 1 <= limit <= 100 or offset < 0:
        raise ValueError("Invalid pagination")
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT d.*, (SELECT archived_at FROM families WHERE id=d.family_id) AS archived_at FROM content_drafts d WHERE d.deleted_at IS NULL ORDER BY updated_at DESC, id LIMIT $1 OFFSET $2", limit, offset)
        return [serialize(row, False) for row in rows]


async def update_draft(draft_id, expected_revision, source):
    source = check_source(source)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE content_drafts SET family_txt=$3, revision=revision+1, updated_at=now()
            WHERE id=$1 AND revision=$2 AND published_at IS NULL AND deleted_at IS NULL RETURNING *
        """, uuid.UUID(draft_id), expected_revision, source)
        if not row:
            raise DraftConflict("Draft missing, published, or revision changed")
        return serialize(row)


async def validate_draft(draft_id):
    draft = await get_draft(draft_id)
    report = preview(draft["family_txt"])
    if report["valid"] and draft["status"] == "draft":
        pool = await get_pool()
        async with pool.acquire() as conn:
            existing = await conn.fetch("SELECT name, target FROM families")
            try:
                await validate_audio_files(conn, parse_publishable(draft["family_txt"]))
                report["errors"] = []
                report["publishable"] = True
            except ContentValidationError as exc:
                report["errors"] = [str(exc)]
                report["publishable"] = False
        identity = family_identity(report["name"], report["target"])
        if any(family_identity(row["name"], row["target"]) == identity for row in existing):
            report["errors"].append("A group with this name and access level already exists")
            report["publishable"] = False
    if draft["status"] != "draft":
        report["publishable"] = False
        report["errors"].append("Draft deleted or already published; create a new draft or restore the group")
    return {**report, "draft_id": draft_id, "revision": draft["revision"]}


async def publish_draft(draft_id, expected_revision):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("SELECT d.*, (SELECT archived_at FROM families WHERE id=d.family_id) AS archived_at FROM content_drafts d WHERE d.id=$1 FOR UPDATE", uuid.UUID(draft_id))
            if not row:
                raise ValueError("Draft not found")
            if row["deleted_at"] is not None:
                raise DraftConflict("Draft was deleted")
            if row["revision"] != expected_revision:
                raise DraftConflict("Draft revision changed; validate the current revision first")
            if row["published_at"]:
                return serialize(row, False)
            family = parse_publishable(row["family_txt"])
            await validate_audio_files(conn, family)
            family_id = await insert_family_on_connection(conn, family)
            row = await conn.fetchrow("""
                UPDATE content_drafts SET published_at=now(), updated_at=now(), family_id=$2
                WHERE id=$1 RETURNING *
            """, row["id"], family_id)
            return serialize(row, False)


async def get_published_source(family_id):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval("SELECT family_txt FROM content_drafts WHERE family_id=$1", family_id)


async def delete_draft(draft_id, expected_revision):
    """Tombstone an unpublished draft; reserve its UUID and keep retry safety."""
    ident = uuid.UUID(draft_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("SELECT * FROM content_drafts WHERE id=$1 FOR UPDATE", ident)
            if not row or row["published_at"] is not None:
                raise DraftConflict("Draft missing or already published")
            if row["deleted_at"] is not None:
                if row["revision"] != expected_revision + 1:
                    raise DraftConflict("Draft revision changed")
                return serialize(row, False)
            if row["revision"] != expected_revision:
                raise DraftConflict("Draft revision changed")
            row = await conn.fetchrow("""
                UPDATE content_drafts SET deleted_at=now(), updated_at=now(), revision=revision+1
                WHERE id=$1 RETURNING *
            """, ident)
            return serialize(row, False)

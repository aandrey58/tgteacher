"""TGTeacher content MCP — Streamable HTTP, no Telegram or provider keys needed."""
import os
from urllib.parse import urlsplit
from typing import Any, Literal

import uvicorn
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from mcp.server.auth.settings import AuthSettings
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations

from tgteacher_bot.db import drafts_repo as drafts, families_repo as families, content_versions as versions, content_operations as operations
from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.db.user_repo import init_db_pg
from tgteacher_bot.mcp_auth import READ, EDIT, PUBLISH, require_scope, verifier_from_env


def _page(limit, offset):
    if not 1 <= limit <= 100 or offset < 0:
        raise ValueError("limit must be 1–100 and offset nonnegative")


def build_server(verifier, resource="http://127.0.0.1:8000/mcp", issuer="http://127.0.0.1:8000"):
    parsed = urlsplit(resource)
    if parsed.path != "/mcp" or parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("Resource URL must end in /mcp")
    if parsed.scheme == "http" and parsed.hostname not in ("localhost", "127.0.0.1"):
        raise ValueError("HTTPS is required for non-local MCP resources")
    origin = f"{parsed.scheme}://{parsed.netloc}"
    server = FastMCP(
        "TGTeacher",
        instructions="Manage English lesson content. Drafts are not visible to learners. Validate the current revision before requesting approval to publish. Never publish edits, roll back, archive or restore without the user's explicit instruction. Preview edit drafts before requesting approval. Archived groups retain all learner progress.",
        stateless_http=True, json_response=True,
        token_verifier=verifier,
        auth=AuthSettings(issuer_url=issuer, resource_server_url=resource, required_scopes=[READ], validate_token_resource=True),
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=[parsed.netloc, "127.0.0.1:*", "localhost:*"],
            allowed_origins=[origin, "http://127.0.0.1:*", "http://localhost:*"],
        ),
    )
    read = ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=False)
    write = ToolAnnotations(readOnlyHint=False, destructiveHint=False, openWorldHint=False)

    @server.tool(annotations=read)
    async def list_families(limit: int = 30, offset: int = 0, status: str = 'active') -> dict[str, Any]:
        """List published groups: status active, archived, or all."""
        require_scope(READ)
        _page(limit, offset)
        pool = await get_pool()
        async with pool.acquire() as conn:
            if status not in ("active", "archived", "all"):
                raise ValueError("status must be active, archived or all")
            rows = await conn.fetch("""
                SELECT id, name, description, target, archived_at::text AS archived_at FROM families
                WHERE $3='all' OR ($3='active' AND archived_at IS NULL)
                    OR ($3='archived' AND archived_at IS NOT NULL)
                ORDER BY id DESC LIMIT $1 OFFSET $2
            """, limit + 1, offset, status)
        return {"items": [dict(row) for row in rows[:limit]], "next_offset": offset + limit if len(rows) > limit else None}

    @server.tool(annotations=read)
    async def get_family(family_id: int) -> dict[str, Any]:
        """Read one published group and all eight exercise sets."""
        require_scope(READ)
        return await versions.get_current_content(family_id)

    @server.tool(annotations=read)
    async def search_words(query: str, limit: int = 30, offset: int = 0, include_archived: bool = False) -> dict[str, Any]:
        """Search existing English words and Russian translations by substring."""
        require_scope(READ)
        _page(limit, offset)
        if not query.strip() or len(query) > 200:
            raise ValueError("Query must contain 1–200 characters")
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT f.id AS family_id, f.name AS family_name, w.word, w.translation, (f.archived_at IS NOT NULL) AS archived
                FROM stage1_words w JOIN families f ON f.id=w.family_id
                WHERE w.archived_at IS NULL AND ($4::boolean OR f.archived_at IS NULL) AND (strpos(lower(w.word), lower($1)) > 0
                   OR strpos(lower(coalesce(w.translation,'')), lower($1)) > 0)
                ORDER BY lower(w.word), f.id, w.word_order LIMIT $2 OFFSET $3
            """, query.strip(), limit + 1, offset, include_archived)
        return {"items": [dict(row) for row in rows[:limit]], "next_offset": offset + limit if len(rows) > limit else None}

    @server.tool(annotations=read)
    async def get_content_format() -> dict[str, Any]:
        """Get the actual TGTeacher TXT template and authoring rules."""
        require_scope(READ)
        from tgteacher_bot.core.paths import families_dir
        return {"format": (families_dir() / "family_example.txt").read_text(encoding="utf-8"),
                "note": "All stages except stage 1 are optional. Media publication requires later integration."}

    @server.tool(annotations=write)
    async def create_draft(draft_id: str, family_txt: str) -> dict[str, Any]:
        """Create a private draft. Supply a new UUID; reusing it with identical text is idempotent."""
        require_scope(EDIT)
        return await drafts.create_draft(draft_id, family_txt)

    @server.tool(annotations=read)
    async def list_drafts(limit: int = 30, offset: int = 0) -> dict[str, Any]:
        """List draft metadata without lesson text."""
        require_scope(READ)
        return {"items": await drafts.list_drafts(limit, offset)}

    @server.tool(annotations=read)
    async def get_draft(draft_id: str) -> dict[str, Any]:
        """Read full draft text, status and current revision."""
        require_scope(READ)
        return await drafts.get_draft(draft_id)

    @server.tool(annotations=write)
    async def update_draft(draft_id: str, expected_revision: int, family_txt: str) -> dict[str, Any]:
        """Replace the text of an unpublished draft if its revision still matches."""
        require_scope(EDIT)
        return await drafts.update_draft(draft_id, expected_revision, family_txt)

    @server.tool(annotations=read)
    async def validate_draft(draft_id: str) -> dict[str, Any]:
        """Validate current draft, report exercise counts, duplicates and publication blockers."""
        require_scope(READ)
        return await drafts.validate_draft(draft_id)

    @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False))
    async def publish_draft(draft_id: str, expected_revision: int, confirm: bool) -> dict[str, Any]:
        """Publish a validated revision to the learner catalog. Requires explicit user approval and publish scope."""
        require_scope(PUBLISH)
        if not confirm:
            raise ValueError("Publication requires explicit confirmation")
        return await drafts.publish_draft(draft_id, expected_revision)

    @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False))
    async def archive_family(family_id: int, confirm: bool) -> dict[str, Any]:
        """Remove a group from the learner catalog; preserve tasks, media and all progress."""
        require_scope(PUBLISH)
        if not confirm:
            raise ValueError("Archiving requires explicit confirmation")
        return await families.set_family_archived_pg(family_id, True)

    @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=False))
    async def restore_family(family_id: int, confirm: bool) -> dict[str, Any]:
        """Restore the same archived group ID and all existing learner progress."""
        require_scope(PUBLISH)
        if not confirm:
            raise ValueError("Restoration requires explicit confirmation")
        return await families.set_family_archived_pg(family_id, False)

    @server.tool(annotations=write)
    async def create_edit_draft(family_id: int, draft_id: str, expected_version: int) -> dict[str, Any]:
        """Clone a published group's current JSON content with permanent stage-local task IDs into a private edit draft."""
        require_scope(EDIT)
        return await versions.create_edit_draft(family_id, draft_id, expected_version)

    @server.tool(annotations=read)
    async def list_edit_drafts(family_id: int | None = None, limit: int = 30, offset: int = 0) -> dict[str, Any]:
        """List edit drafts across all groups or filter by family ID."""
        require_scope(READ)
        return await versions.list_edit_drafts(family_id, limit, offset)

    @server.tool(annotations=read)
    async def get_edit_draft(draft_id: str) -> dict[str, Any]:
        """Read an edit draft including its current revision and complete content."""
        require_scope(READ)
        return await versions.get_edit_draft(draft_id)

    @server.tool(annotations=write)
    async def update_edit_draft(draft_id: str, expected_revision: int, content: dict[str, Any]) -> dict[str, Any]:
        """Replace draft content. Keep task_id for edits/reorders; omit task_id for new tasks; omit rows to archive."""
        require_scope(EDIT)
        return await versions.update_edit_draft(draft_id, expected_revision, content)

    @server.tool(annotations=read)
    async def preview_edit(draft_id: str) -> dict[str, Any]:
        """Validate draft and return preserved, archived, replaced and new exercise IDs before approval."""
        require_scope(READ)
        return await versions.preview_edit(draft_id)

    @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False))
    async def publish_edit(draft_id: str, expected_revision: int, confirm: bool) -> dict[str, Any]:
        """Publish the reviewed revision; preserve unaffected progress, archive superseded tasks. Requires explicit approval."""
        require_scope(PUBLISH)
        if not confirm:
            raise ValueError("Publication requires explicit confirmation")
        return await versions.publish_edit(draft_id, expected_revision)

    @server.tool(annotations=read)
    async def list_family_versions(family_id: int) -> dict[str, Any]:
        """List immutable published versions."""
        require_scope(READ)
        return {"items": await versions.list_versions(family_id)}

    @server.tool(annotations=read)
    async def get_family_version(family_id: int, version: int) -> dict[str, Any]:
        """Read an immutable version, including original task IDs."""
        require_scope(READ)
        return await versions.get_version(family_id, version)

    @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=False))
    async def rollback_family(family_id: int, version: int, expected_version: int, confirm: bool) -> dict[str, Any]:
        """Restore old content and exercise IDs as a new version, without deleting newer progress."""
        require_scope(PUBLISH)
        if not confirm:
            raise ValueError("Rollback requires explicit confirmation")
        return await versions.rollback_family(family_id, version, expected_version)

    destructive_draft = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=False)

    @server.tool(annotations=destructive_draft)
    async def delete_draft(draft_id: str, expected_revision: int, confirm: bool) -> dict[str, Any]:
        """Discard an unpublished new-group draft. Published lessons and learner progress are untouched."""
        require_scope(EDIT)
        if not confirm:
            raise ValueError("Draft deletion requires explicit confirmation")
        return await drafts.delete_draft(draft_id, expected_revision)

    @server.tool(annotations=destructive_draft)
    async def delete_edit_draft(draft_id: str, expected_revision: int, confirm: bool) -> dict[str, Any]:
        """Discard an unpublished edit draft. The original published group and its versions remain unchanged."""
        require_scope(EDIT)
        if not confirm:
            raise ValueError("Draft deletion requires explicit confirmation")
        return await versions.delete_edit_draft(draft_id, expected_revision)

    @server.tool(annotations=write)
    async def add_tasks(draft_kind: Literal["new", "edit"], draft_id: str, expected_revision: int, stage: int, tasks: list[dict[str, Any]], insert_at: int | None = None) -> dict[str, Any]:
        """Append or insert 1–100 tasks in a draft stage. Stage 1 is words. Zero-based insert_at; omit task_id. Use the returned revision for the next operation."""
        require_scope(EDIT)
        return await operations.add_tasks(draft_kind, draft_id, expected_revision, stage, tasks, insert_at)

    @server.tool(annotations=write)
    async def update_task(draft_kind: Literal["new", "edit"], draft_id: str, expected_revision: int, stage: int, task_index: int, patch: dict[str, Any]) -> dict[str, Any]:
        """Patch fields of one draft task at a zero-based position in the expected revision. The task's permanent ID cannot be changed."""
        require_scope(EDIT)
        return await operations.update_task(draft_kind, draft_id, expected_revision, stage, task_index, patch)

    @server.tool(annotations=ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=False, openWorldHint=False))
    async def archive_task(draft_kind: Literal["new", "edit"], draft_id: str, expected_revision: int, stage: int, task_index: int, confirm: bool) -> dict[str, Any]:
        """Remove a task from the draft only. On later edit publication, old task IDs and learner answers are archived, never erased."""
        require_scope(EDIT)
        if not confirm:
            raise ValueError("Task removal requires explicit confirmation")
        return await operations.archive_task(draft_kind, draft_id, expected_revision, stage, task_index)

    @server.tool(annotations=write)
    async def reorder_tasks(draft_kind: Literal["new", "edit"], draft_id: str, expected_revision: int, stage: int, order: list[int]) -> dict[str, Any]:
        """Reorder one draft stage using a complete permutation of zero-based positions in the expected revision. Existing IDs are preserved."""
        require_scope(EDIT)
        return await operations.reorder_tasks(draft_kind, draft_id, expected_revision, stage, order)

    return server


def main():
    load_dotenv()
    resource = os.getenv("TGTEACHER_MCP_RESOURCE_URL", "http://127.0.0.1:8000/mcp")
    issuer = os.getenv("TGTEACHER_MCP_ISSUER_URL", "http://127.0.0.1:8000")
    server = build_server(verifier_from_env(resource), resource, issuer)
    app = server.streamable_http_app()
    # Run database initialization in the same event loop as the server.
    from contextlib import asynccontextmanager
    previous = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        await init_db_pg()
        try:
            async with previous(application):
                yield
        finally:
            from tgteacher_bot.db import pool
            if pool._pool is not None:
                await pool._pool.close()
                pool._pool = None

    app.router.lifespan_context = lifespan
    uvicorn.run(app, host=os.getenv("TGTEACHER_MCP_HOST", "127.0.0.1"),
                port=int(os.getenv("TGTEACHER_MCP_PORT", "8000")), proxy_headers=False)


if __name__ == "__main__":
    main()

"""TGTeacher content MCP — Streamable HTTP, no Telegram or provider keys needed."""
import os
from urllib.parse import urlsplit
from typing import Any

import uvicorn
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
from mcp.server.auth.settings import AuthSettings
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations

from tgteacher_bot.db import drafts_repo as drafts, families_repo as families
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
        instructions="Manage English lesson content. Drafts are not visible to learners. Validate the current revision before requesting approval to publish. Never publish, archive or restore without the user's explicit instruction. Archived groups retain all learner progress.",
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
        family = await families.get_family_data_pg(family_id, include_archived=True)
        if not family:
            raise ValueError("Family not found")
        family.pop("folder_name", None)
        family["words"] = await families.get_stage1_words_pg(family_id)
        for stage in range(2, 9):
            family[f"stage{stage}_tasks"] = await getattr(families, f"get_stage{stage}_tasks_pg")(family_id)
        return family

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
                WHERE ($4::boolean OR f.archived_at IS NULL) AND (strpos(lower(w.word), lower($1)) > 0
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

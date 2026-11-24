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
from tgteacher_bot.db import tts_repo as tts
from tgteacher_bot.db.user_repo import init_db_pg
from tgteacher_bot.mcp_auth import READ, EDIT, PUBLISH, require_scope, verifier_from_env, LearnerTokenVerifier, require_learner


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
        token_verifier=LearnerTokenVerifier(verifier, resource),
        auth=AuthSettings(issuer_url=issuer, resource_server_url=resource, required_scopes=[], validate_token_resource=True),
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
                "note": "All stages except stage 1 are optional. Stage 7 audio can be generated and attached; new images require Stage 5."}

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

    @server.tool(annotations=read)
    async def list_tts_providers() -> dict[str, Any]:
        """List configured providers, conservative byte-based rates and daily budget; no secrets."""
        require_scope(READ)
        return await tts.discover_providers()

    @server.tool(annotations=read)
    async def list_voices(provider: str, model: str | None = None) -> dict[str, Any]:
        """Discover model voices or use administrator-configured voices; specify model when multiple exist."""
        require_scope(READ)
        return await tts.discover_voices(provider, model)

    @server.tool(annotations=read)
    async def estimate_tts_cost(items: list[dict[str, str]]) -> dict[str, Any]:
        """Estimate incremental cost in integer micro-USD for 1–10 provider/model/voice/text requests; check cache and daily budget."""
        require_scope(READ)
        return await tts.estimate(items)

    paid = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=True)

    @server.tool(annotations=paid)
    async def generate_audio(provider: str, voice: str, text: str, max_cost_microusd: int, confirm: bool, model: str | None = None) -> dict[str, Any]:
        """Generate or reuse one immutable MP3. Requires user approval and maximum approved cost in micro-USD. Failed/pending requests are not retried."""
        require_scope(EDIT)
        if not confirm:
            raise ValueError("Audio generation requires explicit confirmation")
        item = {"provider": provider, "voice": voice, "text": text}
        if model is not None:
            item["model"] = model
        return await tts.generate_batch([item], max_cost_microusd)

    @server.tool(annotations=paid)
    async def batch_generate_audio(items: list[dict[str, str]], max_cost_microusd: int, confirm: bool) -> dict[str, Any]:
        """Generate 1–10 MP3 assets. Reserve the entire batch budget atomically before contacting providers. Explicit approval required."""
        require_scope(EDIT)
        if not confirm:
            raise ValueError("Audio generation requires explicit confirmation")
        return await tts.generate_batch(items, max_cost_microusd)

    @server.tool(annotations=write)
    async def attach_audio(draft_kind: Literal["new", "edit"], draft_id: str, expected_revision: int, task_index: int, asset_id: str, expected_task_id: int | None = None) -> dict[str, Any]:
        """Attach an existing immutable audio asset to a Stage 7 draft task. Existing tasks require their permanent ID; new tasks use null. Publish separately."""
        require_scope(EDIT)
        return await tts.attach_audio(draft_kind, draft_id, expected_revision, task_index, asset_id, expected_task_id)

    @server.tool(annotations=paid)
    async def generate_image(provider: str, model: str, prompt: str, max_cost_microusd: int = 0, confirm: bool = False, estimate_only: bool = True) -> dict[str, Any]:
        """Estimate or generate one image with an explicitly configured model. Default is dry run; paid generation requires approval and a cost ceiling."""
        from tgteacher_bot.db import image_repo
        require_scope(READ if estimate_only else EDIT)
        if not estimate_only and not confirm:
            raise ValueError("Image generation requires explicit confirmation")
        return await image_repo.generate_batch([{"provider": provider, "model": model, "prompt": prompt}], max_cost_microusd, estimate_only)

    @server.tool(annotations=paid)
    async def batch_generate_images(items: list[dict[str, str]], max_cost_microusd: int = 0, confirm: bool = False, estimate_only: bool = True) -> dict[str, Any]:
        """Estimate or generate 1–10 images, reserving the entire cost atomically before paid requests."""
        from tgteacher_bot.db import image_repo
        require_scope(READ if estimate_only else EDIT)
        if not estimate_only and not confirm:
            raise ValueError("Image generation requires explicit confirmation")
        return await image_repo.generate_batch(items, max_cost_microusd, estimate_only)

    @server.tool(annotations=write)
    async def attach_image(draft_kind: Literal["new", "edit"], draft_id: str, expected_revision: int, task_index: int, asset_id: str, expected_task_id: int | None = None) -> dict[str, Any]:
        """Attach immutable generated image to a Stage 5 draft task; specify stable ID for existing tasks. Publish separately."""
        from tgteacher_bot.db import media_repo
        require_scope(EDIT)
        return await media_repo.attach_image(draft_kind, draft_id, expected_revision, task_index, asset_id, expected_task_id)

    @server.tool(annotations=read)
    async def list_media(kind: Literal["all", "image", "audio"] = "all", limit: int = 30, offset: int = 0) -> dict[str, Any]:
        """List generated audio/image metadata, configured image models and budget. No binary payloads."""
        from tgteacher_bot.db import media_repo
        require_scope(READ)
        return await media_repo.list_media(kind, limit, offset)

    @server.tool(annotations=read)
    async def get_media(kind: Literal["image", "audio"], asset_id: str, limit: int = 30, offset: int = 0) -> dict[str, Any]:
        """Inspect immutable media metadata, generation provenance and paginated published version links."""
        from tgteacher_bot.db import media_repo
        require_scope(READ)
        return await media_repo.get_media(kind, asset_id, limit, offset)

    @server.tool(annotations=write)
    async def detach_media(kind: Literal["image", "audio"], draft_kind: Literal["new", "edit"], draft_id: str, expected_revision: int, task_index: int, confirm: bool, expected_task_id: int | None = None) -> dict[str, Any]:
        """Detach media from a draft only. Asset and history are retained; publication is blocked until replacement or removal of the task."""
        from tgteacher_bot.db import media_repo
        require_scope(EDIT)
        if not confirm:
            raise ValueError("Detachment requires explicit confirmation")
        return await media_repo.detach_media(kind, draft_kind, draft_id, expected_revision, task_index, expected_task_id)

    @server.tool(annotations=read)
    async def get_learning_progress(limit: int = 30, offset: int = 0) -> dict[str, Any]:
        """Read only the authenticated learner's group progress. Requires a Telegram-linked learner credential."""
        from tgteacher_bot.db.srs_repo import learning_progress
        return await learning_progress(require_learner(),limit,offset)

    @server.tool(annotations=read)
    async def get_learning_statistics(days: int = 30) -> dict[str, Any]:
        """Read own dated lesson attempts, FSRS ratings and review counts; old undated records are reported separately."""
        from tgteacher_bot.db.srs_repo import learning_statistics
        return await learning_statistics(require_learner(),days)

    @server.tool(annotations=read)
    async def get_weak_words(limit: int = 30, offset: int = 0) -> dict[str, Any]:
        """Read own difficult vocabulary based on explicit recall ratings and FSRS difficulty."""
        from tgteacher_bot.db.srs_repo import weak_words
        return await weak_words(require_learner(),limit,offset)

    @server.tool(annotations=read)
    async def get_due_reviews(limit: int = 30, offset: int = 0) -> dict[str, Any]:
        """Read own currently due accessible vocabulary cards. Does not record a review."""
        from tgteacher_bot.db.srs_repo import due_reviews
        return await due_reviews(require_learner(),limit,offset)

    @server.tool(annotations=read)
    async def get_learner_profile() -> dict[str, Any]:
        """Read own preferences, review goal, due workload and evidence-based study recommendations. CEFR is self-assessed."""
        from tgteacher_bot.db.learner_preferences import profile
        return await profile(require_learner())

    @server.tool(annotations=write)
    async def update_learning_preferences(expected_revision: int, patch: dict[str, Any], confirm: bool) -> dict[str, Any]:
        """Update only own preferences with explicit consent and revision check. Fields: daily_review_goal (5–200), desired_retention (0.80–0.97), review_order (due/weak_first), self_assessed_level (A1–C2/unspecified). New retention applies at subsequent reviews."""
        from tgteacher_bot.db.learner_preferences import update_preferences
        uid = require_learner(write=True)
        if not confirm:
            raise ValueError("Preference changes require confirmation")
        return await update_preferences(uid,expected_revision,patch)

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

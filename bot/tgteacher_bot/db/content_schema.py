"""Stable exercise identities and idempotent migration from positional progress."""
STAGES = {
    1: ("stage1_words", "word_order", ("word", "translation", "example", "example_translation", "hint")),
    2: ("stage2_tasks", "task_order", ("sentence", "answer", "choices", "explanation")),
    3: ("stage3_tasks", "task_order", ("word", "definition", "choices", "explanation")),
    4: ("stage4_tasks", "task_order", ("sentence", "answer", "choices", "explanation")),
    5: ("stage5_tasks", "task_order", ("image", "answer", "alternatives", "explanation")),
    6: ("stage6_tasks", "task_order", ("word", "synonyms", "wrong_synonyms", "explanation")),
    7: ("stage7_tasks", "task_order", ("task", "audio", "answer", "choices", "explanation")),
    8: ("stage8_tasks", "task_order", ("text", "answers", "explanations", "equal")),
}
JSON_FIELDS = {"choices", "alternatives", "synonyms", "wrong_synonyms", "answers", "explanations"}


def stage_key(stage):
    return "words" if stage == 1 else f"stage{stage}_tasks"


async def migrate_content(conn):
    # All DDL and migration changes commit together, including the marker.
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(847231, 1)")
        await conn.execute("ALTER TABLE families ADD COLUMN IF NOT EXISTS content_version INTEGER NOT NULL DEFAULT 1")
        for table, _, _ in STAGES.values():
            await conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ")
        parts = []
        for stage, (table, order, _) in STAGES.items():
            parts.append(f"SELECT family_id AS family_idx, {stage} AS stage_num, id AS task_id, "
                         f"(row_number() OVER (PARTITION BY family_id ORDER BY {order}, id)-1)::integer AS task_idx "
                         f"FROM {table} WHERE archived_at IS NULL")
        await conn.execute("CREATE OR REPLACE VIEW active_task_positions AS " + " UNION ALL ".join(parts))
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS content_schema_migrations (name TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS legacy_unmapped_progress (
                user_id BIGINT NOT NULL, family_idx INTEGER NOT NULL, stage_num INTEGER NOT NULL,
                task_idx INTEGER NOT NULL, original_row JSONB NOT NULL,
                PRIMARY KEY(user_id, family_idx, stage_num, task_idx)
            );
            CREATE TABLE IF NOT EXISTS family_versions (
                family_id INTEGER NOT NULL REFERENCES families(id) ON DELETE CASCADE,
                version INTEGER NOT NULL, content JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(), restored_from INTEGER,
                PRIMARY KEY(family_id, version)
            );
            CREATE TABLE IF NOT EXISTS family_edit_drafts (
                id UUID PRIMARY KEY, family_id INTEGER NOT NULL REFERENCES families(id) ON DELETE CASCADE,
                base_version INTEGER NOT NULL, revision INTEGER NOT NULL DEFAULT 1, content JSONB NOT NULL,
                published_version INTEGER, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE TABLE IF NOT EXISTS family_version_learner_history (
                family_id INTEGER NOT NULL REFERENCES families(id) ON DELETE CASCADE,
                version INTEGER NOT NULL, user_id BIGINT NOT NULL, progress JSONB,
                session_states JSONB, last_place JSONB,
                PRIMARY KEY(family_id, version, user_id)
            );
        """)
        await conn.execute("ALTER TABLE family_edit_drafts ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ")
        await conn.execute("ALTER TABLE family_version_learner_history ADD COLUMN IF NOT EXISTS task_progress JSONB")
        if not await conn.fetchval("SELECT 1 FROM content_schema_migrations WHERE name='stable_progress_v1'"):
            await conn.execute("ALTER TABLE user_task_progress ADD COLUMN IF NOT EXISTS task_id INTEGER")
            await conn.execute("""
                UPDATE user_task_progress p SET task_id=t.task_id FROM active_task_positions t
                WHERE p.family_idx=t.family_idx AND p.stage_num=t.stage_num AND p.task_idx=t.task_idx
            """)
            # Preserve unmatched legacy records verbatim; never guess an exercise identity.
            await conn.execute("""
                INSERT INTO legacy_unmapped_progress
                SELECT user_id, family_idx, stage_num, task_idx, to_jsonb(p)
                FROM user_task_progress p WHERE task_id IS NULL ON CONFLICT DO NOTHING;
                DELETE FROM user_task_progress WHERE task_id IS NULL;
                ALTER TABLE user_task_progress DROP CONSTRAINT user_task_progress_pkey;
                ALTER TABLE user_task_progress ALTER COLUMN task_id SET NOT NULL;
                ALTER TABLE user_task_progress ADD PRIMARY KEY(user_id, family_idx, stage_num, task_id);
                INSERT INTO content_schema_migrations(name) VALUES ('stable_progress_v1');
            """)
        await conn.execute("""
            CREATE OR REPLACE VIEW active_user_task_progress AS
            SELECT p.user_id, p.family_idx, p.stage_num, t.task_idx, p.is_done,
                   p.selected_choice_text, p.is_correct, p.task_id
            FROM user_task_progress p JOIN active_task_positions t
            USING (family_idx, stage_num, task_id)
        """)

        # Snapshot every pre-existing group exactly once, without changing its IDs.
        from tgteacher_bot.db.content_versions import record_initial_version
        missing = await conn.fetch("SELECT id FROM families f WHERE NOT EXISTS (SELECT 1 FROM family_versions v WHERE v.family_id=f.id AND v.version=f.content_version)")
        for row in missing:
            await record_initial_version(conn, row["id"])

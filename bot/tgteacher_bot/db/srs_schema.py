"""Idempotent SRS schema. Legacy completion is not a fabricated recall rating."""


async def init_srs(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS srs_cards (
            user_id BIGINT NOT NULL, family_id INTEGER NOT NULL REFERENCES families(id) ON DELETE CASCADE,
            task_id INTEGER NOT NULL REFERENCES stage1_words(id) ON DELETE CASCADE,
            card JSONB, due TIMESTAMPTZ NOT NULL DEFAULT now(), revision INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(user_id,family_id,task_id)
        );
        CREATE INDEX IF NOT EXISTS srs_due_idx ON srs_cards(user_id,due);
        CREATE TABLE IF NOT EXISTS srs_reviews (
            id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, family_id INTEGER NOT NULL,
            task_id INTEGER NOT NULL, reviewed_at TIMESTAMPTZ NOT NULL, rating INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 4),
            review_log JSONB NOT NULL, before_card JSONB NOT NULL, after_card JSONB NOT NULL,
            FOREIGN KEY(user_id,family_id,task_id) REFERENCES srs_cards(user_id,family_id,task_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS srs_reviews_user_idx ON srs_reviews(user_id,reviewed_at);
        CREATE TABLE IF NOT EXISTS learner_answer_events (
            id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, family_id INTEGER NOT NULL,
            stage_num INTEGER NOT NULL, task_id INTEGER NOT NULL, answered_at TIMESTAMPTZ,
            is_correct INTEGER, source TEXT NOT NULL CHECK(source IN ('legacy','lesson')),
            CHECK(is_correct IN (0,1))
        );
        CREATE INDEX IF NOT EXISTS learner_answer_events_user_idx ON learner_answer_events(user_id,answered_at);
        CREATE TABLE IF NOT EXISTS srs_review_tickets (
            nonce UUID PRIMARY KEY, user_id BIGINT NOT NULL, family_id INTEGER NOT NULL, task_id INTEGER NOT NULL,
            card_revision INTEGER NOT NULL, family_version INTEGER NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL, revealed BOOLEAN NOT NULL DEFAULT false,
            FOREIGN KEY(user_id,family_id,task_id) REFERENCES srs_cards(user_id,family_id,task_id) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS srs_one_ticket_per_user ON srs_review_tickets(user_id);
        CREATE TABLE IF NOT EXISTS learner_tokens (
            token_hash TEXT PRIMARY KEY, user_id BIGINT NOT NULL UNIQUE REFERENCES users(user_id) ON DELETE CASCADE,
            expires_at TIMESTAMPTZ NOT NULL
        );
    """)
    from tgteacher_bot.db.learner_preferences import init_preferences
    await init_preferences(conn)
    if not await conn.fetchval("SELECT 1 FROM content_schema_migrations WHERE name='srs_v1'"):
        await conn.execute("""
            INSERT INTO srs_cards(user_id,family_id,task_id)
            SELECT p.user_id,p.family_idx,p.task_id FROM user_task_progress p
            JOIN stage1_words w ON w.id=p.task_id AND w.family_id=p.family_idx
            WHERE p.stage_num=1 AND p.is_done=1 ON CONFLICT DO NOTHING;
            INSERT INTO learner_answer_events(user_id,family_id,stage_num,task_id,is_correct,source)
            SELECT user_id,family_idx,stage_num,task_id,is_correct,'legacy'
            FROM user_task_progress WHERE is_done=1 AND stage_num BETWEEN 2 AND 8 AND is_correct IS NOT NULL;
            INSERT INTO content_schema_migrations(name) VALUES('srs_v1');
        """)
    await conn.execute("""
        CREATE OR REPLACE FUNCTION record_learning_event() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.is_done <> 1 OR (to_jsonb(NEW)->>'task_id') IS NULL THEN
                RETURN NEW;
            END IF;
            IF NEW.stage_num=1 THEN
                INSERT INTO srs_cards(user_id,family_id,task_id)
                SELECT NEW.user_id,NEW.family_idx,NEW.task_id FROM stage1_words
                WHERE id=NEW.task_id AND family_id=NEW.family_idx ON CONFLICT DO NOTHING;
            ELSIF NEW.stage_num BETWEEN 2 AND 8 AND NEW.is_correct IS NOT NULL THEN
                INSERT INTO learner_answer_events(user_id,family_id,stage_num,task_id,answered_at,is_correct,source)
                VALUES(NEW.user_id,NEW.family_idx,NEW.stage_num,NEW.task_id,now(),NEW.is_correct,'lesson');
            END IF;
            RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS srs_learning_event ON user_task_progress;
        CREATE TRIGGER srs_learning_event AFTER INSERT OR UPDATE ON user_task_progress
        FOR EACH ROW EXECUTE FUNCTION record_learning_event();
    """)

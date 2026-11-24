"""Learner-owned vocabulary reviews using the official Py-FSRS 6 scheduler."""
import hashlib
import secrets
import uuid
from datetime import datetime, timezone
from fsrs import Card, Rating, Scheduler

from tgteacher_bot.db.pool import get_pool

SCHEDULER = Scheduler(desired_retention=0.9, enable_fuzzing=False)
# Archived/inaccessible cards remain in storage but are never offered for review.
ACCESS = """f.archived_at IS NULL AND w.archived_at IS NULL
    AND (coalesce(f.target,'VIP+FREE') IN ('FREE','VIP+FREE')
         OR EXISTS (SELECT 1 FROM users u WHERE u.user_id=c.user_id AND u.is_subscribed IS TRUE
                    AND (u.subscription_until IS NULL OR u.subscription_until>now())))"""
BASE = """FROM srs_cards c JOIN families f ON f.id=c.family_id
    JOIN stage1_words w ON w.id=c.task_id AND w.family_id=c.family_id"""
COLS = "c.family_id,c.task_id,c.due,c.revision,w.word,w.translation,f.name AS family_name"


def _page(limit, offset):
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
        raise ValueError("Invalid pagination")


def _row(row):
    return {k: v.isoformat() if isinstance(v,datetime) else v for k,v in dict(row).items()}


def _result(rows,limit,offset):
    return {"items":[_row(r) for r in rows[:limit]],"next_offset":offset+limit if len(rows)>limit else None}


async def lock_user(conn, uid):
    await conn.execute("SELECT pg_advisory_xact_lock(847232,hashtext($1::text))",str(uid))


async def issue_token(uid):
    raw = "tgtl_" + secrets.token_urlsafe(32)
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await lock_user(conn,uid)
            if not await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1",uid):
                raise ValueError("Start the bot before linking")
            await conn.execute("""INSERT INTO learner_tokens(token_hash,user_id,expires_at)
                VALUES($1,$2,now()+interval '24 hours')
                ON CONFLICT(user_id) DO UPDATE SET token_hash=excluded.token_hash,expires_at=excluded.expires_at""",
                hashlib.sha256(raw.encode()).hexdigest(),uid)
    return raw


async def revoke_token(uid):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await lock_user(conn,uid)
            await conn.execute("DELETE FROM learner_tokens WHERE user_id=$1",uid)


async def verify_token(raw):
    if not isinstance(raw,str) or not raw.startswith("tgtl_") or len(raw)!=48 or not raw.isascii():
        return None
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow("SELECT user_id,expires_at FROM learner_tokens WHERE token_hash=$1 AND expires_at>now()",
                                   hashlib.sha256(raw.encode()).hexdigest())


async def due_reviews(uid, limit=30, offset=0):
    _page(limit,offset)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""SELECT {COLS}, c.card IS NULL AS unreviewed
            {BASE} WHERE c.user_id=$1 AND c.due<=now() AND {ACCESS}
            ORDER BY c.due,c.family_id,c.task_id LIMIT $2 OFFSET $3""",uid,limit+1,offset)
    return _result(rows,limit,offset)


async def learning_progress(uid, limit=30, offset=0):
    _page(limit,offset)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT f.id AS family_id,f.name,f.archived_at IS NOT NULL AS archived,
                coalesce(p.completion_count,0) AS completion_count,
                (SELECT count(*) FROM active_task_positions t WHERE t.family_idx=f.id) AS total_tasks,
                (SELECT count(*) FROM active_user_task_progress a WHERE a.family_idx=f.id AND a.user_id=$1 AND a.is_done=1) AS completed_tasks,
                (SELECT count(*) FROM srs_cards c WHERE c.family_id=f.id AND c.user_id=$1) AS enrolled_cards
            FROM families f LEFT JOIN user_family_progress p ON p.family_idx=f.id AND p.user_id=$1
            WHERE p.user_id IS NOT NULL OR EXISTS(SELECT 1 FROM user_task_progress a WHERE a.family_idx=f.id AND a.user_id=$1)
                OR EXISTS(SELECT 1 FROM srs_cards c WHERE c.family_id=f.id AND c.user_id=$1)
            ORDER BY f.id LIMIT $2 OFFSET $3
        """,uid,limit+1,offset)
    return _result(rows,limit,offset)


async def learning_statistics(uid, days=30):
    if type(days) is not int or not 1 <= days <= 365:
        raise ValueError("days must be 1–365")
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read",readonly=True):
            answers = await conn.fetchrow("""SELECT count(*) AS attempts,
                count(*) FILTER(WHERE is_correct=1) AS correct
                FROM learner_answer_events WHERE user_id=$1 AND source='lesson'
                AND answered_at>=now()-$2::integer*interval '1 day'""",uid,days)
            legacy = await conn.fetchval("SELECT count(*) FROM learner_answer_events WHERE user_id=$1 AND source='legacy'",uid)
            reviews = await conn.fetch("""SELECT rating,count(*) AS count FROM srs_reviews
                WHERE user_id=$1 AND reviewed_at>=now()-$2::integer*interval '1 day' GROUP BY rating ORDER BY rating""",uid,days)
            due = await conn.fetchval(f"SELECT count(*) {BASE} WHERE c.user_id=$1 AND c.due<=now() AND {ACCESS}",uid)
            total = await conn.fetchval(f"SELECT count(*) {BASE} WHERE c.user_id=$1 AND {ACCESS}",uid)
    return {"days":days,"lesson_attempts":answers["attempts"],"correct_lesson_attempts":answers["correct"],
            "legacy_answer_snapshots":legacy,"review_ratings":{str(r["rating"]):r["count"] for r in reviews},
            "due_cards":due,"accessible_cards":total}


async def weak_words(uid, limit=30, offset=0):
    _page(limit,offset)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""SELECT {COLS},h.last_rating,h.again_count,h.review_count,
            (c.card->>'difficulty')::double precision AS difficulty
            {BASE}
            JOIN LATERAL (SELECT count(*) AS review_count,count(*) FILTER(WHERE rating=1) AS again_count,
                 (array_agg(rating ORDER BY id DESC))[1] AS last_rating FROM srs_reviews r
                 WHERE r.user_id=c.user_id AND r.family_id=c.family_id AND r.task_id=c.task_id) h ON true
            WHERE c.user_id=$1 AND {ACCESS} AND h.review_count>0
                AND (h.last_rating<=2 OR (c.card->>'difficulty')::double precision>=7)
            ORDER BY h.last_rating,h.again_count DESC,c.due,c.family_id,c.task_id LIMIT $2 OFFSET $3""",uid,limit+1,offset)
    return _result(rows,limit,offset)


async def next_review(uid):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock_shared(847231,1)")
            await lock_user(conn,uid)
            await conn.execute("DELETE FROM srs_review_tickets WHERE user_id=$1",uid)
            row = await conn.fetchrow(f"""SELECT {COLS},f.content_version
                {BASE} WHERE c.user_id=$1 AND c.due<=now() AND {ACCESS}
                ORDER BY c.due,c.family_id,c.task_id LIMIT 1""",uid)
            if not row:
                return None
            nonce = uuid.uuid4()
            await conn.execute("""INSERT INTO srs_review_tickets(nonce,user_id,family_id,task_id,card_revision,family_version,expires_at)
                VALUES($1,$2,$3,$4,$5,$6,now()+interval '15 minutes')""",nonce,uid,row["family_id"],row["task_id"],row["revision"],row["content_version"])
            return {"nonce":nonce.hex,"word":row["word"],"family_name":row["family_name"]}


async def _ticket(conn,uid,nonce):
    row = await conn.fetchrow(f"""SELECT {COLS},c.card,t.revealed
        {BASE} JOIN srs_review_tickets t ON t.user_id=c.user_id AND t.family_id=c.family_id AND t.task_id=c.task_id
        WHERE t.nonce=$2 AND c.user_id=$1 AND t.expires_at>now() AND t.card_revision=c.revision
        AND t.family_version=f.content_version AND c.due<=now() AND {ACCESS}""",uid,uuid.UUID(nonce))
    if not row:
        raise ValueError("Review expired or content changed")
    return row


async def reveal_review(uid,nonce):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock_shared(847231,1)")
            await lock_user(conn,uid)
            row = await _ticket(conn,uid,nonce)
            await conn.execute("UPDATE srs_review_tickets SET revealed=true WHERE nonce=$1",uuid.UUID(nonce))
            return {"word":row["word"],"translation":row["translation"]}


async def rate_review(uid,nonce,rating):
    if type(rating) is not int or rating not in (1,2,3,4):
        raise ValueError("Rating must be 1–4")
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock_shared(847231,1)")
            await lock_user(conn,uid)
            row = await _ticket(conn,uid,nonce)
            if not row["revealed"]:
                raise ValueError("Reveal translation first")
            now = datetime.now(timezone.utc)
            card = Card.from_json(row["card"]) if row["card"] else Card(card_id=row["task_id"],due=now)
            updated,log = SCHEDULER.review_card(card,Rating(rating),review_datetime=now)
            await conn.execute("""UPDATE srs_cards SET card=$4::jsonb,due=$5,revision=revision+1
                WHERE user_id=$1 AND family_id=$2 AND task_id=$3""",uid,row["family_id"],row["task_id"],updated.to_json(),updated.due)
            await conn.execute("""INSERT INTO srs_reviews(user_id,family_id,task_id,reviewed_at,rating,review_log,before_card,after_card)
                VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb)""",
                uid,row["family_id"],row["task_id"],now,rating,log.to_json(),card.to_json(),updated.to_json())
            await conn.execute("DELETE FROM srs_review_tickets WHERE nonce=$1",uuid.UUID(nonce))
    return {"due":updated.due.isoformat(),"rating":rating}

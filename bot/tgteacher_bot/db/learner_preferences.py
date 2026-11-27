"""Revisioned learner preferences and evidence-based study recommendations."""
from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.db import srs_repo as srs

DEFAULTS = {"daily_review_goal":20,"desired_retention":0.9,"review_order":"due","self_assessed_level":"unspecified"}
LEVELS = {"unspecified","A1","A2","B1","B2","C1","C2"}
ORDERS = {"due","weak_first"}


async def init_preferences(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS learner_preferences (
            user_id BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
            revision INTEGER NOT NULL DEFAULT 1,
            daily_review_goal INTEGER NOT NULL CHECK(daily_review_goal BETWEEN 5 AND 200),
            desired_retention DOUBLE PRECISION NOT NULL CHECK(desired_retention BETWEEN 0.8 AND 0.97),
            review_order TEXT NOT NULL CHECK(review_order IN ('due','weak_first')),
            self_assessed_level TEXT NOT NULL CHECK(self_assessed_level IN ('unspecified','A1','A2','B1','B2','C1','C2')),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


async def read_preferences(conn,uid):
    row = await conn.fetchrow("""SELECT revision,daily_review_goal,desired_retention,review_order,self_assessed_level
        FROM learner_preferences WHERE user_id=$1""",uid)
    return dict(row) if row else {"revision":0,**DEFAULTS}


def validate_patch(patch):
    if not isinstance(patch,dict) or not patch or set(patch)-set(DEFAULTS):
        raise ValueError("Provide only supported learner preference fields")
    if "daily_review_goal" in patch and (type(patch["daily_review_goal"]) is not int or not 5<=patch["daily_review_goal"]<=200):
        raise ValueError("Daily review goal must be 5–200")
    if "desired_retention" in patch and (type(patch["desired_retention"]) not in (int,float) or not 0.8<=patch["desired_retention"]<=0.97):
        raise ValueError("Desired retention must be 0.80–0.97")
    for field,values in (("review_order",ORDERS),("self_assessed_level",LEVELS)):
        if field in patch and (not isinstance(patch[field],str) or patch[field] not in values):
            raise ValueError("Invalid "+field)


async def update_preferences(uid,expected_revision,patch):
    validate_patch(patch)
    if type(expected_revision) is not int or expected_revision<0:
        raise ValueError("Invalid expected revision")
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await srs.lock_user(conn,uid)
            if not await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1",uid):
                raise PermissionError("Learner account unavailable")
            previous = await read_preferences(conn,uid)
            if previous["revision"]!=expected_revision:
                raise ValueError("Preferences changed; reload profile")
            new = {**previous,**patch}
            row = await conn.fetchrow("""INSERT INTO learner_preferences(user_id,revision,daily_review_goal,desired_retention,review_order,self_assessed_level)
                VALUES($1,1,$2,$3,$4,$5)
                ON CONFLICT(user_id) DO UPDATE SET revision=learner_preferences.revision+1,
                    daily_review_goal=excluded.daily_review_goal,desired_retention=excluded.desired_retention,
                    review_order=excluded.review_order,self_assessed_level=excluded.self_assessed_level,updated_at=now()
                RETURNING revision,daily_review_goal,desired_retention,review_order,self_assessed_level""",
                uid,new["daily_review_goal"],new["desired_retention"],new["review_order"],new["self_assessed_level"])
            return dict(row)


async def daily_counts(conn,uid):
    return await conn.fetchval("""SELECT count(*) FROM srs_reviews WHERE user_id=$1
        AND reviewed_at>=date_trunc('day',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'""",uid)


async def profile(uid):
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read",readonly=True):
            if not await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1",uid):
                raise PermissionError("Learner account unavailable")
            preferences = await read_preferences(conn,uid)
            reviewed = await daily_counts(conn,uid)
            due = await conn.fetchval(f"SELECT count(*) {srs.BASE} WHERE c.user_id=$1 AND c.due<=now() AND {srs.ACCESS}",uid)
            enrolled = await conn.fetchval(f"SELECT count(*) {srs.BASE} WHERE c.user_id=$1 AND {srs.ACCESS}",uid)
            evidence = await conn.fetch("""SELECT stage_num,count(*) AS attempts,count(*) FILTER(WHERE is_correct=1) AS correct
                FROM learner_answer_events WHERE user_id=$1 AND source='lesson' AND answered_at>=now()-interval '30 days'
                GROUP BY stage_num ORDER BY stage_num""",uid)
    remaining=max(0,preferences["daily_review_goal"]-reviewed)
    weakness=[dict(r) for r in evidence if r["attempts"]>=5 and r["correct"]*100<70*r["attempts"]]
    weakness.sort(key=lambda r:(r["correct"]/r["attempts"],r["stage_num"]))
    recommendation = "review_due" if due else "study_new_words"
    if not remaining:
        recommendation="daily_goal_met"
    return {"preferences":preferences,"reviewed_today":reviewed,"remaining_daily_goal":remaining,
            "due_cards":due,"accessible_cards":enrolled,"suggested_review_count":min(due,remaining),
            "recommended_action":recommendation,"weak_stages":weakness,
            "stage_evidence_30_days":[dict(r) for r in evidence],
            "level_source":"self_assessed","day_boundary":"UTC"}

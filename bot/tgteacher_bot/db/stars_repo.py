"""Durable one-off VIP invoices; never mix XTR with kopecks."""
import uuid
from tgteacher_bot.db.pool import get_pool

class StarsConflict(ValueError):
    pass

async def init_stars(c):
    await c.execute("""
        CREATE TABLE IF NOT EXISTS stars_tariffs(
            months INTEGER PRIMARY KEY CHECK(months BETWEEN 1 AND 60),
            amount INTEGER NOT NULL CHECK(amount BETWEEN 1 AND 10000)
        );
        CREATE TABLE IF NOT EXISTS stars_orders(
            id UUID PRIMARY KEY,user_id BIGINT NOT NULL,
            months INTEGER NOT NULL CHECK(months BETWEEN 1 AND 60),
            amount INTEGER NOT NULL CHECK(amount BETWEEN 1 AND 10000),
            state TEXT NOT NULL DEFAULT 'prepared' CHECK(state IN ('prepared','authorized','paid','refunded')),
            checkout_id TEXT UNIQUE,charge_id TEXT UNIQUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),authorized_at TIMESTAMPTZ,
            paid_at TIMESTAMPTZ,refunded_at TIMESTAMPTZ,result_subscription_until TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS stars_orders_user_idx ON stars_orders(user_id,created_at DESC);
    """)

async def tariffs():
    pool=await get_pool()
    async with pool.acquire() as c:
        return [dict(r) for r in await c.fetch("SELECT months,amount FROM stars_tariffs ORDER BY months")]

async def set_tariff(months,amount=None,remove=False):
    if type(months) is not int or not 1<=months<=60 or type(remove) is not bool:
        raise StarsConflict("Invalid Stars tariff")
    if not remove and (type(amount) is not int or not 1<=amount<=10000):
        raise StarsConflict("Invalid XTR amount")
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            await c.execute("SELECT pg_advisory_xact_lock(847231,95)")
            if remove:
                await c.execute("DELETE FROM stars_tariffs WHERE months=$1",months)
            else:
                if not await c.fetchval("SELECT 1 FROM stars_tariffs WHERE months=$1",months) and await c.fetchval("SELECT count(*) FROM stars_tariffs")>=4:
                    raise StarsConflict("At most four Stars tariffs")
                await c.execute("INSERT INTO stars_tariffs(months,amount) VALUES($1,$2) ON CONFLICT(months) DO UPDATE SET amount=excluded.amount",months,amount)
    return {"ok":True}

def order_id(payload):
    if not isinstance(payload,str) or not payload.startswith("vipxtr:"):
        raise StarsConflict("Invalid invoice payload")
    try:
        return uuid.UUID(payload[7:])
    except ValueError as exc:
        raise StarsConflict("Invalid invoice payload") from exc

def verify(row,uid,currency,amount):
    if not row or row["user_id"]!=uid or currency!="XTR" or type(amount) is not int or row["amount"]!=amount:
        raise StarsConflict("Invoice owner or amount mismatch")

def nonempty(value):
    if not isinstance(value,str) or not 1<=len(value)<=255:
        raise StarsConflict("Invalid Telegram transaction ID")
    return value

async def prepare(uid,months,amount):
    if type(months) is not int or not 1<=months<=60 or type(amount) is not int or not 1<=amount<=10000:
        raise StarsConflict("Invalid tariff")
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            if await c.fetchval("SELECT amount FROM stars_tariffs WHERE months=$1",months)!=amount:
                raise StarsConflict("Tariff changed")
            if not await c.fetchval("SELECT 1 FROM users WHERE user_id=$1",uid):
                raise StarsConflict("Unknown learner")
            ident=uuid.uuid4()
            await c.execute("INSERT INTO stars_orders(id,user_id,months,amount) VALUES($1,$2,$3,$4)",ident,uid,months,amount)
    return "vipxtr:"+str(ident)

async def authorize(uid,payload,currency,amount,checkout_id):
    checkout_id=nonempty(checkout_id)
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            row=await c.fetchrow("SELECT *,created_at>now()-interval '30 minutes' AS fresh FROM stars_orders WHERE id=$1 FOR UPDATE",order_id(payload))
            verify(row,uid,currency,amount)
            if row["state"]=="authorized" and row["checkout_id"]==checkout_id:
                return
            if row["state"]!="prepared" or not row["fresh"]:
                raise StarsConflict("Invoice expired or already authorized")
            if not await c.fetchval("SELECT 1 FROM users WHERE user_id=$1",uid):
                raise StarsConflict("Unknown learner")
            if await c.fetchval("SELECT amount FROM stars_tariffs WHERE months=$1",row["months"])!=amount:
                raise StarsConflict("Tariff changed")
            await c.execute("UPDATE stars_orders SET state='authorized',checkout_id=$2,authorized_at=now() WHERE id=$1",row["id"],checkout_id)

async def settle(uid,payload,currency,amount,charge_id):
    charge_id=nonempty(charge_id)
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            row=await c.fetchrow("SELECT * FROM stars_orders WHERE id=$1 FOR UPDATE",order_id(payload))
            verify(row,uid,currency,amount)
            if row["state"] in ("paid","refunded"):
                if row["charge_id"]!=charge_id:
                    raise StarsConflict("Different charge for same order")
                return dict(row)
            if row["state"]!="authorized":
                raise StarsConflict("Order not authorized")
            learner=await c.fetchrow("""UPDATE users SET
                subscription_until=CASE WHEN is_subscribed IS TRUE AND subscription_until IS NULL THEN NULL
                    WHEN is_subscribed IS TRUE THEN greatest(now(),subscription_until)+make_interval(days=>$2::int)
                    ELSE now()+make_interval(days=>$2::int) END,
                subscription_count=coalesce(subscription_count,0)+$3,is_subscribed=true
                WHERE user_id=$1 RETURNING subscription_until""",uid,row["months"]*30,row["months"])
            if not learner:
                raise StarsConflict("Learner missing; contact payment support")
            row=await c.fetchrow("""UPDATE stars_orders SET state='paid',charge_id=$2,paid_at=now(),result_subscription_until=$3
                WHERE id=$1 RETURNING *""",row["id"],charge_id,learner["subscription_until"])
            return dict(row)

async def observe_refund(uid,payload,currency,amount,charge_id):
    charge_id=nonempty(charge_id)
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            row=await c.fetchrow("SELECT * FROM stars_orders WHERE id=$1 FOR UPDATE",order_id(payload))
            verify(row,uid,currency,amount)
            if row["state"]=="refunded" and row["charge_id"]==charge_id:
                return dict(row)
            if row["state"] not in ("authorized","paid") or (row["charge_id"] and row["charge_id"]!=charge_id):
                raise StarsConflict("Refund does not match order")
            return dict(await c.fetchrow("UPDATE stars_orders SET state='refunded',charge_id=$2,refunded_at=now() WHERE id=$1 RETURNING *",row["id"],charge_id))

async def operator_orders(state="authorized",limit=100,offset=0):
    """Read-only, bounded local ledger inspection; not provider reconciliation."""
    if state not in ("prepared","authorized","paid","refunded","all"):
        raise StarsConflict("Invalid order state")
    if type(limit) is not int or not 1<=limit<=500 or type(offset) is not int or offset<0:
        raise StarsConflict("Invalid pagination")
    pool=await get_pool()
    async with pool.acquire() as c:
        return [dict(r) for r in await c.fetch("""
            SELECT id,user_id,months,amount,state,created_at,authorized_at,paid_at,
                   refunded_at,charge_id,result_subscription_until
            FROM stars_orders WHERE ($1='all' OR state=$1)
            ORDER BY created_at DESC,id DESC LIMIT $2 OFFSET $3
        """,state,limit,offset)]

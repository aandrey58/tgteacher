"""Durable YooKassa order state. No external call is made inside a DB transaction."""
import uuid
from decimal import Decimal, InvalidOperation
from urllib.parse import urlsplit
from tgteacher_bot.db.pool import get_pool

class PaymentConflict(ValueError):
    pass

ACTIVE=("creating","uncertain","pending","waiting_for_capture")
REMOTE=("pending","waiting_for_capture","succeeded","canceled")

async def init_orders(c):
    await c.execute("""
        CREATE TABLE IF NOT EXISTS payment_orders (
            id UUID PRIMARY KEY, user_id BIGINT NOT NULL, months INTEGER NOT NULL CHECK(months BETWEEN 1 AND 60),
            amount INTEGER NOT NULL CHECK(amount BETWEEN 100 AND 10000000),
            state TEXT NOT NULL DEFAULT 'prepared' CHECK(state IN ('prepared','creating','uncertain','pending','waiting_for_capture','succeeded','canceled')),
            provider_id TEXT UNIQUE, payment_url TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), processed_at TIMESTAMPTZ,
            result_subscription_until TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS payment_orders_user_idx ON payment_orders(user_id,created_at DESC);
    """)

def ident(value):
    try:
        return uuid.UUID(str(value))
    except (TypeError,ValueError,AttributeError) as exc:
        raise PaymentConflict("Unknown order") from exc

async def _tariff(c,months,amount):
    if type(months) is not int or not 1<=months<=60 or type(amount) is not int or not 100<=amount<=10000000:
        raise PaymentConflict("Invalid tariff")
    current=await c.fetchval("SELECT amount FROM payment_options WHERE months=$1",months)
    if current is None or current*100!=amount:
        raise PaymentConflict("Tariff changed; select it again")

async def prepare(uid,months,amount):
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            await _tariff(c,months,amount)
            if not await c.fetchval("SELECT 1 FROM users WHERE user_id=$1",uid):
                raise PaymentConflict("Unknown learner")
            oid=uuid.uuid4()
            await c.execute("INSERT INTO payment_orders(id,user_id,months,amount) VALUES($1,$2,$3,$4)",oid,uid,months,amount)
    return oid

async def get_order(uid,order_id):
    pool=await get_pool()
    async with pool.acquire() as c:
        row=await c.fetchrow("SELECT * FROM payment_orders WHERE id=$1 AND user_id=$2",ident(order_id),uid)
    if not row:
        raise PaymentConflict("Unknown order; contact support for legacy payments")
    return dict(row)

async def open_orders(uid):
    pool=await get_pool()
    async with pool.acquire() as c:
        rows=await c.fetch("SELECT id,state,months,amount FROM payment_orders WHERE user_id=$1 AND state=ANY($2::text[]) ORDER BY created_at DESC LIMIT 10",uid,list(ACTIVE))
    return [dict(row) for row in rows]

async def claim(uid,order_id):
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            row=await c.fetchrow("SELECT *,created_at>now()-interval '30 minutes' AS fresh FROM payment_orders WHERE id=$1 AND user_id=$2 FOR UPDATE",ident(order_id),uid)
            if not row:
                raise PaymentConflict("Unknown order")
            if row["state"]!="prepared":
                return dict(row),False
            if not row["fresh"]:
                raise PaymentConflict("Quote expired")
            if not await c.fetchval("SELECT 1 FROM users WHERE user_id=$1 FOR UPDATE",uid):
                raise PaymentConflict("Unknown learner")
            if await c.fetchval("SELECT 1 FROM payment_orders WHERE user_id=$1 AND id<>$2 AND state=ANY($3::text[]) LIMIT 1",uid,row["id"],list(ACTIVE)):
                raise PaymentConflict("Resolve existing payment first")
            await _tariff(c,row["months"],row["amount"])
            updated=await c.fetchrow("UPDATE payment_orders SET state='creating',updated_at=now() WHERE id=$1 RETURNING *",row["id"])
            return dict(updated),True

async def mark_uncertain(uid,order_id):
    pool=await get_pool()
    async with pool.acquire() as c:
        await c.execute("UPDATE payment_orders SET state='uncertain',updated_at=now() WHERE id=$1 AND user_id=$2 AND state='creating'",ident(order_id),uid)

def verified_remote(order,payment):
    try:
        pid=str(ident(payment.id))
        metadata=payment.metadata
        if (str(metadata.get("order_id"))!=str(order["id"])
            or str(metadata.get("user_id"))!=str(order["user_id"])
            or str(metadata.get("months"))!=str(order["months"])
            or metadata.get("payment_type")!="subscription"):
            raise PaymentConflict("Payment metadata mismatch")
        amount=Decimal(str(payment.amount.value))*100
        if not amount.is_finite() or amount!=order["amount"] or payment.amount.currency!="RUB":
            raise PaymentConflict("Payment amount/currency mismatch")
        if order["provider_id"] and pid!=order["provider_id"]:
            raise PaymentConflict("Payment ID mismatch")
        state=payment.status
        if state not in REMOTE or (state=="succeeded" and payment.paid is not True):
            raise PaymentConflict("Unsupported payment state")
        confirmation=getattr(payment,"confirmation",None)
        url=getattr(confirmation,"confirmation_url",None)
        if url:
            parsed=urlsplit(url)
            if parsed.scheme!="https" or not parsed.hostname or parsed.username or parsed.password:
                raise PaymentConflict("Invalid payment URL")
        return pid,state,url
    except (AttributeError,TypeError,InvalidOperation,ValueError) as exc:
        raise PaymentConflict("Invalid provider payment") from exc

async def bind_created(uid,order_id,payment):
    """Bind verified provider identity before attempting to grant access."""
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            row=await c.fetchrow("SELECT * FROM payment_orders WHERE id=$1 AND user_id=$2 FOR UPDATE",ident(order_id),uid)
            if not row or row["state"] not in ("creating","uncertain"):
                raise PaymentConflict("Order not awaiting provider binding")
            pid,state,url=verified_remote(row,payment)
            if await c.fetchval("SELECT 1 FROM payments WHERE payment_id=$1",pid):
                raise PaymentConflict("Provider ID already used in legacy ledger")
            if state=="pending" and not url:
                raise PaymentConflict("Provider payment URL missing")
            # Keep a recoverable binding even if immediate settlement fails later.
            row=await c.fetchrow("UPDATE payment_orders SET provider_id=$2,payment_url=$3,state=$4,updated_at=now() WHERE id=$1 RETURNING *",row["id"],pid,url,"pending" if state=="succeeded" else state)
            return dict(row)

async def settle(uid,order_id,payment):
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            row=await c.fetchrow("SELECT * FROM payment_orders WHERE id=$1 AND user_id=$2 FOR UPDATE",ident(order_id),uid)
            if not row or not row["provider_id"]:
                raise PaymentConflict("Order not bound")
            pid,state,url=verified_remote(row,payment)
            if row["state"] in ("succeeded","canceled"):
                return dict(row)
            if row["state"]=="waiting_for_capture" and state=="pending":
                return dict(row)
            if state!="succeeded":
                row=await c.fetchrow("UPDATE payment_orders SET state=$2,updated_at=now() WHERE id=$1 RETURNING *",row["id"],state)
                return dict(row)
            # UPDATE locks the user row; preserves a permanent VIP. 30 days per purchased month.
            learner=await c.fetchrow("""UPDATE users SET
                subscription_until=CASE WHEN is_subscribed IS TRUE AND subscription_until IS NULL THEN NULL
                    WHEN is_subscribed IS TRUE THEN greatest(now(),subscription_until)+make_interval(days=>$2::int)
                    ELSE now()+make_interval(days=>$2::int) END,
                subscription_count=coalesce(subscription_count,0)+$3,is_subscribed=true
                WHERE user_id=$1 RETURNING subscription_until""",uid,row["months"]*30,row["months"])
            if not learner:
                raise PaymentConflict("Learner deleted; operator reconciliation required")
            until=learner["subscription_until"]
            await c.execute("""INSERT INTO payments(payment_id,user_id,months,amount,status,payment_url,processed,processed_at,result_subscription_until)
                VALUES($1,$2,$3,$4,'succeeded',$5,true,now(),$6)""",pid,uid,row["months"],row["amount"],row["payment_url"],until)
            row=await c.fetchrow("""UPDATE payment_orders SET state='succeeded',processed_at=now(),updated_at=now(),result_subscription_until=$2
                WHERE id=$1 RETURNING *""",row["id"],until)
            return dict(row)

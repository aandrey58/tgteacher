"""Web operator actions and persistent Telegram delivery queue.

The polling bot owns Telegram credentials. HTTP handlers only enqueue messages.
Uncertain sends are never automatically repeated.
"""
import asyncio
import io
import uuid
from datetime import datetime
from tgteacher_bot.db.pool import get_pool

VIP = "is_subscribed IS TRUE AND (subscription_until IS NULL OR subscription_until>now())"
TARGETS = {"all":"TRUE","subs":VIP,"nosubs":f"NOT coalesce(({VIP}),false)"}


async def init_operations(c):
    await c.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY, payment_id VARCHAR(255) UNIQUE NOT NULL, user_id BIGINT NOT NULL,
            months INTEGER NOT NULL, amount INTEGER NOT NULL, status VARCHAR(50) NOT NULL, payment_url TEXT,
            processed BOOLEAN DEFAULT FALSE, created_at TIMESTAMP DEFAULT now(), updated_at TIMESTAMP DEFAULT now(),
            processed_at TIMESTAMP, result_subscription_until TIMESTAMPTZ
        );
        ALTER TABLE payments ADD COLUMN IF NOT EXISTS result_subscription_until TIMESTAMPTZ;
        CREATE TABLE IF NOT EXISTS web_admin_list_change (
            singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
            expected_ids JSONB, desired_ids JSONB, status TEXT NOT NULL DEFAULT 'idle',
            changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        INSERT INTO web_admin_list_change(singleton) VALUES(true) ON CONFLICT DO NOTHING;
        CREATE TABLE IF NOT EXISTS web_system_control (
            singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK(singleton),
            auto_snapshots BOOLEAN,
            capture_requested BOOLEAN NOT NULL DEFAULT false,
            capture_status TEXT NOT NULL DEFAULT 'idle',
            capture_started TIMESTAMPTZ
        );
        ALTER TABLE web_system_control ADD COLUMN IF NOT EXISTS capture_started TIMESTAMPTZ;
        INSERT INTO web_system_control(singleton) VALUES(true) ON CONFLICT DO NOTHING;
        CREATE TABLE IF NOT EXISTS web_support (
            id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            question TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            status TEXT NOT NULL DEFAULT 'open' CHECK(status IN ('open','queued','answered','closed')),
            answer TEXT
        );
        CREATE TABLE IF NOT EXISTS web_message_attachments (
            asset_id TEXT PRIMARY KEY, filename TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('video','document')),
            content BYTEA NOT NULL CHECK(octet_length(content) BETWEEN 1 AND 16777216)
        );
        CREATE TABLE IF NOT EXISTS web_campaigns (
            id UUID PRIMARY KEY, target TEXT NOT NULL, body TEXT NOT NULL,
            photo_id TEXT REFERENCES image_assets(asset_id), support_id BIGINT REFERENCES web_support(id) ON DELETE CASCADE,
            status TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','queued','cancelled')),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        ALTER TABLE web_campaigns ADD COLUMN IF NOT EXISTS attachment_id TEXT REFERENCES web_message_attachments(asset_id);
        CREATE TABLE IF NOT EXISTS web_deliveries (
            campaign_id UUID NOT NULL REFERENCES web_campaigns(id) ON DELETE CASCADE,
            user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','sending','sent','failed','uncertain','cancelled')),
            changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY(campaign_id,user_id)
        );
        CREATE INDEX IF NOT EXISTS web_pending_delivery_idx ON web_deliveries(status,changed_at);
    """)


def ids(value):
    if not isinstance(value,list) or not 1<=len(value)<=100 or any(type(i) is not int or not 0<i<2**53 for i in value) or len(set(value))!=len(value):
        raise ValueError("Invalid selection")
    return sorted(value)


async def bulk_archive(family_ids, archived):
    selected=ids(family_ids)
    if type(archived) is not bool:
        raise ValueError("Invalid archive state")
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            await c.execute("SELECT pg_advisory_xact_lock(847231,1)")
            rows=await c.fetch("""UPDATE families SET archived_at=CASE WHEN $2 THEN coalesce(archived_at,now()) ELSE NULL END
                WHERE id=ANY($1::int[]) RETURNING id""",selected,archived)
            if len(rows)!=len(selected):
                raise ValueError("Group not found")
    return {"count":len(rows)}


async def subscription(user_ids, months=None, revoke=False, days=None, operation_id=None):
    selected=ids(user_ids)
    if type(revoke) is not bool:
        raise ValueError("Invalid subscription change")
    if revoke:
        if days is not None or months is not None:
            raise ValueError("Revoke does not accept duration")
    else:
        if days is None and type(months) is int and 1<=months<=12:
            days=months*30
        elif months is not None:
            raise ValueError("Use one duration field")
        if type(days) is not int or not 1<=days<=365:
            raise ValueError("Invalid gift duration")
        operation_id=uuid.UUID(operation_id) if operation_id else uuid.uuid4()
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            if revoke:
                rows=await c.fetch("UPDATE users SET is_subscribed=false,subscription_until=NULL WHERE user_id=ANY($1::bigint[]) RETURNING user_id",selected)
            else:
                # Stable operation ID prevents accidental replay from the client.
                await c.execute("SELECT pg_advisory_xact_lock(847233,hashtext($1::text))",str(operation_id))
                previous=await c.fetchval("SELECT count(*) FROM payments WHERE left(payment_id,length($1::text))=$1::text",'webgift_'+str(operation_id)+'_')
                if previous:
                    raise ValueError("Gift operation already applied")
                rows=await c.fetch("""UPDATE users SET is_subscribed=true,
                    subscription_until=CASE WHEN is_subscribed THEN greatest(now(),coalesce(subscription_until,now())) ELSE now() END+make_interval(days=>$2::int),
                    subscription_count=coalesce(subscription_count,0)+1 WHERE user_id=ANY($1::bigint[]) RETURNING user_id,subscription_until""",selected,days)
                for row in rows:
                    await c.execute("""INSERT INTO payments(payment_id,user_id,months,amount,status,processed,processed_at,result_subscription_until)
                        VALUES($1,$2,$3,0,'gift',true,now(),$4)""",
                        f"webgift_{operation_id}_{row['user_id']}",row["user_id"],(days+29)//30,row["subscription_until"])
            if len(rows)!=len(selected):
                raise ValueError("Learner not found")
    return {"count":len(rows)}


async def set_tariff(months, amount=None, remove=False):
    if type(months) is not int or not 1<=months<=60 or type(remove) is not bool:
        raise ValueError("Invalid tariff")
    if not remove and (type(amount) is not int or not 1<=amount<=100000):
        raise ValueError("Invalid amount in RUB")
    pool=await get_pool()
    async with pool.acquire() as c:
        if remove:
            await c.execute("DELETE FROM payment_options WHERE months=$1",months)
        else:
            async with c.transaction():
                await c.execute("SELECT pg_advisory_xact_lock(847231,94)")
                if not await c.fetchval("SELECT 1 FROM payment_options WHERE months=$1",months) and await c.fetchval("SELECT count(*) FROM payment_options")>=4:
                    raise ValueError("At most four tariffs")
                await c.execute("""INSERT INTO payment_options(months,amount) VALUES($1,$2)
                    ON CONFLICT(months) DO UPDATE SET amount=excluded.amount""",months,amount)
    return {"ok":True}


async def tariffs():
    pool=await get_pool()
    async with pool.acquire() as c:
        return [dict(r) for r in await c.fetch("SELECT months,amount FROM payment_options ORDER BY months")]


async def create_question(user_id, question):
    if not isinstance(question,str) or not 1<=len(question.strip())<=4096:
        raise ValueError("Invalid question")
    pool=await get_pool()
    async with pool.acquire() as c:
        return await c.fetchval("INSERT INTO web_support(user_id,question) VALUES($1,$2) RETURNING id",user_id,question.strip())


async def support_list(offset=0):
    if type(offset) is not int or not 0<=offset<=100000:
        raise ValueError("Invalid offset")
    pool=await get_pool()
    async with pool.acquire() as c:
        rows=await c.fetch("""SELECT s.id,s.user_id::text AS user_id,s.question,s.answer,s.status,s.created_at::text AS created_at,u.first_name,u.username
            FROM web_support s JOIN users u USING(user_id) ORDER BY (s.status='open') DESC,s.id DESC LIMIT 51 OFFSET $1""",offset)
    return {"items":[dict(r) for r in rows[:50]],"next":len(rows)>50}


async def close_question(question_id):
    pool=await get_pool()
    async with pool.acquire() as c:
        result=await c.execute("UPDATE web_support SET status='closed' WHERE id=$1 AND status IN ('open','answered')",question_id)
        if result!="UPDATE 1":
            raise ValueError("Question unavailable")
    return {"ok":True}


async def create_campaign(campaign_id, target, text, user_ids=None, photo_id=None, question_id=None, attachment_id=None):
    ident=uuid.UUID(campaign_id)
    if photo_id and attachment_id:
        raise ValueError("Choose one attachment")
    if not isinstance(text,str) or len(text.strip())>(1024 if photo_id or attachment_id else 4000) or (not text.strip() and not (photo_id or attachment_id)):
        raise ValueError("Invalid message length")
    if target not in (*TARGETS,"selected","support"):
        raise ValueError("Invalid recipient target")
    if target=="selected":
        selected=ids(user_ids)
    elif user_ids is not None:
        raise ValueError("Unexpected recipient selection")
    if (target=="support")!=(question_id is not None):
        raise ValueError("Invalid support reference")
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            if await c.fetchval("SELECT 1 FROM web_campaigns WHERE id=$1",ident):
                raise ValueError("Campaign ID already used")
            if photo_id and not await c.fetchval("SELECT 1 FROM image_assets WHERE asset_id=$1",photo_id):
                raise ValueError("Unknown image")
            if attachment_id and not await c.fetchval("SELECT 1 FROM web_message_attachments WHERE asset_id=$1",attachment_id):
                raise ValueError("Unknown attachment")
            if target=="support":
                q=await c.fetchrow("SELECT user_id,status FROM web_support WHERE id=$1 FOR UPDATE",question_id)
                if not q or q["status"]!="open":
                    raise ValueError("Question not open")
                selected=[q["user_id"]]
            await c.execute("INSERT INTO web_campaigns(id,target,body,photo_id,support_id,attachment_id) VALUES($1,$2,$3,$4,$5,$6)",ident,target,text.strip(),photo_id,question_id,attachment_id)
            if target in TARGETS:
                from tgteacher_bot.handlers.admin.admin_list import get_admin_ids
                excluded=[int(i) for i in get_admin_ids() if str(i).isdigit()]
                await c.execute(f"INSERT INTO web_deliveries(campaign_id,user_id) SELECT $1,user_id FROM users WHERE ({TARGETS[target]}) AND NOT (user_id=ANY($2::bigint[]))",ident,excluded)
            else:
                await c.execute("INSERT INTO web_deliveries(campaign_id,user_id) SELECT $1,user_id FROM users WHERE user_id=ANY($2::bigint[])",ident,selected)
            count=await c.fetchval("SELECT count(*) FROM web_deliveries WHERE campaign_id=$1",ident)
            if not count or (target in ("selected","support") and count!=len(selected)):
                raise ValueError("No recipients or unknown learner")
    return {"id":str(ident),"recipients":count,"status":"draft"}


async def confirm_campaign(campaign_id, expected_count):
    if type(expected_count) is not int or expected_count<1:
        raise ValueError("Invalid recipient count")
    ident=uuid.UUID(campaign_id)
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            row=await c.fetchrow("SELECT * FROM web_campaigns WHERE id=$1 FOR UPDATE",ident)
            if not row or row["status"]!="draft":
                raise ValueError("Campaign already confirmed or cancelled")
            count=await c.fetchval("SELECT count(*) FROM web_deliveries WHERE campaign_id=$1",ident)
            if count!=expected_count:
                raise ValueError("Recipient count changed")
            if row["support_id"] is not None:
                result=await c.execute("UPDATE web_support SET status='queued',answer=$2 WHERE id=$1 AND status='open'",row["support_id"],row["body"])
                if result!="UPDATE 1":
                    raise ValueError("Question already answered or queued")
            await c.execute("UPDATE web_campaigns SET status='queued' WHERE id=$1",ident)
    return {"id":str(ident),"recipients":count,"status":"queued"}


async def cancel_campaign(campaign_id):
    ident=uuid.UUID(campaign_id)
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            row=await c.fetchrow("SELECT * FROM web_campaigns WHERE id=$1 FOR UPDATE",ident)
            if not row or row["status"]=="cancelled":
                raise ValueError("Campaign unavailable")
            if row["support_id"] and await c.fetchval("SELECT 1 FROM web_deliveries WHERE campaign_id=$1 AND status IN ('sending','sent','uncertain')",ident):
                raise ValueError("Answer already dispatched or uncertain")
            await c.execute("UPDATE web_campaigns SET status='cancelled' WHERE id=$1",ident)
            await c.execute("UPDATE web_deliveries SET status='cancelled',changed_at=now() WHERE campaign_id=$1 AND status='pending'",ident)
            if row["support_id"] and row["status"]=="queued":
                await c.execute("UPDATE web_support SET status='open',answer=NULL WHERE id=$1 AND status='queued'",row["support_id"])
    return {"ok":True}


async def campaigns(offset=0):
    if type(offset) is not int or not 0<=offset<=100000:
        raise ValueError("Invalid offset")
    pool=await get_pool()
    async with pool.acquire() as c:
        rows=await c.fetch("""SELECT w.id::text AS id,w.target,w.body,w.photo_id,w.attachment_id,w.status,w.created_at::text AS created_at,
            count(d.user_id) AS recipients,count(*) FILTER(WHERE d.status='sent') AS sent,
            count(*) FILTER(WHERE d.status='pending') AS pending,count(*) FILTER(WHERE d.status='sending') AS sending,
            count(*) FILTER(WHERE d.status='failed') AS failed,count(*) FILTER(WHERE d.status='uncertain') AS uncertain,
            count(*) FILTER(WHERE d.status='cancelled') AS cancelled
            FROM web_campaigns w LEFT JOIN web_deliveries d ON d.campaign_id=w.id GROUP BY w.id ORDER BY w.created_at DESC LIMIT 51 OFFSET $1""",offset)
    return {"items":[dict(r) for r in rows[:50]],"next":len(rows)>50}


async def deliver_one(bot):
    from telegram.error import Forbidden, BadRequest, RetryAfter
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            # A process that died after claiming a message might have sent it.
            await c.execute("UPDATE web_deliveries SET status='uncertain' WHERE status='sending' AND changed_at<now()-interval '10 minutes'")
            row=await c.fetchrow("""SELECT d.campaign_id,d.user_id,w.body,w.photo_id,w.support_id,w.attachment_id
                FROM web_deliveries d JOIN web_campaigns w ON w.id=d.campaign_id
                WHERE d.status='pending' AND w.status='queued'
                ORDER BY d.changed_at,d.campaign_id,d.user_id LIMIT 1 FOR UPDATE OF d,w SKIP LOCKED""")
            if not row:
                return False
            await c.execute("UPDATE web_deliveries SET status='sending',changed_at=now() WHERE campaign_id=$1 AND user_id=$2",row["campaign_id"],row["user_id"])
            photo=await c.fetchval("SELECT jpeg FROM image_assets WHERE asset_id=$1",row["photo_id"]) if row["photo_id"] else None
            attachment=await c.fetchrow("SELECT filename,kind,content FROM web_message_attachments WHERE asset_id=$1",row["attachment_id"]) if row["attachment_id"] else None
    status="uncertain"
    try:
        if photo:
            await asyncio.wait_for(bot.send_photo(chat_id=row["user_id"],photo=io.BytesIO(photo),caption=row["body"]),40)
        elif attachment:
            from telegram import InputFile
            file=InputFile(attachment["content"],filename=attachment["filename"])
            if attachment["kind"]=="video":
                await asyncio.wait_for(bot.send_video(chat_id=row["user_id"],video=file,caption=row["body"]),40)
            else:
                await asyncio.wait_for(bot.send_document(chat_id=row["user_id"],document=file,caption=row["body"]),40)
        else:
            await asyncio.wait_for(bot.send_message(chat_id=row["user_id"],text=row["body"]),40)
        status="sent"
    except (Forbidden,BadRequest):
        status="failed"
    except RetryAfter:
        status="failed"  # explicit rejection; operator decides whether to send a new campaign
    except Exception:
        pass
    async with pool.acquire() as c:
        async with c.transaction():
            await c.execute("UPDATE web_deliveries SET status=$3,changed_at=now() WHERE campaign_id=$1 AND user_id=$2 AND status='sending'",row["campaign_id"],row["user_id"],status)
            if row["support_id"] is not None:
                if status=="sent":
                    await c.execute("UPDATE web_support SET status='answered' WHERE id=$1 AND status='queued'",row["support_id"])
                elif status=="failed":
                    await c.execute("UPDATE web_support SET status='open' WHERE id=$1 AND status='queued'",row["support_id"])
    return True


async def delivery_job(context):
    for _ in range(10):
        if not await deliver_one(context.bot):
            break
        await asyncio.sleep(0.25)


async def extend_active(days, expected_count):
    if type(days) is not int or not 1<=days<=365 or type(expected_count) is not int or expected_count<1:
        raise ValueError("Invalid extension")
    pool=await get_pool()
    async with pool.acquire() as c:
        async with c.transaction():
            rows=await c.fetch(f"SELECT user_id FROM users WHERE {VIP} AND subscription_until IS NOT NULL ORDER BY user_id FOR UPDATE")
            if len(rows)!=expected_count:
                raise ValueError("Active subscriber count changed")
            await c.execute("""UPDATE users SET subscription_until=greatest(now(),coalesce(subscription_until,now()))
                +make_interval(days=>$2::int) WHERE user_id=ANY($1::bigint[]) AND subscription_until IS NOT NULL""",
                [r["user_id"] for r in rows],days)
    return {"count":len(rows)}


async def active_subscription_count():
    pool=await get_pool()
    async with pool.acquire() as c:
        return {"count":await c.fetchval(f"SELECT count(*) FROM users WHERE {VIP} AND subscription_until IS NOT NULL")}


async def reports(kind="learners", days=30, family_id=None, start_date=None, end_date=None):
    from datetime import date, timedelta
    if type(days) is not int or not 1<=days<=365:
        raise ValueError("Invalid report range")
    if start_date is not None or end_date is not None:
        start=date.fromisoformat(start_date)
        end=date.fromisoformat(end_date)
        if not 0<=(end-start).days<366:
            raise ValueError("Invalid report dates")
        params=[start,end+timedelta(days=1)]
    else:
        params=[days]
    def recent(col):
        if len(params)==2:
            return f"({col}>=$1::date AND {col}<$2::date)"
        return f"({col}>=now()-$1::int*interval '1 day')"
    pool=await get_pool()
    async with pool.acquire() as c:
        if kind in ("learners","new","inactive"):
            if kind=="inactive" and len(params)==2:
                raise ValueError("Inactive report uses a relative inactivity period")
            condition={"learners":"TRUE","new":recent("registered_at"),"inactive":"last_active_at<now()-$1::int*interval '1 day' OR last_active_at IS NULL"}[kind]
            args=[] if kind=="learners" else params
            rows=await c.fetch(f"""SELECT user_id::text AS user_id,username,first_name,last_name,registered_at::text AS registered_at,
                last_active_at::text AS last_active_at,subscription_until::text AS subscription_until,is_subscribed
                FROM users WHERE {condition} ORDER BY user_id LIMIT 10001""",*args)
        elif kind=="progress":
            if type(family_id) is not int or family_id<1:
                raise ValueError("Family required")
            rows=await c.fetch("""SELECT p.user_id::text AS user_id,p.stage_num,count(*) AS completed,
                count(*) FILTER(WHERE p.is_correct=1) AS correct
                FROM active_user_task_progress p WHERE p.family_idx=$1 AND p.is_done=1
                GROUP BY p.user_id,p.stage_num ORDER BY p.user_id,p.stage_num LIMIT 10001""",family_id)
        elif kind=="payments":
            if not await c.fetchval("SELECT to_regclass('payments') IS NOT NULL"):
                return []
            rows=await c.fetch(f"""SELECT payment_id,user_id::text AS user_id,months,amount AS amount_kopecks,status,
                created_at::text AS created_at FROM payments WHERE {recent("created_at")}
                ORDER BY created_at DESC LIMIT 10001""",*params)
        elif kind=="system":
            if not await c.fetchval("SELECT to_regclass('system_snapshots') IS NOT NULL"):
                return []
            rows=await c.fetch(f"""SELECT ts::text AS ts,data::text AS data FROM system_snapshots
                WHERE {recent("ts")} ORDER BY ts DESC LIMIT 10001""",*params)
        else:
            raise ValueError("Invalid report type")
    if len(rows)>10000:
        raise ValueError("Report exceeds 10000 rows; narrow the date range")
    return [dict(r) for r in rows]

def workbook(rows):
    from openpyxl import Workbook
    wb=Workbook(write_only=True)
    ws=wb.create_sheet("Отчёт")
    if rows:
        keys=list(rows[0])
        titles={'user_id':'Telegram ID','username':'Имя пользователя','first_name':'Имя','last_name':'Фамилия','registered_at':'Регистрация','last_active_at':'Последняя активность','subscription_until':'VIP до','is_subscribed':'Флаг подписки','stage_num':'Этап','completed':'Выполнено','correct':'Верно','payment_id':'ID платежа','months':'Месяцы','amount_kopecks':'Сумма, коп.','status':'Статус','created_at':'Дата создания','ts':'Дата снимка','data':'Данные снимка'}
        ws.append([titles.get(k,k) for k in keys])
        for row in rows:
            # Do not let user-provided text become executable spreadsheet formulas.
            ws.append(["'"+v if isinstance(v,str) and v.lstrip().startswith(("=","+","-","@")) else v for v in (row[k] for k in keys)])
    else:
        ws.append(["Нет записей"])
    out=io.BytesIO()
    wb.save(out)
    return out.getvalue()


async def system_status():
    import json
    from tgteacher_bot.handlers.admin.admin_list import get_admin_ids
    pool=await get_pool()
    async with pool.acquire() as c:
        control=dict(await c.fetchrow("SELECT auto_snapshots,capture_requested,capture_status FROM web_system_control WHERE singleton"))
        row=None
        if await c.fetchval("SELECT to_regclass('system_snapshots') IS NOT NULL"):
            row=await c.fetchrow("SELECT ts::text AS ts,data FROM system_snapshots ORDER BY ts DESC LIMIT 1")
    snapshot=None
    if row:
        snapshot={**(json.loads(row["data"]) if isinstance(row["data"],str) else row["data"]),"ts":row["ts"]}
    return {**control,"snapshot":snapshot,"telegram_admin_count":len(get_admin_ids())}


async def set_snapshot_auto(enabled):
    if type(enabled) is not bool:
        raise ValueError("Invalid auto-snapshot setting")
    pool=await get_pool()
    async with pool.acquire() as c:
        await c.execute("UPDATE web_system_control SET auto_snapshots=$1 WHERE singleton",enabled)
    return {"enabled":enabled}


async def request_snapshot():
    pool=await get_pool()
    async with pool.acquire() as c:
        row=await c.fetchrow("""UPDATE web_system_control SET capture_requested=true,capture_status='queued'
            WHERE singleton AND NOT capture_requested AND (capture_status<>'running' OR capture_started<now()-interval '10 minutes') RETURNING capture_status""")
        if not row:
            raise ValueError("Snapshot already queued or running")
    return {"status":"queued"}


async def snapshot_job(context):
    from tgteacher_bot.services.legacy.system_snapshots import capture_and_store_snapshot
    pool=await get_pool()
    async with pool.acquire() as c:
        row=await c.fetchrow("""UPDATE web_system_control SET capture_requested=false,capture_status='running',capture_started=now()
            WHERE singleton AND capture_requested RETURNING singleton""")
    if not row:
        return
    status="failed"
    try:
        await capture_and_store_snapshot(context)
        status="ready"
    finally:
        async with pool.acquire() as c:
            await c.execute("UPDATE web_system_control SET capture_status=$1 WHERE singleton",status)


async def learner_answers(uid, family_id, limit=50, offset=0):
    if type(uid) is not int or type(family_id) is not int or not 1<=limit<=100 or offset<0:
        raise ValueError("Invalid answer pagination")
    pool=await get_pool()
    async with pool.acquire() as c:
        rows=await c.fetch("""SELECT stage_num,task_id,task_idx,is_done,selected_choice_text,is_correct
            FROM active_user_task_progress WHERE user_id=$1 AND family_idx=$2
            ORDER BY stage_num,task_idx LIMIT $3 OFFSET $4""",uid,family_id,limit+1,offset)
    return {"items":[dict(r) for r in rows[:limit]],"next":len(rows)>limit}


async def upload_message_attachment(filename,kind,data):
    import hashlib
    import re
    from pathlib import Path
    if (not isinstance(filename,str) or not 1<=len(filename)<=120 or Path(filename).name!=filename
            or any(ord(c)<32 for c in filename) or "\\" in filename or not 1<=len(data)<=16*1024*1024
            or kind not in ("video","document")):
        raise ValueError("Invalid attachment")
    suffix=Path(filename).suffix.lower()
    if kind=="video":
        if suffix!=".mp4" or data[4:8]!=b"ftyp":
            raise ValueError("MP4 required")
    elif suffix not in (".pdf",".txt",".zip",".docx",".xlsx",".pptx"):
        raise ValueError("Unsupported document type")
    # Documents are opaque attachments, never executed, parsed or displayed inline.
    ident=hashlib.sha256(kind.encode()+b"\0"+filename.encode()+b"\0"+data).hexdigest()
    pool=await get_pool()
    async with pool.acquire() as c:
        await c.execute("INSERT INTO web_message_attachments VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",ident,filename,kind,data)
    return {"asset_id":ident,"filename":filename,"kind":kind}


def legacy_admin_ids():
    from tgteacher_bot.core.paths import admins_path
    path=admins_path()
    try:
        raw=path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    result=[]
    for line in raw.splitlines():
        value=line.strip()
        if not value:
            continue
        if not value.isascii() or not value.isdecimal() or not 0<int(value)<2**53:
            raise ValueError("Invalid legacy administrator list")
        result.append(int(value))
    return sorted(set(result))


async def admin_list():
    pool=await get_pool()
    async with pool.acquire() as c:
        status=await c.fetchval("SELECT status FROM web_admin_list_change WHERE singleton")
    return {"ids":[str(i) for i in legacy_admin_ids()],"status":status}


async def change_admin_list(expected_ids, desired_ids):
    import json
    # Legacy file controls the Telegram admin/link only; no web secret is changed.
    if expected_ids==[]:
        expected=[]
    else:
        expected=ids(expected_ids)
    desired=ids(desired_ids)
    if expected!=legacy_admin_ids():
        raise ValueError("Administrator list changed")
    pool=await get_pool()
    async with pool.acquire() as c:
        result=await c.execute("""UPDATE web_admin_list_change SET expected_ids=$1::jsonb,desired_ids=$2::jsonb,
            status='queued',changed_at=now() WHERE singleton AND status NOT IN ('queued','running')""",json.dumps(expected),json.dumps(desired))
        if result!="UPDATE 1":
            raise ValueError("Another administrator change is pending")
    return {"status":"queued"}


def replace_legacy_admins(expected,desired):
    import os
    import tempfile
    from tgteacher_bot.core.paths import admins_path
    path=admins_path()
    if legacy_admin_ids()!=expected:
        raise ValueError("Administrator list changed before execution")
    if path.is_symlink() or not path.parent.is_dir():
        raise ValueError("Invalid administrator path")
    fd,name=tempfile.mkstemp(prefix=".admins-",dir=path.parent)
    try:
        with os.fdopen(fd,"w",encoding="utf-8") as out:
            out.write("".join(str(i)+"\n" for i in desired))
            out.flush()
            os.fsync(out.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


async def admin_list_job(context):
    import json
    pool=await get_pool()
    async with pool.acquire() as c:
        # Recover a claim after restart without repeating an unknown write.
        await c.execute("UPDATE web_admin_list_change SET status='uncertain' WHERE singleton AND status='running' AND changed_at<now()-interval '10 minutes'")
        row=await c.fetchrow("""UPDATE web_admin_list_change SET status='running',changed_at=now()
            WHERE singleton AND status='queued' RETURNING expected_ids,desired_ids""")
    if not row:
        return
    status="failed"
    try:
        expected=row["expected_ids"] if isinstance(row["expected_ids"],list) else json.loads(row["expected_ids"])
        desired=row["desired_ids"] if isinstance(row["desired_ids"],list) else json.loads(row["desired_ids"])
        replace_legacy_admins(expected,desired)
        status="ready"
    finally:
        async with pool.acquire() as c:
            await c.execute("UPDATE web_admin_list_change SET status=$1,changed_at=now() WHERE singleton",status)

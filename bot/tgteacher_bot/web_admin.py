"""Same-origin web admin; independent from MCP and learner credentials."""
import hashlib
import hmac
import json
import os
import re
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, FileResponse, Response
from starlette.routing import Route, Mount
from starlette.staticfiles import StaticFiles
from tgteacher_bot.db import drafts_repo as drafts, content_versions as versions, families_repo as families
from tgteacher_bot.db import tts_repo as tts, image_repo as images, media_repo as media, user_repo as users, srs_repo as srs
from tgteacher_bot.db.pool import get_pool

COOKIE = "tgteacher_admin"
READ = {
    "family": versions.get_current_content, "draft": drafts.get_draft,
    "edit_draft": versions.get_edit_draft, "validate": drafts.validate_draft,
    "preview": versions.preview_edit, "versions": versions.list_versions, "version": versions.get_version,
    "media": media.get_media, "voices": tts.discover_voices, "tts_models": tts.discover_providers,
    "estimate_audio": tts.estimate, "learner_progress": srs.learning_progress,
    "learner_statistics": srs.learning_statistics,
}
WRITE = {
    "create_draft": drafts.create_draft, "update_draft": drafts.update_draft,
    "create_edit": versions.create_edit_draft, "update_edit": versions.update_edit_draft,
    "delete_draft": drafts.delete_draft, "delete_edit": versions.delete_edit_draft,
    "publish": drafts.publish_draft, "publish_edit": versions.publish_edit,
    "archive": families.set_family_archived_pg, "rollback": versions.rollback_family,
    "attach_audio": tts.attach_audio, "attach_image": media.attach_image, "detach": media.detach_media,
    "extend_subscription": users.extend_user_subscription_pg,
}
async def parse_source(source):
    from tgteacher_bot.utils.family_parser import parse_family_text
    return parse_family_text(drafts.check_source(source))


async def render_source(content):
    from tgteacher_bot.utils.family_renderer import render_family_txt
    from tgteacher_bot.db.content_schema import STAGES, stage_key
    candidate=versions.clean_content(content)
    if any(t["task_id"] is not None for stage in STAGES for t in candidate[stage_key(stage)]):
        raise ValueError("New drafts must not reference published task IDs")
    return {"source":render_family_txt(candidate)}


READ.update({"parse_source":parse_source,"render_source":render_source})
PAGES = {"dashboard","groups","drafts","learners","reviews","media","tts","images","budgets","settings"}


def digest(token):
    return hashlib.sha256(token.encode()).hexdigest()


async def init_web(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS web_admin_sessions (
            token_hash TEXT PRIMARY KEY, credential_hash TEXT NOT NULL,
            expires_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS web_admin_audit (
            id BIGSERIAL PRIMARY KEY, action TEXT NOT NULL, status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
    """)


def reply(value, status=200):
    return JSONResponse(value,status_code=status,headers={"Cache-Control":"no-store","X-Content-Type-Options":"nosniff"})


async def body(request):
    raw = bytearray()
    async for chunk in request.stream():
        if len(raw)+len(chunk)>512*1024:
            raise ValueError("Request too large")
        raw.extend(chunk)
    result=json.loads(raw)
    if not isinstance(result,dict):
        raise ValueError("Expected JSON object")
    return result


async def page_data(page, offset, query):
    pool=await get_pool()
    async with pool.acquire() as c:
        if page=="dashboard":
            totals=await c.fetchrow("""SELECT
                (SELECT count(*) FROM families WHERE archived_at IS NULL) published_groups,
                (SELECT count(*) FROM families WHERE archived_at IS NOT NULL) archived_groups,
                (SELECT count(*) FROM users WHERE last_active_at>=now()-interval '7 days') active_learners,
                (SELECT count(*) FROM content_drafts WHERE deleted_at IS NULL AND published_at IS NULL)
                  +(SELECT count(*) FROM family_edit_drafts WHERE deleted_at IS NULL AND published_version IS NULL) drafts,
                (SELECT count(*) FROM srs_cards c JOIN families f ON f.id=c.family_id JOIN stage1_words w ON w.id=c.task_id
                 WHERE c.due<=now() AND f.archived_at IS NULL AND w.archived_at IS NULL) due_cards,
                (SELECT count(*) FROM audio_assets) audio_assets,(SELECT count(*) FROM image_assets) image_assets""")
            activity=await c.fetch("""SELECT answered_at::date::text AS day,count(*) AS attempts,
                count(DISTINCT user_id) AS learners FROM learner_answer_events
                WHERE answered_at>=now()-interval '7 days' GROUP BY 1 ORDER BY 1""")
            return {"metrics":dict(totals),"activity":[dict(r) for r in activity]}
        if page=="groups":
            rows=await c.fetch("""SELECT f.id,f.name,f.description,f.target,f.content_version,
                CASE WHEN f.archived_at IS NULL THEN 'Published' ELSE 'Archived' END AS status,
                (SELECT count(*) FROM stage1_words w WHERE w.family_id=f.id AND w.archived_at IS NULL) AS words
                FROM families f WHERE strpos(lower(f.name),lower($1))>0 OR $1=''
                ORDER BY f.id DESC LIMIT 51 OFFSET $2""",query,offset)
        elif page=="drafts":
            rows=await c.fetch("""SELECT * FROM (
                SELECT id::text AS draft_id,'new' AS kind,family_id,revision,
                    CASE WHEN published_at IS NULL THEN 'Draft' ELSE 'Published' END AS status,updated_at::text AS updated_at
                FROM content_drafts WHERE deleted_at IS NULL
                UNION ALL SELECT id::text,'edit',family_id,revision,
                    CASE WHEN published_version IS NULL THEN 'Draft' ELSE 'Published' END,created_at::text
                FROM family_edit_drafts WHERE deleted_at IS NULL
                ) d WHERE $1='' OR strpos(lower(draft_id),lower($1))>0 ORDER BY updated_at DESC LIMIT 51 OFFSET $2""",query,offset)
        elif page=="learners":
            rows=await c.fetch("""SELECT u.user_id::text AS user_id,u.username,u.first_name,u.last_name,
                CASE WHEN u.is_subscribed AND (u.subscription_until IS NULL OR u.subscription_until>now()) THEN 'VIP' ELSE 'Free' END AS plan,
                u.last_active_at::text AS last_active_at,u.subscription_until::text AS subscription_until,
                (SELECT count(*) FROM srs_cards c WHERE c.user_id=u.user_id AND c.due<=now()) due_cards
                FROM users u WHERE $1='' OR strpos(lower(concat_ws(' ',u.user_id::text,u.username,u.first_name,u.last_name)),lower($1))>0
                ORDER BY last_active_at DESC NULLS LAST,u.user_id LIMIT 51 OFFSET $2""",query,offset)
        elif page=="reviews":
            rows=await c.fetch("""SELECT f.id,f.name,count(*) AS cards,count(*) FILTER(WHERE c.due<=now()) AS due,
                count(*) FILTER(WHERE c.card IS NOT NULL) AS rated
                FROM srs_cards c JOIN families f ON f.id=c.family_id JOIN stage1_words w ON w.id=c.task_id
                WHERE f.archived_at IS NULL AND w.archived_at IS NULL AND ($1='' OR strpos(lower(f.name),lower($1))>0)
                GROUP BY f.id,f.name ORDER BY due DESC,f.id LIMIT 51 OFFSET $2""",query,offset)
        elif page=="budgets":
            rows=await c.fetch("""SELECT * FROM (
                SELECT 'TTS' AS service,provider,model,status,reserved_microusd,reserved_at::text AS reserved_at FROM tts_requests
                UNION ALL SELECT 'Images',provider,model,status,reserved_microusd,reserved_at::text FROM image_requests
                ) r WHERE $1='' OR strpos(lower(concat_ws(' ',service,provider,model,status)),lower($1))>0
                ORDER BY reserved_at DESC LIMIT 51 OFFSET $2""",query,offset)
            today=await c.fetch("""SELECT 'TTS' AS service,coalesce(sum(reserved_microusd),0)::bigint AS reserved FROM tts_requests WHERE reserved_at>=date_trunc('day',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
              UNION ALL SELECT 'Images',coalesce(sum(reserved_microusd),0)::bigint FROM image_requests WHERE reserved_at>=date_trunc('day',now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'""")
            return {"items":[dict(r) for r in rows[:50]],"next":len(rows)>50,"metrics":{r["service"]:r["reserved"] for r in today},"limits":{"TTS":tts.daily_limit(),"Images":images.daily_limit()}}
        elif page=="settings":
            rows=await c.fetch("SELECT action,status,created_at::text AS created_at FROM web_admin_audit ORDER BY id DESC LIMIT 50")
            return {"items":[dict(r) for r in rows],"metrics":{"MCP tools":45,"Web access":"Single operator","Database":"Connected"}}
        else:
            rows=None
    if rows is not None:
        return {"items":[dict(r) for r in rows[:50]],"next":len(rows)>50}
    kind={"media":"all","tts":"audio","images":"image"}[page]
    result=await media.list_media(kind,50,offset)
    return {"items":result["items"],"next":result["next_offset"] is not None,
            "models":tts.list_providers()["providers"] if page=="tts" else result["image_models"] if page=="images" else []}


def build_app(secret, origin, static_dir=None, initialize=True):
    if not secret.isascii() or len(secret)<32:
        raise ValueError("Set a unique random WEB_ADMIN_SECRET of at least 32 characters")
    parsed=urlsplit(origin)
    if origin!=f"{parsed.scheme}://{parsed.netloc}" or not parsed.hostname or (parsed.scheme!="https" and not (parsed.scheme=="http" and parsed.hostname in ("127.0.0.1","localhost"))):
        raise ValueError("WEB_ADMIN_ORIGIN must be HTTPS or local HTTP origin")
    credential=digest(secret)

    async def authorized(request):
        token=request.cookies.get(COOKIE,"")
        if not re.fullmatch(r"[A-Za-z0-9_-]{43}",token):
            return False
        pool=await get_pool()
        async with pool.acquire() as c:
            return bool(await c.fetchval("SELECT 1 FROM web_admin_sessions WHERE token_hash=$1 AND credential_hash=$2 AND expires_at>now()",digest(token),credential))

    async def login(request):
        if request.headers.get("origin")!=origin:
            return reply({"error":"Origin rejected"},403)
        data=await body(request)
        key=data.get("secret")
        if not isinstance(key,str) or not key.isascii() or not hmac.compare_digest(key,secret):
            return reply({"error":"Invalid credential"},401)
        token=secrets.token_urlsafe(32)
        pool=await get_pool()
        async with pool.acquire() as c:
            await c.execute("DELETE FROM web_admin_sessions WHERE expires_at<=now() OR credential_hash<>$1",credential)
            await c.execute("INSERT INTO web_admin_sessions VALUES($1,$2,now()+interval '8 hours')",digest(token),credential)
        response=reply({"authenticated":True})
        response.set_cookie(COOKIE,token,max_age=28800,httponly=True,secure=parsed.scheme=="https",samesite="strict",path="/")
        return response

    async def api(request):
        if request.method=="POST" and request.headers.get("origin")!=origin:
            return reply({"error":"Origin rejected"},403)
        if not await authorized(request):
            return reply({"error":"Authentication required"},401)
        route=request.path_params["route"]
        if route=="session" and request.method=="GET":
            return reply({"authenticated":True})
        if route=="logout" and request.method=="POST":
            pool=await get_pool()
            async with pool.acquire() as c:
                await c.execute("DELETE FROM web_admin_sessions WHERE token_hash=$1",digest(request.cookies[COOKIE]))
            response=reply({"authenticated":False})
            response.delete_cookie(COOKIE,path="/")
            return response
        if request.method=="GET" and route in PAGES:
            offset=int(request.query_params.get("offset",0))
            query=request.query_params.get("q","")
            if not 0<=offset<=1000000 or len(query)>200:
                raise ValueError("Invalid search or pagination")
            return reply(await page_data(route,offset,query))
        if request.method=="GET" and route=="asset":
            kind=request.query_params.get("kind")
            ident=request.query_params.get("id")
            mod={"audio":tts,"image":images}.get(kind)
            if not mod:
                raise ValueError("Invalid asset kind")
            filename=mod.asset_filename(ident)
            data=await (tts.get_audio_bytes(filename) if kind=="audio" else images.get_image_bytes(filename))
            return Response(data,media_type="audio/mpeg" if kind=="audio" else "image/jpeg",headers={"Cache-Control":"private, no-store","X-Content-Type-Options":"nosniff"}) if data else reply({"error":"Not found"},404)
        if request.method!="POST" or route!="action":
            return reply({"error":"Not found"},404)
        data=await body(request)
        name,args=data.get("action"),data.get("args",{})
        if not isinstance(name,str) or not isinstance(args,dict):
            raise ValueError("Invalid action")
        if name in READ:
            return reply(await READ[name](**args))
        if name not in WRITE and name not in ("generate_audio","generate_image","estimate_image"):
            return reply({"error":"Unknown action"},404)
        if name!="estimate_image" and data.get("confirm") is not True:
            raise ValueError("Confirmation required")
        if name=="extend_subscription" and (type(args.get("months")) is not int or not 1<=args["months"]<=12):
            raise ValueError("Months must be 1–12")
        status="failed"
        try:
            if name=="estimate_image":
                return reply(await images.generate_batch(args["items"],0,True))
            if name=="generate_image":
                result=await images.generate_batch(args["items"],args["max_cost_microusd"],False)
            elif name=="generate_audio":
                result=await tts.generate_batch(args["items"],args["max_cost_microusd"])
            else:
                result=await WRITE[name](**args)
            status="ok"
            return reply(result)
        finally:
            if name!="estimate_image":
                pool=await get_pool()
                async with pool.acquire() as c:
                    await c.execute("INSERT INTO web_admin_audit(action,status) VALUES($1,$2)",name,status)

    async def safe(request):
        try:
            return await (login(request) if request.url.path=="/api/login" else api(request))
        except (ValueError,TypeError,KeyError):
            return reply({"error":"Invalid request or revision conflict. Check fields and reload."},400)
        except Exception:
            return reply({"error":"Operation failed"},500)

    @asynccontextmanager
    async def lifespan(app):
        if initialize:
            await users.init_db_pg()
            pool=await get_pool()
            async with pool.acquire() as c:
                async with c.transaction():
                    await c.execute("SELECT pg_advisory_xact_lock(847231,8)")
                    await init_web(c)
        yield

    routes=[Route("/api/login",safe,methods=["POST"]),Route("/api/{route}",safe,methods=["GET","POST"])]
    if static_dir:
        routes.append(Mount("/",app=StaticFiles(directory=static_dir,html=True)))
    app=Starlette(routes=routes,lifespan=lifespan)
    @app.middleware("http")
    async def security_headers(request,call_next):
        response=await call_next(request)
        response.headers["X-Frame-Options"]="DENY"
        response.headers["Referrer-Policy"]="no-referrer"
        response.headers["Content-Security-Policy"]="default-src 'self'; img-src 'self' data:; media-src 'self'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"
        return response
    return app


def main():
    import uvicorn
    uvicorn.run(build_app(os.environ["WEB_ADMIN_SECRET"],os.environ["WEB_ADMIN_ORIGIN"],Path("/app/admin-dist")),host="0.0.0.0",port=8080,proxy_headers=False)


if __name__=="__main__":
    main()

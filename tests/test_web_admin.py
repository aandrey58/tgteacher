import os
import io
import zipfile
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch
from starlette.testclient import TestClient

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"bot"))
from tgteacher_bot import web_admin
from tgteacher_bot.db import web_operations as ops
from tgteacher_bot.services.exports import web_transfer as transfer
from test_images import fixture
from test_tts import MP3
from tgteacher_bot.db import user_repo, drafts_repo, families_repo, content_versions, srs_repo, media_repo, tts_repo, image_repo

SECRET="web-only-secret-"+"a"*32
ORIGIN="http://localhost:8080"


class WebSecurityTests(unittest.TestCase):
    def test_fail_closed_configuration(self):
        for secret,origin in (("short",ORIGIN),(SECRET,"http://example.com"),(SECRET,"https://example.com/path")):
            with self.assertRaises(ValueError):
                web_admin.build_app(secret,origin,initialize=False)

    def test_denies_anonymous_and_cross_origin(self):
        app=web_admin.build_app(SECRET,ORIGIN,initialize=False)
        with TestClient(app,base_url=ORIGIN) as client:
            self.assertEqual(client.get("/api/groups").status_code,401)
            for route in ("export_zip?family_id=1","export_report","template"):
                self.assertEqual(client.get("/api/"+route).status_code,401)
            for route in ("import_zip","upload_media?kind=image"):
                self.assertEqual(client.post("/api/"+route,content=b"x",headers={"Origin":ORIGIN,"X-Confirm":"true"}).status_code,401)
            self.assertEqual(client.get("/api/asset?kind=image&id="+"a"*64).status_code,401)
            self.assertEqual(client.post("/api/login",json={"secret":SECRET},headers={"Origin":"https://foreign.example"}).status_code,403)
            self.assertEqual(client.post("/api/login",json={"secret":"wrong"},headers={"Origin":ORIGIN}).status_code,401)
            self.assertEqual(client.post("/api/action",json={"action":"publish"},headers={"Origin":ORIGIN}).status_code,401)


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"),"requires PostgreSQL")
class WebDatabaseTests(unittest.TestCase):
    def test_login_content_lifecycle_logout_and_all_pages(self):
        import asyncpg
        app=web_admin.build_app(SECRET,ORIGIN,initialize=False)
        with TestClient(app,base_url=ORIGIN) as client:
            schema="test_"+uuid.uuid4().hex
            async def setup():
                conn=await asyncpg.connect(os.environ["TGTEACHER_TEST_DSN"])
                await conn.execute(f'CREATE SCHEMA "{schema}"')
                pool=await asyncpg.create_pool(os.environ["TGTEACHER_TEST_DSN"],min_size=1,max_size=4,server_settings={"search_path":schema})
                return conn,pool
            conn,pool=client.portal.call(setup)
            try:
                from contextlib import ExitStack
                with ExitStack() as stack:
                    for mod in (web_admin,user_repo,drafts_repo,families_repo,content_versions,srs_repo,media_repo,tts_repo,image_repo,ops,transfer):
                        stack.enter_context(patch.object(mod,"get_pool",new=AsyncMock(return_value=pool)))
                    async def init():
                        await user_repo.init_db_pg()
                        async with pool.acquire() as c:
                            await web_admin.init_web(c)
                    client.portal.call(init)
                    headers={"Origin":ORIGIN}
                    login=client.post("/api/login",json={"secret":SECRET},headers=headers)
                    self.assertEqual(login.status_code,200,login.text)
                    self.assertIn("HttpOnly",login.headers["set-cookie"])
                    self.assertIn("samesite=strict",login.headers["set-cookie"].lower())
                    for page in web_admin.PAGES:
                        r=client.get("/api/"+page)
                        self.assertEqual(r.status_code,200,(page,r.text))
                    ident=str(uuid.uuid4())
                    args={"draft_id":ident,"source":'#FAMILY "Web group"\n#WORD ticket\n#TRANSLATION билет\n'}
                    def action(name,args,confirm=False,origin=ORIGIN):
                        return client.post("/api/action",json={"action":name,"args":args,"confirm":confirm},headers={"Origin":origin})
                    self.assertEqual(action("create_draft",args).status_code,400)
                    self.assertEqual(action("create_draft",args,True,"https://foreign.example").status_code,403)
                    self.assertEqual(action("create_draft",args,True).status_code,200)
                    self.assertEqual(action("validate",{"draft_id":ident}).status_code,200)
                    result=action("publish",{"draft_id":ident,"expected_revision":1},True)
                    self.assertEqual(result.status_code,200,result.text)
                    self.assertEqual(client.get("/api/groups").json()["items"][0]["name"],"Web group")
                    fid=result.json()["family_id"]
                    self.assertEqual(client.get("/api/groups?q=билет").json()["items"][0]["id"],fid)
                    self.assertEqual(client.get("/api/groups?status=Archived").json()["items"],[])
                    self.assertEqual(client.get("/api/groups?sort=invalid").status_code,400)
                    self.assertEqual(client.get("/api/groups?access=invalid").status_code,400)
                    self.assertEqual(client.get("/api/reviews?status=Archived").status_code,400)
                    self.assertEqual(client.get("/api/dashboard").json()["publishing"][0]["family_id"],fid)
                    self.assertEqual(action("archive",{"family_id":fid,"archived":True},True).status_code,200)
                    self.assertEqual(client.get("/api/groups?status=Published").json()["items"],[])
                    self.assertEqual(client.get("/api/groups?status=Archived").json()["items"][0]["id"],fid)
                    self.assertEqual(action("archive",{"family_id":fid,"archived":False},True).status_code,200)
                    self.assertEqual(client.get("/api/drafts?status=Draft").json()["items"],[])
                    self.assertEqual(len(client.get("/api/drafts?status=Published").json()["items"]),1)
                    # All new endpoints are exercised against the same isolated database.
                    def post_file(route,data,confirmed=True):
                        return client.post("/api/"+route,content=data,headers={**headers,"X-Confirm":str(confirmed).lower()})
                    self.assertEqual(post_file("upload_media?kind=image",fixture(),False).status_code,400)
                    self.assertEqual(post_file("upload_media?kind=image",b"not an image").status_code,400)
                    image=post_file("upload_media?kind=image",fixture())
                    self.assertEqual(image.status_code,200,image.text)
                    self.assertEqual(post_file("upload_media?kind=image",fixture()).json()["asset_id"],image.json()["asset_id"])
                    audio=post_file("upload_media?kind=audio",MP3)
                    self.assertEqual(audio.status_code,200,audio.text)
                    archive=io.BytesIO()
                    with zipfile.ZipFile(archive,"w") as z:
                        z.writestr("lesson/lesson.txt",'#FAMILY "Portable"\n#WORD ticket\n#TRANSLATION билет\n#STAGE5\n#TASK\n#IMAGE picture.png\n#ANSWER ticket\n#STAGE7\n#TASK\n#AUDIO sound.mp3\n#ANSWER ticket\n#CHOICES ticket; hat; bag; coat\n')
                        z.writestr("lesson/picture.png",fixture())
                        z.writestr("lesson/sound.mp3",MP3)
                    imp=post_file("import_zip",archive.getvalue())
                    self.assertEqual(imp.status_code,200,imp.text)
                    imported=imp.json()["draft_id"]
                    self.assertEqual(imp.json()["assets"],2)
                    self.assertTrue(action("validate",{"draft_id":imported}).json()["publishable"])
                    published=action("publish",{"draft_id":imported,"expected_revision":1},True)
                    self.assertEqual(published.status_code,200,published.text)
                    portable=published.json()["family_id"]
                    exported=client.get("/api/export_zip?family_id="+str(portable))
                    self.assertEqual(exported.status_code,200,exported.text if exported.status_code!=200 else "")
                    self.assertEqual(len(transfer.parse_archive(exported.content)[1]),2)
                    self.assertEqual(action("bulk_archive",{"family_ids":[fid,99999],"archived":True},True).status_code,400)
                    self.assertFalse(action("family",{"family_id":fid}).json()["archived_at"])
                    self.assertEqual(action("bulk_archive",{"family_ids":[fid,portable],"archived":True},True).json()["count"],2)
                    self.assertEqual(action("bulk_archive",{"family_ids":[fid,portable],"archived":False},True).json()["count"],2)
                    client.portal.call(user_repo.add_or_update_user_pg,11,"=SUM(1,2)","Demo","One")
                    client.portal.call(user_repo.add_or_update_user_pg,22,"two","Demo","Two")
                    gift={"user_ids":[11],"days":15,"operation_id":str(uuid.uuid4())}
                    self.assertEqual(action("subscription",gift,True).status_code,200)
                    self.assertEqual(action("subscription",gift,True).status_code,400)
                    self.assertEqual(action("active_subscription_count",{}).json()["count"],1)
                    self.assertEqual(action("extend_active",{"days":3,"expected_count":1},True).status_code,200)
                    self.assertEqual(action("set_tariff",{"months":60,"amount":100000},True).status_code,200)
                    self.assertEqual(action("tariffs",{}).json(),[{"months":60,"amount":100000}])
                    self.assertEqual(action("set_tariff",{"months":60,"remove":True},True).status_code,200)
                    self.assertEqual(action("reports",{"kind":"payments"}).json()[0]["status"],"gift")
                    from openpyxl import load_workbook
                    report=client.get("/api/export_report?kind=learners")
                    self.assertEqual(report.status_code,200)
                    ws=load_workbook(io.BytesIO(report.content)).active
                    self.assertEqual(ws["B2"].data_type,"s")
                    self.assertTrue(ws["B2"].value.startswith("'="))
                    # Confirm exactly once; preview does not send.
                    cid=str(uuid.uuid4())
                    args={"campaign_id":cid,"target":"selected","user_ids":[11,22],"text":"Demo message"}
                    self.assertEqual(action("create_campaign",args,True).json()["recipients"],2)
                    bot=type("Bot",(),{"send_message":AsyncMock(),"send_photo":AsyncMock()})()
                    self.assertFalse(client.portal.call(ops.deliver_one,bot))
                    self.assertEqual(action("confirm_campaign",{"campaign_id":cid,"expected_count":1},True).status_code,400)
                    self.assertEqual(action("confirm_campaign",{"campaign_id":cid,"expected_count":2},True).status_code,200)
                    self.assertEqual(action("confirm_campaign",{"campaign_id":cid,"expected_count":2},True).status_code,400)
                    self.assertTrue(client.portal.call(ops.deliver_one,bot))
                    bot.send_message.assert_awaited_once()
                    self.assertEqual(action("cancel_campaign",{"campaign_id":cid},True).status_code,200)
                    self.assertFalse(client.portal.call(ops.deliver_one,bot))
                    campaign=action("campaigns",{}).json()["items"][0]
                    self.assertEqual((campaign["sent"],campaign["cancelled"]),(1,1))
                    question=client.portal.call(ops.create_question,22,"Where is the lesson?")
                    cid2=str(uuid.uuid4())
                    self.assertEqual(action("create_campaign",{"campaign_id":cid2,"target":"support","question_id":question,"text":"Open Groups"},True).status_code,200)
                    self.assertEqual(action("confirm_campaign",{"campaign_id":cid2,"expected_count":1},True).status_code,200)
                    self.assertTrue(client.portal.call(ops.deliver_one,bot))
                    self.assertEqual(action("support",{}).json()["items"][0]["status"],"answered")
                    self.assertEqual(action("close_question",{"question_id":question},True).status_code,200)
                    self.assertEqual(action("subscription",{"user_ids":[11],"revoke":True},True).status_code,200)
                    self.assertEqual(action("delete_learner",{"user_id":22},True).status_code,200)
                    self.assertEqual(action("support",{}).json()["items"],[])
                    # Legacy help import is dry-run by default, idempotent and never queues delivery.
                    import json
                    legacy=json.dumps({"123":{"user_id":11,"question_text":"Archived","asked_at":"25.11.2026 12:00:00"},"124":{"user_id":99999,"question_text":"Missing"}}).encode()
                    self.assertEqual(client.portal.call(ops.import_legacy_help,legacy,-100)["inserted"],0)
                    self.assertEqual(client.portal.call(ops.import_legacy_help,legacy,-100,True)["inserted"],1)
                    self.assertEqual(client.portal.call(ops.import_legacy_help,legacy,-100,True)["existing"],1)
                    self.assertEqual(action("support",{}).json()["items"][0]["status"],"closed")
                    self.assertFalse(client.portal.call(ops.deliver_one,bot))
                    # Support reply with an attachment uses the same explicit delivery queue.
                    reply_file=post_file("upload_attachment?kind=document&filename=answer.txt",b"answer")
                    self.assertEqual(reply_file.status_code,200)
                    question=client.portal.call(ops.create_question,11,"Can I get the file?")
                    reply_id=str(uuid.uuid4())
                    self.assertEqual(action("create_campaign",{"campaign_id":reply_id,"target":"support","question_id":question,"text":"","attachment_id":reply_file.json()["asset_id"]},True).status_code,200)
                    self.assertEqual(action("confirm_campaign",{"campaign_id":reply_id,"expected_count":1},True).status_code,200)
                    bot.send_document=AsyncMock()
                    self.assertTrue(client.portal.call(ops.deliver_one,bot))
                    bot.send_document.assert_awaited_once()
                    self.assertEqual(action("support",{}).json()["items"][0]["status"],"answered")
                    # Uploaded message documents and unknown-send handling use a mock bot.
                    attachment=post_file("upload_attachment?kind=document&filename=guide.txt",b"study guide")
                    self.assertEqual(attachment.status_code,200,attachment.text)
                    cid3=str(uuid.uuid4())
                    self.assertEqual(action("create_campaign",{"campaign_id":cid3,"target":"selected","user_ids":[11],"text":"","attachment_id":attachment.json()["asset_id"]},True).status_code,200)
                    self.assertEqual(action("confirm_campaign",{"campaign_id":cid3,"expected_count":1},True).status_code,200)
                    bot.send_document=AsyncMock(side_effect=TimeoutError())
                    self.assertTrue(client.portal.call(ops.deliver_one,bot))
                    self.assertFalse(client.portal.call(ops.deliver_one,bot))
                    bot.send_document.assert_awaited_once()
                    result=next(r for r in action("campaigns",{}).json()["items"] if r["id"]==cid3)
                    self.assertEqual(result["uncertain"],1)
                    self.assertEqual(action("set_snapshot_auto",{"enabled":False},True).status_code,200)
                    self.assertFalse(action("system_status",{}).json()["auto_snapshots"])
                    self.assertEqual(action("safe_system_events",{}).status_code,200)
                    self.assertEqual(action("request_snapshot",{},True).status_code,200)
                    self.assertEqual(action("request_snapshot",{},True).status_code,400)
                    from tempfile import TemporaryDirectory
                    with TemporaryDirectory() as tmp, patch("tgteacher_bot.core.paths.admins_path",return_value=Path(tmp)/"admins.txt"):
                        self.assertEqual(action("admin_list",{}).json()["ids"],[])
                        self.assertEqual(action("change_admin_list",{"expected_ids":[],"desired_ids":[11]},True).status_code,200)
                        self.assertEqual(action("change_admin_list",{"expected_ids":[],"desired_ids":[22]},True).status_code,400)
                        client.portal.call(ops.admin_list_job,None)
                        self.assertEqual(action("admin_list",{}).json()["ids"],["11"])
                        self.assertEqual(action("change_admin_list",{"expected_ids":[11],"desired_ids":[]},True).status_code,400)
                        self.assertEqual(action("change_admin_list",{"expected_ids":[],"desired_ids":[22]},True).status_code,400)
                    self.assertEqual(action("reports",{"kind":"new","start_date":"2020-01-01","end_date":"2020-01-02"}).json(),[])
                    self.assertEqual(action("reports",{"kind":"new","start_date":"2020-02-01","end_date":"2020-01-01"}).status_code,400)
                    self.assertEqual(action("arbitrary",{},True).status_code,404)
                    cookie=client.cookies.get(web_admin.COOKIE)
                    self.assertEqual(client.post("/api/logout",json={},headers=headers).status_code,200)
                    client.cookies.set(web_admin.COOKIE,cookie)
                    self.assertEqual(client.get("/api/session").status_code,401)
            finally:
                client.portal.call(pool.close)
                client.portal.call(conn.execute,f'DROP SCHEMA "{schema}" CASCADE')
                client.portal.call(conn.close)


class TransferTests(unittest.TestCase):
    def test_reject_traversal_duplicate_and_missing_media(self):
        source='#FAMILY "Safe"\n#WORD ticket\n#TRANSLATION билет\n'
        for entries in (
            [("../lesson.txt",source)],
            [("lesson.txt",source),("LESSON.TXT",source)],
            [("lesson.txt",source+"#STAGE5\n#TASK\n#IMAGE missing.jpg\n#ANSWER ticket\n")],
        ):
            out=io.BytesIO()
            with zipfile.ZipFile(out,"w") as z:
                for name,data in entries:
                    z.writestr(name,data)
            with self.assertRaises(ValueError):
                transfer.parse_archive(out.getvalue())

    def test_legacy_windows_zip_names(self):
        for encoding in ("cp866","cp1251"):
            out=io.BytesIO()
            with zipfile.ZipFile(out,"w") as z:
                z.writestr("lesson/lesson.txt",'#FAMILY "Portable"\n#WORD ticket\n#TRANSLATION билет\n#STAGE5\n#TASK\n#IMAGE фото.png\n#ANSWER ticket\n')
                z.writestr("lesson/xxxx.png",fixture())
            raw=out.getvalue().replace(b"xxxx.png","фото.png".encode(encoding))
            self.assertEqual(len(transfer.parse_archive(raw)[1]),1)

    def test_legacy_help_validation(self):
        import json
        row={"user_id":11,"question_text":"Question","asked_at":"25.11.2026 12:00:00"}
        raw=json.dumps({"1":row}).encode()
        self.assertEqual(ops.parse_legacy_help(raw,-100)[0],("-100:1",11,"Question",row["asked_at"]))
        for invalid in (b"[]", b'{"1":{}}', b'{"0":{}}'):
            with self.assertRaises(ValueError):
                ops.parse_legacy_help(invalid,-100)

    def test_safe_log_never_exposes_message_or_traceback(self):
        import logging
        import json
        from tempfile import TemporaryDirectory
        from tgteacher_bot.services.legacy.safe_events import SafeFormatter,read_events
        record=logging.LogRecord("tgteacher_bot.db.pool",logging.ERROR,"secret.py",1,"secret token %s",("123456",),None)
        record.exc_text="private traceback"
        formatted=SafeFormatter().format(record)
        self.assertNotIn("secret",formatted)
        self.assertNotIn("123456",formatted)
        self.assertNotIn("traceback",formatted)
        with TemporaryDirectory() as temp:
            path=Path(temp)/"safe.jsonl"
            path.write_text(formatted+"\n"+json.dumps({"time":"2026-11-25T12:00:00+00:00","level":"error","category":"bot","message":"SECRET"})+"\n",encoding="utf-8")
            self.assertEqual(len(read_events(path)),1)
            self.assertEqual(read_events(path)[0]["category"],"database")

    def test_invalid_media_rejected(self):
        for kind in ("image","audio"):
            with self.assertRaises(ValueError):
                transfer.prepare_asset(kind,b"invalid")

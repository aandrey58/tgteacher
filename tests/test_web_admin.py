import os
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch
from starlette.testclient import TestClient

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"bot"))
from tgteacher_bot import web_admin
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
                    for mod in (web_admin,user_repo,drafts_repo,families_repo,content_versions,srs_repo,media_repo,tts_repo,image_repo):
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
                    self.assertEqual(action("arbitrary",{},True).status_code,404)
                    cookie=client.cookies.get(web_admin.COOKIE)
                    self.assertEqual(client.post("/api/logout",json={},headers=headers).status_code,200)
                    client.cookies.set(web_admin.COOKIE,cookie)
                    self.assertEqual(client.get("/api/session").status_code,401)
            finally:
                client.portal.call(pool.close)
                client.portal.call(conn.execute,f'DROP SCHEMA "{schema}" CASCADE')
                client.portal.call(conn.close)

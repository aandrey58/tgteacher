import asyncio
import base64
import io
import json
import os
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bot"))
from tgteacher_bot.services.image_providers import ImageProvider, normalize_image, configured_models
from tgteacher_bot.db import image_repo as images, media_repo as media, tts_repo as tts, content_versions as versions, drafts_repo as drafts, families_repo as families, user_repo as users

SOURCE = '#FAMILY "Image travel"\n#WORD ticket\n#TRANSLATION билет\n#STAGE5\n#TASK\n#IMAGE placeholder.jpg\n#ANSWER ticket\n'
ITEM = {"provider":"openrouter","model":"vendor/image","prompt":"A train ticket without text"}


def fixture(color="red", fmt="PNG"):
    buf = io.BytesIO()
    Image.new("RGB",(64,48),color).save(buf,format=fmt)
    return buf.getvalue()


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_three_gateways_and_chat(self):
        calls = []
        encoded = base64.b64encode(fixture()).decode()
        def handler(request):
            calls.append(request)
            body = {"choices":[{"message":{"images":[{"image_url":{"url":"data:image/png;base64,"+encoded}}]}}]} if request.url.path.endswith("chat/completions") else {"data":[{"b64_json":encoded}]}
            return httpx.Response(200,json=body)
        real = httpx.AsyncClient
        for name,surface in (("openrouter","images"),("kodikrouter","images"),("timeweb","images"),("openrouter","chat")):
            p = ImageProvider(name,"vendor/image","1024x1024",100,"secret",surface)
            with patch("tgteacher_bot.services.image_providers.httpx.AsyncClient",side_effect=lambda **kw: real(transport=httpx.MockTransport(handler),**kw)):
                self.assertEqual(await p.generate("ticket"),fixture())
        self.assertEqual([str(r.url) for r in calls],["https://openrouter.ai/api/v1/images","https://api.kodikrouter.ru/v1/images/generations","https://api.timeweb.ai/v1/images/generations","https://openrouter.ai/api/v1/chat/completions"])
        self.assertTrue(calls[1].headers["Idempotency-Key"])
        self.assertEqual(json.loads(calls[2].content)["response_format"],"b64_json")
        self.assertEqual(json.loads(calls[3].content)["modalities"],["image","text"])

    async def test_reject_remote_url(self):
        real = httpx.AsyncClient
        def handler(request):
            return httpx.Response(200,json={"choices":[{"message":{"images":[{"image_url":{"url":"http://127.0.0.1/private"}}]}}]})
        with patch("tgteacher_bot.services.image_providers.httpx.AsyncClient",side_effect=lambda **kw:real(transport=httpx.MockTransport(handler),**kw)):
            with self.assertRaises(ValueError):
                await ImageProvider("openrouter","vendor/image","1024x1024",100,"secret","chat").generate("ticket")

    def test_image_formats_and_validation(self):
        for fmt in ("PNG","JPEG","WEBP"):
            data,w,h = normalize_image(fixture(fmt=fmt))
            self.assertEqual((w,h),(64,48))
            with Image.open(io.BytesIO(data)) as im:
                self.assertEqual(im.format,"JPEG")
                self.assertEqual(im.mode,"RGB")
        with self.assertRaises(Exception):
            normalize_image(b"not an image")
        buf = io.BytesIO()
        Image.new("RGB",(1,1)).save(buf,format="PNG")
        with self.assertRaises(ValueError):
            normalize_image(buf.getvalue())

    def test_configuration_requires_price(self):
        env = {"OPENROUTER_API_KEY":"secret","TGTEACHER_IMAGE_MODELS":json.dumps({"openrouter":{"vendor/image":{"microusd_per_image":100}}})}
        with patch.dict(os.environ,env,clear=True):
            p = configured_models()[("openrouter","vendor/image")]
            self.assertEqual(p.cost_microusd,100)
            self.assertNotIn("secret",repr(p))
        with patch.dict(os.environ,{**env,"TGTEACHER_IMAGE_MODELS":'{"openrouter":{"vendor/image":{}}}'},clear=True):
            with self.assertRaises(ValueError):
                configured_models()


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"),"requires PostgreSQL")
class ImageDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncpg
        dsn = os.environ["TGTEACHER_TEST_DSN"]
        schema = "test_" + uuid.uuid4().hex
        admin = await asyncpg.connect(dsn)
        self.addAsyncCleanup(admin.close)
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        self.addAsyncCleanup(admin.execute,f'DROP SCHEMA "{schema}" CASCADE')
        self.pool = await asyncpg.create_pool(dsn,min_size=1,max_size=4,server_settings={"search_path":schema})
        self.addAsyncCleanup(self.pool.close)
        for module in (images,media,tts,versions,drafts,families,users):
            p = patch.object(module,"get_pool",new=AsyncMock(return_value=self.pool))
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(images,"configured_models",return_value={("openrouter","vendor/image"):ImageProvider("openrouter","vendor/image","1024x1024",100,"secret")})
        p.start()
        self.addCleanup(p.stop)
        p = patch.object(images,"daily_limit",return_value=1000)
        self.budget = p.start()
        self.addCleanup(p.stop)
        await users.init_db_pg()

    async def make_asset(self,prompt,color):
        with patch.object(ImageProvider,"generate",new=AsyncMock(return_value=fixture(color))):
            result = await images.generate_batch([{**ITEM,"prompt":prompt}],100)
        self.assertEqual(result["items"][0]["status"],"ready")
        return result["items"][0]

    async def test_estimate_cache_and_budget(self):
        with patch.object(ImageProvider,"generate",new=AsyncMock(return_value=fixture())) as generate:
            estimate = await images.generate_batch([ITEM,ITEM],0,True)
            self.assertEqual(estimate["estimated_microusd"],100)
            with self.assertRaises(ValueError):
                await images.generate_batch([ITEM],99)
            self.budget.return_value = 150
            with self.assertRaises(ValueError):
                await images.generate_batch([ITEM,{**ITEM,"prompt":"bag"}],200)
            generate.assert_not_awaited()
            result = await images.generate_batch([ITEM,ITEM],100)
            self.assertEqual(result["reserved_microusd"],100)
            self.assertEqual(result["items"][0]["asset_id"],result["items"][1]["asset_id"])
            cached = await images.generate_batch([ITEM],0)
            self.assertTrue(cached["items"][0]["cached"])
            self.assertEqual(cached["reserved_microusd"],0)
            generate.assert_awaited_once()

    async def test_concurrent_reservations(self):
        self.budget.return_value = 100
        entered, release = asyncio.Event(),asyncio.Event()
        async def generate(*args):
            entered.set()
            await release.wait()
            return fixture()
        with patch.object(ImageProvider,"generate",new=AsyncMock(side_effect=generate)) as call:
            first = asyncio.create_task(images.generate_batch([ITEM],100))
            try:
                await asyncio.wait_for(entered.wait(),3)
                duplicate = await images.generate_batch([ITEM],0)
                self.assertEqual(duplicate["items"][0]["status"],"pending")
                with self.assertRaises(ValueError):
                    await images.generate_batch([{**ITEM,"prompt":"bag"}],100)
            finally:
                release.set()
                await first
            call.assert_awaited_once()

    async def test_failed_request_no_retry(self):
        with patch.object(ImageProvider,"generate",new=AsyncMock(return_value=b"bad")) as call:
            self.assertEqual((await images.generate_batch([ITEM],100))["items"][0]["status"],"failed")
            self.assertEqual((await images.generate_batch([ITEM],0))["items"][0]["status"],"failed")
            self.assertEqual((await images.generate_batch([ITEM],0,True))["daily_reserved_microusd"],100)
            call.assert_awaited_once()

    async def test_attach_detach_publish_rollback_and_progress(self):
        a = await self.make_asset("ticket","red")
        b = await self.make_asset("different ticket","blue")
        ident = str(uuid.uuid4())
        await drafts.create_draft(ident,SOURCE)
        self.assertFalse((await drafts.validate_draft(ident))["publishable"])
        with self.assertRaises(ValueError):
            await drafts.publish_draft(ident,1)
        with self.assertRaises(ValueError):
            await media.attach_image("new",ident,1,0,"a"*64)
        await media.attach_image("new",ident,1,0,a["asset_id"])
        self.assertTrue((await drafts.validate_draft(ident))["publishable"])
        await media.detach_media("image","new",ident,2,0)
        self.assertFalse((await drafts.validate_draft(ident))["publishable"])
        await media.attach_image("new",ident,3,0,a["asset_id"])
        fid = (await drafts.publish_draft(ident,4))["family_id"]
        tid = (await versions.get_current_content(fid))["stage5_tasks"][0]["task_id"]
        await users.set_stage5_answer_pg(42,fid,0,"ticket",True)
        eid = str(uuid.uuid4())
        await versions.create_edit_draft(fid,eid,1)
        with self.assertRaises(ValueError):
            await media.attach_image("edit",eid,1,0,b["asset_id"],tid+1)
        await media.detach_media("image","edit",eid,1,0,tid)
        with self.assertRaises(ValueError):
            await versions.publish_edit(eid,2)
        await media.attach_image("edit",eid,2,0,b["asset_id"],tid)
        self.assertTrue((await versions.preview_edit(eid))["valid"])
        await versions.publish_edit(eid,3)
        task = (await versions.get_current_content(fid))["stage5_tasks"][0]
        self.assertEqual(task["task_id"],tid)
        self.assertEqual(task["image"],b["filename"])
        self.assertEqual(await users.get_stage5_answer_pg(42,fid,0),("ticket",True))
        await versions.rollback_family(fid,1,2)
        self.assertEqual((await versions.get_current_content(fid))["stage5_tasks"][0]["image"],a["filename"])
        details = await media.get_media("image",a["asset_id"])
        self.assertEqual([x["version"] for x in details["versions"]],[1,3])
        self.assertEqual((await media.get_media("image",b["asset_id"]))["versions"][0]["version"],2)
        self.assertEqual(len((await media.list_media())["items"]),2)
        self.assertTrue(await images.get_image_bytes(b["filename"]))

    async def test_audio_metadata_and_detach(self):
        import hashlib
        from test_tts import MP3,SOURCE as AUDIO_SOURCE
        aid = hashlib.sha256(MP3).hexdigest()
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO audio_assets(asset_id,mp3) VALUES($1,$2)",aid,MP3)
        ident = str(uuid.uuid4())
        await drafts.create_draft(ident,AUDIO_SOURCE)
        await tts.attach_audio("new",ident,1,0,aid)
        self.assertEqual((await media.list_media("audio"))["items"][0]["asset_id"],aid)
        self.assertEqual((await media.get_media("audio",aid))["stage"],7)
        await media.detach_media("audio","new",ident,2,0)
        self.assertFalse((await drafts.validate_draft(ident))["publishable"])
        self.assertEqual(await tts.get_audio_bytes(tts.asset_filename(aid)),MP3)

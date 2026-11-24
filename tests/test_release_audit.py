"""Release regression tests for access control, startup and portable media."""
import asyncio
import os
import sys
import tempfile
import unittest
import uuid
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))
from telegram.ext import CallbackQueryHandler, MessageHandler, ApplicationHandlerStop, filters
from tgteacher_bot.core.admin_guard import AdminRegistration
from tgteacher_bot.db import user_repo as users, families_repo as families, content_versions as versions
from tgteacher_bot.services.exports import family_upload
from tgteacher_bot.utils.family_parser import parse_family_text
from test_images import fixture
from test_tts import MP3


class AdminGuardTests(unittest.IsolatedAsyncioTestCase):
    async def test_every_wrapped_admin_callback_checks_current_identity(self):
        app = SimpleNamespace(add_handler=lambda handler,group: None)
        registration = AdminRegistration(app)
        action = AsyncMock()
        cb = CallbackQueryHandler(action,pattern="^admin_delete_user_")
        registration.add_handler(cb)
        query = SimpleNamespace(answer=AsyncMock())
        update = SimpleNamespace(effective_user=SimpleNamespace(id=11),effective_chat=SimpleNamespace(id=11,type="private"),callback_query=query)
        with patch("tgteacher_bot.handlers.admin.admin_list.get_admin_ids",return_value=["22"]):
            with self.assertRaises(ApplicationHandlerStop):
                await cb.callback(update,None)
            action.assert_not_awaited()
        with patch("tgteacher_bot.handlers.admin.admin_list.get_admin_ids",return_value=["11"]):
            await cb.callback(update,None)
            action.assert_awaited_once()
            update.effective_chat.type="group"
            with self.assertRaises(ApplicationHandlerStop):
                await cb.callback(update,None)
            self.assertEqual(action.await_count,1)
        handler=MessageHandler(filters.TEXT,action)
        registration.add_handler(handler)
        with patch("tgteacher_bot.handlers.admin.admin_list.get_admin_ids",return_value=[]):
            await handler.callback(update,None)
        self.assertEqual(action.await_count,1)


class PortableMediaTests(unittest.TestCase):
    def test_generated_zip_reimport_rewrites_reserved_names(self):
        import hashlib
        img="img-"+hashlib.sha256(fixture()).hexdigest()+".jpg"
        audio="tts-"+hashlib.sha256(MP3).hexdigest()+".mp3"
        source='#FAMILY "Portable travel"\n#WORD ticket\n#TRANSLATION билет\n#STAGE5\n#TASK\n#IMAGE '+img+'\n#ANSWER ticket\n#STAGE7\n#TASK\n#AUDIO '+audio+'\n#ANSWER ticket\n#CHOICES ticket; hat; bag; coat\n'
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            zip_path=root/"lesson.zip"
            with zipfile.ZipFile(zip_path,"w") as archive:
                archive.writestr("group/lesson.txt",source)
                archive.writestr("group/"+img,fixture(fmt="JPEG"))
                archive.writestr("group/"+audio,MP3)
            with patch.object(family_upload,"FAMILIES_DIR",tmp):
                data,folder=family_upload.validate_and_extract_family_zip(str(zip_path))
            path=Path(folder)
            for key,field in (("stage5_tasks","image"),("stage7_tasks","audio")):
                name=data[key][0][field]
                self.assertTrue(name.startswith("imported-"))
                self.assertTrue((path/name).is_file())
            disk=parse_family_text(next(path.glob("*.txt")).read_text(encoding="utf-8"))
            self.assertEqual(disk["stage5_tasks"][0]["image"],data["stage5_tasks"][0]["image"])
            self.assertEqual(disk["stage7_tasks"][0]["audio"],data["stage7_tasks"][0]["audio"])


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"),"requires PostgreSQL")
class StartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_fresh_initialization(self):
        import asyncpg
        dsn=os.environ["TGTEACHER_TEST_DSN"]
        schema="test_"+uuid.uuid4().hex
        admin=await asyncpg.connect(dsn)
        self.addAsyncCleanup(admin.close)
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        self.addAsyncCleanup(admin.execute,f'DROP SCHEMA "{schema}" CASCADE')
        pool=await asyncpg.create_pool(dsn,min_size=1,max_size=4,server_settings={"search_path":schema})
        self.addAsyncCleanup(pool.close)
        with patch.object(users,"get_pool",new=AsyncMock(return_value=pool)):
            await asyncio.gather(users.init_db_pg(),users.init_db_pg())
        async with pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM content_schema_migrations"),2)
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM learner_preferences"),0)
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM pg_trigger WHERE tgname='srs_learning_event' AND tgrelid='user_task_progress'::regclass"),1)

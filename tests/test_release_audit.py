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



class CutoverTests(unittest.IsolatedAsyncioTestCase):
    def build(self, legacy, support):
        from tgteacher_bot.core import app
        with patch.dict(os.environ,{"BOT_TOKEN":"123456:ci-only-test-token","TGTEACHER_LEGACY_ADMIN":legacy,"TGTEACHER_WEB_SUPPORT":support}):
            return app.build_application()

    async def test_handler_inventory_switch_and_rollback(self):
        from tgteacher_bot.core import app
        def inventory(application):
            return [(group,h) for group,handlers in application.handlers.items() for h in handlers]
        legacy=self.build("1","0")
        web=self.build("0","1")
        rollback=self.build("1","0")
        def callbacks(a):
            return [(g,h.pattern.pattern,h.callback.__name__) for g,h in inventory(a)
                    if isinstance(h,CallbackQueryHandler) and hasattr(h.pattern,"pattern")]
        before=callbacks(legacy)
        after=callbacks(web)
        self.assertEqual(before,callbacks(rollback))
        retired=[x for x in before if x[1].startswith(("^admin_","^snapshots_","^status_info_","^payment_option_"))]
        self.assertGreater(len(retired),50)
        self.assertTrue(all(x not in after for x in retired))
        self.assertEqual([x[2] for x in after if x[1]==app.LEGACY_ADMIN_CALLBACK],["_legacy_admin_retired"])
        for pattern in ("^main_menu$","^subscription$","^confirm_payment_","^check_payment_","^help$","^help_ask$","^noop$"):
            self.assertTrue(any(x[1]==pattern for x in after),pattern)
        self.assertTrue(any(h.callback.__name__=="help_group_reply_handler" for _,h in inventory(legacy)))
        self.assertFalse(any(h.callback.__name__=="help_group_reply_handler" for _,h in inventory(web)))
        self.assertTrue(any(h.callback.__name__=="help_user_text_handler" for _,h in inventory(web)))
        old_names={h.callback.__name__ for _,h in inventory(legacy)}
        new_names={h.callback.__name__ for _,h in inventory(web)}
        self.assertIn("start",new_names)
        self.assertFalse(any(name.startswith("admin_") for name in new_names))
        self.assertGreater(len(old_names-new_names),30)
        query=SimpleNamespace(answer=AsyncMock())
        update=SimpleNamespace(callback_query=query)
        for allowed in (True,False):
            with patch.object(app,"is_admin",return_value=allowed):
                with self.assertRaises(ApplicationHandlerStop):
                    await app._legacy_admin_retired(update,None)
        self.assertEqual(query.answer.await_count,2)
        await app._noop_callback(update,None)
        self.assertEqual(query.answer.await_count,3)

    async def test_menu_and_web_jobs_independent_of_legacy_registration(self):
        from tgteacher_bot.core import app
        with tempfile.TemporaryDirectory() as tmp:
            admins=Path(tmp)/"admins.txt"
            admins.write_text("11"+chr(10),encoding="utf-8")
            with patch.object(app.paths,"admins_path",return_value=admins),patch.dict(os.environ,{"TGTEACHER_LEGACY_ADMIN":"0","TGTEACHER_WEB_ADMIN_URL":"https://example.test/admin"}):
                for uid in (11,22):
                    buttons=[b for row in app.get_main_menu(uid).inline_keyboard for b in row]
                    self.assertNotIn("admin_panel",[b.callback_data for b in buttons])
                    self.assertEqual(any(b.url=="https://example.test/admin" for b in buttons),uid==11)
                with patch.dict(os.environ,{"TGTEACHER_LEGACY_ADMIN":"1"}):
                    self.assertIn("admin_panel",[b.callback_data for row in app.get_main_menu(11).inline_keyboard for b in row])
        queue=SimpleNamespace(run_repeating=lambda fn,**kwargs: scheduled.append(fn.__name__))
        scheduled=[]
        with patch.dict(os.environ,{"TGTEACHER_LEGACY_ADMIN":"0","TGTEACHER_WEB_SUPPORT":"1"}),patch.object(app,"init_db_pg",new=AsyncMock()),patch.object(app,"init_system_snapshots_pg",new=AsyncMock()),patch("tgteacher_bot.utils.families_data.refresh_families_meta",new=AsyncMock()):
            await app._post_startup(SimpleNamespace(job_queue=queue))
        self.assertTrue({"delivery_job","snapshot_job","admin_list_job","auto_snapshot_job"}<=set(scheduled))

    async def test_web_help_persists_before_ack_and_stops_other_handlers(self):
        from tgteacher_bot.handlers.user import help as support
        calls=[]
        async def create(uid,text):
            calls.append(("save",uid,text))
        async def ack(*args,**kwargs):
            calls.append(("ack",))
        message=SimpleNamespace(text="question",message_id=1,delete=AsyncMock(),reply_text=AsyncMock(),chat=SimpleNamespace(send_message=AsyncMock(side_effect=ack)))
        update=SimpleNamespace(message=message,effective_user=SimpleNamespace(id=11),effective_chat=SimpleNamespace(id=11,type="private"))
        context=SimpleNamespace(user_data={support.AWAITING_HELP_QUESTION:True},bot=SimpleNamespace(send_message=AsyncMock(),delete_message=AsyncMock()))
        with patch.dict(os.environ,{"TGTEACHER_WEB_SUPPORT":"1"}),patch.object(support,"mark_user_active_if_needed",new=AsyncMock()),patch("tgteacher_bot.db.web_operations.create_question",new=AsyncMock(side_effect=create)):
            with self.assertRaises(ApplicationHandlerStop):
                await support.help_user_text_handler(update,context)
        self.assertEqual(calls,[("save",11,"question"),("ack",)])
        self.assertNotIn(support.AWAITING_HELP_QUESTION,context.user_data)
        context.bot.send_message.assert_not_awaited()
        context.user_data[support.AWAITING_HELP_QUESTION]=True
        with patch.dict(os.environ,{"TGTEACHER_WEB_SUPPORT":"1"}),patch.object(support,"mark_user_active_if_needed",new=AsyncMock()),patch("tgteacher_bot.db.web_operations.create_question",new=AsyncMock(side_effect=RuntimeError("DB unavailable"))):
            with self.assertRaises(ApplicationHandlerStop):
                await support.help_user_text_handler(update,context)
        self.assertTrue(context.user_data[support.AWAITING_HELP_QUESTION])
        self.assertEqual(len(calls),2)
        message.reply_text.assert_awaited_once()
        # The old group reply route cannot send in web mode or for a non-admin.
        with patch.dict(os.environ,{"TGTEACHER_WEB_SUPPORT":"1"}):
            await support.help_group_reply_handler(update,context)
        with patch.dict(os.environ,{"TGTEACHER_WEB_SUPPORT":"0"}),patch("tgteacher_bot.handlers.admin.admin_list.get_admin_ids",return_value=[]):
            await support.help_group_reply_handler(update,context)
        context.bot.send_message.assert_not_awaited()

    async def test_legacy_help_ack_only_after_delivery_and_index(self):
        from tgteacher_bot.handlers.user import help as support
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/"support.json"
            calls=[]
            async def deliver(**kwargs):
                calls.append("deliver")
                self.assertIn("&lt;question&gt;",kwargs["text"])
                return SimpleNamespace(message_id=77)
            async def ack(*args,**kwargs):
                self.assertTrue(path.exists())
                import json
                self.assertEqual(json.loads(path.read_text())["77"]["user_id"],11)
                calls.append("ack")
            message=SimpleNamespace(text="<question>",message_id=1,delete=AsyncMock(),reply_text=AsyncMock(),
                chat=SimpleNamespace(send_message=AsyncMock(side_effect=ack)))
            update=SimpleNamespace(message=message,effective_user=SimpleNamespace(id=11,username="student",first_name="<Name>"),
                effective_chat=SimpleNamespace(id=11,type="private"))
            ctx=SimpleNamespace(user_data={},application=SimpleNamespace(bot_data={}),
                bot=SimpleNamespace(send_message=AsyncMock(side_effect=deliver),delete_message=AsyncMock()))
            with patch.object(support,"HELP_INDEX_FILE_PATH",str(path)),patch.object(support,"mark_user_active_if_needed",new=AsyncMock()),patch.dict(os.environ,{"TGTEACHER_WEB_SUPPORT":"0","HELP_QUESTIONS_GROUP_ID":"-100123"}):
                for failure in ("config","telegram","disk"):
                    ctx.user_data[support.AWAITING_HELP_QUESTION]=True
                    ctx.user_data[support.HELP_PROMPT_MESSAGE_ID]=8
                    with patch.dict(os.environ,{"HELP_QUESTIONS_GROUP_ID":"" if failure=="config" else "-100123"}),patch.object(ctx.bot.send_message,"side_effect",RuntimeError("send failed") if failure=="telegram" else deliver),patch.object(support,"save_help_index_to_disk",return_value=False) if failure=="disk" else patch.object(support,"save_help_index_to_disk",wraps=support.save_help_index_to_disk):
                        with self.assertRaises(ApplicationHandlerStop):
                            await support.help_user_text_handler(update,ctx)
                    self.assertTrue(ctx.user_data[support.AWAITING_HELP_QUESTION])
                    self.assertEqual(ctx.user_data[support.HELP_PROMPT_MESSAGE_ID],8)
                    message.delete.assert_not_awaited()
                    message.chat.send_message.assert_not_awaited()
                self.assertEqual(message.reply_text.await_count,3)
                with self.assertRaises(ApplicationHandlerStop):
                    await support.help_user_text_handler(update,ctx)
                self.assertEqual(calls[-2:],["deliver","ack"])
                self.assertNotIn(support.AWAITING_HELP_QUESTION,ctx.user_data)
                message.delete.assert_awaited_once()
                ctx.bot.delete_message.assert_awaited_once_with(11,8)
                # A failed replace never corrupts the last durable index.
                old=path.read_bytes()
                with patch.object(support.os,"replace",side_effect=OSError("disk failure")):
                    self.assertFalse(support.save_help_index_to_disk(ctx))
                self.assertEqual(path.read_bytes(),old)
                self.assertEqual(list(Path(tmp).iterdir()),[path])


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

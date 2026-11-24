import asyncio
import os
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))
from tgteacher_bot.db import tts_repo as tts, content_versions as versions, drafts_repo as drafts, families_repo as families, user_repo as users
from tgteacher_bot.services.tts_providers import Provider, check_mp3, configured_providers

# Synthetic MPEG frame headers and padding for structural validation; no speech or external API.
MP3 = (bytes.fromhex("fffb9064") + bytes(413)) * 8
OTHER_MP3 = MP3 + (bytes.fromhex("fffb9064") + bytes(413))
SOURCE = '#FAMILY "Listening travel"\n#WORD ticket\n#TRANSLATION билет\n#STAGE7\n#TASK\n#AUDIO placeholder.mp3\n#ANSWER ticket\n#CHOICES ticket; hat; bag; coat\n'
ITEM = {"provider": "openai", "voice": "alloy", "text": "ticket"}


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_provider_payloads(self):
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(200, content=MP3)
        real = httpx.AsyncClient
        for p in (Provider("openai", "tts-1", ("alloy",), 20, "secret"),
                  Provider("elevenlabs", "eleven_multilingual_v2", ("voice123",), 100, "secret")):
            with patch("tgteacher_bot.services.tts_providers.httpx.AsyncClient",
                       side_effect=lambda **kw: real(transport=httpx.MockTransport(handler), **kw)):
                self.assertEqual(await p.synthesize("ticket", p.voices[0]), MP3)
        import json
        self.assertEqual(str(calls[0].url), "https://api.openai.com/v1/audio/speech")
        self.assertEqual(json.loads(calls[0].content)["response_format"], "mp3")
        self.assertEqual(calls[0].headers["Authorization"], "Bearer secret")
        self.assertEqual(calls[1].url.path, "/v1/text-to-speech/voice123")
        self.assertEqual(calls[1].url.params["output_format"], "mp3_44100_128")
        self.assertEqual(json.loads(calls[1].content)["text"], "ticket")

    def test_configuration_and_mp3(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(configured_providers(), {})
        self.assertEqual(check_mp3(MP3), MP3)
        for invalid in (b"", b'{"error":"bad"}'*40, b"x"*(5*1024*1024+1)):
            with self.assertRaises(ValueError):
                check_mp3(invalid)
        with patch.dict(os.environ, {"OPENAI_API_KEY": "secret", "TGTEACHER_TTS_OPENAI_MICROUSD_PER_BYTE": "20",
                                     "TGTEACHER_TTS_DAILY_MICROUSD": "500"}, clear=True):
            result = tts.list_providers()
            self.assertEqual(result["providers"][0]["provider"], "openai")
            self.assertNotIn("secret", str(result))
            self.assertIn("alloy", tts.list_voices("openai")["voices"])


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"), "requires PostgreSQL")
class TTSDatabaseTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncpg
        dsn = os.environ["TGTEACHER_TEST_DSN"]
        schema = "test_" + uuid.uuid4().hex
        admin = await asyncpg.connect(dsn)
        self.addAsyncCleanup(admin.close)
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        self.addAsyncCleanup(admin.execute, f'DROP SCHEMA "{schema}" CASCADE')
        self.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4, server_settings={"search_path": schema})
        self.addAsyncCleanup(self.pool.close)
        for module in (tts, versions, drafts, families, users):
            p = patch.object(module, "get_pool", new=AsyncMock(return_value=self.pool))
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(tts, "configured_providers", return_value={"openai": Provider("openai", "tts-1", ("alloy",), 20, "test")})
        p.start()
        self.addCleanup(p.stop)
        p = patch.object(tts, "daily_limit", return_value=1000)
        self.budget = p.start()
        self.addCleanup(p.stop)
        await users.init_db_pg()

    async def generate(self, text, data=MP3):
        with patch.object(Provider, "synthesize", new=AsyncMock(return_value=data)):
            result = await tts.generate_batch([{**ITEM, "text": text}], 1000)
        self.assertEqual(result["items"][0]["status"], "ready")
        return result["items"][0]

    async def test_batch_budget_cache_and_duplicate(self):
        with patch.object(Provider, "synthesize", new=AsyncMock(return_value=MP3)) as call:
            estimate = await tts.estimate([ITEM, ITEM])
            self.assertEqual(estimate["estimated_microusd"], 120)
            with self.assertRaises(ValueError):
                await tts.generate_batch([ITEM], 119)
            self.budget.return_value = 200
            with self.assertRaises(ValueError):
                await tts.generate_batch([ITEM, {**ITEM, "text": "luggage"}], 1000)
            call.assert_not_awaited()
            result = await tts.generate_batch([ITEM, ITEM], 120)
            self.assertEqual(result["reserved_microusd"], 120)
            self.assertEqual(result["items"][0]["asset_id"], result["items"][1]["asset_id"])
            cached = await tts.generate_batch([ITEM], 0)
            self.assertEqual(cached["reserved_microusd"], 0)
            self.assertTrue(cached["items"][0]["cached"])
            self.assertEqual(await tts.get_audio_bytes(result["items"][0]["filename"]), MP3)
            call.assert_awaited_once()

    async def test_concurrent_reservations_and_no_double_charge(self):
        started, release = asyncio.Event(), asyncio.Event()
        async def synth(*args):
            started.set()
            await release.wait()
            return MP3
        self.budget.return_value = 200
        with patch.object(Provider, "synthesize", new=AsyncMock(side_effect=synth)) as call:
            first = asyncio.create_task(tts.generate_batch([ITEM], 120))
            try:
                await asyncio.wait_for(started.wait(), 3)
                duplicate = await tts.generate_batch([ITEM], 0)
                self.assertEqual(duplicate["items"][0]["status"], "pending")
                with self.assertRaises(ValueError):
                    await tts.generate_batch([{**ITEM, "text": "luggage"}], 1000)
            finally:
                release.set()
                await first
            call.assert_awaited_once()

    async def test_failure_reserved_and_no_retry(self):
        with patch.object(Provider, "synthesize", new=AsyncMock(return_value=b"not mp3")) as call:
            result = await tts.generate_batch([ITEM], 120)
            self.assertEqual(result["items"][0]["status"], "failed")
            self.assertIsNone(result["items"][0]["asset_id"])
            retry = await tts.generate_batch([ITEM], 0)
            self.assertEqual(retry["items"][0]["status"], "failed")
            self.assertEqual((await tts.estimate([ITEM]))["daily_reserved_microusd"], 120)
            call.assert_awaited_once()

    async def test_attach_publish_revision_history_and_rollback(self):
        first = await self.generate("ticket")
        second = await self.generate("your ticket", OTHER_MP3)
        ident = str(uuid.uuid4())
        await drafts.create_draft(ident, SOURCE)
        self.assertFalse((await drafts.validate_draft(ident))["publishable"])
        with self.assertRaises(ValueError):
            await drafts.publish_draft(ident, 1)
        with self.assertRaises(ValueError):
            await tts.attach_audio("new", ident, 1, 0, "a"*64)
        attached = await tts.attach_audio("new", ident, 1, 0, first["asset_id"])
        self.assertEqual(attached["revision"], 2)
        self.assertTrue((await drafts.validate_draft(ident))["publishable"])
        with self.assertRaises(ValueError):
            await tts.attach_audio("new", ident, 1, 0, second["asset_id"])
        published = await drafts.publish_draft(ident, 2)
        fid = published["family_id"]
        current = await versions.get_current_content(fid)
        task_id = current["stage7_tasks"][0]["task_id"]
        self.assertEqual(current["stage7_tasks"][0]["audio"], first["filename"])
        await users.set_stage7_answer_pg(42, fid, 0, "ticket", True)
        edit_id = str(uuid.uuid4())
        await versions.create_edit_draft(fid, edit_id, 1)
        with self.assertRaises(ValueError):
            await tts.attach_audio("edit", edit_id, 1, 0, second["asset_id"], task_id+1)
        result = await tts.attach_audio("edit", edit_id, 1, 0, second["asset_id"], task_id)
        self.assertEqual(result["revision"], 2)
        self.assertTrue((await versions.preview_edit(edit_id))["valid"])
        await versions.publish_edit(edit_id, 2)
        current = await versions.get_current_content(fid)
        self.assertEqual(current["stage7_tasks"][0]["task_id"], task_id)
        self.assertEqual(current["stage7_tasks"][0]["audio"], second["filename"])
        self.assertEqual(await users.get_stage7_answer_pg(42, fid, 0), ("ticket", True))
        await versions.rollback_family(fid, 1, 2)
        self.assertEqual((await versions.get_current_content(fid))["stage7_tasks"][0]["audio"], first["filename"])
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT version,task_id,asset_id FROM family_audio_versions WHERE family_id=$1 ORDER BY version", fid)
        self.assertEqual([(r["version"], r["task_id"], r["asset_id"]) for r in rows],
                         [(1, task_id, first["asset_id"]), (2, task_id, second["asset_id"]), (3, task_id, first["asset_id"])])
        self.assertEqual(await tts.get_audio_bytes(second["filename"]), OTHER_MP3)

    async def test_invalid_provider_and_batch(self):
        for items in ([], [ITEM]*11, [{**ITEM, "voice": "../x"}],
                      [{**ITEM, "text": "x"*2001}], [{**ITEM, "provider": "unknown"}]):
            with self.assertRaises(ValueError):
                await tts.generate_batch(items, 1000)

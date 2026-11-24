import asyncio
import copy
import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))

from tgteacher_bot.utils.content_validation import (
    ContentValidationError, DuplicateFamilyError, validate_family,
)
from tgteacher_bot.utils.family_parser import parse_family_txt


def sample():
    return {
        "name": "Travel", "target": "FREE",
        "words": [{"word": "ticket", "translation": "билет"}],
        "stage2_tasks": [{"sentence": "Show your ___ to board.", "answer": "ticket",
                         "choices": ["ticket", "bag", "hat", "coat"]}],
        "stage3_tasks": [{"word": "ticket", "definition": "A document for travel.",
                         "choices": ["ticket", "bag", "hat", "coat"]}],
        "stage4_tasks": [{"sentence": "I bought a train ___.", "answer": "ticket",
                         "choices": ["ticket", "bag", "hat", "coat"]}],
        "stage5_tasks": [{"image": "ticket.jpg", "answer": "ticket",
                         "alternatives": ["a ticket"]}],
        "stage6_tasks": [{"word": "luggage", "synonyms": ["baggage"],
                         "wrong_synonyms": ["ticket", "hat", "coat"]}],
        "stage7_tasks": [{"audio": "ticket.mp3", "answer": "ticket",
                         "choices": ["ticket", "bag", "hat", "coat"]}],
        "stage8_tasks": [{"text": "I bought a ___.", "answers": ["ticket"]}],
    }


class ValidationTests(unittest.TestCase):
    def test_all_stages(self):
        self.assertEqual(validate_family(sample())["name"], "Travel")

    def test_real_english_files(self):
        for path in (ROOT / "families").rglob("*.txt"):
            with self.subTest(path=path):
                validate_family(parse_family_txt(str(path)))

    def test_invalid_inputs(self):
        cases = [
            ("words", [{"word": "ticket", "translation": ""}]),
            ("words", [{"word": "ticket", "translation": "билет"},
                       {"word": "TICKET", "translation": "билет"}]),
            ("target", "INVALID"),
            ("name", "../unsafe"),
            ("stage2_tasks", [{"sentence": "I bought ___.", "answer": "ticket",
                               "choices": ["bag", "hat", "coat", "shoe"]}]),
            ("stage3_tasks", [{"word": "ticket", "definition": "A document",
                               "choices": ["ticket", "bag"]}]),
            ("stage4_tasks", [{"sentence": "No blank", "answer": "ticket",
                               "choices": ["ticket", "bag", "hat", "coat"]}]),
            ("stage5_tasks", [{"answer": "ticket", "image": "../ticket.jpg"}]),
            ("stage6_tasks", [{"word": "ticket", "synonyms": ["pass"],
                               "wrong_synonyms": ["PASS", "hat", "bag"]}]),
            ("stage7_tasks", [{"answer": "ticket", "audio": "ticket.exe",
                               "choices": ["ticket", "hat", "bag", "coat"]}]),
            ("stage8_tasks", [{"text": "___ ___", "answers": ["ticket"]}]),
            ("stage8_tasks", [{"text": "___ ___", "answers": ["ticket", "bag"], "equal": "1=2"}]),
        ]
        for key, value in cases:
            with self.subTest(field=key, value=value):
                family = sample()
                family[key] = value
                with self.assertRaises(ContentValidationError):
                    validate_family(family)

    def test_media_existence(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ContentValidationError):
                validate_family(sample(), directory)
            (Path(directory) / "ticket.jpg").write_bytes(b"placeholder")
            (Path(directory) / "ticket.mp3").write_bytes(b"placeholder")
            validate_family(sample(), directory)


@unittest.skipUnless(os.getenv("TGTEACHER_TEST_DSN"), "requires isolated PostgreSQL")
class PostgresTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import asyncpg
        from tgteacher_bot.db import families_repo, user_repo
        self.repo = families_repo
        dsn = os.environ["TGTEACHER_TEST_DSN"]
        self.schema = "test_" + uuid.uuid4().hex
        self.admin = await asyncpg.connect(dsn)
        self.addAsyncCleanup(self.admin.close)
        await self.admin.execute(f'CREATE SCHEMA "{self.schema}"')
        self.addAsyncCleanup(self.admin.execute, f'DROP SCHEMA "{self.schema}" CASCADE')
        self.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4,
                                              server_settings={"search_path": self.schema})
        self.addAsyncCleanup(self.pool.close)
        for module in (families_repo, user_repo):
            patcher = patch.object(module, "get_pool", new=AsyncMock(return_value=self.pool))
            patcher.start()
            self.addCleanup(patcher.stop)
        await user_repo.init_db_pg()

    async def test_all_eight_stages_saved(self):
        ident = await self.repo.save_family_to_pg(sample(), "travel")
        self.assertGreater(ident, 0)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM families"), 1)
            for table in ["stage1_words", *(f"stage{i}_tasks" for i in range(2, 9))]:
                self.assertEqual(await conn.fetchval(f"SELECT count(*) FROM {table} WHERE family_id=$1", ident), 1)

    async def test_invalid_import_does_not_write(self):
        family = sample()
        family["stage8_tasks"][0]["answers"] = []
        with self.assertRaises(ContentValidationError):
            await self.repo.save_family_to_pg(family)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM families"), 0)

    async def test_rollback_on_late_database_failure(self):
        import asyncpg
        async with self.pool.acquire() as conn:
            await conn.execute("ALTER TABLE stage8_tasks ADD CONSTRAINT fail_insert CHECK (false)")
        with self.assertRaises(asyncpg.CheckViolationError):
            await self.repo.save_family_to_pg(sample())
        async with self.pool.acquire() as conn:
            for table in ["families", "stage1_words", *(f"stage{i}_tasks" for i in range(2, 9))]:
                self.assertEqual(await conn.fetchval(f"SELECT count(*) FROM {table}"), 0)

    async def test_concurrent_duplicate_and_separate_access(self):
        one = sample()
        two = copy.deepcopy(one)
        two["name"] = "  TRAVEL "
        results = await asyncio.gather(
            self.repo.save_family_to_pg(one), self.repo.save_family_to_pg(two),
            return_exceptions=True,
        )
        self.assertEqual(sum(isinstance(x, int) for x in results), 1)
        self.assertEqual(sum(isinstance(x, DuplicateFamilyError) for x in results), 1)
        two["target"] = "VIP"
        await self.repo.save_family_to_pg(two)
        async with self.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM families"), 2)

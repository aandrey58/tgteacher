import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))

from tgteacher_bot.utils.english_answers import is_correct_answer
from tgteacher_bot.utils.family_parser import parse_family_txt


class EnglishAnswersTests(unittest.TestCase):
    def test_case_whitespace_and_apostrophes(self):
        self.assertTrue(is_correct_answer("  APPLE  ", "apple"))
        self.assertTrue(is_correct_answer("look   after", "look after"))
        self.assertTrue(is_correct_answer("Don’t", "don't"))

    def test_articles_are_not_required_unless_authored(self):
        self.assertTrue(is_correct_answer("umbrella", "umbrella"))
        self.assertTrue(is_correct_answer("An umbrella", "umbrella", ["an umbrella"]))
        self.assertFalse(is_correct_answer("a umbrella", "umbrella", ["an umbrella"]))
        self.assertFalse(is_correct_answer("a luggage", "luggage"))
        self.assertFalse(is_correct_answer("end", "the end"))

    def test_explicit_spelling_variants(self):
        self.assertTrue(is_correct_answer("COLOR", "colour", ["color"]))
        self.assertFalse(is_correct_answer("colur", "colour", ["color"]))
        self.assertFalse(is_correct_answer("apples", "apple"))
        self.assertFalse(is_correct_answer("   ", "apple"))
        self.assertFalse(is_correct_answer("", ""))


class EnglishContentTests(unittest.TestCase):
    def test_demo_family(self):
        family = parse_family_txt(str(ROOT / "families" / "Fruits & Vegetables" / "Fruits & Vegetables.txt"))
        self.assertEqual(len(family["words"]), 10)
        for stage in (2, 3, 4):
            tasks = family[f"stage{stage}_tasks"]
            self.assertEqual(len(tasks), 10)
            for task in tasks:
                answer = task["word"] if stage == 3 else task["answer"]
                self.assertIn(answer, task["choices"])
                self.assertEqual(len(set(task["choices"])), 4)
        task = family["stage8_tasks"][0]
        self.assertEqual(task["text"].count("___"), 10)
        self.assertEqual(len(task["answers"]), 10)
        self.assertEqual(len(task["explanations"]), 10)

    def test_template_is_parseable_without_media(self):
        family = parse_family_txt(str(ROOT / "families" / "family_example.txt"))
        self.assertEqual(family["name"], "Travel essentials")
        self.assertEqual(len(family["words"]), 3)
        self.assertFalse(family["stage5_tasks"])
        self.assertFalse(family["stage7_tasks"])
        self.assertEqual(len(family["stage8_tasks"][0]["answers"]), 3)

    def test_malformed_cloze_is_rejected(self):
        content = '#FAMILY "Bad cloze"\n#WORD apple\n#TRANSLATION яблоко\n#STAGE8\n#TEXT\nAn ___ is a fruit.\n#ANSWER1 apple\n#ANSWER2 banana\n'
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "bad.txt"
            path.write_text(content, encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Этап 8"):
                parse_family_txt(str(path))

    def test_legacy_fixture_import_format(self):
        fixtures = sorted((ROOT / "test").glob("family_*.txt"))
        self.assertTrue(fixtures)
        for path in fixtures:
            with self.subTest(path=path.name):
                self.assertTrue(parse_family_txt(str(path))["words"])


if __name__ == "__main__":
    unittest.main()

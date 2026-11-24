import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bot"))

from tgteacher_bot.services.exports import family_upload
from tgteacher_bot.utils.content_validation import ContentValidationError


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.families = self.root / "families"
        self.families.mkdir()
        patcher = patch.object(family_upload, "FAMILIES_DIR", str(self.families))
        patcher.start()
        self.addCleanup(patcher.stop)

    def make_archive(self, entries):
        archive = self.root / "lesson.zip"
        with zipfile.ZipFile(archive, "w") as z:
            for name, contents in entries.items():
                z.writestr(name, contents)
        return archive

    def test_valid_archive(self):
        txt = (ROOT / "families" / "family_example.txt").read_text(encoding="utf-8")
        archive = self.make_archive({"Travel/lesson.txt": txt})
        family, folder = family_upload.validate_and_extract_family_zip(str(archive))
        self.assertEqual(family["name"], "Travel essentials")
        self.assertTrue((Path(folder) / "Travel essentials.txt").is_file())
        self.assertEqual(list(self.families.iterdir()), [Path(folder)])

    def test_traversal_rejected_without_writing(self):
        archive = self.make_archive({"Travel/../../outside.txt": "bad"})
        with self.assertRaises(family_upload.FamilyArchiveError):
            family_upload.validate_and_extract_family_zip(str(archive))
        self.assertFalse((self.root / "outside.txt").exists())
        self.assertEqual(list(self.families.iterdir()), [])

    def test_missing_media_cleans_staging(self):
        txt = '#FAMILY "Travel"\n#WORD ticket\n#TRANSLATION билет\n#STAGE5\n#TASK\n#IMAGE ticket.jpg\n#ANSWER ticket\n'
        archive = self.make_archive({"Travel/lesson.txt": txt})
        with self.assertRaises(ContentValidationError):
            family_upload.validate_and_extract_family_zip(str(archive))
        self.assertEqual(list(self.families.iterdir()), [])

"""Structural checks shared by Telegram imports and future MCP tools."""
from pathlib import Path
import re

from tgteacher_bot.utils.english_answers import normalize_english_answer as norm


class ContentValidationError(ValueError):
    pass


class DuplicateFamilyError(ContentValidationError):
    pass


def _fail(where, message):
    raise ContentValidationError(f"{where}: {message}")


def _text(obj, key, where):
    value = obj.get(key)
    if not isinstance(value, str) or not value.strip():
        _fail(where, f"{key} must be nonempty text")
    return value


def _list(obj, key, where, minimum=0, unique=False):
    values = obj.get(key, [])
    if not isinstance(values, list) or len(values) < minimum:
        _fail(where, f"{key} requires at least {minimum} entries")
    if any(not isinstance(v, str) or not v.strip() for v in values):
        _fail(where, f"{key} contains an empty or invalid entry")
    if unique and len({norm(v) for v in values}) != len(values):
        _fail(where, f"{key} contains duplicate entries")
    return values


def _records(obj, key, minimum=0):
    values = obj.get(key, [])
    if not isinstance(values, list) or len(values) < minimum or any(not isinstance(v, dict) for v in values):
        _fail(key, f"expected a list of at least {minimum} objects")
    return values


def family_identity(name, target):
    return norm(name), (target or "VIP+FREE").upper()


def _media(task, key, where, extensions, media_dir):
    name = _text(task, key, where)
    if name in (".", "..") or "/" in name or "\\" in name or ":" in name or "\x00" in name:
        _fail(where, f"{key} must be a local filename")
    if Path(name).suffix.lower() not in extensions:
        _fail(where, f"unsupported {key} format")
    if media_dir is not None:
        root = Path(media_dir).resolve()
        matches = [p for p in root.iterdir() if p.name.casefold() == name.casefold()] if root.is_dir() else []
        if len(matches) != 1 or not matches[0].is_file() or matches[0].is_symlink():
            _fail(where, f"missing or ambiguous media: {name}")


def validate_family(family, media_dir=None):
    """Validate publishable content; raise with the offending stage/field.

    File existence is checked when a media directory is supplied. Semantic
    correctness of translations and distractors still needs human review.
    """
    name = _text(family, "name", "family")
    if name in (".", "..") or any(c in name for c in '/\\:\x00'):
        _fail("family", "unsafe name")
    if family.get("target", "VIP+FREE") not in ("FREE", "VIP", "VIP+FREE"):
        _fail("family", "invalid target")
    seen = set()
    for i, word in enumerate(_records(family, "words", 1), 1):
        where = f"stage 1 #{i}"
        key = (norm(_text(word, "word", where)), norm(_text(word, "translation", where)))
        if key in seen:
            _fail(where, "duplicate word and translation")
        seen.add(key)
    for stage in range(2, 9):
        for i, task in enumerate(_records(family, f"stage{stage}_tasks"), 1):
            where = f"stage {stage} #{i}"
            if stage in (2, 3, 4, 7):
                answer = _text(task, "word" if stage == 3 else "answer", where)
                choices = _list(task, "choices", where, 4, unique=True)
                if answer not in choices:
                    _fail(where, "correct answer is missing from choices")
                if stage in (2, 4):
                    if _text(task, "sentence", where).count("___") != 1:
                        _fail(where, "expected exactly one blank")
                if stage == 3:
                    _text(task, "definition", where)
                if stage == 7:
                    _media(task, "audio", where, {".mp3"}, media_dir)
            elif stage == 5:
                _text(task, "answer", where)
                _list(task, "alternatives", where, unique=True)
                _media(task, "image", where, {".jpg", ".jpeg", ".png"}, media_dir)
            elif stage == 6:
                _text(task, "word", where)
                synonyms = _list(task, "synonyms", where, 1, unique=True)
                wrong = _list(task, "wrong_synonyms", where, 1, unique=True)
                if len(synonyms) > 3 or len(synonyms) + len(wrong) < 4:
                    _fail(where, "need 1–3 synonyms and enough distractors for four choices")
                if {norm(v) for v in synonyms} & {norm(v) for v in wrong}:
                    _fail(where, "synonyms overlap distractors")
            else:
                answers = _list(task, "answers", where, 1)
                if _text(task, "text", where).count("___") != len(answers):
                    _fail(where, "blank and answer counts differ")
                explanations = _list(task, "explanations", where)
                if explanations and len(explanations) != len(answers):
                    _fail(where, "explanations must match answer count")
                equal = task.get("equal")
                if equal:
                    used = set()
                    for group in equal.split(","):
                        if not re.fullmatch(r"\s*\d+(?:\s*=\s*\d+)+\s*", group):
                            _fail(where, "invalid EQUAL group")
                        positions = [int(v.strip()) - 1 for v in group.split("=")]
                        if (len(set(positions)) != len(positions) or used.intersection(positions)
                                or any(p < 0 or p >= len(answers) for p in positions)
                                or len({answers[p] for p in positions}) != 1):
                            _fail(where, "EQUAL must reference distinct positions with identical answers")
                        used.update(positions)
    return family

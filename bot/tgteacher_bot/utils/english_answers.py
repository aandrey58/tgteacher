"""Answer matching for English vocabulary recall exercises."""

import unicodedata

_APOSTROPHES = str.maketrans({"\u2018": "'", "\u2019": "'"})


def normalize_english_answer(text: str) -> str:
    """Ignore case, whitespace and typographic apostrophes, not spelling."""
    text = unicodedata.normalize("NFC", text).translate(_APOSTROPHES)
    return " ".join(text.split()).casefold()


def is_correct_answer(
    user_text: str, answer: str, alternatives: list[str] | None = None
) -> bool:
    """Accept the canonical answer or explicitly authored alternatives.

    Articles are not stripped: an author can allow 'an apple' alongside
    'apple', without accidentally accepting 'a apple' or 'a apples'.
    """
    normalized = normalize_english_answer(user_text)
    if not normalized:
        return False
    return any(
        normalized == normalize_english_answer(candidate)
        for candidate in [answer, *(alternatives or [])]
    )

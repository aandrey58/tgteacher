from __future__ import annotations

from pathlib import Path


def package_dir() -> Path:
    """Directory of `tgteacher_bot` package."""
    return Path(__file__).resolve().parent


def bot_dir() -> Path:
    """Directory containing the legacy entrypoint and runtime files (admins.txt, help index)."""
    return package_dir().parent


def project_root() -> Path:
    """Workspace/project root directory (contains `families/`, `requirements.txt`)."""
    return bot_dir().parent


def families_dir() -> Path:
    """Directory with word families content (expected at project root)."""
    return project_root() / "families"


def help_index_path() -> Path:
    """Path for persistent help index JSON (kept in `bot/` for backward compatibility)."""
    return bot_dir() / "help_questions_index.json"


def admins_path() -> Path:
    """Path to admins list (kept in `bot/` for backward compatibility)."""
    return bot_dir() / "admins.txt"


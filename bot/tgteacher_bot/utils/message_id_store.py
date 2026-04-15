import os
import json
from typing import Dict, Any
from tgteacher_bot.core import paths


def _root_dir() -> str:
    return str(paths.project_root() / 'runtime_cache' / 'message_ids')


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _file_path(user_id: int, family_idx: int, stage_num: int) -> str:
    base = _root_dir()
    dir_path = os.path.join(base, str(user_id), str(family_idx))
    _ensure_dir(dir_path)
    return os.path.join(dir_path, f'{stage_num}.json')


def load_message_ids(user_id: int, family_idx: int, stage_num: int) -> Dict[str, Any]:
    try:
        fp = _file_path(user_id, family_idx, stage_num)
        if not os.path.exists(fp):
            return {}
        with open(fp, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
            return {}
    except Exception:
        return {}


def save_message_ids(user_id: int, family_idx: int, stage_num: int, updates: Dict[str, Any]) -> None:
    try:
        fp = _file_path(user_id, family_idx, stage_num)
        current = {}
        if os.path.exists(fp):
            try:
                with open(fp, 'r', encoding='utf-8') as f:
                    current = json.load(f) or {}
            except Exception:
                current = {}
        if not isinstance(current, dict):
            current = {}
        current.update(updates or {})
        tmp_fp = fp + '.tmp'
        with open(tmp_fp, 'w', encoding='utf-8') as f:
            json.dump(current, f, ensure_ascii=False)
        os.replace(tmp_fp, fp)
    except Exception:
        # молча игнорим ошибки кэша, чтобы не ломать основной поток
        pass 
from tgteacher_bot.utils.family_parser import parse_family_txt
import os
from typing import List, Dict, Any
from tgteacher_bot.db.user_repo import init_db_pg
from tgteacher_bot.db.families_repo import save_family_to_pg, get_all_families_meta_pg, get_family_data_pg, delete_family_from_pg
from tgteacher_bot.core import paths

families_dir = str(paths.families_dir())

# ALL_FAMILIES_META будет содержать только id, name, description из PostgreSQL
ALL_FAMILIES_META: List[Dict[str, Any]] = []

async def _load_initial_families_meta():
    global ALL_FAMILIES_META
    await init_db_pg()
    ALL_FAMILIES_META = await get_all_families_meta_pg()
    # MCP DEBUG принты удалены

def update_families_meta():
    # Эта функция будет вызываться, например, после добавления/удаления группы слов,
    # чтобы обновить кэш метаданных
    global ALL_FAMILIES_META
    # Мы не можем просто вызвать await get_all_families_meta() здесь,
    # потому что это синхронная функция.
    # Для таких случаев, когда надо обновить синхронно, можно использовать
    # временную заглушку или пересмотреть архитектуру, если это критично.
    # Но для первичной загрузки и админских действий асинхронность подходит.
    # Пока оставим ее синхронной заглушкой
    pass 
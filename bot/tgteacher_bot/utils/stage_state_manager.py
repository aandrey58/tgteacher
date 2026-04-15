import json
import logging
from tgteacher_bot.db.user_repo import set_user_stage_state_pg, get_user_stage_state_pg, get_current_family_idx_pg, get_last_opened_family_place_pg
from tgteacher_bot.db.families_repo import get_family_data_pg

logger = logging.getLogger(__name__)

async def save_stage_state(user_id: int, family_idx: int, stage_num: int, state: dict):
    """Сохраняет состояние этапа в базу данных"""
    # Очищаем случайные данные, которые не должны сохраняться
    clean_state = state.copy()
    
    # Для этапов с случайными вариантами ответов очищаем их, но НЕ трогаем displayed_choices_map и choices_order_map для этапа 3
    if stage_num in [2, 3, 4, 6, 7]:
        if 'displayed_choices' in clean_state:
            del clean_state['displayed_choices']
        if 'last_feedback_message' in clean_state:
            del clean_state['last_feedback_message']
        # НЕ удаляем choices_order - он нужен для восстановления порядка кнопок
        # НЕ удаляем displayed_choices_map и choices_order_map для этапа 3
    
    # Для этапа 5 очищаем id сообщений и кеш file_id
    if stage_num == 5:
        for k in ['last_photo_message_id', 'last_text_message_id', 'file_id_cache']:
            if k in clean_state:
                del clean_state[k]

    # Для этапа 7 очищаем id сообщений и кеш file_id, чтобы не сохранять мусор в БД
    if stage_num == 7:
        for k in ['last_audio_message_id', 'last_text_message_id', 'file_id_cache']:
            if k in clean_state:
                del clean_state[k]
    
    # Для этапа 8 удаляем только displayed_choices, но оставляем shuffled_order и choices_order
    if stage_num == 8:
        if 'displayed_choices' in clean_state:
            del clean_state['displayed_choices']
        if 'feedbacks' in clean_state:
            del clean_state['feedbacks']
        if 'correct_indices' in clean_state:
            del clean_state['correct_indices']
        if 'error_indices' in clean_state:
            del clean_state['error_indices']
        if 'last_feedback_message' in clean_state:
            del clean_state['last_feedback_message']
        # НЕ удаляем choices_order - он нужен для восстановления
        # if 'choices_order' in clean_state:
        #     del clean_state['choices_order']
    
    await set_user_stage_state_pg(user_id, family_idx, stage_num, clean_state)
    
    # Оптимизация: сборка мусора после сохранения состояния
    import gc
    gc.collect()

async def load_stage_state(user_id: int, family_idx: int, stage_num: int):
    """Загружает состояние этапа из базы данных"""
    return await get_user_stage_state_pg(user_id, family_idx, stage_num)

async def initialize_stage_state(context, stage_key: str, family_idx: int, stage_num: int, default_state: dict, user_id: int = None):
    """Инициализирует состояние этапа, загружая из БД или создавая новое"""
    if user_id is None:
        # Пытаемся получить user_id из context разными способами
        if hasattr(context, 'effective_user') and context.effective_user:
            user_id = context.effective_user.id
        elif hasattr(context, 'user') and context.user:
            user_id = context.user.id
        else:
            logger.error("Не удалось получить user_id из context")
            return
    
    # Пытаемся загрузить сохранённое состояние
    saved_state = await load_stage_state(user_id, family_idx, stage_num)
    
    if saved_state:
        # Объединяем сохранённое состояние с дефолтным
        merged_state = default_state.copy()
        merged_state.update(saved_state)
        context.user_data[stage_key] = merged_state
        logger.info(f"Загружено сохранённое состояние для этапа {stage_num}, группы слов {family_idx}")
    else:
        # Создаём новое состояние
        context.user_data[stage_key] = default_state
        logger.info(f"Создано новое состояние для этапа {stage_num}, группы слов {family_idx}")
    
    # Сохраняем начальное состояние
    await save_stage_state(user_id, family_idx, stage_num, context.user_data[stage_key])

async def update_stage_state(context, stage_key: str, family_idx: int, stage_num: int, user_id: int = None):
    """Обновляет состояние этапа в базе данных"""
    if stage_key in context.user_data:
        if user_id is None:
            # Пытаемся получить user_id из context разными способами
            if hasattr(context, 'effective_user') and context.effective_user:
                user_id = context.effective_user.id
            elif hasattr(context, 'user') and context.user:
                user_id = context.user.id
            else:
                logger.error("Не удалось получить user_id из context")
                return
        
        await save_stage_state(user_id, family_idx, stage_num, context.user_data[stage_key])
        logger.info(f"Обновлено состояние для этапа {stage_num}, группы слов {family_idx}")

async def ensure_stage_state(update, context, stage_key: str, stage_num: int, default_state_func):
    user_id = update.effective_user.id
    family_id = context.user_data.get('current_family_idx')

    if not family_id:
        family_id = await get_current_family_idx_pg(user_id)
        if family_id:
            context.user_data['current_family_idx'] = family_id
        else:
            # Если группа слов до сих пор не найдена, это критическая ошибка
            logger.error(f"Не удалось определить family_id для пользователя {user_id} в этапе {stage_num}.")
            try:
                await update.callback_query.edit_message_text('Ошибка: группа слов не выбрана или не найдена.')
            except AttributeError:
                # Если это не callback_query (например, /start), то используем reply_text
                await update.message.reply_text('Ошибка: группа слов не выбрана или не найдена.')
            return False, None # Возвращаем False и None, чтобы вызывающая функция знала об ошибке

    st = context.user_data.get(stage_key)
    if not st:
        saved_state = await get_user_stage_state_pg(user_id, family_id, stage_num)
        if saved_state:
            st = saved_state
            context.user_data[stage_key] = st
            logger.info(f"Загружено состояние {stage_key} из БД для пользователя {user_id}, группы слов {family_id}")
        else:
            # Инициализируем дефолтное состояние, используя переданную функцию/лямбду
            default_state = await default_state_func(user_id, family_id, stage_num)
            context.user_data[stage_key] = default_state
            st = default_state
            await update_stage_state(context, stage_key, family_id, stage_num, user_id)
            logger.info(f"Создано новое состояние {stage_key} для пользователя {user_id}, группы слов {family_id}")
    
    return True, st # Возвращаем True и состояние, если всё успешно 
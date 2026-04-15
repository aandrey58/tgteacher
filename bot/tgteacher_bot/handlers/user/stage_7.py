from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile
from telegram.ext import ContextTypes
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
from tgteacher_bot.db.user_repo import (
    set_task_done_pg,
    set_last_opened_family_place_pg,
    get_last_opened_family_place_pg,
    set_stage7_answer_pg,
    get_stage7_answer_pg,
    set_family_stage_done_pg,
    mark_user_active_if_needed,
    get_current_family_idx_pg,
)
from tgteacher_bot.utils.stage_state_manager import ensure_stage_state, update_stage_state
from tgteacher_bot.handlers.admin.admin_status import track_metrics
from tgteacher_bot.utils.common import OK_MENU, find_file_case_insensitive
import logging
import os
import html
import random
from tgteacher_bot.utils.message_id_store import load_message_ids, save_message_ids
from functools import lru_cache
import gc
from tgteacher_bot.core import paths

logger = logging.getLogger(__name__)

# Оптимизация: LRU кэш для file_id с ограничением в 100 записей
@lru_cache(maxsize=100)
def _get_cached_audio_file_id(cache_key: str) -> str:
    """Кэш для audio file_id с ограничением размера"""
    return None

# Глобальный кэш с ограничением размера для аудио
_global_audio_cache = {}
_MAX_AUDIO_CACHE_SIZE = 100

def _get_global_audio_cache():
    """Возвращает глобальный кэш аудио с проверкой размера"""
    global _global_audio_cache
    if len(_global_audio_cache) > _MAX_AUDIO_CACHE_SIZE:
        # Удаляем 20% самых старых записей
        items_to_remove = int(_MAX_AUDIO_CACHE_SIZE * 0.2)
        keys_to_remove = list(_global_audio_cache.keys())[:items_to_remove]
        for key in keys_to_remove:
            del _global_audio_cache[key]
        logger.info(f"[stage7] Очищен кэш audio file_id: удалено {items_to_remove} записей")
    return _global_audio_cache


def get_stage7_keyboard(choices, answered_idx=None, correct_idx=None, task_idx: int = 0, total_tasks: int = 1, is_final_finish: bool = False):
    keyboard = []
    row = []
    for i, (text, is_correct, is_selected) in enumerate(choices):
        btn_text = text
        if is_selected:
            btn_text += ' ✅' if is_correct else ' ❌'
        if answered_idx is not None:
            cb_data = f'stage7_answer_{i}' if i == answered_idx else 'stage7_no_action'
        else:
            cb_data = f'stage7_answer_{i}'
        row.append(InlineKeyboardButton(btn_text, callback_data=cb_data))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    nav_row = []
    if task_idx == 0:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage7_first_task_alert'))
    else:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage7_prev'))
    if task_idx < total_tasks - 1:
        nav_row.append(InlineKeyboardButton('Вперёд ➡️', callback_data='stage7_next'))
    else:
        nav_row.append(InlineKeyboardButton('🏁 Завершить' if is_final_finish else '✅ Далее', callback_data='stage7_finish'))
    keyboard.append(nav_row)

    if is_final_finish and task_idx == total_tasks - 1:
        keyboard.append([InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')])
    elif is_final_finish:
        keyboard.append([
            InlineKeyboardButton('🏁 Завершить', callback_data='stage7_finish'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    else:
        keyboard.append([
            InlineKeyboardButton('⏩ Пропустить этап', callback_data='stage7_skip_confirm'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])

    return InlineKeyboardMarkup(keyboard)


def format_stage7_text(task, idx: int, total: int, feedback: str | None):
    base = f"Задание {idx+1}/{total}\n<b>Выбери подходящее по смыслу слово:</b>\n{html.escape((task.get('task') or ''))}"
    if feedback:
        base += f"\n\n{feedback}"
    return base


def _families_root_dir():
    return str(paths.families_dir())


async def _delete_previous_messages(context: ContextTypes.DEFAULT_TYPE, chat_id: int, st7: dict, user_id: int):
    bot = context.bot
    persisted = load_message_ids(user_id, st7.get('family_idx'), 7)
    to_clear_updates = {}
    for mid_key in ['last_text_message_id', 'last_audio_message_id']:
        mid = st7.get(mid_key) or (persisted.get(mid_key) if isinstance(persisted, dict) else None)
        if mid:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass
            st7[mid_key] = None
            to_clear_updates[mid_key] = None
    if to_clear_updates:
        save_message_ids(user_id, st7.get('family_idx'), 7, to_clear_updates)


async def _send_audio_and_text(update: Update, context: ContextTypes.DEFAULT_TYPE, st7: dict, task: dict, idx: int, total: int, feedback: str | None = None, is_final_finish: bool = False):
    chat_id = update.effective_chat.id
    bot = context.bot

    await _delete_previous_messages(context, chat_id, st7, update.effective_user.id)

    family_meta = await get_family_data_pg(st7['family_idx'])
    audio_name = task.get('audio')
    # Оптимизированный глобальный кэш с ограничением размера
    global_file_id_cache = _get_global_audio_cache()

    audio_message = None
    if audio_name:
        cache_key = f"{family_meta.get('folder_name') or ''}/{audio_name}"
        cached_id = global_file_id_cache.get(cache_key)
        
        # Поиск файла без учёта регистра
        family_dir = os.path.join(_families_root_dir(), family_meta['folder_name'] or '')
        audio_path = find_file_case_insensitive(family_dir, audio_name)
        
        if not audio_path:
            logger.warning(f"[stage7] Файл аудио '{audio_name}' не найден в папке '{family_dir}'")
        else:
            try:
                if cached_id:
                    # Пробуем отправить по cached file_id
                    tmp_msg = await bot.send_audio(chat_id=chat_id, audio=cached_id)
                    # Валидируем, что файл не "битый" (например, 64 байта)
                    if tmp_msg and getattr(tmp_msg, 'audio', None):
                        file_size = getattr(tmp_msg.audio, 'file_size', None)
                        if file_size is not None and file_size < 1024:
                            logger.warning(f"[stage7] cached file_id для '{audio_name}' выглядит битым (size={file_size}). Пере-заливаю файл и обновляю cache.")
                            global_file_id_cache.pop(cache_key, None)
                            # Удаляем битое сообщение, чтобы у пользователя не оставалось мусора
                            try:
                                await bot.delete_message(chat_id=chat_id, message_id=tmp_msg.message_id)
                            except Exception:
                                pass
                        else:
                            audio_message = tmp_msg
                if audio_message is None:
                    # Заливаем файл с явным именем
                    try:
                        with open(audio_path, 'rb') as f:
                            audio_message = await bot.send_audio(chat_id=chat_id, audio=InputFile(f, filename=os.path.basename(audio_path)))
                        if audio_message and getattr(audio_message, 'audio', None):
                            global_file_id_cache[cache_key] = audio_message.audio.file_id
                    except Exception as e_up:
                        logger.warning(f"[stage7] Ошибка отправки аудио файлом '{audio_name}': {type(e_up).__name__}: {e_up}")
                        # Фоллбек на документ с явным именем
                        try:
                            with open(audio_path, 'rb') as f:
                                audio_message = await bot.send_document(chat_id=chat_id, document=InputFile(f, filename=os.path.basename(audio_path)))
                        except Exception as e2:
                            logger.warning(f"[stage7] Ошибка отправки документа '{audio_name}': {type(e2).__name__}: {e2}")
            except Exception as e:
                logger.warning(f"[stage7] Ошибка отправки аудио '{audio_name}' (cached or upload): {type(e).__name__}: {e}")
                # Фоллбек на документ с явным именем
                try:
                    with open(audio_path, 'rb') as f:
                        audio_message = await bot.send_document(chat_id=chat_id, document=InputFile(f, filename=os.path.basename(audio_path)))
                except Exception as e2:
                    logger.warning(f"[stage7] Ошибка отправки документа '{audio_name}': {type(e2).__name__}: {e2}")
    # Обновлять st7['file_id_cache'] больше не нужно; используется глобальный кэш

    if audio_message:
        st7['last_audio_message_id'] = audio_message.message_id

    text = format_stage7_text(task, idx, total, feedback)
    text_message = await bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=get_stage7_keyboard(st7['rendered_choices'], answered_idx=st7.get('answered_idx'), correct_idx=st7.get('correct_idx'), task_idx=idx, total_tasks=total, is_final_finish=is_final_finish),
        parse_mode='HTML'
    )
    st7['last_text_message_id'] = text_message.message_id
    # Persist message ids in JSON for cleanup after restarts
    try:
        save_message_ids(update.effective_user.id, st7['family_idx'], 7, {
            'last_audio_message_id': st7.get('last_audio_message_id'),
            'last_text_message_id': st7.get('last_text_message_id'),
        })
    except Exception:
        pass


async def get_default_stage7_state(user_id, family_id, stage_num):
    last_place = await get_last_opened_family_place_pg(user_id, family_id)
    task_idx = 0
    if last_place and last_place[0] == 7:
        tasks = await get_stage7_tasks_pg(family_id)
        if tasks:
            task_idx = min(last_place[1], len(tasks) - 1)
    return {
        'family_idx': family_id,
        'task_idx': task_idx,
        'answered': False,
        'choices_order': None,
        'displayed_choices': None,
        'rendered_choices': [],
        'answered_idx': None,
        'correct_idx': None,
        'last_audio_message_id': None,
        'last_text_message_id': None,
        'file_id_cache': {},
        'tasks_order': None,
    }


def _build_displayed_choices(task: dict) -> list:
    correct_answer = task['answer']
    choices_all = [c for c in (task.get('choices') or []) if c]
    # Гарантируем, что правильный ответ есть в списке
    if correct_answer not in choices_all:
        choices_all.append(correct_answer)
    # Оставляем не более 4 вариантов, включая правильный
    other = [c for c in choices_all if c != correct_answer]
    random.shuffle(other)
    needed = max(0, 3)
    distractors = other[:needed]
    displayed = [correct_answer] + distractors
    random.shuffle(displayed)
    return displayed


@track_metrics
async def show_stage7_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    success, st7 = await ensure_stage_state(update, context, 'stage7', 7, get_default_stage7_state)
    if not success:
        return

    family_id = st7['family_idx']
    tasks = await get_stage7_tasks_pg(family_id)
    if not tasks:
        from tgteacher_bot.handlers.user.stage_8 import stage8_start
        await stage8_start(update, context)
        return

    # Initialize or validate tasks_order
    total = len(tasks)
    order = st7.get('tasks_order')
    if (not order) or (len(order) != total) or (sorted(order) != list(range(total))):
        order = list(range(total))
        random.shuffle(order)
        st7['tasks_order'] = order
        context.user_data['stage7'] = st7
        await update_stage_state(context, 'stage7', family_id, 7, update.effective_user.id)

    idx_display = st7['task_idx']
    if idx_display >= len(tasks):
        st7['task_idx'] = 0
        idx_display = 0
        context.user_data['stage7'] = st7

    orig_idx = order[idx_display]

    task = tasks[orig_idx]
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

    correct_answer = task['answer']

    previous_answer = await get_stage7_answer_pg(user_id, family_id, orig_idx)

    # Определяем, последний ли это этап в семье
    from tgteacher_bot.db.families_repo import get_stage8_tasks_pg as _g8
    tasks8 = await _g8(family_id)
    is_last_stage = not bool(tasks8)

    # Определяем, нужно ли показывать "Завершить" в навигации
    is_final_finish = False
    if idx_display == len(tasks) - 1:
        is_final_finish = is_last_stage

    # displayed_choices
    if st7.get('displayed_choices'):
        displayed_choices = st7['displayed_choices']
    else:
        displayed_choices = _build_displayed_choices(task)
        st7['displayed_choices'] = displayed_choices
        context.user_data['stage7'] = st7
        await update_stage_state(context, 'stage7', family_id, 7, user_id)

    # Порядок отображения
    st7['choices_order'] = list(range(len(displayed_choices)))

    order_choices = st7['choices_order']
    choices = []
    answered_idx = None
    correct_idx = None

    if previous_answer:
        selected_text, is_correct = previous_answer
        try:
            selected_orig_idx = displayed_choices.index(selected_text)
            correct_orig_idx = displayed_choices.index(correct_answer)
            for i, orig_idx_c in enumerate(order_choices):
                is_selected = (i == order_choices.index(selected_orig_idx))
                is_correct_choice = (orig_idx_c == correct_orig_idx)
                choices.append((displayed_choices[orig_idx_c], is_correct_choice, is_selected))
            answered_idx = order_choices.index(selected_orig_idx)
            correct_idx = order_choices.index(correct_orig_idx)
        except ValueError:
            # Если по какой-то причине выбранного текста нет в отображаемых вариантах, показываем без выделения
            for i, orig_idx_c in enumerate(order_choices):
                choices.append((displayed_choices[orig_idx_c], False, False))
    else:
        for i, orig_idx_c in enumerate(order_choices):
            choices.append((displayed_choices[orig_idx_c], False, False))

    st7['rendered_choices'] = choices
    st7['answered_idx'] = answered_idx if previous_answer else None
    st7['correct_idx'] = correct_idx if previous_answer else None

    await set_last_opened_family_place_pg(user_id, family_id, 7, idx_display)
    context.user_data['stage7'] = st7

    feedback = None
    if previous_answer:
        _, is_correct = previous_answer
        if is_correct:
            feedback = f"✅ Верно! {task.get('explanation', '')}"
        else:
            feedback = f"❌ Неправильно! Правильный ответ: <b>{html.escape((correct_answer or ''))}</b>\n{task.get('explanation', '')}"

    await _send_audio_and_text(update, context, st7, task, idx_display, len(tasks), feedback, is_final_finish=is_last_stage)
    # Обновим клавиатуру с учётом is_last_stage
    try:
        last_text_id = st7.get('last_text_message_id')
        if last_text_id:
            await context.bot.edit_message_reply_markup(
                chat_id=update.effective_chat.id,
                message_id=last_text_id,
                reply_markup=get_stage7_keyboard(choices, answered_idx=st7.get('answered_idx'), correct_idx=st7.get('correct_idx'), task_idx=idx_display, total_tasks=len(tasks), is_final_finish=is_last_stage)
            )
    except Exception:
        pass


@track_metrics
async def stage7_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    try:
        if query and query.message:
            await query.message.delete()
    except Exception:
        pass

    success, st7 = await ensure_stage_state(update, context, 'stage7', 7, get_default_stage7_state)
    if not success:
        return
    await show_stage7_task(update, context)


@track_metrics
async def stage7_prev_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

    success, st7 = await ensure_stage_state(update, context, 'stage7', 7, get_default_stage7_state)
    if not success:
        return

    if st7['task_idx'] > 0:
        st7['task_idx'] -= 1
        st7['choices_order'] = None
        st7['displayed_choices'] = None
        st7['answered'] = False
        st7['answered_idx'] = None
        st7['correct_idx'] = None
        context.user_data['stage7'] = st7
        await update_stage_state(context, 'stage7', st7['family_idx'], 7, update.effective_user.id)
        await show_stage7_task(update, context)
    else:
        await query.answer('Это первое задание', show_alert=True)


@track_metrics
async def stage7_next_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

    success, st7 = await ensure_stage_state(update, context, 'stage7', 7, get_default_stage7_state)
    if not success:
        return

    family_id = st7['family_idx']
    tasks = await get_stage7_tasks_pg(family_id)
    if st7['task_idx'] < len(tasks) - 1:
        st7['task_idx'] += 1
        st7['choices_order'] = None
        st7['displayed_choices'] = None
        st7['answered'] = False
        st7['answered_idx'] = None
        st7['correct_idx'] = None
        context.user_data['stage7'] = st7
        await update_stage_state(context, 'stage7', family_id, 7, update.effective_user.id)
        await show_stage7_task(update, context)
    else:
        await query.answer('Это последнее задание.', show_alert=False)


@track_metrics
async def stage7_answer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    success, st7 = await ensure_stage_state(update, context, 'stage7', 7, get_default_stage7_state)
    if not success:
        await query.answer()
        return

    family_id = st7['family_idx']
    tasks = await get_stage7_tasks_pg(family_id)
    # Ensure tasks_order
    order = st7.get('tasks_order') or list(range(len(tasks)))
    if (not order) or (len(order) != len(tasks)) or (sorted(order) != list(range(len(tasks)))):
        order = list(range(len(tasks)))
        random.shuffle(order)
        st7['tasks_order'] = order
        context.user_data['stage7'] = st7
        await update_stage_state(context, 'stage7', family_id, 7, update.effective_user.id)

    task_idx_display = st7['task_idx']
    task = tasks[order[task_idx_display]]

    displayed_choices = st7.get('displayed_choices')
    order_choices = st7.get('choices_order')
    if not displayed_choices or not order_choices:
        await query.edit_message_text('Ошибка: порядок вариантов потерян. Начни этап заново.', reply_markup=OK_MENU)
        return

    answer_idx = int(query.data.replace('stage7_answer_', ''))
    selected_orig_idx = order_choices[answer_idx]
    correct_answer = task['answer']
    correct_orig_idx = displayed_choices.index(correct_answer)
    selected_text = displayed_choices[selected_orig_idx]

    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    is_correct = (selected_orig_idx == correct_orig_idx)
    orig_idx_for_db = order[task_idx_display]
    await set_stage7_answer_pg(user_id, family_id, orig_idx_for_db, selected_text, is_correct)

    feedback_text = ''
    if is_correct:
        await set_task_done_pg(user_id, family_id, 7, orig_idx_for_db)
        feedback_text = f"✅ Верно! {task.get('explanation', '')}"
    else:
        feedback_text = f"❌ Неправильно! Правильный ответ: <b>{html.escape((correct_answer or ''))}</b>\n{task.get('explanation', '')}"

    choices = []
    for i, orig_idx_c in enumerate(order_choices):
        choices.append((displayed_choices[orig_idx_c], (orig_idx_c == correct_orig_idx), (i == answer_idx)))

    st7['answered'] = True
    st7['rendered_choices'] = choices
    st7['answered_idx'] = answer_idx
    st7['correct_idx'] = correct_orig_idx
    context.user_data['stage7'] = st7
    await update_stage_state(context, 'stage7', family_id, 7, user_id)

    # Определяем, последний ли это этап в семье
    from tgteacher_bot.db.families_repo import get_stage8_tasks_pg as _g8
    tasks8 = await _g8(family_id)
    is_last_stage = not bool(tasks8)

    # Определяем, нужно ли показывать "Завершить" в навигации
    is_final_finish = False
    if task_idx_display == len(tasks) - 1:
        is_final_finish = is_last_stage

    # Обновляем только текст и клавиатуру (аудио оставляем)
    await query.edit_message_text(
        format_stage7_text(task, task_idx_display, len(tasks), feedback_text),
        reply_markup=get_stage7_keyboard(choices, answered_idx=answer_idx, correct_idx=correct_orig_idx, task_idx=task_idx_display, total_tasks=len(tasks), is_final_finish=is_final_finish),
        parse_mode='HTML'
    )


@track_metrics
async def stage7_skip_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    # Удаляем аудио, чтобы остался только промпт подтверждения
    try:
        st7 = context.user_data.get('stage7') or {}
        last_audio_id = st7.get('last_audio_message_id')
        if last_audio_id:
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=last_audio_id)
            except Exception:
                pass
            st7['last_audio_message_id'] = None
            context.user_data['stage7'] = st7
    except Exception:
        pass
    # JSON-фоллбек на случай пустого user_data после рестарта
    try:
        user_id = update.effective_user.id
        family_id = context.user_data.get('current_family_idx')
        if not family_id:
            try:
                family_id = await get_current_family_idx_pg(user_id)
            except Exception:
                family_id = None
        if family_id:
            persisted = load_message_ids(user_id, family_id, 7)
            if isinstance(persisted, dict):
                aid = persisted.get('last_audio_message_id')
                if aid:
                    try:
                        await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=aid)
                    except Exception:
                        pass
                    try:
                        save_message_ids(user_id, family_id, 7, {'last_audio_message_id': None})
                    except Exception:
                        pass
    except Exception:
        pass

    text = (
        'Ты уверен, что хочешь пропустить этот этап?\n'
        'Рекомендуем пройти все этапы для лучшего запоминания слов!'
    )
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да, пропустить', callback_data='stage7_skip'),
            InlineKeyboardButton('❌ Отмена', callback_data='stage7_cancel_skip')
        ]
    ])
    await query.edit_message_text(text, reply_markup=keyboard)


@track_metrics
async def stage7_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    # Чистим только аудио этапа 7
    try:
        st7 = context.user_data.get('stage7') or {}
        chat_id = update.effective_chat.id
        last_audio_id = st7.get('last_audio_message_id')
        if last_audio_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_audio_id)
            except Exception:
                pass
            st7['last_audio_message_id'] = None
        context.user_data['stage7'] = st7
    except Exception:
        pass
    # JSON-фоллбек на случай пустого user_data после рестарта
    try:
        user_id = update.effective_user.id
        family_id = context.user_data.get('current_family_idx')
        if not family_id:
            try:
                family_id = await get_current_family_idx_pg(user_id)
            except Exception:
                family_id = None
        if family_id:
            persisted = load_message_ids(user_id, family_id, 7)
            if isinstance(persisted, dict):
                aid = persisted.get('last_audio_message_id')
                if aid:
                    try:
                        await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=aid)
                    except Exception:
                        pass
                    try:
                        save_message_ids(user_id, family_id, 7, {'last_audio_message_id': None})
                    except Exception:
                        pass
    except Exception:
        pass
    # Определяем family_id надёжно, до очистки состояния
    st7 = context.user_data.get('stage7') or {}
    family_id = st7.get('family_idx') or context.user_data.get('current_family_idx')
    context.user_data.pop('stage7', None)
    from tgteacher_bot.handlers.user.stage_8 import get_stage8_tasks_pg, stage8_start
    if family_id:
        tasks8 = await get_stage8_tasks_pg(family_id)
        if tasks8 and len(tasks8) > 0:
            await stage8_start(update, context)
            return
        # Если этап 8 отсутствует — завершаем семью
        from tgteacher_bot.db.user_repo import set_family_finished_pg, set_last_opened_family_place_pg
        from tgteacher_bot.db.families_repo import get_family_data_pg
        await set_family_finished_pg(user_id, family_id)
        await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
        await update_stage_state(context, 'stage7', family_id, 7, user_id)
        family_meta = await get_family_data_pg(family_id)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{family_id}_0")],
            [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
        ])
        await query.edit_message_text(
            f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
            reply_markup=keyboard
        )
        return
    await query.edit_message_text('Этап 8 не реализован для этой группы слов.', reply_markup=OK_MENU)


@track_metrics
async def stage7_cancel_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await show_stage7_task(update, context)


@track_metrics
async def stage7_finish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    # Чистим только аудио этапа 7
    try:
        st7_local = context.user_data.get('stage7') or {}
        chat_id = update.effective_chat.id
        last_audio_id = st7_local.get('last_audio_message_id')
        if last_audio_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_audio_id)
            except Exception:
                pass
            st7_local['last_audio_message_id'] = None
        context.user_data['stage7'] = st7_local
        try:
            save_message_ids(update.effective_user.id, st7_local.get('family_idx'), 7, {
                'last_audio_message_id': None
            })
        except Exception:
            pass
    except Exception:
        pass

    st7 = context.user_data.get('stage7')
    if st7:
        user_id = update.effective_user.id
        await set_family_stage_done_pg(user_id, st7['family_idx'], 7)
        family_id = st7['family_idx']
        context.user_data.pop('stage7', None)
        from tgteacher_bot.handlers.user.stage_8 import get_stage8_tasks_pg, stage8_start
        tasks8 = await get_stage8_tasks_pg(family_id)
        if tasks8 and len(tasks8) > 0:
            await stage8_start(update, context)
        else:
            from tgteacher_bot.db.user_repo import set_family_finished_pg
            await set_family_finished_pg(user_id, family_id)
            await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
            await update_stage_state(context, 'stage7', family_id, 7, user_id)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{family_id}_0")],
                [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
            ])
            family_meta = await get_family_data_pg(family_id)
            await query.edit_message_text(
                f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
                reply_markup=keyboard
            )


@track_metrics
async def stage7_no_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await update.callback_query.answer()


@track_metrics
async def stage7_first_task_alert_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await update.callback_query.answer('Это первое задание.', show_alert=False) 
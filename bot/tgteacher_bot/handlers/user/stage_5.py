from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile
from telegram.ext import ContextTypes
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage5_tasks_pg
from tgteacher_bot.db.user_repo import (
    set_task_done_pg,
    set_last_opened_family_place_pg,
    get_last_opened_family_place_pg,
    set_stage5_answer_pg,
    get_stage5_answer_pg,
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
import mimetypes
import io
from tgteacher_bot.utils.message_id_store import load_message_ids, save_message_ids
import random
from functools import lru_cache
import gc
from tgteacher_bot.core import paths

logger = logging.getLogger(__name__)

# Оптимизация: LRU кэш для file_id с ограничением в 100 записей
@lru_cache(maxsize=100)
def _get_cached_file_id(cache_key: str) -> str:
    """Кэш для file_id с ограничением размера"""
    return None

# Глобальный кэш с ограничением размера
_global_photo_cache = {}
_MAX_CACHE_SIZE = 100

def _get_global_photo_cache():
    """Возвращает глобальный кэш с проверкой размера"""
    global _global_photo_cache
    if len(_global_photo_cache) > _MAX_CACHE_SIZE:
        # Удаляем 20% самых старых записей
        items_to_remove = int(_MAX_CACHE_SIZE * 0.2)
        keys_to_remove = list(_global_photo_cache.keys())[:items_to_remove]
        for key in keys_to_remove:
            del _global_photo_cache[key]
        logger.info(f"[stage5] Очищен кэш file_id: удалено {items_to_remove} записей")
    return _global_photo_cache


def get_stage5_keyboard(task_idx: int, total_tasks: int, answered: bool, is_final_finish: bool = False):
    nav_row = []
    if task_idx == 0:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage5_first_task_alert'))
    else:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage5_prev'))

    if task_idx < total_tasks - 1:
        nav_row.append(InlineKeyboardButton('Вперёд ➡️', callback_data='stage5_next'))
    else:
        nav_row.append(InlineKeyboardButton('🏁 Завершить' if is_final_finish else '✅ Далее', callback_data='stage5_finish'))

    keyboard = [
        nav_row,
    ]
    if is_final_finish and task_idx == total_tasks - 1:
        keyboard.append([InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')])
    elif is_final_finish:
        keyboard.append([
            InlineKeyboardButton('🏁 Завершить', callback_data='stage5_finish'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    else:
        keyboard.append([
            InlineKeyboardButton('⏩ Пропустить этап', callback_data='stage5_skip_confirm'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    return InlineKeyboardMarkup(keyboard)


def format_stage5_text(idx: int, total: int, awaiting_input: bool, user_answer: str | None, feedback: str | None):
    base = f"Задание {idx+1}/{total}\n<b>Впиши подходящее слово:</b>\nПришли в ответ текст с определённым артиклем."
    if user_answer is not None:
        safe_answer = html.escape(user_answer)
        base += f"\n\nВаш ответ: <code>{safe_answer}</code>"
    if feedback:
        base += f"\n{feedback}"
    if not awaiting_input and user_answer is None:
        base += "\n"
    return base


async def get_default_stage5_state(user_id, family_id, stage_num):
    last_place = await get_last_opened_family_place_pg(user_id, family_id)
    task_idx = 0
    if last_place and last_place[0] == 5:
        tasks = await get_stage5_tasks_pg(family_id)
        if tasks:
            task_idx = min(last_place[1], len(tasks) - 1)
    return {
        'family_idx': family_id,
        'task_idx': task_idx,
        'awaiting_input': True,
        'answered': False,
        'user_answer': None,
        'last_photo_message_id': None,
        'last_text_message_id': None,
        'file_id_cache': {},  # image_name -> file_id
        'tasks_order': None,
    }


def _families_root_dir():
    return str(paths.families_dir())


async def _delete_previous_messages(context: ContextTypes.DEFAULT_TYPE, chat_id: int, st5: dict, user_id: int):
    bot = context.bot
    persisted = load_message_ids(user_id, st5.get('family_idx'), 5)
    to_clear_updates = {}
    for mid_key in ['last_text_message_id', 'last_photo_message_id']:
        mid = st5.get(mid_key) or (persisted.get(mid_key) if isinstance(persisted, dict) else None)
        if mid:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass
            st5[mid_key] = None
            to_clear_updates[mid_key] = None
    if to_clear_updates:
        save_message_ids(user_id, st5.get('family_idx'), 5, to_clear_updates)


def _normalize_spaces(s: str) -> str:
    return ' '.join(s.strip().split())


def _is_correct_answer(user_text: str, answer: str, alternatives: list[str]) -> bool:
    u = _normalize_spaces(user_text)
    a = _normalize_spaces(answer)
    if u == a:
        return True
    for alt in alternatives or []:
        if u == _normalize_spaces(alt):
            return True
    return False


def _log_image_diagnostics(image_path: str):
    try:
        exists = os.path.exists(image_path)
        size = os.path.getsize(image_path) if exists else None
        mime = mimetypes.guess_type(image_path)[0]
        logger.warning(f"[stage5] DIAG image_path='{image_path}', exists={exists}, size={size}, mime={mime}")
        try:
            from PIL import Image, ImageOps  # type: ignore
            with Image.open(image_path) as im:
                logger.warning(f"[stage5] DIAG PIL format={im.format}, mode={im.mode}, size={im.size}")
        except Exception as e_pil:
            logger.warning(f"[stage5] DIAG PIL open failed: {type(e_pil).__name__}: {e_pil}")
    except Exception as e:
        logger.warning(f"[stage5] DIAG failed: {type(e).__name__}: {e}")


async def _send_photo_converted(bot, chat_id: int, image_path: str):
    try:
        from PIL import Image, ImageOps  # type: ignore
    except Exception as e:
        logger.warning(f"[stage5] PIL not available for conversion: {type(e).__name__}: {e}")
        return None

    try:
        with Image.open(image_path) as im:
            try:
                im = ImageOps.exif_transpose(im)
            except Exception:
                pass
            if im.mode not in ('RGB', 'L'):
                im = im.convert('RGB')
            elif im.mode == 'L':
                im = im.convert('RGB')

            quality_candidates = [85, 75, 65]
            last_error: Exception | None = None
            for q in quality_candidates:
                buffer = io.BytesIO()
                try:
                    im.save(buffer, format='JPEG', quality=q, optimize=True)
                    data = buffer.getvalue()
                    if len(data) >= 9_900_000:
                        last_error = Exception(f'Converted JPEG still too large with quality={q}: {len(data)} bytes')
                        continue
                    buffer.seek(0)
                    filename = os.path.splitext(os.path.basename(image_path))[0] + '_conv.jpg'
                    msg = await bot.send_photo(chat_id=chat_id, photo=InputFile(buffer, filename=filename))
                    return msg
                except Exception as e:
                    last_error = e
                finally:
                    buffer.close()
            if last_error:
                logger.warning(f"[stage5] Conversion send_photo failed: {type(last_error).__name__}: {last_error}")
            return None
    except Exception as e:
        logger.warning(f"[stage5] Conversion failed: {type(e).__name__}: {e}")
        return None


async def _send_photo_and_text(update: Update, context: ContextTypes.DEFAULT_TYPE, st5: dict, task: dict, idx: int, total: int, feedback: str | None = None, is_final_finish: bool = False):
    chat_id = update.effective_chat.id
    bot = context.bot

    await _delete_previous_messages(context, chat_id, st5, update.effective_user.id)

    family_meta = await get_family_data_pg(st5['family_idx'])
    image_name = task.get('image')
    # Оптимизированный глобальный кэш с ограничением размера
    global_photo_cache = _get_global_photo_cache()

    photo_message = None
    if image_name:
        cache_key = f"{family_meta.get('folder_name') or ''}/{image_name}"
        cache_entry = global_photo_cache.get(cache_key)
        cached_id = cache_entry['file_id'] if isinstance(cache_entry, dict) else None
        cached_unique = cache_entry.get('file_unique_id') if isinstance(cache_entry, dict) else None
        
        # Поиск файла без учёта регистра
        family_dir = os.path.join(_families_root_dir(), family_meta['folder_name'] or '')
        image_path = find_file_case_insensitive(family_dir, image_name)
        
        if not image_path:
            logger.warning(f"[stage5] Файл изображения '{image_name}' не найден в папке '{family_dir}'")
        else:
            _log_image_diagnostics(image_path)
            
        if image_path:
            try:
                if cached_id:
                    tmp_msg = await bot.send_photo(chat_id=chat_id, photo=cached_id)
                    if tmp_msg and tmp_msg.photo:
                        p = tmp_msg.photo[-1]
                        file_size = getattr(p, 'file_size', None)
                        file_unique_id = getattr(p, 'file_unique_id', None)
                        if (file_size is not None and file_size < 1024) or (cached_unique and file_unique_id and cached_unique != file_unique_id):
                            logger.warning(f"[stage5] cached file_id для '{image_name}' битый/не совпал (size={file_size}, cached_unique_match={cached_unique == file_unique_id}). Пере-заливаю.")
                            global_photo_cache.pop(cache_key, None)
                            # Удаляем битое сообщение, чтобы у пользователя не оставалось мусора
                            try:
                                await bot.delete_message(chat_id=chat_id, message_id=tmp_msg.message_id)
                            except Exception:
                                pass
                        else:
                            photo_message = tmp_msg
                if photo_message is None:
                    # Пытаемся отправить исходный файл как фото с явным именем
                    try:
                        with open(image_path, 'rb') as f:
                            photo_message = await bot.send_photo(chat_id=chat_id, photo=InputFile(f, filename=os.path.basename(image_path)))
                        if photo_message and photo_message.photo:
                            p = photo_message.photo[-1]
                            global_photo_cache[cache_key] = {
                                'file_id': p.file_id,
                                'file_unique_id': getattr(p, 'file_unique_id', None),
                            }
                    except Exception as e_up:
                        logger.warning(f"[stage5] Ошибка отправки фото файлом '{image_name}': {type(e_up).__name__}: {e_up}")
                        # Конвертим и пробуем снова
                        conv_msg = await _send_photo_converted(bot, chat_id, image_path)
                        if conv_msg and conv_msg.photo:
                            photo_message = conv_msg
                            try:
                                p = conv_msg.photo[-1]
                                global_photo_cache[cache_key] = {
                                    'file_id': p.file_id,
                                    'file_unique_id': getattr(p, 'file_unique_id', None),
                                }
                            except Exception:
                                pass
                        else:
                            # Фоллбек на документ с явным именем
                            try:
                                with open(image_path, 'rb') as f:
                                    photo_message = await bot.send_document(chat_id=chat_id, document=InputFile(f, filename=os.path.basename(image_path)))
                            except Exception as e2:
                                logger.warning(f"[stage5] Ошибка отправки документа '{image_name}': {type(e2).__name__}: {e2}")
            except Exception as e:
                logger.warning(f"[stage5] Ошибка отправки фото '{image_name}' (cached or upload): {type(e).__name__}: {e}")
                # Фоллбек на документ с явным именем
                try:
                    with open(image_path, 'rb') as f:
                        photo_message = await bot.send_document(chat_id=chat_id, document=InputFile(f, filename=os.path.basename(image_path)))
                except Exception as e3:
                    logger.warning(f"[stage5] Ошибка отправки документа '{image_name}' во внешнем except: {type(e3).__name__}: {e3}")
    # Кэш file_id больше не сохраняем в состояние этапа; используем глобальный кэш

    if photo_message:
        st5['last_photo_message_id'] = photo_message.message_id

    text_feedback = format_stage5_text(idx, total, st5.get('awaiting_input', True), st5.get('user_answer'), feedback)
    text_message = await bot.send_message(
        chat_id=chat_id,
        text=text_feedback,
        reply_markup=get_stage5_keyboard(idx, total, st5.get('answered', False), is_final_finish=is_final_finish),
        parse_mode='HTML'
    )
    st5['last_text_message_id'] = text_message.message_id
    # Persist message ids in JSON for cleanup after restarts
    try:
        save_message_ids(update.effective_user.id, st5['family_idx'], 5, {
            'last_photo_message_id': st5.get('last_photo_message_id'),
            'last_text_message_id': st5.get('last_text_message_id'),
        })
    except Exception:
        pass


@track_metrics
async def show_stage5_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    success, st5 = await ensure_stage_state(update, context, 'stage5', 5, get_default_stage5_state)
    if not success:
        return

    family_id = st5['family_idx']
    tasks = await get_stage5_tasks_pg(family_id)
    if not tasks:
        from tgteacher_bot.handlers.user.stage_6 import stage6_start
        await stage6_start(update, context)
        return

    # Initialize or validate tasks_order
    total = len(tasks)
    order = st5.get('tasks_order')
    if (not order) or (len(order) != total) or (sorted(order) != list(range(total))):
        order = list(range(total))
        random.shuffle(order)
        st5['tasks_order'] = order
        context.user_data['stage5'] = st5
        await update_stage_state(context, 'stage5', family_id, 5, update.effective_user.id)

    idx_display = st5['task_idx']
    if idx_display >= len(tasks):
        st5['task_idx'] = 0
        idx_display = 0
        context.user_data['stage5'] = st5

    # Определяем, является ли текущий этап последним
    from tgteacher_bot.db.families_repo import get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
    has_next = False
    for fetch in (get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
        next_tasks = await fetch(family_id)
        if next_tasks:
            has_next = True
            break
    is_last_stage = not has_next

    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

    orig_idx = order[idx_display]
    prev = await get_stage5_answer_pg(user_id, family_id, orig_idx)
    feedback = None
    if prev:
        selected_text, is_correct = prev
        st5['user_answer'] = selected_text
        st5['answered'] = True
        st5['awaiting_input'] = False
        if is_correct:
            feedback = '✅ Верно!'
        else:
            correct = tasks[orig_idx]['answer']
            feedback = f"❌ Неправильно! Правильный ответ: <b>{html.escape((correct or ''))}</b>"
    else:
        st5['user_answer'] = None
        st5['answered'] = False
        st5['awaiting_input'] = True

    context.user_data['stage5'] = st5
    await set_last_opened_family_place_pg(user_id, family_id, 5, idx_display)
    await update_stage_state(context, 'stage5', family_id, 5, user_id)

    await _send_photo_and_text(update, context, st5, tasks[orig_idx], idx_display, len(tasks), feedback, is_final_finish=is_last_stage)
    # Обновим клавиатуру с учётом is_last_stage
    try:
        last_text_id = st5.get('last_text_message_id')
        if last_text_id:
            await context.bot.edit_message_reply_markup(
                chat_id=update.effective_chat.id,
                message_id=last_text_id,
                reply_markup=get_stage5_keyboard(idx_display, len(tasks), st5.get('answered', False), is_final_finish=is_last_stage)
            )
    except Exception:
        pass


@track_metrics
async def stage5_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    try:
        if query and query.message:
            await query.message.delete()
    except Exception:
        pass
    success, st5 = await ensure_stage_state(update, context, 'stage5', 5, get_default_stage5_state)
    if not success:
        return
    await show_stage5_task(update, context)


@track_metrics
async def stage5_prev_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    success, st5 = await ensure_stage_state(update, context, 'stage5', 5, get_default_stage5_state)
    if not success:
        return
    if st5['task_idx'] > 0:
        st5['task_idx'] -= 1
        st5['answered'] = False
        st5['awaiting_input'] = True
        st5['user_answer'] = None
        context.user_data['stage5'] = st5
        await update_stage_state(context, 'stage5', st5['family_idx'], 5, update.effective_user.id)
        await show_stage5_task(update, context)
    else:
        await query.answer('Это первое задание.', show_alert=False)


@track_metrics
async def stage5_next_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    success, st5 = await ensure_stage_state(update, context, 'stage5', 5, get_default_stage5_state)
    if not success:
        return
    family_id = st5['family_idx']
    tasks = await get_stage5_tasks_pg(family_id)
    if st5['task_idx'] < len(tasks) - 1:
        st5['task_idx'] += 1
        st5['answered'] = False
        st5['awaiting_input'] = True
        st5['user_answer'] = None
        context.user_data['stage5'] = st5
        await update_stage_state(context, 'stage5', family_id, 5, update.effective_user.id)
        await show_stage5_task(update, context)
    else:
        await query.answer('Это последнее задание.', show_alert=False)


@track_metrics
async def stage5_skip_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    # Удаляем фото задания, чтобы остался только промпт подтверждения
    try:
        st5 = context.user_data.get('stage5') or {}
        last_photo_id = st5.get('last_photo_message_id')
        if last_photo_id:
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=last_photo_id)
            except Exception:
                pass
            st5['last_photo_message_id'] = None
            context.user_data['stage5'] = st5
            try:
                save_message_ids(update.effective_user.id, st5.get('family_idx'), 5, {
                    'last_photo_message_id': None
                })
            except Exception:
                pass
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
            persisted = load_message_ids(user_id, family_id, 5)
            if isinstance(persisted, dict):
                pid = persisted.get('last_photo_message_id')
                if pid:
                    try:
                        await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=pid)
                    except Exception:
                        pass
                    try:
                        save_message_ids(user_id, family_id, 5, {'last_photo_message_id': None})
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
            InlineKeyboardButton('✅ Да, пропустить', callback_data='stage5_skip'),
            InlineKeyboardButton('❌ Отмена', callback_data='stage5_cancel_skip')
        ]
    ])
    await query.edit_message_text(text, reply_markup=keyboard)


@track_metrics
async def stage5_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    # Чистим только фото этапа 5, текст оставляем для редактирования следующими этапами
    try:
        st5 = context.user_data.get('stage5') or {}
        chat_id = update.effective_chat.id
        last_photo_id = st5.get('last_photo_message_id')
        if last_photo_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_photo_id)
            except Exception:
                pass
            st5['last_photo_message_id'] = None
        context.user_data['stage5'] = st5
        try:
            save_message_ids(update.effective_user.id, st5.get('family_idx'), 5, {
                'last_photo_message_id': None
            })
        except Exception:
            pass
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
            persisted = load_message_ids(user_id, family_id, 5)
            if isinstance(persisted, dict):
                pid = persisted.get('last_photo_message_id')
                if pid:
                    try:
                        await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=pid)
                    except Exception:
                        pass
                    try:
                        save_message_ids(user_id, family_id, 5, {'last_photo_message_id': None})
                    except Exception:
                        pass
    except Exception:
        pass
    st5 = context.user_data.get('stage5', {})
    st5['awaiting_input'] = False
    context.user_data['stage5'] = st5
    context.user_data.pop('stage5', None)
    from tgteacher_bot.handlers.user.stage_6 import stage6_start
    await stage6_start(update, context)


@track_metrics
async def stage5_cancel_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await show_stage5_task(update, context)


@track_metrics
async def stage5_finish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    # Чистим только фото этапа 5, текст оставляем для редактирования следующими этапами
    try:
        st5_local = context.user_data.get('stage5') or {}
        chat_id = update.effective_chat.id
        last_photo_id = st5_local.get('last_photo_message_id')
        if last_photo_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=last_photo_id)
            except Exception:
                pass
            st5_local['last_photo_message_id'] = None
        context.user_data['stage5'] = st5_local
        try:
            save_message_ids(update.effective_user.id, st5_local.get('family_idx'), 5, {
                'last_photo_message_id': None
            })
        except Exception:
            pass
    except Exception:
        pass
    st5 = context.user_data.get('stage5')
    if st5:
        user_id = update.effective_user.id
        family_id = st5['family_idx']
        await set_family_stage_done_pg(user_id, family_id, 5)
        # Проверяем, есть ли задачи на этапах 6–8
        from tgteacher_bot.db.families_repo import get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
        has_next = False
        for fetch in (get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
            next_tasks = await fetch(family_id)
            if next_tasks:
                has_next = True
                break
        context.user_data.pop('stage5', None)
        if has_next:
            from tgteacher_bot.handlers.user.stage_6 import stage6_start
            await stage6_start(update, context)
        else:
            from tgteacher_bot.db.user_repo import set_family_finished_pg
            await set_family_finished_pg(user_id, family_id)
            await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
            await update_stage_state(context, 'stage5', family_id, 5, user_id)
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
async def stage5_first_task_alert_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await update.callback_query.answer('Это первое задание.', show_alert=False)


@track_metrics
async def stage5_text_answer_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Обрабатываем только приватные чаты
    if not update.effective_chat or update.effective_chat.type != 'private':
        return
    user_data = getattr(context, 'user_data', None) or {}
    st5 = user_data.get('stage5')
    if not st5 or not st5.get('awaiting_input', False):
        return

    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

    family_id = st5['family_idx']
    tasks = await get_stage5_tasks_pg(family_id)
    idx_display = st5['task_idx']
    if idx_display >= len(tasks):
        return

    # Ensure tasks_order
    order = st5.get('tasks_order') or list(range(len(tasks)))
    if (not order) or (len(order) != len(tasks)) or (sorted(order) != list(range(len(tasks)))):
        order = list(range(len(tasks)))
        random.shuffle(order)
        st5['tasks_order'] = order
        context.user_data['stage5'] = st5
        await update_stage_state(context, 'stage5', family_id, 5, user_id)

    task = tasks[order[idx_display]]
    user_text = update.message.text or ''

    try:
        await update.message.delete()
    except Exception:
        pass

    is_correct = _is_correct_answer(user_text, task['answer'], task.get('alternatives', []))
    await set_stage5_answer_pg(user_id, family_id, order[idx_display], user_text, is_correct)
    if is_correct:
        await set_task_done_pg(user_id, family_id, 5, order[idx_display])
        feedback = f"✅ Верно! {task.get('explanation', '') or ''}"
    else:
        correct = html.escape((task.get('answer') or ''))
        explanation = task.get('explanation', '') or ''
        feedback = f"❌ Неправильно! Правильный ответ: <b>{correct}</b>\n{explanation}"

    st5['user_answer'] = user_text
    st5['answered'] = True
    st5['awaiting_input'] = False
    context.user_data['stage5'] = st5
    await update_stage_state(context, 'stage5', family_id, 5, user_id)

    chat_id = update.effective_chat.id
    last_text_id = st5.get('last_text_message_id')
    if last_text_id:
        try:
            # Determine if this stage is the last stage for the family
            from tgteacher_bot.db.families_repo import get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
            has_next = False
            for fetch in (get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
                next_tasks = await fetch(family_id)
                if next_tasks:
                    has_next = True
                    break
            is_last_stage = not has_next
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=last_text_id,
                text=format_stage5_text(idx_display, len(tasks), st5['awaiting_input'], st5['user_answer'], feedback),
                reply_markup=get_stage5_keyboard(idx_display, len(tasks), True, is_final_finish=is_last_stage),
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"[stage5] Не удалось отредактировать текст сообщения: {e}") 
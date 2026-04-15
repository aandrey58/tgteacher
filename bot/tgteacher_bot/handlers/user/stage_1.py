from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage1_words_pg, get_stage2_tasks_pg, get_stage3_tasks_pg, get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
from tgteacher_bot.db.user_repo import set_task_done_pg, set_last_opened_family_place_pg, get_last_opened_family_place_pg, get_user_stage_state_pg, get_current_family_idx_pg, mark_user_active_if_needed, set_family_finished_pg
from tgteacher_bot.utils.stage_state_manager import update_stage_state, ensure_stage_state
from tgteacher_bot.handlers.user.stage_2 import stage2_start
import logging
from tgteacher_bot.handlers.admin.admin_status import track_metrics
import random

logger = logging.getLogger(__name__)

def get_stage1_keyboard(word_idx, total_words, show_translation, is_last, is_final_finish: bool = False, is_last_stage: bool = False):
    nav_buttons = [
        InlineKeyboardButton('⬅️ Предыдущее слово', callback_data='stage1_prev'),
    ]
    if not is_last:
        nav_buttons.append(InlineKeyboardButton('➡️ Следующее слово', callback_data='stage1_next'))
    else:
        nav_buttons.append(InlineKeyboardButton('🏁 Завершить' if is_last_stage else '✅ Далее', callback_data='stage1_next'))
    trans_button = [
        InlineKeyboardButton(
            '🙈 Скрыть перевод' if show_translation else '👁 Показать перевод',
            callback_data='stage1_toggle_translation')
    ]
    bottom_row = []
    if is_last_stage and is_last:
        bottom_row = [InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')]
    elif is_last_stage:
        bottom_row = [
            InlineKeyboardButton('🏁 Завершить', callback_data='stage1_finish'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ]
    else:
        bottom_row = [
            InlineKeyboardButton('⏩ Пропустить этап', callback_data='stage1_skip_confirm'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ]
    return InlineKeyboardMarkup([
        nav_buttons,
        trans_button,
        bottom_row
    ])

async def _is_final_finish_after_stage1(family_id: int) -> bool:
    for fetch in (get_stage2_tasks_pg, get_stage3_tasks_pg, get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
        try:
            next_tasks = await fetch(family_id)
            if next_tasks:
                return False
        except Exception:
            # Если что-то пошло не так при проверке — считаем, что не финал
            return False
    return True

def get_stage1_text(family, word_idx, show_translation):
    word_obj = family['words'][word_idx]
    text = f"Слово {word_idx+1}/{len(family['words'])}\n<b>{word_obj['word']}</b>"
    if show_translation:
        text += f"\nПеревод: {word_obj.get('translation','') or ''}"
        if word_obj.get('example'):
            text += f"\nПример: {word_obj['example']}"
        if word_obj.get('example_translation'):
            text += f"\nПеревод примера: {word_obj['example_translation']}"
        if word_obj.get('hint'):
            text += f"\n{word_obj['hint']}"
    return text

async def get_default_stage1_state(user_id, family_id, stage_num):
    family_meta = await get_family_data_pg(family_id)
    if not family_meta:
        logger.error(f"Группа слов {family_id} не найдена при инициализации stage1 состояния.")
        return None

    last_place = await get_last_opened_family_place_pg(user_id, family_id)
    word_idx = 0
    if last_place and last_place[0] == 1:
        words = await get_stage1_words_pg(family_id)
        if words:
            word_idx = min(last_place[1], len(words) - 1)

    return {
        'family_idx': family_id,
        'word_idx': word_idx,
        'show_translation': False,
        'tasks_order': None,
    }

@track_metrics
async def stage1_nav_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    
    success, st = await ensure_stage_state(update, context, 'stage1', 1, get_default_stage1_state)
    if not success:
        await query.answer()
        return
    
    family_id = st['family_idx']
    family_meta = await get_family_data_pg(family_id)
    
    if not family_meta:
        await query.edit_message_text('Ошибка: группа слов не найдена.')
        return

    words = await get_stage1_words_pg(family_id)
    total = len(words)
    # Инициализируем/проверяем порядок задач (слов)
    order = st.get('tasks_order')
    if (not order) or (len(order) != total) or (sorted(order) != list(range(total))):
        order = list(range(total))
        random.shuffle(order)
        st['tasks_order'] = order
        context.user_data['stage1'] = st
        await update_stage_state(context, 'stage1', family_id, 1, user_id)
    
    word_idx_display = st['word_idx']
    is_last = (word_idx_display == total - 1)

    if query.data == 'stage1_prev':
        if word_idx_display == 0:
            await query.answer('Это первое слово', show_alert=False)
            return
        word_idx_display -= 1
    elif query.data == 'stage1_next':
        if is_last:
            # Если это финальный финиш (других этапов нет) — завершаем семью
            if await _is_final_finish_after_stage1(family_id):
                context.user_data.pop('stage1', None)
                await set_family_finished_pg(user_id, family_id)
                await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
                await update_stage_state(context, 'stage1', family_id, 1, user_id)
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{family_id}_0")],
                    [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
                ])
                await query.edit_message_text(
                    f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
                    reply_markup=keyboard
                )
                return
            # иначе двигаем на этап 2
            context.user_data.pop('stage1', None)
            await stage2_start(update, context)
            return
        word_idx_display += 1
    # Сохраняем прогресс по оригинальному индексу
    orig_idx = order[word_idx_display]
    await set_task_done_pg(user_id, family_id, 1, orig_idx)
    await set_last_opened_family_place_pg(user_id, family_id, 1, word_idx_display)
    st['word_idx'] = word_idx_display
    st['show_translation'] = False
    context.user_data['stage1'] = st
    await update_stage_state(context, 'stage1', family_id, 1, user_id)
    # Готовим отображаемый список слов в зафиксированном порядке
    words_display = [words[i] for i in order]
    family_for_text = {'words': words_display}

    is_last_now = (word_idx_display == total - 1)
    is_last_stage = await _is_final_finish_after_stage1(family_id)

    await query.edit_message_text(
        get_stage1_text(family_for_text, word_idx_display, False),
        reply_markup=get_stage1_keyboard(word_idx_display, total, False, is_last=is_last_now, is_final_finish=(is_last_now and is_last_stage), is_last_stage=is_last_stage),
        parse_mode='HTML'
    )

@track_metrics
async def stage1_toggle_translation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    
    success, st = await ensure_stage_state(update, context, 'stage1', 1, get_default_stage1_state)
    if not success:
        return
    
    family_id = st['family_idx']
    family_meta = await get_family_data_pg(family_id)

    if not family_meta:
        await query.edit_message_text('Ошибка: группа слов не найдена.')
        return

    words = await get_stage1_words_pg(family_id)
    total = len(words)
    # Ensure order
    order = st.get('tasks_order')
    if (not order) or (len(order) != total) or (sorted(order) != list(range(total))):
        order = list(range(total))
        random.shuffle(order)
        st['tasks_order'] = order
        context.user_data['stage1'] = st
        await update_stage_state(context, 'stage1', family_id, 1, user_id)

    word_idx_display = st['word_idx']
    st['show_translation'] = not st['show_translation']
    context.user_data['stage1'] = st
    user_id = update.effective_user.id
    await update_stage_state(context, 'stage1', family_id, 1, user_id)

    # Список слов в порядке отображения
    words_display = [words[i] for i in order]
    family_for_text = {'words': words_display}

    is_last_now = (word_idx_display == len(words) - 1)
    is_last_stage = await _is_final_finish_after_stage1(family_id)

    await query.edit_message_text(
        get_stage1_text(family_for_text, word_idx_display, st['show_translation']),
        reply_markup=get_stage1_keyboard(word_idx_display, len(words), st['show_translation'], is_last=(word_idx_display==len(words)-1), is_final_finish=(is_last_now and is_last_stage), is_last_stage=is_last_stage),
        parse_mode='HTML'
    )

@track_metrics
async def stage1_skip_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    text = (
        'Ты уверен, что хочешь пропустить этот этап?\n'
        'Рекомендуем пройти все этапы для лучшего запоминания слов!'
    )
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да, пропустить', callback_data='stage1_skip'),
            InlineKeyboardButton('❌ Отмена', callback_data='stage1_cancel_skip')
        ]
    ])
    await query.edit_message_text(text, reply_markup=keyboard)

@track_metrics
async def stage1_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    success, st = await ensure_stage_state(update, context, 'stage1', 1, get_default_stage1_state)
    if not success:
        return
    family_id = st['family_idx']
    # Если это последний этап — завершаем семью
    if await _is_final_finish_after_stage1(family_id):
        context.user_data.pop('stage1', None)
        await set_family_finished_pg(user_id, family_id)
        await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
        await update_stage_state(context, 'stage1', family_id, 1, user_id)
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
    # Иначе — переход к этапу 2
    context.user_data.pop('stage1', None)
    await stage2_start(update, context)

@track_metrics
async def stage1_cancel_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    
    success, st = await ensure_stage_state(update, context, 'stage1', 1, get_default_stage1_state)
    if not success:
        return
    
    family_id = st['family_idx']
    family_meta = await get_family_data_pg(family_id)

    if not family_meta:
        await query.edit_message_text('Ошибка: группа слов не найдена.')
        return

    words = await get_stage1_words_pg(family_id)
    total = len(words)
    # Ensure order
    order = st.get('tasks_order')
    if (not order) or (len(order) != total) or (sorted(order) != list(range(total))):
        order = list(range(total))
        random.shuffle(order)
        st['tasks_order'] = order
        context.user_data['stage1'] = st
        await update_stage_state(context, 'stage1', family_id, 1, user_id)

    word_idx_display = st['word_idx']

    # Список слов в порядке отображения
    words_display = [words[i] for i in order]
    family_for_text = {'words': words_display}

    is_last_now = (word_idx_display == len(words) - 1)
    is_last_stage = await _is_final_finish_after_stage1(family_id)

    await query.edit_message_text(
        get_stage1_text(family_for_text, word_idx_display, st['show_translation']),
        reply_markup=get_stage1_keyboard(word_idx_display, len(words), st['show_translation'], is_last=(word_idx_display==len(words)-1), is_final_finish=(is_last_now and is_last_stage), is_last_stage=is_last_stage),
        parse_mode='HTML'
    )

@track_metrics
async def stage1_finish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    success, st = await ensure_stage_state(update, context, 'stage1', 1, get_default_stage1_state)
    if not success:
        return
    family_id = st['family_idx']
    # Завершаем семью
    context.user_data.pop('stage1', None)
    await set_family_finished_pg(user_id, family_id)
    await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
    await update_stage_state(context, 'stage1', family_id, 1, user_id)
    family_meta = await get_family_data_pg(family_id)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{family_id}_0")],
        [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
    ])
    await query.edit_message_text(
        f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
        reply_markup=keyboard
    )

    
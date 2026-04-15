from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters
from tgteacher_bot.db.families_repo import get_family_data_pg, delete_family_from_pg, get_stage1_words_pg, get_stage2_tasks_pg, get_stage3_tasks_pg, get_stage6_tasks_pg, get_stage8_tasks_pg, get_all_stage_tasks_counts_for_families # <-- Добавлено
from tgteacher_bot.db.user_repo import set_family_stage_done_pg, start_family_progress_pg, set_task_done_pg, set_last_opened_family_place_pg, get_last_opened_family_place_pg, reset_family_progress_pg, set_stage2_answer_pg, get_stage2_answer_pg, get_completed_tasks_pg, set_stage3_answer_pg, get_stage3_answer_pg, get_stage6_answer_pg, set_stage6_answer_pg, get_stage8_answer_pg, get_user_stage_state_pg, set_user_stage_state_pg, set_current_family_idx_pg, get_current_family_idx_pg, get_all_user_completed_tasks_counts # <-- Добавлено
import logging
from telegram.error import BadRequest
from datetime import datetime
import unicodedata
import random
import json
import tgteacher_bot.utils.families_data as families_data
from tgteacher_bot.utils.stage_state_manager import initialize_stage_state, update_stage_state, ensure_stage_state
from tgteacher_bot.handlers.user.stage_1 import get_stage1_text, get_stage1_keyboard, stage1_nav_callback, stage1_toggle_translation_callback, stage1_skip_confirm_callback, stage1_skip_callback, stage1_cancel_skip_callback, stage1_finish_callback
from tgteacher_bot.handlers.user.stage_2 import stage2_start, stage2_answer_callback, stage2_next_callback, stage2_skip_confirm_callback, stage2_skip_callback, stage2_cancel_skip_callback, stage2_finish_callback, stage2_prev_callback, stage2_no_action_callback, stage2_first_task_alert_callback, show_stage2_task
from tgteacher_bot.handlers.user.stage_3 import stage3_start, stage3_answer_callback, stage3_next_callback, stage3_skip_confirm_callback, stage3_skip_callback, stage3_cancel_skip_callback, stage3_finish_callback, stage3_prev_callback, stage3_no_action_callback, stage3_first_task_alert_callback, show_stage3_task
from tgteacher_bot.handlers.user.stage_6 import stage6_start, stage6_toggle_choice_callback, stage6_confirm_callback, stage6_next_callback, stage6_skip_confirm_callback, stage6_skip_callback, stage6_cancel_skip_callback, stage6_finish_callback, stage6_prev_callback, stage6_no_action_callback, stage6_first_task_alert_callback, show_stage6_task
from tgteacher_bot.handlers.user.stage_7 import stage7_start, show_stage7_task, stage7_answer_callback, stage7_next_callback, stage7_prev_callback, stage7_skip_confirm_callback, stage7_skip_callback, stage7_cancel_skip_callback, stage7_finish_callback, stage7_no_action_callback, stage7_first_task_alert_callback
from tgteacher_bot.handlers.user.stage_4 import stage4_start, stage4_answer_callback, stage4_next_callback, stage4_skip_confirm_callback, stage4_skip_callback, stage4_cancel_skip_callback, stage4_finish_callback, stage4_prev_callback, stage4_no_action_callback, stage4_first_task_alert_callback, show_stage4_task
from tgteacher_bot.handlers.user.stage_8 import stage8_start, stage8_select_callback, stage8_confirm_callback, stage8_retry_callback, stage8_no_action_callback, stage8_finish_callback
# NEW: stage 5
from tgteacher_bot.handlers.user.stage_5 import stage5_start, stage5_prev_callback, stage5_next_callback, stage5_skip_confirm_callback, stage5_skip_callback, stage5_cancel_skip_callback, stage5_finish_callback, stage5_text_answer_handler, stage5_first_task_alert_callback
from tgteacher_bot.utils.common import OK_MENU
from tgteacher_bot.db.user_repo import mark_user_active_pg, mark_user_active_if_needed
# NEW: import missing stage fetchers for last-stage detection in stage1
from tgteacher_bot.db.families_repo import get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage7_tasks_pg
from tgteacher_bot.db.user_repo import get_user_subscription_info_pg

logger = logging.getLogger(__name__)

FAMILIES_PER_PAGE = 7


def _is_family_accessible(target: str | None, is_subscribed: bool) -> bool:
    t = (target or 'VIP+FREE').upper()
    if is_subscribed:
        return True
    return t in ('FREE', 'VIP+FREE')

def _get_filtered_families_for_user(is_subscribed: bool):
    """Возвращает отфильтрованные семьи для пользователя с учетом приоритетов"""
    accessible_families = [f for f in families_data.ALL_FAMILIES_META if _is_family_accessible(f.get('target'), is_subscribed)]
    
    if is_subscribed:
        # Для VIP пользователей: показываем ВСЕ доступные семьи
        # НЕ группируем по названию - показываем все версии
        return accessible_families
    else:
        # Для бесплатных пользователей: показываем только FREE версии
        # Группируем по названию и выбираем лучшую версию для каждого названия
        families_by_name = {}
        for family in accessible_families:
            name = family['name']
            target = family.get('target', 'VIP+FREE')
            
            if name not in families_by_name:
                families_by_name[name] = family
            else:
                current_target = families_by_name[name].get('target', 'VIP+FREE')
                
                # Для неподписчиков: FREE > VIP+FREE (VIP недоступен)
                if target == 'FREE' and current_target != 'FREE':
                    families_by_name[name] = family
                elif target == 'VIP+FREE' and current_target == 'VIP':
                    families_by_name[name] = family
        
        return list(families_by_name.values())

async def get_families_menu(user_id, page=0):
    start = page * FAMILIES_PER_PAGE
    end = start + FAMILIES_PER_PAGE
    
    # Фильтруем по доступности (подписка vs target)
    info = await get_user_subscription_info_pg(user_id)
    is_subscribed = bool(info.get('is_subscribed'))

    # Получаем отфильтрованные семьи с учетом приоритетов
    allowed = _get_filtered_families_for_user(is_subscribed)
    
    # Получаем ID семей для текущей страницы
    family_ids_on_page = [f_meta['id'] for f_meta in allowed[start:end]]

    # MCP: Делаем один запрос для получения общего количества заданий по всем этапам для всех семей
    all_tasks_counts = await get_all_stage_tasks_counts_for_families(family_ids_on_page)

    # MCP: Делаем один запрос для получения выполненных заданий для всех семей для текущего юзера
    user_completed_tasks_counts = await get_all_user_completed_tasks_counts(user_id, family_ids_on_page)

    families_page = allowed[start:end]
    # MCP: Если страница пустая, но не первая — возвращаем первую страницу
    if not families_page and page > 0:
        return await get_families_menu(user_id, page=0)
    keyboard = []
    for f_meta in families_page:
        family_id = f_meta['id'] 
        
        # Теперь используем предзагруженные данные
        s1_tasks_total = all_tasks_counts.get(family_id, {}).get(1, 0)
        s1_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(1, 0)
        
        s2_tasks_total = all_tasks_counts.get(family_id, {}).get(2, 0)
        s2_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(2, 0)
        
        s3_tasks_total = all_tasks_counts.get(family_id, {}).get(3, 0)
        s3_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(3, 0)
        
        s4_tasks_total = all_tasks_counts.get(family_id, {}).get(4, 0)
        s4_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(4, 0)
        
        # NEW stage 5
        s5_tasks_total = all_tasks_counts.get(family_id, {}).get(5, 0)
        s5_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(5, 0)
        
        s6_tasks_total = all_tasks_counts.get(family_id, {}).get(6, 0)
        s6_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(6, 0)

        # NEW stage 7
        s7_tasks_total = all_tasks_counts.get(family_id, {}).get(7, 0)
        s7_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(7, 0)
        
        s8_tasks_total = all_tasks_counts.get(family_id, {}).get(8, 0)
        s8_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(8, 0)
        
        total_tasks = s1_tasks_total + s2_tasks_total + s3_tasks_total + s4_tasks_total + s5_tasks_total + s6_tasks_total + s7_tasks_total + s8_tasks_total
        total_done = s1_tasks_done + s2_tasks_done + s3_tasks_done + s4_tasks_done + s5_tasks_done + s6_tasks_done + s7_tasks_done + s8_tasks_done
        overall_percent = round((total_done / total_tasks * 100)) if total_tasks > 0 else 0
        # Показываем бейдж таргета только для подписчиков, чтобы они видели VIP контент
        target_badge = ""
        if is_subscribed and f_meta.get('target') == 'VIP':
            target_badge = "💎"
        
        btn_text = f"{target_badge}📖 {f_meta['name']} ({overall_percent}%)"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"family_select_{family_id}")])

    total_families = len(allowed)
    total_pages = (total_families + FAMILIES_PER_PAGE - 1) // FAMILIES_PER_PAGE or 1

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'families_page_{page-1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
    
    nav_buttons.append(InlineKeyboardButton(f'{page+1}/{total_pages}', callback_data='noop'))

    if end < total_families:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data=f'families_page_{page+1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))

    keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton('🏠 В меню', callback_data='main_menu')])
    return InlineKeyboardMarkup(keyboard)

async def choose_family_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    menu = await get_families_menu(user_id, page=0)
    
    # Подсчитываем общее количество доступных семей
    info = await get_user_subscription_info_pg(user_id)
    is_subscribed = bool(info.get('is_subscribed'))
    allowed = _get_filtered_families_for_user(is_subscribed)
    
    title = f"📚 Выбери группу слов:\n📊 <b>Общее количество:</b> {len(allowed)}"
    
    await query.edit_message_text(title, reply_markup=menu, parse_mode='HTML')

async def families_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    page = int(query.data.replace('families_page_', ''))
    menu = await get_families_menu(user_id, page=page)
    
    # Подсчитываем общее количество доступных семей
    info = await get_user_subscription_info_pg(user_id)
    is_subscribed = bool(info.get('is_subscribed'))
    allowed = _get_filtered_families_for_user(is_subscribed)
    
    title = f"📚 Выбери группу слов:\n📊 <b>Общее количество:</b> {len(allowed)}"
    
    await query.edit_message_text(title, reply_markup=menu, parse_mode='HTML')

async def _is_family_last_stage_stage1(family_id: int) -> bool:
    # Returns True if there are no tasks for stages 2..8 for this family
    for fetch in (get_stage2_tasks_pg, get_stage3_tasks_pg, get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
        try:
            next_tasks = await fetch(family_id)
            if next_tasks:
                return False
        except Exception:
            return False
    return True

async def family_selected_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    family_id = int(query.data.replace('family_select_', ''))
    family_meta = await get_family_data_pg(family_id)
    
    if family_meta:
        user_id = update.effective_user.id
        await mark_user_active_if_needed(user_id, context)
        
        # Проверяем доступность VIP группы
        info = await get_user_subscription_info_pg(user_id)
        is_subscribed = bool(info.get('is_subscribed'))
        last_place = await get_last_opened_family_place_pg(user_id, family_id)
        
        # Если группа VIP и пользователь не подписан (даже если начинал)
        if family_meta.get('target') == 'VIP' and not is_subscribed:
            await query.edit_message_text(
                '💎 Эта группа слов доступна только для VIP подписчиков.\n\nОформите подписку, чтобы продолжить изучение премиум контента!',
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]])
            )
            return
        
        context.user_data['current_family_idx'] = family_id
        await set_current_family_idx_pg(user_id, family_id)
        
        if last_place is not None:
            keyboard = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton('▶️ Продолжить', callback_data=f'family_continue_{family_id}'),
                    InlineKeyboardButton('🔄 Начать сначала', callback_data=f'family_restart_{family_id}')
                ]
            ])
            await query.edit_message_text(
                '🤔 Вы уже изучали этот раздел.\n Хотите продолжить или начать сначала?',
                reply_markup=keyboard
            )
        else:
            # Инициализируем порядок и помечаем первым просмотренным именно оригинальный индекс из order[0]
            from tgteacher_bot.handlers.user.stage_1 import get_default_stage1_state
            success, st1 = await ensure_stage_state(update, context, 'stage1', 1, get_default_stage1_state)
            if not success:
                await query.edit_message_text('Ошибка: не удалось инициализировать этап 1.')
                return
            words = await get_stage1_words_pg(family_id)
            import random
            order = st1.get('tasks_order')
            if (not order) or (len(order) != len(words)) or (sorted(order) != list(range(len(words)))):
                order = list(range(len(words)))
                random.shuffle(order)
                st1['tasks_order'] = order
                context.user_data['stage1'] = st1
                await update_stage_state(context, 'stage1', family_id, 1, user_id)
            # Сохраняем last_opened как дисплейный 0 и done по оригинальному индексу
            await set_last_opened_family_place_pg(user_id, family_id, 1, 0)
            await set_task_done_pg(user_id, family_id, 1, order[0])
            words_display = [words[i] for i in order]
            family_for_text = {'words': words_display}
            is_last_stage = await _is_family_last_stage_stage1(family_id)
            await query.edit_message_text(
                get_stage1_text(family_for_text, 0, False),
                reply_markup=get_stage1_keyboard(0, len(words), False, is_last=(len(words)==1), is_last_stage=is_last_stage),
                parse_mode='HTML'
            )
    else:
        await query.edit_message_text('Ошибка: группа слов не найдена.')

async def family_restart_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    family_id = int(query.data.replace('family_restart_', ''))
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    family_meta = await get_family_data_pg(family_id)
    
    if not family_meta:
        await query.edit_message_text('Ошибка: группа слов не найдена.')
        return

    # Проверяем доступность VIP группы
    info = await get_user_subscription_info_pg(user_id)
    is_subscribed = bool(info.get('is_subscribed'))
    
    # Если группа VIP и пользователь не подписан
    if family_meta.get('target') == 'VIP' and not is_subscribed:
        await query.edit_message_text(
            '💎 Эта группа слов доступна только для VIP подписчиков.\n\nОформите подписку, чтобы получить доступ к премиум контенту!',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]])
        )
        return

    await reset_family_progress_pg(user_id, family_id)
    context.user_data['current_family_idx'] = family_id
    await set_current_family_idx_pg(user_id, family_id)
    # Очищаем все stageX из памяти, чтобы не подтянулось старое состояние
    for stage_key in ['stage1', 'stage2', 'stage3', 'stage4', 'stage5', 'stage6', 'stage7', 'stage8']:
        context.user_data.pop(stage_key, None)
    from tgteacher_bot.handlers.user.stage_1 import get_default_stage1_state
    success, st1 = await ensure_stage_state(update, context, 'stage1', 1, get_default_stage1_state)
    if not success:
        await query.edit_message_text('Ошибка: не удалось инициализировать этап 1.')
        return
    words = await get_stage1_words_pg(family_id)
    import random
    order = st1.get('tasks_order')
    if (not order) or (len(order) != len(words)) or (sorted(order) != list(range(len(words)))):
        order = list(range(len(words)))
        random.shuffle(order)
        st1['tasks_order'] = order
        context.user_data['stage1'] = st1
        await update_stage_state(context, 'stage1', family_id, 1, user_id)
    # Сохраняем last_opened как дисплейный 0 и done по оригинальному индексу
    await set_last_opened_family_place_pg(user_id, family_id, 1, 0)
    await set_task_done_pg(user_id, family_id, 1, order[0])
    words_display = [words[i] for i in order]
    family_for_text = {'words': words_display}
    is_last_stage = await _is_family_last_stage_stage1(family_id)
    await query.edit_message_text(
        get_stage1_text(family_for_text, 0, False),
        reply_markup=get_stage1_keyboard(0, len(words), False, is_last=(len(words)==1), is_last_stage=is_last_stage),
        parse_mode='HTML'
    )

async def family_continue_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    family_id = int(query.data.replace('family_continue_', ''))
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    last_place = await get_last_opened_family_place_pg(user_id, family_id)
    family_meta = await get_family_data_pg(family_id)
    
    if not family_meta:
        await query.edit_message_text('Ошибка: группа слов не найдена.')
        return
    
    context.user_data['current_family_idx'] = family_id
    await set_current_family_idx_pg(user_id, family_id)
    if last_place is not None:
        stage_num, task_idx = last_place
        # Удаляем промпт только если продолжаем этап 5
        if stage_num in (5, 7):
            try:
                if query and query.message:
                    await query.message.delete()
            except Exception:
                pass
        if stage_num == 1:
            from tgteacher_bot.handlers.user.stage_1 import get_default_stage1_state
            success, st1 = await ensure_stage_state(update, context, 'stage1', 1, get_default_stage1_state)
            if not success:
                await query.edit_message_text('Ошибка: не удалось инициализировать этап 1.')
                return
            words = await get_stage1_words_pg(family_id)
            import random
            order = st1.get('tasks_order')
            if (not order) or (len(order) != len(words)) or (sorted(order) != list(range(len(words)))):
                order = list(range(len(words)))
                random.shuffle(order)
                st1['tasks_order'] = order
                context.user_data['stage1'] = st1
                await update_stage_state(context, 'stage1', family_id, 1, user_id)
            words_display = [words[i] for i in order]
            family_for_text = {'words': words_display}
            # Пометим просмотренным оригинальный индекс для текущей дисплейной позиции
            try:
                await set_task_done_pg(user_id, family_id, 1, order[task_idx])
            except Exception:
                pass
            is_last_stage = await _is_family_last_stage_stage1(family_id)
            await query.edit_message_text(
                get_stage1_text(family_for_text, task_idx, False),
                reply_markup=get_stage1_keyboard(task_idx, len(words), False, is_last=(task_idx==len(words)-1), is_last_stage=is_last_stage),
                parse_mode='HTML'
            )
        elif stage_num == 2:
            from tgteacher_bot.handlers.user.stage_2 import get_default_stage2_state, show_stage2_task
            success, st2 = await ensure_stage_state(update, context, 'stage2', 2, get_default_stage2_state)
            if not success:
                await query.edit_message_text('Ошибка: не удалось инициализировать этап 2.')
                return
            await show_stage2_task(update, context)
        elif stage_num == 3:
            from tgteacher_bot.handlers.user.stage_3 import get_default_stage3_state, show_stage3_task
            success, st3 = await ensure_stage_state(update, context, 'stage3', 3, get_default_stage3_state)
            if not success:
                await query.edit_message_text('Ошибка: не удалось инициализировать этап 3.')
                return
            await show_stage3_task(update, context)
        elif stage_num == 4:
            from tgteacher_bot.handlers.user.stage_4 import get_default_stage4_state, show_stage4_task
            success, st4 = await ensure_stage_state(update, context, 'stage4', 4, get_default_stage4_state)
            if not success:
                await query.edit_message_text('Ошибка: не удалось инициализировать этап 4.')
                return
            await show_stage4_task(update, context)
        elif stage_num == 5:
            from tgteacher_bot.handlers.user.stage_5 import get_default_stage5_state, show_stage5_task
            success, st5 = await ensure_stage_state(update, context, 'stage5', 5, get_default_stage5_state)
            if not success:
                await query.edit_message_text('Ошибка: не удалось инициализировать этап 5.')
                return
            await show_stage5_task(update, context)
        elif stage_num == 6:
            from tgteacher_bot.handlers.user.stage_6 import get_default_stage6_state, show_stage6_task
            success, st6 = await ensure_stage_state(update, context, 'stage6', 6, get_default_stage6_state)
            if not success:
                await query.edit_message_text('Ошибка: не удалось инициализировать этап 6.')
                return
            await show_stage6_task(update, context)
        elif stage_num == 7:
            from tgteacher_bot.handlers.user.stage_7 import get_default_stage7_state, show_stage7_task
            success, st7 = await ensure_stage_state(update, context, 'stage7', 7, get_default_stage7_state)
            if not success:
                await query.edit_message_text('Ошибка: не удалось инициализировать этап 7.')
                return
            await show_stage7_task(update, context)
        elif stage_num == 8:
            await stage8_start(update, context)
        else:
            await query.edit_message_text('Ошибка: неизвестный этап.', parse_mode='HTML')
    else:
        await query.edit_message_text('Ошибка: не найдено место для продолжения.', parse_mode='HTML')

def register_family_handlers(application):
    application.add_handler(CallbackQueryHandler(choose_family_callback, pattern='^choose_family$'))
    application.add_handler(CallbackQueryHandler(families_page_callback, pattern='^families_page_'))
    application.add_handler(CallbackQueryHandler(family_selected_callback, pattern='^family_select_'))
    application.add_handler(CallbackQueryHandler(family_restart_callback, pattern='^family_restart_'))
    application.add_handler(CallbackQueryHandler(family_continue_callback, pattern='^family_continue_'))
    application.add_handler(CallbackQueryHandler(stage1_nav_callback, pattern='^stage1_(prev|next)$'))
    application.add_handler(CallbackQueryHandler(stage1_toggle_translation_callback, pattern='^stage1_toggle_translation$'))
    application.add_handler(CallbackQueryHandler(stage1_skip_confirm_callback, pattern='^stage1_skip_confirm$'))
    application.add_handler(CallbackQueryHandler(stage1_skip_callback, pattern='^stage1_skip$'))
    application.add_handler(CallbackQueryHandler(stage1_cancel_skip_callback, pattern='^stage1_cancel_skip$'))
    application.add_handler(CallbackQueryHandler(stage1_finish_callback, pattern='^stage1_finish$'))
    application.add_handler(CallbackQueryHandler(stage2_start, pattern='^stage2_start$'))
    application.add_handler(CallbackQueryHandler(stage2_answer_callback, pattern='^stage2_answer_'))
    application.add_handler(CallbackQueryHandler(stage2_next_callback, pattern='^stage2_next$'))
    application.add_handler(CallbackQueryHandler(stage2_skip_confirm_callback, pattern='^stage2_skip_confirm$'))
    application.add_handler(CallbackQueryHandler(stage2_skip_callback, pattern='^stage2_skip$'))
    application.add_handler(CallbackQueryHandler(stage2_cancel_skip_callback, pattern='^stage2_cancel_skip$'))
    application.add_handler(CallbackQueryHandler(stage2_finish_callback, pattern='^stage2_finish$'))
    application.add_handler(CallbackQueryHandler(stage2_prev_callback, pattern='^stage2_prev$'))
    application.add_handler(CallbackQueryHandler(stage2_no_action_callback, pattern='^stage2_no_action$'))
    application.add_handler(CallbackQueryHandler(stage2_first_task_alert_callback, pattern='^stage2_first_task_alert$'))
    application.add_handler(CallbackQueryHandler(stage3_start, pattern='^stage3_start$'))
    application.add_handler(CallbackQueryHandler(stage3_answer_callback, pattern='^stage3_answer_'))
    application.add_handler(CallbackQueryHandler(stage3_next_callback, pattern='^stage3_next$'))
    application.add_handler(CallbackQueryHandler(stage3_skip_confirm_callback, pattern='^stage3_skip_confirm$'))
    application.add_handler(CallbackQueryHandler(stage3_skip_callback, pattern='^stage3_skip$'))
    application.add_handler(CallbackQueryHandler(stage3_cancel_skip_callback, pattern='^stage3_cancel_skip$'))
    application.add_handler(CallbackQueryHandler(stage3_finish_callback, pattern='^stage3_finish$'))
    application.add_handler(CallbackQueryHandler(stage3_prev_callback, pattern='^stage3_prev$'))
    application.add_handler(CallbackQueryHandler(stage3_no_action_callback, pattern='^stage3_no_action$'))
    application.add_handler(CallbackQueryHandler(stage3_first_task_alert_callback, pattern='^stage3_first_task_alert$'))
    application.add_handler(CallbackQueryHandler(stage6_start, pattern='^stage6_start$'))
    application.add_handler(CallbackQueryHandler(stage6_toggle_choice_callback, pattern='^stage6_toggle_choice_'))
    application.add_handler(CallbackQueryHandler(stage6_confirm_callback, pattern='^stage6_confirm$'))
    application.add_handler(CallbackQueryHandler(stage6_next_callback, pattern='^stage6_next$'))
    application.add_handler(CallbackQueryHandler(stage6_skip_confirm_callback, pattern='^stage6_skip_confirm$'))
    application.add_handler(CallbackQueryHandler(stage6_skip_callback, pattern='^stage6_skip$'))
    application.add_handler(CallbackQueryHandler(stage6_cancel_skip_callback, pattern='^stage6_cancel_skip$'))
    application.add_handler(CallbackQueryHandler(stage6_finish_callback, pattern='^stage6_finish$'))
    application.add_handler(CallbackQueryHandler(stage6_prev_callback, pattern='^stage6_prev$'))
    application.add_handler(CallbackQueryHandler(stage6_no_action_callback, pattern='^stage6_no_action$'))
    application.add_handler(CallbackQueryHandler(stage6_first_task_alert_callback, pattern='^stage6_first_task_alert$'))
    # Stage 7
    application.add_handler(CallbackQueryHandler(stage7_start, pattern='^stage7_start$'))
    application.add_handler(CallbackQueryHandler(stage7_answer_callback, pattern='^stage7_answer_'))
    application.add_handler(CallbackQueryHandler(stage7_next_callback, pattern='^stage7_next$'))
    application.add_handler(CallbackQueryHandler(stage7_prev_callback, pattern='^stage7_prev$'))
    application.add_handler(CallbackQueryHandler(stage7_skip_confirm_callback, pattern='^stage7_skip_confirm$'))
    application.add_handler(CallbackQueryHandler(stage7_skip_callback, pattern='^stage7_skip$'))
    application.add_handler(CallbackQueryHandler(stage7_cancel_skip_callback, pattern='^stage7_cancel_skip$'))
    application.add_handler(CallbackQueryHandler(stage7_finish_callback, pattern='^stage7_finish$'))
    application.add_handler(CallbackQueryHandler(stage7_no_action_callback, pattern='^stage7_no_action$'))
    application.add_handler(CallbackQueryHandler(stage7_first_task_alert_callback, pattern='^stage7_first_task_alert$'))
    application.add_handler(CallbackQueryHandler(stage4_start, pattern='^stage4_start$'))
    application.add_handler(CallbackQueryHandler(stage4_answer_callback, pattern='^stage4_answer_'))
    application.add_handler(CallbackQueryHandler(stage4_next_callback, pattern='^stage4_next$'))
    application.add_handler(CallbackQueryHandler(stage4_skip_confirm_callback, pattern='^stage4_skip_confirm$'))
    application.add_handler(CallbackQueryHandler(stage4_skip_callback, pattern='^stage4_skip$'))
    application.add_handler(CallbackQueryHandler(stage4_cancel_skip_callback, pattern='^stage4_cancel_skip$'))
    application.add_handler(CallbackQueryHandler(stage4_finish_callback, pattern='^stage4_finish$'))
    application.add_handler(CallbackQueryHandler(stage4_prev_callback, pattern='^stage4_prev$'))
    application.add_handler(CallbackQueryHandler(stage4_no_action_callback, pattern='^stage4_no_action$'))
    application.add_handler(CallbackQueryHandler(stage4_first_task_alert_callback, pattern='^stage4_first_task_alert$'))
    application.add_handler(CallbackQueryHandler(stage8_start, pattern='^stage8_start$'))
    application.add_handler(CallbackQueryHandler(stage8_select_callback, pattern='^stage8_select_'))
    application.add_handler(CallbackQueryHandler(stage8_confirm_callback, pattern='^stage8_confirm$'))
    application.add_handler(CallbackQueryHandler(stage8_retry_callback, pattern='^stage8_retry$'))
    application.add_handler(CallbackQueryHandler(stage8_no_action_callback, pattern='^stage8_no_action$'))
    application.add_handler(CallbackQueryHandler(stage8_finish_callback, pattern='^stage8_finish$'))
    # NEW: Stage 5
    application.add_handler(CallbackQueryHandler(stage5_start, pattern='^stage5_start$'))
    application.add_handler(CallbackQueryHandler(stage5_prev_callback, pattern='^stage5_prev$'))
    application.add_handler(CallbackQueryHandler(stage5_next_callback, pattern='^stage5_next$'))
    application.add_handler(CallbackQueryHandler(stage5_skip_confirm_callback, pattern='^stage5_skip_confirm$'))
    application.add_handler(CallbackQueryHandler(stage5_skip_callback, pattern='^stage5_skip$'))
    application.add_handler(CallbackQueryHandler(stage5_cancel_skip_callback, pattern='^stage5_cancel_skip$'))
    application.add_handler(CallbackQueryHandler(stage5_finish_callback, pattern='^stage5_finish$'))
    application.add_handler(CallbackQueryHandler(stage5_first_task_alert_callback, pattern='^stage5_first_task_alert$'))
    # Текстовые ответы для этапа 5
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, stage5_text_answer_handler)) 
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler
import tgteacher_bot.utils.families_data as families_data
from tgteacher_bot.db.user_repo import get_started_families_ids_pg, get_completed_tasks_pg, get_stage2_answer_pg, get_stage3_answer_pg, get_stage6_answer_pg, get_stage8_answer_pg, get_all_stage_answers_for_family_pg, get_all_user_completed_tasks_counts, mark_user_active_pg, mark_user_active_if_needed, get_user_stage_state_pg, get_families_completion_counts_pg, get_user_subscription_info_pg
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage1_words_pg, get_stage2_tasks_pg, get_stage3_tasks_pg, get_stage6_tasks_pg, get_stage8_tasks_pg, get_all_stage_tasks_counts_for_families, get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage7_tasks_pg
from tgteacher_bot.handlers.admin.admin_status import track_metrics

PROGRESS_PER_PAGE = 7


def _is_family_accessible(target: str | None, is_subscribed: bool) -> bool:
    t = (target or 'VIP+FREE').upper()
    if is_subscribed:
        return True
    return t in ('FREE', 'VIP+FREE')

@track_metrics
async def get_stage_details_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает детальный прогресс по каждому заданию этапа."""
    query = update.callback_query
    await query.answer()
    
    try:
        _, _, family_idx_str, stage_num_str, page_str = query.data.split('_')
        family_idx = int(family_idx_str)
        stage_num = int(stage_num_str)
        page = int(page_str)
    except (ValueError, IndexError):
        await query.edit_message_text("Ошибка в данных. Попробуй снова.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]]))
        return

    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    family_meta = await get_family_data_pg(family_idx)
    if not family_meta:
        await query.edit_message_text("Ошибка: группа слов не найдена.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]]))
        return
    completed_tasks_dict = await get_completed_tasks_pg([user_id], family_idx, stage_num)
    completed_tasks = completed_tasks_dict.get(user_id, set())
    all_answers_dict = await get_all_stage_answers_for_family_pg([user_id], family_idx)
    all_answers = all_answers_dict.get(user_id, {}).get(stage_num, {})
    
    # Определяем отображаемый номер этапа по наличию этапов в группе (сдвигаем номера без изменения логики)
    all_tasks_counts_map = (await get_all_stage_tasks_counts_for_families([family_idx])).get(family_idx, {})
    present_stages = [s for s in [1, 2, 3, 4, 5, 6, 7, 8] if all_tasks_counts_map.get(s, 0) > 0]
    stage_display_map = {s: idx + 1 for idx, s in enumerate(present_stages)}
    display_stage_num = stage_display_map.get(stage_num, stage_num)

    if stage_num == 1:
        tasks = await get_stage1_words_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Слова"
    elif stage_num == 2:
        tasks = await get_stage2_tasks_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Задания"
    elif stage_num == 4:
        tasks = await get_stage4_tasks_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Задания"
    elif stage_num == 3:
        tasks = await get_stage3_tasks_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Определения"
    elif stage_num == 5:
        tasks = await get_stage5_tasks_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Картинки"
    elif stage_num == 6:
        tasks = await get_stage6_tasks_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Синонимы"
    elif stage_num == 7:
        tasks = await get_stage7_tasks_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Аудио"
    elif stage_num == 8:
        tasks = await get_stage8_tasks_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Текст"
    else:
        await query.edit_message_text("Неизвестный этап.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]]))
        return

    stage_name = f"Этап {display_stage_num}: {stage_title}"

    progress_text_lines = [f"<b>{stage_name}</b> (Выполнено {len(completed_tasks)}/{total_tasks})"]
    
    if stage_num == 1:
        completion_percent = round((len(completed_tasks) / total_tasks * 100) if total_tasks > 0 else 0)
        progress_text_lines.append(f"🏁 Выполнение: {completion_percent}%")
    elif stage_num in (2, 3, 4, 5, 6, 7):
        completion_percent = round((len(completed_tasks) / total_tasks * 100) if total_tasks > 0 else 0)
        progress_text_lines.append(f"🏁 Выполнение: {completion_percent}%")
        correct_answers = 0
        for i in range(total_tasks):
            if i in completed_tasks:
                answer = all_answers.get(i) # Используем данные из кэша
                if answer:
                    _, is_correct = answer
                    if is_correct:
                        correct_answers += 1
        accuracy_percent = round((correct_answers / len(completed_tasks) * 100) if len(completed_tasks) > 0 else 0)
        progress_text_lines.append(f"🎯 Точность: {accuracy_percent}%")
    elif stage_num == 8:
        completion_percent = round((len(completed_tasks) / total_tasks * 100) if total_tasks > 0 else 0)
        progress_text_lines.append(f"🏁 Выполнение: {completion_percent}%")
    
    progress_text_lines.append("")
    
    # Вытаскиваем tasks_order для юзера (только для этапов 1–7). Если нет/битый — показываем оригинальный порядок
    if stage_num in (1, 2, 3, 4, 5, 6, 7):
        user_state = await get_user_stage_state_pg(user_id, family_idx, stage_num)
        if user_state and isinstance(user_state, dict):
            order = user_state.get('tasks_order')
            if not order or len(order) != total_tasks or sorted(order) != list(range(total_tasks)):
                order = list(range(total_tasks))
        else:
            order = list(range(total_tasks))
    else:
        order = list(range(total_tasks))
    
    for display_idx in range(total_tasks):
        orig_idx = order[display_idx]
        if stage_num == 1:
            status = '✅ Просмотрено' if orig_idx in completed_tasks else '🚫 Не просмотрено'
            progress_text_lines.append(f"{display_idx+1}. {status}")
        elif stage_num == 8:
            tasks8 = tasks
            task = tasks8[orig_idx] if orig_idx < len(tasks8) else None
            if orig_idx in completed_tasks and task:
                answer = all_answers.get(orig_idx)
                correct_words = task['answers']
                if answer:
                    selected_words, _ = answer
                    for idx, correct_word in enumerate(correct_words):
                        user_word = selected_words[idx] if idx < len(selected_words) else ''
                        is_correct = (user_word == correct_word)
                        mark = '✅' if is_correct else '❌'
                        label = 'Правильно' if is_correct else 'Неправильно'
                        progress_text_lines.append(f"{idx+1}. {mark} {label}")
                else:
                    for idx in range(len(correct_words)):
                        progress_text_lines.append(f"{idx+1}. 🚫 Не отвечено")
            elif task:
                for idx in range(len(task['answers'])):
                    progress_text_lines.append(f"{idx+1}. 🚫 Не отвечено")
            else:
                progress_text_lines.append(f"{display_idx+1}. 🚫 Не отвечено")
        else:
            if orig_idx in completed_tasks:
                answer = all_answers.get(orig_idx)
                if answer:
                    _, is_correct = answer
                    status = '✅ Правильно' if is_correct else '❌ Неправильно'
                else:
                    status = '✅'
            else:
                status = '🚫 Не отвечено'
            progress_text_lines.append(f"{display_idx+1}. {status}")
    
    text = "\n".join(progress_text_lines)
    
    back_button = InlineKeyboardButton('⬅️ Назад', callback_data=f"progress_select_{family_idx}_{page}")
    keyboard = InlineKeyboardMarkup([[back_button]])
    
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


@track_metrics
async def get_family_progress_submenu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает подменю с выбором этапа для группы слов."""
    query = update.callback_query
    await query.answer()
    
    try:
        _, _, family_idx_str, page_str = query.data.split('_')
        family_idx = int(family_idx_str)
        page = int(page_str)
    except (ValueError, IndexError):
        await query.edit_message_text("Ошибка в данных.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]]))
        return

    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    family_meta = await get_family_data_pg(family_idx)
    if not family_meta:
        await query.edit_message_text("Ошибка: группа слов не найдена.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]]))
        return
    
    # MCP: Делаем один запрос для получения общего количества заданий по всем этапам для этой группы слов
    all_tasks_counts = await get_all_stage_tasks_counts_for_families([family_idx])
    # MCP: Делаем один запрос для получения выполненных заданий для этой группы слов для текущего юзера
    user_completed_tasks_counts = await get_all_user_completed_tasks_counts(user_id, [family_idx])
    # MCP: Получаем количество завершений для этой семьи
    completion_counts = await get_families_completion_counts_pg(user_id, [family_idx])
    completion_count = completion_counts.get(family_idx, 0)

    # Получаем данные для текущей группы слов
    family_all_tasks = all_tasks_counts.get(family_idx, {})
    family_user_completed_tasks = user_completed_tasks_counts.get(family_idx, {})

    # Определяем последовательную нумерацию отображаемых этапов
    present_stages = [s for s in [1, 2, 3, 4, 5, 6, 7, 8] if family_all_tasks.get(s, 0) > 0]
    stage_display_map = {s: idx + 1 for idx, s in enumerate(present_stages)}

    # Этап 1
    s1_tasks_total = family_all_tasks.get(1, 0)
    s1_tasks_done = family_user_completed_tasks.get(1, 0)
    s1_check = '✅' if s1_tasks_done >= s1_tasks_total > 0 else '❌'
    s1_percent = round((s1_tasks_done / s1_tasks_total * 100) if s1_tasks_total > 0 else 0)
    
    # Этап 2
    s2_tasks_total = family_all_tasks.get(2, 0)
    s2_tasks_done = family_user_completed_tasks.get(2, 0)
    s2_check = '✅' if s2_tasks_done >= s2_tasks_total > 0 else '❌'
    s2_percent = round((s2_tasks_done / s2_tasks_total * 100) if s2_tasks_total > 0 else 0)
    
    # Этап 4
    s4_tasks_total = family_all_tasks.get(4, 0)
    s4_tasks_done = family_user_completed_tasks.get(4, 0)
    s4_check = '✅' if s4_tasks_done >= s4_tasks_total > 0 else '❌'
    s4_percent = round((s4_tasks_done / s4_tasks_total * 100) if s4_tasks_total > 0 else 0)

    # Этап 3
    s3_tasks_total = family_all_tasks.get(3, 0)
    s3_tasks_done = family_user_completed_tasks.get(3, 0)
    s3_check = '✅' if s3_tasks_done >= s3_tasks_total > 0 else '❌'
    s3_percent = round((s3_tasks_done / s3_tasks_total * 100) if s3_tasks_total > 0 else 0)

    # Этап 5
    s5_tasks_total = family_all_tasks.get(5, 0)
    s5_tasks_done = family_user_completed_tasks.get(5, 0)
    s5_check = '✅' if s5_tasks_done >= s5_tasks_total > 0 else '❌'
    s5_percent = round((s5_tasks_done / s5_tasks_total * 100) if s5_tasks_total > 0 else 0)

    # Этап 6
    s6_tasks_total = family_all_tasks.get(6, 0)
    s6_tasks_done = family_user_completed_tasks.get(6, 0)
    s6_check = '✅' if s6_tasks_done >= s6_tasks_total > 0 else '❌'
    s6_percent = round((s6_tasks_done / s6_tasks_total * 100) if s6_tasks_total > 0 else 0)
    # Этап 7
    s7_tasks_total = family_all_tasks.get(7, 0)
    s7_tasks_done = family_user_completed_tasks.get(7, 0)
    s7_check = '✅' if s7_tasks_done >= s7_tasks_total > 0 else '❌'
    s7_percent = round((s7_tasks_done / s7_tasks_total * 100) if s7_tasks_total > 0 else 0)

    # Этап 8
    s8_tasks_total = family_all_tasks.get(8, 0)
    s8_tasks_done = family_user_completed_tasks.get(8, 0)
    s8_check = '✅' if s8_tasks_done >= s8_tasks_total > 0 else '❌'
    s8_percent = round((s8_tasks_done / s8_tasks_total * 100) if s8_tasks_total > 0 else 0)

    keyboard_rows = []
    if s1_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[1]}: Слова {s1_check} ({s1_percent}%)", callback_data=f"progress_stage_{family_idx}_1_{page}")])
    if s2_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[2]}: Задания {s2_check} ({s2_percent}%)", callback_data=f"progress_stage_{family_idx}_2_{page}")])
    if s3_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[3]}: Определения {s3_check} ({s3_percent}%)", callback_data=f"progress_stage_{family_idx}_3_{page}")])
    if s4_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[4]}: Задания {s4_check} ({s4_percent}%)", callback_data=f"progress_stage_{family_idx}_4_{page}")])
    if s5_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[5]}: Картинки {s5_check} ({s5_percent}%)", callback_data=f"progress_stage_{family_idx}_5_{page}")])
    if s6_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[6]}: Синонимы {s6_check} ({s6_percent}%)", callback_data=f"progress_stage_{family_idx}_6_{page}")])
    if s7_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[7]}: Аудио {s7_check} ({s7_percent}%)", callback_data=f"progress_stage_{family_idx}_7_{page}")])
    if s8_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[8]}: Текст {s8_check} ({s8_percent}%)", callback_data=f"progress_stage_{family_idx}_8_{page}")])
    
    keyboard_rows.append([InlineKeyboardButton('⬅️ Назад к прогрессу', callback_data=f'progress_page_{page}')])

    # MCP: Добавляем информацию о количестве завершений
    completion_text = f"\nГруппа слов пройдена раз: {completion_count}" if completion_count > 0 else ""
    text = f"📈 Прогресс по группе слов: <b>{family_meta['name']}</b>{completion_text}"
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard_rows), parse_mode='HTML')


async def get_main_progress_keyboard(user_id: int, page: int = 0):
    """Генерирует главный экран прогресса с пагинацией."""
    started_families_ids = await get_started_families_ids_pg(user_id)
    # MCP: Оставляем только реально существующие группы слов
    existing_family_ids = set(f['id'] for f in families_data.ALL_FAMILIES_META)
    started_families_ids = [fid for fid in started_families_ids if fid in existing_family_ids]
    
    if not started_families_ids:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton('🎓 Вперёд к знаниям!', callback_data='choose_family')],
            [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
        ]), "Вы ещё не начали ни одного раздела.\nВперёд к знаниям!"

    # Получаем информацию о подписке пользователя
    info = await get_user_subscription_info_pg(user_id)
    is_subscribed = bool(info.get('is_subscribed'))

    start = page * PROGRESS_PER_PAGE
    end = start + PROGRESS_PER_PAGE
    page_ids = started_families_ids[start:end]

    # MCP: Если страница пустая, но не первая — возвращаем первую страницу
    if not page_ids and page > 0:
        return await get_main_progress_keyboard(user_id, page=0)

    # MCP: Делаем один запрос для получения общего количества заданий по всем этапам для всех семей на странице
    all_tasks_counts = await get_all_stage_tasks_counts_for_families(page_ids)

    # MCP: Делаем один запрос для получения выполненных заданий для всех семей на странице для текущего юзера
    user_completed_tasks_counts = await get_all_user_completed_tasks_counts(user_id, page_ids)

    # MCP: Получаем количество завершений для всех семей на странице
    completion_counts = await get_families_completion_counts_pg(user_id, page_ids)

    keyboard_rows = []
    for family_id in page_ids:
        family_meta = await get_family_data_pg(family_id)
        if not family_meta:
            continue
        family_name = family_meta['name']
        
        # Считаем общий прогресс по всем этапам, используя предзагруженные данные
        s1_tasks_total = all_tasks_counts.get(family_id, {}).get(1, 0)
        s1_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(1, 0)
        
        s2_tasks_total = all_tasks_counts.get(family_id, {}).get(2, 0)
        s2_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(2, 0)
        
        s3_tasks_total = all_tasks_counts.get(family_id, {}).get(3, 0)
        s3_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(3, 0)
        
        s4_tasks_total = all_tasks_counts.get(family_id, {}).get(4, 0)
        s4_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(4, 0)

        s5_tasks_total = all_tasks_counts.get(family_id, {}).get(5, 0)
        s5_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(5, 0)
        
        s6_tasks_total = all_tasks_counts.get(family_id, {}).get(6, 0)
        s6_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(6, 0)
        
        s7_tasks_total = all_tasks_counts.get(family_id, {}).get(7, 0)
        s7_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(7, 0)
        s8_tasks_total = all_tasks_counts.get(family_id, {}).get(8, 0)
        s8_tasks_done = user_completed_tasks_counts.get(family_id, {}).get(8, 0)
        
        total_tasks = s1_tasks_total + s2_tasks_total + s3_tasks_total + s4_tasks_total + s5_tasks_total + s6_tasks_total + s7_tasks_total + s8_tasks_total
        total_done = s1_tasks_done + s2_tasks_done + s3_tasks_done + s4_tasks_done + s5_tasks_done + s6_tasks_done + s7_tasks_done + s8_tasks_done
        
        overall_percent = round((total_done / total_tasks * 100) if total_tasks > 0 else 0)
        
        # Показываем бейдж таргета для VIP групп, которые пользователь уже начал
        # (независимо от текущего статуса подписки)
        target_badge = ""
        if family_meta.get('target') == 'VIP':
            target_badge = "💎"
        
        btn_text = f"{target_badge}📖 Группа слов «{family_name}» ({overall_percent}%)"
        callback_data = f"progress_select_{family_id}_{page}"
        keyboard_rows.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])

    total_started_families = len(started_families_ids)
    total_pages = (total_started_families + PROGRESS_PER_PAGE - 1) // PROGRESS_PER_PAGE or 1
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'progress_page_{page-1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))

    nav_buttons.append(InlineKeyboardButton(f'{page+1}/{total_pages}', callback_data='noop'))

    if end < total_started_families:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data=f'progress_page_{page+1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    
    keyboard_rows.append(nav_buttons)

    keyboard_rows.append([InlineKeyboardButton('🏠 В меню', callback_data='main_menu')])
    
    text = f"📈 Ваш прогресс:\n\nНажмите на группу слов, чтобы увидеть детали."
    
    return InlineKeyboardMarkup(keyboard_rows), text


@track_metrics
async def my_progress_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    keyboard, text = await get_main_progress_keyboard(user_id, page=0)
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


@track_metrics
async def progress_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    page = int(query.data.replace('progress_page_', ''))
    keyboard, text = await get_main_progress_keyboard(user_id, page=page)
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


def register_progress_handlers(application):
    application.add_handler(CallbackQueryHandler(my_progress_callback, pattern='^my_progress$'))
    application.add_handler(CallbackQueryHandler(progress_page_callback, pattern='^progress_page_'))
    application.add_handler(CallbackQueryHandler(get_family_progress_submenu, pattern='^progress_select_'))
    application.add_handler(CallbackQueryHandler(get_stage_details_view, pattern='^progress_stage_')) 
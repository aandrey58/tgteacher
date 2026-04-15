from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler
from tgteacher_bot.db.user_repo import get_started_families_ids_pg, get_completed_tasks_pg, get_stage2_answer_pg, get_stage3_answer_pg, get_stage6_answer_pg, get_stage8_answer_pg, get_all_stage_answers_for_family_pg, get_all_user_completed_tasks_counts, get_families_completion_counts_pg
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage1_words_pg, get_stage2_tasks_pg, get_stage3_tasks_pg, get_stage6_tasks_pg, get_stage8_tasks_pg, get_all_stage_tasks_counts_for_families, get_families_data_bulk, get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage7_tasks_pg

PROGRESS_PER_PAGE = 7

# Главный экран прогресса пользователя (список семей)
async def admin_progress_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # MCP: Проверяем, пришли ли мы из поиска
    from_search = 'from_search' in query.data
    # MCP: Проверяем, пришли ли мы из поиска НОВЫХ юзеров
    from_new_search = context.user_data.get('from_new_search', False)

    if from_search:
        context.user_data['from_search'] = True
        context.user_data.pop('from_new_search', None) # Чистим флаг поиска новых
        data = query.data.replace('admin_progress_from_search_', '')
    else:
        context.user_data.pop('from_search', None) # Чистим флаг, если пришли не из поиска
        # MCP: не чистим from_new_search, если он есть
        if not from_new_search:
            context.user_data.pop('from_new_search', None)
        data = query.data.replace('admin_progress_', '')

    parts = data.split('_')
    try:
        # MCP: поддержка admin_progress_inactive_{user_id}_1_{period}_{page}
        if parts[0] == 'inactive':
            user_id_str = parts[1]
            page_str = parts[2]
            if len(parts) > 3:
                period = '_'.join(parts[3:-1])
                period_page = int(parts[-1])
                context.user_data['admin_period'] = period
                context.user_data['admin_period_page'] = period_page
            else:
                period = context.user_data.get('admin_period')
                period_page = context.user_data.get('admin_period_page')
        else:
            user_id_str = parts[0]
            page_str = parts[1]
            if len(parts) > 2:
                period = '_'.join(parts[2:-1])
                period_page = int(parts[-1])
                context.user_data['admin_period'] = period
                context.user_data['admin_period_page'] = period_page
            else:
                period = context.user_data.get('admin_period')
                period_page = context.user_data.get('admin_period_page')
    except (ValueError, IndexError) as e:
        await query.edit_message_text(f'❌ Ошибка парсинга callback_data: {query.data}')
        return

    user_id = int(user_id_str)
    page = int(page_str)
    keyboard, text = await get_admin_main_progress_keyboard(user_id, page, period, period_page, from_search, from_new_search)
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

async def get_admin_main_progress_keyboard(user_id: int, page: int = 1, period=None, period_page=None, from_search=False, from_new_search=False):
    started_families_ids = await get_started_families_ids_pg(user_id)
    if not started_families_ids:
        # MCP: Адаптируем кнопку "В профиль" под режим поиска
        if from_search:
            back_cb = 'admin_search_id_back_to_results'
        # MCP: Адаптируем кнопку "В профиль" под режим поиска НОВЫХ
        elif from_new_search:
            back_cb = 'admin_new_search_back_to_results'
        else:
            back_cb = f'admin_back_to_profile_{user_id}'
            if period and period_page is not None:
                back_cb = f'admin_back_to_profile_{user_id}_{period}_{period_page}'
        return InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ В профиль', callback_data=back_cb)]
        ]), "❗️ У пользователя нет прогресса."
    
    # MCP: Фильтруем удалённые/недоступные семьи ДО пагинации, чтобы не было пустых страниц
    families_meta_all = await get_families_data_bulk(started_families_ids)
    valid_families_ids = [fid for fid in started_families_ids if families_meta_all.get(fid)]
    if not valid_families_ids:
        # MCP: Адаптируем кнопку "В профиль" под режим поиска
        if from_search:
            back_cb = 'admin_search_id_back_to_results'
        # MCP: Адаптируем кнопку "В профиль" под режим поиска НОВЫХ
        elif from_new_search:
            back_cb = 'admin_new_search_back_to_results'
        else:
            back_cb = f'admin_back_to_profile_{user_id}'
            if period and period_page is not None:
                back_cb = f'admin_back_to_profile_{user_id}_{period}_{period_page}'
        return InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ В профиль', callback_data=back_cb)]
        ]), "❗️ У пользователя нет прогресса."

    # MCP: Корректируем номер страницы в допустимый диапазон
    total_pages = (len(valid_families_ids) + PROGRESS_PER_PAGE - 1) // PROGRESS_PER_PAGE
    if page < 1:
        page = 1
    if page > total_pages:
        page = total_pages

    start = (page-1) * PROGRESS_PER_PAGE
    end = start + PROGRESS_PER_PAGE
    page_ids = valid_families_ids[start:end]
    
    # ОПТИМИЗАЦИЯ: Получаем все данные одним махом
    all_tasks_counts = await get_all_stage_tasks_counts_for_families(page_ids)
    user_completed_tasks_counts = await get_all_user_completed_tasks_counts(user_id, page_ids)
    families_meta = await get_families_data_bulk(page_ids)
    # MCP: Получаем количество завершений для всех семей на странице
    completion_counts = await get_families_completion_counts_pg(user_id, page_ids)

    keyboard_rows = []
    total_pages = (len(valid_families_ids) + PROGRESS_PER_PAGE - 1) // PROGRESS_PER_PAGE
    for family_id in page_ids:
        family_meta = families_meta.get(family_id)
        if not family_meta:
            continue
        family_name = family_meta['name']
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
        
        # Добавляем эмодзи 💎 для VIP групп
        target = family_meta.get('target', 'VIP+FREE')
        if target == 'VIP':
            emoji = "💎📖"
        else:
            emoji = "📖"
        btn_text = f"{emoji} Группа слов «{family_name}» ({overall_percent}%)"
        callback_data = f"admin_progress_select_{user_id}_{family_id}_{page}"
        keyboard_rows.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])
    nav_buttons = []
    # MCP: Делаю пагинацию как в users — три кнопки в одной строке
    if page > 1:
        # MCP: Адаптируем пагинацию под режим поиска
        cb_prev = f'admin_progress_{"from_search_" if from_search else ""}{user_id}_{page-1}'
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=cb_prev))
    else:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
    nav_buttons.append(InlineKeyboardButton(f'{page}/{total_pages}', callback_data='noop'))
    if end < len(valid_families_ids):
        # MCP: Адаптируем пагинацию под режим поиска
        cb_next = f'admin_progress_{"from_search_" if from_search else ""}{user_id}_{page+1}'
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data=cb_next))
    else:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    keyboard_rows.append(nav_buttons)
    # MCP: Адаптируем кнопку "В профиль" под режим поиска
    if from_search:
        back_cb = 'admin_search_id_back_to_results'
    # MCP: Адаптируем кнопку "В профиль" под режим поиска НОВЫХ
    elif from_new_search:
        back_cb = 'admin_new_search_back_to_results'
    else:
        if period and period_page is not None and str(period).startswith('inactive'):
            back_cb = f'admin_back_to_profile_inactive_{user_id}_{period}_{period_page}'
        else:
            back_cb = f'admin_back_to_profile_{user_id}'
            if period and period_page is not None:
                back_cb = f'admin_back_to_profile_{user_id}_{period}_{period_page}'
    keyboard_rows.append([InlineKeyboardButton('⬅️ В профиль', callback_data=back_cb)])
    text = f"📈 Прогресс пользователя <b>{user_id}:</b>"
    return InlineKeyboardMarkup(keyboard_rows), text

# Подменю этапов группы слов
async def admin_family_progress_submenu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.replace('admin_progress_select_', '')
    user_id_str, family_idx_str, page_str = data.split('_')
    user_id = int(user_id_str)
    family_idx = int(family_idx_str)
    page = int(page_str)
    # MCP: Проверяем, не в режиме ли мы поиска
    from_search = context.user_data.get('from_search', False)
    # MCP: Проверяем, не в режиме ли мы поиска НОВЫХ юзеров
    from_new_search = context.user_data.get('from_new_search', False)
    # MCP: Пробуем достать period и period_page из context.user_data, если есть
    period = context.user_data.get('admin_period')
    period_page = context.user_data.get('admin_period_page')
    family_meta = await get_family_data_pg(family_idx)
    if not family_meta:
        # MCP: Адаптируем кнопку "В профиль" под режим поиска
        if from_search:
            back_cb = 'admin_search_id_back_to_results'
        # MCP: Адаптируем кнопку "В профиль" под режим поиска НОВЫХ
        elif from_new_search:
            back_cb = 'admin_new_search_back_to_results'
        else:
            back_cb = f'admin_back_to_profile_{user_id}'
            if period and period_page is not None:
                back_cb = f'admin_back_to_profile_{user_id}_{period}_{period_page}'
        await query.edit_message_text("Ошибка: группа слов не найдена.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ В профиль', callback_data=back_cb)]]))
        return
    all_tasks_counts = await get_all_stage_tasks_counts_for_families([family_idx])
    user_completed_tasks_counts = await get_all_user_completed_tasks_counts(user_id, [family_idx])
    # MCP: Получаем количество завершений для этой семьи
    completion_counts = await get_families_completion_counts_pg(user_id, [family_idx])
    completion_count = completion_counts.get(family_idx, 0)
    family_all_tasks = all_tasks_counts.get(family_idx, {})
    family_user_completed_tasks = user_completed_tasks_counts.get(family_idx, {})

    # Определяем последовательную нумерацию отображаемых этапов
    present_stages = [s for s in [1, 2, 3, 4, 5, 6, 7, 8] if family_all_tasks.get(s, 0) > 0]
    stage_display_map = {s: idx + 1 for idx, s in enumerate(present_stages)}

    s1_tasks_total = family_all_tasks.get(1, 0)
    s1_tasks_done = family_user_completed_tasks.get(1, 0)
    s1_check = '✅' if s1_tasks_done >= s1_tasks_total > 0 else '❌'
    s1_percent = round((s1_tasks_done / s1_tasks_total * 100) if s1_tasks_total > 0 else 0)
    s2_tasks_total = family_all_tasks.get(2, 0)
    s2_tasks_done = family_user_completed_tasks.get(2, 0)
    s2_check = '✅' if s2_tasks_done >= s2_tasks_total > 0 else '❌'
    s2_percent = round((s2_tasks_done / s2_tasks_total * 100) if s2_tasks_total > 0 else 0)
    s3_tasks_total = family_all_tasks.get(3, 0)
    s3_tasks_done = family_user_completed_tasks.get(3, 0)
    s3_check = '✅' if s3_tasks_done >= s3_tasks_total > 0 else '❌'
    s3_percent = round((s3_tasks_done / s3_tasks_total * 100) if s3_tasks_total > 0 else 0)
    s6_tasks_total = family_all_tasks.get(6, 0)
    s6_tasks_done = family_user_completed_tasks.get(6, 0)
    s6_check = '✅' if s6_tasks_done >= s6_tasks_total > 0 else '❌'
    s6_percent = round((s6_tasks_done / s6_tasks_total * 100) if s6_tasks_total > 0 else 0)
    s7_tasks_total = family_all_tasks.get(7, 0)
    s7_tasks_done = family_user_completed_tasks.get(7, 0)
    s7_check = '✅' if s7_tasks_done >= s7_tasks_total > 0 else '❌'
    s7_percent = round((s7_tasks_done / s7_tasks_total * 100) if s7_tasks_total > 0 else 0)
    s8_tasks_total = family_all_tasks.get(8, 0)
    s8_tasks_done = family_user_completed_tasks.get(8, 0)
    s8_check = '✅' if s8_tasks_done >= s8_tasks_total > 0 else '❌'
    s8_percent = round((s8_tasks_done / s8_tasks_total * 100) if s8_tasks_total > 0 else 0)
    # Этап 4
    s4_tasks_total = family_all_tasks.get(4, 0)
    s4_tasks_done = family_user_completed_tasks.get(4, 0)
    s4_check = '✅' if s4_tasks_done >= s4_tasks_total > 0 else '❌'
    s4_percent = round((s4_tasks_done / s4_tasks_total * 100) if s4_tasks_total > 0 else 0)
    # Этап 5
    s5_tasks_total = family_all_tasks.get(5, 0)
    s5_tasks_done = family_user_completed_tasks.get(5, 0)
    s5_check = '✅' if s5_tasks_done >= s5_tasks_total > 0 else '❌'
    s5_percent = round((s5_tasks_done / s5_tasks_total * 100) if s5_tasks_total > 0 else 0)
    keyboard_rows = []
    if s1_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[1]}: Слова {s1_check} ({s1_percent}%)", callback_data=f"admin_progress_stage_{user_id}_{family_idx}_1_{page}")])
    if s2_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[2]}: Задания {s2_check} ({s2_percent}%)", callback_data=f"admin_progress_stage_{user_id}_{family_idx}_2_{page}")])
    if s3_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[3]}: Определения {s3_check} ({s3_percent}%)", callback_data=f"admin_progress_stage_{user_id}_{family_idx}_3_{page}")])
    if s4_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[4]}: Задания {s4_check} ({s4_percent}%)", callback_data=f"admin_progress_stage_{user_id}_{family_idx}_4_{page}")])
    if s5_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[5]}: Картинки {s5_check} ({s5_percent}%)", callback_data=f"admin_progress_stage_{user_id}_{family_idx}_5_{page}")])
    if s6_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[6]}: Синонимы {s6_check} ({s6_percent}%)", callback_data=f"admin_progress_stage_{user_id}_{family_idx}_6_{page}")])
    if s7_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[7]}: Аудио {s7_check} ({s7_percent}%)", callback_data=f"admin_progress_stage_{user_id}_{family_idx}_7_{page}")])
    if s8_tasks_total > 0:
        keyboard_rows.append([InlineKeyboardButton(f"Этап {stage_display_map[8]}: Текст {s8_check} ({s8_percent}%)", callback_data=f"admin_progress_stage_{user_id}_{family_idx}_8_{page}")])
    # MCP: Адаптируем кнопку "К списку семей" под режим поиска
    back_cb = f'admin_progress_{"from_search_" if from_search else ""}{user_id}_{page}'
    keyboard_rows.append([InlineKeyboardButton('⬅️ К списку семей', callback_data=back_cb)])
    
    # MCP: Добавляем информацию о количестве завершений
    completion_text = f"\nГруппа слов пройдена раз: {completion_count}" if completion_count > 0 else ""
    text = f"📈 Прогресс по группе слов: <b>{family_meta['name']}</b>{completion_text}"
    return await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard_rows), parse_mode='HTML')

# Детальный прогресс по этапу
async def admin_stage_details_view(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.replace('admin_progress_stage_', '')
    user_id_str, family_idx_str, stage_num_str, page_str = data.split('_')
    user_id = int(user_id_str)
    family_idx = int(family_idx_str)
    stage_num = int(stage_num_str)
    page = int(page_str)
    # MCP: Пробуем достать period и period_page из context.user_data, если есть
    period = context.user_data.get('admin_period')
    period_page = context.user_data.get('admin_period_page')
    family_meta = await get_family_data_pg(family_idx)
    if not family_meta:
        back_cb = f'admin_back_to_profile_{user_id}'
        if period and period_page is not None:
            back_cb = f'admin_back_to_profile_{user_id}_{period}_{period_page}'
        await query.edit_message_text("Ошибка: группа слов не найдена.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ В профиль', callback_data=back_cb)]]))
        return
    
    # ИСПРАВЛЕНИЕ: передаем user_id в виде списка и правильно разбираем результат
    completed_tasks_dict = await get_completed_tasks_pg([user_id], family_idx, stage_num)
    completed_tasks = completed_tasks_dict.get(user_id, set())

    all_answers_dict = await get_all_stage_answers_for_family_pg([user_id], family_idx)
    all_answers = all_answers_dict.get(user_id, {}).get(stage_num, {})

    # Определяем отображаемый номер этапа
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
    elif stage_num == 3:
        tasks = await get_stage3_tasks_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Определения"
    elif stage_num == 4:
        tasks = await get_stage4_tasks_pg(family_idx)
        total_tasks = len(tasks)
        stage_title = "Задания"
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
        await query.edit_message_text("Неизвестный этап.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ В профиль', callback_data=f'admin_back_to_profile_{user_id}_{period}_{period_page}')]]))
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
                answer = all_answers.get(i)
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
    for i in range(total_tasks):
        if stage_num == 1:
            status = '✅ Просмотрено' if i in completed_tasks else '🚫 Не просмотрено'
            progress_text_lines.append(f"{i+1}. {status}")
        elif stage_num == 8:
            tasks8 = tasks
            task = tasks8[i] if i < len(tasks8) else None
            if i in completed_tasks and task:
                answer = all_answers.get(i)
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
                progress_text_lines.append(f"{i+1}. 🚫 Не отвечено")
        else:
            if i in completed_tasks:
                answer = all_answers.get(i)
                if answer:
                    _, is_correct = answer
                    status = '✅ Правильно' if is_correct else '❌ Неправильно'
                else:
                    status = '✅'
            else:
                status = '🚫 Не отвечено'
            progress_text_lines.append(f"{i+1}. {status}")
    text = "\n".join(progress_text_lines)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('⬅️ К этапам', callback_data=f'admin_progress_select_{user_id}_{family_idx}_{page}')]
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

# Возврат в профиль пользователя
async def admin_back_to_profile_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Проверяем, нужно ли возвращаться к результатам поиска
    if 'from_search' in query.data:
        from tgteacher_bot.handlers.admin.admin_users_search_id import send_search_id_page
        # Просто повторно вызываем send_search_id_page через edit_message_text
        await send_search_id_page(update, context, use_edit=True)
        return
    # MCP: Проверяем, нужно ли возвращаться к результатам поиска НОВЫХ
    if context.user_data.get('from_new_search'):
        from tgteacher_bot.handlers.admin.admin_new_users_search_id import send_new_search_id_page
        await send_new_search_id_page(update, context, use_edit=True)
        return

    if query.data.startswith('admin_back_to_profile_inactive_'):
        # MCP: Возврат к списку неактивных пользователей
        data = query.data.replace('admin_back_to_profile_inactive_', '')
        parts = data.split('_')
        user_id = int(parts[0])
        period = parts[1]
        page = int(parts[2])
        from tgteacher_bot.handlers.admin.admin_inactive_users import show_inactive_users_for_period
        await show_inactive_users_for_period(update, context, period, edit_message=query.message)
        return

    data = query.data.replace('admin_back_to_profile_', '')
    parts = data.split('_')
    user_id = int(parts[0])

    # MCP: Гибкий парсинг для кастомного периода
    if len(parts) > 1:
        # Если последний элемент - число, это номер страницы
        if parts[-1].isdigit():
            page = int(parts[-1])
            period = '_'.join(parts[1:-1])
        else:
            # Иначе, это просто user_id
            page = None
            period = None
    else:
        period = None
        page = None
        
    from tgteacher_bot.handlers.admin.admin_users import get_admin_user_profile
    if period and page is not None:
        text, keyboard = await get_admin_user_profile(user_id, context, period, page)
    else:
        text, keyboard = await get_admin_user_profile(user_id, context)
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

def register_admin_user_progress_handlers(application):
    application.add_handler(CallbackQueryHandler(admin_family_progress_submenu, pattern=r'^admin_progress_select_'))
    application.add_handler(CallbackQueryHandler(admin_stage_details_view, pattern=r'^admin_progress_stage_'))
    # MCP: Добавляю обработчик для возврата в профиль из поиска
    application.add_handler(CallbackQueryHandler(admin_back_to_profile_callback, pattern=r'^admin_back_to_profile_from_search_'))
    # MCP: Добавляю обработчик для возврата в профиль из поиска НОВЫХ
    application.add_handler(CallbackQueryHandler(admin_back_to_profile_callback, pattern=r'^admin_new_search_back_to_results$'))
    # Общий обработчик для возврата в профиль, чтобы он ловил и кастомные периоды
    application.add_handler(CallbackQueryHandler(admin_back_to_profile_callback, pattern=r'^admin_back_to_profile_'))
    # MCP: Добавляю обработчик для возврата в профиль неактивных
    application.add_handler(CallbackQueryHandler(admin_back_to_profile_callback, pattern=r'^admin_back_to_profile_inactive_'))
    # MCP: Добавляю обработчик для прогресса из поиска
    application.add_handler(CallbackQueryHandler(admin_progress_callback, pattern=r'^admin_progress_from_search_'))
    # Общий обработчик для прогресса, чтобы он ловил и кастомные периоды
    application.add_handler(CallbackQueryHandler(admin_progress_callback, pattern=r'^admin_progress_')) 
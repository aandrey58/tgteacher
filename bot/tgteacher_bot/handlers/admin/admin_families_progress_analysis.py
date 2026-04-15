from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler
import tgteacher_bot.utils.families_data as families_data
from tgteacher_bot.db.user_repo import get_all_users_progress_for_family_pg, get_family_stage_answers_stats_pg, get_family_total_completion_count_pg
from tgteacher_bot.db.families_repo import get_family_data_pg, get_all_stage_tasks_counts_for_families
from collections import defaultdict

FAMILIES_PER_PAGE = 7

# MCP: Добавляем меню выбора типа пользователей для анализа прогресса
def get_admin_families_progress_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('👥 Для всех пользователей', callback_data='admin_fam_progress_all')],
        [InlineKeyboardButton('💎 Для пользователей с подпиской', callback_data='admin_fam_progress_vip')],
        [InlineKeyboardButton('👤 Для пользователей без подписки', callback_data='admin_fam_progress_free')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_users')]
    ])

def get_admin_families_progress_keyboard(page: int = 0, target_filter: str = None):
    families = families_data.ALL_FAMILIES_META
    
    # Фильтруем по target в зависимости от фильтра
    if target_filter == 'vip':
        # Для VIP пользователей: только группы с target VIP
        families = [f for f in families if f.get('target') == 'VIP']
    elif target_filter == 'free':
        # Для бесплатных пользователей: только группы с target FREE
        families = [f for f in families if f.get('target') == 'FREE']
    elif target_filter == 'all' or target_filter is None:
        # Для всех пользователей: только группы с target VIP+FREE
        families = [f for f in families if f.get('target') == 'VIP+FREE']
    
    start = page * FAMILIES_PER_PAGE
    end = start + FAMILIES_PER_PAGE
    families_page = families[start:end]
    keyboard = []
    for fam in families_page:
        # Добавляем эмодзи 💎 для VIP групп
        target = fam.get('target', 'VIP+FREE')
        if target == 'VIP':
            emoji = "💎📖"
        else:
            emoji = "📖"
        keyboard.append([
            InlineKeyboardButton(f"{emoji} {fam['name']}", callback_data=f"admin_fam_progress_{fam['id']}_{page}_{target_filter or 'all'}")
        ])
    # Пагинация
    total = len(families)
    total_pages = max(1, (total + FAMILIES_PER_PAGE - 1) // FAMILIES_PER_PAGE)
    nav_buttons = []
    # Кнопка "Назад"
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_fam_progress_page_{page-1}_{target_filter or "all"}'))
    else:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
    # Кнопка с номером страницы
    nav_buttons.append(InlineKeyboardButton(f'{page+1}/{total_pages}', callback_data='noop'))
    # Кнопка "Далее"
    if (page+1)*FAMILIES_PER_PAGE < total:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data=f'admin_fam_progress_page_{page+1}_{target_filter or "all"}'))
    else:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton('⬅️ Назад', callback_data='admin_users_progress_analysis')])
    return InlineKeyboardMarkup(keyboard)

async def admin_families_progress_analysis_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await families_data._load_initial_families_meta()  # MCP: всегда актуализируем кэш семей
    # MCP: debug-вывод только в консоль, не в Telegram
    query = update.callback_query
    await query.answer()
    text = '📈 <b>Анализ прогресса по группам слов</b>\nВыберите тип пользователей для анализа:'
    await query.edit_message_text(text, reply_markup=get_admin_families_progress_menu(), parse_mode='HTML')

# MCP: Добавляем callback'и для разных типов пользователей
async def admin_fam_progress_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Если это возврат к списку, очищаем сохранённую страницу
    context.user_data.pop('admin_fam_progress_last_page', None)
    
    # Подсчитываем количество семей для "всех"
    all_families = [f for f in families_data.ALL_FAMILIES_META if f.get('target') == 'VIP+FREE']
    title = f'👥 <b>Анализ прогресса для всех пользователей:</b>\n📊 <b>Общее количество групп:</b> {len(all_families)}'
    
    # MCP: Проверяем, не пытаемся ли мы установить то же содержимое
    try:
        await query.edit_message_text(title, reply_markup=get_admin_families_progress_keyboard(0, 'all'), parse_mode='HTML')
    except Exception as e:
        if "Message is not modified" in str(e):
            # Если содержимое не изменилось, просто отвечаем на callback
            await query.answer("Уже на странице 'Для всех пользователей'")
        else:
            # Если другая ошибка, пробрасываем дальше
            raise e

async def admin_fam_progress_vip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Если это возврат к списку, очищаем сохранённую страницу
    context.user_data.pop('admin_fam_progress_last_page', None)
    
    # Подсчитываем количество семей для VIP
    vip_families = [f for f in families_data.ALL_FAMILIES_META if f.get('target') == 'VIP']
    title = f'💎 <b>Анализ прогресса для пользователей с подпиской:</b>\n📊 <b>Общее количество групп:</b> {len(vip_families)}'
    
    # MCP: Проверяем, не пытаемся ли мы установить то же содержимое
    try:
        await query.edit_message_text(title, reply_markup=get_admin_families_progress_keyboard(0, 'vip'), parse_mode='HTML')
    except Exception as e:
        if "Message is not modified" in str(e):
            # Если содержимое не изменилось, просто отвечаем на callback
            await query.answer("Уже на странице 'Для пользователей с подпиской'")
        else:
            # Если другая ошибка, пробрасываем дальше
            raise e

async def admin_fam_progress_free_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Если это возврат к списку, очищаем сохранённую страницу
    context.user_data.pop('admin_fam_progress_last_page', None)
    
    # Подсчитываем количество семей для FREE
    free_families = [f for f in families_data.ALL_FAMILIES_META if f.get('target') == 'FREE']
    title = f'👤 <b>Анализ прогресса для пользователей без подписки:</b>\n📊 <b>Общее количество групп:</b> {len(free_families)}'
    
    # MCP: Проверяем, не пытаемся ли мы установить то же содержимое
    try:
        await query.edit_message_text(title, reply_markup=get_admin_families_progress_keyboard(0, 'free'), parse_mode='HTML')
    except Exception as e:
        if "Message is not modified" in str(e):
            # Если содержимое не изменилось, просто отвечаем на callback
            await query.answer("Уже на странице 'Для пользователей без подписки'")
        else:
            # Если другая ошибка, пробрасываем дальше
            raise e

async def admin_fam_progress_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Парсим callback_data для получения страницы и фильтра
    data = query.data
    page = 0
    target_filter = None
    
    if data.startswith('admin_fam_progress_page_'):
        try:
            parts = data.replace('admin_fam_progress_page_', '').split('_')
            page = int(parts[0])
            if len(parts) > 1:
                target_filter = parts[1] if parts[1] != 'all' else None
        except Exception:
            page = 0
            target_filter = None
    
    # Определяем заголовок в зависимости от фильтра
    if target_filter == 'vip':
        title = '💎 <b>Анализ прогресса для пользователей с подпиской:</b>'
    elif target_filter == 'free':
        title = '👤 <b>Анализ прогресса для пользователей без подписки:</b>'
    else:
        title = '👥 <b>Анализ прогресса для всех пользователей:</b>'
    
    # Добавляем счётчик количества семей
    filtered_families = families_data.ALL_FAMILIES_META
    if target_filter == 'vip':
        filtered_families = [f for f in filtered_families if f.get('target') == 'VIP']
    elif target_filter == 'free':
        filtered_families = [f for f in filtered_families if f.get('target') == 'FREE']
    else:
        filtered_families = [f for f in filtered_families if f.get('target') == 'VIP+FREE']
    
    title += f'\n📊 <b>Общее количество групп:</b> {len(filtered_families)}'
    
    await query.edit_message_text(title, reply_markup=get_admin_families_progress_keyboard(page, target_filter), parse_mode='HTML')

async def admin_fam_progress_noop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()

async def admin_fam_progress_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # ОПТИМИЗАЦИЯ: Упрощенный и более надежный парсинг callback_data
    parts = query.data.split('_')
    family_id = None
    page = 0
    target_filter = None
    
    try:
        # Ожидаемый формат: admin_fam_progress_{id}_{page}_{filter}
        if len(parts) == 6 and parts[0] == 'admin' and parts[1] == 'fam' and parts[2] == 'progress':
            family_id = int(parts[3])
            page = int(parts[4])
            target_filter = parts[5] if parts[5] != 'all' else None
        else:
            raise ValueError("Неизвестный формат callback_data")

    except (ValueError, IndexError):
        await query.edit_message_text('❌ Ошибка в данных. Не удалось определить группу слов или страницу.')
        return

    # MCP: Сохраняем текущую страницу и фильтр для возврата
    context.user_data['admin_fam_progress_last_page'] = page
    context.user_data['admin_fam_progress_target_filter'] = target_filter

    family_meta = await get_family_data_pg(family_id)
    if not family_meta:
        await query.edit_message_text('❌ Группа слов не найдена.')
        return

    # 1. Получаем все данные одним махом
    all_user_progress = await get_all_users_progress_for_family_pg(family_id)
    answers_stats = await get_family_stage_answers_stats_pg(family_id)
    total_tasks_per_stage = (await get_all_stage_tasks_counts_for_families([family_id])).get(family_id, {})
    from tgteacher_bot.db.user_repo import get_family_finished_users_pg
    finished_user_ids = set(await get_family_finished_users_pg(family_id))
    # MCP: Получаем общее количество завершений семьи
    total_completion_count = await get_family_total_completion_count_pg(family_id)
    
    total_stages_count = sum(1 for count in total_tasks_per_stage.values() if count > 0)

    # 2. Обрабатываем данные
    users_data = defaultdict(lambda: {'completed_stages': set(), 'current_stage': 0, 'total_tasks_done': 0})
    for progress in all_user_progress:
        user_id = progress['user_id']
        stage_num = progress['stage_num']
        completed_tasks = progress['completed_tasks']
        
        users_data[user_id]['total_tasks_done'] += completed_tasks
        if completed_tasks >= total_tasks_per_stage.get(stage_num, 0):
            users_data[user_id]['completed_stages'].add(stage_num)
        
        # Определяем текущий этап пользователя (максимальный, где есть прогресс)
        if stage_num > users_data[user_id]['current_stage']:
            users_data[user_id]['current_stage'] = stage_num

    # 3. Считаем статистику
    completed_users_count = 0
    finished_users_count = 0
    users_on_stage = defaultdict(int)
    in_progress_count = 0

    for user_id, data in users_data.items():
        is_fully_completed = len(data['completed_stages']) == total_stages_count
        is_finished = user_id in finished_user_ids
        if is_fully_completed:
            completed_users_count += 1
        if is_finished:
            finished_users_count += 1
        if not is_fully_completed and not is_finished:
            # Считаем, на каком этапе 'завис' пользователь
            current_stage = data['current_stage']
            if current_stage > 0:
                users_on_stage[current_stage] += 1
            in_progress_count += 1
        # Все остальные не учитываются в распределении

    # Статистика по ошибкам
    stage_errors = defaultdict(lambda: {'correct': 0, 'incorrect': 0})
    for stat in answers_stats:
        stage_num = stat['stage_num']
        count = stat['count']
        if stat['is_correct']:
            stage_errors[stage_num]['correct'] += count
        else:
            stage_errors[stage_num]['incorrect'] += count

    # Определяем присутствующие этапы и отображаемую нумерацию
    present_stages = [s for s in [1, 2, 3, 4, 5, 6, 7, 8] if total_tasks_per_stage.get(s, 0) > 0]
    stage_display_map = {s: idx + 1 for idx, s in enumerate(present_stages)}

    # 4. Формируем текст
    text_lines = [f"📊 <b>Анализ прогресса по группе слов «{family_meta['name']}»</b>\n"]
    text_lines.append(f"✅ <b>Прошли полностью:</b> {completed_users_count} чел.")
    text_lines.append(f"🏁 <b>Завершили:</b> {finished_users_count} чел.")
    text_lines.append(f"⏳ <b>В процессе:</b> {in_progress_count} чел.")
    # MCP: Добавляем общее количество завершений
    text_lines.append(f"🔄 <b>Всего прохождений:</b> {total_completion_count} раз")
    
    text_lines.append("\n<b>Распределение по этапам:</b>")
    for s in present_stages:
        count = users_on_stage.get(s, 0)
        text_lines.append(f"  - Этап {stage_display_map[s]}: {count} чел.")
            
    text_lines.append("\n<b>❌ Процент ошибок по этапам:</b>")
    printed_any_stage = False
    for s in present_stages:
        if s == 1:
            continue  # на этапе слов ошибок быть не может
        stats = stage_errors.get(s, {'correct': 0, 'incorrect': 0})
        total_answers = stats['correct'] + stats['incorrect']
        error_percent = (stats['incorrect'] / total_answers * 100) if total_answers > 0 else 0
        text_lines.append(f"  - Этап {stage_display_map[s]}: {error_percent:.1f}% ошибок ({stats['incorrect']} из {total_answers})")
        printed_any_stage = True

    # Вариант Б: если есть хотя бы один этап (2–8), показываем строки по этапам и НЕ добавляем финальную строку.
    # Добавляем "Нет данных по ответам." только если вообще нет тестовых этапов для показа.
    if not printed_any_stage:
        text_lines.append("  - Нет данных по ответам.")

    text = "\n".join(text_lines)
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('📥 Выгрузить в Excel', callback_data=f'admin_fam_progress_export_{family_id}_{page}_{target_filter or "all"}')],
        [InlineKeyboardButton('❓ Справка', callback_data=f'admin_fam_progress_help_{family_id}_{page}_{target_filter or "all"}')],
        [InlineKeyboardButton('⬅️ Назад к списку семей', callback_data=f'admin_fam_progress_page_{page}_{target_filter or "all"}')]
    ])

    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

async def admin_fam_progress_export_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        # Формат: admin_fam_progress_export_{family_id}_{page}_{filter}
        parts = query.data.split('_')
        family_id = int(parts[4])
        page = int(parts[5])
        target_filter = parts[6] if len(parts) > 6 else 'all'
    except (ValueError, IndexError):
        await query.edit_message_text('❌ Ошибка: не удалось определить группу слов.')
        return
    from tgteacher_bot.services.exports.excel_export import export_family_progress_to_excel
    import os
    import io
    file_path = await export_family_progress_to_excel(family_id)
    filename = os.path.basename(file_path)
    with open(file_path, 'rb') as f:
        file_bytes = f.read()
    # try:
    #     os.remove(file_path)
    # except Exception:
    #     pass
    await query.message.reply_document(
        document=io.BytesIO(file_bytes),
        filename=filename,
        caption='Выгрузка прогресса по группе слов в Excel',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('✅ Спасибо', callback_data='admin_fam_export_thanks')]
        ])
    )

async def admin_fam_export_thanks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass

# ====== Справка по отчёту ======
async def admin_fam_progress_help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Кнопка назад возвращает к тому же отчёту
    try:
        parts = query.data.split('_')
        family_id = int(parts[4])
        page = int(parts[5])
        target_filter = parts[6] if len(parts) > 6 else 'all'
    except Exception:
        family_id = 0
        page = 0
        target_filter = 'all'
    
    # Делаем справку динамической: показываем только этапы, которые есть у выбранной группы слов
    total_tasks_per_stage = (await get_all_stage_tasks_counts_for_families([family_id])).get(family_id, {}) if family_id else {}
    present_stages = [s for s in [1,2,3,4,5,6,7,8] if total_tasks_per_stage.get(s, 0) > 0]
    stage_display_map = {s: idx + 1 for idx, s in enumerate(present_stages)}

    # Заголовки этапов
    stage_titles = {
        1: 'Слова',
        2: 'Задания',
        3: 'Определения',
        4: 'Задания-2',
        5: 'Картинки',
        6: 'Синонимы',
        7: 'Аудио',
        8: 'Текст',
    }

    # Краткие описания без ссылок на номера этапов
    stage_details = {
        1: 'ознакомление со словами, ошибок быть не может',
        2: 'выбор правильного варианта ответа',
        3: 'выбор слов к определению',
        4: 'дополнительные задания (логика как при выборе варианта ответа)',
        5: 'ввод слова по картинке',
        6: 'выбор синонимов к словам',
        7: 'прослушивание и выбор слова',
        8: 'работа с текстом',
    }

    help_lines = [
        '<b>❓ Справка по анализу прогресса</b>\n\n',
        '• <b>✅ Прошли полностью</b> — количество пользователей, которые выполнили все задания на всех этапах группы слов (100% прогресса).\n',
        '• <b>🏁 Завершили</b> — количество пользователей, которые нажали кнопку "Завершить" (даже если не все задания выполнены).\n',
        '• <b>⏳ В процессе</b> — количество пользователей, которые ещё не завершили все этапы и не нажали "Завершить".\n',
        '• <b>🔄 Всего прохождений</b> — общее количество раз, когда группа слов была завершена всеми пользователями (учитываются повторные прохождения).\n\n',
        '• <b>Распределение по этапам</b> — показывает, на каком этапе находятся пользователи, которые ещё не завершили прохождение и не нажали "Завершить".\n',
        '  Например: <i>Этап 2: 3 чел.</i> — это значит, что три пользователя сейчас находятся на втором этапе.\n\n',
        '• <b>❌ Процент ошибок по этапам</b> — доля неправильных ответов на задания этапа.\n',
        '  <i>20.0% ошибок (2 из 10)</i> — это значит, что из 10 попыток на данном этапе у всех пользователей вместе 2 были с ошибкой, то есть 20% неверных ответов.\n',
        '  Все значения считаются по всем попыткам всех пользователей на этапе. Проценты отражают долю ошибок.\n\n',
    ]

    # Добавляем только существующие этапы с последовательной нумерацией
    for s in present_stages:
        disp = stage_display_map[s]
        title = stage_titles[s]
        detail = stage_details[s]
        help_lines.append(f'— <b>Этап {disp}</b>: {title} ({detail}).\n')

    help_lines.append('\nЗначения в скобках (<b>ошибок из всех попыток</b>) показывают абсолютное количество ошибок и общее число попыток на этапе. Проценты отражают долю ошибок.')

    help_text = ''.join(help_lines)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('⬅️ Назад к отчёту', callback_data=f'admin_fam_progress_{family_id}_{page}_{target_filter}')]
    ])
    await query.edit_message_text(help_text, reply_markup=keyboard, parse_mode='HTML')



def register_admin_families_progress_analysis_handlers(application):
    application.add_handler(CallbackQueryHandler(admin_families_progress_analysis_callback, pattern='^admin_users_progress_analysis$'))
    application.add_handler(CallbackQueryHandler(admin_fam_progress_all_callback, pattern='^admin_fam_progress_all$'))
    application.add_handler(CallbackQueryHandler(admin_fam_progress_vip_callback, pattern='^admin_fam_progress_vip$'))
    application.add_handler(CallbackQueryHandler(admin_fam_progress_free_callback, pattern='^admin_fam_progress_free$'))
    application.add_handler(CallbackQueryHandler(admin_fam_progress_page_callback, pattern=r'^admin_fam_progress_page_\d+_\w+$'))
    application.add_handler(CallbackQueryHandler(admin_fam_progress_noop_callback, pattern='^noop$'))
    application.add_handler(CallbackQueryHandler(admin_fam_progress_callback, pattern=r'^admin_fam_progress_\d+_\d+_\w+$'))
    application.add_handler(CallbackQueryHandler(admin_fam_progress_export_callback, pattern=r'^admin_fam_progress_export_\d+_\d+_\w+$'))
    application.add_handler(CallbackQueryHandler(admin_fam_progress_help_callback, pattern=r'^admin_fam_progress_help_\d+_\d+_\w+$'))
    application.add_handler(CallbackQueryHandler(admin_fam_export_thanks_callback, pattern='^admin_fam_export_thanks$')) 
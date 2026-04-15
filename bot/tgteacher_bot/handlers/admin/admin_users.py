from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile
from telegram.ext import ContextTypes, CallbackQueryHandler
from tgteacher_bot.db.user_repo import get_pool
from tgteacher_bot.services.exports.excel_export import export_all_users_to_excel
from datetime import datetime, timedelta
import asyncio
from telegram.ext import MessageHandler, filters
from tgteacher_bot.handlers.admin.admin_families_progress_analysis import register_admin_families_progress_analysis_handlers
import os
import io

USERS_PER_PAGE = 7
AWAITING_PAGE_INPUT = 'awaiting_page_input'
AWAITING_CUSTOM_PERIOD_START = 'awaiting_custom_period_start'
AWAITING_CUSTOM_PERIOD_END = 'awaiting_custom_period_end'
# MCP: Импортируем новое состояние из модуля поиска новых юзеров
from tgteacher_bot.handlers.admin.admin_new_users_search_id import AWAITING_NEW_USER_ID_INPUT

# MCP: Функция для форматирования относительного времени
def format_relative_time(dt_object):
    if not dt_object:
        return ""
    
    if dt_object.tzinfo:
        now = datetime.now(dt_object.tzinfo)
    else:
        now = datetime.now()

    diff = now - dt_object

    if diff.total_seconds() < 0:
        return ""

    parts = []

    years = diff.days // 365
    if years > 0:
        parts.append(f"{years} г.")
    
    remaining_days_after_years = diff.days % 365
    months = remaining_days_after_years // 30 # Приближенно, считаем 30 дней в месяце
    if months > 0:
        parts.append(f"{months} мес.")
    
    remaining_days_after_months = remaining_days_after_years % 30
    days = remaining_days_after_months
    if days > 0:
        parts.append(f"{days} дн.")
    
    if not parts:
        seconds = diff.total_seconds()
        if seconds < 60:
            return "(только что)"
        elif seconds < 3600: # меньше 1 часа
            minutes = int(seconds / 60)
            return f"({minutes} мин. назад)"
        else: # меньше 24 часов
            hours = int(seconds / 3600)
            return f"({hours} ч. назад)"
    
    # MCP: Изменяем способ склейки частей для корректного отображения "и"
    if len(parts) == 1:
        return f"({parts[0]} назад)"
    elif len(parts) == 2:
        return f"({parts[0]} и {parts[1]} назад)"
    else:
        return f"({', '.join(parts[:-1])} и {parts[-1]} назад)"

# MCP: Функция для расчета оставшегося времени подписки
def format_subscription_time_left(subscription_until):
    if not subscription_until:
        return ""
    
    # MCP: Учитываем московское время (UTC+3)
    from datetime import timezone
    moscow_tz = timezone(timedelta(hours=3))
    now = datetime.now(moscow_tz)
    
    # subscription_until уже имеет timezone (из базы данных TIMESTAMPTZ)
    # Конвертируем в московское время для сравнения
    if subscription_until.tzinfo:
        # Если есть timezone, конвертируем в московское время
        subscription_until_moscow = subscription_until.astimezone(moscow_tz)
    else:
        # Если нет timezone, считаем что это московское время
        subscription_until_moscow = subscription_until.replace(tzinfo=moscow_tz)
    
    diff = subscription_until_moscow - now
    
    if diff.total_seconds() <= 0:
        return "(истекла)"
    
    days = diff.days
    hours = int(diff.seconds // 3600)
    
    if days > 0:
        return f"({days} дн. осталось)"
    elif hours > 0:
        return f"({hours} ч. осталось)"
    else:
        return "(менее часа)"


def get_admin_users_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('👥 Все пользователи', callback_data='admin_users_all')],
        [InlineKeyboardButton('🆕 Новые пользователи', callback_data='admin_new_users_menu')],
        [InlineKeyboardButton('📈 Анализ прогресса', callback_data='admin_users_progress_analysis')],
        [InlineKeyboardButton('📊 Аналитика посещения', callback_data='admin_inactive_users_menu')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_panel')],
    ])

# MCP: Подменю для новых пользователей
NEW_USERS_PERIODS = [
    ('📅 Сегодня', 'today'),
    ('🗓️ За неделю', 'week'),
    ('📆 За месяц', 'month'),
]

def get_admin_new_users_menu():
    keyboard = [
        [InlineKeyboardButton(f'{label}', callback_data=f'admin_new_users_{period}')]
        for label, period in NEW_USERS_PERIODS
    ]
    keyboard.append([InlineKeyboardButton('📊 Выбрать период', callback_data='admin_new_users_custom_period')])
    keyboard.append([InlineKeyboardButton('⬅️ Назад', callback_data='admin_users')])
    return InlineKeyboardMarkup(keyboard)

def get_period_label(period):
    for label, code in NEW_USERS_PERIODS:
        if code == period:
            return label
    if period.startswith('custom_'):
        try:
            _, start_str, end_str = period.split('_')
            start = datetime.strptime(start_str, '%Y-%m-%d')
            end = datetime.strptime(end_str, '%Y-%m-%d')
            return f"с {start.strftime('%d.%m.%Y')} по {end.strftime('%d.%m.%Y')}"
        except (ValueError, IndexError):
            pass
    return period  # если вдруг не найдено

async def get_users_info_bulk(user_ids: list):
    """Получает информацию о нескольких пользователях одним запросом."""
    if not user_ids:
        return {}
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM users WHERE user_id = ANY($1::bigint[])', user_ids)
        return {row['user_id']: row for row in rows}

async def get_users_list_keyboard(user_ids, page, total):
    keyboard = []
    users_info = await get_users_info_bulk(user_ids)
    for uid in user_ids:
        user = users_info.get(uid)
        emoji = '💎' if user and user['is_subscribed'] else '👤'
        first_name = user['first_name'] if user and user['first_name'] else '-'
        keyboard.append([
            InlineKeyboardButton(f'{emoji} {first_name} ({uid})', callback_data=f'admin_user_{uid}')
        ])
    # Пагинация
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    nav_buttons = []
    # Кнопка "Назад"
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_users_page_{page-1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
    # Кнопка с номером страницы
    nav_buttons.append(InlineKeyboardButton(f'{page+1}/{total_pages}', callback_data='noop'))
    # Кнопка "Далее"
    if (page+1)*USERS_PER_PAGE < total:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data=f'admin_users_page_{page+1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    keyboard.append(nav_buttons)
    # MCP: Добавляю две кнопки над "⬅️ Управление пользователями"
    keyboard.append([InlineKeyboardButton('🔍 Поиск по ID', callback_data='admin_users_search_id')])
    keyboard.append([InlineKeyboardButton('🔎➡️ Перейти на страницу', callback_data='admin_go_to_page')])
    keyboard.append([InlineKeyboardButton('📥 Выгрузить всех в Excel', callback_data='admin_users_export_excel')])
    keyboard.append([InlineKeyboardButton('⬅️ Управление пользователями', callback_data='admin_users')])
    return InlineKeyboardMarkup(keyboard)

async def admin_users_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Чистим всё неактуальное при возврате в меню пользователей
    clear_admin_search_context(context.user_data)
    clear_admin_pagination_context(context.user_data)
    clear_admin_filter_context(context.user_data)
    clear_admin_from_search(context.user_data)
    clear_admin_last_page(context.user_data)
    await query.edit_message_text('👤 Управление пользователями:', reply_markup=get_admin_users_menu())

async def admin_new_users_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Чистим только фильтры и пагинацию при возврате к меню новых пользователей
    clear_admin_filter_context(context.user_data)
    clear_admin_pagination_context(context.user_data)
    await query.edit_message_text('🆕 Новые пользователи — выбери период:', reply_markup=get_admin_new_users_menu())

async def get_all_user_ids():
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT user_id FROM users ORDER BY registered_at DESC')
        return [row['user_id'] for row in rows]

async def get_users_stats():
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = await conn.fetchval('SELECT COUNT(*) FROM users')
        subs = await conn.fetchval('SELECT COUNT(*) FROM users WHERE is_subscribed = TRUE')
    percent = round((subs / total * 100), 1) if total else 0
    return total, subs, percent

async def admin_users_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_ids = await get_all_user_ids()
    page = 0
    context.user_data['admin_users_last_page'] = page  # Сохраняем страницу
    total = len(user_ids)
    page_ids = user_ids[page*USERS_PER_PAGE:(page+1)*USERS_PER_PAGE]
    total_users, subs_users, subs_percent = await get_users_stats()
    text = (
        f"👥 <b>Все пользователи</b>\n\n"
        f"👨‍👩‍👧‍👦 Всего пользователей: <b>{total_users}</b>\n"
        f"💎 С подпиской: <b>{subs_users}</b> (<b>{subs_percent}%</b>)\n"
    )
    await query.edit_message_text(text, reply_markup=await get_users_list_keyboard(page_ids, page, total), parse_mode='HTML')

async def admin_users_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_ids = await get_all_user_ids()
    try:
        page = int(query.data.replace('admin_users_page_', ''))
    except Exception:
        page = 0
    context.user_data['admin_users_last_page'] = page  # Сохраняем страницу
    total = len(user_ids)
    page_ids = user_ids[page*USERS_PER_PAGE:(page+1)*USERS_PER_PAGE]
    total_users, subs_users, subs_percent = await get_users_stats()
    text = (
        f"👥 <b>Все пользователи</b>\n\n"
        f"👨‍👩‍👧‍👦 Всего пользователей: <b>{total_users}</b>\n"
        f"💎 С подпиской: <b>{subs_users}</b> (<b>{subs_percent}%</b>)\n"
    )
    await query.edit_message_text(text, reply_markup=await get_users_list_keyboard(page_ids, page, total), parse_mode='HTML')

async def get_user_info(user_id):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM users WHERE user_id = $1', user_id)
        return row

async def get_admin_user_profile(user_id, context, period=None, page=None):
    user = await get_user_info(user_id)
    if not user:
        return 'Пользователь не найден.', get_admin_users_menu()
    
    registered_at = user['registered_at'] + timedelta(hours=3) if user['registered_at'] else None
    registered_at_str = registered_at.strftime('%Y-%m-%d %H:%M:%S') if registered_at else '-'
    registered_at_relative = format_relative_time(user['registered_at'])

    last_active_at = user.get('last_active_at')
    last_active_at_str = last_active_at.strftime('%Y-%m-%d %H:%M:%S') if last_active_at else '-'
    last_active_at_relative = format_relative_time(user.get('last_active_at'))

    username = f"@{user['username']}" if user['username'] else '-'
    user_id_code = f"<code>{user['user_id']}</code>"

    # MCP: Добавляем информацию о дате окончания подписки
    subscription_until = user.get('subscription_until')
    if subscription_until:
        # MCP: Конвертируем в московское время для отображения
        from datetime import timezone
        moscow_tz = timezone(timedelta(hours=3))
        if subscription_until.tzinfo:
            subscription_until_moscow = subscription_until.astimezone(moscow_tz)
        else:
            subscription_until_moscow = subscription_until.replace(tzinfo=moscow_tz)
        subscription_until_str = subscription_until_moscow.strftime('%d.%m.%Y %H:%M')
    else:
        subscription_until_str = '-'

    text = (
        f"<b>🆔 Telegram ID:</b> {user_id_code}\n"
        f"<b>👤 Username:</b> {username}\n"
        f"<b>📝 Имя:</b> {user['first_name'] or '-'}\n"
        f"<b>📝 Фамилия:</b> {user['last_name'] or '-'}\n"
        f"<b>📅 Дата регистрации:</b> {registered_at_str} {registered_at_relative}\n"
        f"<b>💎 Подписка:</b> {'✅ Да' if user['is_subscribed'] else '❌ Нет'}\n"
        f"<b>📅 Активна до:</b> {subscription_until_str} {format_subscription_time_left(user.get('subscription_until'))}\n"
        f"<b>🔁 Кол-во подписок:</b> {user['subscription_count']}\n"
        f"<b>🕒 Последнее взаимодействие:</b> {last_active_at_str} {last_active_at_relative}\n"
    )
    last_page = context.user_data.get('admin_users_last_page', 0)
    if period is not None and page is not None:
        # MCP: приоритет по флагам
        if context.user_data.get('from_inactive_users'):
            back_button_cb = f'admin_inactive_users_page_{period}_{page}'
        elif context.user_data.get('from_new_users'):
            back_button_cb = f'admin_new_users_page_{period}_{page}'
        elif period in ['week', '3weeks', 'month']:
            back_button_cb = f'admin_inactive_users_page_{period}_{page}'
        else:
            back_button_cb = f'admin_new_users_page_{period}_{page}'
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('✍️ Написать пользователю', callback_data=f'admin_private_message_{user_id}')],
            [
                InlineKeyboardButton('🎁 Подарить подписку', callback_data=f'admin_gift_subscription_{user_id}'),
                InlineKeyboardButton('❌ Удалить подписку', callback_data=f'admin_remove_subscription_{user_id}')
            ],
            [InlineKeyboardButton('📈 Прогресс', callback_data=f'admin_progress_{user_id}_1_{period}_{page}')],
            [InlineKeyboardButton('🗑️ Удалить', callback_data=f'admin_delete_user_confirm_{user_id}')],
            [InlineKeyboardButton('⬅️ К периоду', callback_data=back_button_cb)]
        ])
        # MCP: чистим флаги после показа профиля
        context.user_data.pop('from_inactive_users', None)
        context.user_data.pop('from_new_users', None)
    else:
        # MCP: Добавляем period и page в callback_data, если они есть в контексте
        period = context.user_data.get('admin_period')
        page = context.user_data.get('admin_period_page')
        
        back_button_cb = f'admin_users_page_{last_page}'
        progress_button_cb = f'admin_progress_{user_id}_1'
        
        if period and page is not None:
            back_button_cb = f'admin_new_users_page_{period}_{page}'
            progress_button_cb = f'admin_progress_{user_id}_1_{period}_{page}'
        
        # MCP: Если пришли из поиска по всем пользователям — возвращаемся к результатам поиска
        if context.user_data.get('from_search'):
            back_button_cb = 'admin_search_id_back_to_results'
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('✍️ Написать пользователю', callback_data=f'admin_private_message_{user_id}')],
            [
                InlineKeyboardButton('🎁 Подарить подписку', callback_data=f'admin_gift_subscription_{user_id}'),
                InlineKeyboardButton('❌ Удалить подписку', callback_data=f'admin_remove_subscription_{user_id}')
            ],
            [InlineKeyboardButton('📈 Прогресс', callback_data=progress_button_cb)],
            [InlineKeyboardButton('🗑️ Удалить', callback_data=f'admin_delete_user_confirm_{user_id}')],
            [InlineKeyboardButton('⬅️ Назад', callback_data=back_button_cb)]
        ])
    return text, keyboard

async def admin_user_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Чистим флаг from_search, так как мы пришли не из поиска
    clear_admin_from_search(context.user_data)
    
    # MCP: Правильное парсинг callback_data
    data = query.data.replace('admin_user_', '')
    parts = data.split('_')
    
    user_id = int(parts[-1])
    period = None
    page = None

    # Если есть period и page в callback_data
    if len(parts) >= 3: # Ожидаем admin_user_{period}_{page}_{user_id} или просто admin_user_{user_id}
        try:
            # Пытаемся распарсить как admin_user_{period}_{page}_{user_id}
            # period может содержать _ (например, custom_2023-01-01_2023-01-31)
            # Поэтому берем последние два элемента как page и user_id
            # А все остальное до них как period
            user_id = int(parts[-1])
            page = int(parts[-2])
            period = '_'.join(parts[:-2]) # Объединяем все остальное как period
            
            context.user_data['admin_period'] = period
            context.user_data['admin_period_page'] = page
        except (ValueError, IndexError):
            # Если не получается, значит это просто admin_user_{user_id}
            user_id = int(parts[0]) if parts else int(data) # На случай, если data - это только user_id
            period = None
            page = None
    else: # Старый формат, когда data - это только user_id
        user_id = int(data)

    text, keyboard = await get_admin_user_profile(user_id, context, period, page)
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

async def admin_users_noop_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()

async def admin_users_export_excel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    file_path = await export_all_users_to_excel()
    filename = os.path.basename(file_path)
    with open(file_path, 'rb') as f:
        file_bytes = f.read()
    # try:
    #     os.remove(file_path)
    # except Exception as e:
    #     print(f'MCP DEBUG: Не удалось удалить файл {file_path}: {e}')
    await query.message.reply_document(
        document=InputFile(io.BytesIO(file_bytes), filename=filename),
        caption='Выгрузка всех пользователей в Excel',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('✅ Спасибо', callback_data='admin_users_export_thanks')]
        ])
    )

async def admin_users_export_thanks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass

# MCP: Получение новых пользователей за период
from tgteacher_bot.services.exports.excel_export import get_period_dates

async def get_new_user_ids(period):
    start, end = get_period_dates(period)
    if start is None or end is None:
        return []
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT user_id FROM users WHERE registered_at >= $1 AND registered_at <= $2 ORDER BY registered_at DESC', start, end)
        return [row['user_id'] for row in rows]

async def admin_new_users_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    period = query.data.replace('admin_new_users_', '')
    await show_new_users_for_period(update, context, period, edit_message=query.message)

async def show_new_users_for_period(update, context, period, edit_message=None, send_chat=None):
    context.user_data['admin_period'] = period
    page = 0
    user_ids = await get_new_user_ids(period)
    total = len(user_ids)
    page_ids = user_ids[page*USERS_PER_PAGE:(page+1)*USERS_PER_PAGE]
    text = f"🆕 Новые пользователи за период: <b>\n{get_period_label(period)}</b>\n\nВсего: <b>{total}</b>\n"
    keyboard = await get_new_users_list_keyboard(page_ids, page, total, period)
    if edit_message:
        await edit_message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    elif send_chat:
        await send_chat.send_message(text, reply_markup=keyboard, parse_mode='HTML')

async def get_new_users_list_keyboard(user_ids, page, total, period):
    keyboard = []
    users_info = await get_users_info_bulk(user_ids)
    for uid in user_ids:
        user = users_info.get(uid)
        emoji = '💎' if user and user['is_subscribed'] else '👤'
        first_name = user['first_name'] if user and user['first_name'] else '-'
        keyboard.append([
            InlineKeyboardButton(f'{emoji} {first_name} ({uid})', callback_data=f'admin_new_userinfo_{period}_{page}_{uid}')
        ])
    # Пагинация
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_new_users_page_{period}_{page-1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
    nav_buttons.append(InlineKeyboardButton(f'{page+1}/{total_pages}', callback_data='noop'))
    if (page+1)*USERS_PER_PAGE < total:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data=f'admin_new_users_page_{period}_{page+1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    keyboard.append(nav_buttons)
    # MCP: Передаем period в колбек поиска по ID
    keyboard.append([InlineKeyboardButton('🔍 Поиск по ID', callback_data=f'admin_new_users_search_id_{period}')])
    keyboard.append([InlineKeyboardButton('📥 Выгрузить в Excel', callback_data='admin_new_users_export_excel')])
    keyboard.append([InlineKeyboardButton('⬅️ К периодам', callback_data='admin_new_users_menu')])
    return InlineKeyboardMarkup(keyboard)

async def admin_new_user_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.replace('admin_new_userinfo_', '')
    
    # MCP: Определяем, пришли ли мы из поиска, и парсим данные корректно
    from_search = data.endswith('_from_search')
    
    if from_search:
        # Убираем суффикс, чтобы парсить чисто
        clean_data = data.removesuffix('_from_search')
        parts = clean_data.split('_')
        context.user_data['from_new_search'] = True # Флаг для возврата
    else:
        parts = data.split('_')
        context.user_data.pop('from_new_search', None) # Чистим флаг

    user_id = int(parts[-1])
    page = int(parts[-2])
    period = '_'.join(parts[:-2])

    # MCP: Ставим флаг, что мы пришли из новых пользователей
    context.user_data['from_new_users'] = True
    context.user_data['admin_period'] = period
    context.user_data['admin_period_page'] = page
    # MCP: Сохраняем период для поиска, чтобы вернуться если что
    context.user_data['admin_period_for_search'] = period

    user = await get_user_info(user_id)
    if not user:
        await query.edit_message_text('Пользователь не найден.', reply_markup=get_admin_new_users_menu())
        return
    registered_at = user['registered_at'] + timedelta(hours=3) if user['registered_at'] else None
    registered_at_str = registered_at.strftime('%Y-%m-%d %H:%M:%S') if registered_at else '-'
    registered_at_relative = format_relative_time(user['registered_at'])
    last_active_at = user.get('last_active_at')
    last_active_at_str = last_active_at.strftime('%Y-%m-%d %H:%M:%S') if last_active_at else '-'
    last_active_at_relative = format_relative_time(user.get('last_active_at'))
    username = f"@{user['username']}" if user['username'] else '-'
    user_id_code = f"<code>{user['user_id']}</code>"
    
    # MCP: Добавляем информацию о дате окончания подписки
    subscription_until = user.get('subscription_until')
    if subscription_until:
        # MCP: Конвертируем в московское время для отображения
        from datetime import timezone
        moscow_tz = timezone(timedelta(hours=3))
        if subscription_until.tzinfo:
            subscription_until_moscow = subscription_until.astimezone(moscow_tz)
        else:
            subscription_until_moscow = subscription_until.replace(tzinfo=moscow_tz)
        subscription_until_str = subscription_until_moscow.strftime('%d.%m.%Y %H:%M')
    else:
        subscription_until_str = '-'
    
    text = (
        f"<b>🆔 Telegram ID:</b> {user_id_code}\n"
        f"<b>👤 Username:</b> {username}\n"
        f"<b>📝 Имя:</b> {user['first_name'] or '-'}\n"
        f"<b>📝 Фамилия:</b> {user['last_name'] or '-'}\n"
        f"<b>📅 Дата регистрации:</b> {registered_at_str} {registered_at_relative}\n"
        f"<b>💎 Подписка:</b> {'✅ Да' if user['is_subscribed'] else '❌ Нет'}\n"
        f"<b>📅 Активна до:</b> {subscription_until_str} {format_subscription_time_left(user.get('subscription_until'))}\n"
        f"<b>🔁 Кол-во подписок:</b> {user['subscription_count']}\n"
        f"<b>🕒 Последнее взаимодействие:</b> {last_active_at_str} {last_active_at_relative}\n"
    )

    # MCP: Адаптируем кнопку "Назад" для возврата к результатам поиска
    if from_search:
        back_button_cb = 'admin_new_search_back_to_results'
    else:
        back_button_cb = f'admin_new_users_page_{period}_{page}'

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✍️ Написать пользователю', callback_data=f'admin_private_message_{user_id}')],
        [
            InlineKeyboardButton('🎁 Подарить подписку', callback_data=f'admin_gift_subscription_{user_id}'),
            InlineKeyboardButton('❌ Удалить подписку', callback_data=f'admin_remove_subscription_{user_id}')
        ],
        [InlineKeyboardButton('📈 Прогресс', callback_data=f'admin_progress_{user_id}_1_{period}_{page}')],
        [InlineKeyboardButton('🗑️ Удалить', callback_data=f'admin_delete_user_confirm_{user_id}')],
        [InlineKeyboardButton('⬅️ Назад', callback_data=back_button_cb)]
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

async def admin_new_users_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.replace('admin_new_users_page_', '')
    
    # MCP: Исправляю парсинг, чтобы он корректно работал с кастомными периодами
    try:
        period, page_str = data.rsplit('_', 1)
        page = int(page_str)
    except (ValueError, IndexError):
        # Fallback на случай, если что-то пойдёт не так
        await query.edit_message_text("❌ Ошибка навигации. Попробуйте начать заново.")
        return

    context.user_data['admin_period'] = period
    user_ids = await get_new_user_ids(period)
    total = len(user_ids)
    page_ids = user_ids[page*USERS_PER_PAGE:(page+1)*USERS_PER_PAGE]
    text = f"🆕 Новые пользователи за период:<b>\n{get_period_label(period)}</b>\n\nВсего: <b>{total}</b>\n"
    await query.edit_message_text(text, reply_markup=await get_new_users_list_keyboard(page_ids, page, total, period), parse_mode='HTML')

# MCP: Удаляем старую заглушку
# async def admin_new_users_search_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     await update.callback_query.answer('Поиск по ID (ещё не реализовано)', show_alert=True)
#     # MCP: Чистим только поиск
#     clear_admin_search_context(context.user_data)
#     clear_admin_last_page(context.user_data)

async def admin_new_users_export_excel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    period = context.user_data.get('admin_period', 'today')
    from tgteacher_bot.services.exports.excel_export import export_new_users_to_excel
    file_path = await export_new_users_to_excel(period)
    filename = os.path.basename(file_path)
    with open(file_path, 'rb') as f:
        file_bytes = f.read()
    # try:
    #     os.remove(file_path)
    # except Exception as e:
    #     print(f'MCP DEBUG: Не удалось удалить файл {file_path}: {e}')
    await query.message.reply_document(
        document=InputFile(io.BytesIO(file_bytes), filename=filename),
        caption=f'Выгрузка новых пользователей за период: {get_period_label(period)}',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('✅ Спасибо', callback_data='admin_users_export_thanks')]
        ])
    )

async def admin_users_export_thanks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass

# MCP: Подтверждение удаления пользователя
async def admin_delete_user_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = int(query.data.replace('admin_delete_user_confirm_', ''))
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да, удалить', callback_data=f'admin_delete_user_{user_id}'),
            InlineKeyboardButton('❌ Отмена', callback_data=f'admin_user_{user_id}')
        ]
    ])
    await query.edit_message_text(
        f'🗑 Ты действительно хочешь удалить пользователя <b>{user_id}</b> вместе со всем его прогрессом?\n\nЭто действие <b>НЕОБРАТИМО</b>!\n\nПодтверди удаление:',
        reply_markup=keyboard,
        parse_mode='HTML'
    )

# MCP: Удаление пользователя и прогресса
async def admin_delete_user_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = int(query.data.replace('admin_delete_user_', ''))
    from tgteacher_bot.db.user_repo import delete_user_and_progress_pg
    await delete_user_and_progress_pg(user_id)
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('⬅️ К списку пользователей', callback_data='admin_users_all')]
    ])
    await query.edit_message_text(
        f'🗑️ Пользователь <b>{user_id}</b> и все связанные с ним данные были успешно удалены.',
        reply_markup=keyboard,
        parse_mode='HTML'
    )

# MCP: Состояние ожидания ввода номера страницы
AWAITING_PAGE_INPUT = 'awaiting_page_input'

async def admin_go_to_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Сбрасываем режим поиска по ID, если он был активен
    context.user_data.pop('awaiting_id_input', None)
    context.user_data['awaiting_page_input'] = True
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data='admin_go_to_page_cancel')]
    ])
    # Сначала удаляем сообщение со списком юзеров
    try:
        await query.message.delete()
    except Exception as e:
        pass
    # Потом отправляем запрос на ввод
    sent = await query.message.chat.send_message('➡️ Введите номер страницы на которую перейти:', reply_markup=keyboard)
    context.user_data['page_input_message_id'] = sent.message_id
    context.user_data.pop('admin_users_last_page', None)

async def admin_go_to_page_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer('Переход отменён')
    # MCP: Чистим только пагинацию
    clear_admin_pagination_context(context.user_data)
    try:
        await query.message.delete()
    except Exception:
        pass
    # MCP: После отмены показываем меню всех пользователей
    user_ids = await get_all_user_ids()
    page = 0
    total = len(user_ids)
    page_ids = user_ids[page*USERS_PER_PAGE:(page+1)*USERS_PER_PAGE]
    total_users, subs_users, subs_percent = await get_users_stats()
    text = (
        f"👥 <b>Все пользователи</b>\n\n"
        f"👨‍👩‍👧‍👦 Всего пользователей: <b>{total_users}</b>\n"
        f"💎 С подпиской: <b>{subs_users}</b> (<b>{subs_percent}%</b>)\n"
    )
    await query.message.chat.send_message(
        text,
        reply_markup=await get_users_list_keyboard(page_ids, page, total),
        parse_mode='HTML'
    )
    clear_admin_last_page(context.user_data)

# MCP: Хендлер для кнопки "Выбрать период"
async def admin_new_users_custom_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data[AWAITING_CUSTOM_PERIOD_START] = True
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data='admin_new_users_custom_period_cancel')]
    ])
    try:
        await query.message.delete()
    except Exception:
        pass
    sent_message = await query.message.chat.send_message(
        '➡️ Введите <b>начальную</b> дату в формате <b>ДД.ММ.ГГГГ</b>:',
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    context.user_data['custom_period_input_message_id'] = sent_message.message_id

# MCP: Хендлер для отмены выбора периода
async def admin_new_users_custom_period_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    clear_admin_filter_context(context.user_data)
    try:
        await query.message.delete()
    except Exception:
        pass
    await query.message.chat.send_message(
        '🆕 Новые пользователи — выбери период:',
        reply_markup=get_admin_new_users_menu()
    )

# MCP: Универсальный текстовый хендлер для перехода по странице и поиска по ID
async def admin_users_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f'MCP DEBUG: admin_users_text_message вызван для текста: {update.message.text}')
    
    # MCP: Если активен режим личного сообщения — НЕ переадресуем, обработку сделает private_message хэндлер
    if context.user_data.get('waiting_for_private_message'):
        print('MCP DEBUG: waiting_for_private_message активен, обработка доверена private_message хэндлеру, выходим')
        return
    
    # MCP: Проверяем, не ждем ли мы ввод дней для подписки
    if context.user_data.get('awaiting_subscription_days'):
        print('MCP DEBUG: awaiting_subscription_days активен, обработка доверена sub_gift хэндлеру, выходим')
        return
    
    # MCP: Сначала проверяем, не ждем ли мы ввод ID для новых пользователей
    if context.user_data.get(AWAITING_NEW_USER_ID_INPUT):
        print('MCP DEBUG: AWAITING_NEW_USER_ID_INPUT активен, обрабатываем')
        from tgteacher_bot.handlers.admin.admin_new_users_search_id import admin_new_users_search_id_message
        await admin_new_users_search_id_message(update, context)
        return
    
    # MCP: Логика для кастомного периода
    if context.user_data.get(AWAITING_CUSTOM_PERIOD_START):
        print('MCP DEBUG: AWAITING_CUSTOM_PERIOD_START активен, обрабатываем')
        await handle_custom_period_start(update, context)
        return
    if context.user_data.get(AWAITING_CUSTOM_PERIOD_END):
        print('MCP DEBUG: AWAITING_CUSTOM_PERIOD_END активен, обрабатываем')
        await handle_custom_period_end(update, context)
        return
    
    # Переход на страницу
    if context.user_data.get('awaiting_page_input'):
        print('MCP DEBUG: awaiting_page_input активен, обрабатываем')
        text = update.message.text.strip()
        if not text.isdigit():
            # MCP: Заменяем текст в сообщении с запросом номера страницы
            page_input_message_id = context.user_data.get('page_input_message_id')
            if page_input_message_id:
                try:
                    chat_id = update.effective_chat.id
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=page_input_message_id,
                        text='❗️ Введите только цифры или нажмите "Отмена"',
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton('❌ Отмена', callback_data='admin_go_to_page_cancel')]
                        ])
                    )
                except Exception as e:
                    pass
            # Удаляем невалидное сообщение пользователя
            try:
                await update.message.delete()
            except Exception as e:
                pass
            return
        # MCP: обработка валидного номера страницы
        page = int(text) - 1
        user_ids = await get_all_user_ids()
        total = len(user_ids)
        total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
        if page < 0 or page >= total_pages:
            # MCP: Заменяем текст в сообщении с запросом номера страницы
            page_input_message_id = context.user_data.get('page_input_message_id')
            if page_input_message_id:
                try:
                    chat_id = update.effective_chat.id
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=page_input_message_id,
                        text=f'❗ Такой страницы нет. Введите число от 1 до {total_pages} или нажмите "Отмена"',
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton('❌ Отмена', callback_data='admin_go_to_page_cancel')]
                        ])
                    )
                except Exception as e:
                    pass
            try:
                await update.message.delete()
            except Exception as e:
                pass
            return
        page_ids = user_ids[page*USERS_PER_PAGE:(page+1)*USERS_PER_PAGE]
        total_users, subs_users, subs_percent = await get_users_stats()
        text_out = (
            f"👥 <b>Все пользователи</b>\n\n"
            f"👨‍👩‍👧‍👦 Всего пользователей: <b>{total_users}</b>\n"
            f"💎 С подпиской: <b>{subs_users}</b> (<b>{subs_percent}%</b>)\n"
        )
        await update.message.reply_text(
            text_out,
            reply_markup=await get_users_list_keyboard(page_ids, page, total),
            parse_mode='HTML'
        )
        context.user_data.pop('awaiting_page_input', None)
        # MCP: Удаляем сообщение с запросом номера страницы
        page_input_message_id = context.user_data.pop('page_input_message_id', None)
        if page_input_message_id:
            try:
                chat_id = update.effective_chat.id
                await context.bot.delete_message(chat_id, page_input_message_id)
            except Exception as e:
                pass
        try:
            await update.message.delete()
        except Exception:
            pass
        return
    # Поиск по ID
    if context.user_data.get('awaiting_id_input'):
        print('MCP DEBUG: awaiting_id_input активен, обрабатываем')
        text = update.message.text.strip()
        if not text.isdigit():
            print('MCP DEBUG: введено не число для поиска по ID')
            await update.message.reply_text('❗ Введите только цифры или нажмите "Отмена"')
            return
        # Тут будет логика поиска по ID, пока просто эхо
        print(f'MCP DEBUG: поиск по ID: {text}')
        await update.message.reply_text(f'🔍 Запрос: {text}\n(поиск пока не реализован)')
        context.user_data.pop('awaiting_id_input', None)
        return
    print('MCP DEBUG: ни один режим не активен, текст проигнорирован в admin_users_text_message')
    
    # ВАЖНО: возвращаем None чтобы другие хэндлеры могли обработать сообщение
    print('MCP DEBUG: admin_users_text_message возвращает None')
    return None

async def handle_custom_period_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    prompt_msg_id = context.user_data.get('custom_period_input_message_id')
    chat_id = update.effective_chat.id
    cancel_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton('❌ Отмена', callback_data='admin_new_users_custom_period_cancel')]])

    try:
        await update.message.delete()
    except Exception: pass

    try:
        start_date = datetime.strptime(text, '%d.%m.%Y')
        if start_date.date() > datetime.now().date():
            if prompt_msg_id:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=prompt_msg_id,
                    text='❗️ Начальная дата не может быть в будущем. Попробуй ещё раз:',
                    reply_markup=cancel_keyboard,
                    parse_mode='HTML'
                )
            return
        context.user_data['custom_period_start'] = start_date
        context.user_data.pop(AWAITING_CUSTOM_PERIOD_START)
        context.user_data[AWAITING_CUSTOM_PERIOD_END] = True
        if prompt_msg_id:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=prompt_msg_id,
                text='✅ Начальная дата принята.\n\n➡️ Теперь введите <b>конечную</b> дату в формате <b>ДД.ММ.ГГГГ</b>:',
                reply_markup=cancel_keyboard,
                parse_mode='HTML'
            )
    except ValueError:
        if prompt_msg_id:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=prompt_msg_id,
                text='❗️ Неверный формат. Введите дату как <b>ДД.ММ.ГГГГ</b>:',
                reply_markup=cancel_keyboard,
                parse_mode='HTML'
            )

async def handle_custom_period_end(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    prompt_msg_id = context.user_data.get('custom_period_input_message_id')
    chat_id = update.effective_chat.id
    cancel_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton('❌ Отмена', callback_data='admin_new_users_custom_period_cancel')]])

    try:
        await update.message.delete()
    except Exception: pass

    try:
        end_date = datetime.strptime(text, '%d.%m.%Y')
        start_date = context.user_data.get('custom_period_start')

        if end_date.date() > datetime.now().date():
            if prompt_msg_id:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=prompt_msg_id,
                    text='❗️ Конечная дата не может быть в будущем. Попробуй ещё раз:',
                    reply_markup=cancel_keyboard,
                    parse_mode='HTML'
                )
            return

        if end_date < start_date:
            if prompt_msg_id:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=prompt_msg_id,
                    text='❗️ Конечная дата не может быть раньше начальной. Попробуй ещё раз:',
                    reply_markup=cancel_keyboard,
                    parse_mode='HTML'
                )
            return

        # Всё ок, показываем результат
        if prompt_msg_id:
            try:
                await context.bot.delete_message(chat_id, prompt_msg_id)
            except Exception: pass

        period = f"custom_{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"
        clear_admin_filter_context(context.user_data) # Чистим всё, включая стейты
        await show_new_users_for_period(update, context, period, send_chat=update.message.chat)

    except ValueError:
        if prompt_msg_id:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=prompt_msg_id,
                text='❗️ Неверный формат. Введите дату как <b>ДД.ММ.ГГГГ</b>:',
                reply_markup=cancel_keyboard,
                parse_mode='HTML'
            )

async def debug_text_handler(update, context):
    print('MCP DEBUG: debug_text_handler поймал текст:', getattr(update.message, 'text', None))
    # Безопасные проверки контекста
    if context is None or getattr(context, 'user_data', None) is None:
        return None
    # Проверяем активные режимы
    if context.user_data.get('waiting_for_broadcast'):
        print('MCP DEBUG: режим waiting_for_broadcast активен')
    elif context.user_data.get('waiting_for_admin_id'):
        print('MCP DEBUG: режим waiting_for_admin_id активен') 
    elif context.user_data.get('waiting_for_page_number'):
        print('MCP DEBUG: режим waiting_for_page_number активен')
    elif context.user_data.get('waiting_for_user_search_id'):
        print('MCP DEBUG: режим waiting_for_user_search_id активен')
    elif context.user_data.get('waiting_for_new_user_search_id'):
        print('MCP DEBUG: режим waiting_for_new_user_search_id активен')
    elif context.user_data.get('waiting_for_custom_date_start'):
        print('MCP DEBUG: режим waiting_for_custom_date_start активен')
    elif context.user_data.get('waiting_for_custom_date_end'):
        print('MCP DEBUG: режим waiting_for_custom_date_end активен')
    else:
        print('MCP DEBUG: ни один режим не активен, текст проигнорирован')
    
    # ВАЖНО: возвращаем None чтобы другие хэндлеры могли обработать сообщение
    return None

async def admin_users_progress_analysis_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from tgteacher_bot.handlers.admin.admin_families_progress_analysis import admin_families_progress_analysis_callback
    return await admin_families_progress_analysis_callback(update, context)

def register_admin_users_handlers(application):
    application.add_handler(CallbackQueryHandler(admin_users_all_callback, pattern='^admin_users_all$'))
    application.add_handler(CallbackQueryHandler(admin_users_page_callback, pattern='^admin_users_page_'))
    application.add_handler(CallbackQueryHandler(admin_user_info_callback, pattern='^admin_user_\\d+$'))
    application.add_handler(CallbackQueryHandler(admin_users_noop_callback, pattern='^noop$'))
    application.add_handler(CallbackQueryHandler(admin_users_export_excel_callback, pattern='^admin_users_export_excel$'))
    application.add_handler(CallbackQueryHandler(admin_users_export_thanks_callback, pattern='^admin_users_export_thanks$'))
    # MCP: Добавляю обработчик для анализа прогресса всех пользователей
    application.add_handler(CallbackQueryHandler(admin_users_progress_analysis_callback, pattern='^admin_users_progress_analysis$'))
    # MCP: Регистрируем обработчик анализа прогресса семей
    register_admin_families_progress_analysis_handlers(application)
    # MCP: Добавляю обработчик для меню новых пользователей
    application.add_handler(CallbackQueryHandler(admin_new_users_menu_callback, pattern='^admin_new_users_menu$'))
    # MCP: Обработчик для периодов новых пользователей
    application.add_handler(CallbackQueryHandler(admin_new_users_period_callback, pattern='^admin_new_users_(today|week|month)$'))
    application.add_handler(CallbackQueryHandler(admin_new_users_page_callback, pattern='^admin_new_users_page_'))
    # MCP: УДАЛЯЕМ СТАРУЮ ЗАГЛУШКУ
    # application.add_handler(CallbackQueryHandler(admin_new_users_search_id_callback, pattern='^admin_new_users_search_id$'))
    application.add_handler(CallbackQueryHandler(admin_new_users_export_excel_callback, pattern='^admin_new_users_export_excel$'))
    application.add_handler(CallbackQueryHandler(admin_new_user_info_callback, pattern='^admin_new_userinfo_'))
    application.add_handler(CallbackQueryHandler(admin_go_to_page_callback, pattern='^admin_go_to_page$'))
    application.add_handler(CallbackQueryHandler(admin_go_to_page_cancel_callback, pattern='^admin_go_to_page_cancel$'))
    # MCP: Добавляю обработчики для кастомного периода
    application.add_handler(CallbackQueryHandler(admin_new_users_custom_period_callback, pattern='^admin_new_users_custom_period$'))
    application.add_handler(CallbackQueryHandler(admin_new_users_custom_period_cancel_callback, pattern='^admin_new_users_custom_period_cancel$'))
    # MCP: Универсальный текстовый хендлер для перехода по странице и поиска по ID, теперь group=998 и фильтр ALL
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), admin_users_text_message), group=998)
    # Не регистрируем отдельные текстовые хендлеры для этих режимов!
    from tgteacher_bot.handlers.admin.admin_users_search_id import register_admin_users_search_id_handler
    register_admin_users_search_id_handler(application)
    # MCP: Регистрируем поиск для новых пользователей
    from tgteacher_bot.handlers.admin.admin_new_users_search_id import register_admin_new_users_search_handlers
    register_admin_new_users_search_handlers(application)
    # MCP: Глобальный дебаг-хендлер для всех сообщений
    application.add_handler(MessageHandler(filters.ALL, debug_text_handler), group=999)
    # MCP: Регистрируем админский просмотр прогресса пользователя
    from tgteacher_bot.handlers.admin.admin_user_progress import register_admin_user_progress_handlers
    register_admin_user_progress_handlers(application)
    # MCP: Регистрируем обработчик личных сообщений
    from tgteacher_bot.handlers.user.private_message import register_private_message_handler
    register_private_message_handler(application)
    # MCP: Регистрируем обработчики удаления пользователя
    application.add_handler(CallbackQueryHandler(admin_delete_user_confirm_callback, pattern='^admin_delete_user_confirm_\\d+$'))
    application.add_handler(CallbackQueryHandler(admin_delete_user_callback, pattern='^admin_delete_user_\\d+$'))
    # MCP: Регистрируем аналитику посещения
    from tgteacher_bot.handlers.admin.admin_inactive_users import register_admin_inactive_users_handlers
    register_admin_inactive_users_handlers(application)
    # MCP: Регистрируем обработчики управления подписками
    from tgteacher_bot.services.payments.sub_gift import register_sub_gift_handlers
    register_sub_gift_handlers(application)

def clear_admin_search_context(user_data):
    for k in ['search_id_results', 'search_id_page', 'search_id_instruction_message_id', 'awaiting_id_input', 'admin_period_for_search', 'new_search_results', 'new_search_page', 'AWAITING_NEW_USER_ID_INPUT']:
        user_data.pop(k, None)

def clear_admin_pagination_context(user_data):
    for k in ['awaiting_page_input', 'page_input_message_id']:
        user_data.pop(k, None)

def clear_admin_filter_context(user_data):
    for k in [
        'admin_period', 'admin_period_page',
        'AWAITING_CUSTOM_PERIOD_START', 'AWAITING_CUSTOM_PERIOD_END',
        'custom_period_start', 'custom_period_input_message_id'
    ]:
        user_data.pop(k, None)

def clear_admin_from_search(user_data):
    user_data.pop('from_search', None)
    user_data.pop('from_new_search', None)

def clear_admin_last_page(user_data):
    user_data.pop('admin_users_last_page', None)
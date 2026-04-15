from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile
from telegram.ext import ContextTypes, CallbackQueryHandler
from tgteacher_bot.db.user_repo import get_pool
from tgteacher_bot.services.exports.excel_export import export_all_users_to_excel
from datetime import datetime, timedelta
import os
import io
from tgteacher_bot.handlers.admin.admin_users import get_users_info_bulk, format_relative_time, format_subscription_time_left # MCP: Импорт format_relative_time и format_subscription_time_left

INACTIVE_PERIODS = [
    ('⏳ Больше недели', 'week'),
    ('⏳ Больше 2 недель', '2weeks'),
    ('⏳ Больше 3 недель', '3weeks'),
    ('⏳ Больше месяца', 'month'),
]
USERS_PER_PAGE = 7

def get_admin_inactive_users_menu():
    keyboard = [
        [InlineKeyboardButton(label, callback_data=f'admin_inactive_users_{code}')]
        for label, code in INACTIVE_PERIODS
    ]
    keyboard.append([InlineKeyboardButton('⬅️ Назад', callback_data='admin_users')])
    return InlineKeyboardMarkup(keyboard)

def get_inactive_period_dates(period):
    now = datetime.now()
    if period == 'week':
        end = now - timedelta(days=7)
    elif period == '2weeks':
        end = now - timedelta(days=14)
    elif period == '3weeks':
        end = now - timedelta(days=21)
    elif period == 'month':
        end = now - timedelta(days=30)
    else:
        end = now
    return end

async def get_inactive_user_ids(period):
    end = get_inactive_period_dates(period)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT user_id FROM users WHERE last_active_at < $1 ORDER BY last_active_at ASC', end)
        return [row['user_id'] for row in rows]

async def get_users_info_bulk(user_ids: list):
    if not user_ids:
        return {}
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM users WHERE user_id = ANY($1::bigint[])', user_ids)
        return {row['user_id']: row for row in rows}

async def get_inactive_users_list_keyboard(user_ids, page, total, period):
    keyboard = []
    users_info = await get_users_info_bulk(user_ids)
    for uid in user_ids:
        user = users_info.get(uid)
        if user:
            emoji = '💎' if user.get('is_subscribed') else '👤'
            first_name = user['first_name'] if user.get('first_name') else '-'
            label = f'💤{emoji} {first_name} ({uid})'
        else:
            label = f'💤👤 - ({uid})'
        keyboard.append([InlineKeyboardButton(label, callback_data=f'admin_inactive_userinfo_{period}_{page}_{uid}')])
    total_pages = max(1, (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_inactive_users_page_{period}_{page-1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
    nav_buttons.append(InlineKeyboardButton(f'{page+1}/{total_pages}', callback_data='noop'))
    if (page+1)*USERS_PER_PAGE < total:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data=f'admin_inactive_users_page_{period}_{page+1}'))
    else:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton('📥 Выгрузить в Excel', callback_data=f'admin_inactive_users_export_excel_{period}')])
    keyboard.append([InlineKeyboardButton('⬅️ К периодам', callback_data='admin_inactive_users_menu')])
    return InlineKeyboardMarkup(keyboard)

async def admin_inactive_users_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text('📊 Аналитика посещения:\nПоказывает пользователей, которые не посещали бота в течение указанного периода.', reply_markup=get_admin_inactive_users_menu())

async def admin_inactive_users_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    period = query.data.replace('admin_inactive_users_', '')
    await show_inactive_users_for_period(update, context, period, edit_message=query.message)

async def show_inactive_users_for_period(update, context, period, edit_message=None, send_chat=None):
    page = 0
    user_ids = await get_inactive_user_ids(period)
    total = len(user_ids)
    page_ids = user_ids[page*USERS_PER_PAGE:(page+1)*USERS_PER_PAGE]
    period_label = next((label for label, code in INACTIVE_PERIODS if code == period), period)
    text = f"💤 Неактивные пользователи: \n<b>{period_label}</b>\n\nВсего: <b>{total}</b>\n"
    keyboard = await get_inactive_users_list_keyboard(page_ids, page, total, period)
    if edit_message:
        await edit_message.edit_text(text, reply_markup=keyboard, parse_mode='HTML')
    elif send_chat:
        await send_chat.send_message(text, reply_markup=keyboard, parse_mode='HTML')

async def admin_inactive_users_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.replace('admin_inactive_users_page_', '')
    try:
        period, page_str = data.rsplit('_', 1)
        page = int(page_str)
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Ошибка навигации. Попробуйте начать заново.")
        return
    user_ids = await get_inactive_user_ids(period)
    total = len(user_ids)
    page_ids = user_ids[page*USERS_PER_PAGE:(page+1)*USERS_PER_PAGE]
    period_label = next((label for label, code in INACTIVE_PERIODS if code == period), period)
    text = f"💤 Неактивные пользователи: \n<b>{period_label}</b>\n\nВсего: <b>{total}</b>\n"
    await query.edit_message_text(text, reply_markup=await get_inactive_users_list_keyboard(page_ids, page, total, period), parse_mode='HTML')

async def admin_inactive_user_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.replace('admin_inactive_userinfo_', '')
    parts = data.split('_')
    user_id = int(parts[-1])
    page = int(parts[-2])
    period = '_'.join(parts[:-2])
    # MCP: Ставим флаг, что мы пришли из неактивных
    context.user_data['from_inactive_users'] = True
    users_info = await get_users_info_bulk([user_id])
    user = users_info.get(user_id)
    if not user:
        await query.edit_message_text('Пользователь не найден.', reply_markup=get_admin_inactive_users_menu())
        return

    # MCP: Форматируем даты с относительным временем
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
        f"<b>💎 Подписка:</b> {'Да' if user['is_subscribed'] else 'Нет'}\n"
        f"<b>📅 Активна до:</b> {subscription_until_str} {format_subscription_time_left(user.get('subscription_until'))}\n"
        f"<b>🔁 Кол-во подписок:</b> {user['subscription_count']}\n"
        f"<b>🕒 Последнее взаимодействие:</b> {last_active_at_str} {last_active_at_relative}\n"
    )
    # MCP: Кнопки как в других разделах, с прокидкой period и page
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✍️ Написать пользователю', callback_data=f'admin_private_message_{user_id}')],
        [InlineKeyboardButton('📈 Прогресс', callback_data=f'admin_progress_inactive_{user_id}_1_{period}_{page}')],
        [InlineKeyboardButton('🗑️ Удалить', callback_data=f'admin_delete_user_confirm_{user_id}')],
        [InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_back_to_profile_inactive_{user_id}_{period}_{page}')]
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

async def admin_inactive_users_export_excel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    period = query.data.replace('admin_inactive_users_export_excel_', '')
    end = get_inactive_period_dates(period)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT user_id, username, first_name, last_name, registered_at, is_subscribed, subscription_count, last_active_at FROM users WHERE last_active_at < $1 ORDER BY last_active_at ASC', end)
        users = [dict(row) for row in rows]
    from tgteacher_bot.services.exports.excel_export import _prepare_users_df, _export_df_to_excel, EXPORTS_DIR
    import datetime as dt
    now_str = dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_path = os.path.join(EXPORTS_DIR, f'inactive_users_{period}_{now_str}.xlsx')
    df = _prepare_users_df(users)
    await _export_df_to_excel(df, file_path)
    filename = os.path.basename(file_path)
    with open(file_path, 'rb') as f:
        file_bytes = f.read()
    # try:
    #     os.remove(file_path)
    # except Exception:
    #     pass
    await query.message.reply_document(
        document=InputFile(io.BytesIO(file_bytes), filename=filename),
        caption=f'Выгрузка неактивных пользователей: {period}',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('✅ Спасибо', callback_data='admin_inactive_users_export_thanks')]
        ])
    )

async def admin_inactive_users_export_thanks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass

def register_admin_inactive_users_handlers(application):
    application.add_handler(CallbackQueryHandler(admin_inactive_users_menu_callback, pattern='^admin_inactive_users_menu$'))
    application.add_handler(CallbackQueryHandler(admin_inactive_users_period_callback, pattern='^admin_inactive_users_(week|2weeks|3weeks|month)$'))
    application.add_handler(CallbackQueryHandler(admin_inactive_users_page_callback, pattern='^admin_inactive_users_page_'))
    application.add_handler(CallbackQueryHandler(admin_inactive_user_info_callback, pattern='^admin_inactive_userinfo_'))
    application.add_handler(CallbackQueryHandler(admin_inactive_users_export_excel_callback, pattern='^admin_inactive_users_export_excel_'))
    application.add_handler(CallbackQueryHandler(admin_inactive_users_export_thanks_callback, pattern='^admin_inactive_users_export_thanks$')) 
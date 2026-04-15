from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters
from tgteacher_bot.handlers.admin.admin_users import USERS_PER_PAGE, get_users_info_bulk, format_relative_time
from tgteacher_bot.db.user_repo import get_pool
from datetime import timedelta, datetime

# MCP: Состояние ожидания ввода ID
AWAITING_ID_INPUT = 'awaiting_id_input'

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

async def admin_users_search_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Сбрасываем режим перехода по странице, если он был активен
    context.user_data.pop('awaiting_page_input', None)
    # Сохраняем состояние ожидания ввода ID
    context.user_data[AWAITING_ID_INPUT] = True
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data='admin_users_all')]
    ])
    await query.edit_message_text('🔍 Введите ID или часть ID для поиска среди пользователей:', reply_markup=keyboard)
    # Сохраняем message_id инструкции для удаления
    context.user_data['search_id_instruction_message_id'] = query.message.message_id

async def admin_users_search_id_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer('❗️ Поиск отменён')
    context.user_data.pop(AWAITING_ID_INPUT, None)
    # Возврат к меню пользователей
    from tgteacher_bot.handlers.admin.admin_users import get_admin_users_menu
    await query.edit_message_text('👤 Управление пользователями:', reply_markup=get_admin_users_menu())

async def admin_users_search_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get(AWAITING_ID_INPUT):
        return  # Не в режиме поиска
    # Удаляем сообщение юзера
    try:
        await update.message.delete()
    except Exception:
        pass
    # Удаляем инструкцию, если message_id есть
    msg_id = context.user_data.get('search_id_instruction_message_id')
    if msg_id:
        try:
            chat_id = update.effective_chat.id
            await context.bot.delete_message(chat_id, msg_id)
            context.user_data.pop('search_id_instruction_message_id', None)
        except Exception:
            pass
    # Чистим флаг источника, мы снова в списке
    context.user_data.pop('from_search', None)
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text('❗ Введите только цифры или нажмите "Отмена"')
        return
    
    # ОПТИМИЗАЦИЯ: Поиск по user_id на стороне БД
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Ищем по частичному совпадению, user_id приводим к тексту
        rows = await conn.fetch("SELECT user_id FROM users WHERE CAST(user_id AS TEXT) ILIKE $1 ORDER BY registered_at DESC", f'%{text}%')
        found_ids = [row['user_id'] for row in rows]

    context.user_data['search_id_results'] = found_ids
    context.user_data['search_id_query'] = text
    context.user_data['search_id_page'] = 0
    await send_search_id_page(update, context, use_edit=False)
    context.user_data.pop(AWAITING_ID_INPUT, None)

async def send_search_id_page(update, context, use_edit=False, send_chat=None):
    found_ids = context.user_data.get('search_id_results', [])
    page = context.user_data.get('search_id_page', 0)
    query = context.user_data.get('search_id_query', '')
    total = len(found_ids)
    if total == 0:
        total_pages = 1
        page_ids = []
    else:
        total_pages = (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE
        page_ids = found_ids[page*USERS_PER_PAGE:(page+1)*USERS_PER_PAGE]
    
    # ОПТИМИЗАЦИЯ: Используем bulk-запрос
    keyboard = []
    users_info = await get_users_info_bulk(page_ids)
    for uid in page_ids:
        user = users_info.get(uid)
        emoji = '💎' if user and user['is_subscribed'] else '👤'
        first_name = user['first_name'] if user and user['first_name'] else '-'
        keyboard.append([InlineKeyboardButton(f"{emoji} {first_name} ({uid})", callback_data=f"admin_user_from_search_{uid}")])

    nav_buttons = []
    if total == 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
        nav_buttons.append(InlineKeyboardButton('1/1', callback_data='noop'))
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    else:
        if page > 0:
            nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='admin_search_id_page_prev'))
        else:
            nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
        nav_buttons.append(InlineKeyboardButton(f'{page+1}/{total_pages}', callback_data='noop'))
        if (page+1)*USERS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='admin_search_id_page_next'))
        else:
            nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton('🔍 Новый поиск', callback_data='admin_users_search_id')])
    keyboard.append([InlineKeyboardButton('⬅️ Все пользователи', callback_data='admin_users_all')])
    if total == 0:
        text = f'❌ Пользователи по запросу "<b>{query}</b>" не найдены'
    else:
        text = f'🔍 Найдено пользователей: <b>{total}</b> по запросу "<b>{query}</b>"'

    reply_markup = InlineKeyboardMarkup(keyboard)

    if use_edit and hasattr(update, 'callback_query') and update.callback_query:
        await update.callback_query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    elif send_chat is not None:
        await send_chat.send_message(
            text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    else:
        await update.message.reply_text(
            text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )

async def admin_search_id_page_prev_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['search_id_page'] = max(0, context.user_data.get('search_id_page', 0) - 1)
    await send_search_id_page(update, context, use_edit=True)

async def admin_search_id_page_next_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['search_id_page'] = context.user_data.get('search_id_page', 0) + 1
    await send_search_id_page(update, context, use_edit=True)

# Новый handler для профиля из поиска по ID
from tgteacher_bot.handlers.admin.admin_users import get_user_info

async def admin_user_from_search_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = int(query.data.replace('admin_user_from_search_', ''))
    
    # Ставим флаг источника — пришли из поиска по всем пользователям
    context.user_data['from_search'] = True
    
    # ОПТИМИЗАЦИЯ: Используем get_users_info_bulk, хотя тут всего 1 юзер, но для консистентности
    user_info_map = await get_users_info_bulk([user_id])
    user = user_info_map.get(user_id)

    if not user:
        await query.edit_message_text('Пользователь не найден.')
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
        f"<b>💎 Подписка:</b> {'✅ Да' if user['is_subscribed'] else '❌ Нет'}\n"
        f"<b>📅 Активна до:</b> {subscription_until_str} {format_subscription_time_left(user.get('subscription_until'))}\n"
        f"<b>🔁 Кол-во подписок:</b> {user['subscription_count']}\n"
        f"<b>🕒 Последнее взаимодействие:</b> {last_active_at_str} {last_active_at_relative}\n"
    )
    # MCP: Добавляем кнопку Удалить и возврат к результатам поиска
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✍️ Написать пользователю', callback_data=f'admin_private_message_{user_id}')],
        [
            InlineKeyboardButton('🎁 Подарить подписку', callback_data=f'admin_gift_subscription_{user_id}'),
            InlineKeyboardButton('❌ Удалить подписку', callback_data=f'admin_remove_subscription_{user_id}')
        ],
        [InlineKeyboardButton('📈 Прогресс', callback_data=f'admin_progress_from_search_{user_id}_1')],
        [InlineKeyboardButton('🗑️ Удалить', callback_data=f'admin_delete_user_confirm_{user_id}')],
        [InlineKeyboardButton('⬅️ Назад к результатам поиска', callback_data='admin_search_id_back_to_results')]
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

# Handler для кнопки "Назад к результатам поиска"
async def admin_search_id_back_to_results_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # Просто повторно вызываем send_search_id_page через edit_message_text
    await send_search_id_page(update, context, use_edit=True)
    # Чистим флаг источника — мы снова на списке
    context.user_data.pop('from_search', None)

# MCP: Регистрация обработчиков

def register_admin_users_search_id_handler(application):
    application.add_handler(CallbackQueryHandler(admin_users_search_id_callback, pattern='^admin_users_search_id$'))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), admin_users_search_id_message), group=997)
    application.add_handler(CallbackQueryHandler(admin_search_id_page_prev_callback, pattern='^admin_search_id_page_prev$'))
    application.add_handler(CallbackQueryHandler(admin_search_id_page_next_callback, pattern='^admin_search_id_page_next$'))
    application.add_handler(CallbackQueryHandler(admin_user_from_search_callback, pattern='^admin_user_from_search_\\d+$'))
    application.add_handler(CallbackQueryHandler(admin_search_id_back_to_results_callback, pattern='^admin_search_id_back_to_results$')) 
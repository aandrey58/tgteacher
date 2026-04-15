from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters
from tgteacher_bot.db.user_repo import get_pool
import asyncio
from datetime import datetime, timedelta

# MCP: Состояние ожидания ввода ID для новых пользователей
AWAITING_NEW_USER_ID_INPUT = 'awaiting_new_user_id_input'

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

async def admin_new_users_search_id_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # MCP: Извлекаем период из callback_data
    try:
        period = query.data.replace('admin_new_users_search_id_', '')
        context.user_data['admin_period_for_search'] = period
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Ошибка: не удалось определить период для поиска.")
        return

    context.user_data[AWAITING_NEW_USER_ID_INPUT] = True
    context.user_data.pop('awaiting_page_input', None)
    context.user_data.pop('awaiting_id_input', None) # MCP: Чистим старый стейт
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data=f'admin_new_users_{period}')]
    ])
    
    await query.edit_message_text('🔍 Введите ID или часть ID для поиска среди новых пользователей:', reply_markup=keyboard)
    context.user_data['search_id_instruction_message_id'] = query.message.message_id


async def admin_new_users_search_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get(AWAITING_NEW_USER_ID_INPUT):
        return

    from tgteacher_bot.handlers.admin.admin_users import get_new_user_ids
    try:
        await update.message.delete()
    except Exception:
        pass

    msg_id = context.user_data.pop('search_id_instruction_message_id', None)
    if msg_id:
        try:
            await context.bot.delete_message(update.effective_chat.id, msg_id)
        except Exception:
            pass
            
    text = update.message.text.strip()
    period = context.user_data.get('admin_period_for_search')
    
    if not text.isdigit():
        # MCP: Отправляем временное сообщение об ошибке
        error_msg = await update.message.chat.send_message('❗ Введите только цифры.')
        # Ждем 3 секунды и удаляем сообщение
        await asyncio.sleep(3)
        try:
            await error_msg.delete()
        except Exception:
            pass
        # MCP: Возвращаем клавиатуру для нового ввода
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('❌ Отмена', callback_data=f'admin_new_users_{period}')]
        ])
        sent_message = await update.message.chat.send_message('🔍 Введите ID или часть ID для поиска среди новых пользователей:', reply_markup=keyboard)
        context.user_data['search_id_instruction_message_id'] = sent_message.message_id
        return

    # Получаем ID новых пользователей за период
    new_user_ids = await get_new_user_ids(period)
    if not new_user_ids:
        found_ids = []
    else:
        # Фильтруем ID по введенному тексту
        found_ids = [user_id for user_id in new_user_ids if text in str(user_id)]

    context.user_data['new_search_results'] = found_ids
    context.user_data['new_search_query'] = text
    context.user_data['new_search_page'] = 0
    await send_new_search_id_page(update, context, use_edit=False)
    context.user_data.pop(AWAITING_NEW_USER_ID_INPUT)

async def send_new_search_id_page(update, context, use_edit=False, send_chat=None):
    from tgteacher_bot.handlers.admin.admin_users import USERS_PER_PAGE, get_users_info_bulk, format_relative_time, get_period_label
    found_ids = context.user_data.get('new_search_results', [])
    page = context.user_data.get('new_search_page', 0)
    period = context.user_data.get('admin_period_for_search')
    query = context.user_data.get('new_search_query', '')
    total = len(found_ids)

    if total == 0:
        page_ids = []
        total_pages = 1
    else:
        total_pages = (total + USERS_PER_PAGE - 1) // USERS_PER_PAGE
        page_ids = found_ids[page * USERS_PER_PAGE:(page + 1) * USERS_PER_PAGE]

    keyboard_rows = []
    users_info = await get_users_info_bulk(page_ids)
    for uid in page_ids:
        user = users_info.get(uid)
        emoji = '💎' if user and user['is_subscribed'] else '👤'
        first_name = user['first_name'] if user and user['first_name'] else '-'
        # MCP: Форматируем даты
        registered_at_str = user['registered_at'].strftime('%Y-%m-%d %H:%M:%S') if user and user['registered_at'] else '-'
        registered_at_relative = format_relative_time(user['registered_at']) if user else ''
        last_active_at_str = user['last_active_at'].strftime('%Y-%m-%d %H:%M:%S') if user and user.get('last_active_at') else '-'
        last_active_at_relative = format_relative_time(user.get('last_active_at')) if user else ''
        # MCP: Адаптируем callback для возврата в профиль из поиска новых юзеров
        callback_data = f'admin_new_userinfo_{period}_{page}_{uid}_from_search'
        keyboard_rows.append([
            InlineKeyboardButton(
                f"{emoji} {first_name} ({uid})",
                callback_data=callback_data
            )
        ])
        # Можно добавить отдельное сообщение с профилем при нажатии, если нужно
    
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_new_search_page_prev_{period}'))
    else:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
    
    nav_buttons.append(InlineKeyboardButton(f'{page + 1}/{total_pages}', callback_data='noop'))
    
    if (page + 1) * USERS_PER_PAGE < total:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data=f'admin_new_search_page_next_{period}'))
    else:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    
    keyboard_rows.append(nav_buttons)
    keyboard_rows.append([InlineKeyboardButton('🔍 Новый поиск', callback_data=f'admin_new_users_search_id_{period}')])
    keyboard_rows.append([InlineKeyboardButton('⬅️ К списку новых', callback_data=f'admin_new_users_{period}')])

    if total == 0:
        text = f'❌ Пользователи по запросу "<b>{query}</b>" не найдены в периоде "<b>{get_period_label(period)}</b>"'
    else:
        text = f'🔍 Найдено пользователей: <b>{total}</b> по запросу "<b>{query}</b>" за период "<b>{get_period_label(period)}</b>"'
    
    reply_markup = InlineKeyboardMarkup(keyboard_rows)
    
    if use_edit and hasattr(update, 'callback_query') and update.callback_query:
        await update.callback_query.edit_message_text(text, reply_markup=reply_markup, parse_mode='HTML')
    elif send_chat is not None:
        await send_chat.send_message(text, reply_markup=reply_markup, parse_mode='HTML')
    else:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='HTML')


async def admin_new_search_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action, period = query.data.replace('admin_new_search_page_', '').rsplit('_', 1)
    
    page = context.user_data.get('new_search_page', 0)
    if action == 'next':
        context.user_data['new_search_page'] = page + 1
    elif action == 'prev':
        context.user_data['new_search_page'] = max(0, page - 1)
        
    context.user_data['admin_period_for_search'] = period
    await send_new_search_id_page(update, context, use_edit=True)

async def admin_new_search_back_to_results_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await send_new_search_id_page(update, context, use_edit=True)


def register_admin_new_users_search_handlers(application):
    application.add_handler(CallbackQueryHandler(admin_new_users_search_id_callback, pattern=r'^admin_new_users_search_id_'))
    application.add_handler(CallbackQueryHandler(admin_new_search_page_callback, pattern=r'^admin_new_search_page_'))
    application.add_handler(CallbackQueryHandler(admin_new_search_back_to_results_callback, pattern=r'^admin_new_search_back_to_results$')) 
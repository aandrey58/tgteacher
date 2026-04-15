import datetime
from typing import Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

from tgteacher_bot.db.user_repo import get_user_subscription_info_pg, mark_user_active_if_needed
from tgteacher_bot.handlers.admin.admin_status import track_metrics


def _format_timedelta(dt_end: Optional[datetime.datetime]) -> str:
    if not dt_end:
        return '—'
    now = datetime.datetime.now(tz=dt_end.tzinfo) if dt_end.tzinfo else datetime.datetime.now()
    delta = dt_end - now
    if delta.total_seconds() <= 0:
        return '0 дней'
    days = delta.days
    hours = (delta.seconds // 3600)
    if days > 0:
        return f"{days} дн. {hours} ч."
    minutes = (delta.seconds % 3600) // 60
    return f"{hours} ч. {minutes} мин."


def _format_date(dt: Optional[datetime.datetime]) -> str:
    if not dt:
        return '—'
    # Покажем локально без TZ, краткий формат
    return dt.strftime('%d.%m.%Y %H:%M')


@track_metrics
async def subscription_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await mark_user_active_if_needed(user_id, context)
    await query.answer()

    info = await get_user_subscription_info_pg(user_id)
    is_subscribed = info.get('is_subscribed', False)
    count = info.get('subscription_count', 0)
    until = info.get('subscription_until')

    status_line = 'Активна ✅' if is_subscribed else 'Нет ❌'
    until_str = _format_date(until)
    left_str = f"({_format_timedelta(until)})" if until else ''

    # Новое описание подписки
    text = (
        '<b>💎 Подписка</b>\n\n'
        f"<b>Статус:</b> {status_line}\n"
        f"<b>Действует до:</b> {until_str} {left_str}\n\n"
        'В настоящее время реализован один вариант платной подписки:\n\n'
        '- 3 месяца\n'
        'Покупая подписку, вы получаете доступ к заданиям всех существующих групп слов, и дополнительно получаете 3 дополнительных типа заданий для отработки лексики: визуальный квиз, задание на аудирование, дополнительное задание на подбор слова в предложении. Новая группа слова появляется в боте примерно один раз в месяц.   \n\n'
        'Стоимость подписки составляет 300 рублей. Стоимость латте 🥤🥤🥤'
    )

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('💳 Приобрести подписку', callback_data='buy_subscription')],
        [InlineKeyboardButton('🧾 История покупок', callback_data='subscription_history')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='main_menu')],
    ])

    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


@track_metrics
async def buy_subscription_placeholder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    try:
        from tgteacher_bot.db.pool import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch('SELECT months, amount FROM payment_options ORDER BY months')
            
        if not rows:
            text = '❌ <b>Варианты оплаты недоступны</b>\n\nК сожалению, варианты оплаты временно недоступны.\nПопробуйте позже.'
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('⬅️ Назад', callback_data='subscription')]
            ])
        else:
            text = '💳 <b>Выберите срок и сумму подписки</b>\n\nВыберите подходящий для Вас вариант:'
            buttons = []
            
            # Создаем кнопки подписки в 2 столбика
            for i in range(0, len(rows), 2):
                row_buttons = []
                months = rows[i]['months']
                amount = rows[i]['amount']
                button_text = f'💎 {months} мес. - {amount}₽'
                callback_data = f'buy_option_{months}_{amount}'
                row_buttons.append(InlineKeyboardButton(button_text, callback_data=callback_data))
                
                # Добавляем вторую кнопку в строку, если есть
                if i + 1 < len(rows):
                    months2 = rows[i + 1]['months']
                    amount2 = rows[i + 1]['amount']
                    button_text2 = f'💎 {months2} мес. - {amount2}₽'
                    callback_data2 = f'buy_option_{months2}_{amount2}'
                    row_buttons.append(InlineKeyboardButton(button_text2, callback_data=callback_data2))
                
                buttons.append(row_buttons)
            
            # Добавляем кнопку "Назад" внизу
            buttons.append([InlineKeyboardButton('⬅️ Назад', callback_data='subscription')])
            keyboard = InlineKeyboardMarkup(buttons)
            
    except Exception as e:
        text = '❌ <b>Ошибка загрузки</b>\n\nНе удалось загрузить варианты оплаты.\nПопробуйте позже.'
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ Назад', callback_data='subscription')]
        ])
    
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


@track_metrics
async def buy_option_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора варианта оплаты"""
    query = update.callback_query
    await query.answer()
    
    # Парсим callback_data: buy_option_{months}_{amount}
    callback_data = query.data
    parts = callback_data.split('_')
    
    if len(parts) != 4:
        await query.edit_message_text('❌ Ошибка: неверный формат данных')
        return
    
    try:
        months = int(parts[2])
        amount = int(parts[3])
    except ValueError:
        await query.edit_message_text('❌ Ошибка: неверные данные')
        return
    
    # Показываем информацию о выбранном варианте
    text = f'💳 <b>Подтверждение покупки</b>\n\n'
    text += f'Вы выбрали подписку:\n'
    text += f'<b>Срок:</b> {months} мес.\n'
    text += f'<b>Стоимость:</b> {amount}₽\n\n'
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Подтвердить', callback_data=f'confirm_payment_{months}_{amount}')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='buy_subscription')]
    ])
    
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


@track_metrics
async def confirm_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик подтверждения оплаты - создает платеж в ЮKassa"""
    from tgteacher_bot.services.payments.yookassa_payment import create_payment_callback
    await create_payment_callback(update, context)


@track_metrics
async def subscription_history_placeholder(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает историю платежей пользователя"""
    query = update.callback_query
    user_id = query.from_user.id
    await mark_user_active_if_needed(user_id, context)
    await query.answer()
    
    try:
        from tgteacher_bot.db.pool import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Получаем историю платежей пользователя
            rows = await conn.fetch('''
                SELECT payment_id, months, amount, status, created_at, processed
                FROM payments 
                WHERE user_id = $1 
                ORDER BY created_at DESC 
                LIMIT 10
            ''', user_id)
            
            # Получаем общую статистику
            stats = await conn.fetchrow('''
                SELECT 
                    COUNT(*) as total_payments
                FROM payments 
                WHERE user_id = $1
            ''', user_id)
            
        if not rows:
            text = '📋 <b>История платежей</b>\n\n'
            text += 'У вас пока нет платежей.\n'
            text += 'Совершите первую покупку подписки!'
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('💳 Приобрести подписку', callback_data='buy_subscription')],
                [InlineKeyboardButton('⬅️ Назад', callback_data='subscription')]
            ])
        else:
            text = '📋 <b>История платежей</b>\n\n'
            
            # Общая статистика (без суммы и купленных месяцев)
            total_payments = stats['total_payments'] or 0
            text += f'<b>Общая статистика:</b>\n'
            text += f'• Всего платежей: {total_payments}\n\n'
            
            # Последние платежи
            text += f'<b>Последние платежи:</b>\n'
            for i, row in enumerate(rows[:5], 1):  # Показываем только 5 последних
                amount_rub = row['amount'] / 100
                date = row['created_at'].strftime('%d.%m.%Y %H:%M')
                # Без эмодзи статусов и обработанности
                text += f'{i}. {date} - {amount_rub}₽ ({row["months"]} мес.)\n'
            
            remaining = max(0, (total_payments or 0) - 5)
            if remaining > 0:
                text += f'\n<i>… и ещё {remaining} платежей</i>'
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('💳 Новый платеж', callback_data='buy_subscription')],
                [InlineKeyboardButton('⬅️ Назад', callback_data='subscription')]
            ])
            
    except Exception as e:
        print(f"Ошибка загрузки истории платежей: {e}")
        text = '❌ <b>Ошибка загрузки</b>\n\nНе удалось загрузить историю платежей.\nПопробуйте позже.'
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ Назад', callback_data='subscription')]
        ])
    
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


def register_subscription_handlers(application: Application):
    application.add_handler(CallbackQueryHandler(subscription_callback, pattern='^subscription$'))
    application.add_handler(CallbackQueryHandler(buy_subscription_placeholder, pattern='^buy_subscription$'))
    application.add_handler(CallbackQueryHandler(buy_option_callback, pattern='^buy_option_'))
    application.add_handler(CallbackQueryHandler(confirm_payment_callback, pattern='^confirm_payment_'))
    application.add_handler(CallbackQueryHandler(subscription_history_placeholder, pattern='^subscription_history$'))
    
    # Добавляем обработчик для проверки платежей
    from tgteacher_bot.services.payments.yookassa_payment import check_payment_callback
    application.add_handler(CallbackQueryHandler(check_payment_callback, pattern='^check_payment_')) 
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

    text = (
        '<b>💎 Подписка</b>\n\n'
        f"<b>Статус:</b> {status_line}\n"
        f"<b>Действует до:</b> {('Бессрочно' if is_subscribed and until is None else until_str)} {left_str}\n\n"
        'VIP открывает доступ к дополнительным учебным материалам. '
        'Актуальные сроки и стоимость указаны в списке тарифов.'
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
    from tgteacher_bot.services.payments.yookassa_payment import private_owner
    from tgteacher_bot.db.payment_orders import open_orders
    if not private_owner(update):
        return
    active=await open_orders(query.from_user.id)
    if active:
        buttons=[[InlineKeyboardButton("Заказ "+str(row["id"])[:8],callback_data="check_payment_"+str(row["id"]))] for row in active]
        buttons.append([InlineKeyboardButton("Назад",callback_data="subscription")])
        await query.edit_message_text("Сначала проверьте существующий заказ. При неопределённом результате обратитесь в поддержку.",reply_markup=InlineKeyboardMarkup(buttons))
        return
    await query.edit_message_text("Новые покупки временно недоступны. Ранее оплаченные заказы можно проверить.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад",callback_data="subscription")]]))

@track_metrics
async def buy_option_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора варианта оплаты"""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("Новые покупки временно недоступны.",reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("К подписке",callback_data="subscription")]]))

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
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters
from tgteacher_bot.db.pool import get_pool
import asyncio
import os
import io
from datetime import datetime
from math import ceil

AWAITING_EXTEND_ALL_DAYS = 'awaiting_extend_all_days'
AWAITING_PAYMENT_MONTHS = 'awaiting_payment_months'
AWAITING_PAYMENT_AMOUNT = 'awaiting_payment_amount'


def get_admin_sub_pay_menu():
    """Меню управления подписками"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🎁 Управление подписками', callback_data='admin_sub_pay_manage')],
        [InlineKeyboardButton('💰 Настройки оплаты', callback_data='admin_sub_pay_settings')],
        [InlineKeyboardButton('📊 История покупок', callback_data='admin_sub_pay_history')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_panel')]
    ])

async def admin_sub_pay_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню управления подписками"""
    query = update.callback_query
    await query.answer()
    
    text = '💎 <b>Управление подписками</b>\n\nВыберите раздел для управления подписками пользователей:'
    await query.edit_message_text(text, reply_markup=get_admin_sub_pay_menu(), parse_mode='HTML')


def get_admin_sub_pay_manage_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('⏱ Продлить подписки', callback_data='admin_sub_pay_extend_all')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay')]
    ])

async def admin_sub_pay_manage_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Управление подписками"""
    query = update.callback_query
    await query.answer()
    
    text = '🎁 <b>Управление подписками</b>\n\nДоступные действия:'
    await query.edit_message_text(text, reply_markup=get_admin_sub_pay_manage_menu(), parse_mode='HTML')

async def admin_sub_pay_settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Настройки оплаты"""
    query = update.callback_query
    await query.answer()
    
    text = '💰 <b>Настройки оплаты</b>\n\nЗдесь можно добавить срок и суммы оплаты подписки\n\n<i>Максимально можно добавить 4 варианта оплаты</i>'
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('➕ Добавить значение', callback_data='admin_sub_pay_add_value')],
        [InlineKeyboardButton('📋 Список значений', callback_data='admin_sub_pay_list_values')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay')]
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

# MCP: Периоды для истории покупок
PAYMENT_HISTORY_PERIODS = [
    ('📅 Сегодня', 'today'),
    ('🗓️ За неделю', 'week'),
    ('📆 За месяц', 'month'),
]

def get_admin_payment_history_menu():
    """Меню истории покупок"""
    keyboard = [
        [InlineKeyboardButton(f'{label}', callback_data=f'admin_payment_history_{period}')]
        for label, period in PAYMENT_HISTORY_PERIODS
    ]
    keyboard.append([InlineKeyboardButton('📊 Выбрать период', callback_data='admin_payment_history_custom_period')])
    keyboard.append([InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay')])
    return InlineKeyboardMarkup(keyboard)

async def admin_sub_pay_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """История покупок"""
    query = update.callback_query
    await query.answer()
    
    text = '📊 <b>История покупок</b>\n\nЗдесь можно выгрузить все платежи\n\nВыберите период:'
    await query.edit_message_text(text, reply_markup=get_admin_payment_history_menu(), parse_mode='HTML')

async def admin_payment_history_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора периода для истории покупок"""
    query = update.callback_query
    await query.answer()
    period = query.data.replace('admin_payment_history_', '')
    await export_payment_history_for_period(update, context, period, edit_message=query.message)

async def export_payment_history_for_period(update, context, period, edit_message=None, send_chat=None):
    """Экспорт истории покупок за период"""
    from tgteacher_bot.services.exports.excel_export import export_payment_history_to_excel
    from tgteacher_bot.handlers.admin.admin_users import get_period_label
    
    try:
        file_path = await export_payment_history_to_excel(period)
        filename = os.path.basename(file_path)
        with open(file_path, 'rb') as f:
            file_bytes = f.read()
        
        # Отправляем файл с кнопкой "Спасибо"
        if edit_message:
            await edit_message.reply_document(
                document=InputFile(io.BytesIO(file_bytes), filename=filename),
                caption=f'История покупок за период: {get_period_label(period)}',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('✅ Спасибо', callback_data='admin_payment_history_thanks')]
                ])
            )
        elif send_chat:
            await send_chat.send_document(
                document=InputFile(io.BytesIO(file_bytes), filename=filename),
                caption=f'История покупок за период: {get_period_label(period)}',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('✅ Спасибо', callback_data='admin_payment_history_thanks')]
                ])
            )
            
    except Exception as e:
        error_text = '❌ Ошибка при выгрузке истории покупок. Попробуйте позже.'
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay_history')]
        ])
        if edit_message:
            await edit_message.edit_text(error_text, reply_markup=keyboard, parse_mode='HTML')
        elif send_chat:
            await send_chat.send_message(error_text, reply_markup=keyboard, parse_mode='HTML')

# MCP: Состояния для кастомного периода истории покупок
AWAITING_PAYMENT_HISTORY_PERIOD_START = 'awaiting_payment_history_period_start'
AWAITING_PAYMENT_HISTORY_PERIOD_END = 'awaiting_payment_history_period_end'

async def admin_payment_history_custom_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрос начальной даты для кастомного периода истории покупок"""
    query = update.callback_query
    await query.answer()
    context.user_data[AWAITING_PAYMENT_HISTORY_PERIOD_START] = True
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data='admin_payment_history_custom_period_cancel')]
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
    context.user_data['payment_history_period_input_message_id'] = sent_message.message_id

async def admin_payment_history_custom_period_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена выбора кастомного периода истории покупок"""
    query = update.callback_query
    await query.answer()
    context.user_data.pop(AWAITING_PAYMENT_HISTORY_PERIOD_START, None)
    context.user_data.pop(AWAITING_PAYMENT_HISTORY_PERIOD_END, None)
    context.user_data.pop('payment_history_period_input_message_id', None)
    context.user_data.pop('payment_history_period_start', None)
    try:
        await query.message.delete()
    except Exception:
        pass
    await query.message.chat.send_message(
        '📊 История покупок — выберите период:',
        reply_markup=get_admin_payment_history_menu()
    )

async def admin_payment_history_thanks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Спасибо' в истории покупок"""
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass


async def admin_sub_pay_add_value_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрос срока оплаты в месяцах"""
    query = update.callback_query
    await query.answer()

    # Проверяем количество существующих вариантов
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval('SELECT COUNT(*) FROM payment_options')
            
        if count >= 4:
            text = '❌ <b>Достигнут лимит</b>\n\nМаксимально можно добавить 4 варианта оплаты.\nУдалите один из существующих вариантов, чтобы добавить новый.'
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('📋 Список значений', callback_data='admin_sub_pay_list_values')],
                [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay_settings')]
            ])
            await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')
            return
            
    except Exception as e:
        # Если не удалось проверить, продолжаем
        pass

    context.user_data[AWAITING_PAYMENT_MONTHS] = True

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data='admin_sub_pay_add_value_cancel')]
    ])
    sent_message = await query.edit_message_text(
        '➕ <b>Добавить значение</b>\n\n'
        'Введите срок подписки в месяцах:\n'
        '<i>(от 1 до 60 месяцев)</i>',
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    # Запоминаем message_id, чтобы удалить после ввода
    try:
        context.user_data['add_value_instruction_message_id'] = sent_message.message_id
    except Exception:
        pass


async def admin_sub_pay_add_value_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена добавления значения"""
    query = update.callback_query
    await query.answer()
    context.user_data.pop(AWAITING_PAYMENT_MONTHS, None)
    context.user_data.pop(AWAITING_PAYMENT_AMOUNT, None)
    context.user_data.pop('add_value_instruction_message_id', None)
    context.user_data.pop('temp_months', None)
    context.user_data.pop('temp_amount', None)
    
    text = '💰 <b>Настройки оплаты</b>\n\nЗдесь можно добавить срок и суммы оплаты подписки'
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('➕ Добавить значение', callback_data='admin_sub_pay_add_value')],
        [InlineKeyboardButton('📋 Список значений', callback_data='admin_sub_pay_list_values')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay')]
    ])
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


async def admin_sub_pay_list_values_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр списка значений оплаты"""
    query = update.callback_query
    await query.answer()
    
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch('SELECT months, amount FROM payment_options ORDER BY months')
            count = len(rows)
            
        if not rows:
            text = f'📋 <b>Список значений</b>\n\nПока нет добавленных вариантов оплаты.\n\n<i>Добавлено: 0/4 вариантов</i>'
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay_settings')]
            ])
        else:
            text = f'📋 <b>Список значений</b>\n\nДоступные варианты оплаты:\n\n<i>Добавлено: {count}/4 вариантов</i>'
            buttons = []
            for row in rows:
                months = row['months']
                amount = row['amount']
                button_text = f'💎 {months} мес. - {amount}₽'
                callback_data = f'payment_option_{months}_{amount}'
                buttons.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
            
            buttons.append([InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay_settings')])
            keyboard = InlineKeyboardMarkup(buttons)
            
    except Exception as e:
        text = '❌ Ошибка при загрузке списка значений.'
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay_settings')]
        ])
    
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

async def admin_sub_pay_payment_option_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатия на кнопку варианта оплаты"""
    query = update.callback_query
    await query.answer()
    
    # Парсим callback_data: payment_option_{months}_{amount}
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
    
    # Сохраняем данные в context для удаления
    context.user_data['delete_payment_months'] = months
    context.user_data['delete_payment_amount'] = amount
    
    text = f'💰 <b>Вариант оплаты</b>\n\nСрок: <b>{months}</b> мес.\nСумма: <b>{amount}</b>₽\n\nВыберите действие:'
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('💰 Изменить сумму', callback_data='admin_sub_pay_edit_amount')],
        [InlineKeyboardButton('🗑 Удалить', callback_data='admin_sub_pay_delete_option')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay_list_values')]
    ])
    
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

async def admin_sub_pay_delete_option_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик удаления варианта оплаты - показываем подтверждение"""
    query = update.callback_query
    await query.answer()
    
    # Получаем данные из context
    months = context.user_data.get('delete_payment_months')
    amount = context.user_data.get('delete_payment_amount')
    
    if months is None or amount is None:
        await query.edit_message_text('❌ Ошибка: данные не найдены')
        return
    
    # Показываем подтверждение удаления
    text = f'🗑 <b>Подтверждение удаления</b>\n\n'
    text += f'Вы действительно хотите удалить вариант оплаты:\n'
    text += f'<b>{months}</b> мес. - <b>{amount}</b>₽\n\n'
    text += f'Это действие нельзя отменить!'
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Да, удалить', callback_data='admin_sub_pay_delete_confirm'), 
         InlineKeyboardButton('❌ Отмена', callback_data='admin_sub_pay_delete_cancel')]
    ])
    
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


async def admin_sub_pay_delete_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик подтверждения удаления варианта оплаты"""
    query = update.callback_query
    await query.answer()
    
    # Получаем данные из context
    months = context.user_data.get('delete_payment_months')
    amount = context.user_data.get('delete_payment_amount')
    
    if months is None or amount is None:
        await query.edit_message_text('❌ Ошибка: данные не найдены')
        return
    
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Удаляем вариант оплаты
            result = await conn.execute('DELETE FROM payment_options WHERE months = $1', months)
            
            if result == 'DELETE 1':
                message_text = f'✅ Удалил вариант оплаты: <b>{months}</b> мес. - <b>{amount}</b>₽'
            else:
                message_text = '❌ Вариант оплаты не найден'
                
    except Exception as e:
        message_text = '❌ Ошибка при удалении. Попробуйте позже.'
    finally:
        # Очищаем данные из context
        context.user_data.pop('delete_payment_months', None)
        context.user_data.pop('delete_payment_amount', None)
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Спасибо', callback_data='admin_sub_pay_list_values')]
    ])
    
    await query.edit_message_text(message_text, reply_markup=keyboard, parse_mode='HTML')


async def admin_sub_pay_delete_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отмены удаления варианта оплаты"""
    query = update.callback_query
    await query.answer()
    
    # Получаем данные из context для возврата к варианту оплаты
    months = context.user_data.get('delete_payment_months')
    amount = context.user_data.get('delete_payment_amount')
    
    if months is None or amount is None:
        await query.edit_message_text('❌ Ошибка: данные не найдены')
        return
    
    # Возвращаемся к варианту оплаты
    text = f'💰 <b>Вариант оплаты</b>\n\nСрок: <b>{months}</b> мес.\nСумма: <b>{amount}</b>₽\n\nВыберите действие:'
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('💰 Изменить сумму', callback_data='admin_sub_pay_edit_amount')],
        [InlineKeyboardButton('🗑 Удалить', callback_data='admin_sub_pay_delete_option')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay_list_values')]
    ])
    
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


async def admin_sub_pay_edit_amount_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик изменения суммы варианта оплаты"""
    query = update.callback_query
    await query.answer()
    
    # Получаем данные из context
    months = context.user_data.get('delete_payment_months')
    old_amount = context.user_data.get('delete_payment_amount')
    
    if months is None or old_amount is None:
        await query.edit_message_text('❌ Ошибка: данные не найдены')
        return
    
    # Устанавливаем состояние ожидания ввода новой суммы
    context.chat_data[AWAITING_PAYMENT_AMOUNT] = True
    context.chat_data['temp_months'] = months
    context.chat_data['editing_existing'] = True
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data='admin_sub_pay_edit_amount_cancel')]
    ])
    
    # Используем edit_message_text и сохраняем ID отредактированного сообщения
    edited_message = await query.edit_message_text(
        f'💰 <b>Изменить сумму</b>\n\n'
        f'Срок: <b>{months}</b> мес.\n'
        f'Текущая сумма: <b>{old_amount}</b>₽\n\n'
        f'Введите новую сумму в рублях:\n'
        f'<i>(от 1 до 100000)</i>',
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    # Запоминаем message_id для удаления после ввода
    try:
        context.chat_data['edit_amount_instruction_message_id'] = edited_message.message_id
    except Exception:
        pass

async def admin_sub_pay_extend_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запрос количества дней для массового продления подписок"""
    query = update.callback_query
    await query.answer()

    context.user_data[AWAITING_EXTEND_ALL_DAYS] = True

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data='admin_sub_pay_extend_all_cancel')]
    ])
    sent_message = await query.edit_message_text(
        '⏱ <b>Продлить подписки</b>\n\n'
        'Введите количество дней для продления подписок всем, у кого подписка активна или истекла не позже 1 дня назад.\n'
        '<i>(от 1 до 365 дней)</i>',
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    # Запоминаем message_id, чтобы удалить после ввода
    try:
        context.user_data['extend_all_instruction_message_id'] = sent_message.message_id
    except Exception:
        pass

async def admin_sub_pay_extend_all_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена ожидания ввода дней"""
    query = update.callback_query
    await query.answer()
    context.user_data.pop(AWAITING_EXTEND_ALL_DAYS, None)
    context.user_data.pop('extend_all_instruction_message_id', None)
    await query.edit_message_text('🎁 <b>Управление подписками</b>\n\nДоступные действия:', reply_markup=get_admin_sub_pay_manage_menu(), parse_mode='HTML')

async def admin_sub_pay_extend_all_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода числа дней для массового продления"""
    if not context.user_data.get(AWAITING_EXTEND_ALL_DAYS):
        return

    # Пытаемся сразу удалить сообщение пользователя, чтобы не засорять чат
    try:
        await update.message.delete()
    except Exception:
        pass

    text = (update.message.text or '').strip()
    instruction_message_id = context.user_data.get('extend_all_instruction_message_id')

    # Отмена
    if text.lower() in {'отмена', 'cancel', 'стоп'}:
        # Удаляем инструкцию, если есть
        if instruction_message_id:
            try:
                await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            except Exception:
                pass
        context.user_data.pop('extend_all_instruction_message_id', None)
        context.user_data.pop(AWAITING_EXTEND_ALL_DAYS, None)
        await update.message.chat.send_message('❌ Отменено.')
        return

    # Валидация
    if not text.isdigit():
        err = await update.message.chat.send_message('❗ Введите только цифры от 1 до 365')
        await asyncio.sleep(3)
        try:
            await err.delete()
        except Exception:
            pass
        return

    days = int(text)
    if days < 1 or days > 365:
        err = await update.message.chat.send_message('❗ Количество дней должно быть от 1 до 365')
        await asyncio.sleep(3)
        try:
            await err.delete()
        except Exception:
            pass
        return

    # Удаляем инструкцию после валидного ввода
    if instruction_message_id:
        try:
            await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
        except Exception:
            pass
        context.user_data.pop('extend_all_instruction_message_id', None)

    # Массовое продление в БД
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch('''
                UPDATE users
                SET subscription_until = CASE
                    WHEN subscription_until IS NOT NULL THEN subscription_until + make_interval(days => $1)
                    ELSE NOW() + make_interval(days => $1)
                END,
                is_subscribed = TRUE
                WHERE subscription_until IS NOT NULL
                  AND subscription_until >= NOW() - INTERVAL '1 day'
                RETURNING user_id, subscription_until
            ''', days)
            updated = len(rows) if rows is not None else 0

            # Гарантируем таблицу payments
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS payments (
                    id SERIAL PRIMARY KEY,
                    payment_id VARCHAR(255) UNIQUE NOT NULL,
                    user_id BIGINT NOT NULL,
                    months INTEGER NOT NULL,
                    amount INTEGER NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    payment_url TEXT,
                    processed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    processed_at TIMESTAMP,
                    result_subscription_until TIMESTAMP WITH TIME ZONE
                )
            ''')

            # Рассчитываем месяцы для записи (минимум 1)
            extend_months = max(1, ceil(days / 30))
            now_ts = int(datetime.now().timestamp())
            for idx, row in enumerate(rows or []):
                uid = row['user_id']
                new_until = row['subscription_until']
                payment_id = f'extend_all_{uid}_{now_ts}_{idx}'
                await conn.execute('''
                    INSERT INTO payments (payment_id, user_id, months, amount, status, processed, created_at, updated_at, processed_at, result_subscription_until)
                    VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW(), NOW(), $7)
                    ON CONFLICT (payment_id) DO NOTHING
                ''', payment_id, uid, extend_months, 0, 'extend_all', True, new_until)

    except Exception as e:
        await update.message.chat.send_message('❌ Ошибка при продлении подписок. Попробуйте позже.')
        return
    finally:
        context.user_data.pop(AWAITING_EXTEND_ALL_DAYS, None)
        context.user_data.pop('extend_all_instruction_message_id', None)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Спасибо', callback_data='admin_sub_pay_manage')]
    ])
    await update.message.chat.send_message(
        f'✅ Продлил подписки на <b>{days}</b> дн.\nЗатронуто пользователей: <b>{updated}</b>.',
        reply_markup=keyboard,
        parse_mode='HTML'
    )


async def admin_sub_pay_add_value_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода месяцев и суммы для добавления значения оплаты"""
    # Проверяем, ожидаем ли мы ввод месяцев или суммы
    awaiting_months = context.user_data.get(AWAITING_PAYMENT_MONTHS)
    awaiting_amount = context.user_data.get(AWAITING_PAYMENT_AMOUNT)
    
    if not awaiting_months and not awaiting_amount:
        return

    # Пытаемся сразу удалить сообщение пользователя
    try:
        await update.message.delete()
    except Exception:
        pass

    text = (update.message.text or '').strip()
    instruction_message_id = context.user_data.get('add_value_instruction_message_id')

    # Отмена
    if text.lower() in {'отмена', 'cancel', 'стоп'}:
        if instruction_message_id:
            try:
                await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            except Exception:
                pass
        context.user_data.pop('add_value_instruction_message_id', None)
        context.user_data.pop(AWAITING_PAYMENT_MONTHS, None)
        context.user_data.pop(AWAITING_PAYMENT_AMOUNT, None)
        context.user_data.pop('temp_months', None)
        context.user_data.pop('temp_amount', None)
        await update.message.chat.send_message('❌ Отменено.')
        return

    # Валидация - только цифры
    if not text.isdigit():
        err = await update.message.chat.send_message('❗ Введите только цифры')
        await asyncio.sleep(3)
        try:
            await err.delete()
        except Exception:
            pass
        return

    value = int(text)

    if awaiting_months:
        # Валидация месяцев
        if value < 1 or value > 60:
            err = await update.message.chat.send_message('❗ Количество месяцев должно быть от 1 до 60')
            await asyncio.sleep(3)
            try:
                await err.delete()
            except Exception:
                pass
            return

        # Сохраняем месяцы и запрашиваем сумму
        context.user_data['temp_months'] = value
        context.user_data.pop(AWAITING_PAYMENT_MONTHS, None)
        context.user_data[AWAITING_PAYMENT_AMOUNT] = True

        # Удаляем старую инструкцию
        if instruction_message_id:
            try:
                await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            except Exception:
                pass

        # Запрашиваем сумму
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('❌ Отмена', callback_data='admin_sub_pay_add_value_cancel')]
        ])
        sent_message = await update.message.chat.send_message(
            f'➕ <b>Добавить значение</b>\n\n'
            f'Срок: <b>{value}</b> мес.\n'
            f'Теперь введите сумму в рублях:\n'
            f'<i>(от 1 до 100000)</i>',
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        try:
            context.user_data['add_value_instruction_message_id'] = sent_message.message_id
        except Exception:
            pass

    elif awaiting_amount:
        # Валидация суммы
        if value < 1 or value > 100000:
            err = await update.message.chat.send_message('❗ Сумма должна быть от 1 до 100000 рублей')
            await asyncio.sleep(3)
            try:
                await err.delete()
            except Exception:
                pass
            return

        months = context.user_data.get('temp_months')
        
        # Удаляем инструкцию
        if instruction_message_id:
            try:
                await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            except Exception:
                pass
            context.user_data.pop('add_value_instruction_message_id', None)

        # Сохраняем в БД
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                # Проверяем, существует ли уже такой вариант
                existing = await conn.fetchrow('SELECT id FROM payment_options WHERE months = $1', months)
                if existing:
                    # Обновляем существующий
                    await conn.execute('UPDATE payment_options SET amount = $1 WHERE months = $2', value, months)
                    message_text = f'✅ Обновил вариант оплаты: <b>{months}</b> мес. - <b>{value}</b>₽'
                else:
                    # Добавляем новый
                    await conn.execute('INSERT INTO payment_options (months, amount) VALUES ($1, $2)', months, value)
                    message_text = f'✅ Добавил новый вариант оплаты: <b>{months}</b> мес. - <b>{value}</b>₽'
        except Exception as e:
            await update.message.chat.send_message('❌ Ошибка при сохранении. Попробуйте позже.')
            return
        finally:
            # Очищаем состояние
            context.user_data.pop(AWAITING_PAYMENT_AMOUNT, None)
            context.user_data.pop('temp_months', None)
            context.user_data.pop('temp_amount', None)

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('✅ Спасибо', callback_data='admin_sub_pay_settings')]
        ])
        await update.message.chat.send_message(message_text, reply_markup=keyboard, parse_mode='HTML')


async def admin_sub_pay_edit_amount_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода новой суммы для изменения"""
    
    if not context.chat_data.get(AWAITING_PAYMENT_AMOUNT):
        return

    
    # Пытаемся сразу удалить сообщение пользователя
    try:
        await update.message.delete()
    except Exception:
        pass

    text = (update.message.text or '').strip()
    instruction_message_id = context.chat_data.get('edit_amount_instruction_message_id')
    
    # Отмена
    if text.lower() in {'отмена', 'cancel', 'стоп'}:
        if instruction_message_id:
            try:
                await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            except Exception as e:
                pass
        context.chat_data.pop('edit_amount_instruction_message_id', None)
        context.chat_data.pop(AWAITING_PAYMENT_AMOUNT, None)
        context.chat_data.pop('editing_existing', None)
        context.chat_data.pop('temp_months', None)
        await update.message.chat.send_message('❌ Отменено.')
        return

    # Валидация - только цифры
    if not text.isdigit():
        err = await update.message.chat.send_message('❗ Введите только цифры')
        await asyncio.sleep(3)
        try:
            await err.delete()
        except Exception:
            pass
        return

    new_amount = int(text)

    if new_amount < 1 or new_amount > 100000:
        err = await update.message.chat.send_message('❗ Сумма должна быть от 1 до 100000 рублей')
        await asyncio.sleep(3)
        try:
            await err.delete()
        except Exception:
            pass
        return

    # Изменяем сумму в БД
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute('UPDATE payment_options SET amount = $1 WHERE months = $2', new_amount, context.chat_data['temp_months'])
            message_text = f'✅ Изменена сумма варианта оплаты: <b>{context.chat_data["temp_months"]}</b> мес. - <b>{new_amount}</b>₽'
    except Exception as e:
        message_text = '❌ Ошибка при изменении суммы. Попробуйте позже.'
    finally:
        # Удаляем инструкцию в любом случае
        if instruction_message_id:
            try:
                await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            except Exception as e:
                pass
        else:
            pass
        context.chat_data.pop('edit_amount_instruction_message_id', None)
        
        # Очищаем состояние
        context.chat_data.pop(AWAITING_PAYMENT_AMOUNT, None)
        context.chat_data.pop('editing_existing', None)
        context.chat_data.pop('temp_months', None)

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Спасибо', callback_data='admin_sub_pay_list_values')]
    ])
    await update.message.chat.send_message(message_text, reply_markup=keyboard, parse_mode='HTML')

async def admin_payment_history_custom_period_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода кастомных дат для истории покупок"""
    
    # Проверяем, ожидаем ли мы ввод начальной или конечной даты
    awaiting_start = context.user_data.get(AWAITING_PAYMENT_HISTORY_PERIOD_START)
    awaiting_end = context.user_data.get(AWAITING_PAYMENT_HISTORY_PERIOD_END)
    
    if not awaiting_start and not awaiting_end:
        return

    # Пытаемся сразу удалить сообщение пользователя
    try:
        await update.message.delete()
    except Exception:
        pass

    text = (update.message.text or '').strip()
    instruction_message_id = context.user_data.get('payment_history_period_input_message_id')

    # Отмена
    if text.lower() in {'отмена', 'cancel', 'стоп'}:
        if instruction_message_id:
            try:
                await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            except Exception:
                pass
        context.user_data.pop('payment_history_period_input_message_id', None)
        context.user_data.pop(AWAITING_PAYMENT_HISTORY_PERIOD_START, None)
        context.user_data.pop(AWAITING_PAYMENT_HISTORY_PERIOD_END, None)
        context.user_data.pop('payment_history_period_start', None)
        await update.message.chat.send_message('❌ Отменено.')
        return

    # Валидация формата даты
    try:
        date_obj = datetime.strptime(text, '%d.%m.%Y')
    except ValueError:
        err = await update.message.chat.send_message('❗ Неверный формат даты. Введите в формате ДД.ММ.ГГГГ')
        await asyncio.sleep(3)
        try:
            await err.delete()
        except Exception:
            pass
        return

    if date_obj.date() > datetime.now().date():
        err = await update.message.chat.send_message('❗ Дата не может быть в будущем')
        await asyncio.sleep(3)
        try:
            await err.delete()
        except Exception:
            pass
        return

    if awaiting_start:
        # Сохраняем начальную дату и запрашиваем конечную
        context.user_data['payment_history_period_start'] = date_obj
        context.user_data.pop(AWAITING_PAYMENT_HISTORY_PERIOD_START, None)
        context.user_data[AWAITING_PAYMENT_HISTORY_PERIOD_END] = True

        # Удаляем старую инструкцию
        if instruction_message_id:
            try:
                await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            except Exception:
                pass

        # Запрашиваем конечную дату
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('❌ Отмена', callback_data='admin_payment_history_custom_period_cancel')]
        ])
        sent_message = await update.message.chat.send_message(
            f'✅ Начальная дата принята: <b>{date_obj.strftime("%d.%m.%Y")}</b>\n\n'
            f'➡️ Теперь введите <b>конечную</b> дату в формате <b>ДД.ММ.ГГГГ</b>:',
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        try:
            context.user_data['payment_history_period_input_message_id'] = sent_message.message_id
        except Exception:
            pass

    elif awaiting_end:
        # Проверяем, что конечная дата не раньше начальной
        start_date = context.user_data.get('payment_history_period_start')
        if start_date and date_obj < start_date:
            err = await update.message.chat.send_message('❗ Конечная дата не может быть раньше начальной')
            await asyncio.sleep(3)
            try:
                await err.delete()
            except Exception:
                pass
            return

        # Удаляем инструкцию
        if instruction_message_id:
            try:
                await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            except Exception:
                pass
            context.user_data.pop('payment_history_period_input_message_id', None)

        # Формируем период и экспортируем
        period = f"custom_{start_date.strftime('%Y-%m-%d')}_{date_obj.strftime('%Y-%m-%d')}"
        
        # Очищаем состояние
        context.user_data.pop(AWAITING_PAYMENT_HISTORY_PERIOD_END, None)
        context.user_data.pop('payment_history_period_start', None)

        # Экспортируем данные
        await export_payment_history_for_period(update, context, period, send_chat=update.message.chat)
        
        # После экспорта показываем меню выбора периода снова
        await update.message.chat.send_message(
            '📊 История покупок — выберите период:',
            reply_markup=get_admin_payment_history_menu()
        )


async def admin_sub_pay_edit_amount_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена изменения суммы"""
    query = update.callback_query
    await query.answer()
    context.user_data.pop('edit_amount_instruction_message_id', None)
    context.user_data.pop(AWAITING_PAYMENT_AMOUNT, None)
    context.user_data.pop('editing_existing', None)
    context.user_data.pop('temp_months', None)
    await query.edit_message_text('💰 <b>Настройки оплаты</b>\n\nЗдесь можно добавить срок и суммы оплаты подписки', reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton('➕ Добавить значение', callback_data='admin_sub_pay_add_value')],
        [InlineKeyboardButton('📋 Список значений', callback_data='admin_sub_pay_list_values')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_sub_pay')]
    ]), parse_mode='HTML')


def register_admin_sub_pay_handlers(application):
    """Регистрация обработчиков для управления подписками"""
    application.add_handler(CallbackQueryHandler(admin_sub_pay_callback, pattern='^admin_sub_pay$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_manage_callback, pattern='^admin_sub_pay_manage$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_settings_callback, pattern='^admin_sub_pay_settings$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_add_value_callback, pattern='^admin_sub_pay_add_value$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_add_value_cancel_callback, pattern='^admin_sub_pay_add_value_cancel$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_list_values_callback, pattern='^admin_sub_pay_list_values$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_payment_option_callback, pattern='^payment_option_'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_delete_option_callback, pattern='^admin_sub_pay_delete_option$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_delete_confirm_callback, pattern='^admin_sub_pay_delete_confirm$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_delete_cancel_callback, pattern='^admin_sub_pay_delete_cancel$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_edit_amount_callback, pattern='^admin_sub_pay_edit_amount$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_edit_amount_cancel_callback, pattern='^admin_sub_pay_edit_amount_cancel$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_extend_all_callback, pattern='^admin_sub_pay_extend_all$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_extend_all_cancel_callback, pattern='^admin_sub_pay_extend_all_cancel$'))
    application.add_handler(CallbackQueryHandler(admin_sub_pay_history_callback, pattern='^admin_sub_pay_history$'))
    application.add_handler(CallbackQueryHandler(admin_payment_history_period_callback, pattern='^admin_payment_history_(today|week|month)$'))
    application.add_handler(CallbackQueryHandler(admin_payment_history_custom_period_callback, pattern='^admin_payment_history_custom_period$'))
    application.add_handler(CallbackQueryHandler(admin_payment_history_custom_period_cancel_callback, pattern='^admin_payment_history_custom_period_cancel$'))
    application.add_handler(CallbackQueryHandler(admin_payment_history_thanks_callback, pattern='^admin_payment_history_thanks$'))
    # Изменяем приоритет на более высокий (меньшие номера групп)
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), admin_sub_pay_extend_all_message), group=1)
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), admin_sub_pay_add_value_message), group=2) 
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), admin_sub_pay_edit_amount_message), group=3) 
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), admin_payment_history_custom_period_message), group=4) 
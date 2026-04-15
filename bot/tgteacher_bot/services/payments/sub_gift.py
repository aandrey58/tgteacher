from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters
from tgteacher_bot.db.user_repo import get_pool
from datetime import datetime, timedelta
import asyncio
from math import ceil

# MCP: Состояния для управления подписками
AWAITING_SUBSCRIPTION_DAYS = 'awaiting_subscription_days'

async def gift_subscription_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки '🎁Подарить подписку'"""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем user_id из callback_data
    user_id = int(query.data.replace('admin_gift_subscription_', ''))
    context.user_data['gift_subscription_user_id'] = user_id
    
    # Сохраняем информацию о том, откуда пришли
    context.user_data['gift_subscription_source'] = query.data
    
    # Устанавливаем состояние ожидания ввода дней
    context.user_data[AWAITING_SUBSCRIPTION_DAYS] = True
    
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data='admin_gift_subscription_cancel')]
    ])
    
    # MCP: Сохраняем message_id инструкции для последующего удаления
    sent_message = await query.edit_message_text(
        '🎁 <b>Подарить подписку</b>\n\n'
        f'Введите количество дней для пользователя <b>{user_id}</b>:\n'
        '<i>(от 1 до 365 дней)</i>',
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    context.user_data['gift_subscription_instruction_message_id'] = sent_message.message_id

async def gift_subscription_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена подарка подписки"""
    query = update.callback_query
    await query.answer('❌ Операция отменена')
    
    # MCP: Получаем user_id до очистки контекста
    user_id = context.user_data.get('gift_subscription_user_id')
    
    # MCP: Удаляем сообщение-инструкцию, если есть message_id
    instruction_message_id = context.user_data.get('gift_subscription_instruction_message_id')
    if instruction_message_id:
        try:
            await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
        except Exception:
            pass
    
    # Чистим контекст
    clear_gift_subscription_context(context.user_data)
    
    # Возвращаемся к профилю пользователя
    if user_id:
        await return_to_user_profile(update, context, user_id)
    else:
        # Fallback - возвращаемся к меню пользователей
        from tgteacher_bot.handlers.admin.admin_users import get_admin_users_menu
        await query.edit_message_text('👤 Управление пользователями:', reply_markup=get_admin_users_menu())

async def gift_subscription_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ввода количества дней для подписки"""
    if not context.user_data.get(AWAITING_SUBSCRIPTION_DAYS):
        return
    
    try:
        await update.message.delete()
    except Exception:
        pass
    
    text = update.message.text.strip()
    user_id = context.user_data.get('gift_subscription_user_id')
    
    # Валидация ввода
    if not text.isdigit():
        # Проверяем, есть ли буквы или символы
        if any(char.isalpha() for char in text):
            error_msg = await update.message.chat.send_message(
                '❗ Запрещено вводить буквы. Введите только цифры от 1 до 365'
            )
        elif any(not char.isdigit() for char in text):
            error_msg = await update.message.chat.send_message(
                '❗ Запрещено вводить символы. Введите только цифры от 1 до 365'
            )
        else:
            error_msg = await update.message.chat.send_message(
                '❗ Введите только цифры от 1 до 365'
            )
        await asyncio.sleep(3)
        try:
            await error_msg.delete()
        except Exception:
            pass
        return
    
    days = int(text)
    if days < 1 or days > 365:
        error_msg = await update.message.chat.send_message(
            '❗ Количество дней должно быть от 1 до 365'
        )
        await asyncio.sleep(3)
        try:
            await error_msg.delete()
        except Exception:
            pass
        return
    
    # Сохраняем количество дней
    context.user_data['gift_subscription_days'] = days
    
    # MCP: Удаляем сообщение-инструкцию, если есть message_id
    instruction_message_id = context.user_data.get('gift_subscription_instruction_message_id')
    if instruction_message_id:
        try:
            await context.bot.delete_message(update.effective_chat.id, instruction_message_id)
            context.user_data.pop('gift_subscription_instruction_message_id', None)
        except Exception:
            pass
    
    # Показываем подтверждение
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да, подарить', callback_data=f'admin_gift_subscription_confirm_{user_id}_{days}')
            ,InlineKeyboardButton('❌ Нет, отмена', callback_data='admin_gift_subscription_cancel')
        ]
    ])
    
    await update.message.chat.send_message(
        f'🎁 <b>Подтверждение подарка подписки</b>\n\n'
        f'Вы хотите активировать подписку пользователю <b>{user_id}</b> на <b>{days}</b> дней?\n\n'
        f'<i>Это действие изменит статус подписки пользователя.</i>',
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    
    # Чистим состояние ожидания
    context.user_data.pop(AWAITING_SUBSCRIPTION_DAYS, None)

async def gift_subscription_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение подарка подписки"""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем данные из callback_data
    data = query.data.replace('admin_gift_subscription_confirm_', '')
    user_id, days = map(int, data.split('_'))
    
    try:
        # Обновляем подписку в базе данных
        pool = await get_pool()
        async with pool.acquire() as conn:
            # Получаем текущую информацию о пользователе
            user = await conn.fetchrow('SELECT is_subscribed, subscription_count, subscription_until FROM users WHERE user_id = $1', user_id)
            if not user:
                await query.edit_message_text('❌ Пользователь не найден')
                return
            
            # Вычисляем новую дату окончания подписки
            from datetime import timezone
            moscow_tz = timezone(timedelta(hours=3))
            now = datetime.now(moscow_tz)
            
            if user['is_subscribed']:
                current_end = user['subscription_until']
                if current_end and current_end > now:
                    new_end = current_end + timedelta(days=days)
                else:
                    new_end = now + timedelta(days=days)
            else:
                new_end = now + timedelta(days=days)
            
            # Обновляем данные пользователя
            await conn.execute('''
                UPDATE users 
                SET is_subscribed = TRUE, 
                    subscription_count = subscription_count + 1,
                    subscription_until = $2
                WHERE user_id = $1
            ''', user_id, new_end)

            # MCP: Гарантируем, что таблица payments существует
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
                    processed_at TIMESTAMP
                )
            ''')

            # MCP: Фиксируем запись об оплате как "подарок" (0 рублей)
            gifted_months = max(1, ceil(days / 30))
            gift_payment_id = f'gift_{user_id}_{int(datetime.now().timestamp())}'
            await conn.execute('''
                INSERT INTO payments (payment_id, user_id, months, amount, status, processed, created_at, updated_at, processed_at, result_subscription_until)
                VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW(), NOW(), $7)
            ''', gift_payment_id, user_id, gifted_months, 0, 'gift', True, new_end)
        
        # Показываем успешное сообщение
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('✅ Готово', callback_data=f'admin_user_{user_id}')]
        ])
        
        await query.edit_message_text(
            f'🎁 <b>Подписка успешно подарена!</b>\n\n'
            f'Пользователю <b>{user_id}</b> активирована подписка на <b>{days}</b> дней.\n'
            f'Дата окончания: <b>{new_end.strftime("%d.%m.%Y %H:%M")}</b>',
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
    except Exception as e:
        print(f'MCP ERROR: Ошибка при подарке подписки: {e}')
        await query.edit_message_text(
            '❌ Произошла ошибка при активации подписки. Попробуйте позже.',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_user_{user_id}')]
            ])
        )
    
    # Чистим контекст
    clear_gift_subscription_context(context.user_data)

async def remove_subscription_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки '❌ Удалить подписку'"""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем user_id из callback_data
    user_id = int(query.data.replace('admin_remove_subscription_', ''))
    
    # Показываем подтверждение
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да, удалить', callback_data=f'admin_remove_subscription_confirm_{user_id}'),
            InlineKeyboardButton('❌ Нет, отмена', callback_data=f'admin_user_{user_id}')
        ]
    ])
    
    await query.edit_message_text(
        f'❌ <b>Подтверждение удаления подписки</b>\n\n'
        f'Вы хотите деактивировать подписку пользователю <b>{user_id}</b>?\n\n'
        f'<i>Это действие отменит активную подписку пользователя.</i>',
        reply_markup=keyboard,
        parse_mode='HTML'
    )

async def remove_subscription_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение удаления подписки"""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем user_id из callback_data
    user_id = int(query.data.replace('admin_remove_subscription_confirm_', ''))
    
    try:
        # Обновляем подписку в базе данных
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute('''
                UPDATE users 
                SET is_subscribed = FALSE,
                    subscription_until = NULL
                WHERE user_id = $1
            ''', user_id)
        
        # Показываем успешное сообщение
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('✅ Готово', callback_data=f'admin_user_{user_id}')]
        ])
        
        await query.edit_message_text(
            f'❌ <b>Подписка успешно удалена!</b>\n\n'
            f'Пользователю <b>{user_id}</b> деактивирована подписка.',
            reply_markup=keyboard,
            parse_mode='HTML'
        )
        
    except Exception as e:
        print(f'MCP ERROR: Ошибка при удалении подписки: {e}')
        await query.edit_message_text(
            '❌ Произошла ошибка при деактивации подписки. Попробуйте позже.',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_user_{user_id}')]
            ])
        )

async def return_to_user_profile(update, context, user_id):
    """Возврат к профилю пользователя"""
    from tgteacher_bot.handlers.admin.admin_users import get_admin_user_profile
    
    text, keyboard = await get_admin_user_profile(user_id, context)
    
    # MCP: Всегда отправляем новое сообщение вместо редактирования
    if hasattr(update, 'callback_query') and update.callback_query:
        await update.callback_query.message.chat.send_message(text, reply_markup=keyboard, parse_mode='HTML')
    else:
        # Если нет callback_query, отправляем новое сообщение
        await update.message.chat.send_message(text, reply_markup=keyboard, parse_mode='HTML')

def clear_gift_subscription_context(user_data):
    """Очистка контекста подарка подписки"""
    for key in ['gift_subscription_user_id', 'gift_subscription_days', 'gift_subscription_source', AWAITING_SUBSCRIPTION_DAYS, 'gift_subscription_instruction_message_id']:
        user_data.pop(key, None)

def register_sub_gift_handlers(application):
    """Регистрация обработчиков управления подписками"""
    application.add_handler(CallbackQueryHandler(gift_subscription_callback, pattern='^admin_gift_subscription_\\d+$'))
    application.add_handler(CallbackQueryHandler(gift_subscription_cancel_callback, pattern='^admin_gift_subscription_cancel$'))
    application.add_handler(CallbackQueryHandler(gift_subscription_confirm_callback, pattern='^admin_gift_subscription_confirm_\\d+_\\d+$'))
    application.add_handler(CallbackQueryHandler(remove_subscription_callback, pattern='^admin_remove_subscription_\\d+$'))
    application.add_handler(CallbackQueryHandler(remove_subscription_confirm_callback, pattern='^admin_remove_subscription_confirm_\\d+$'))
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), gift_subscription_message), group=996) 
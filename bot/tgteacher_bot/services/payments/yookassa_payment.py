import os
import uuid
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from yookassa import Payment, Configuration
from yookassa.domain.request import PaymentRequest
from yookassa.domain.models import Amount, Receipt, ReceiptItem
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.db.user_repo import get_user_subscription_info_pg, extend_user_subscription_pg
from tgteacher_bot.handlers.admin.admin_status import track_metrics


class YooKassaPayment:
    """Класс для работы с ЮKassa"""
    
    def __init__(self):
        # Получаем настройки из переменных окружения
        self.shop_id = os.getenv('YOOKASSA_SHOP_ID')
        self.secret_key = os.getenv('YOOKASSA_SECRET_KEY')
        self.return_url = os.getenv('YOOKASSA_RETURN_URL', 'https://t.me/your_bot_username')
        
        if not self.shop_id or not self.secret_key:
            raise ValueError("YOOKASSA_SHOP_ID и YOOKASSA_SECRET_KEY должны быть установлены в переменных окружения")
        
        # Инициализируем ЮKassa
        Configuration.account_id = self.shop_id
        Configuration.secret_key = self.secret_key
    
    async def create_payment(self, user_id: int, months: int, amount: int, description: str = None) -> Dict[str, Any]:
        """
        Создает платеж в ЮKassa
        
        Args:
            user_id: ID пользователя в Telegram
            months: Количество месяцев подписки
            amount: Сумма в копейках (рубли * 100)
            description: Описание платежа
            
        Returns:
            Dict с информацией о платеже
        """
        try:
            # Генерируем уникальный ID платежа
            payment_id = str(uuid.uuid4())
            
            # Базовое описание + принудительное добавление UID
            base_description = description or f"Подписка на {months} мес."
            final_description = f"{base_description} | ID {user_id}"
            
            # Создаем чек для фискализации
            receipt = Receipt(
                customer={
                    "email": f"user_{user_id}@telegram.local"  # Заглушка для email
                },
                items=[
                    ReceiptItem(
                        description=f"Подписка на {months} мес. | ID {user_id}",
                        quantity=1,
                        amount=Amount(
                            value=str(amount / 100),  # Конвертируем копейки в рубли
                            currency="RUB"
                        ),
                        vat_code=1,  # НДС 20%
                        payment_subject="service",  # Услуга
                        payment_mode="full_payment"  # Полная предварительная оплата
                    )
                ]
            )
            
            # Создаем запрос на платеж
            payment_request = PaymentRequest(
                amount=Amount(
                    value=str(amount / 100),  # Конвертируем копейки в рубли
                    currency="RUB"
                ),
                confirmation={
                    "type": "redirect",
                    "return_url": self.return_url
                },
                capture=True,  # Автоматическое списание
                description=final_description,
                receipt=receipt,
                metadata={
                    "user_id": str(user_id),
                    "months": str(months),
                    "payment_type": "subscription"
                }
            )
            
            # Создаем платеж
            payment = Payment.create(payment_request)
            
            # НЕ сохраняем в БД сразу - только после успешной оплаты
            
            return {
                "payment_id": payment.id,
                "status": payment.status,
                "payment_url": payment.confirmation.confirmation_url,
                "amount": amount,
                "months": months
            }
            
        except Exception as e:
            print(f"Ошибка создания платежа: {e}")
            raise
    
    async def check_payment_status(self, payment_id: str) -> Dict[str, Any]:
        """
        Проверяет статус платежа
        
        Args:
            payment_id: ID платежа в ЮKassa
            
        Returns:
            Dict с информацией о статусе платежа
        """
        try:
            payment = Payment.find_one(payment_id)
            
            # Обновляем статус в БД
            await self._update_payment_status(payment_id, payment.status)
            
            # Если платеж успешен, активируем подписку
            if payment.status == "succeeded":
                await self._activate_subscription(payment_id)
            
            return {
                "payment_id": payment.id,
                "status": payment.status,
                "amount": payment.amount.value,
                "currency": payment.amount.currency,
                "metadata": payment.metadata
            }
            
        except Exception as e:
            print(f"Ошибка проверки статуса платежа: {e}")
            raise
    
    async def _save_payment_to_db(self, payment_id: str, user_id: int, months: int, 
                                 amount: int, status: str, payment_url: str):
        """Сохраняет информацию о платеже в БД"""
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute('''
                    INSERT INTO payments (payment_id, user_id, months, amount, status, payment_url, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, NOW())
                ''', payment_id, user_id, months, amount, status, payment_url)
        except Exception as e:
            print(f"Ошибка сохранения платежа в БД: {e}")
            raise
    
    async def _update_payment_status(self, payment_id: str, status: str):
        """Обновляет статус платежа в БД"""
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.execute('''
                    UPDATE payments SET status = $1, updated_at = NOW()
                    WHERE payment_id = $2
                ''', status, payment_id)
        except Exception as e:
            print(f"Ошибка обновления статуса платежа: {e}")
            raise
    
    async def _activate_subscription(self, payment_id: str):
        """Активирует подписку после успешной оплаты"""
        try:
            # Получаем информацию о платеже из YooKassa
            payment = Payment.find_one(payment_id)
            
            # Извлекаем данные из metadata
            user_id = int(payment.metadata.get('user_id'))
            months = int(payment.metadata.get('months'))
            amount = int(float(payment.amount.value) * 100)  # Конвертируем рубли в копейки
            
            pool = await get_pool()
            async with pool.acquire() as conn:
                # Создаем запись о платеже в БД только после успешной оплаты
                payment_url = payment.confirmation.confirmation_url if payment.confirmation else None
                await conn.execute('''
                    INSERT INTO payments (payment_id, user_id, months, amount, status, payment_url, processed, created_at, updated_at, processed_at)
                    VALUES ($1, $2, $3, $4, $5, $6, true, NOW(), NOW(), NOW())
                ''', payment_id, user_id, months, amount, payment.status, payment_url)
                
                # Продлеваем подписку пользователя
                await extend_user_subscription_pg(user_id, months)
                
                # Читаем итоговую дату подписки из users
                result_until = await conn.fetchval('SELECT subscription_until FROM users WHERE user_id = $1', user_id)
                
                # Обновляем запись с итоговой датой подписки
                await conn.execute('''
                    UPDATE payments 
                    SET result_subscription_until = $2
                    WHERE payment_id = $1
                ''', payment_id, result_until)
                
                print(f"Подписка активирована для пользователя {user_id} на {months} месяцев")
                    
        except Exception as e:
            print(f"Ошибка активации подписки: {e}")
            raise


# Создаем глобальный экземпляр
yookassa_payment = None

def get_yookassa_payment() -> YooKassaPayment:
    """Получает экземпляр YooKassaPayment"""
    global yookassa_payment
    if yookassa_payment is None:
        yookassa_payment = YooKassaPayment()
    return yookassa_payment


@track_metrics
async def create_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик создания платежа"""
    query = update.callback_query
    await query.answer()
    
    # Парсим callback_data: confirm_payment_{months}_{amount}
    callback_data = query.data
    parts = callback_data.split('_')
    
    if len(parts) != 4:
        await query.edit_message_text('❌ Ошибка: неверный формат данных')
        return
    
    try:
        months = int(parts[2])
        amount = int(parts[3])  # Сумма в рублях
        user_id = query.from_user.id
        
        # Конвертируем рубли в копейки для ЮKassa
        amount_kopecks = amount * 100
        
        # Создаем платеж
        payment_system = get_yookassa_payment()
        payment_info = await payment_system.create_payment(
            user_id=user_id,
            months=months,
            amount=amount_kopecks,
            description=f"Подписка на {months} месяцев"
        )
        
        # Создаем кнопку для оплаты
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('💳 Оплатить', url=payment_info['payment_url'])],
            [InlineKeyboardButton('🔄 Проверить оплату', callback_data=f'check_payment_{payment_info["payment_id"]}')],
            [InlineKeyboardButton('⬅️ Назад', callback_data='buy_subscription')]
        ])
        
        text = f'💳 <b>Оплата подписки</b>\n\n'
        text += f'Сумма к оплате: <b>{amount}₽</b>\n'
        text += f'Срок подписки: <b>{months} мес.</b>\n\n'
        text += f'Нажмите кнопку "Оплатить" для перехода к оплате.\n'
        text += f'После оплаты нажмите "Проверить оплату".'
        
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')
        
    except Exception as e:
        print(f"Ошибка создания платежа: {e}")
        text = '❌ <b>Ошибка создания платежа</b>\n\nНе удалось создать платеж. Попробуйте позже.'
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ Назад', callback_data='buy_subscription')]
        ])
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


@track_metrics
async def check_payment_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик проверки статуса платежа"""
    query = update.callback_query
    await query.answer()
    
    # Парсим callback_data: check_payment_{payment_id}
    callback_data = query.data
    parts = callback_data.split('_')
    
    if len(parts) != 3:
        await query.edit_message_text('❌ Ошибка: неверный формат данных')
        return
    
    payment_id = parts[2]
    
    try:
        # Проверяем статус платежа
        payment_system = get_yookassa_payment()
        payment_status = await payment_system.check_payment_status(payment_id)
        
        if payment_status['status'] == 'succeeded':
            # Платеж успешен
            text = '✅ <b>Оплата прошла успешно!</b>\n\n'
            text += f'Ваша подписка активирована.\n'
            text += f'Спасибо за покупку!'
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('💎 Моя подписка', callback_data='subscription')],
                [InlineKeyboardButton('🏠 Главное меню', callback_data='main_menu')]
            ])
            
        elif payment_status['status'] == 'pending':
            # Платеж в обработке
            text = '⏳ <b>Платеж в обработке</b>\n\n'
            text += f'Ваш платеж обрабатывается.\n'
            text += f'Попробуйте проверить статус через несколько минут.'
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('🔄 Проверить снова', callback_data=f'check_payment_{payment_id}')],
                [InlineKeyboardButton('⬅️ Назад', callback_data='buy_subscription')]
            ])
            
        else:
            # Платеж не прошел
            text = '❌ <b>Платеж не прошел</b>\n\n'
            text += f'Статус: {payment_status["status"]}\n'
            text += f'Попробуйте оплатить снова.'
            
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('💳 Попробовать снова', callback_data='buy_subscription')],
                [InlineKeyboardButton('⬅️ Назад', callback_data='subscription')]
            ])
        
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')
        
    except Exception as e:
        print(f"Ошибка проверки платежа: {e}")
        text = '❌ <b>Ошибка проверки платежа</b>\n\nНе удалось проверить статус платежа. Попробуйте позже.'
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('🔄 Попробовать снова', callback_data=f'check_payment_{payment_id}')],
            [InlineKeyboardButton('⬅️ Назад', callback_data='buy_subscription')]
        ])
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')


async def setup_payment_table():
    """Создает таблицу для хранения платежей"""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
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
            # Безопасная миграция на случай старой таблицы без колонки
            await conn.execute('''
                ALTER TABLE payments
                ADD COLUMN IF NOT EXISTS result_subscription_until TIMESTAMP WITH TIME ZONE
            ''')
            print("Таблица payments создана/проверена")
    except Exception as e:
        print(f"Ошибка создания таблицы payments: {e}")
        raise 
"""YooKassa adapter with durable local orders and conservative unknown outcomes."""
import asyncio
import os
import logging
from urllib.parse import urlsplit
from yookassa import Payment, Configuration
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from tgteacher_bot.db import payment_orders as orders
from tgteacher_bot.db.pool import get_pool
from tgteacher_bot.handlers.admin.admin_status import track_metrics

logger=logging.getLogger(__name__)

def creation_enabled():
    # Do not silently invent fiscal data. External fiscalization must be approved by merchant.
    return os.getenv("TGTEACHER_YOOKASSA_CREATE","0")=="1" and os.getenv("TGTEACHER_YOOKASSA_RECEIPT_MODE")=="external"

class YooKassaPayment:
    def __init__(self):
        self.shop_id=os.getenv("YOOKASSA_SHOP_ID")
        self.secret_key=os.getenv("YOOKASSA_SECRET_KEY")
        self.return_url=os.getenv("YOOKASSA_RETURN_URL","")
        if not self.shop_id or not self.secret_key:
            raise ValueError("YooKassa credentials required")
        Configuration.configure(self.shop_id,self.secret_key)

    async def create_payment(self,user_id,order_id):
        if not creation_enabled():
            raise orders.PaymentConflict("New payments disabled")
        parsed=urlsplit(self.return_url)
        if parsed.scheme!="https" or not parsed.hostname or parsed.username or parsed.password:
            raise orders.PaymentConflict("HTTPS return URL required")
        row,claimed=await orders.claim(user_id,order_id)
        if not claimed:
            return row
        amount=row["amount"]
        payload={"amount":{"value":f"{amount//100}.{amount%100:02d}","currency":"RUB"},
                 "confirmation":{"type":"redirect","return_url":self.return_url},"capture":True,
                 "description":f"Подписка на {row['months']} мес. | ID {user_id}",
                 "metadata":{"order_id":str(row["id"]),"user_id":str(user_id),"months":str(row["months"]),"payment_type":"subscription"}}
        try:
            # SDK is synchronous. A timeout cannot prove the upstream request failed.
            payment=await asyncio.wait_for(asyncio.to_thread(Payment.create,payload,str(row["id"])),25)
            row=await orders.bind_created(user_id,order_id,payment)
        except Exception:
            await orders.mark_uncertain(user_id,order_id)
            logger.warning("Payment creation needs reconciliation; order=%s",row["id"])
            raise
        if payment.status=="succeeded":
            return await orders.settle(user_id,order_id,payment)
        return row

    async def check_payment_status(self,user_id,order_id):
        row=await orders.get_order(user_id,order_id)
        if row["state"] in ("succeeded","canceled") or not row["provider_id"]:
            return row
        payment=await asyncio.wait_for(asyncio.to_thread(Payment.find_one,row["provider_id"]),25)
        return await orders.settle(user_id,order_id,payment)

yookassa_payment=None

def get_yookassa_payment():
    global yookassa_payment
    if yookassa_payment is None:
        yookassa_payment=YooKassaPayment()
    return yookassa_payment

def private_owner(update):
    user=update.effective_user
    chat=update.effective_chat
    return bool(user and chat and chat.type=="private" and chat.id==user.id)

async def show_order(query,row):
    oid=str(row["id"])
    state=row["state"]
    if state=="succeeded":
        text="✅ Оплата подтверждена, подписка была начислена."
        buttons=[[InlineKeyboardButton("Моя подписка",callback_data="subscription")]]
    elif state=="canceled":
        text="Платёж отменён. Можно выбрать новый тариф."
        buttons=[[InlineKeyboardButton("Выбрать тариф",callback_data="buy_subscription")]]
    elif state in ("pending","waiting_for_capture"):
        text=f"Заказ: {oid}\nСрок: {row['months']} мес.\nСумма: {row['amount']/100:.2f} ₽\nПлатёж ожидает подтверждения."
        buttons=[]
        if row["payment_url"] and state=="pending":
            buttons.append([InlineKeyboardButton("Оплатить",url=row["payment_url"])])
        buttons.append([InlineKeyboardButton("Проверить оплату",callback_data="check_payment_"+oid)])
    elif state in ("creating","uncertain"):
        text=f"Заказ {oid}: результат создания не подтверждён. Не оплачивайте повторно. Обратитесь в поддержку с номером заказа."
        buttons=[[InlineKeyboardButton("Поддержка",callback_data="help")],[InlineKeyboardButton("Проверить заказ",callback_data="check_payment_"+oid)]]
    else:
        text="Заказ ещё не подтверждён."
        buttons=[[InlineKeyboardButton("Подтвердить",callback_data="confirm_payment_"+oid)]]
    buttons.append([InlineKeyboardButton("Назад",callback_data="subscription")])
    await query.edit_message_text(text,reply_markup=InlineKeyboardMarkup(buttons))

async def show_error(query,order_id=None):
    buttons=[]
    if order_id:
        buttons.append([InlineKeyboardButton("Проверить заказ",callback_data="check_payment_"+str(order_id))])
    buttons.extend([[InlineKeyboardButton("Поддержка",callback_data="help")],[InlineKeyboardButton("К подписке",callback_data="subscription")]])
    await query.edit_message_text("Не удалось подтвердить операцию. Если платёж уже был создан, не оплачивайте повторно; проверьте заказ или обратитесь в поддержку.",reply_markup=InlineKeyboardMarkup(buttons))

@track_metrics
async def create_payment_callback(update,context):
    query=update.callback_query
    if not private_owner(update):
        await query.answer("Оплата доступна только в личном чате.",show_alert=True)
        return
    await query.answer()
    oid=None
    try:
        oid=orders.ident(query.data.removeprefix("confirm_payment_"))
        row=await get_yookassa_payment().create_payment(query.from_user.id,oid)
        await show_order(query,row)
    except Exception:
        await show_error(query,oid)

@track_metrics
async def check_payment_callback(update,context):
    query=update.callback_query
    if not private_owner(update):
        await query.answer("Проверка доступна только в личном чате.",show_alert=True)
        return
    await query.answer()
    oid=None
    try:
        oid=orders.ident(query.data.removeprefix("check_payment_"))
        row=await orders.get_order(query.from_user.id,oid)
        if row["provider_id"] and row["state"] not in ("succeeded","canceled"):
            row=await get_yookassa_payment().check_payment_status(query.from_user.id,oid)
        await show_order(query,row)
    except Exception:
        await show_error(query,oid)

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
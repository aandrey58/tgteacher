"""Telegram Stars one-off VIP payments."""
import asyncio
import logging
import os
from telegram import InlineKeyboardButton,InlineKeyboardMarkup,LabeledPrice
from telegram.ext import CallbackQueryHandler,CommandHandler,MessageHandler,PreCheckoutQueryHandler,ApplicationHandlerStop,filters
from tgteacher_bot.db import stars_repo as repo
from tgteacher_bot.services.payments.yookassa_payment import private_owner

logger=logging.getLogger(__name__)

def enabled():
    return os.getenv("TGTEACHER_STARS_ENABLED","0")=="1"

async def choose(update,context):
    query=update.callback_query
    await query.answer()
    if not private_owner(update) or not enabled():
        return
    try:
        months,amount=map(int,query.data.removeprefix("stars_buy_").split("_"))
        payload=await repo.prepare(query.from_user.id,months,amount)
        await context.bot.send_invoice(chat_id=query.from_user.id,title=f"VIP на {months} мес.",
            description=f"Доступ к учебному контенту на {months*30} дней. Разовая покупка.",
            payload=payload,provider_token="",currency="XTR",prices=[LabeledPrice("VIP",amount)],
            start_parameter="vip-"+payload[7:])
    except Exception:
        await query.edit_message_text("Не удалось подготовить счёт. Выберите актуальный тариф.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("К тарифам",callback_data="buy_subscription")]]))

async def pre_checkout(update,context):
    q=update.pre_checkout_query
    try:
        if not enabled():
            raise repo.StarsConflict("Purchases disabled")
        await asyncio.wait_for(repo.authorize(q.from_user.id,q.invoice_payload,q.currency,q.total_amount,q.id),5)
    except Exception:
        await q.answer(ok=False,error_message="Счёт устарел или недоступен. Выберите тариф заново.")
    else:
        await q.answer(ok=True)
    raise ApplicationHandlerStop

async def successful_payment(update,context):
    m=update.message
    p=m.successful_payment
    try:
        if not private_owner(update):
            raise repo.StarsConflict("Private chat required")
        row=await repo.settle(update.effective_user.id,p.invoice_payload,p.currency,p.total_amount,p.telegram_payment_charge_id)
    except Exception:
        logger.error("Stars settlement requires operator reconciliation")
        await m.reply_text("Платёж получен Telegram, но начисление не подтверждено. Не платите повторно. Обратитесь /paysupport.")
    else:
        await m.reply_text("✅ Оплата подтверждена. VIP начислен." if row["state"]=="paid" else "По заказу зарегистрирован возврат. Обратитесь /paysupport.")
    raise ApplicationHandlerStop

async def refunded_payment(update,context):
    m=update.message
    p=m.refunded_payment
    try:
        if not private_owner(update):
            raise repo.StarsConflict("Private chat required")
        await repo.observe_refund(update.effective_user.id,p.invoice_payload,p.currency,p.total_amount,p.telegram_payment_charge_id)
    except Exception:
        logger.error("Stars refund requires operator reconciliation")
        await m.reply_text("Не удалось сверить возврат. Обратитесь /paysupport.")
    else:
        await m.reply_text("Возврат зафиксирован. Вопрос об остатке VIP решается поддержкой.")
    raise ApplicationHandlerStop

async def paysupport(update,context):
    if private_owner(update):
        await update.effective_message.reply_text("По вопросам оплаты и возвратов обратитесь в поддержку. Укажите дату, сумму и номер транзакции, не отправляйте пароли.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Поддержка",callback_data="help")]]))

class RefundedFilter(filters.MessageFilter):
    def filter(self,message):
        return getattr(message,"refunded_payment",None) is not None

def register(application):
    application.add_handler(CallbackQueryHandler(choose,pattern=r"^stars_buy_[0-9]+_[0-9]+$"))
    application.add_handler(CommandHandler("paysupport",paysupport))
    application.add_handler(PreCheckoutQueryHandler(pre_checkout),group=-5)
    application.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT,successful_payment),group=-5)
    application.add_handler(MessageHandler(RefundedFilter(),refunded_payment),group=-5)

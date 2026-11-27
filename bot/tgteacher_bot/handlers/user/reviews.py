"""Private Telegram recall flow. No user ID from callback data is trusted."""
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackQueryHandler, CommandHandler
from tgteacher_bot.db import srs_repo as srs, learner_oauth

MENU = InlineKeyboardMarkup([[InlineKeyboardButton("🔁 Следующее слово",callback_data="srs_next")],
                             [InlineKeyboardButton("🏠 Главное меню",callback_data="main_menu")]])


async def _private(update):
    if update.effective_chat and update.effective_chat.type == "private" and update.effective_user and update.effective_chat.id == update.effective_user.id:
        return True
    if update.callback_query:
        await update.callback_query.answer("Откройте личный чат с ботом.",show_alert=True)
    elif update.effective_message:
        await update.effective_message.reply_text("Откройте личный чат с ботом.")
    return False


async def _display(update,text,markup):
    if update.callback_query:
        await update.callback_query.edit_message_text(text,reply_markup=markup)
    else:
        await update.effective_message.reply_text(text,reply_markup=markup)


async def start_review(update,context):
    if not await _private(update):
        return
    if update.callback_query:
        await update.callback_query.answer()
    # Avoid consuming unrelated free-text answers while reviewing.
    for n in range(1,9):
        context.user_data.pop(f"stage{n}",None)
    card = await srs.next_review(update.effective_user.id)
    if card is None:
        await _display(update,"На данный момент нет слов для повторения. Изучайте новые слова или возвращайтесь позже.",
                       InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главное меню",callback_data="main_menu")]]))
        return
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("Показать перевод",callback_data="srs_reveal_"+card["nonce"])],
                                   [InlineKeyboardButton("🏠 Главное меню",callback_data="main_menu")]])
    await _display(update,f"🔁 Повторение · {card['family_name']}\n\n{card['word']}\n\nВспомните перевод, затем откройте ответ.",markup)


async def reveal(update,context):
    if not await _private(update):
        return
    query = update.callback_query
    nonce = query.data.removeprefix("srs_reveal_")
    try:
        card = await srs.reveal_review(query.from_user.id,nonce)
    except ValueError:
        await query.answer("Карточка устарела. Начните повторение заново.",show_alert=True)
        return
    await query.answer()
    labels = ("1 · Не вспомнил","2 · С трудом","3 · Вспомнил","4 · Легко")
    markup = InlineKeyboardMarkup([[InlineKeyboardButton(labels[i],callback_data=f"srs_rate_{nonce}_{i+1}") for i in (0,1)],
                                   [InlineKeyboardButton(labels[i],callback_data=f"srs_rate_{nonce}_{i+1}") for i in (2,3)],
                                   [InlineKeyboardButton("🏠 Главное меню",callback_data="main_menu")]])
    await _display(update,f"{card['word']}\n\nПеревод: {card['translation']}\n\nОцените, насколько хорошо вы вспомнили слово.",markup)


async def rate(update,context):
    if not await _private(update):
        return
    query = update.callback_query
    nonce,rating = query.data.removeprefix("srs_rate_").rsplit("_",1)
    try:
        result = await srs.rate_review(query.from_user.id,nonce,int(rating))
    except ValueError:
        await query.answer("Карточка уже оценена или устарела. Начните повторение заново.",show_alert=True)
        return
    await query.answer()
    goal = result["daily_review_goal"]
    count = result["reviewed_today"]
    progress = f"\n\nСегодня: {count}/{goal} повторений (UTC)."
    if count >= goal:
        progress += "\nДневная цель выполнена! Можно продолжить по желанию."
    await _display(update,"Оценка сохранена. Следующее повторение: "+result["due"][:16].replace("T"," ")+" UTC."+progress,MENU)


async def issue_token(update,context):
    if not await _private(update):
        return
    try:
        token = await srs.issue_token(update.effective_user.id)
    except ValueError:
        await update.effective_message.reply_text("Сначала выполните /start.")
        return
    await update.effective_message.reply_text("Персональный MCP-токен (24 часа). Не пересылайте его другим. Новый токен отменяет предыдущий. Для отзыва: /review_revoke.\n\n"+token)


async def revoke_token(update,context):
    if await _private(update):
        await srs.revoke_token(update.effective_user.id)
        await update.effective_message.reply_text("Персональный MCP-токен отозван.")


async def confirm_oauth_link(update, context):
    if not await _private(update):
        return
    if not learner_oauth.enabled():
        await update.effective_message.reply_text("Привязка OAuth пока отключена.")
        return
    if len(context.args or []) != 1:
        await update.effective_message.reply_text("Выполните команду /review_link с одноразовым кодом из вашей собственной OAuth-сессии. Не используйте чужие коды.")
        return
    ok = await learner_oauth.confirm(update.effective_user.id, context.args[0])
    await update.effective_message.reply_text("OAuth-аккаунт привязан. Отвязать: /review_unlink." if ok else
        "Привязка не выполнена: код недействителен, истёк или аккаунт уже привязан.")


async def unlink_oauth(update, context):
    if await _private(update):
        await learner_oauth.unlink(update.effective_user.id)
        await update.effective_message.reply_text("Привязка OAuth удалена. Персональный токен /review_token управляется отдельно.")


def register_review_handlers(application):
    application.add_handler(CommandHandler("review_link",confirm_oauth_link))
    application.add_handler(CommandHandler("review_unlink",unlink_oauth))
    application.add_handler(CommandHandler("review",start_review))
    application.add_handler(CommandHandler("review_token",issue_token))
    application.add_handler(CommandHandler("review_revoke",revoke_token))
    application.add_handler(CallbackQueryHandler(start_review,pattern=r"^srs_next$"))
    application.add_handler(CallbackQueryHandler(reveal,pattern=r"^srs_reveal_[a-f0-9]{32}$"))
    application.add_handler(CallbackQueryHandler(rate,pattern=r"^srs_rate_[a-f0-9]{32}_[1-4]$"))

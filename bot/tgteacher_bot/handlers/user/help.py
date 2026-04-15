from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters
import os
from datetime import datetime, timedelta
import logging
import json
from tgteacher_bot.db.user_repo import mark_user_active_if_needed
from tgteacher_bot.core import paths

# Ключи для user_data/bot_data
AWAITING_HELP_QUESTION = 'awaiting_help_question'
HELP_PROMPT_MESSAGE_ID = 'help_prompt_message_id'
HELP_QUESTIONS_INDEX = 'help_questions_index'
HELP_INDEX_LOADED_FLAG = 'help_index_loaded'
HELP_INDEX_FILE_PATH = 'help_questions_index.json'
MAX_HELP_INDEX_SIZE = 5000  # Оптимизация: ограничение размера индекса помощи

logger = logging.getLogger(__name__)

# Читаем ID группы для вопросов из окружения
# Должен быть числом (chat_id группы). Пример: -1001234567890
HELP_GROUP_ID_ENV = 'HELP_QUESTIONS_GROUP_ID'

# Файл для персистентного хранения индекса вопросов
HELP_INDEX_FILE_PATH = str(paths.help_index_path())

def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    for fmt in ('%d:%m:%Y %H:%M:%S', '%d.%m.%Y %H:%M'):
        try:
            return datetime.strptime(value, fmt)
        except Exception:
            continue
    return None

def cleanup_help_index(context: ContextTypes.DEFAULT_TYPE, max_age_days: int = 30) -> int:
    """Удаляет записи старше max_age_days из индекса. Возвращает количество удалённых записей."""
    try:
        index = context.application.bot_data.get(HELP_QUESTIONS_INDEX, {})
        if not index:
            return 0
        threshold = datetime.now() - timedelta(days=max_age_days)
        to_delete: list[int] = []
        for msg_id, data in list(index.items()):
            asked_at_str = (data or {}).get('asked_at')
            asked_dt = _parse_datetime(asked_at_str)
            if asked_dt is None:
                continue
            if asked_dt < threshold:
                to_delete.append(msg_id)
        for msg_id in to_delete:
            index.pop(msg_id, None)
        if to_delete:
            logger.info("HELP: cleanup removed %d old entries (>%d days)", len(to_delete), max_age_days)
            save_help_index_to_disk(context)
        return len(to_delete)
    except Exception as e:
        logger.exception("HELP: failed to cleanup index: %s", e)
        return 0

def load_help_index_from_disk(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Загружает индекс вопросов из JSON-файла в bot_data один раз за сессию."""
    try:
        if context.application.bot_data.get(HELP_INDEX_LOADED_FLAG):
            return
        if not os.path.exists(HELP_INDEX_FILE_PATH):
            context.application.bot_data[HELP_QUESTIONS_INDEX] = context.application.bot_data.get(HELP_QUESTIONS_INDEX, {})
            context.application.bot_data[HELP_INDEX_LOADED_FLAG] = True
            logger.info("HELP: index file not found, starting with empty index")
            return
        with open(HELP_INDEX_FILE_PATH, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        loaded_index = {}
        for key_str, value in raw.items():
            try:
                loaded_index[int(key_str)] = value
            except Exception:
                # Пропускаем кривые ключи
                continue
        
        # Оптимизация: ограничение размера индекса
        if len(loaded_index) > MAX_HELP_INDEX_SIZE:
            # Оставляем только самые новые записи
            sorted_items = sorted(loaded_index.items(), key=lambda x: x[1].get('timestamp', 0), reverse=True)
            loaded_index = dict(sorted_items[:MAX_HELP_INDEX_SIZE])
            logger.info(f"HELP: ограничен размер индекса до {MAX_HELP_INDEX_SIZE} записей")
        
        context.application.bot_data[HELP_QUESTIONS_INDEX] = loaded_index
        context.application.bot_data[HELP_INDEX_LOADED_FLAG] = True
        logger.info("HELP: loaded index from disk: path=%s size=%d", HELP_INDEX_FILE_PATH, len(loaded_index))
    except Exception as e:
        logger.exception("HELP: failed to load index from disk: %s", e)

def save_help_index_to_disk(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Сохраняет текущий индекс вопросов из bot_data в JSON-файл."""
    try:
        index = context.application.bot_data.get(HELP_QUESTIONS_INDEX, {})
        serializable = {str(k): v for k, v in index.items()}
        with open(HELP_INDEX_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
        logger.info("HELP: saved index to disk: path=%s size=%d", HELP_INDEX_FILE_PATH, len(index))
    except Exception as e:
        logger.exception("HELP: failed to save index to disk: %s", e)

async def cleanup_help_index_job(context: ContextTypes.DEFAULT_TYPE):
    """Задача для планировщика: чистит индекс вопросов от записей старше 30 дней."""
    try:
        # Подгружаем индекс, затем чистим
        load_help_index_from_disk(context)
        removed = cleanup_help_index(context, max_age_days=30)
        logger.info("HELP: scheduled cleanup executed, removed=%d entries", removed)
    except Exception as e:
        logger.exception("HELP: scheduled cleanup failed: %s", e)

def get_help_menu():
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('📖 FAQ', callback_data='help_faq')],
        [InlineKeyboardButton('✉️ Задать вопрос', callback_data='help_ask')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='main_menu')],
    ])
    return keyboard

async def help_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    try:
        await query.edit_message_text('🆘 Помощь', reply_markup=get_help_menu())
    except Exception:
        await query.message.reply_text('🆘 Помощь', reply_markup=get_help_menu())

async def help_faq_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await mark_user_active_if_needed(update.effective_user.id, context)
    text = (
        '📖 <b>FAQ</b>\n\n'
        '— Здесь будет список часто задаваемых вопросов и ответов.\n\n'
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✉️ Задать вопрос', callback_data='help_ask')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='help')],
    ])
    try:
        await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')
    except Exception:
        await query.message.reply_text(text, reply_markup=keyboard, parse_mode='HTML')

async def help_ask_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await mark_user_active_if_needed(update.effective_user.id, context)
    user = query.from_user
    logger.info("HELP: help_ask_callback: user_id=%s chat_id=%s", user.id, query.message.chat_id if query.message else None)
    context.user_data[AWAITING_HELP_QUESTION] = True
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('❌ Отмена', callback_data='help_cancel_ask')]
    ])
    try:
        sent = await query.edit_message_text('✍️ Введите Ваш запрос, Вам скоро ответят', reply_markup=keyboard)
    except Exception:
        sent = await query.message.reply_text('✍️ Введите Ваш запрос, Вам скоро ответят', reply_markup=keyboard)
    context.user_data[HELP_PROMPT_MESSAGE_ID] = sent.message_id
    logger.info("HELP: help_ask_callback: prompt_message_id=%s set awaiting", sent.message_id)

async def help_cancel_ask_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await mark_user_active_if_needed(update.effective_user.id, context)
    logger.info("HELP: help_cancel_ask_callback: chat_id=%s", query.message.chat_id if query.message else None)
    context.user_data.pop(AWAITING_HELP_QUESTION, None)
    prompt_id = context.user_data.pop(HELP_PROMPT_MESSAGE_ID, None)
    if prompt_id:
        try:
            await context.bot.delete_message(query.message.chat_id, prompt_id)
            logger.info("HELP: deleted prompt_message_id=%s", prompt_id)
        except Exception as e:
            logger.exception("HELP: failed to delete prompt_message_id=%s: %s", prompt_id, e)
    try:
        await query.edit_message_text('🆘 Помощь', reply_markup=get_help_menu())
    except Exception:
        await query.message.reply_text('🆘 Помощь', reply_markup=get_help_menu())

async def help_ok_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await mark_user_active_if_needed(update.effective_user.id, context)
    try:
        await query.edit_message_text('🆘 Помощь', reply_markup=get_help_menu())
    except Exception:
        await query.message.reply_text('🆘 Помощь', reply_markup=get_help_menu())

async def help_user_ok_delete_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await mark_user_active_if_needed(update.effective_user.id, context)
    try:
        await query.message.delete()
        logger.info("HELP: user ok deleted notification message_id=%s", query.message.message_id)
    except Exception as e:
        logger.exception("HELP: failed to delete user ok message: %s", e)

async def help_user_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get(AWAITING_HELP_QUESTION):
        # Логируем пропуски для отладки порядка хендлеров
        try:
            logger.debug("HELP: skip text handler: awaiting flag is False; msg=%r", getattr(update.message, 'text', None))
        except Exception:
            pass
        return
    # Обрабатываем только текст из приватного чата, не команды
    if not update.message or not update.message.text:
        logger.debug("HELP: skip: no message or no text")
        return
    if update.effective_chat and update.effective_chat.type != 'private':
        logger.debug("HELP: skip: non-private chat type=%s", update.effective_chat.type)
        return
    if update.message.text.startswith('/'):
        logger.debug("HELP: skip: command text=%r", update.message.text)
        return
    await mark_user_active_if_needed(update.effective_user.id, context)
    user = update.effective_user
    chat_id = update.effective_chat.id if update.effective_chat else None
    # Получаем текст
    text = (update.message.text or '').strip()
    logger.info("HELP: help_user_text_handler: user_id=%s chat_id=%s text_len=%s text_preview=%r", user.id, chat_id, len(text), text[:100])
    # Чистим промпт, если он есть
    prompt_id = context.user_data.pop(HELP_PROMPT_MESSAGE_ID, None)
    if prompt_id:
        try:
            await context.bot.delete_message(chat_id, prompt_id)
            logger.info("HELP: deleted prompt_message_id=%s", prompt_id)
        except Exception as e:
            logger.exception("HELP: failed to delete prompt prompt_message_id=%s: %s", prompt_id, e)
    # Снимаем флаг ожидания
    context.user_data.pop(AWAITING_HELP_QUESTION, None)

    # Отвечаем пользователю о получении вопроса
    ack_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Ок', callback_data='help_ok_back')]
    ])
    try:
        await update.message.delete()
        logger.info("HELP: deleted user question message_id=%s", update.message.message_id)
    except Exception as e:
        logger.exception("HELP: failed to delete user question message: %s", e)
    try:
        await update.message.chat.send_message('✅ Ваш вопрос получен! Скоро Вам ответят!', reply_markup=ack_keyboard)
        logger.info("HELP: sent ack to user_id=%s", user.id)
    except Exception as e:
        logger.exception("HELP: failed to send ack to user_id=%s: %s", user.id, e)

    # Готовим отправку в группу
    group_id_str = os.getenv(HELP_GROUP_ID_ENV)
    logger.info("HELP: env %s raw=%r", HELP_GROUP_ID_ENV, group_id_str)
    if not group_id_str:
        logger.error("HELP: %s is not set in environment", HELP_GROUP_ID_ENV)
        return
    group_id_str_clean = group_id_str.strip().strip('"').strip("'")
    logger.info("HELP: env %s cleaned=%r", HELP_GROUP_ID_ENV, group_id_str_clean)
    try:
        group_id = int(group_id_str_clean)
    except Exception as e:
        logger.exception("HELP: failed to convert %s to int: raw=%r cleaned=%r error=%s", HELP_GROUP_ID_ENV, group_id_str, group_id_str_clean, e)
        return

    username = f"@{user.username}" if user.username else '-'
    first_name = user.first_name or '-'
    user_id = user.id
    asked_at = datetime.now().strftime('%d.%m.%Y %H:%M:%S')

    group_text = (
        f"❓ <b>Новый вопрос</b>\n\n"
        f"<b>Telegram ID:</b> <code>{user_id}</code>\n"
        f"<b>Имя:</b> {first_name}\n"
        f"<b>Username:</b> {username}\n"
        f"<b>Время вопроса:</b> {asked_at}\n\n"
        f"<b>Текст вопроса:</b>\n{text}"
    )

    try:
        logger.info("HELP: sending question to group_id=%s", group_id)
        sent = await context.bot.send_message(
            chat_id=group_id,
            text=group_text,
            parse_mode='HTML'
        )
        logger.info("HELP: sent to group ok: group_message_id=%s", sent.message_id)
    except Exception as e:
        logger.exception("HELP: failed to send message to group_id=%s: %s", group_id, e)
        return

    # Сохраняем связь message_id -> данные вопроса
    # Подгружаем индекс с диска перед изменением, чтобы не потерять предыдущие записи
    load_help_index_from_disk(context)
    index = context.application.bot_data.setdefault(HELP_QUESTIONS_INDEX, {})
    index[sent.message_id] = {
        'user_id': user_id,
        'first_name': first_name,
        'username': username,
        'question_text': text,
        'asked_at': asked_at,
    }
    logger.info("HELP: index updated: key=%s size=%d", sent.message_id, len(index))
    # Персистим на диск
    save_help_index_to_disk(context)

async def help_group_reply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Обрабатываем ответы админов в группе на сообщение бота с вопросом
    group_id_str = os.getenv(HELP_GROUP_ID_ENV)
    # Получаем сообщение (для групп это обычный message)
    msg = update.message or getattr(update, 'channel_post', None)
    logger.info(
        "HELP: group_reply_handler triggered: chat_id=%s msg_id=%s has_reply=%s",
        (msg.chat.id if msg and msg.chat else None),
        (msg.message_id if msg else None),
        bool(msg and msg.reply_to_message)
    )
    if not group_id_str:
        logger.error("HELP: %s is not set in environment (group_reply_handler)", HELP_GROUP_ID_ENV)
        return
    try:
        group_id = int(group_id_str.strip().strip('"').strip("'"))
    except Exception as e:
        logger.exception("HELP: failed to convert %s to int in reply handler: %s", HELP_GROUP_ID_ENV, e)
        return

    if not msg:
        logger.debug("HELP: skip: no message or channel_post in update")
        return
    if msg.chat.id != group_id:
        logger.info("HELP: message in another chat: %s != %s", msg.chat.id, group_id)
        return
    if not msg.reply_to_message:
        logger.info("HELP: skip: not a reply message")
        return

    # При первом использовании после рестарта загрузим индекс с диска
    load_help_index_from_disk(context)
    index = context.application.bot_data.get(HELP_QUESTIONS_INDEX, {})
    ref = index.get(msg.reply_to_message.message_id)
    logger.info("HELP: lookup ref by replied_id=%s -> %s", msg.reply_to_message.message_id, 'FOUND' if ref else 'NOT_FOUND')
    if not ref:
        return

    # Формируем ответ пользователю
    answer_text = (msg.text or msg.caption or '').strip()
    answered_at = datetime.now().strftime('%d.%m.%Y %H:%M:%S')

    user_id = ref['user_id']
    asked_at = ref['asked_at']
    question_text = ref['question_text']

    user_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Ок', callback_data='help_user_ok_delete')]
    ])

    dm_text = (
        '📩 <b>Ответ получен!</b>\n\n'
        f'<b>Время Вашего вопроса:</b> {asked_at}\n'
        f'<b>Ваш вопрос:</b> {question_text}\n\n'
        f'<b>Время ответа:</b> {answered_at}\n'
        f'<b>Ответ:</b> {answer_text}'
    )

    try:
        await context.bot.send_message(chat_id=user_id, text=dm_text, reply_markup=user_keyboard, parse_mode='HTML')
        logger.info("HELP: sent answer to user_id=%s from_admin_id=%s via_group_msg_id=%s", user_id, msg.from_user.id if msg.from_user else None, msg.reply_to_message.message_id)
    except Exception as e:
        logger.exception("HELP: failed to send answer to user_id=%s: %s", user_id, e)

    # Пересылаем вложения при наличии
    try:
        if getattr(msg, 'photo', None):
            await context.bot.send_photo(chat_id=user_id, photo=msg.photo[-1].file_id)
        if getattr(msg, 'document', None):
            await context.bot.send_document(chat_id=user_id, document=msg.document.file_id)
        if getattr(msg, 'audio', None):
            await context.bot.send_audio(chat_id=user_id, audio=msg.audio.file_id)
        if getattr(msg, 'voice', None):
            await context.bot.send_voice(chat_id=user_id, voice=msg.voice.file_id)
        if getattr(msg, 'video', None):
            await context.bot.send_video(chat_id=user_id, video=msg.video.file_id)
        if getattr(msg, 'sticker', None):
            await context.bot.send_sticker(chat_id=user_id, sticker=msg.sticker.file_id)
    except Exception as e:
        logger.exception("HELP: failed to forward attachments to user_id=%s: %s", user_id, e)

    # Можно оставить связь для возможных повторных ответов


def register_help_handlers(application):
    logger.info("HELP: registering help handlers")
    application.add_handler(CallbackQueryHandler(help_menu_callback, pattern='^help$'))
    application.add_handler(CallbackQueryHandler(help_faq_callback, pattern='^help_faq$'))
    application.add_handler(CallbackQueryHandler(help_ask_callback, pattern='^help_ask$'))
    application.add_handler(CallbackQueryHandler(help_cancel_ask_callback, pattern='^help_cancel_ask$'))
    application.add_handler(CallbackQueryHandler(help_ok_back_callback, pattern='^help_ok_back$'))
    application.add_handler(CallbackQueryHandler(help_user_ok_delete_callback, pattern='^help_user_ok_delete$'))

    # Текст от пользователя в режиме ожидания вопроса — ранний, НЕ блокируем другие хендлеры
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND),
            help_user_text_handler,
            block=True
        ),
        group=-3
    )

    # Ответы админов в группе (только текст, по реплаю)
    application.add_handler(
        MessageHandler(
            filters.REPLY,
            help_group_reply_handler,
            block=True
        ),
        group=-1
    ) 
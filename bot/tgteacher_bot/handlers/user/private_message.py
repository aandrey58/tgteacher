from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters
import asyncio
# MCP: Переиспользуем отправку из рассылки (надёжная обработка ретраев/ошибок)
from tgteacher_bot.handlers.admin.admin_broadcast import _send_to_user

# MCP: Вспомогательные клавиатуры

def _get_private_prompt_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton('❌ Отмена', callback_data='admin_private_message_cancel')]])


def _get_private_preview_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('Это предпросмотр личного сообщения', callback_data='noop')],
        [InlineKeyboardButton('▶️ Отправить', callback_data='admin_private_message_send'), InlineKeyboardButton('❌ Отмена', callback_data='admin_private_message_cancel')],
    ])


# MCP: Коллбек по кнопке "✍️ Написать пользователю" — включаем режим ожидания контента
async def admin_private_message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # Парсим user_id из callback_data
    data = query.data.replace('admin_private_message_', '')
    try:
        user_id = int(data)
    except Exception:
        user_id = None
    print(f"MCP DEBUG: admin_private_message_callback user_id={user_id}")

    # Готовим клавиатуру "Отмена" и (опционально) возврат к профилю отдельной кнопкой не делаем — профиль уже выше в чате
    keyboard = _get_private_prompt_keyboard()

    # MCP: Сохраняем стейты ожидания
    context.user_data['waiting_for_private_message'] = True
    context.user_data['private_message_target_user_id'] = user_id
    print("MCP DEBUG: set waiting_for_private_message=True target_user_id=", user_id)

    # Отправляем приглашение в чат
    prompt = await query.message.chat.send_message(
        f"✍️ Напишите сообщение для пользователя <code>{user_id}</code>\n\nВы можете прикрепить до одного фото/видео/файл",
        reply_markup=keyboard,
        parse_mode='HTML'
    )
    context.user_data['private_message_prompt_chat_id'] = prompt.chat_id
    context.user_data['private_message_prompt_msg_id'] = prompt.message_id
    print(f"MCP DEBUG: prompt sent chat_id={prompt.chat_id} msg_id={prompt.message_id}")

    # MCP: Удаляем карточку профиля, из которой вошли в режим личного сообщения
    try:
        await query.message.delete()
        print("MCP DEBUG: source profile message deleted")
    except Exception as e:
        print("MCP DEBUG: failed to delete source profile message:", e)


# MCP: Обработчик отмены — чистим черновик и временные сообщения, затем показываем карточку юзера
async def admin_private_message_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    print("MCP DEBUG: admin_private_message_cancel_callback invoked")

    # Берём user_id до очистки стейтов
    target_user_id = context.user_data.get('private_message_target_user_id')
    print("MCP DEBUG: cancel target_user_id=", target_user_id)

    # Снимаем флаги ожидания
    context.user_data.pop('waiting_for_private_message', None)

    # Удаляем приглашение, если существует
    prompt_chat_id = context.user_data.pop('private_message_prompt_chat_id', None)
    prompt_msg_id = context.user_data.pop('private_message_prompt_msg_id', None)
    print(f"MCP DEBUG: cancel remove prompt chat_id={prompt_chat_id} msg_id={prompt_msg_id}")
    if prompt_chat_id and prompt_msg_id:
        try:
            await context.bot.delete_message(chat_id=prompt_chat_id, message_id=prompt_msg_id)
        except Exception as e:
            print("MCP DEBUG: cancel delete prompt error:", e)

    # Удаляем превью, если существует
    preview_chat_id = context.user_data.pop('private_message_preview_chat_id', None)
    preview_msg_id = context.user_data.pop('private_message_preview_msg_id', None)
    print(f"MCP DEBUG: cancel remove preview chat_id={preview_chat_id} msg_id={preview_msg_id}")
    if preview_chat_id and preview_msg_id:
        try:
            await context.bot.delete_message(chat_id=preview_chat_id, message_id=preview_msg_id)
        except Exception as e:
            print("MCP DEBUG: cancel delete preview error:", e)

    # Чистим черновик
    context.user_data.pop('private_message_draft', None)

    # Показываем карточку пользователя
    if target_user_id:
        try:
            from tgteacher_bot.handlers.admin.admin_users import get_admin_user_profile
            text, keyboard = await get_admin_user_profile(target_user_id, context)
            await query.message.chat.send_message(text, reply_markup=keyboard, parse_mode='HTML')
            print("MCP DEBUG: cancel show user profile ok")
        except Exception as e:
            print("MCP DEBUG: cancel show profile error:", e)

    # Завершаем очистку user_id
    context.user_data.pop('private_message_target_user_id', None)


# MCP: Обработчик отправки — берём черновик и шлём конкретному пользователю, затем показываем карточку
async def admin_private_message_send_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    print("MCP DEBUG: admin_private_message_send_callback invoked")

    draft = context.user_data.get('private_message_draft')
    target_user_id = context.user_data.get('private_message_target_user_id')
    print(f"MCP DEBUG: send draft_exists={bool(draft)} target_user_id={target_user_id}")

    if not draft or not target_user_id:
        try:
            await query.answer('⚠️ Черновик не найден. Попробуйте ещё раз.', show_alert=True)
        except Exception:
            pass
        return

    text = draft.get('text')
    media = draft.get('media')
    print(f"MCP DEBUG: send text_len={len(text) if text else 0} media={media}")

    # Удаляем превью, если было
    preview_chat_id = context.user_data.pop('private_message_preview_chat_id', None)
    preview_msg_id = context.user_data.pop('private_message_preview_msg_id', None)
    if preview_chat_id and preview_msg_id:
        try:
            await context.bot.delete_message(chat_id=preview_chat_id, message_id=preview_msg_id)
        except Exception as e:
            print("MCP DEBUG: send delete preview error:", e)

    # Чистим приглашение, если ещё существует
    prompt_chat_id = context.user_data.pop('private_message_prompt_chat_id', None)
    prompt_msg_id = context.user_data.pop('private_message_prompt_msg_id', None)
    if prompt_chat_id and prompt_msg_id:
        try:
            await context.bot.delete_message(chat_id=prompt_chat_id, message_id=prompt_msg_id)
        except Exception as e:
            print("MCP DEBUG: send delete prompt error:", e)

    # Снимаем флаги
    context.user_data.pop('waiting_for_private_message', None)
    context.user_data.pop('private_message_draft', None)

    # Отправляем пользователю
    ok = await _send_to_user(context.application.bot, target_user_id, text, media)
    print(f"MCP DEBUG: send _send_to_user ok={ok}")

    # Отмашка админу и автоудаление через 3 секунды
    try:
        status_text = (f"✅ Сообщение пользователю {target_user_id} отправлено"
                       if ok else f"❌ Сообщение пользователю {target_user_id} не отправлено")
        status_msg = await query.message.chat.send_message(status_text)

        async def _delete_status_later(chat_id: int, message_id: int):
            try:
                await asyncio.sleep(3)
                await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
            except Exception as e:
                print("MCP DEBUG: failed to delete status message:", e)

        context.application.create_task(_delete_status_later(status_msg.chat_id, status_msg.message_id))
    except Exception as e:
        print("MCP DEBUG: failed to send/delete status message:", e)

    # После отправки — показываем профиль пользователя; кнопка Назад будет вести либо к списку, либо к обычной навигации
    try:
        from tgteacher_bot.handlers.admin.admin_users import get_admin_user_profile
        profile_text, profile_keyboard = await get_admin_user_profile(target_user_id, context)
        await query.message.chat.send_message(profile_text, reply_markup=profile_keyboard, parse_mode='HTML')
        print("MCP DEBUG: send show user profile ok")
    except Exception as e:
        print("MCP DEBUG: send post-action error:", e)

    # Завершаем очистку user_id
    context.user_data.pop('private_message_target_user_id', None)


# MCP: Хендлер получения контента для личного сообщения (текст/фото/видео/документ)
async def admin_private_message_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print('MCP DEBUG: admin_private_message_message_handler invoked for message:', getattr(update.message, 'text', None) or getattr(update.message, 'caption', None))
    # Обрабатываем только если ждём личное сообщение
    if not context.user_data.get('waiting_for_private_message'):
        print('MCP DEBUG: waiting_for_private_message is FALSE, ignoring')
        return

    message = update.effective_message

    # Собираем текст (при наличии) и одно медиа
    text = message.text or message.caption or None

    media_candidates = []
    if getattr(message, 'photo', None):
        media_candidates.append(('photo', message.photo[-1].file_id))
    if getattr(message, 'video', None):
        media_candidates.append(('video', message.video.file_id))
    if getattr(message, 'document', None):
        media_candidates.append(('document', message.document.file_id))

    print(f"MCP DEBUG: pm message parsed text_len={len(text) if text else 0} media_candidates={media_candidates}")

    # Валидируем содержимое
    if not text and not media_candidates:
        await message.reply_text('❌ Нужно отправить текст и/или один файл (фото/видео/документ).', reply_markup=_get_private_prompt_keyboard())
        print('MCP DEBUG: pm validation failed (no content)')
        return

    if len(media_candidates) > 1:
        await message.reply_text('❌ Можно прикрепить только один файл (фото/видео/документ). Пришлите заново.', reply_markup=_get_private_prompt_keyboard())
        print('MCP DEBUG: pm validation failed (too many media)')
        return

    media = None
    if len(media_candidates) == 1:
        mtype, file_id = media_candidates[0]
        media = {'type': mtype, 'file_id': file_id}

    # Удаляем приглашение на ввод (если есть)
    prompt_chat_id = context.user_data.pop('private_message_prompt_chat_id', None)
    prompt_msg_id = context.user_data.pop('private_message_prompt_msg_id', None)
    print(f"MCP DEBUG: pm remove prompt chat_id={prompt_chat_id} msg_id={prompt_msg_id}")
    if prompt_chat_id and prompt_msg_id:
        try:
            await context.bot.delete_message(chat_id=prompt_chat_id, message_id=prompt_msg_id)
        except Exception as e:
            print('MCP DEBUG: pm delete prompt error:', e)

    # Сохраняем черновик
    target_user_id = context.user_data.get('private_message_target_user_id')
    context.user_data['private_message_draft'] = {
        'user_id': target_user_id,
        'text': text,
        'media': media,
    }
    print(f"MCP DEBUG: pm draft saved for user_id={target_user_id} has_media={media is not None}")

    # Удаляем исходное сообщение администратора для чистоты чата
    try:
        await context.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
        print('MCP DEBUG: pm admin input message deleted')
    except Exception as e:
        print('MCP DEBUG: pm delete admin message error:', e)

    # Показываем превью
    preview_msg = None
    try:
        if media:
            mtype = media['type']
            file_id = media['file_id']
            caption = text or None
            if mtype == 'photo':
                preview_msg = await message.chat.send_photo(photo=file_id, caption=caption, reply_markup=_get_private_preview_keyboard())
            elif mtype == 'video':
                preview_msg = await message.chat.send_video(video=file_id, caption=caption, reply_markup=_get_private_preview_keyboard())
            elif mtype == 'document':
                preview_msg = await message.chat.send_document(document=file_id, caption=caption, reply_markup=_get_private_preview_keyboard())
        else:
            preview_msg = await message.chat.send_message(text=text, reply_markup=_get_private_preview_keyboard())
        print(f"MCP DEBUG: pm preview sent chat_id={preview_msg.chat_id} msg_id={preview_msg.message_id}")
    except Exception as e:
        print('MCP DEBUG: pm preview send error:', e)

    context.user_data['private_message_preview_chat_id'] = preview_msg.chat_id if preview_msg else None
    context.user_data['private_message_preview_msg_id'] = preview_msg.message_id if preview_msg else None

    # Снимаем режим ожидания ввода (ждём подтверждение/отмену)
    context.user_data.pop('waiting_for_private_message', None)
    print('MCP DEBUG: pm waiting_for_private_message cleared, awaiting confirm/cancel')


def register_private_message_handler(application):
    application.add_handler(CallbackQueryHandler(admin_private_message_callback, pattern=r'^admin_private_message_\d+$'))
    application.add_handler(CallbackQueryHandler(admin_private_message_cancel_callback, pattern=r'^admin_private_message_cancel$'))
    application.add_handler(CallbackQueryHandler(admin_private_message_send_callback, pattern=r'^admin_private_message_send$'))
    # MCP: Ставим самый высокий приоритет, чтобы личные сообщения ловились первыми
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND),
            admin_private_message_message_handler,
            block=False
        ),
        group=-2
    )
    # MCP: Доп. хендлеры для медиа, чтобы гарантированно ловить фото/видео/документы
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & (filters.PHOTO | filters.VIDEO | filters.Document.ALL | filters.ANIMATION),
            admin_private_message_message_handler,
            block=False
        ),
        group=-2
    ) 
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from tgteacher_bot.handlers.admin.admin_status import track_metrics
from tgteacher_bot.db.user_repo import get_pool
from tgteacher_bot.handlers.admin.admin_list import get_admin_ids
import asyncio
import time
from telegram.error import RetryAfter, TimedOut, NetworkError, TelegramError, Forbidden, BadRequest

BROADCAST_TARGET_ALL = 'all'
BROADCAST_TARGET_SUBS = 'subs'
BROADCAST_TARGET_NOSUBS = 'nosubs'


def _get_broadcast_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('👥 Для всех', callback_data='admin_broadcast_target_all')],
        [InlineKeyboardButton('💎 Пользователи с подпиской', callback_data='admin_broadcast_target_subs')],
        [InlineKeyboardButton('👤 Пользователи без подписки', callback_data='admin_broadcast_target_nosubs')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_panel')],
    ])


def _get_broadcast_cancel_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton('❌ Отмена', callback_data='admin_broadcast_cancel')]])


def _get_broadcast_hide_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton('🙈 Скрыть', callback_data='admin_broadcast_hide')]])


def _get_broadcast_preview_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('Это предпросмотр рассылки', callback_data='noop')],
        [InlineKeyboardButton('▶️ Отправить', callback_data='admin_broadcast_send'), InlineKeyboardButton('❌ Отмена', callback_data='admin_broadcast_cancel')],
    ])


def _target_label(target: str) -> str:
    if target == BROADCAST_TARGET_ALL:
        return 'всех'
    if target == BROADCAST_TARGET_SUBS:
        return 'пользователей с подпиской'
    if target == BROADCAST_TARGET_NOSUBS:
        return 'пользователей без подписки'
    return target


async def _get_user_ids_for_target(target: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if target == BROADCAST_TARGET_ALL:
            rows = await conn.fetch('SELECT user_id FROM users ORDER BY registered_at DESC')
        elif target == BROADCAST_TARGET_SUBS:
            rows = await conn.fetch('SELECT user_id FROM users WHERE is_subscribed = TRUE ORDER BY registered_at DESC')
        elif target == BROADCAST_TARGET_NOSUBS:
            rows = await conn.fetch('SELECT user_id FROM users WHERE is_subscribed = FALSE ORDER BY registered_at DESC')
        else:
            rows = []
    user_ids = [row['user_id'] for row in rows]
    # Исключаем админов из рассылки
    try:
        admin_ids_raw = get_admin_ids()
        admin_ids_set = {int(x) for x in admin_ids_raw if str(x).isdigit()}
    except Exception:
        admin_ids_set = set()
    filtered_ids = [uid for uid in user_ids if uid not in admin_ids_set]
    excluded = len(user_ids) - len(filtered_ids)
    print(f"MCP DEBUG: _get_user_ids_for_target target={target} total={len(user_ids)} excluded_admins={excluded} result={len(filtered_ids)}")
    return filtered_ids


@track_metrics
async def admin_broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text('📢 <b>Управление рассылкой</b>\n\nВыберите кому хотите сделать рассылку:', reply_markup=_get_broadcast_menu_keyboard(), parse_mode='HTML')


@track_metrics
async def admin_broadcast_target_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_code = query.data.replace('admin_broadcast_target_', '')
    context.user_data['broadcast_target'] = target_code
    print(f"MCP DEBUG: admin_broadcast_target_callback target={target_code}")
    # Если уже идёт рассылка — позволяем подготовить черновик, подсказку показываем алертом
    if context.application.bot_data.get('broadcast_in_progress'):
        try:
            await query.answer('⏳ Рассылка уже выполняется. Можете подготовить новый текст — он будет поставлен в очередь.', show_alert=True)
        except Exception:
            pass
    prompt = await query.message.chat.send_message(
        f"✍️ Напишите текст рассылки <b>для { _target_label(target_code) }</b>, Вы можете прикрепить до одного фото/видео/файл",
        reply_markup=_get_broadcast_cancel_keyboard(),
        parse_mode='HTML'
    )
    context.user_data['waiting_for_broadcast'] = True
    context.user_data['broadcast_prompt_msg_id'] = prompt.message_id
    context.user_data['broadcast_prompt_chat_id'] = prompt.chat_id
    print("MCP DEBUG: waiting_for_broadcast установлен, ожидаем контент для рассылки")
    # Удаляем сообщение с меню рассылки
    try:
        await query.delete_message()
    except Exception:
        pass


@track_metrics
async def admin_broadcast_cancel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop('waiting_for_broadcast', None)
    # Удаляем приглашение, если ещё есть
    prompt_chat_id = context.user_data.pop('broadcast_prompt_chat_id', None)
    prompt_msg_id = context.user_data.pop('broadcast_prompt_msg_id', None)
    if prompt_chat_id and prompt_msg_id:
        try:
            await context.bot.delete_message(chat_id=prompt_chat_id, message_id=prompt_msg_id)
        except Exception:
            pass
    # Удаляем превью, если было
    preview_chat_id = context.user_data.pop('broadcast_preview_chat_id', None)
    preview_msg_id = context.user_data.pop('broadcast_preview_msg_id', None)
    if preview_chat_id and preview_msg_id:
        try:
            await context.bot.delete_message(chat_id=preview_chat_id, message_id=preview_msg_id)
        except Exception:
            pass
    else:
        # Фоллбэк: удаляем сообщение, в котором нажата "Отмена"
        try:
            await query.message.delete()
        except Exception:
            pass
    context.user_data.pop('broadcast_draft', None)
    print("MCP DEBUG: admin_broadcast_cancel_callback — черновик и приглашение очищены")
    await query.message.chat.send_message('📢 <b>Управление рассылкой</b>\n\nВыберите кому хотите сделать рассылку:', reply_markup=_get_broadcast_menu_keyboard(), parse_mode='HTML')


@track_metrics
async def admin_broadcast_hide_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass


async def _send_to_user(bot, user_id: int, text: str | None, media: dict | None, max_retries: int = 3):
    attempts = 0
    while attempts < max_retries:
        try:
            print(f"MCP DEBUG: _send_to_user start user_id={user_id} attempt={attempts+1} media={media}")
            if media:
                mtype = media.get('type')
                file_id = media.get('file_id')
                caption = text or None
                if mtype == 'photo':
                    await bot.send_photo(chat_id=user_id, photo=file_id, caption=caption)
                elif mtype == 'video':
                    await bot.send_video(chat_id=user_id, video=file_id, caption=caption)
                elif mtype == 'document':
                    await bot.send_document(chat_id=user_id, document=file_id, caption=caption)
                else:
                    await bot.send_message(chat_id=user_id, text=text or '')
            else:
                if text:
                    await bot.send_message(chat_id=user_id, text=text)
            print(f"MCP DEBUG: _send_to_user success user_id={user_id}")
            return True
        except RetryAfter as e:
            ra = float(getattr(e, 'retry_after', 1))
            print(f"MCP DEBUG: _send_to_user RetryAfter user_id={user_id} retry_after={ra}")
            await asyncio.sleep(ra + 0.5)
            attempts += 1
            continue
        except (TimedOut, NetworkError) as e:
            print(f"MCP DEBUG: _send_to_user network/timeout user_id={user_id} err={e}")
            await asyncio.sleep(1.0)
            attempts += 1
            continue
        except (Forbidden, BadRequest) as e:
            print(f"MCP DEBUG: _send_to_user forbidden/badrequest user_id={user_id} err={e}")
            return False
        except TelegramError as e:
            print(f"MCP DEBUG: _send_to_user telegramerror user_id={user_id} err={e}")
            return False
        except Exception as e:
            print(f"MCP DEBUG: _send_to_user unexpected error user_id={user_id} err={e}")
            return False
    print(f"MCP DEBUG: _send_to_user failed after retries user_id={user_id}")
    return False


async def _run_broadcast(context: ContextTypes.DEFAULT_TYPE, admin_chat_id: int, progress_msg_id: int, target_code: str, text: str | None, media: dict | None):
    bot = context.application.bot
    user_ids = await _get_user_ids_for_target(target_code)
    total = len(user_ids)
    delivered = 0
    failed = 0
    processed = 0

    context.application.bot_data['broadcast_in_progress'] = True
    print(f"MCP DEBUG: _run_broadcast START target={target_code} total={total}")
    last_update_ts = 0.0

    async def update_progress(force: bool = False):
        nonlocal last_update_ts
        now = time.time()
        if not force and now - last_update_ts < 1.5:
            return
        last_update_ts = now
        try:
            await bot.edit_message_text(
                chat_id=admin_chat_id,
                message_id=progress_msg_id,
                text=(
                    f"⏳ Рассылка <b>для { _target_label(target_code) }</b> в процессе... {processed} из {total}\n\n"
                    f"✅ Успешно: {delivered}\n"
                    f"❌ С ошибками: {failed}"
                ),
                reply_markup=_get_broadcast_hide_keyboard(),
                parse_mode='HTML'
            )
        except Exception as e:
            print(f"MCP DEBUG: update_progress edit_message_text error: {e}")

    # Первое обновление
    await update_progress(force=True)

    for uid in user_ids:
        print(f"MCP DEBUG: sending to uid={uid}")
        ok = await _send_to_user(bot, uid, text, media)
        if ok:
            delivered += 1
        else:
            failed += 1
        processed += 1
        print(f"MCP DEBUG: sent to uid={uid} ok={ok} delivered={delivered}/{total}")
        await update_progress()
        await asyncio.sleep(0.05)

    context.application.bot_data['broadcast_in_progress'] = False
    print(f"MCP DEBUG: _run_broadcast DONE delivered={delivered}/{total}")

    try:
        await bot.edit_message_text(
            chat_id=admin_chat_id,
            message_id=progress_msg_id,
            text=(
                f"✅ Рассылка <b>для { _target_label(target_code) }</b> завершена!\n\n"
                f"✅ Успешно: {delivered}\n"
                f"❌ С ошибками: {failed}\n"
                f"📦 Всего: {total}"
            ),
            reply_markup=_get_broadcast_hide_keyboard(),
            parse_mode='HTML'
        )
    except Exception as e:
        print(f"MCP DEBUG: final edit_message_text error: {e}")

    # Проверяем очередь и запускаем следующую, если есть
    try:
        queue = context.application.bot_data.get('broadcast_queue', [])
        if queue:
            next_job = queue.pop(0)
            next_target = next_job.get('target', BROADCAST_TARGET_ALL)
            next_text = next_job.get('text')
            next_media = next_job.get('media')
            next_admin_chat_id = next_job.get('admin_chat_id', admin_chat_id)
            status_chat_id = next_job.get('status_chat_id')
            status_msg_id = next_job.get('status_msg_id')
            print(f"MCP DEBUG: starting next queued broadcast target={next_target} remaining_queue={len(queue)}")
            if status_chat_id and status_msg_id:
                try:
                    await bot.edit_message_text(
                        chat_id=status_chat_id,
                        message_id=status_msg_id,
                        text=f"⏳ Рассылка <b>для { _target_label(next_target) }</b> в процессе...",
                        reply_markup=_get_broadcast_hide_keyboard(),
                        parse_mode='HTML'
                    )
                    context.application.create_task(_run_broadcast(context, status_chat_id, status_msg_id, next_target, next_text, next_media))
                    return
                except Exception as e:
                    print(f"MCP DEBUG: failed to edit queued status message, will send new one: {e}")
            # Fallback — создаём новое статус-сообщение
            progress = await bot.send_message(
                chat_id=next_admin_chat_id,
                text=f"⏳ Рассылка <b>для { _target_label(next_target) }</b> в процессе...",
                reply_markup=_get_broadcast_hide_keyboard(),
                parse_mode='HTML'
            )
            # И отдельное меню под ним
            await bot.send_message(chat_id=next_admin_chat_id, text='📢 <b>Управление рассылкой</b>\n\n✅ Рассылка в процессе', reply_markup=_get_broadcast_menu_keyboard(), parse_mode='HTML')
            context.application.create_task(_run_broadcast(context, progress.chat_id, progress.message_id, next_target, next_text, next_media))
    except Exception as e:
        print(f"MCP DEBUG: error while starting queued broadcast: {e}")


@track_metrics
async def admin_broadcast_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print('MCP DEBUG: admin_broadcast_message_handler вызван')

    if not context.user_data.get('waiting_for_broadcast'):
        print('MCP DEBUG: waiting_for_broadcast НЕ установлен, выходим')
        return

    print('MCP DEBUG: waiting_for_broadcast установлен, продолжаем')

    # Не вмешиваться, если активен ввод ID админа в другом разделе
    if context.user_data.get('waiting_for_admin_id'):
        print('MCP DEBUG: waiting_for_admin_id активен, выходим')
        return

    print('MCP DEBUG: начинаем обработку сообщения рассылки')

    message = update.effective_message
    text = message.text or message.caption or None

    print(f'MCP DEBUG: извлечен текст: {text}')

    media_candidates = []
    if getattr(message, 'photo', None):
        media_candidates.append(('photo', message.photo[-1].file_id))
    if getattr(message, 'video', None):
        media_candidates.append(('video', message.video.file_id))
    if getattr(message, 'document', None):
        media_candidates.append(('document', message.document.file_id))

    if not text and not media_candidates:
        await message.reply_text('❌ Нужно отправить текст или один файл (фото/видео/документ).', reply_markup=_get_broadcast_cancel_keyboard())
        return

    if len(media_candidates) > 1:
        await message.reply_text('❌ Можно прикрепить только один файл (фото/видео/документ). Пришлите заново.', reply_markup=_get_broadcast_cancel_keyboard())
        return

    media = None
    if len(media_candidates) == 1:
        mtype, file_id = media_candidates[0]
        media = {'type': mtype, 'file_id': file_id}

    target_code = context.user_data.get('broadcast_target', BROADCAST_TARGET_ALL)
    print(f"MCP DEBUG: создан черновик рассылки target={target_code} media={media is not None}")

    # Удаляем приглашение на ввод
    prompt_chat_id = context.user_data.pop('broadcast_prompt_chat_id', None)
    prompt_msg_id = context.user_data.pop('broadcast_prompt_msg_id', None)
    if prompt_chat_id and prompt_msg_id:
        try:
            await context.bot.delete_message(chat_id=prompt_chat_id, message_id=prompt_msg_id)
        except Exception:
            pass

    # Удаляем исходное сообщение администратора для чистоты чата
    try:
        await context.bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
    except Exception:
        pass

    # Сохраняем черновик
    context.user_data['broadcast_draft'] = {
        'target': target_code,
        'text': text,
        'media': media,
    }

    # Показываем превью (контент + кнопки)
    preview_msg = None
    if media:
        mtype = media['type']
        file_id = media['file_id']
        caption = text or None
        if mtype == 'photo':
            preview_msg = await message.chat.send_photo(photo=file_id, caption=caption, reply_markup=_get_broadcast_preview_keyboard())
        elif mtype == 'video':
            preview_msg = await message.chat.send_video(video=file_id, caption=caption, reply_markup=_get_broadcast_preview_keyboard())
        elif mtype == 'document':
            preview_msg = await message.chat.send_document(document=file_id, caption=caption, reply_markup=_get_broadcast_preview_keyboard())
    else:
        preview_msg = await message.chat.send_message(text=text, reply_markup=_get_broadcast_preview_keyboard())

    context.user_data['broadcast_preview_chat_id'] = preview_msg.chat_id
    context.user_data['broadcast_preview_msg_id'] = preview_msg.message_id

    # Больше не ждём ввод
    context.user_data.pop('waiting_for_broadcast', None)
    print("MCP DEBUG: превью отправлено, waiting_for_broadcast снят")


@track_metrics
async def admin_broadcast_send_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    draft = context.user_data.get('broadcast_draft')
    if not draft:
        await query.answer('⚠️ Черновик рассылки не найден. Попробуйте ещё раз.', show_alert=True)
        return

    target_code = draft.get('target', BROADCAST_TARGET_ALL)
    text = draft.get('text')
    media = draft.get('media')
    print(f"MCP DEBUG: admin_broadcast_send_callback target={target_code} text_len={len(text) if text else 0} media={media}")

    # Удаляем превью
    preview_chat_id = context.user_data.pop('broadcast_preview_chat_id', None)
    preview_msg_id = context.user_data.pop('broadcast_preview_msg_id', None)
    if preview_chat_id and preview_msg_id:
        try:
            await context.bot.delete_message(chat_id=preview_chat_id, message_id=preview_msg_id)
        except Exception:
            pass

    # Чистим черновик
    context.user_data.pop('broadcast_draft', None)

    # Если сейчас идёт рассылка — ставим в очередь с отдельным статус-сообщением
    if context.application.bot_data.get('broadcast_in_progress'):
        queue = context.application.bot_data.setdefault('broadcast_queue', [])
        position = len(queue) + 1
        queued_status_msg = await query.message.chat.send_message(
            f"🧾 Рассылка <b>для { _target_label(target_code) }</b> в очереди, позиция №{position}",
            reply_markup=_get_broadcast_hide_keyboard(),
            parse_mode='HTML'
        )
        await query.message.chat.send_message('📢 <b>Управление рассылкой</b>\n\n🧾 Черновик поставлен в очередь', reply_markup=_get_broadcast_menu_keyboard(), parse_mode='HTML')
        job = {
            'target': target_code,
            'text': text,
            'media': media,
            'admin_chat_id': queued_status_msg.chat_id,
            'status_chat_id': queued_status_msg.chat_id,
            'status_msg_id': queued_status_msg.message_id,
        }
        queue.append(job)
        print(f"MCP DEBUG: broadcast queued position={position} target={target_code} queued_msg_id={queued_status_msg.message_id}")
        return

    # Запускаем рассылку (создаём статус-сообщение)
    progress = await query.message.chat.send_message(
        f"⏳ Рассылка <b>для { _target_label(target_code) }</b> в процессе...",
        reply_markup=_get_broadcast_hide_keyboard(),
        parse_mode='HTML'
    )

    await query.message.chat.send_message('📢 <b>Управление рассылкой</b>\n\n✅ Рассылка в процессе', reply_markup=_get_broadcast_menu_keyboard(), parse_mode='HTML')

    print(f"MCP DEBUG: создаю таск _run_broadcast chat_id={progress.chat_id} msg_id={progress.message_id}")
    context.application.create_task(_run_broadcast(context, progress.chat_id, progress.message_id, target_code, text, media)) 
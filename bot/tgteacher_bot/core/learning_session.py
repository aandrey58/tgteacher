"""Fence positional Telegram callbacks against concurrent catalog revisions."""
import re
from contextvars import ContextVar
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from tgteacher_bot.db.pool import get_pool

SESSION = ContextVar("learning_content_session", default=None)
STAGE = re.compile(r"^stage[1-8]_")
FAMILY = re.compile(r"^family_(?:select|continue|restart)_(\d+)$")
STAMP = re.compile(r"^(.*?)~(\d+)\.(\d+)$")


def versioned_markup(inline_keyboard, **kwargs):
    session = SESSION.get()
    if session:
        fid, version = session
        rows = []
        for row in inline_keyboard:
            buttons = []
            for button in row:
                data = button.callback_data
                if isinstance(data, str) and (STAGE.match(data) or FAMILY.match(data)):
                    data = f"{data}~{fid}.{version}"
                    if len(data.encode()) > 64:
                        raise ValueError("Telegram callback exceeds 64 bytes")
                    button = InlineKeyboardButton.de_json({**button.to_dict(), "callback_data": data}, None)
                buttons.append(button)
            rows.append(buttons)
        inline_keyboard = rows
    return InlineKeyboardMarkup(inline_keyboard, **kwargs)


def callback_matches(data, fid, version):
    match = STAMP.fullmatch(data)
    if match:
        return (int(match[2]), int(match[3])) == (fid, version)
    return version == 1


class LearningApplication(Application):
    async def process_update(self, update):
        query = getattr(update, "callback_query", None)
        user = getattr(update, "effective_user", None)
        data = query.data if query and isinstance(query.data, str) else ""
        stamp = STAMP.fullmatch(data)
        plain = stamp[1] if stamp else data
        family_match = FAMILY.fullmatch(plain)
        is_stage = bool(STAGE.match(plain))
        is_history = plain.startswith(("progress_", "families_page_")) or plain in ("my_progress", "choose_family")
        user_data = self.user_data.get(user.id, {}) if user else {}
        state5 = user_data.get("stage5") or {}
        is_text = bool(not query and getattr(update, "message", None) and getattr(update.message, "text", None)
                       and not update.message.text.startswith("/") and state5.get("awaiting_input"))
        if not user or not (family_match or is_stage or is_history or is_text):
            return await super().process_update(update)
        pool = await get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock_shared(847231, 1)")
                fid = int(family_match[1]) if family_match else (
                    state5.get("family_idx") if is_text else user_data.get("current_family_idx"))
                if is_stage and not fid:
                    fid = await conn.fetchval("SELECT family_idx FROM user_current_family WHERE user_id=$1", user.id)
                version = None
                if fid and (family_match or is_stage or is_text):
                    row = await conn.fetchrow("SELECT content_version, archived_at FROM families WHERE id=$1", fid)
                    if row and row["archived_at"] is None:
                        version = row["content_version"]
                    valid = version is not None
                    if is_stage:
                        valid = valid and callback_matches(data, fid, version)
                    elif family_match and stamp:
                        valid = valid and callback_matches(data, fid, version)
                    elif is_text:
                        valid = valid and state5.get("content_version", 1) == version
                    if not valid:
                        message = "Группа изменилась или недоступна. Откройте её заново. Прогресс сохранён."
                        if query:
                            await query.answer(message, show_alert=True)
                        else:
                            await update.message.reply_text(message)
                        return
                if family_match and fid and version:
                    for n in range(1, 9):
                        key = f'stage{n}'
                        state = user_data.get(key)
                        if state and (state.get('family_idx') != fid or state.get('content_version', 1) != version):
                            user_data.pop(key, None)
                if query and stamp:
                    query.update_callback_data(plain)
                token = SESSION.set((fid, version) if fid and version else None)
                try:
                    return await super().process_update(update)
                finally:
                    SESSION.reset(token)

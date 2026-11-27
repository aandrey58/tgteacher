"""Fence positional Telegram callbacks against concurrent catalog revisions."""
import asyncio
import logging
import re
import time
from contextlib import AsyncExitStack
from contextvars import ContextVar
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application
from tgteacher_bot.db.pool import get_pool

logger = logging.getLogger(__name__)
GUARDED_LIMIT = 4
GUARD_WAIT = 3
BUSY = "Учебный каталог сейчас обновляется или занят. Повторите действие через несколько секунд."
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
        # A guard occupies one connection while existing handlers use separate
        # connections. Limit guard holders to preserve headroom in the 15-slot pool.
        gate = getattr(self, "_catalog_gate", None)
        if gate is None:
            gate = self._catalog_gate = asyncio.BoundedSemaphore(GUARDED_LIMIT)
        try:
            await asyncio.wait_for(gate.acquire(), timeout=GUARD_WAIT)
        except asyncio.TimeoutError:
            rejection = BUSY
        else:
            try:
                rejection = await self._guarded_dispatch(update, query, user, data, stamp, plain,
                    family_match, is_stage, is_text, user_data, state5)
            finally:
                gate.release()
        # Never hold the catalog guard or a pool connection during an error reply.
        if rejection:
            if query:
                await query.answer(rejection, show_alert=True)
            elif getattr(update, "message", None):
                await update.message.reply_text(rejection)

    async def _guarded_dispatch(self, update, query, user, data, stamp, plain,
                                family_match, is_stage, is_text, user_data, state5):
        async with AsyncExitStack() as stack:
            pool = await get_pool()
            try:
                conn = await stack.enter_async_context(pool.acquire(timeout=GUARD_WAIT))
            except asyncio.TimeoutError:
                return BUSY
            await stack.enter_async_context(conn.transaction())
            if not await conn.fetchval("SELECT pg_try_advisory_xact_lock_shared(847231, 1)", timeout=GUARD_WAIT):
                return BUSY
            fid = int(family_match[1]) if family_match else (
                state5.get("family_idx") if is_text else user_data.get("current_family_idx"))
            if is_stage and not fid:
                fid = await conn.fetchval("SELECT family_idx FROM user_current_family WHERE user_id=$1", user.id)
            version = None
            if (is_stage or is_text) and not fid:
                return "Откройте группу заново."
            if fid and (family_match or is_stage or is_text):
                row = await conn.fetchrow("SELECT f.content_version, f.archived_at, (coalesce(f.target,\'VIP+FREE\') IN (\'FREE\',\'VIP+FREE\') OR EXISTS (SELECT 1 FROM users u WHERE u.user_id=$2 AND u.is_subscribed IS TRUE AND (u.subscription_until IS NULL OR u.subscription_until>now()))) AS accessible FROM families f WHERE f.id=$1", fid, user.id)
                if row and row["archived_at"] is None and row["accessible"]:
                    version = row["content_version"]
                valid = version is not None
                if is_stage:
                    valid = valid and callback_matches(data, fid, version)
                elif family_match and stamp:
                    valid = valid and callback_matches(data, fid, version)
                elif is_text:
                    valid = valid and state5.get("content_version", 1) == version
                if not valid:
                    return "Группа изменилась или недоступна. Откройте её заново. Прогресс сохранён."
            if family_match and fid and version:
                for n in range(1, 9):
                    key = f'stage{n}'
                    state = user_data.get(key)
                    if state and (state.get('family_idx') != fid or state.get('content_version', 1) != version):
                        user_data.pop(key, None)
            if query and stamp:
                # PTB TelegramObject is frozen; normalize this in-memory update only.
                object.__setattr__(query, 'data', plain)
            token = SESSION.set((fid, version) if fid and version else None)
            started = time.monotonic()
            try:
                await super().process_update(update)
            finally:
                SESSION.reset(token)
                if query and stamp:
                    object.__setattr__(query, "data", data)
                elapsed = time.monotonic() - started
                if elapsed >= 2:
                    logger.warning("Learning catalog guard held %.2fs", elapsed)
            return None

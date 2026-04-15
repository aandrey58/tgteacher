"""
Application implementation previously located in `bot/main.py`.

The entrypoint is intentionally kept as `python bot/main.py`.
"""

# NOTE: This file is intentionally a near-verbatim copy of the previous `bot/main.py`,
# with imports adjusted to package-relative form and file paths stabilized via `paths.py`.

import asyncio
import datetime
import logging
import os
import time
from logging.handlers import RotatingFileHandler

from dotenv import find_dotenv, load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from tgteacher_bot.core import paths
from tgteacher_bot.handlers.admin.admin_panel import get_admin_families_menu, get_admin_menu, register_admin_handlers
from tgteacher_bot.handlers.admin.admin_status import track_metrics
from tgteacher_bot.handlers.admin.admin_status_info import register_status_info_handlers
from tgteacher_bot.handlers.admin.admin_users import register_admin_users_handlers
from tgteacher_bot.utils.common import OK_MENU
from tgteacher_bot.services.exports.excel_export import EXPORTS_DIR
from tgteacher_bot.handlers.user.families import register_family_handlers
from tgteacher_bot.handlers.user.help import cleanup_help_index_job, register_help_handlers
from tgteacher_bot.services.legacy.legacy_users_collector import (
    collect_all_users_from_context,
    collect_context_users_job,
    process_legacy_users_job,
)
from tgteacher_bot.utils.message_id_store import load_message_ids, save_message_ids
from tgteacher_bot.handlers.user.progress import register_progress_handlers
from tgteacher_bot.handlers.user.settings import register_settings_handlers
from tgteacher_bot.handlers.user.sub import register_subscription_handlers
from tgteacher_bot.services.legacy.system_snapshots import (
    AUTO_SNAPSHOT_INTERVAL_SECONDS,
    auto_snapshot_job,
    init_system_snapshots_pg,
    retention_job,
)
from tgteacher_bot.db.user_repo import (
    add_or_update_user_pg,
    get_current_family_idx_pg,
    init_db_pg,
    mark_user_active_if_needed,
)


load_dotenv(find_dotenv())

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)

# Keep runtime log file behaviour (relative path), but default it to project root if run from elsewhere.
log_file_handler = RotatingFileHandler(
    "log.txt", maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
log_file_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logging.getLogger().addHandler(log_file_handler)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def get_main_menu(user_id=None):
    admin_ids = set()
    try:
        with open(paths.admins_path(), "r", encoding="utf-8") as f:
            admin_ids = set(line.strip() for line in f if line.strip())
    except Exception:
        pass
    buttons = [
        [InlineKeyboardButton("▶️ Начать обучение", callback_data="start_learning")],
        [InlineKeyboardButton("📚 Выбрать группу слов", callback_data="choose_family")],
        [InlineKeyboardButton("📈 Мой прогресс", callback_data="my_progress")],
        [InlineKeyboardButton("💎 Подписка", callback_data="subscription")],
        [InlineKeyboardButton("🆘 Помощь", callback_data="help")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="settings")],
    ]
    if user_id and str(user_id) in admin_ids:
        buttons.append(
            [InlineKeyboardButton("🛠️ Админ-панель", callback_data="admin_panel")]
        )
    return InlineKeyboardMarkup(buttons)


STAGE_KEYS = ["stage1", "stage2", "stage3", "stage4", "stage5", "stage6", "stage7", "stage8"]


def clear_stage_user_data(context):
    for k in STAGE_KEYS:
        context.user_data.pop(k, None)


@track_metrics
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    username = update.effective_user.username or ""
    first_name = update.effective_user.first_name or ""
    last_name = update.effective_user.last_name or ""
    await add_or_update_user_pg(user_id, username, first_name, last_name)

    try:
        from tgteacher_bot.services.legacy.legacy_users_collector import save_user_to_legacy_file

        save_user_to_legacy_file(user_id, username, first_name, last_name)
    except Exception as e:
        logger.error(f"Ошибка при сборе данных пользователя {user_id} в start: {e}")

    if first_name:
        welcome_text = (
            f"Здравствуйте, {first_name}!\n\n"
            "Добро пожаловать в бот для изучения немецкого языка. Здесь вы сможете эффективно расширить свой словарный запас.\n\n"
            "Пожалуйста, выберите один из пунктов меню, чтобы начать."
        )
    else:
        welcome_text = (
            "Здравствуйте!\n\n"
            "Добро пожаловать в бот для изучения немецкого языка. Здесь вы сможете эффективно расширить свой словарный запас.\n\n"
            "Пожалуйста, выберите один из пунктов меню, чтобы начать."
        )

    await update.message.reply_text(welcome_text, reply_markup=get_main_menu(user_id))


@track_metrics
async def main_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    await mark_user_active_if_needed(user_id, context)
    await query.answer()

    try:
        st5 = context.user_data.get("stage5")
        if st5:
            last_photo_id = st5.get("last_photo_message_id")
            if last_photo_id:
                try:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id, message_id=last_photo_id
                    )
                except Exception:
                    pass
        st7 = context.user_data.get("stage7")
        if st7:
            last_audio_id = st7.get("last_audio_message_id")
            if last_audio_id:
                try:
                    await context.bot.delete_message(
                        chat_id=update.effective_chat.id, message_id=last_audio_id
                    )
                except Exception:
                    pass

        family_id = context.user_data.get("current_family_idx")
        if not family_id:
            try:
                family_id = await get_current_family_idx_pg(user_id)
            except Exception:
                family_id = None
        if family_id:
            persisted5 = load_message_ids(user_id, family_id, 5)
            if isinstance(persisted5, dict):
                pid = persisted5.get("last_photo_message_id")
                if pid:
                    try:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id, message_id=pid
                        )
                    except Exception:
                        pass
                    try:
                        save_message_ids(user_id, family_id, 5, {"last_photo_message_id": None})
                    except Exception:
                        pass
            persisted7 = load_message_ids(user_id, family_id, 7)
            if isinstance(persisted7, dict):
                aid = persisted7.get("last_audio_message_id")
                if aid:
                    try:
                        await context.bot.delete_message(
                            chat_id=update.effective_chat.id, message_id=aid
                        )
                    except Exception:
                        pass
                    try:
                        save_message_ids(user_id, family_id, 7, {"last_audio_message_id": None})
                    except Exception:
                        pass
    except Exception:
        pass

    clear_stage_user_data(context)

    try:
        await query.edit_message_text("🏠 Главное меню", reply_markup=get_main_menu(user_id))
    except Exception:
        try:
            await query.message.reply_text("🏠 Главное меню", reply_markup=get_main_menu(user_id))
        except Exception:
            pass


def _register_handlers(application: Application):
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(main_menu_callback, pattern="^main_menu$"))

    register_family_handlers(application)
    register_progress_handlers(application)
    register_settings_handlers(application)
    register_help_handlers(application)
    register_subscription_handlers(application)
    register_admin_handlers(application)
    register_status_info_handlers(application)
    register_admin_users_handlers(application)


async def _post_startup(application: Application):
    await init_db_pg()
    try:
        await init_system_snapshots_pg()
    except Exception:
        logger.exception("Failed to init snapshots")

    # Scheduled jobs (kept as-is)
    try:
        jobq = application.job_queue
        if jobq:
            jobq.run_repeating(
                auto_snapshot_job, interval=AUTO_SNAPSHOT_INTERVAL_SECONDS, first=10
            )
            jobq.run_repeating(retention_job, interval=24 * 3600, first=120)
            jobq.run_repeating(cleanup_help_index_job, interval=24 * 3600, first=300)
            jobq.run_repeating(process_legacy_users_job, interval=6 * 3600, first=600)
            jobq.run_repeating(collect_context_users_job, interval=12 * 3600, first=1200)
    except Exception:
        logger.exception("Failed to register jobs")


def build_application() -> Application:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN is not set in environment (.env)")
    application = Application.builder().token(token).build()
    _register_handlers(application)
    application.post_init = _post_startup
    return application


def run_polling():
    application = build_application()
    application.run_polling(allowed_updates=Update.ALL_TYPES)


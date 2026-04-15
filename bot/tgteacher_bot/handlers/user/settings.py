from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler
from tgteacher_bot.db.user_repo import get_started_families_ids_pg, reset_family_progress_pg, mark_user_active_pg, mark_user_active_if_needed
from tgteacher_bot.db.families_repo import get_families_data_bulk
import tgteacher_bot.utils.families_data as families_data
from tgteacher_bot.handlers.admin.admin_status import track_metrics

def get_settings_menu():
    """Возвращает меню настроек."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🗑️ Сбросить весь прогресс', callback_data='settings_reset_progress')],
        [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
    ])

def get_reset_confirm_menu():
    """Возвращает меню подтверждения сброса прогресса."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да, сбросить всё', callback_data='settings_reset_confirm'),
            InlineKeyboardButton('❌ Отмена', callback_data='settings')
        ]
    ])

@track_metrics
async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Настройки'."""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    started_families = await get_started_families_ids_pg(user_id)
    
    if not started_families:
        text = "⚙️ Настройки"
    else:
        text = "⚙️ Настройки"
    
    await query.edit_message_text(text, reply_markup=get_settings_menu(), parse_mode='HTML')

@track_metrics
async def settings_reset_progress_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки 'Сбросить прогресс'."""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    started_families = await get_started_families_ids_pg(user_id)

    # MCP: Фильтруем удалённые/недоступные семьи, чтобы корректно показать количество
    valid_started_families = []
    if started_families:
        families_meta_all = await get_families_data_bulk(started_families)
        valid_started_families = [fid for fid in started_families if families_meta_all.get(fid)]
    
    if not valid_started_families:
        await query.edit_message_text(
            "🗑️ <b>Сброс прогресса</b>\n\nУ Вас нет прогресса для сброса.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]]),
            parse_mode='HTML'
        )
        return
    
    families_count = len(valid_started_families)
    text = (
        f"🗑️ <b>Сброс прогресса</b>\n\n"
        f"Вы уверены, что хотите сбросить весь прогресс?\n"
        f"Это удалит данные по {families_count} групп{'ам' if families_count > 1 else 'е'} слов.\n\n"
        f"⚠️ <b>Это действие нельзя отменить!</b>"
    )
    
    await query.edit_message_text(text, reply_markup=get_reset_confirm_menu(), parse_mode='HTML')

@track_metrics
async def settings_reset_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик подтверждения сброса прогресса."""
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    started_families = await get_started_families_ids_pg(user_id)

    # MCP: Повторяем фильтрацию, чтобы сбрасывать только реально существующие группы
    valid_started_families = []
    if started_families:
        families_meta_all = await get_families_data_bulk(started_families)
        valid_started_families = [fid for fid in started_families if families_meta_all.get(fid)]
    
    if not valid_started_families:
        await query.edit_message_text(
            "❌ Ошибка: прогресс уже был сброшен.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]])
        )
        return
        
    for family_idx in valid_started_families:
        await reset_family_progress_pg(user_id, family_idx)
    
    # Очищаем context.user_data для всех этапов и текущей группы слов
    for k in ['stage1', 'stage2', 'stage3', 'stage6', 'stage8', 'current_family_idx']:
        context.user_data.pop(k, None)
    
    families_count = len(valid_started_families)
    text = (
        f"✅ <b>Прогресс сброшен!</b>\n\n"
        f"Весь прогресс по {families_count} групп{'ам' if families_count > 1 else 'е'} слов был удалён.\n"
        f"Теперь ты можешь начать обучение заново!"
    )
    
    await query.edit_message_text(
        text, 
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]]),
        parse_mode='HTML'
    )

def register_settings_handlers(application):
    """Регистрирует обработчики настроек."""
    application.add_handler(CallbackQueryHandler(settings_callback, pattern='^settings$'))
    application.add_handler(CallbackQueryHandler(settings_reset_progress_callback, pattern='^settings_reset_progress$'))
    application.add_handler(CallbackQueryHandler(settings_reset_confirm_callback, pattern='^settings_reset_confirm$')) 
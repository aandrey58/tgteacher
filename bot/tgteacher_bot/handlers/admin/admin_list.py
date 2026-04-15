from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters
import os
from telegram.error import BadRequest
from tgteacher_bot.core import paths

ADMINS_PER_PAGE = 7

# Получаем список админов из файла
def get_admin_ids():
    admins_path = str(paths.admins_path())
    try:
        with open(admins_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception:
        return []

def get_admins_list_menu(page=0):
    admin_ids = get_admin_ids()
    total = len(admin_ids)
    start = page * ADMINS_PER_PAGE
    end = start + ADMINS_PER_PAGE
    page_admins = admin_ids[start:end]
    keyboard = []
    for admin_id in page_admins:
        keyboard.append([InlineKeyboardButton(f"🆔 {admin_id}", callback_data=f"admin_menu_{admin_id}")])
    nav_buttons = []
    if start > 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_list_admins_page_{page-1}'))
    if end < total:
        nav_buttons.append(InlineKeyboardButton('➡️ Далее', callback_data=f'admin_list_admins_page_{page+1}'))
    if nav_buttons:
        keyboard.append(nav_buttons)
    keyboard.append([InlineKeyboardButton('⬅️ Назад', callback_data='admin_list_admins')])
    return InlineKeyboardMarkup(keyboard)

async def admin_list_admins_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.replace('admin_list_admins_page_', ''))
    await query.edit_message_text(
        '👥 Список администраторов:',
        reply_markup=get_admins_list_menu(page)
    )

async def admin_add_admin_callback(update, context):
    query = update.callback_query
    await query.answer()
    msg = await query.message.reply_text(
        '🆔 Пришлите Telegram ID пользователя, которого хочешь сделать админом.',
        reply_markup=get_add_admin_cancel_menu()
    )
    context.user_data['waiting_for_admin_id'] = True
    context.user_data['add_admin_msg_id'] = msg.message_id
    context.user_data['add_admin_chat_id'] = msg.chat_id
    try:
        await query.delete_message()
    except Exception:
        pass

async def admin_cancel_add_admin_callback(update, context):
    query = update.callback_query
    await query.answer()
    context.user_data['waiting_for_admin_id'] = False
    await query.edit_message_text('👥 Управление администраторами:', reply_markup=get_admins_main_menu())

async def admin_add_admin_ok_callback(update, context):
    query = update.callback_query
    await query.answer()
    try:
        await query.delete_message()
    except Exception:
        pass
    await query.message.chat.send_message('👥 Список администраторов:', reply_markup=get_admins_list_menu(0))

async def admin_add_admin_text_handler(update, context):
    if not context.user_data.get('waiting_for_admin_id'):
        return None
    # Удаляем сообщение-инструкцию, если оно есть и мы реально ждём ID
    msg_id = context.user_data.get('add_admin_msg_id')
    chat_id = context.user_data.get('add_admin_chat_id')
    if msg_id and chat_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            pass
        context.user_data.pop('add_admin_msg_id', None)
        context.user_data.pop('add_admin_chat_id', None)
    admin_id = update.message.text.strip()
    if not admin_id.isdigit():
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
        except Exception:
            pass
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text='❌ Введи только числовой Telegram ID!',
            reply_markup=get_add_admin_cancel_menu()
        )
        context.user_data['add_admin_msg_id'] = msg.message_id
        context.user_data['add_admin_chat_id'] = msg.chat_id
        return
    admins_path = str(paths.admins_path())
    admins = set(get_admin_ids())
    if admin_id in admins:
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
        except Exception:
            pass
        msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f'⚠️ Админ {admin_id} уже есть в списке!',
            reply_markup=get_add_admin_cancel_menu()
        )
        context.user_data['add_admin_msg_id'] = msg.message_id
        context.user_data['add_admin_chat_id'] = msg.chat_id
        return
    with open(admins_path, 'a', encoding='utf-8') as f:
        f.write(f'{admin_id}\n')
    context.user_data['waiting_for_admin_id'] = False
    await update.message.reply_text(f'✅ Админ {admin_id} добавлен! Обновлённый список:', reply_markup=get_admins_list_menu(0))

def get_admins_main_menu():
    keyboard = [
        [InlineKeyboardButton('➕ Добавить администратора', callback_data='admin_add_admin')],
        [InlineKeyboardButton('🆔 Список администраторов', callback_data='admin_list_admins_list')],
        [InlineKeyboardButton('⬅️ В меню', callback_data='admin_panel')],
    ]
    return InlineKeyboardMarkup(keyboard)

# Кнопки для UX добавления админа
def get_add_admin_cancel_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton('❌ Отмена', callback_data='admin_cancel_add_admin')]])

def get_add_admin_ok_menu():
    return InlineKeyboardMarkup([[InlineKeyboardButton('✅ Ок', callback_data='admin_add_admin_ok')]])

def get_admin_menu(admin_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🗑️ Удалить', callback_data=f'admin_delete_confirm_{admin_id}')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_list_admins_list')]
    ])

def get_admin_delete_confirm_menu(admin_id):
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да', callback_data=f'admin_delete_{admin_id}'),
            InlineKeyboardButton('❌ Отмена', callback_data=f'admin_menu_{admin_id}')
        ]
    ])

async def admin_menu_callback(update, context):
    query = update.callback_query
    await query.answer()
    admin_id = query.data.replace('admin_menu_', '')
    await query.edit_message_text(
        f'🆔 Админ: {admin_id}',
        reply_markup=get_admin_menu(admin_id)
    )

async def admin_delete_confirm_callback(update, context):
    query = update.callback_query
    await query.answer()
    admin_id = query.data.replace('admin_delete_confirm_', '')
    await query.edit_message_text(
        f'Удалить администратора {admin_id}?',
        reply_markup=get_admin_delete_confirm_menu(admin_id)
    )

async def admin_delete_callback(update, context):
    query = update.callback_query
    await query.answer()
    admin_id = query.data.replace('admin_delete_', '')
    admins_path = str(paths.admins_path())
    admins = get_admin_ids()
    if admin_id in admins:
        admins.remove(admin_id)
        with open(admins_path, 'w', encoding='utf-8') as f:
            for a in admins:
                f.write(f'{a}\n')
        text = f'✅ Админ {admin_id} удалён.'
    else:
        text = f'⚠️ Админ {admin_id} не найден.'
    await query.edit_message_text(
        text,
        reply_markup=get_admins_list_menu(0)
    )

def register_admin_list_handlers(application):
    application.add_handler(CallbackQueryHandler(admin_add_admin_callback, pattern='^admin_add_admin$'))
    application.add_handler(CallbackQueryHandler(admin_cancel_add_admin_callback, pattern='^admin_cancel_add_admin$'))
    application.add_handler(CallbackQueryHandler(admin_add_admin_ok_callback, pattern='^admin_add_admin_ok$'))
    # MCP: УДАЛЯЕМ ДУБЛИРУЮЩИЙ ХЭНДЛЕР - он уже регистрируется в admin_panel.py
    # application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), admin_add_admin_text_handler))
    application.add_handler(CallbackQueryHandler(admin_menu_callback, pattern='^admin_menu_\d+$'))
    application.add_handler(CallbackQueryHandler(admin_delete_confirm_callback, pattern='^admin_delete_confirm_\d+$'))
    application.add_handler(CallbackQueryHandler(admin_delete_callback, pattern='^admin_delete_\d+$')) 
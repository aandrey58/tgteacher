from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, Document
from telegram.ext import ContextTypes, CallbackQueryHandler, MessageHandler, filters
from tgteacher_bot.utils.family_parser import parse_family_txt, load_all_families_from_dir
from tgteacher_bot.services.exports.family_upload import validate_and_extract_family_zip_with_ux
import os
import tgteacher_bot.utils.families_data as families_data
import shutil
from tgteacher_bot.handlers.admin.admin_list import (
    get_admins_list_menu, admin_list_admins_page_callback, get_admins_main_menu,
    admin_add_admin_callback, admin_cancel_add_admin_callback, admin_add_admin_ok_callback, admin_add_admin_text_handler,
    register_admin_list_handlers
)
from tgteacher_bot.db.families_repo import delete_family_from_pg
from tgteacher_bot.utils.common import OK_MENU
from tgteacher_bot.handlers.admin.admin_status import track_metrics
from tgteacher_bot.handlers.admin.admin_users import admin_users_callback
from tgteacher_bot.core import paths
from tgteacher_bot.services.legacy.system_snapshots import (
    capture_and_store_snapshot,
    get_last_snapshot,
    get_prev_snapshot,
    get_recent_snapshots,
    export_snapshots_xlsx,
    format_snapshot_text_for_msg,
    format_snapshot_text_with_trends,
)
import time
import datetime
from zoneinfo import ZoneInfo

# MCP: Первая кнопка теперь "Пользователи" (admin_users)
ADMIN_MENU_BUTTONS = [
    [InlineKeyboardButton('👤 Пользователи', callback_data='admin_users')],
    [InlineKeyboardButton('📚 Группы слов', callback_data='admin_families')],
    [InlineKeyboardButton('📢 Сделать рассылку', callback_data='admin_broadcast')],
    [InlineKeyboardButton('💎 Подписка', callback_data='admin_sub_pay')],
    [InlineKeyboardButton('👥 Список администраторов', callback_data='admin_list_admins')],
    [InlineKeyboardButton('🖥 Состояние сервера', callback_data='admin_status')],
    [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')],
]

ADMIN_FAMILIES_MENU_BUTTONS = [
    [InlineKeyboardButton('➕ Добавить группу слов', callback_data='admin_add_family')],
    [InlineKeyboardButton('👥 Для всех пользователей', callback_data='admin_list_families_all')],
    [InlineKeyboardButton('💎 Для пользователей с подпиской', callback_data='admin_list_families_vip')],
    [InlineKeyboardButton('👤 Для пользователей без подписки', callback_data='admin_list_families_free')],
    [InlineKeyboardButton('📄 Получить шаблон', callback_data='admin_get_template')],
    [InlineKeyboardButton('⬅️ Назад', callback_data='admin_panel')],
]

CANCEL_ADD_FAMILY_BUTTONS = [
    [InlineKeyboardButton('❌ Отмена', callback_data='admin_families')],
]

OK_BUTTON = [
    [InlineKeyboardButton('✅ Ок', callback_data='admin_add_family_ok')],
]

ADD_ADMIN_CANCEL_BUTTONS = [
    [InlineKeyboardButton('❌ Отмена', callback_data='admin_list_admins')],
]

ADD_ADMIN_OK_BUTTONS = [
    [InlineKeyboardButton('✅ Ок', callback_data='admin_add_admin_ok')],
]

LOGS_DISMISS_BUTTON = [
    [InlineKeyboardButton('✅ Ок', callback_data='admin_logs_dismiss')],
]

FAMILIES_PER_PAGE = 7

def get_admin_menu():
    return InlineKeyboardMarkup(ADMIN_MENU_BUTTONS)

def get_admin_families_menu():
    return InlineKeyboardMarkup(ADMIN_FAMILIES_MENU_BUTTONS)

def get_cancel_add_family_menu():
    return InlineKeyboardMarkup(CANCEL_ADD_FAMILY_BUTTONS)

def get_ok_button():
    return InlineKeyboardMarkup(OK_BUTTON)

def get_add_admin_cancel_menu():
    return InlineKeyboardMarkup(ADD_ADMIN_CANCEL_BUTTONS)

def get_add_admin_ok_menu():
    return InlineKeyboardMarkup(ADD_ADMIN_OK_BUTTONS)

def get_logs_dismiss_menu():
    return InlineKeyboardMarkup(LOGS_DISMISS_BUTTON)

def get_template_ok_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('✅ Спасибо', callback_data='admin_template_ok')]
    ])

def get_admin_families_list_menu(page=0, target_filter=None):
    families = families_data.ALL_FAMILIES_META
    
    # Фильтруем по target если указан фильтр
    if target_filter == 'vip':
        families = [f for f in families if f.get('target') in ['VIP', 'VIP+FREE', 'FREE', None]]
    elif target_filter == 'free':
        families = [f for f in families if f.get('target') in ['FREE', 'VIP+FREE', None]]
    elif target_filter == 'all' or target_filter is None:
        families = [f for f in families if f.get('target') in ['VIP+FREE', None]]
    
    total = len(families)
    total_pages = max(1, (total + FAMILIES_PER_PAGE - 1) // FAMILIES_PER_PAGE)
    start = page * FAMILIES_PER_PAGE
    end = start + FAMILIES_PER_PAGE
    page_families = families[start:end]
    keyboard = []
    for fam in page_families:
        # Добавляем эмодзи 💎 для VIP групп
        target = fam.get('target', 'VIP+FREE')
        if target == 'VIP':
            emoji = "💎📖"
        else:
            emoji = "📖"
        keyboard.append([InlineKeyboardButton(f"{emoji} {fam['name']}", callback_data=f"admin_family_{fam['id']}_{page}_{target_filter or 'all'}")])
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data=f'admin_list_families_page_{page-1}_{target_filter or "all"}'))
    else:
        nav_buttons.append(InlineKeyboardButton('⬅️ Назад', callback_data='noop'))
    nav_buttons.append(InlineKeyboardButton(f'{page+1}/{total_pages}', callback_data='noop'))
    if (page+1)*FAMILIES_PER_PAGE < total:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data=f'admin_list_families_page_{page+1}_{target_filter or "all"}'))
    else:
        nav_buttons.append(InlineKeyboardButton('Далее ➡️', callback_data='noop'))
    keyboard.append(nav_buttons)
    
    # Кнопка "Назад" возвращает в подменю групп слов
    back_callback = 'admin_families'
    keyboard.append([InlineKeyboardButton('⬅️ Назад', callback_data=back_callback)])
    return InlineKeyboardMarkup(keyboard)

def get_admin_family_menu(fam_id, last_page: int | None = None, target_filter: str | None = None):
    if last_page is not None and target_filter is not None:
        back_cb = f'admin_list_families_page_{last_page}_{target_filter}'
    elif last_page is not None:
        back_cb = f'admin_list_families_page_{last_page}_all'
    else:
        back_cb = 'admin_list_families'
    
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('📤 Выгрузить файлы', callback_data=f'admin_family_export_{fam_id}')],
        [InlineKeyboardButton('🗑️ Удалить', callback_data=f'admin_delete_family_confirm_{fam_id}')],
        [InlineKeyboardButton('⬅️ Назад', callback_data=back_cb)]
    ])

def get_admin_delete_confirm_menu(fam_id, last_page=None, target_filter=None):
    if last_page is not None and target_filter is not None:
        cancel_cb = f'admin_family_{fam_id}_{last_page}_{target_filter}'
    elif last_page is not None:
        cancel_cb = f'admin_family_{fam_id}_{last_page}_all'
    else:
        cancel_cb = 'admin_list_families'
    
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да', callback_data=f'admin_delete_family_{fam_id}'),
            InlineKeyboardButton('❌ Отмена', callback_data=cancel_cb)
        ]
    ])

def get_admin_status_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton('🔄 Обновить данные', callback_data='admin_status_refresh')],
        [InlineKeyboardButton('📝 Логи', callback_data='admin_status_logs')],
        [InlineKeyboardButton('📊 Снимки системы', callback_data='admin_status_snapshots')],
        [InlineKeyboardButton('❓ Справка', callback_data='admin_status_info')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_panel')]
    ])

@track_metrics
async def admin_panel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text('🛠️ Админ-панель', reply_markup=get_admin_menu())

@track_metrics
async def admin_families_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text('📚 <b>Группы слов (админ)</b>', reply_markup=get_admin_families_menu(), parse_mode='HTML')

@track_metrics
async def admin_add_family_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    msg = await query.message.reply_text(
        '📚 Пришлите <b>zip-архив</b> с новой группой слов.\nВ архиве должна быть папка с одним <i>.txt</i> <b>(обязательно)</b>, аудио <i>.mp3</i> и картинками  <i>.png</i>,  <i>.jpg</i>  или  <i>.jpeg</i> <b>(при необходимости)</b>.',
        reply_markup=get_cancel_add_family_menu(),
        parse_mode='HTML'
    )
    context.user_data['waiting_for_family_zip'] = True
    context.user_data['add_family_msg_id'] = msg.message_id
    context.user_data['add_family_chat_id'] = msg.chat_id
    try:
        await query.delete_message()
    except Exception:
        pass

@track_metrics
async def admin_cancel_add_family_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['waiting_for_family_zip'] = False
    await admin_families_callback(update, context)

@track_metrics
async def admin_list_families_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Если это возврат к списку, очищаем сохранённую страницу
    context.user_data.pop('admin_families_last_page', None)
    data = query.data
    page = 0
    target_filter = None
    
    if data.startswith('admin_list_families_page_'):
        try:
            parts = data.replace('admin_list_families_page_', '').split('_')
            page = int(parts[0])
            if len(parts) > 1:
                target_filter = parts[1] if parts[1] != 'all' else None
        except Exception:
            page = 0
            target_filter = None
    
    if not families_data.ALL_FAMILIES_META:
        text = '❗️ Групп слов пока нет.'
        await query.edit_message_text(text, reply_markup=get_admin_families_menu())
    else:
        # Определяем заголовок в зависимости от фильтра
        if target_filter == 'vip':
            title = '💎 <b>Группы слов для пользователей с подпиской:</b>'
        elif target_filter == 'free':
            title = '👤 <b>Группы слов для пользователей без подписки:</b>'
        else:
            title = '👥 <b>Группы слов для всех пользователей:</b>'
        
        # Добавляем счётчик количества семей
        filtered_families = families_data.ALL_FAMILIES_META
        if target_filter == 'vip':
            filtered_families = [f for f in filtered_families if f.get('target') in ['VIP', 'VIP+FREE', 'FREE', None]]
        elif target_filter == 'free':
            filtered_families = [f for f in filtered_families if f.get('target') in ['FREE', 'VIP+FREE', None]]
        else:
            filtered_families = [f for f in filtered_families if f.get('target') in ['VIP+FREE', None]]
        
        title += f'\n📊 <b>Общее количество:</b> {len(filtered_families)}'
        
        await query.edit_message_text(title, reply_markup=get_admin_families_list_menu(page, target_filter), parse_mode='HTML')

@track_metrics
async def admin_list_families_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Если это возврат к списку, очищаем сохранённую страницу
    context.user_data.pop('admin_families_last_page', None)
    
    # Подсчитываем количество семей для "всех"
    all_families = [f for f in families_data.ALL_FAMILIES_META if f.get('target') in ['VIP+FREE', None]]
    title = f'👥 <b>Группы слов для всех пользователей:</b>\n📊 <b>Общее количество:</b> {len(all_families)}'
    
    await query.edit_message_text(title, reply_markup=get_admin_families_list_menu(0, 'all'), parse_mode='HTML')

@track_metrics
async def admin_list_families_vip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Если это возврат к списку, очищаем сохранённую страницу
    context.user_data.pop('admin_families_last_page', None)
    
    # Подсчитываем количество семей для VIP
    vip_families = [f for f in families_data.ALL_FAMILIES_META if f.get('target') in ['VIP', 'VIP+FREE', 'FREE', None]]
    title = f'💎 <b>Группы слов для пользователей с подпиской:</b>\n📊 <b>Общее количество:</b> {len(vip_families)}'
    
    await query.edit_message_text(title, reply_markup=get_admin_families_list_menu(0, 'vip'), parse_mode='HTML')

@track_metrics
async def admin_list_families_free_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # MCP: Если это возврат к списку, очищаем сохранённую страницу
    context.user_data.pop('admin_families_last_page', None)
    
    # Подсчитываем количество семей для FREE
    free_families = [f for f in families_data.ALL_FAMILIES_META if f.get('target') in ['FREE', 'VIP+FREE', None]]
    title = f'👤 <b>Группы слов для пользователей без подписки:</b>\n📊 <b>Общее количество:</b> {len(free_families)}'
    
    await query.edit_message_text(title, reply_markup=get_admin_families_list_menu(0, 'free'), parse_mode='HTML')

@track_metrics
async def admin_family_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data_parts = query.data.replace('admin_family_', '').split('_')
    fam_id = int(data_parts[0])
    last_page = int(data_parts[1])
    target_filter = data_parts[2] if len(data_parts) > 2 else None
    
    context.user_data['admin_families_last_page'] = last_page
    context.user_data['admin_families_target_filter'] = target_filter
    
    fam = next((f for f in families_data.ALL_FAMILIES_META if f['id'] == fam_id), None)
    if not fam:
        await query.edit_message_text('Ошибка: группа слов не найдена.', reply_markup=get_admin_families_list_menu(last_page, target_filter))
        return
    text = f'📚 <b>Группа слов:</b> {fam["name"]}'
    # Кнопка "Назад" возвращает на сохранённую страницу, добавили кнопку выгрузки
    keyboard = get_admin_family_menu(fam_id, last_page, target_filter)
    await query.edit_message_text(text, reply_markup=keyboard, parse_mode='HTML')

@track_metrics
async def admin_delete_family_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    fam_id = int(query.data.replace('admin_delete_family_confirm_', ''))
    fam = next((f for f in families_data.ALL_FAMILIES_META if f['id'] == fam_id), None)
    if not fam:
        await query.edit_message_text('Ошибка: группа слов не найдена.', reply_markup=get_admin_families_list_menu(context.user_data.get('admin_families_last_page', 0), context.user_data.get('admin_families_target_filter', 'all')))
        return
    text = f'🗑 <b>Удалить группу слов:</b> {fam["name"]}?'
    last_page = context.user_data.get('admin_families_last_page', None)
    target_filter = context.user_data.get('admin_families_target_filter', None)
    await query.edit_message_text(text, reply_markup=get_admin_delete_confirm_menu(fam_id, last_page, target_filter), parse_mode='HTML')

@track_metrics
async def admin_delete_family_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    fam_id = int(query.data.replace('admin_delete_family_', ''))
    fam = next((f for f in families_data.ALL_FAMILIES_META if f['id'] == fam_id), None)
    if not fam:
        last_page = context.user_data.pop('admin_families_last_page', 0)
        target_filter = context.user_data.pop('admin_families_target_filter', 'all')
        await query.edit_message_text('Ошибка: группа слов не найдена.', reply_markup=get_admin_families_list_menu(last_page, target_filter))
        return
    folder_name = fam.get('folder_name')
    family_folder = os.path.join(families_data.families_dir, folder_name) if folder_name else None
    if family_folder and os.path.isdir(family_folder):
        shutil.rmtree(family_folder)
    # Удаляем из БД
    await delete_family_from_pg(fam['id'])
    # MCP: Обновляем метаданные семей после удаления
    await families_data._load_initial_families_meta()
    last_page = context.user_data.pop('admin_families_last_page', 0)
    target_filter = context.user_data.pop('admin_families_target_filter', 'all')
    await query.edit_message_text('✅ Группа слов удалена!', reply_markup=get_admin_families_list_menu(last_page, target_filter))

@track_metrics
async def admin_add_family_ok_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.delete_message()
    except Exception:
        pass
    await query.message.chat.send_message('📚 <b>Группы слов (админ)</b>', reply_markup=get_admin_families_menu(), parse_mode='HTML')

@track_metrics
async def admin_get_template_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    families_dir = paths.families_dir()
    template_path = os.path.join(families_dir, 'family_example.txt')
    if not os.path.exists(template_path):
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('✅ Ок', callback_data='admin_families')]
        ])
        await query.edit_message_text('Файл шаблона family_example.txt не найден!', reply_markup=keyboard)
        return
    await query.message.reply_document(
        open(template_path, 'rb'),
        filename='family_example.txt',
        caption='Шаблон для загрузки группы слов. Заполните по образцу и загрузите!',
        reply_markup=get_template_ok_menu()
    )

@track_metrics
async def admin_template_ok_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.delete_message()
    except Exception:
        pass

@track_metrics
async def admin_file_exists_ok_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.delete_message()
    except Exception:
        pass

@track_metrics
async def admin_file_format_ok_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.delete_message()
    except Exception:
        pass

@track_metrics
async def admin_list_admins_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        '👥 <b>Управление администраторами:</b>',
        reply_markup=get_admins_main_menu(),
        parse_mode='HTML'
    )

@track_metrics
async def admin_list_admins_list_callback(update, context):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        '👥 Список администраторов:',
        reply_markup=get_admins_list_menu(0)
    )

@track_metrics
async def admin_status_callback(update, context):
    from tgteacher_bot.handlers.admin.admin_status import get_status_text
    bot_start_time = context.application.bot_data.get('bot_start_time', 0)
    text = await get_status_text(context, bot_start_time)
    await update.callback_query.edit_message_text(text, reply_markup=get_admin_status_menu(), parse_mode='HTML')

@track_metrics
async def admin_status_refresh_callback(update, context):
    from tgteacher_bot.handlers.admin.admin_status import get_status_text
    bot_start_time = context.application.bot_data.get('bot_start_time', 0)
    text = await get_status_text(context, bot_start_time)
    await update.callback_query.edit_message_text(text, reply_markup=get_admin_status_menu(), parse_mode='HTML')

@track_metrics
async def admin_status_info_callback(update, context):
    from tgteacher_bot.handlers.admin.admin_status_info import get_status_info_menu
    query = update.callback_query
    await query.answer()
    await query.edit_message_text('❓ Что значат эти показатели?\nВыберите интересующий пункт:', reply_markup=get_status_info_menu())

@track_metrics
async def admin_status_logs_callback(update, context):
    query = update.callback_query
    await query.answer()
    log_path = 'log.txt'
    if not os.path.exists(log_path):
        await query.message.reply_text('Файл логов не найден.')
        return
    with open(log_path, 'rb') as f:
        await query.message.reply_document(
            f,
            filename='log.txt',
            caption='Последние логи',
            reply_markup=get_logs_dismiss_menu()
        )

@track_metrics
async def admin_logs_ok_callback(update, context):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        pass

@track_metrics
async def admin_status_snapshots_callback(update, context):
    query = update.callback_query
    await query.answer()
    auto_state = 'Вкл' if context.application.bot_data.get('snapshots_auto_enabled', True) else 'Выкл'
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton('📸 Сделать снимок сейчас', callback_data='snapshots_capture_now')],
        [InlineKeyboardButton('🕒 Последний снимок', callback_data='snapshots_last')],
        [InlineKeyboardButton('🗂 История (последние 20)', callback_data='snapshots_recent')],
        [InlineKeyboardButton('📤 Экспорт XLSX: 1ч', callback_data='snapshots_export_1h')],
        [InlineKeyboardButton('📤 Экспорт XLSX: 24ч', callback_data='snapshots_export_24h')],
        [InlineKeyboardButton('📤 Экспорт XLSX: 7д', callback_data='snapshots_export_7d')],
        [InlineKeyboardButton(f'⚙️ Автоснимки: {auto_state}', callback_data='snapshots_toggle_auto')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status')]
    ])
    await query.edit_message_text('📊 <b>Снимки системы</b>', reply_markup=keyboard, parse_mode='HTML')


@track_metrics
async def snapshots_capture_now_callback(update, context):
    query = update.callback_query
    await query.answer()
    # Антиспам: кулдаун 30 сек на пользователя
    now = time.time()
    last_ts = context.user_data.get('last_manual_snapshot_ts', 0)
    if now - last_ts < 30:
        await query.edit_message_text('⏱️ Подождите пару секунд перед следующим снимком (30 сек кулдаун).', reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status_snapshots')]
        ]))
        return
    context.user_data['last_manual_snapshot_ts'] = now
    data = await capture_and_store_snapshot(context)
    prev = await get_prev_snapshot()
    text = format_snapshot_text_with_trends(data, prev)
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status_snapshots')]
    ]), parse_mode='HTML')


@track_metrics
async def snapshots_last_callback(update, context):
    query = update.callback_query
    await query.answer()
    data = await get_last_snapshot()
    if not data:
        await query.edit_message_text('Пока нет ни одного снимка.', reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status_snapshots')]
        ]))
        return
    prev = await get_prev_snapshot()
    text = format_snapshot_text_with_trends(data, prev)
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status_snapshots')]
    ]), parse_mode='HTML')


@track_metrics
async def snapshots_recent_callback(update, context):
    query = update.callback_query
    await query.answer()
    snaps = await get_recent_snapshots(20)
    if not snaps:
        await query.edit_message_text('История пуста.', reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status_snapshots')]
        ]))
        return
    # Короткий список: ts, cpu, ram_proc, db_ping
    lines = ['🗂 <b>Последние 20 снимков\n</b>']
    for s in snaps:
        ts_raw = s.get('ts', '-')
        ts = ts_raw
        if isinstance(ts_raw, str):
            try:
                dt = datetime.datetime.fromisoformat(ts_raw.replace('Z', '+00:00'))
                dt_msk = dt.astimezone(ZoneInfo('Europe/Moscow'))
                ts = dt_msk.strftime('%d-%m-%Y %H:%M:%S')
            except Exception:
                ts = ts_raw
        cpu = s.get('cpu_percent_avg', '-')
        ram = s.get('ram_process_mb', '-')
        dbp = s.get('db_ping_ms', '-')
        lines.append(f"• {ts} | CPU {cpu}% | RAM {ram}MB | DB {dbp}мс")
    text = '\n'.join(lines)
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status_snapshots')]
    ]), parse_mode='HTML')


@track_metrics
async def snapshots_export_callback(update, context):
    query = update.callback_query
    await query.answer()
    period = '24h'
    if query.data == 'snapshots_export_1h':
        period = '1h'
    elif query.data == 'snapshots_export_7d':
        period = '7d'
    file_xlsx = await export_snapshots_xlsx(period)
    if not file_xlsx:
        await query.edit_message_text('Нет данных для экспорта за выбранный период.', reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status_snapshots')]
        ]))
        return
    try:
        thanks_kb = InlineKeyboardMarkup([[InlineKeyboardButton('✅ Спасибо', callback_data='snapshots_export_thanks')]])
        with open(file_xlsx, 'rb') as f2:
            await query.message.reply_document(f2, filename=os.path.basename(file_xlsx), caption=f'Экспорт снимков XLSX ({period})', reply_markup=thanks_kb)
    except Exception:
        await query.message.reply_text('Не удалось отправить файл экспорта.')
    # Возврат в меню
    await query.edit_message_text('📊 <b>Снимки системы</b>', reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton('📸 Сделать снимок сейчас', callback_data='snapshots_capture_now')],
        [InlineKeyboardButton('🕒 Последний снимок', callback_data='snapshots_last')],
        [InlineKeyboardButton('🗂 История (последние 20)', callback_data='snapshots_recent')],
        [InlineKeyboardButton('📤 Экспорт XLSX: 1ч', callback_data='snapshots_export_1h')],
        [InlineKeyboardButton('📤 Экспорт XLSX: 24ч', callback_data='snapshots_export_24h')],
        [InlineKeyboardButton('📤 Экспорт XLSX: 7д', callback_data='snapshots_export_7d')],
        [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status')]
    ]), parse_mode='HTML')


@track_metrics
async def snapshots_toggle_auto_callback(update, context):
    query = update.callback_query
    await query.answer()
    current = context.application.bot_data.get('snapshots_auto_enabled', True)
    context.application.bot_data['snapshots_auto_enabled'] = not current
    await admin_status_snapshots_callback(update, context)

@track_metrics
async def snapshots_export_thanks_callback(update, context):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        try:
            await query.edit_message_text('✅ Спасибо!', reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ Назад', callback_data='admin_status_snapshots')]]))
        except Exception:
            pass

def register_admin_handlers(application):
    # Callback-обработчики
    application.add_handler(CallbackQueryHandler(admin_panel_callback, pattern='^admin_panel$'))
    application.add_handler(CallbackQueryHandler(admin_families_callback, pattern='^admin_families$'))
    application.add_handler(CallbackQueryHandler(admin_add_family_callback, pattern='^admin_add_family$'))
    application.add_handler(CallbackQueryHandler(admin_cancel_add_family_callback, pattern='^admin_cancel_add_family$'))
    application.add_handler(CallbackQueryHandler(admin_list_families_callback, pattern='^admin_list_families$'))
    application.add_handler(CallbackQueryHandler(admin_list_families_callback, pattern='^admin_list_families_page_\d+_\w+$'))
    application.add_handler(CallbackQueryHandler(admin_list_families_all_callback, pattern='^admin_list_families_all$'))
    application.add_handler(CallbackQueryHandler(admin_list_families_vip_callback, pattern='^admin_list_families_vip$'))
    application.add_handler(CallbackQueryHandler(admin_list_families_free_callback, pattern='^admin_list_families_free$'))
    application.add_handler(CallbackQueryHandler(admin_family_menu_callback, pattern='^admin_family_\d+_\d+_\w+$'))
    application.add_handler(CallbackQueryHandler(admin_delete_family_confirm_callback, pattern='^admin_delete_family_confirm_\d+$'))
    application.add_handler(CallbackQueryHandler(admin_delete_family_callback, pattern='^admin_delete_family_\d+$'))
    application.add_handler(CallbackQueryHandler(admin_add_family_ok_callback, pattern='^admin_add_family_ok$'))
    application.add_handler(CallbackQueryHandler(admin_file_exists_ok_callback, pattern='^admin_file_exists_ok$'))
    application.add_handler(CallbackQueryHandler(admin_file_format_ok_callback, pattern='^admin_file_format_ok$'))
    application.add_handler(CallbackQueryHandler(admin_get_template_callback, pattern='^admin_get_template$'))
    application.add_handler(CallbackQueryHandler(admin_template_ok_callback, pattern='^admin_template_ok$'))
    application.add_handler(CallbackQueryHandler(admin_list_admins_callback, pattern='^admin_list_admins$'))
    application.add_handler(CallbackQueryHandler(admin_list_admins_list_callback, pattern='^admin_list_admins_list$'))
    application.add_handler(CallbackQueryHandler(admin_list_admins_page_callback, pattern='^admin_list_admins_page_\d+$'))
    application.add_handler(CallbackQueryHandler(admin_add_admin_callback, pattern='^admin_add_admin$'))
    application.add_handler(CallbackQueryHandler(admin_cancel_add_admin_callback, pattern='^admin_cancel_add_admin$'))
    application.add_handler(CallbackQueryHandler(admin_add_admin_ok_callback, pattern='^admin_add_admin_ok$'))
    application.add_handler(CallbackQueryHandler(admin_status_callback, pattern='^admin_status$'))
    application.add_handler(CallbackQueryHandler(admin_status_refresh_callback, pattern='^admin_status_refresh$'))
    application.add_handler(CallbackQueryHandler(admin_status_info_callback, pattern='^admin_status_info$'))
    application.add_handler(CallbackQueryHandler(admin_status_logs_callback, pattern='^admin_status_logs$'))
    application.add_handler(CallbackQueryHandler(admin_logs_ok_callback, pattern='^admin_logs_dismiss$'))
    application.add_handler(CallbackQueryHandler(admin_status_snapshots_callback, pattern='^admin_status_snapshots$'))
    application.add_handler(CallbackQueryHandler(snapshots_capture_now_callback, pattern='^snapshots_capture_now$'))
    application.add_handler(CallbackQueryHandler(snapshots_last_callback, pattern='^snapshots_last$'))
    application.add_handler(CallbackQueryHandler(snapshots_recent_callback, pattern='^snapshots_recent$'))
    application.add_handler(CallbackQueryHandler(snapshots_export_callback, pattern='^snapshots_export_(1h|24h|7d)$'))
    application.add_handler(CallbackQueryHandler(snapshots_export_thanks_callback, pattern='^snapshots_export_thanks$'))
    application.add_handler(CallbackQueryHandler(snapshots_toggle_auto_callback, pattern='^snapshots_toggle_auto$'))
    application.add_handler(CallbackQueryHandler(admin_users_callback, pattern='^admin_users$'))
    # MCP: не забудь зарегистрировать хэндлеры из admin_users.py для подменю, пагинации и просмотра инфы о юзере

    from tgteacher_bot.handlers.admin.admin_broadcast import (
        admin_broadcast_callback,
        admin_broadcast_target_callback,
        admin_broadcast_cancel_callback,
        admin_broadcast_hide_callback,
        admin_broadcast_message_handler,
        admin_broadcast_send_callback,
    )
    application.add_handler(CallbackQueryHandler(admin_broadcast_callback, pattern='^admin_broadcast$'))
    application.add_handler(CallbackQueryHandler(admin_broadcast_target_callback, pattern='^admin_broadcast_target_(all|subs|nosubs)$'))
    application.add_handler(CallbackQueryHandler(admin_broadcast_cancel_callback, pattern='^admin_broadcast_cancel$'))
    application.add_handler(CallbackQueryHandler(admin_broadcast_hide_callback, pattern='^admin_broadcast_hide$'))
    application.add_handler(CallbackQueryHandler(admin_broadcast_send_callback, pattern='^admin_broadcast_send$'))

    application.add_handler(MessageHandler(filters.Document.ALL | filters.ANIMATION, validate_and_extract_family_zip_with_ux, block=False))
    
    async def admin_wrong_file_handler(update, context):
        # Если ждём ID админа, не мешаем другому хэндлеру
        if context.user_data.get('waiting_for_admin_id'):
            return None
        if not context.user_data.get('waiting_for_family_zip'):
            return None
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
        except Exception as e:
            import logging
            logging.error(f'[TGTeacher] Не удалось удалить сообщение пользователя с неправильным файлом: {e}', exc_info=True)
        await update.message.reply_text(
            '❌ Пожалуйста, пришли .txt-файл по шаблону.',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('✅ Ок', callback_data='admin_file_format_ok')]
            ])
        )
        return None

    # MCP: ОТЛАДОЧНЫЕ ОБЕРТКИ ДЛЯ ХЭНДЛЕРОВ
    async def debug_admin_add_admin_text_handler(update, context):
        print('MCP DEBUG: debug_admin_add_admin_text_handler вызван')
        result = await admin_add_admin_text_handler(update, context)
        print(f'MCP DEBUG: admin_add_admin_text_handler вернул: {result}')
        return result
    
    async def debug_admin_wrong_file_handler(update, context):
        print('MCP DEBUG: debug_admin_wrong_file_handler вызван')
        result = await admin_wrong_file_handler(update, context)
        print(f'MCP DEBUG: admin_wrong_file_handler вернул: {result}')
        return result

    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), debug_admin_add_admin_text_handler, block=False))
    
    application.add_handler(MessageHandler(filters.ChatType.PRIVATE & filters.TEXT & (~filters.COMMAND), debug_admin_wrong_file_handler, block=False))

    # MCP: экспорт файлов семьи
    from tgteacher_bot.services.exports.admin_family_export import admin_family_export_callback, admin_family_export_thanks_callback
    application.add_handler(CallbackQueryHandler(admin_family_export_callback, pattern='^admin_family_export_\d+$'))
    application.add_handler(CallbackQueryHandler(admin_family_export_thanks_callback, pattern='^admin_family_export_thanks$'))

    application.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & (
            (filters.TEXT & (~filters.COMMAND)) | filters.PHOTO | filters.VIDEO | filters.Document.ALL
        ),
        admin_broadcast_message_handler,
        block=False
    ), group=-1)

    register_admin_list_handlers(application)

    # MCP: Регистрируем хэндлеры для управления подписками
    from tgteacher_bot.services.payments.sub_pay import register_admin_sub_pay_handlers
    register_admin_sub_pay_handlers(application)

def update_families():
    families_data.ALL_FAMILIES_META.clear()
    families_data.ALL_FAMILIES_META.extend(load_all_families_from_dir(families_data.families_dir)) 
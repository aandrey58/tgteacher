from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile
from telegram.ext import ContextTypes
import os
import io
import shutil
import tempfile
import tgteacher_bot.utils.families_data as families_data
from tgteacher_bot.handlers.admin.admin_status import track_metrics

@track_metrics
async def admin_family_export_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        family_id = int(query.data.replace('admin_family_export_', ''))
    except Exception:
        await query.edit_message_text('❌ Ошибка: не удалось определить группу слов.')
        return

    fam = next((f for f in families_data.ALL_FAMILIES_META if f['id'] == family_id), None)
    if not fam:
        await query.edit_message_text('❌ Группа слов не найдена.')
        return

    folder_name = fam.get('folder_name') or ''
    family_folder = os.path.join(families_data.families_dir, folder_name)
    if not folder_name or not os.path.isdir(family_folder):
        await query.edit_message_text('❌ Папка группы слов не найдена на сервере.')
        return

    # Готовим zip в память (tempfile -> BytesIO)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, f'{folder_name}.zip')
            shutil.make_archive(base_name=zip_path[:-4], format='zip', root_dir=family_folder)
            with open(zip_path, 'rb') as f:
                data = f.read()
        bio = io.BytesIO(data)
        bio.seek(0)
    except Exception as e:
        await query.edit_message_text(f'❌ Ошибка упаковки архива: {type(e).__name__}: {e}')
        return

    kb = InlineKeyboardMarkup([[InlineKeyboardButton('✅ Спасибо', callback_data='admin_family_export_thanks')]])
    await query.message.reply_document(
        document=InputFile(bio, filename=f'{folder_name}.zip'),
        caption=f'📤 Архив группы слов: {fam.get("name")}',
        reply_markup=kb
    )

@track_metrics
async def admin_family_export_thanks_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception:
        try:
            await query.edit_message_text('✅ Спасибо!')
        except Exception:
            pass 
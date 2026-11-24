from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, InputFile
from telegram.ext import ContextTypes
import os
import io
import shutil
import tempfile
import zipfile
from tgteacher_bot.db.tts_repo import get_audio_bytes
from tgteacher_bot.db.drafts_repo import get_published_source
from tgteacher_bot.db.content_versions import get_current_content
from tgteacher_bot.utils.family_renderer import render_family_txt
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
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = os.path.join(tmpdir, 'family.zip')
            current = await get_current_content(family_id)
            if current['content_version'] > 1 or not folder_name:
                source = render_family_txt(current)
                archive_name = f'family-{family_id}'
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as archive:
                    archive.writestr(f'{archive_name}/lesson.txt', source)
                    media = {t['image'] for t in current['stage5_tasks']} | {t['audio'] for t in current['stage7_tasks']}
                    for name in sorted(media):
                        if name in {t['audio'] for t in current['stage7_tasks']}:
                            data = await get_audio_bytes(name)
                            if data is not None:
                                archive.writestr(f'{archive_name}/{name}', data)
                                continue
                        if not folder_name or os.path.basename(name) != name:
                            raise ValueError('Некорректный медиафайл')
                        matches = [f for f in os.listdir(family_folder) if f.casefold() == name.casefold()]
                        if len(matches) != 1:
                            raise ValueError('Медиафайл не найден или неоднозначен: ' + name)
                        file_path = os.path.join(family_folder, matches[0])
                        if not os.path.isfile(file_path) or os.path.islink(file_path):
                            raise ValueError('Некорректный медиафайл')
                        archive.write(file_path, f'{archive_name}/{name}')
                folder_name = archive_name
            elif folder_name and os.path.isdir(family_folder):
                shutil.make_archive(base_name=zip_path[:-4], format='zip', root_dir=os.path.dirname(family_folder),
                                    base_dir=os.path.basename(family_folder))
            else:
                source = await get_published_source(family_id)
                if source is None:
                    await query.edit_message_text('❌ Папка группы слов не найдена на сервере.')
                    return
                folder_name = f'family-{family_id}'
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as archive:
                    archive.writestr(f'{folder_name}/lesson.txt', source)
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
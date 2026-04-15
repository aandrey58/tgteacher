import os
import shutil
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
import tgteacher_bot.utils.families_data as families_data
from tgteacher_bot.db.families_repo import save_family_to_pg # Импортируем функцию сохранения в SQLite
import logging
import pyzipper
from tgteacher_bot.utils.family_parser import parse_family_txt # Импортируем парсер
from tgteacher_bot.core import paths
import struct

ALLOWED_EXTENSIONS = {'.txt', '.png', '.jpg', '.jpeg', '.webp', '.mp3'}

FAMILIES_DIR = str(paths.families_dir())

class FamilyArchiveError(Exception):
    pass

def is_allowed_file(filename):
    ext = os.path.splitext(filename)[1].lower()
    return ext in ALLOWED_EXTENSIONS

def decode_filename(raw_bytes, utf8_flag: bool | None = None):
    # Порядок проб: если стоит UTF-8 флаг — сначала utf-8, иначе cp437 по спецификации ZIP
    encodings = []
    if utf8_flag:
        encodings = ['utf-8']
    else:
        encodings = ['cp437', 'utf-8']
    # Фоллбеки для «кривых» архиваторов под Windows
    encodings += ['cp866', 'cp1251']
    for enc in encodings:
        try:
            return raw_bytes.decode(enc)
        except Exception:
            continue
    # Последний шанс: не падать, а вернуть максимально близко к исходнику
    try:
        return raw_bytes.decode('latin1', errors='replace')
    except Exception:
        logging.warning(f'Не удалось декодировать имя файла окончательно: {raw_bytes!r}')
        raise FamilyArchiveError('Не удалось декодировать имя файла в архиве! Используй латиницу или стандартные кодировки.')

def _try_decode_best(raw_bytes: bytes) -> str:
	"""Подбирает лучшую декодировку для пути с кириллицей: предпочитает UTF-8, затем cp1251/cp866, затем cp437/cp850, latin1."""
	candidates: list[tuple[str, str]] = []
	for enc in ['utf-8', 'cp1251', 'cp866', 'cp850', 'cp437', 'latin1']:
		try:
			decoded = raw_bytes.decode(enc)
			candidates.append((enc, decoded))
		except Exception:
			continue
	if not candidates:
		return raw_bytes.decode('latin1', errors='replace')
	# Эвристика: выбираем строку с наибольшим кол-вом кириллических букв и наименьшим числом замен ""
	def score(s: str) -> tuple[int, int]:
		cyr = sum(1 for ch in s if '\u0400' <= ch <= '\u04FF')
		repl = s.count('\uFFFD')
		return (cyr, -repl)
	best = max(candidates, key=lambda kv: score(kv[1]))
	return best[1]


def _choose_best_string(options: list[str], original: str) -> str:
	"""Выбирает лучшую строку по эвристике количества кириллицы и отсутствию; при равенстве — ближайшую к оригиналу по длине."""
	if not options:
		return original
	def score(s: str) -> tuple[int, int, int]:
		cyr = sum(1 for ch in s if '\u0400' <= ch <= '\u04FF')
		repl = s.count('\uFFFD')
		return (cyr, -repl, -abs(len(s) - len(original)))
	return max(options, key=score)


def _fix_common_mojibake(name: str) -> str:
	"""Фиксит типичные кракозябры: latin1->utf8, cp1251->utf8 и т.п., возвращает лучшую версию либо исходник."""
	candidates: list[str] = [name]
	try:
		candidates.append(name.encode('latin1').decode('utf-8'))
	except Exception:
		pass
	try:
		candidates.append(name.encode('cp1251').decode('utf-8'))
	except Exception:
		pass
	try:
		# Обратный случай: utf-8 считали как cp1251
		candidates.append(name.encode('utf-8').decode('cp1251'))
	except Exception:
		pass
	try:
		# Иногда помогает двойной прогон через cp437
		candidates.append(name.encode('cp437', errors='ignore').decode('cp1251', errors='ignore'))
	except Exception:
		pass
	return _choose_best_string(list(dict.fromkeys([c for c in candidates if c])), name)


def _extract_unicode_name_from_extra(extra: bytes) -> str | None:
	"""Парсит Info-ZIP Unicode Path Extra Field (0x7075) и возвращает имя в UTF-8, если есть."""
	try:
		offset = 0
		while offset + 4 <= len(extra):
			header_id, data_size = struct.unpack_from('<HH', extra, offset)
			offset += 4
			if offset + data_size > len(extra):
				break
			data = extra[offset:offset+data_size]
			offset += data_size
			if header_id == 0x7075 and len(data) >= 5:
				# u8[1]=version, u32=nameCRC32, rest = utf8 name
				version = data[0]
				if version != 1:
					continue
				unicode_name = data[5:].decode('utf-8', errors='strict')
				return unicode_name
	except Exception:
		return None
	return None

def _normalize_image_file(path: str) -> tuple[bool, str | None]:
    """
    Нормализует изображение на месте. Возвращает (was_renamed, new_filename).
    - JPG/JPEG: EXIF-ориентация, RGB, повторное сохранение JPEG (quality=85, optimize).
    - PNG: при наличии альфы — сплющиваем на белый фон; сохраняем PNG; оставляем имя.
    - WEBP: конвертируем в JPEG и переименовываем на .jpg.
    В случае ошибок — логируем и не роняем процесс.
    """
    try:
        from PIL import Image, ImageOps  # type: ignore
    except Exception as e:
        logging.warning(f'[FAMILY NORMALIZE] PIL недоступен: {type(e).__name__}: {e}')
        return (False, None)

    ext = os.path.splitext(path)[1].lower()
    try:
        with Image.open(path) as im:
            try:
                im = ImageOps.exif_transpose(im)
            except Exception:
                pass
            if ext in ('.jpg', '.jpeg'):
                if im.mode not in ('RGB', 'L'):
                    im = im.convert('RGB')
                elif im.mode == 'L':
                    im = im.convert('RGB')
                im.save(path, format='JPEG', quality=85, optimize=True)
                logging.info(f"[FAMILY NORMALIZE] JPEG нормализован: {os.path.basename(path)}")
                return (False, None)
            elif ext == '.png':
                if im.mode in ('RGBA', 'LA'):
                    bg = Image.new('RGB', im.size, (255, 255, 255))
                    if im.mode == 'LA':
                        alpha = im.getchannel('A') if 'A' in im.getbands() else None
                    else:
                        alpha = im.split()[-1]
                    if alpha is not None:
                        bg.paste(im.convert('RGB'), mask=alpha)
                        im = bg
                    else:
                        im = im.convert('RGB')
                    # сохраняем PNG без альфы
                    im.save(path, format='PNG', optimize=True)
                else:
                    # можно оставить как есть, но пересохраним для чистоты
                    im.save(path, format='PNG', optimize=True)
                logging.info(f"[FAMILY NORMALIZE] PNG нормализован: {os.path.basename(path)}")
                return (False, None)
            elif ext == '.webp':
                # конвертируем в JPEG с новым именем
                if im.mode not in ('RGB', 'L'):
                    im = im.convert('RGB')
                elif im.mode == 'L':
                    im = im.convert('RGB')
                dir_name = os.path.dirname(path)
                base = os.path.splitext(os.path.basename(path))[0]
                new_name = base + '.jpg'
                new_path = os.path.join(dir_name, new_name)
                im.save(new_path, format='JPEG', quality=85, optimize=True)
                try:
                    os.remove(path)
                except Exception:
                    pass
                logging.info(f"[FAMILY NORMALIZE] WEBP -> JPEG: {os.path.basename(path)} -> {new_name}")
                return (True, new_name)
            else:
                # Другие форматы не трогаем
                return (False, None)
    except Exception as e:
        logging.warning(f"[FAMILY NORMALIZE] Ошибка нормализации '{os.path.basename(path)}': {type(e).__name__}: {e}")
        return (False, None)


def _normalize_family_images(folder_path: str, parsed_family_data: dict) -> dict:
    """
    Пробегает по всем файлам группы, нормализует изображения и обновляет ссылки в stage5_tasks при переименованиях.
    Возвращает обновлённый parsed_family_data.
    """
    rename_map: dict[str, str] = {}
    for fname in os.listdir(folder_path):
        fpath = os.path.join(folder_path, fname)
        if not os.path.isfile(fpath):
            continue
        ext = os.path.splitext(fname)[1].lower()
        if ext not in ('.jpg', '.jpeg', '.png', '.webp'):
            continue
        was_renamed, new_name = _normalize_image_file(fpath)
        if was_renamed and new_name and new_name != fname:
            rename_map[fname] = new_name

    if rename_map and parsed_family_data and parsed_family_data.get('stage5_tasks'):
        for task in parsed_family_data['stage5_tasks']:
            img = task.get('image')
            if img and img in rename_map:
                task['image'] = rename_map[img]
                logging.info(f"[FAMILY NORMALIZE] Обновлено имя изображения в задаче: {img} -> {task['image']}")

    return parsed_family_data

def _mojibake_variants(name: str) -> list[str]:
	variants: list[str] = []
	seen: set[str] = set()
	def add(s: str):
		if s not in seen:
			seen.add(s)
			variants.append(s)
	add(name)
	pairs = [
		('utf-8', 'cp1251'), ('cp1251', 'utf-8'),
		('utf-8', 'cp866'), ('cp866', 'utf-8'),
		('utf-8', 'cp437'), ('cp437', 'utf-8'),
		('utf-8', 'latin1'), ('latin1', 'utf-8'),
		('cp1251', 'cp866'), ('cp866', 'cp1251'),
		('cp1251', 'cp437'), ('cp437', 'cp1251'),
	]
	for src, dst in pairs:
		try:
			add(name.encode(src, errors='ignore').decode(dst, errors='ignore'))
		except Exception:
			pass
	return variants

def _digits_of_name(name: str) -> str:
	base = os.path.splitext(os.path.basename(name))[0]
	return ''.join([ch for ch in base if ch.isdigit()])


def _normalize_family_audios(folder_path: str, parsed_family_data: dict) -> dict:
	"""Переименовывает .mp3 с кракозябрами в имена из TXT (этап 7) безопасно: только при однозначном сопоставлении и совпадении цифровых меток."""
	expected_audios: set[str] = set()
	try:
		for task in parsed_family_data.get('stage7_tasks') or []:
			fname = (task or {}).get('audio')
			if fname:
				expected_audios.add(fname)
	except Exception:
		pass
	if not expected_audios:
		return parsed_family_data
	# Собираем существующие mp3
	existing = [f for f in os.listdir(folder_path) if f.lower().endswith('.mp3')]
	existing_set = set(existing)
	if not existing:
		return parsed_family_data
	# Предвычислим варианты для каждого файла и для каждого ожидаемого имени
	real_to_variants: dict[str, set[str]] = {}
	for real in existing:
		real_to_variants[real] = set(_mojibake_variants(real)) | {real}
	want_to_variants: dict[str, set[str]] = {}
	for want in expected_audios:
		want_to_variants[want] = set(_mojibake_variants(want)) | {want}
	assigned_existing: set[str] = set()
	for want in sorted(expected_audios):
		if want in existing_set:
			continue
		# Кандидаты — те реальные файлы, которые по вариантам равны want, либо want-варианты равны реальному
		candidates: list[str] = []
		want_digits = _digits_of_name(want)
		for real in existing:
			if real in assigned_existing:
				continue
			match_by_real = (want in real_to_variants.get(real, set()))
			match_by_want = (real in want_to_variants.get(want, set()))
			if match_by_real or match_by_want:
				# Доп. безопасная проверка: совпадают цифровые подписи (например, "...1" vs "...1")
				real_digits = _digits_of_name(real)
				if want_digits == real_digits:
					candidates.append(real)
		# Разрешаем переименование только при однозначном кандидате
		if len(candidates) == 1:
			candidate = candidates[0]
			if candidate != want:
				old_path = os.path.join(folder_path, candidate)
				new_path = os.path.join(folder_path, want)
				try:
					os.rename(old_path, new_path)
					logging.info(f"[FAMILY NORMALIZE] MP3 переименован: {candidate} -> {want}")
					assigned_existing.add(candidate)
					existing_set.discard(candidate)
					existing_set.add(want)
				except Exception as e:
					logging.warning(f"[FAMILY NORMALIZE] Не удалось переименовать MP3 '{candidate}' -> '{want}': {type(e).__name__}: {e}")
		elif len(candidates) > 1:
			logging.warning(f"[FAMILY NORMALIZE] Пропущено переименование '{want}': найдено несколько возможных файлов {candidates}")
		else:
			# Нет безопасного соответствия — ничего не делаем
			continue
	return parsed_family_data

def _get_unique_folder_name(base_name: str) -> str:
    """Генерирует уникальное имя папки, добавляя приставки (1), (2) и т.д."""
    counter = 1
    folder_name = base_name
    while os.path.exists(os.path.join(FAMILIES_DIR, folder_name)):
        folder_name = f"{base_name} ({counter})"
        counter += 1
    return folder_name

def validate_and_extract_family_zip(zip_path):
    with pyzipper.AESZipFile(zip_path, 'r') as archive:
        # Собираем список файлов/папок, аккуратно декодируя имена
        top_dirs = set()
        decoded_files = []
        for zi in archive.infolist():
            # MCP: Корректно декодируем имя файла из ZIP, учитывая Unicode extra field (0x7075)
            name = None
            try:
                name = _extract_unicode_name_from_extra(getattr(zi, 'extra', b''))
            except Exception:
                name = None
            utf8_flag = bool(getattr(zi, 'flag_bits', 0) & 0x800)
            if not name:
                if isinstance(zi.filename, bytes):
                    # Если есть сырые байты — пробуем лучшие декодировки
                    name = _try_decode_best(zi.filename)
                else:
                    if utf8_flag:
                        name = zi.filename
                    else:
                        # Пытаемся восстановить исходные байты через cp437 и подобрать лучшую декодировку
                        try:
                            raw_bytes = zi.filename.encode('cp437', errors='strict')
                        except Exception:
                            raw_bytes = zi.filename.encode('latin1', errors='replace')
                        name = _try_decode_best(raw_bytes)
            # Нормализуем разделители
            if name:
                name = name.replace('\\', '/')
                name = _fix_common_mojibake(name)
            if name and '/' in name:
                top_dirs.add(name.split('/')[0])
            decoded_files.append((zi, name))
        if len(top_dirs) != 1:
            raise FamilyArchiveError('В архиве должна быть одна папка на верхнем уровне!')
        folder_name = list(top_dirs)[0]
        
        # Распаковываем во временную папку сначала
        temp_target_dir = os.path.join(FAMILIES_DIR, folder_name)
        if os.path.exists(temp_target_dir):
            raise FamilyArchiveError(f'Временная папка "{folder_name}" уже существует! Очистите папку families.')
        
        # Распаковываем вручную с сохранением структуры
        for zi, name in decoded_files:
            if not name or name.endswith('/'):
                # Папка
                out_dir = os.path.join(FAMILIES_DIR, name)
                os.makedirs(out_dir, exist_ok=True)
                continue
            out_path = os.path.join(FAMILIES_DIR, name)
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with archive.open(zi) as src, open(out_path, 'wb') as dst:
                shutil.copyfileobj(src, dst)
        
        # Теперь ищем .txt внутри temp_target_dir
        txt_files = [f for f in os.listdir(temp_target_dir) if f.endswith('.txt')]
        if len(txt_files) != 1:
            raise FamilyArchiveError('В папке должно быть ровно один .txt файл!')
        txt_filepath = os.path.join(temp_target_dir, txt_files[0])
        parsed_family_data = parse_family_txt(txt_filepath)
        
        # Истина имени — из TXT (#FAMILY). Генерируем уникальное имя папки
        desired_name = parsed_family_data.get('name') or folder_name
        unique_folder_name = _get_unique_folder_name(desired_name)
        
        # Переименовываем папку в уникальное имя
        if unique_folder_name != folder_name:
            logging.info(f"Группа слов '{desired_name}' будет сохранена в папку '{unique_folder_name}' (автоматически добавлена приставка для уникальности)")
            new_target_dir = os.path.join(FAMILIES_DIR, unique_folder_name)
            os.rename(temp_target_dir, new_target_dir)
            target_dir = new_target_dir
        else:
            target_dir = temp_target_dir
        
        # Переименовываем TXT-файл под имя семьи: <FAMILY>.txt
        final_txt_basename = desired_name + '.txt'
        current_txt_basename = os.path.basename(txt_filepath)
        if current_txt_basename != final_txt_basename:
            old_txt_path = os.path.join(target_dir, current_txt_basename)
            new_txt_path = os.path.join(target_dir, final_txt_basename)
            if os.path.exists(new_txt_path):
                raise FamilyArchiveError(f'TXT файл "{final_txt_basename}" уже существует в папке группы!')
            try:
                os.rename(old_txt_path, new_txt_path)
                txt_filepath = new_txt_path
            except Exception as e:
                logging.error(f"Не удалось переименовать TXT-файл '{current_txt_basename}' -> '{final_txt_basename}': {e}")
                raise FamilyArchiveError('Не удалось привести имя TXT к имени семьи.')
        
        # MCP: Автонормализация изображений + обновление ссылок
        try:
            parsed_family_data = _normalize_family_images(target_dir, parsed_family_data)
        except Exception as e:
            logging.warning(f"[FAMILY NORMALIZE] Ошибка автонормализации: {type(e).__name__}: {e}")
        # MCP: Попытка починить кракозябры у mp3 по именам из TXT для этапа 7
        try:
            parsed_family_data = _normalize_family_audios(target_dir, parsed_family_data)
        except Exception as e:
            logging.warning(f"[FAMILY NORMALIZE] Ошибка нормализации аудио: {type(e).__name__}: {e}")
        return parsed_family_data, target_dir

async def validate_and_extract_family_zip_with_ux(update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('waiting_for_family_zip'):
        return

    doc = update.message.document
    animation = update.message.animation

    if not doc or animation or not doc.file_name.lower().endswith('.zip'):
        try:
            await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
        except Exception as e:
            logging.error(f'[TGTeacher] Не удалось удалить сообщение пользователя с неправильным архивом: {e}', exc_info=True)
        await update.message.reply_text(
            '❌ Пожалуйста, пришлите zip-архив с группой слов.',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('✅ Ок', callback_data='admin_file_format_ok')]
            ])
        )
        return

    family_name_from_zip = os.path.splitext(doc.file_name)[0]
    # Убираем проверку на существование папки - теперь разрешаем дублирование названий
    # target_dir = os.path.join(FAMILIES_DIR, family_name_from_zip)
    # if os.path.exists(target_dir):
    #     # ❌ Группа слов с таким именем уже существует. Переименуй архив.
    #     return

    zip_path = os.path.join(FAMILIES_DIR, doc.file_name)
    file = await doc.get_file()
    await file.download_to_drive(zip_path)
    try:
        await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id)
    except Exception as e:
        logging.error(f'[TGTeacher] Не удалось удалить сообщение пользователя с архивом: {e}', exc_info=True)
    msg_id = context.user_data.get('add_family_msg_id')
    chat_id = context.user_data.get('add_family_chat_id')
    if msg_id and chat_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            logging.error(f'[TGTeacher] Не удалось удалить сообщение с инструкцией: {e}', exc_info=True)
        context.user_data['add_family_msg_id'] = None
        context.user_data['add_family_chat_id'] = None
    try:
        parsed_family_data, extracted_folder_path = validate_and_extract_family_zip(zip_path)
        # Сохраняем спарсенные данные в БД; folder_name = имя извлечённой папки (после возможного переименования под имя из TXT)
        try:
            folder_name_for_db = os.path.basename(extracted_folder_path)
            await save_family_to_pg(parsed_family_data, folder_name=folder_name_for_db)
        except Exception as e:
            logging.error(f"Ошибка при сохранении группы слов в БД: {e}", exc_info=True)
            # Убираем проверку на уникальность - теперь разрешаем дублирование названий
            # if 'unique' in str(e).lower():
            #     await update.message.reply_text(
            #         '❌ Группа слов с таким именем уже существует! Переименуй архив или удалите старую группу слов.',
            #         reply_markup=InlineKeyboardMarkup([
            #             [InlineKeyboardButton('✅ Ок', callback_data='admin_file_exists_ok')]
            #         ])
            #     )
            #     os.remove(zip_path)
            #     if os.path.exists(extracted_folder_path):
            #         shutil.rmtree(extracted_folder_path)
            #     return
            # else:
            raise
        # И обновляем кэш метаданных в памяти
        await families_data._load_initial_families_meta()
        await update.message.reply_text(
            f'✅ Группа слов "{parsed_family_data["name"]}" успешно добавлена!',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('✅ Ок', callback_data='admin_add_family_ok')]
            ])
        )
        context.user_data['waiting_for_family_zip'] = False
        os.remove(zip_path) # Удаляем временный ZIP-файл
        # shutil.rmtree(extracted_folder_path) # Удаляем извлеченную папку после сохранения в DB
    except FamilyArchiveError as e:
        logging.error(f"Ошибка при разархивировании: {e}", exc_info=True)
        await update.message.reply_text(
            f'❌ Ошибка при разархивировании: {e}',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton('⬅️ В меню', callback_data='admin_panel')]
            ])
        )
        os.remove(zip_path) # Удаляем временный ZIP-файл
        try:
            if 'extracted_folder_path' in locals() and extracted_folder_path and os.path.exists(extracted_folder_path):
                shutil.rmtree(extracted_folder_path)
        except Exception:
            pass
    except Exception as e:
        logging.error(f"Неизвестная ошибка при обработке архива группы слов: {e}", exc_info=True)
        await update.message.reply_text(f'❌ Неизвестная ошибка: {e}')
        os.remove(zip_path) # Удаляем временный ZIP-файл
        try:
            if 'extracted_folder_path' in locals() and extracted_folder_path and os.path.exists(extracted_folder_path):
                shutil.rmtree(extracted_folder_path)
        except Exception:
            pass 
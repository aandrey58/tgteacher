from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import os
import logging

logger = logging.getLogger(__name__)

OK_MENU = InlineKeyboardMarkup([[InlineKeyboardButton('✅ Ок', callback_data='main_menu')]])


def find_file_case_insensitive(directory: str, filename: str) -> str | None:
    """
    Ищет файл в директории без учёта регистра.
    
    Args:
        directory: Путь к директории для поиска
        filename: Имя файла для поиска
        
    Returns:
        Полный путь к найденному файлу или None, если файл не найден
    """
    if not os.path.exists(directory):
        return None
        
    try:
        # Ищем файл без учёта регистра
        filename_lower = filename.lower()
        for file_in_dir in os.listdir(directory):
            if file_in_dir.lower() == filename_lower:
                return os.path.join(directory, file_in_dir)
                
    except Exception as e:
        logger.warning(f"[common] Ошибка поиска файла '{filename}' в '{directory}': {type(e).__name__}: {e}")
        
    return None 
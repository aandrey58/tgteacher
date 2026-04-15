import json
import os
import logging
from typing import Dict, List, Any
from tgteacher_bot.db.user_repo import add_or_update_user_pg
import asyncio

logger = logging.getLogger(__name__)

# Файл для хранения данных о пользователях
LEGACY_USERS_FILE = 'legacy_users.json'

def save_user_to_legacy_file(user_id: int, username: str, first_name: str, last_name: str):
    """Сохраняет данные пользователя в JSON файл для последующей обработки"""
    try:
        # Загружаем существующие данные
        users_data = {}
        if os.path.exists(LEGACY_USERS_FILE):
            try:
                with open(LEGACY_USERS_FILE, 'r', encoding='utf-8') as f:
                    users_data = json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                users_data = {}
        # MCP: Если файла нет, создается пустой словарь - файл будет создан при первой записи
        
        # Добавляем/обновляем пользователя
        users_data[str(user_id)] = {
            'user_id': user_id,
            'username': username or '',
            'first_name': first_name or '',
            'last_name': last_name or '',
            'added_at': asyncio.get_event_loop().time() if asyncio.get_event_loop().is_running() else 0
        }
        
        # Сохраняем обратно в файл (создает файл если его нет)
        with open(LEGACY_USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(users_data, f, ensure_ascii=False, indent=2)
            
        logger.info(f"Пользователь {user_id} добавлен в legacy_users.json")
        
    except Exception as e:
        logger.error(f"Ошибка при сохранении пользователя {user_id} в legacy файл: {e}")

async def process_legacy_users_job(context):
    """Фоновая задача для обработки старых пользователей из JSON файла"""
    logger.info("Запуск обработки legacy пользователей...")
    
    try:
        # Создаем файл если его нет
        if not os.path.exists(LEGACY_USERS_FILE):
            logger.info("Файл legacy_users.json не найден, создаем пустой файл")
            with open(LEGACY_USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump({}, f)
        
        # Загружаем данные из файла
        with open(LEGACY_USERS_FILE, 'r', encoding='utf-8') as f:
            users_data = json.load(f)
        
        if not users_data:
            logger.info("Файл legacy_users.json пуст, пропускаем обработку")
            return
        
        processed_count = 0
        errors_count = 0
        
        # Обрабатываем каждого пользователя
        for user_id_str, user_info in users_data.items():
            try:
                user_id = int(user_id_str)
                # MCP: add_or_update_user_pg использует ON CONFLICT DO UPDATE, 
                # поэтому если пользователь уже есть в базе, его данные просто обновятся
                await add_or_update_user_pg(
                    user_id=user_id,
                    username=user_info['username'],
                    first_name=user_info['first_name'],
                    last_name=user_info['last_name']
                )
                processed_count += 1
                logger.info(f"Пользователь {user_id} успешно добавлен/обновлен в базе")
                
            except Exception as e:
                errors_count += 1
                logger.error(f"Ошибка при добавлении пользователя {user_id_str} в базу: {e}")
        
        # Если все пользователи успешно обработаны, удаляем файл
        if processed_count > 0 and errors_count == 0:
            try:
                # os.remove(LEGACY_USERS_FILE)  # Убираем удаление файла
                logger.info(f"Все {processed_count} пользователей обработаны, файл legacy_users.json сохранен")
            except Exception as e:
                logger.error(f"Ошибка при обработке файла legacy_users.json: {e}")
        else:
            logger.info(f"Обработано {processed_count} пользователей, ошибок: {errors_count}")
            
    except Exception as e:
        logger.error(f"Ошибка при обработке legacy пользователей: {e}")

async def collect_context_users_job(context):
    """Фоновая задача для сбора всех пользователей из context при запуске бота"""
    logger.info("Запуск сбора пользователей из context...")
    
    try:
        users_collected = 0
        for user_id_str, user_data in context.application.user_data.items():
            try:
                user_id = int(user_id_str)
                # Собираем только если есть какие-то данные пользователя
                if user_data:
                    # Пытаемся получить данные пользователя из Telegram API
                    try:
                        chat_member = await context.application.bot.get_chat_member(user_id, user_id)
                        if chat_member and chat_member.user:
                            save_user_to_legacy_file(
                                user_id=user_id,
                                username=chat_member.user.username or '',
                                first_name=chat_member.user.first_name or '',
                                last_name=chat_member.user.last_name or ''
                            )
                            users_collected += 1
                    except Exception:
                        # Если не удалось получить данные из API, используем то что есть в user_data
                        save_user_to_legacy_file(
                            user_id=user_id,
                            username=user_data.get('username', ''),
                            first_name=user_data.get('first_name', ''),
                            last_name=user_data.get('last_name', '')
                        )
                        users_collected += 1
            except (ValueError, TypeError):
                # Пропускаем некорректные user_id
                continue
        
        logger.info(f"Собрано {users_collected} пользователей из context")
        
    except Exception as e:
        logger.error(f"Ошибка при сборе пользователей из context: {e}")

def collect_user_from_context(context, user_id: int):
    """Собирает данные пользователя из context и сохраняет в legacy файл"""
    try:
        # Получаем данные пользователя из context
        user_data = context.application.user_data.get(str(user_id), {})
        
        # Если пользователь уже есть в базе, не добавляем его
        # (это будет проверяться в add_or_update_user_pg через ON CONFLICT)
        
        # Сохраняем в legacy файл для последующей обработки
        save_user_to_legacy_file(
            user_id=user_id,
            username=user_data.get('username', ''),
            first_name=user_data.get('first_name', ''),
            last_name=user_data.get('last_name', '')
        )
        
    except Exception as e:
        logger.error(f"Ошибка при сборе данных пользователя {user_id}: {e}")

def collect_all_users_from_context(context):
    """Собирает всех пользователей из context.user_data"""
    try:
        users_collected = 0
        for user_id_str, user_data in context.application.user_data.items():
            try:
                user_id = int(user_id_str)
                # Собираем только если есть какие-то данные пользователя
                if user_data:
                    collect_user_from_context(context, user_id)
                    users_collected += 1
            except (ValueError, TypeError):
                # Пропускаем некорректные user_id
                continue
        
        logger.info(f"Собрано {users_collected} пользователей из context")
        
    except Exception as e:
        logger.error(f"Ошибка при сборе всех пользователей из context: {e}") 
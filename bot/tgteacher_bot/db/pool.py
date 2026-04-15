# Проверка: если есть относительные импорты, меняю на абсолютные. (В этом файле их нет, просто подтверждаю.)
import os

import asyncpg

_pool = None

async def get_pool():
    global _pool
    if _pool is None:
        db_user = os.getenv('POSTGRES_USER', 'postgres')
        db_name = os.getenv('POSTGRES_DB', 'tgteacher')
        db_host = os.getenv('POSTGRES_HOST', '127.0.0.1')
        db_port = int(os.getenv('POSTGRES_PORT', '5432'))
        db_password = os.getenv('POSTGRES_PASSWORD')
        _pool = await asyncpg.create_pool(
            user=db_user,
            password=db_password,
            database=db_name,
            host=db_host,
            port=db_port,
            min_size=1,
            max_size=15, # Оптимизировано для 1 ГБ RAM: уменьшено с 50 до 15 соединений
            command_timeout=60,
            server_settings={
                'application_name': 'tgteacher_bot'
            }
        )
    return _pool 
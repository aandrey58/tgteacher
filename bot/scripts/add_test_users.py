import asyncio
from tgteacher_bot.db.user_repo import get_pool
import random
from datetime import datetime, timedelta

async def add_test_users():
    pool = await get_pool()
    async with pool.acquire() as conn:
        base_time = datetime(2025, 7, 22, 15, 39, 59)  # MCP: фиксированная точка отсчёта
        for i in range(1, 26):
            user_id = 100000 + i
            username = f"testuser{i}"
            first_name = f"Имя{i}"
            last_name = f"Фамилия{i}"
            registered_at = base_time - timedelta(days=random.randint(0, 30))
            is_subscribed = random.choice([True, False])
            subscription_count = random.randint(0, 5)
            # last_active_at: часть пользователей неактивные (10, 25, 35 дней назад), часть активные (0-2 дня назад)
            if i <= 8:
                last_active_at = base_time - timedelta(days=random.choice([10, 12, 15, 25, 35]))
            elif i <= 16:
                last_active_at = base_time - timedelta(days=random.randint(8, 30))
            else:
                last_active_at = base_time - timedelta(days=random.randint(0, 2))
            await conn.execute('''
                INSERT INTO users (user_id, username, first_name, last_name, registered_at, is_subscribed, subscription_count, last_active_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (user_id) DO NOTHING
            ''', user_id, username, first_name, last_name, registered_at, is_subscribed, subscription_count, last_active_at)
    print('✅ 15 тестовых пользователей добавлено!')

if __name__ == "__main__":
    asyncio.run(add_test_users()) 
import asyncio
from datetime import datetime, timedelta
from tgteacher_bot.db.user_repo import get_pool

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS payments (
    id SERIAL PRIMARY KEY,
    payment_id VARCHAR(255) UNIQUE NOT NULL,
    user_id BIGINT NOT NULL,
    months INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    status VARCHAR(50) NOT NULL,
    payment_url TEXT,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP,
    result_subscription_until TIMESTAMP WITH TIME ZONE
)
'''

async def seed_payments():
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Гарантируем таблицу
        await conn.execute(CREATE_TABLE_SQL)

        # Берем до 3 свежих пользователей
        rows = await conn.fetch('SELECT user_id FROM users ORDER BY registered_at DESC LIMIT 3')
        if not rows:
            print('Нет пользователей для сидирования payments')
            return
        user_ids = [r['user_id'] for r in rows]

        # Набор тестов: (months, amount_rub)
        samples = [(1, 199), (3, 499), (12, 1990)]
        now_ts = int(datetime.now().timestamp())

        for idx, user_id in enumerate(user_ids):
            months, amount_rub = samples[idx % len(samples)]
            payment_id = f'seed_{user_id}_{now_ts}_{idx}'
            amount_kopecks = amount_rub * 100
            active_until = datetime.now() + timedelta(days=30*months)
            await conn.execute(
                '''
                INSERT INTO payments (payment_id, user_id, months, amount, status, processed, created_at, updated_at, processed_at, result_subscription_until)
                VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW(), NOW(), $7)
                ON CONFLICT (payment_id) DO NOTHING
                ''',
                payment_id, user_id, months, amount_kopecks, 'succeeded', True, active_until
            )
            print(f'Добавлен платеж: user={user_id}, months={months}, amount={amount_rub} RUB')

async def main():
    await seed_payments()

if __name__ == '__main__':
    asyncio.run(main()) 
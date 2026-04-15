import asyncpg
from tgteacher_bot.db.pool import get_pool
import json
from tgteacher_bot.db.families_repo import track_db_errors, DB_ERRORS
from typing import List, Dict, Any
import datetime
import logging

logger = logging.getLogger(__name__)

MIN_UPDATE_INTERVAL = 300  # 5 минут в секундах
MAX_USER_DATA_SIZE = 1000  # Максимальное количество пользователей в user_data для экономии памяти

@track_db_errors
async def init_db_pg():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_family_progress (
                user_id BIGINT NOT NULL,
                family_idx INTEGER NOT NULL,
                stage1_done INTEGER DEFAULT 0,
                stage2_done INTEGER DEFAULT 0,
                stage3_done INTEGER DEFAULT 0,
                stage6_done INTEGER DEFAULT 0,
                stage8_done INTEGER DEFAULT 0,
                finished INTEGER DEFAULT 0,
                completion_count INTEGER DEFAULT 0,
                PRIMARY KEY (user_id, family_idx)
            );
        ''')
        await conn.execute('''
            ALTER TABLE user_family_progress
            ADD COLUMN IF NOT EXISTS stage4_done INTEGER DEFAULT 0
        ''')
        # NEW: ensure stage5_done exists
        await conn.execute('''
            ALTER TABLE user_family_progress
            ADD COLUMN IF NOT EXISTS stage5_done INTEGER DEFAULT 0
        ''')
        # NEW: ensure stage7_done exists
        await conn.execute('''
            ALTER TABLE user_family_progress
            ADD COLUMN IF NOT EXISTS stage7_done INTEGER DEFAULT 0
        ''')
        # NEW: ensure completion_count exists
        await conn.execute('''
            ALTER TABLE user_family_progress
            ADD COLUMN IF NOT EXISTS completion_count INTEGER DEFAULT 0
        ''')
        await conn.execute('''
            ALTER TABLE user_family_progress
            ADD COLUMN IF NOT EXISTS first_opened_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_task_progress (
                user_id BIGINT NOT NULL,
                family_idx INTEGER NOT NULL,
                stage_num INTEGER NOT NULL,
                task_idx INTEGER NOT NULL,
                is_done INTEGER DEFAULT 0,
                selected_choice_text TEXT,
                is_correct INTEGER,
                PRIMARY KEY (user_id, family_idx, stage_num, task_idx)
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_last_opened_family_place (
                user_id BIGINT NOT NULL,
                family_idx INTEGER NOT NULL,
                stage_num INTEGER NOT NULL,
                task_idx INTEGER NOT NULL,
                PRIMARY KEY (user_id, family_idx)
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_stage_state (
                user_id BIGINT NOT NULL,
                family_idx INTEGER NOT NULL,
                stage_num INTEGER NOT NULL,
                state JSONB,
                last_activity_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (user_id, family_idx, stage_num)
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_current_family (
                user_id BIGINT NOT NULL,
                family_idx INTEGER NOT NULL,
                PRIMARY KEY (user_id)
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS families (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT,
                folder_name TEXT,
                target TEXT
            );
        ''')
        # Удаляем ограничение UNIQUE с поля name, если оно существует
        await conn.execute('''
            ALTER TABLE families DROP CONSTRAINT IF EXISTS families_name_key
        ''')
        await conn.execute('''
            ALTER TABLE families
            ADD COLUMN IF NOT EXISTS target TEXT
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS stage1_words (
                id SERIAL PRIMARY KEY,
                family_id INTEGER NOT NULL,
                word_order INTEGER NOT NULL,
                word TEXT NOT NULL,
                translation TEXT,
                example TEXT,
                example_translation TEXT,
                hint TEXT,
                FOREIGN KEY (family_id) REFERENCES families(id) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS stage2_tasks (
                id SERIAL PRIMARY KEY,
                family_id INTEGER NOT NULL,
                task_order INTEGER NOT NULL,
                sentence TEXT NOT NULL,
                answer TEXT NOT NULL,
                choices JSONB,
                explanation TEXT,
                FOREIGN KEY (family_id) REFERENCES families(id) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS stage3_tasks (
                id SERIAL PRIMARY KEY,
                family_id INTEGER NOT NULL,
                task_order INTEGER NOT NULL,
                word TEXT NOT NULL,
                definition TEXT NOT NULL,
                explanation TEXT,
                choices JSONB,
                FOREIGN KEY (family_id) REFERENCES families(id) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS stage4_tasks (
                id SERIAL PRIMARY KEY,
                family_id INTEGER NOT NULL,
                task_order INTEGER NOT NULL,
                sentence TEXT NOT NULL,
                answer TEXT NOT NULL,
                choices JSONB,
                explanation TEXT,
                FOREIGN KEY (family_id) REFERENCES families(id) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS stage5_tasks (
                id SERIAL PRIMARY KEY,
                family_id INTEGER NOT NULL,
                task_order INTEGER NOT NULL,
                image TEXT,
                answer TEXT NOT NULL,
                alternatives JSONB,
                explanation TEXT,
                FOREIGN KEY (family_id) REFERENCES families(id) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS stage6_tasks (
                id SERIAL PRIMARY KEY,
                family_id INTEGER NOT NULL,
                task_order INTEGER NOT NULL,
                word TEXT NOT NULL,
                synonyms JSONB,
                wrong_synonyms JSONB,
                explanation TEXT,
                FOREIGN KEY (family_id) REFERENCES families(id) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS stage7_tasks (
                id SERIAL PRIMARY KEY,
                family_id INTEGER NOT NULL,
                task_order INTEGER NOT NULL,
                task TEXT,
                audio TEXT,
                answer TEXT NOT NULL,
                choices JSONB,
                explanation TEXT,
                FOREIGN KEY (family_id) REFERENCES families(id) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS stage8_tasks (
                id SERIAL PRIMARY KEY,
                family_id INTEGER NOT NULL,
                task_order INTEGER NOT NULL,
                text TEXT NOT NULL,
                answers JSONB,
                explanations JSONB,
                equal TEXT,
                FOREIGN KEY (family_id) REFERENCES families(id) ON DELETE CASCADE
            );
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                registered_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                is_subscribed BOOLEAN DEFAULT FALSE,
                subscription_count INTEGER DEFAULT 0,
                last_active_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        ''')
        # Добавляем столбец для даты окончания подписки, если его нет
        await conn.execute('''
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS subscription_until TIMESTAMPTZ
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_user_task_progress_user_family_stage ON user_task_progress (user_id, family_idx, stage_num);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_user_last_opened_family_place_user_family ON user_last_opened_family_place (user_id, family_idx);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_user_stage_state_user_family_stage ON user_stage_state (user_id, family_idx, stage_num);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_stage1_words_family_id ON stage1_words (family_id);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_stage2_tasks_family_id ON stage2_tasks (family_id);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_stage3_tasks_family_id ON stage3_tasks (family_id);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_stage4_tasks_family_id ON stage4_tasks (family_id);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_stage5_tasks_family_id ON stage5_tasks (family_id);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_stage6_tasks_family_id ON stage6_tasks (family_id);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_stage7_tasks_family_id ON stage7_tasks (family_id);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_stage8_tasks_family_id ON stage8_tasks (family_id);
        ''')
        await conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_user_current_family_user_id ON user_current_family (user_id);
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS payment_options (
                id SERIAL PRIMARY KEY,
                months INTEGER NOT NULL UNIQUE,
                amount INTEGER NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        ''')

@track_db_errors
async def set_task_done_pg(user_id: int, family_idx: int, stage_num: int, task_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_task_progress (user_id, family_idx, stage_num, task_idx, is_done)
            VALUES ($1, $2, $3, $4, 1)
            ON CONFLICT (user_id, family_idx, stage_num, task_idx) DO UPDATE SET is_done = 1
        ''', user_id, family_idx, stage_num, task_idx)

@track_db_errors
async def start_family_progress_pg(user_id: int, family_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_family_progress (user_id, family_idx, first_opened_timestamp)
            VALUES ($1, $2, NOW())
            ON CONFLICT (user_id, family_idx) DO NOTHING
        ''', user_id, family_idx)

@track_db_errors
async def set_family_stage_done_pg(user_id: int, family_idx: int, stage: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_family_progress (user_id, family_idx)
            VALUES ($1, $2)
            ON CONFLICT (user_id, family_idx) DO NOTHING
        ''', user_id, family_idx)
        field_name = f'stage{stage}_done'
        if stage in {1, 2, 3, 4, 5, 6, 7, 8}:
            try:
                await conn.execute(f'UPDATE user_family_progress SET {field_name} = 1 WHERE user_id = $1 AND family_idx = $2', user_id, family_idx)
            except Exception as e:
                # Если нет нужной колонки (например, stage5_done), создаём её на лету и повторяем апдейт
                err_text = str(e).lower()
                if 'undefined column' in err_text or 'does not exist' in err_text:
                    try:
                        await conn.execute(f'ALTER TABLE user_family_progress ADD COLUMN IF NOT EXISTS {field_name} INTEGER DEFAULT 0')
                        await conn.execute(f'UPDATE user_family_progress SET {field_name} = 1 WHERE user_id = $1 AND family_idx = $2', user_id, family_idx)
                    except Exception:
                        raise
                else:
                    raise

@track_db_errors
async def get_completed_families_info_pg(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT family_idx, stage1_done, stage2_done, stage3_done, stage4_done, stage5_done, stage6_done, stage7_done, stage8_done
            FROM user_family_progress
            WHERE user_id = $1
            ORDER BY family_idx
        ''', user_id)
        return [{'family_idx': row['family_idx'], 'stage1_done': row['stage1_done'], 'stage2_done': row['stage2_done'], 'stage3_done': row['stage3_done'], 'stage4_done': row['stage4_done'], 'stage5_done': row['stage5_done'], 'stage6_done': row['stage6_done'], 'stage7_done': row['stage7_done'], 'stage8_done': row['stage8_done']} for row in rows]

@track_db_errors
async def set_stage2_answer_pg(user_id: int, family_idx: int, task_idx: int, selected_choice_text: str, is_correct: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_task_progress (user_id, family_idx, stage_num, task_idx, is_done, selected_choice_text, is_correct)
            VALUES ($1, $2, 2, $3, 1, $4, $5)
            ON CONFLICT (user_id, family_idx, stage_num, task_idx) DO UPDATE SET
                is_done = 1, selected_choice_text = $4, is_correct = $5
        ''', user_id, family_idx, task_idx, selected_choice_text, 1 if is_correct else 0)

@track_db_errors
async def set_stage4_answer_pg(user_id: int, family_idx: int, task_idx: int, selected_choice_text: str, is_correct: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_task_progress (user_id, family_idx, stage_num, task_idx, is_done, selected_choice_text, is_correct)
            VALUES ($1, $2, 4, $3, 1, $4, $5)
            ON CONFLICT (user_id, family_idx, stage_num, task_idx) DO UPDATE SET
                is_done = 1, selected_choice_text = $4, is_correct = $5
        ''', user_id, family_idx, task_idx, selected_choice_text, 1 if is_correct else 0)

@track_db_errors
async def set_stage5_answer_pg(user_id: int, family_idx: int, task_idx: int, user_answer: str, is_correct: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_task_progress (user_id, family_idx, stage_num, task_idx, is_done, selected_choice_text, is_correct)
            VALUES ($1, $2, 5, $3, 1, $4, $5)
            ON CONFLICT (user_id, family_idx, stage_num, task_idx) DO UPDATE SET 
                is_done = 1, selected_choice_text = $4, is_correct = $5
        ''', user_id, family_idx, task_idx, user_answer, 1 if is_correct else 0)

@track_db_errors
async def get_stage5_answer_pg(user_id: int, family_idx: int, task_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT selected_choice_text, is_correct FROM user_task_progress
            WHERE user_id = $1 AND family_idx = $2 AND stage_num = 5 AND task_idx = $3 AND is_done = 1
        ''', user_id, family_idx, task_idx)
        if row and row['selected_choice_text'] is not None:
            return (row['selected_choice_text'], bool(row['is_correct']))
        return None

@track_db_errors
async def get_completed_tasks_pg(user_ids: List[int], family_idx: int, stage_num: int) -> Dict[int, set]:
    """
    Возвращает выполненные задания для списка пользователей.
    Ключ словаря - user_id, значение - множество task_idx.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT user_id, task_idx FROM user_task_progress
            WHERE user_id = ANY($1::bigint[]) AND family_idx = $2 AND stage_num = $3 AND is_done = 1
        ''', user_ids, family_idx, stage_num)
        
        results = {user_id: set() for user_id in user_ids}
        for row in rows:
            results[row['user_id']].add(row['task_idx'])
        return results

@track_db_errors
async def get_started_families_ids_pg(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT DISTINCT uss.family_idx, MAX(uss.last_activity_timestamp) as last_activity
            FROM user_stage_state uss
            WHERE uss.user_id = $1 
            GROUP BY uss.family_idx
            ORDER BY last_activity DESC
        ''', user_id)
        return [row['family_idx'] for row in rows]

@track_db_errors
async def set_last_opened_family_place_pg(user_id: int, family_idx: int, stage_num: int, task_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_last_opened_family_place (user_id, family_idx, stage_num, task_idx)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id, family_idx) DO UPDATE SET stage_num = $3, task_idx = $4
        ''', user_id, family_idx, stage_num, task_idx)

@track_db_errors
async def get_last_opened_family_place_pg(user_id: int, family_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT stage_num, task_idx FROM user_last_opened_family_place
            WHERE user_id = $1 AND family_idx = $2
        ''', user_id, family_idx)
        return (row['stage_num'], row['task_idx']) if row else None

@track_db_errors
async def reset_family_progress_pg(user_id: int, family_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # MCP: Сохраняем completion_count перед удалением
        row = await conn.fetchrow('''
            SELECT completion_count FROM user_family_progress 
            WHERE user_id = $1 AND family_idx = $2
        ''', user_id, family_idx)
        completion_count = row['completion_count'] if row else 0
        
        # Удаляем прогресс, но сохраняем запись с completion_count
        await conn.execute('''
            DELETE FROM user_task_progress WHERE user_id = $1 AND family_idx = $2
        ''', user_id, family_idx)
        await conn.execute('''
            DELETE FROM user_last_opened_family_place WHERE user_id = $1 AND family_idx = $2
        ''', user_id, family_idx)
        await conn.execute('''
            DELETE FROM user_stage_state WHERE user_id = $1 AND family_idx = $2
        ''', user_id, family_idx)
        
        # Восстанавливаем запись с сохранённым completion_count, но сбрасываем остальные поля
        await conn.execute('''
            INSERT INTO user_family_progress (user_id, family_idx, completion_count)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, family_idx) DO UPDATE SET 
                stage1_done = 0, stage2_done = 0, stage3_done = 0, stage4_done = 0,
                stage5_done = 0, stage6_done = 0, stage7_done = 0, stage8_done = 0,
                finished = 0, completion_count = $3
        ''', user_id, family_idx, completion_count)

@track_db_errors
async def get_stage2_answer_pg(user_id: int, family_idx: int, task_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT selected_choice_text, is_correct FROM user_task_progress
            WHERE user_id = $1 AND family_idx = $2 AND stage_num = 2 AND task_idx = $3 AND is_done = 1
        ''', user_id, family_idx, task_idx)
        if row and row['selected_choice_text'] is not None:
            return (row['selected_choice_text'], bool(row['is_correct']))
        return None

@track_db_errors
async def get_stage4_answer_pg(user_id: int, family_idx: int, task_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT selected_choice_text, is_correct FROM user_task_progress
            WHERE user_id = $1 AND family_idx = $2 AND stage_num = 4 AND task_idx = $3 AND is_done = 1
        ''', user_id, family_idx, task_idx)
        if row and row['selected_choice_text'] is not None:
            return (row['selected_choice_text'], bool(row['is_correct']))
        return None

@track_db_errors
async def set_stage3_answer_pg(user_id: int, family_idx: int, task_idx: int, selected_choice_text: str, is_correct: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_task_progress (user_id, family_idx, stage_num, task_idx, is_done, selected_choice_text, is_correct)
            VALUES ($1, $2, 3, $3, 1, $4, $5)
            ON CONFLICT (user_id, family_idx, stage_num, task_idx) DO UPDATE SET 
                is_done = 1, selected_choice_text = $4, is_correct = $5
        ''', user_id, family_idx, task_idx, selected_choice_text, 1 if is_correct else 0)

@track_db_errors
async def get_stage3_answer_pg(user_id: int, family_idx: int, task_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT selected_choice_text, is_correct FROM user_task_progress
            WHERE user_id = $1 AND family_idx = $2 AND stage_num = 3 AND task_idx = $3 AND is_done = 1
        ''', user_id, family_idx, task_idx)
        if row and row['selected_choice_text'] is not None:
            return (row['selected_choice_text'], bool(row['is_correct']))
        return None

@track_db_errors
async def set_stage6_answer_pg(user_id: int, family_idx: int, task_idx: int, selected_choices: list, is_correct: bool):
    selected_choices_str = ';'.join(selected_choices)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_task_progress (user_id, family_idx, stage_num, task_idx, is_done, selected_choice_text, is_correct)
            VALUES ($1, $2, 6, $3, 1, $4, $5)
            ON CONFLICT (user_id, family_idx, stage_num, task_idx) DO UPDATE SET 
                is_done = 1, selected_choice_text = $4, is_correct = $5
        ''', user_id, family_idx, task_idx, selected_choices_str, 1 if is_correct else 0)

@track_db_errors
async def get_stage6_answer_pg(user_id: int, family_idx: int, task_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT selected_choice_text, is_correct FROM user_task_progress
            WHERE user_id = $1 AND family_idx = $2 AND stage_num = 6 AND task_idx = $3 AND is_done = 1
        ''', user_id, family_idx, task_idx)
        if row and row['selected_choice_text'] is not None:
            selected_choices_list = row['selected_choice_text'].split(';') if row['selected_choice_text'] else []
            return (selected_choices_list, bool(row['is_correct']))
        return None

@track_db_errors
async def set_stage8_answer_pg(user_id: int, family_idx: int, task_idx: int, selected_words: list, is_correct: bool):
    selected_words_str = ';'.join(selected_words)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_task_progress (user_id, family_idx, stage_num, task_idx, is_done, selected_choice_text, is_correct)
            VALUES ($1, $2, 8, $3, 1, $4, $5)
            ON CONFLICT (user_id, family_idx, stage_num, task_idx) DO UPDATE SET 
                is_done = 1, selected_choice_text = $4, is_correct = $5
        ''', user_id, family_idx, task_idx, selected_words_str, 1 if is_correct else 0)

@track_db_errors
async def get_stage8_answer_pg(user_id: int, family_idx: int, task_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT selected_choice_text, is_correct FROM user_task_progress
            WHERE user_id = $1 AND family_idx = $2 AND stage_num = 8 AND task_idx = $3 AND is_done = 1
        ''', user_id, family_idx, task_idx)
        if row and row['selected_choice_text'] is not None:
            selected_words_list = row['selected_choice_text'].split(';') if row['selected_choice_text'] else []
            return (selected_words_list, bool(row['is_correct']))
        return None

@track_db_errors
async def get_all_stage_answers_for_family_pg(user_ids: List[int], family_idx: int) -> Dict[int, Dict[int, Dict[int, tuple]]]:
    """
    Возвращает все ответы для списка пользователей в одной группе слов.
    Структура: {user_id: {stage_num: {task_idx: (ответ, is_correct)}}}
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT user_id, stage_num, task_idx, selected_choice_text, is_correct 
            FROM user_task_progress
            WHERE user_id = ANY($1::bigint[]) AND family_idx = $2 AND is_done = 1 AND is_correct IS NOT NULL
        ''', user_ids, family_idx)
        
        results = {user_id: {} for user_id in user_ids}
        for row in rows:
            user_id = row['user_id']
            stage_num = row['stage_num']
            task_idx = row['task_idx']
            selected_text = row['selected_choice_text']
            is_correct = bool(row['is_correct'])

            if stage_num not in results[user_id]:
                results[user_id][stage_num] = {}

            if stage_num in (6, 8):
                parsed_selected_text = selected_text.split(';') if selected_text else []
            else:
                parsed_selected_text = selected_text
            
            results[user_id][stage_num][task_idx] = (parsed_selected_text, is_correct)
            
        return results

@track_db_errors
async def set_user_stage_state_pg(user_id: int, family_idx: int, stage_num: int, state: dict):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_stage_state (user_id, family_idx, stage_num, state)
            VALUES ($1, $2, $3, $4::jsonb)
            ON CONFLICT (user_id, family_idx, stage_num) DO UPDATE SET state = $4::jsonb, last_activity_timestamp = NOW()
        ''', user_id, family_idx, stage_num, json.dumps(state))

@track_db_errors
async def get_user_stage_state_pg(user_id: int, family_idx: int, stage_num: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT state FROM user_stage_state
            WHERE user_id = $1 AND family_idx = $2 AND stage_num = $3
        ''', user_id, family_idx, stage_num)
        if row and row['state']:
            return json.loads(row['state'])
        return None

@track_db_errors
async def set_current_family_idx_pg(user_id: int, family_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_current_family (user_id, family_idx)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET family_idx = $2
        ''', user_id, family_idx)

@track_db_errors
async def get_current_family_idx_pg(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT family_idx FROM user_current_family
            WHERE user_id = $1
        ''', user_id)
        return row['family_idx'] if row else None

@track_db_errors
async def clean_inactive_user_states_pg(inactivity_threshold_minutes: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Удаляем записи из user_stage_state, которые не обновлялись дольше указанного порога
        interval_str = f"{int(inactivity_threshold_minutes)} MINUTE"
        await conn.execute(f'''
            DELETE FROM user_stage_state
            WHERE last_activity_timestamp < NOW() - INTERVAL '{interval_str}'
        ''') 

@track_db_errors
async def get_inactive_user_states_pg(inactivity_threshold_minutes: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Получаем список пользователей с неактивными состояниями
        interval_str = f"{int(inactivity_threshold_minutes)} MINUTE"
        rows = await conn.fetch(f'''
            SELECT DISTINCT user_id FROM user_stage_state
            WHERE last_activity_timestamp < NOW() - INTERVAL '{interval_str}'
        ''')
        return [row['user_id'] for row in rows]

@track_db_errors
async def get_all_user_completed_tasks_counts(user_id: int, family_ids: List[int]) -> Dict[int, Dict[int, int]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        results = {}
        if not family_ids:
            return results

        rows = await conn.fetch('''
            SELECT family_idx, stage_num, COUNT(task_idx) as count
            FROM user_task_progress
            WHERE user_id = $1 AND family_idx = ANY($2::int[]) AND is_done = 1
            GROUP BY family_idx, stage_num
        ''', user_id, family_ids)

        for row in rows:
            results.setdefault(row['family_idx'], {})[row['stage_num']] = row['count']
        
        # Ensure all requested family_ids and stages are in the results
        for fam_id in family_ids:
            results.setdefault(fam_id, {})
            for stage_num in [1, 2, 3, 4, 5, 6, 7, 8]:
                results[fam_id].setdefault(stage_num, 0)

        return results 

@track_db_errors
async def add_or_update_user_pg(user_id: int, username: str, first_name: str, last_name: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Сначала проверяем существующего пользователя
        existing_user = await conn.fetchrow('''
            SELECT username, first_name, last_name
            FROM users
            WHERE user_id = $1
        ''', user_id)
        
        if existing_user:
            # Пользователь существует - обновляем только измененные поля
            update_fields = []
            update_values = []
            param_count = 1
            
            if existing_user['username'] != username:
                update_fields.append(f"username = ${param_count}")
                update_values.append(username)
                param_count += 1
                
            if existing_user['first_name'] != first_name:
                update_fields.append(f"first_name = ${param_count}")
                update_values.append(first_name)
                param_count += 1
                
            if existing_user['last_name'] != last_name:
                update_fields.append(f"last_name = ${param_count}")
                update_values.append(last_name)
                param_count += 1
            
            # Обновляем только если есть изменения
            if update_fields:
                query = f'''
                    UPDATE users 
                    SET {', '.join(update_fields)}
                    WHERE user_id = ${param_count}
                '''
                update_values.append(user_id)
                await conn.execute(query, *update_values)
        else:
            # Пользователя нет - вставляем нового
            await conn.execute('''
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES ($1, $2, $3, $4)
            ''', user_id, username, first_name, last_name)

@track_db_errors
async def get_user_subscription_info_pg(user_id: int) -> Dict[str, Any]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT is_subscribed, subscription_count, subscription_until
            FROM users
            WHERE user_id = $1
        ''', user_id)
        if not row:
            return {
                'is_subscribed': False,
                'subscription_count': 0,
                'subscription_until': None,
            }
        return {
            'is_subscribed': bool(row['is_subscribed']) if row['is_subscribed'] is not None else False,
            'subscription_count': int(row['subscription_count']) if row['subscription_count'] is not None else 0,
            'subscription_until': row['subscription_until'],
        }

@track_db_errors
async def extend_user_subscription_pg(user_id: int, months: int):
    """
    Продлевает подписку пользователя на указанное количество месяцев
    
    Args:
        user_id: ID пользователя в Telegram
        months: Количество месяцев для продления
    """
    import datetime
    
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Получаем текущую информацию о подписке
        row = await conn.fetchrow('''
            SELECT subscription_until, subscription_count
            FROM users
            WHERE user_id = $1
        ''', user_id)
        
        if not row:
            # Пользователь не существует, создаем запись
            await conn.execute('''
                INSERT INTO users (user_id, subscription_until, subscription_count, is_subscribed)
                VALUES ($1, NOW() + INTERVAL '$2 months', $2, true)
            ''', user_id, months)
        else:
            # Пользователь существует, продлеваем подписку
            current_until = row['subscription_until']
            current_count = row['subscription_count'] or 0
            
            # Если подписка истекла, начинаем с текущего момента
            if not current_until or current_until < datetime.datetime.now(datetime.timezone.utc):
                new_until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=30 * months)
            else:
                # Если подписка активна, добавляем к текущей дате окончания
                new_until = current_until + datetime.timedelta(days=30 * months)
            
            new_count = current_count + months
            
            await conn.execute('''
                UPDATE users 
                SET subscription_until = $2, 
                    subscription_count = $3, 
                    is_subscribed = true
                WHERE user_id = $1
            ''', user_id, new_until, new_count)
        
        logger.info(f"Подписка пользователя {user_id} продлена на {months} месяцев")

@track_db_errors
async def get_all_users_progress_for_family_pg(family_id: int) -> List[Dict[str, Any]]:
    """
    Возвращает прогресс (количество выполненных заданий) для всех пользователей
    в рамках одной группы слов, сгруппированный по пользователям и этапам.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT user_id, stage_num, COUNT(task_idx) as completed_tasks
            FROM user_task_progress
            WHERE family_idx = $1 AND is_done = 1
            GROUP BY user_id, stage_num
            ORDER BY user_id, stage_num
        ''', family_id)
        return [dict(row) for row in rows]

@track_db_errors
async def get_family_stage_answers_stats_pg(family_id: int) -> List[Dict[str, Any]]:
    """
    Возвращает статистику по правильным/неправильным ответам для всех
    пользователей в рамках одной группы слов, сгруппированную по этапам.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT stage_num, is_correct, COUNT(*) as count
            FROM user_task_progress
            WHERE family_idx = $1 AND stage_num IN (2, 3, 4, 5, 6, 8) AND is_correct IS NOT NULL
            GROUP BY stage_num, is_correct
        ''', family_id)
        return [dict(row) for row in rows] 

@track_db_errors
async def delete_user_and_progress_pg(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Удаляем прогресс по всем группам слов
        await conn.execute('DELETE FROM user_family_progress WHERE user_id = $1', user_id)
        await conn.execute('DELETE FROM user_task_progress WHERE user_id = $1', user_id)
        await conn.execute('DELETE FROM user_last_opened_family_place WHERE user_id = $1', user_id)
        await conn.execute('DELETE FROM user_stage_state WHERE user_id = $1', user_id)
        await conn.execute('DELETE FROM user_current_family WHERE user_id = $1', user_id)
        # Удаляем самого пользователя
        await conn.execute('DELETE FROM users WHERE user_id = $1', user_id) 

@track_db_errors
async def set_family_finished_pg(user_id: int, family_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_family_progress (user_id, family_idx, finished, completion_count)
            VALUES ($1, $2, 1, 1)
            ON CONFLICT (user_id, family_idx) DO UPDATE SET 
                finished = 1,
                completion_count = user_family_progress.completion_count + 1
        ''', user_id, family_idx)

@track_db_errors
async def increment_family_completion_count_pg(user_id: int, family_idx: int):
    """Увеличивает счётчик завершений семьи на 1."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_family_progress (user_id, family_idx, completion_count)
            VALUES ($1, $2, 1)
            ON CONFLICT (user_id, family_idx) DO UPDATE SET 
                completion_count = user_family_progress.completion_count + 1
        ''', user_id, family_idx)

@track_db_errors
async def get_family_completion_count_pg(user_id: int, family_idx: int) -> int:
    """Получает количество завершений семьи для пользователя."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT completion_count FROM user_family_progress
            WHERE user_id = $1 AND family_idx = $2
        ''', user_id, family_idx)
        return row['completion_count'] if row else 0

@track_db_errors
async def get_families_completion_counts_pg(user_id: int, family_ids: List[int]) -> Dict[int, int]:
    """Получает количество завершений для списка семей пользователя."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if not family_ids:
            return {}
        
        rows = await conn.fetch('''
            SELECT family_idx, completion_count FROM user_family_progress
            WHERE user_id = $1 AND family_idx = ANY($2::int[])
        ''', user_id, family_ids)
        
        result = {}
        for row in rows:
            result[row['family_idx']] = row['completion_count']
        
        # Ensure all requested family_ids are in the result
        for fam_id in family_ids:
            if fam_id not in result:
                result[fam_id] = 0
        
        return result

@track_db_errors
async def get_family_finished_users_pg(family_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT user_id FROM user_family_progress WHERE family_idx = $1 AND finished = 1
        ''', family_idx)
        return [row['user_id'] for row in rows]

@track_db_errors
async def get_family_total_completion_count_pg(family_idx: int) -> int:
    """Получает общее количество завершений семьи всеми пользователями."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT COALESCE(SUM(completion_count), 0) as total_count 
            FROM user_family_progress 
            WHERE family_idx = $1
        ''', family_idx)
        return row['total_count'] if row else 0

@track_db_errors
async def mark_user_active_pg(user_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            UPDATE users SET last_active_at = NOW() WHERE user_id = $1
        ''', user_id) 

@track_db_errors
async def set_last_active_pg(user_id: int, ts: float):
    """Обновляет last_active_at в таблице users на заданный timestamp (UNIX time)."""
    import datetime
    pool = await get_pool()
    async with pool.acquire() as conn:
        dt = datetime.datetime.fromtimestamp(ts)
        await conn.execute('''
            UPDATE users SET last_active_at = $2 WHERE user_id = $1
        ''', user_id, dt)

@track_db_errors
async def bulk_set_last_active_pg(user_ids: List[int], timestamps: List[float]):
    """Массово обновляет last_active_at для списка пользователей одним запросом.
    user_ids и timestamps должны быть одинаковой длины; timestamps — UNIX time.
    """
    if not user_ids or not timestamps or len(user_ids) != len(timestamps):
        return
    import datetime
    pool = await get_pool()
    async with pool.acquire() as conn:
        dt_list = [datetime.datetime.fromtimestamp(ts) for ts in timestamps]
        await conn.execute('''
            UPDATE users AS u
            SET last_active_at = data.ts
            FROM (
                SELECT unnest($1::bigint[]) AS user_id,
                       unnest($2::timestamptz[]) AS ts
            ) AS data
            WHERE u.user_id = data.user_id
              AND (u.last_active_at IS NULL OR u.last_active_at < data.ts)
        ''', user_ids, dt_list)

async def mark_user_active_if_needed(user_id, context):
    now = datetime.datetime.now().timestamp()
    context.user_data['last_active_update_ts'] = now
    context.user_data['last_active_committed'] = False
    
    # Оптимизация: ограничение размера user_data
    if hasattr(context, 'application') and hasattr(context.application, 'user_data'):
        app_user_data = context.application.user_data
        if len(app_user_data) > MAX_USER_DATA_SIZE:
            # Удаляем самых старых пользователей (по времени последней активности)
            sorted_users = sorted(
                app_user_data.items(),
                key=lambda x: x[1].get('last_active_update_ts', 0)
            )
            users_to_remove = len(app_user_data) - MAX_USER_DATA_SIZE
            for i in range(users_to_remove):
                user_id_to_remove = sorted_users[i][0]
                del app_user_data[user_id_to_remove]
            logger.info(f"Очищен user_data: удалено {users_to_remove} старых пользователей")
    
    # Важно: не вызываем никакой 'оригинальной' функции здесь — это и есть базовая реализация.
    return

@track_db_errors
async def delete_all_user_progress_for_family(family_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # MCP: Сохраняем completion_count для всех пользователей перед удалением
        rows = await conn.fetch('''
            SELECT user_id, completion_count FROM user_family_progress 
            WHERE family_idx = $1 AND completion_count > 0
        ''', family_idx)
        
        # Удаляем прогресс
        await conn.execute('DELETE FROM user_family_progress WHERE family_idx = $1', family_idx)
        await conn.execute('DELETE FROM user_task_progress WHERE family_idx = $1', family_idx)
        await conn.execute('DELETE FROM user_last_opened_family_place WHERE family_idx = $1', family_idx)
        await conn.execute('DELETE FROM user_stage_state WHERE family_idx = $1', family_idx)
        await conn.execute('DELETE FROM user_current_family WHERE family_idx = $1', family_idx)
        
        # Восстанавливаем записи с сохранёнными completion_count
        for row in rows:
            await conn.execute('''
                INSERT INTO user_family_progress (user_id, family_idx, completion_count)
                VALUES ($1, $2, $3)
            ''', row['user_id'], family_idx, row['completion_count'])

@track_db_errors
async def set_stage7_answer_pg(user_id: int, family_idx: int, task_idx: int, selected_choice_text: str, is_correct: bool):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO user_task_progress (user_id, family_idx, stage_num, task_idx, is_done, selected_choice_text, is_correct)
            VALUES ($1, $2, 7, $3, 1, $4, $5)
            ON CONFLICT (user_id, family_idx, stage_num, task_idx) DO UPDATE SET
                is_done = 1, selected_choice_text = $4, is_correct = $5
        ''', user_id, family_idx, task_idx, selected_choice_text, 1 if is_correct else 0)

@track_db_errors
async def get_stage7_answer_pg(user_id: int, family_idx: int, task_idx: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow('''
            SELECT selected_choice_text, is_correct FROM user_task_progress
            WHERE user_id = $1 AND family_idx = $2 AND stage_num = 7 AND task_idx = $3 AND is_done = 1
        ''', user_id, family_idx, task_idx)
        if row and row['selected_choice_text'] is not None:
            return (row['selected_choice_text'], bool(row['is_correct']))
        return None 
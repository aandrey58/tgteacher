import asyncpg
from tgteacher_bot.db.pool import get_pool
import json
from typing import List, Dict, Any, Tuple
import functools
import time
from collections import deque

DB_ERRORS = deque(maxlen=1000)  # Оптимизировано: уменьшено с 5000 до 1000 для экономии памяти

def track_db_errors(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            DB_ERRORS.append((time.time(), str(e)))
            raise
    return wrapper

async def save_family_to_pg(family_data: dict, folder_name: str = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Вставляем основную информацию о группе слов
        target_value = (family_data.get('target') or 'VIP+FREE')
        family_id = await conn.fetchval('''
            INSERT INTO families (name, description, folder_name, target)
            VALUES ($1, $2, $3, $4)
            RETURNING id
        ''', family_data['name'], family_data.get('description'), folder_name, target_value)

        # Вставляем слова для Этапа 1
        words_to_insert = [
            (family_id, i, w['word'], w.get('translation'), w.get('example'), w.get('example_translation'), w.get('hint'))
            for i, w in enumerate(family_data.get('words', []))
        ]
        if words_to_insert:
            await conn.executemany('''
                INSERT INTO stage1_words (family_id, word_order, word, translation, example, example_translation, hint)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', words_to_insert)

        # Вставляем задания для Этапа 2
        s2_tasks_to_insert = [
            (family_id, i, t['sentence'], t['answer'], json.dumps(t['choices']), t.get('explanation'))
            for i, t in enumerate(family_data.get('stage2_tasks', []))
        ]
        if s2_tasks_to_insert:
            await conn.executemany('''
                INSERT INTO stage2_tasks (family_id, task_order, sentence, answer, choices, explanation)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
            ''', s2_tasks_to_insert)

        # Вставляем задания для Этапа 4 (аналог Этапа 2)
        s4_tasks_to_insert = [
            (family_id, i, t['sentence'], t['answer'], json.dumps(t['choices']), t.get('explanation'))
            for i, t in enumerate(family_data.get('stage4_tasks', []))
        ]
        if s4_tasks_to_insert:
            await conn.executemany('''
                INSERT INTO stage4_tasks (family_id, task_order, sentence, answer, choices, explanation)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
            ''', s4_tasks_to_insert)

        # Вставляем задания для Этапа 3
        s3_tasks_to_insert = [
            (family_id, i, t['word'], t['definition'], t.get('explanation'), json.dumps(t.get('choices', [])))
            for i, t in enumerate(family_data.get('stage3_tasks', []))
        ]
        if s3_tasks_to_insert:
            await conn.executemany('''
                INSERT INTO stage3_tasks (family_id, task_order, word, definition, explanation, choices)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
            ''', s3_tasks_to_insert)

        # Вставляем задания для Этапа 5 (картинка + ответ)
        s5_tasks_to_insert = [
            (family_id, i, t.get('image'), t['answer'], json.dumps(t.get('alternatives', [])), t.get('explanation'))
            for i, t in enumerate(family_data.get('stage5_tasks', []))
        ]
        if s5_tasks_to_insert:
            await conn.executemany('''
                INSERT INTO stage5_tasks (family_id, task_order, image, answer, alternatives, explanation)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
            ''', s5_tasks_to_insert)

        # Вставляем задания для Этапа 6
        s6_tasks_to_insert = [
            (family_id, i, t['word'], json.dumps(t['synonyms']), json.dumps(t['wrong_synonyms']), t.get('explanation'))
            for i, t in enumerate(family_data.get('stage6_tasks', []))
        ]
        if s6_tasks_to_insert:
            await conn.executemany('''
                INSERT INTO stage6_tasks (family_id, task_order, word, synonyms, wrong_synonyms, explanation)
                VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
            ''', s6_tasks_to_insert)

        # Вставляем задания для Этапа 7 (аудио + выбор)
        s7_tasks_to_insert = [
            (family_id, i, t.get('task'), t.get('audio'), t['answer'], json.dumps(t.get('choices', [])), t.get('explanation'))
            for i, t in enumerate(family_data.get('stage7_tasks', []))
        ]
        if s7_tasks_to_insert:
            await conn.executemany('''
                INSERT INTO stage7_tasks (family_id, task_order, task, audio, answer, choices, explanation)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            ''', s7_tasks_to_insert)

        # Вставляем задания для Этапа 8
        s8_tasks_to_insert = [
            (family_id, i, t['text'], json.dumps(t['answers']), json.dumps(t.get('explanations', [])), t.get('equal'))
            for i, t in enumerate(family_data.get('stage8_tasks', []))
        ]
        if s8_tasks_to_insert:
            await conn.executemany('''
                INSERT INTO stage8_tasks (family_id, task_order, text, answers, explanations, equal)
                VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
            ''', s8_tasks_to_insert)
        return family_id

async def get_all_families_meta_pg() -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('''
            SELECT id, name, description, folder_name, target FROM families ORDER BY id DESC
        ''')
        return [{'id': row['id'], 'name': row['name'], 'description': row['description'], 'folder_name': row['folder_name'], 'target': row['target']} for row in rows]

@track_db_errors
async def get_family_data_pg(family_id: int) -> Dict[str, Any] | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        family_meta = await conn.fetchrow('''
            SELECT id, name, description, folder_name, target FROM families WHERE id = $1
        ''', family_id)
        if not family_meta: return None
        
        family = {
            'id': family_meta['id'],
            'name': family_meta['name'],
            'description': family_meta['description'],
            'folder_name': family_meta['folder_name'],
            'target': family_meta['target']
        }

        return family

@track_db_errors
async def get_families_data_bulk(family_ids: list):
    """Получает данные о нескольких группах слов одним запросом."""
    if not family_ids:
        return {}
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch('SELECT * FROM families WHERE id = ANY($1::int[])', family_ids)
        return {row['id']: dict(row) for row in rows}

@track_db_errors
async def get_stage1_words_pg(family_id: int) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        words = await conn.fetch('''
            SELECT word, translation, example, example_translation, hint FROM stage1_words WHERE family_id = $1 ORDER BY word_order
        ''', family_id)
        return [{'word': r['word'], 'translation': r['translation'], 'example': r['example'], 'example_translation': r['example_translation'], 'hint': r['hint']} for r in words]

@track_db_errors
async def get_stage2_tasks_pg(family_id: int) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        s2_tasks = await conn.fetch('''
            SELECT sentence, answer, choices, explanation FROM stage2_tasks WHERE family_id = $1 ORDER BY task_order
        ''', family_id)
        return [{'sentence': r['sentence'], 'answer': r['answer'], 'choices': json.loads(r['choices']), 'explanation': r['explanation']} for r in s2_tasks]

@track_db_errors
async def get_stage4_tasks_pg(family_id: int) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        s4_tasks = await conn.fetch('''
            SELECT sentence, answer, choices, explanation FROM stage4_tasks WHERE family_id = $1 ORDER BY task_order
        ''', family_id)
        return [{'sentence': r['sentence'], 'answer': r['answer'], 'choices': json.loads(r['choices']), 'explanation': r['explanation']} for r in s4_tasks]

@track_db_errors
async def get_stage3_tasks_pg(family_id: int) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        s3_tasks = await conn.fetch('''
            SELECT word, definition, explanation, choices FROM stage3_tasks WHERE family_id = $1 ORDER BY task_order
        ''', family_id)
        tasks_list = []
        for r in s3_tasks:
            task = {'word': r['word'], 'definition': r['definition'], 'explanation': r['explanation']}
            if r['choices']:
                task['choices'] = json.loads(r['choices'])
            tasks_list.append(task)
        return tasks_list

@track_db_errors
async def get_stage5_tasks_pg(family_id: int) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        s5_tasks = await conn.fetch('''
            SELECT image, answer, alternatives, explanation FROM stage5_tasks WHERE family_id = $1 ORDER BY task_order
        ''', family_id)
        tasks = []
        for r in s5_tasks:
            task = {'image': r['image'], 'answer': r['answer'], 'explanation': r['explanation']}
            if r['alternatives']:
                task['alternatives'] = json.loads(r['alternatives'])
            else:
                task['alternatives'] = []
            tasks.append(task)
        return tasks

@track_db_errors
async def get_stage6_tasks_pg(family_id: int) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        s6_tasks = await conn.fetch('''
            SELECT word, synonyms, wrong_synonyms, explanation FROM stage6_tasks WHERE family_id = $1 ORDER BY task_order
        ''', family_id)
        return [{'word': r['word'], 'synonyms': json.loads(r['synonyms']), 'wrong_synonyms': json.loads(r['wrong_synonyms']), 'explanation': r['explanation']} for r in s6_tasks]

@track_db_errors
async def get_stage7_tasks_pg(family_id: int) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        s7_tasks = await conn.fetch('''
            SELECT task, audio, answer, choices, explanation FROM stage7_tasks WHERE family_id = $1 ORDER BY task_order
        ''', family_id)
        tasks = []
        for r in s7_tasks:
            task = {'task': r['task'], 'audio': r['audio'], 'answer': r['answer'], 'explanation': r['explanation']}
            if r['choices']:
                task['choices'] = json.loads(r['choices'])
            else:
                task['choices'] = []
            tasks.append(task)
        return tasks

@track_db_errors
async def get_stage8_tasks_pg(family_id: int) -> List[Dict[str, Any]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        s8_tasks_raw = await conn.fetch('''
            SELECT text, answers, explanations, equal FROM stage8_tasks WHERE family_id = $1 ORDER BY task_order
        ''', family_id)
        tasks_list = []
        for r in s8_tasks_raw:
            task = {'text': r['text'], 'answers': json.loads(r['answers'])}
            if r['explanations']: task['explanations'] = json.loads(r['explanations'])
            if r['equal']: task['equal'] = r['equal']
            tasks_list.append(task)
        return tasks_list

@track_db_errors
async def delete_family_from_pg(family_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute('''
            DELETE FROM families WHERE id = $1
        ''', family_id)
    # MCP: Удаляем прогресс юзеров по этой группе слов
    from tgteacher_bot.db.user_repo import delete_all_user_progress_for_family
    await delete_all_user_progress_for_family(family_id)

@track_db_errors
async def get_all_stage_tasks_counts_for_families(family_ids: List[int]) -> Dict[int, Dict[int, int]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        results = {}
        if not family_ids:
            return results

        # Query for Stage 1 word counts
        s1_counts = await conn.fetch('''
            SELECT family_id, COUNT(*) as count FROM stage1_words WHERE family_id = ANY($1::int[]) GROUP BY family_id
        ''', family_ids)
        for row in s1_counts:
            results.setdefault(row['family_id'], {})[1] = row['count']

        # Query for Stage 2 task counts
        s2_counts = await conn.fetch('''
            SELECT family_id, COUNT(*) as count FROM stage2_tasks WHERE family_id = ANY($1::int[]) GROUP BY family_id
        ''', family_ids)
        for row in s2_counts:
            results.setdefault(row['family_id'], {})[2] = row['count']

        # Query for Stage 4 task counts
        s4_counts = await conn.fetch('''
            SELECT family_id, COUNT(*) as count FROM stage4_tasks WHERE family_id = ANY($1::int[]) GROUP BY family_id
        ''', family_ids)
        for row in s4_counts:
            results.setdefault(row['family_id'], {})[4] = row['count']

        # Query for Stage 3 task counts
        s3_counts = await conn.fetch('''
            SELECT family_id, COUNT(*) as count FROM stage3_tasks WHERE family_id = ANY($1::int[]) GROUP BY family_id
        ''', family_ids)
        for row in s3_counts:
            results.setdefault(row['family_id'], {})[3] = row['count']

        # Query for Stage 5 task counts
        s5_counts = await conn.fetch('''
            SELECT family_id, COUNT(*) as count FROM stage5_tasks WHERE family_id = ANY($1::int[]) GROUP BY family_id
        ''', family_ids)
        for row in s5_counts:
            results.setdefault(row['family_id'], {})[5] = row['count']

        # Query for Stage 6 task counts
        s6_counts = await conn.fetch('''
            SELECT family_id, COUNT(*) as count FROM stage6_tasks WHERE family_id = ANY($1::int[]) GROUP BY family_id
        ''', family_ids)
        for row in s6_counts:
            results.setdefault(row['family_id'], {})[6] = row['count']

        # Query for Stage 7 task counts
        s7_counts = await conn.fetch('''
            SELECT family_id, COUNT(*) as count FROM stage7_tasks WHERE family_id = ANY($1::int[]) GROUP BY family_id
        ''', family_ids)
        for row in s7_counts:
            results.setdefault(row['family_id'], {})[7] = row['count']

        # Query for Stage 8 task counts
        s8_counts = await conn.fetch('''
            SELECT family_id, COUNT(*) as count FROM stage8_tasks WHERE family_id = ANY($1::int[]) GROUP BY family_id
        ''', family_ids)
        for row in s8_counts:
            results.setdefault(row['family_id'], {})[8] = row['count']
        
        # Ensure all requested family_ids are in the results, even if they have 0 tasks for a stage
        for fam_id in family_ids:
            results.setdefault(fam_id, {})
            for stage_num in [1, 2, 3, 4, 5, 6, 7, 8]: # Ensure all stages are present with 0 if no tasks
                results[fam_id].setdefault(stage_num, 0)

        return results 
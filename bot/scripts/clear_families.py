#!/usr/bin/env python3
"""
Скрипт для очистки всех семей слов из базы данных PostgreSQL
"""
import asyncio
import sys
import os
sys.path.append('bot')
from tgteacher_bot.db.user_repo import init_db_pg, get_pool

async def clear_all_families():
    """Очищает все семьи слов из базы данных"""
    print("🔧 Инициализация базы данных...")
    await init_db_pg()
    
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Проверяем текущее состояние
        print("📊 Проверяем текущие семьи...")
        families = await conn.fetch('SELECT id, name, target FROM families ORDER BY id')
        print(f"Всего семей в базе: {len(families)}")
        for f in families:
            print(f"  ID: {f['id']}, Name: '{f['name']}', Target: '{f['target']}'")
        
        if len(families) == 0:
            print("✅ База данных уже пуста")
            return
        
        # Подтверждение
        print(f"\n⚠️  ВНИМАНИЕ! Вы собираетесь удалить {len(families)} семей из базы данных!")
        print("Это действие необратимо! Все данные о семьях слов будут потеряны.")
        
        confirm = input("\nВведите 'YES' для подтверждения удаления: ")
        if confirm != 'YES':
            print("❌ Операция отменена")
            return
        
        # Удаляем все семьи (CASCADE удалит связанные данные)
        print("\n🗑️  Удаляем все семьи...")
        try:
            # Сначала удаляем все связанные данные
            print("  - Удаляем прогресс пользователей...")
            await conn.execute('DELETE FROM user_family_progress')
            await conn.execute('DELETE FROM user_task_progress')
            await conn.execute('DELETE FROM user_last_opened_family_place')
            await conn.execute('DELETE FROM user_stage_state')
            await conn.execute('DELETE FROM user_current_family')
            
            print("  - Удаляем задания всех этапов...")
            await conn.execute('DELETE FROM stage1_words')
            await conn.execute('DELETE FROM stage2_tasks')
            await conn.execute('DELETE FROM stage3_tasks')
            await conn.execute('DELETE FROM stage4_tasks')
            await conn.execute('DELETE FROM stage5_tasks')
            await conn.execute('DELETE FROM stage6_tasks')
            await conn.execute('DELETE FROM stage7_tasks')
            await conn.execute('DELETE FROM stage8_tasks')
            
            print("  - Удаляем семьи...")
            await conn.execute('DELETE FROM families')
            
            print("✅ Все семьи успешно удалены!")
            
        except Exception as e:
            print(f"❌ Ошибка при удалении: {e}")
            return
        
        # Проверяем результат
        print("\n📊 Проверяем результат...")
        families_after = await conn.fetch('SELECT COUNT(*) FROM families')
        count = families_after[0]['count']
        print(f"Семей в базе после удаления: {count}")
        
        if count == 0:
            print("✅ База данных полностью очищена!")
        else:
            print("⚠️  В базе остались семьи")

if __name__ == "__main__":
    asyncio.run(clear_all_families())

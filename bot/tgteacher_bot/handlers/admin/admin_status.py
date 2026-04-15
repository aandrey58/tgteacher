import time
import os
import platform
import functools
import statistics
from telegram import Bot
from collections import deque # Импортируем deque

try:
    import psutil
except ImportError:
    psutil = None

# BOT_START_TIME убираем отсюда

# MCP: Глобальные переменные для сбора метрик
RESPONSE_TIMES = deque(maxlen=1000)  # Оптимизировано: уменьшено с 5000 до 1000 для экономии памяти
ERRORS = deque(maxlen=200)  # Оптимизировано: уменьшено с 1000 до 200 для экономии памяти

# Декоратор для сбора времени ответа и ошибок
def track_metrics(handler):
    @functools.wraps(handler)
    async def wrapper(update, context, *args, **kwargs):
        start = time.time()
        try:
            result = await handler(update, context, *args, **kwargs)
        except Exception as e:
            ERRORS.append((time.time(), str(e)))
            raise
        finally:
            RESPONSE_TIMES.append(time.time() - start)
        return result
    return wrapper

# Функция для подсчёта ошибок за 24 часа
def get_errors_last_24h():
    now = time.time()
    return len([t for t, _ in ERRORS if now - t < 86400])

# Функция для среднего времени ответа
def get_avg_response_time():
    if not RESPONSE_TIMES:
        return 0
    return round(statistics.mean(RESPONSE_TIMES), 3)

# Пинг до базы
async def get_db_ping():
    try:
        from tgteacher_bot.db.pool import get_pool
        pool = await get_pool()
        start = time.time()
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1;")
        return int((time.time() - start) * 1000)  # ms
    except Exception as e:
        return f'Ошибка: {e}'

# Пинг до Telegram API
async def get_telegram_ping(context):
    try:
        bot: Bot = context.bot
        start = time.time()
        await bot.get_me()
        return int((time.time() - start) * 1000)  # ms
    except Exception as e:
        return f'Ошибка: {e}'

async def get_pg_connections_count():
    try:
        from tgteacher_bot.db.pool import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database();")
            return row
    except Exception as e:
        return f'Ошибка: {e}'

from tgteacher_bot.db.families_repo import DB_ERRORS

def get_db_errors_last_24h():
    now = time.time()
    return len([t for t, _ in DB_ERRORS if now - t < 86400])

async def get_status_text(context, bot_start_time):
    if psutil:
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        ram_mb = mem_info.rss // (1024*1024)
        ram_total = psutil.virtual_memory().total // (1024*1024)
        ram_used = psutil.virtual_memory().used // (1024*1024)
        ram_free = psutil.virtual_memory().available // (1024*1024)
        swap = psutil.swap_memory().used // (1024*1024)
        cpu_percent = process.cpu_percent(interval=0.5)
        cpu_per_core = psutil.cpu_percent(percpu=True)
        disk = psutil.disk_usage('/')
        disk_free = disk.free // (1024*1024)
        disk_total = disk.total // (1024*1024)
        threads = process.num_threads()
    else:
        ram_mb = ram_total = ram_used = ram_free = swap = cpu_percent = cpu_per_core = disk_free = disk_total = threads = 'psutil не установлен'
    uptime = int(time.time() - bot_start_time)
    uptime_str = f"{uptime//3600}ч {(uptime%3600)//60}м {uptime%60}с"
    restart_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(bot_start_time))

    user_data = getattr(context.application, 'user_data', {})
    user_data_size = sum([len(str(v)) for v in user_data.values()])
    # Топ-5 самых жирных юзеров
    top_users = sorted(user_data.items(), key=lambda x: len(str(x[1])), reverse=True)[:5]
    top_users_str = '\n'.join([f"<code>{uid}</code>: {len(str(data))} символов" for uid, data in top_users])

    pg_connections = await get_pg_connections_count()
    db_ping = await get_db_ping()
    tg_ping = await get_telegram_ping(context)
    avg_resp = get_avg_response_time()
    errors_24h = get_errors_last_24h()
    db_errors_24h = get_db_errors_last_24h()

    text = (
        f"🖥️ <b>Статус бота</b>\n\n"
        f"💾 <b>RAM процесса:</b> {ram_mb} MB\n"
        f"💾 <b>RAM всего:</b> {ram_used}/{ram_total} MB (свободно: {ram_free} MB)\n"
        f"💾 <b>Swap:</b> {swap} MB\n"
        f"🧠 <b>CPU:</b> {cpu_percent}% (по ядрам: {cpu_per_core})\n"
        f"🧵 <b>Threads:</b> {threads}\n"
        f"⏱ <b>Uptime:</b> {uptime_str}\n"
        f"🕒 <b>Последний рестарт:</b> {restart_time_str}\n"
        f"💽 <b>Диск:</b> {disk_free}/{disk_total} MB свободно\n"
        f"📦 <b>Размер user_data:</b> {user_data_size} символов\n\n"
        f"📋 <b>Топ-5 больших user_data:</b>\n{top_users_str}\n\n"
        f"🗄 <b>PostgreSQL соединений:</b> {pg_connections}\n"
        f"⏳ <b>Среднее время ответа:</b> {avg_resp} сек\n"
        f"❗ <b>Ошибок за 24ч:</b> {errors_24h}\n"
        f"❗ <b>Ошибок по БД за 24ч:</b> {db_errors_24h}\n"
        f"🏓 <b>Пинг до базы:</b> {db_ping} мс\n"
        f"🏓 <b>Пинг до Telegram API:</b> {tg_ping} мс\n"
        f"🐍 <b>Python:</b> {platform.python_version()}\n"
        f"💻 <b>OS:</b> {platform.system()} {platform.release()}\n"
    )
    if not psutil:
        text += '\n⚠️ <b>psutil не установлен, часть информации недоступна</b>'
    return text 
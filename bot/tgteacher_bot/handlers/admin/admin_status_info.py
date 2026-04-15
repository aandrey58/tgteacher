from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, CallbackQueryHandler

STATUS_INFO_BUTTONS = [
    [
        InlineKeyboardButton('💾 RAM процесса', callback_data='status_info_ram_proc'),
        InlineKeyboardButton('💾 RAM всего', callback_data='status_info_ram_total')
    ],
    [
        InlineKeyboardButton('💾 Swap', callback_data='status_info_swap'),
        InlineKeyboardButton('🧠 CPU', callback_data='status_info_cpu')
    ],
    [
        InlineKeyboardButton('🧵 Threads', callback_data='status_info_threads'),
        InlineKeyboardButton('⏱ Uptime', callback_data='status_info_uptime')
    ],
    [
        InlineKeyboardButton('💽 Диск', callback_data='status_info_disk'),
        InlineKeyboardButton('📦 user_data', callback_data='status_info_userdata')
    ],
    [InlineKeyboardButton('🗄 PostgreSQL соединений', callback_data='status_info_pg')],
    [
        InlineKeyboardButton('🐍 Python', callback_data='status_info_python'),
        InlineKeyboardButton('💻 OS', callback_data='status_info_os')
    ],
    [InlineKeyboardButton('⬅️ Назад', callback_data='admin_status')],
]

# Добавляем новые кнопки для новых метрик
STATUS_INFO_BUTTONS.insert(2, [
    InlineKeyboardButton('⏳ Среднее время ответа', callback_data='status_info_avg_resp'),
    InlineKeyboardButton('❗ Ошибки за 24ч', callback_data='status_info_errors_24h')
])
STATUS_INFO_BUTTONS.insert(3, [
    InlineKeyboardButton('🕒 Рестарт', callback_data='status_info_restart_time'),
    InlineKeyboardButton('❗ Ошибки по БД', callback_data='status_info_db_errors_24h')
])
STATUS_INFO_BUTTONS.insert(5, [
    InlineKeyboardButton('🏓 Пинг до базы', callback_data='status_info_db_ping'),
    InlineKeyboardButton('🏓 Пинг до Telegram', callback_data='status_info_tg_ping')
])

def get_status_info_menu():
    return InlineKeyboardMarkup(STATUS_INFO_BUTTONS)

# Тексты справки по каждому пункту
INFO_TEXTS = {
    'status_info_ram_proc': (
        '💾 <b>RAM процесса</b>\n\n'
        'Объём оперативной памяти, используемый текущим процессом бота (Python).\n'
        '<b>Обычно</b>: 50–200 МБ.\n'
        '<b>Внимание</b>: если показатель постоянно растёт или превышает 500 МБ, возможна утечка памяти.\n'
        '<b>Зачем следить</b>: для контроля стабильности и предотвращения сбоев.'
    ),
    'status_info_ram_total': (
        '💾 <b>RAM всего</b>\n\n'
        'Общий объём и использование оперативной памяти на сервере.\n'
        '<b>Обычно</b>: если свободно более 1 ГБ — всё в порядке.\n'
        '<b>Внимание</b>: если свободно менее 200 МБ, возможны замедления.\n'
        '<b>Зачем следить</b>: чтобы система не начала использовать swap и не тормозила.'
    ),
    'status_info_swap': (
        '💾 <b>Swap</b>\n\n'
        'Использование подкачки (виртуальной памяти на диске).\n'
        '<b>Обычно</b>: 0–200 МБ.\n'
        '<b>Внимание</b>: если swap растёт и RAM заполнена — возможны проблемы с производительностью.\n'
        '<b>Зачем следить</b>: избыток swap приводит к замедлению работы.'
    ),
    'status_info_cpu': (
        '🧠 <b>CPU</b>\n\n'
        'Загрузка процессора ботом.\n\n'
        '<b>Общее CPU:</b>\n'
        '• <b>Обычно:</b> 0–10% — бот работает нормально, не грузит сервер.\n'
        '• <b>30% и выше:</b> ощутимая нагрузка, стоит проверить, что выполняется.\n'
        '• <b>50% и выше:</b> возможны проблемы с производительностью, требуется анализ.\n\n'
        '<b>По ядрам:</b>\n'
        '• <b>0–10% на ядро</b> — оптимально, бот практически не нагружает сервер.\n'
        '• <b>10–30%</b> — допустимо при активной работе бота.\n'
        '• <b>30–50%</b> — заметная нагрузка, рекомендуется наблюдать за динамикой.\n'
        '• <b>50% и выше</b> — возможны проблемы с производительностью, требуется анализ и оптимизация.\n'
        '• <b>Если только одно ядро загружено на 100%</b> — это характерно для Python-ботов, однако при снижении производительности стоит искать узкое место в коде.\n'
        '• <b>Если все ядра загружены на 80–100%</b> — сервер работает на пределе, необходима оптимизация или увеличение ресурсов.'
    ),
    'status_info_threads': (
        '🧵 <b>Threads</b>\n\n'
        'Количество потоков, используемых процессом бота.\n'
        '<b>Обычно</b>: 5–20.\n'
        '<b>Внимание</b>: резкий рост может указывать на ошибку в коде.\n'
        '<b>Зачем следить</b>: для контроля корректной работы многопоточности.'
    ),
    'status_info_uptime': (
        '⏱ <b>Uptime</b>\n\n'
        'Время непрерывной работы бота без перезапуска.\n'
        '<b>Обычно</b>: часы или дни.\n'
        '<b>Внимание</b>: если бот часто перезапускается — возможны сбои.\n'
        '<b>Зачем следить</b>: для оценки стабильности работы.'
    ),
    'status_info_disk': (
        '💽 <b>Диск</b>\n\n'
        'Свободное и занятое место на диске сервера.\n'
        '<b>Обычно</b>: свободно более 1 ГБ.\n'
        '<b>Внимание</b>: если свободно менее 200 МБ — возможны сбои при записи данных.\n'
        '<b>Зачем следить</b>: чтобы избежать остановки из-за нехватки места.'
    ),
    'status_info_userdata': (
        '📦 <b>user_data</b>\n\n'
        'Размер пользовательских данных и топ-5 самых больших записей.\n'
        '<b>Обычно</b>: сотни–тысячи символов.\n'
        '<b>Внимание</b>: чрезмерный рост может привести к увеличению расхода памяти.\n'
        '<b>Зачем следить</b>: для оптимизации использования ресурсов.'
    ),
    'status_info_pg': (
        '🗄 <b>PostgreSQL соединений</b>\n\n'
        'Количество открытых соединений с базой данных PostgreSQL.\n'
        '<b>Обычно</b>: 1–10.\n'
        '<b>Внимание</b>: рост числа соединений может привести к исчерпанию лимита и сбоям.\n'
        '<b>Зачем следить</b>: для предотвращения проблем с доступом к базе.'
    ),
    'status_info_python': (
        '🐍 <b>Python</b>\n\n'
        'Версия Python, на которой работает бот.\n'
        '<b>Обычно</b>: любая поддерживаемая версия.\n'
        '<b>Внимание</b>: использование устаревшей версии может привести к проблемам с безопасностью и совместимостью.\n'
        '<b>Зачем следить</b>: для своевременного обновления и поддержки.'
    ),
    'status_info_os': (
        '💻 <b>OS</b>\n\n'
        'Операционная система, на которой запущен бот.\n'
        '<b>Обычно</b>: не критично.\n'
        '<b>Внимание</b>: некоторые функции могут зависеть от ОС.\n'
        '<b>Зачем следить</b>: для диагностики и поддержки.'
    ),
}

INFO_TEXTS.update({
    'status_info_avg_resp': (
        '⏳ <b>Среднее время ответа</b>\n\n'
        'Показывает среднее время, за которое бот обрабатывает входящие команды и события.\n'
        '<b>Обычно:</b> 0.1–1 сек.\n'
        '<b>Внимание:</b> если показатель стабильно выше 2 сек — возможны проблемы с производительностью или внешними сервисами.\n'
        '<b>Зачем следить:</b> помогает выявлять узкие места и оптимизировать работу бота.'
    ),
    'status_info_errors_24h': (
        '❗ <b>Ошибки за 24 часа</b>\n\n'
        'Отображает количество необработанных исключений, возникших за последние сутки.\n'
        '<b>Обычно:</b> 0–5.\n'
        '<b>Внимание:</b> рост числа ошибок может свидетельствовать о наличии багов или нестабильной работе внешних сервисов.\n'
        '<b>Зачем следить:</b> позволяет своевременно реагировать на сбои и поддерживать стабильность работы бота.'
    ),
    'status_info_restart_time': (
        '🕒 <b>Время последнего рестарта</b>\n\n'
        'Дата и время последнего запуска бота.\n'
        '<b>Обычно:</b> совпадает с аптаймом, но отображение даты и времени позволяет точнее отслеживать перезапуски.\n'
        '<b>Зачем следить:</b> помогает контролировать стабильность работы и отслеживать частоту рестартов.'
    ),
    'status_info_db_ping': (
        '🏓 <b>Пинг до базы</b>\n\n'
        'Время отклика базы данных PostgreSQL в миллисекундах.\n'
        '<b>Обычно:</b> 1–50 мс.\n'
        '<b>Внимание:</b> если значение превышает 200 мс — возможны проблемы с производительностью базы или сетевыми соединениями.\n'
        '<b>Зачем следить:</b> позволяет оперативно выявлять и устранять проблемы с доступом к данным.'
    ),
    'status_info_tg_ping': (
        '🏓 <b>Пинг до Telegram API</b>\n\n'
        'Время отклика Telegram API в миллисекундах.\n'
        '<b>Обычно:</b> 50–300 мс.\n'
        '<b>Внимание:</b> если значение превышает 1000 мс — возможны проблемы с интернет-соединением или перегрузка на стороне Telegram.\n'
        '<b>Зачем следить:</b> помогает понять, где возникают задержки — на стороне бота или Telegram.'
    ),
    'status_info_db_errors_24h': (
        '❗ <b>Ошибки по БД за 24 часа</b>\n\n'
        'Количество ошибок, возникших при работе с базой данных за последние сутки.\n'
        '<b>Обычно:</b> 0.\n'
        '<b>Внимание:</b> если появляются ошибки — возможны проблемы с подключением к базе, SQL-запросами или инфраструктурой.\n'
        '<b>Зачем следить:</b> чтобы вовремя замечать сбои в сохранении и получении данных.'
    ),
})

async def status_info_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    key = query.data
    if key == 'admin_status':
        from tgteacher_bot.handlers.admin.admin_panel import get_admin_status_menu
        await query.edit_message_text('🖥️ Статус бота', reply_markup=get_admin_status_menu())
        return
    text = INFO_TEXTS.get(key, 'Нет справки по этому пункту.')
    back_menu = InlineKeyboardMarkup([[InlineKeyboardButton('⬅️ Назад', callback_data='admin_status_info')]])
    await query.edit_message_text(text, reply_markup=back_menu, parse_mode='HTML')

def register_status_info_handlers(application):
    for key in INFO_TEXTS.keys():
        application.add_handler(CallbackQueryHandler(status_info_callback, pattern=f'^{key}$'))
    application.add_handler(CallbackQueryHandler(status_info_callback, pattern='^admin_status$')) 
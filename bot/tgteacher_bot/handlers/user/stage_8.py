from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage8_tasks_pg
from tgteacher_bot.db.user_repo import set_task_done_pg, set_last_opened_family_place_pg, get_last_opened_family_place_pg, set_stage8_answer_pg, get_stage8_answer_pg, get_user_stage_state_pg, get_current_family_idx_pg, mark_user_active_if_needed
from tgteacher_bot.utils.stage_state_manager import initialize_stage_state, update_stage_state, ensure_stage_state
from tgteacher_bot.utils.common import OK_MENU
import logging
import random
from tgteacher_bot.handlers.admin.admin_status import track_metrics

logger = logging.getLogger(__name__)

def get_number_emoji(n):
    base = ['0️⃣','1️⃣','2️⃣','3️⃣','4️⃣','5️⃣','6️⃣','7️⃣','8️⃣','9️⃣']
    if n < 10:
        return base[n]
    else:
        return ''.join([base[int(d)] for d in str(n)])

def get_stage8_keyboard(choices, selected_order, confirmed, correct_indices=None, error_indices=None, retry_count=0):
    keyboard = []
    row = []
    for i, word in enumerate(choices):
        label = word
        if i in selected_order:
            pos = selected_order.index(i)
            label += f' {get_number_emoji(pos+1)}'
            if confirmed:
                if correct_indices and pos in correct_indices:
                    label += ' ✅'
                elif error_indices and pos in error_indices:
                    label += ' ❌'
        button_cb = f'stage8_select_{i}' if not confirmed else 'stage8_no_action'
        row.append(InlineKeyboardButton(label, callback_data=button_cb))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    if retry_count == 0:
        if not confirmed:
            keyboard.append([InlineKeyboardButton('✅ Подтвердить выбор', callback_data='stage8_confirm')])
        elif confirmed and False:
            keyboard.append([InlineKeyboardButton('🔄 Попробовать снова', callback_data='stage8_retry')])
    elif retry_count == 1:
        if not confirmed:
            keyboard.append([InlineKeyboardButton('✅ Подтвердить выбор', callback_data='stage8_confirm')])
    else:
        keyboard.append([InlineKeyboardButton('✅ Подтвердить выбор', callback_data='stage8_confirm')])
    # Bottom row: stage8 — финальный этап. После подтверждения показываем только меню
    if confirmed:
        keyboard.append([InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')])
    else:
        keyboard.append([
            InlineKeyboardButton('🏁 Завершить', callback_data='stage8_finish'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    return InlineKeyboardMarkup(keyboard)

def get_stage8_text(task, selected_words, confirmed, feedbacks=None):
    text_body = task["text"].replace("\\n", "\n")
    text = f'<b>Вставь пропуски в текст:</b>\n\n{text_body}'
    if confirmed and feedbacks:
        for idx, fb in feedbacks.items():
            text += f'\n\n<b>Пропуск {idx+1}:</b> {fb}'
    return text

def analyze_stage8_answer(answers, explanations, equal, selected_order, shuffled_order):
    feedbacks = {}
    correct_indices = []
    error_indices = []
    equal_groups = []
    if equal:
        for group in equal.split(','):
            eq = [int(x.strip())-1 for x in group.split('=') if x.strip().isdigit()]
            if len(eq) > 1:
                equal_groups.append(eq)
    for i in range(len(answers)):
        if i >= len(selected_order):
            feedbacks[i] = explanations[i] if explanations and i < len(explanations) else 'Не выбран ответ.'
            error_indices.append(i)
            continue
        selected_word = answers[shuffled_order[selected_order[i]]]
        correct_word = answers[i]
        in_equal = False
        for group in equal_groups:
            if i in group:
                in_equal = True
                group_words = [answers[shuffled_order[selected_order[j]]] if j < len(selected_order) else None for j in group]
                if len(set(group_words)) == 1 and None not in group_words:
                    correct_indices.append(i)
                else:
                    feedbacks[i] = explanations[i] if explanations and i < len(explanations) else 'Ошибка.'
                    error_indices.append(i)
                break
        if not in_equal:
            if selected_word == correct_word:
                correct_indices.append(i)
            else:
                feedbacks[i] = explanations[i] if explanations and i < len(explanations) else 'Ошибка.'
                error_indices.append(i)
    return feedbacks, correct_indices, error_indices

async def get_default_stage8_state(user_id, family_id, stage_num):
    family_meta = await get_family_data_pg(family_id)
    if not family_meta:
        logger.error(f"Группа слов {family_id} не найдена при инициализации stage8 состояния.")
        return None

    # Для этапа 8 всегда начинаем с первого задания (task_idx = 0)
    tasks = await get_stage8_tasks_pg(family_id)
    task = tasks[0] if tasks else {}
    answers_count = len(task.get('answers', []))
    shuffled_order = list(range(answers_count))
    random.shuffle(shuffled_order)
    
    return {
        'family_idx': family_id,
        'selected_order': [],
        'confirmed': False,
        'shuffled_order': shuffled_order,
        'retry_count': 0
    }

@track_metrics
async def show_stage8_task(update, context):
    success, st8 = await ensure_stage_state(update, context, 'stage8', 8, get_default_stage8_state)
    if not success:
        return
    
    user_id = update.effective_user.id
    family_id = st8['family_idx']
    tasks = await get_stage8_tasks_pg(family_id)
    if not tasks:
        # Нет заданий для этапа 8 — завершаем семью как пройденную
        await stage8_finish_callback(update, context)
        return

    task = tasks[0]
    answers = task['answers']
    explanations = task.get('explanations', [None]*len(answers))
    equal = task.get('equal')
    # --- PATCH: сохраняем и восстанавливаем shuffled_order ---
    if 'shuffled_order' in st8 and st8['shuffled_order']:
        shuffled_order = st8['shuffled_order']
    else:
        shuffled_order = list(range(len(answers)))
        random.shuffle(shuffled_order)
        st8['shuffled_order'] = shuffled_order
        context.user_data['stage8'] = st8
        await update_stage_state(context, 'stage8', family_id, 8, user_id)
    # ГАРАНТИЯ: shuffled_order корректен
    if len(shuffled_order) != len(answers):
        logger.warning(f"[stage8] shuffled_order length mismatch for family {family_id}. Перегенерирую.")
        shuffled_order = list(range(len(answers)))
        random.shuffle(shuffled_order)
        st8['shuffled_order'] = shuffled_order
        context.user_data['stage8'] = st8
        await update_stage_state(context, 'stage8', family_id, 8, user_id)

    saved = await get_stage8_answer_pg(user_id, family_id, 0)
    if saved:
        selected_words, is_correct = saved
        selected_order = []
        for word in selected_words:
            if word in answers:
                idx = answers.index(word)
                # Проверяем, что idx существует в shuffled_order перед использованием
                if idx in shuffled_order:
                    selected_order.append(shuffled_order.index(idx))
                else:
                    logger.warning(f"Word {word} (original index {idx}) not found in shuffled_order for family {family_id}, stage 8.")

        st8['selected_order'] = selected_order
        st8['confirmed'] = True
        # Вот тут всегда анализируем ответ!
        feedbacks, correct_indices, error_indices = analyze_stage8_answer(
            answers, explanations, equal, selected_order, shuffled_order
        )
        st8['feedbacks'] = feedbacks
        st8['correct_indices'] = correct_indices
        st8['error_indices'] = error_indices
        context.user_data['stage8'] = st8
        # Сохраняем восстановленное состояние в БД
        await update_stage_state(context, 'stage8', family_id, 8, user_id)

    await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
    choices = [answers[i] for i in st8['shuffled_order']]
    selected_order = st8['selected_order']
    confirmed = st8['confirmed']
    feedbacks = st8.get('feedbacks')
    correct_indices = st8.get('correct_indices')
    error_indices = st8.get('error_indices')
    retry_count = st8.get('retry_count', 0)
    await update.callback_query.edit_message_text(
        get_stage8_text(task, [choices[i] for i in selected_order], confirmed, feedbacks),
        reply_markup=get_stage8_keyboard(choices, selected_order, confirmed, correct_indices, error_indices, retry_count=retry_count),
        parse_mode='HTML'
    )

@track_metrics
async def stage8_start(update, context):
    # Инициализация состояния через ensure_stage_state
    success, st8 = await ensure_stage_state(update, context, 'stage8', 8, get_default_stage8_state)
    if not success:
        return
    
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    
    family_id = st8['family_idx']
    tasks = await get_stage8_tasks_pg(family_id)
    if not tasks:
        # Нет заданий — считаем прохождение завершённым
        await stage8_finish_callback(update, context)
        return
    
    await show_stage8_task(update, context)

@track_metrics
async def stage8_select_callback(update, context):
    query = update.callback_query
    
    # Используем ensure_stage_state для получения состояния
    success, st8 = await ensure_stage_state(update, context, 'stage8', 8, get_default_stage8_state)
    if not success or st8['confirmed']:
        await query.answer()
        return

    idx = int(query.data.replace('stage8_select_', ''))
    selected_order = st8['selected_order']
    if idx in selected_order:
        pos = selected_order.index(idx)
        selected_order.pop(pos)
    else:
        if len(selected_order) < len(st8['shuffled_order']):
            selected_order.append(idx)
    st8['selected_order'] = selected_order
    context.user_data['stage8'] = st8
    user_id = update.effective_user.id
    family_id = st8['family_idx']
    await update_stage_state(context, 'stage8', family_id, 8, user_id)
    await mark_user_active_if_needed(user_id, context)
    tasks = await get_stage8_tasks_pg(family_id)

    if not tasks:
        # Если задач нет, это ошибка. Возможно, стоит отправить другое сообщение.
        await query.answer('Ошибка: задания для этапа 8 не найдены.', show_alert=True)
        return

    task = tasks[0]
    answers = task['answers']
    choices = [answers[i] for i in st8['shuffled_order']]
    await query.edit_message_reply_markup(
        reply_markup=get_stage8_keyboard(
            choices,
            selected_order,
            False
        )
    )

@track_metrics
async def stage8_confirm_callback(update, context):
    query = update.callback_query
    
    # Используем ensure_stage_state для получения состояния
    success, st8 = await ensure_stage_state(update, context, 'stage8', 8, get_default_stage8_state)
    if not success or st8['confirmed']:
        await query.answer()
        return
    
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

    family_id = st8['family_idx']
    tasks = await get_stage8_tasks_pg(family_id)

    if not tasks:
        # Если задач нет, это ошибка. Возможно, стоит отправить другое сообщение.
        await query.edit_message_text('Ошибка: задания для этапа 8 не найдены.')
        return

    task = tasks[0]
    answers = task['answers']
    explanations = task.get('explanations', [None]*len(answers))
    equal = task.get('equal')
    selected_order = st8['selected_order']
    shuffled_order = st8['shuffled_order']
    feedbacks = {}
    correct_indices = []
    error_indices = []
    equal_groups = []
    if equal:
        for group in equal.split(','):
            eq = [int(x.strip())-1 for x in group.split('=') if x.strip().isdigit()]
            if len(eq) > 1:
                equal_groups.append(eq)
    all_correct = True
    selected_words = []
    for i in range(len(answers)):
        if i >= len(selected_order):
            feedbacks[i] = explanations[i] or 'Не выбран ответ.'
            error_indices.append(i)
            all_correct = False
            selected_words.append('')
            continue
        selected_word = answers[shuffled_order[selected_order[i]]]
        selected_words.append(selected_word)
        correct_word = answers[i]
        in_equal = False
        for group in equal_groups:
            if i in group:
                in_equal = True
                # Добавлена проверка на наличие элемента в selected_order, чтобы избежать IndexError
                group_words = [answers[shuffled_order[selected_order[j]]] if j < len(selected_order) else None for j in group]
                if len(set(group_words)) == 1 and None not in group_words:
                    correct_indices.append(i)
                else:
                    feedbacks[i] = explanations[i] or 'Ошибка.'
                    error_indices.append(i)
                    all_correct = False
                break
        if not in_equal:
            if selected_word == correct_word:
                correct_indices.append(i)
            else:
                feedbacks[i] = explanations[i] or 'Ошибка.'
                error_indices.append(i)
                all_correct = False
    user_id = update.effective_user.id
    await set_stage8_answer_pg(user_id, family_id, 0, selected_words, all_correct)
    await set_task_done_pg(user_id, family_id, 8, 0)
    st8['confirmed'] = True
    st8['feedbacks'] = feedbacks
    st8['correct_indices'] = correct_indices
    st8['error_indices'] = error_indices
    context.user_data['stage8'] = st8
    await update_stage_state(context, 'stage8', family_id, 8, user_id)
    await show_stage8_task(update, context)

@track_metrics
async def stage8_retry_callback(update, context):
    query = update.callback_query
    
    # Используем ensure_stage_state для получения состояния
    success, st8 = await ensure_stage_state(update, context, 'stage8', 8, get_default_stage8_state)
    if not success:
        await query.answer()
        return

    if st8.get('retry_count', 0) >= 1:
        return
    family_id = st8['family_idx']
    tasks = await get_stage8_tasks_pg(family_id)

    if not tasks:
        # Если задач нет, это ошибка. Возможно, стоит отправить другое сообщение.
        await query.answer('Ошибка: задания для этапа 8 не найдены.', show_alert=True)
        return

    answers_count = len(tasks[0]['answers'])
    shuffled_order = list(range(answers_count))
    random.shuffle(shuffled_order)
    st8['selected_order'] = []
    st8['confirmed'] = False
    st8['feedbacks'] = {}
    st8['correct_indices'] = []
    st8['error_indices'] = []
    st8['shuffled_order'] = shuffled_order
    st8['retry_count'] = st8.get('retry_count', 0) + 1
    context.user_data['stage8'] = st8
    user_id = update.effective_user.id
    await update_stage_state(context, 'stage8', family_id, 8, user_id)
    await mark_user_active_if_needed(user_id, context)
    await show_stage8_task(update, context)

@track_metrics
async def stage8_no_action_callback(update, context):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await update.callback_query.answer()

@track_metrics
async def stage8_finish_callback(update, context):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    st8 = context.user_data.get('stage8')
    if st8:
        from tgteacher_bot.db.user_repo import set_family_finished_pg
        await set_family_finished_pg(user_id, st8['family_idx'])
        await set_last_opened_family_place_pg(user_id, st8['family_idx'], 8, 0)
        # Сохраняем финальное состояние в БД
        await update_stage_state(context, 'stage8', st8['family_idx'], 8, user_id)
    context.user_data.pop('stage8', None)
    family_meta = await get_family_data_pg(st8['family_idx'])
    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{st8['family_idx']}_0")],
        [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]])
    await query.edit_message_text(
        f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
        reply_markup=keyboard
    )

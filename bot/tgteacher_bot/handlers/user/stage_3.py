from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage3_tasks_pg
from tgteacher_bot.db.user_repo import set_task_done_pg, set_last_opened_family_place_pg, get_last_opened_family_place_pg, set_stage3_answer_pg, get_stage3_answer_pg, get_user_stage_state_pg, get_current_family_idx_pg, set_family_stage_done_pg, mark_user_active_if_needed
from tgteacher_bot.utils.stage_state_manager import initialize_stage_state, update_stage_state, ensure_stage_state
from tgteacher_bot.handlers.user.stage_6 import stage6_start
from tgteacher_bot.handlers.user.stage_4 import stage4_start
from tgteacher_bot.utils.common import OK_MENU
import logging
import random
from telegram.error import BadRequest
from tgteacher_bot.handlers.admin.admin_status import track_metrics

logger = logging.getLogger(__name__)

def get_stage3_keyboard(choices, answered_idx=None, correct_idx=None, task_idx=0, total_tasks=1, is_final_finish: bool = False):
    keyboard = []
    row = []
    for i, (text, is_correct, is_selected) in enumerate(choices):
        btn_text = text
        if is_selected:
            btn_text += ' ✅' if is_correct else ' ❌'
        if answered_idx is not None:
            if i == answered_idx:
                cb_data = f'stage3_answer_{i}'
            else:
                cb_data = 'stage3_no_action'
        else:
            cb_data = f'stage3_answer_{i}'
        row.append(InlineKeyboardButton(btn_text, callback_data=cb_data))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    
    nav_row = []
    if task_idx == 0:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage3_first_task_alert'))
    else:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage3_prev'))
    
    if task_idx < total_tasks - 1:
        nav_row.append(InlineKeyboardButton('Вперёд ➡️', callback_data='stage3_next'))
    else:
        nav_row.append(InlineKeyboardButton('🏁 Завершить' if is_final_finish else '✅ Далее', callback_data='stage3_finish'))
    keyboard.append(nav_row)
    
    # Bottom row: conditional per final stage
    if is_final_finish and task_idx == total_tasks - 1:
        keyboard.append([InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')])
    elif is_final_finish:
        keyboard.append([
            InlineKeyboardButton('🏁 Завершить', callback_data='stage3_finish'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    else:
        keyboard.append([
            InlineKeyboardButton('⏩ Пропустить этап', callback_data='stage3_skip_confirm'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    return InlineKeyboardMarkup(keyboard)

def get_stage3_text(task, idx, total):
    return f"Задание {idx+1}/{total}\n<b>Что означает это определение:</b>\n{task['definition']}"

async def get_default_stage3_state(user_id, family_id, stage_num):
    family_meta = await get_family_data_pg(family_id)
    if not family_meta:
        logger.error(f"Группа слов {family_id} не найдена при инициализации stage3 состояния.")
        return None

    last_place = await get_last_opened_family_place_pg(user_id, family_id)
    task_idx = 0
    if last_place and last_place[0] == 3:
        tasks = await get_stage3_tasks_pg(family_id) # <-- Загружаем задания отдельно
        if tasks:
            task_idx = min(last_place[1], len(tasks) - 1)

    # MCP PATCH: восстанавливаем displayed_choices_map и choices_order_map из состояния, если есть
    # (ensure_stage_state подставит их из БД, если они были сохранены)
    return {
        'family_idx': family_id,
        'task_idx': task_idx,
        'answered': False,
        'choices_order': None,
        'tasks_order': None,
    }

@track_metrics
async def show_stage3_task(update, context):
    success, st3 = await ensure_stage_state(update, context, 'stage3', 3, get_default_stage3_state)
    if not success:
        return
    
    family_id = st3['family_idx']
    tasks = await get_stage3_tasks_pg(family_id)
    idx_display = st3['task_idx']
    
    # Если этап 3 отсутствует — скипаем на этап 4
    if not tasks:
        from tgteacher_bot.handlers.user.stage_4 import stage4_start
        await stage4_start(update, context)
        return
    
    # Определяем, является ли текущий этап последним
    from tgteacher_bot.db.families_repo import get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
    has_next = False
    for fetch in (get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
        next_tasks = await fetch(family_id)
        if next_tasks:
            has_next = True
            break
    is_last_stage = not has_next
    
    # Initialize or validate tasks_order
    total = len(tasks)
    order = st3.get('tasks_order')
    if (not order) or (len(order) != total) or (sorted(order) != list(range(total))):
        order = list(range(total))
        random.shuffle(order)
        st3['tasks_order'] = order
        context.user_data['stage3'] = st3
        await update_stage_state(context, 'stage3', family_id, 3, update.effective_user.id)
    
    if idx_display >= len(tasks):
        logger.warning(f"Invalid task_idx {idx_display} for family {st3['family_idx']}, stage 3. Resetting to 0.")
        st3['task_idx'] = 0
        idx_display = 0
        context.user_data['stage3'] = st3
    
    orig_idx = order[idx_display]
    task = tasks[orig_idx]
    user_id = update.effective_user.id
    correct_answer = task['word']

    if 'word' not in task:
        logger.error(f"Неверный формат задания в группе слов {st3['family_idx']}, этап 3, задание {idx_display}. Отсутствует 'word'.")
        await update.callback_query.edit_message_text(
            "❗️ Ошибка в данных задания. Пожалуйста, сообщите администратору.",
            reply_markup=OK_MENU
        )
        return

    if 'choices' not in task or not task['choices']:
        logger.error(f"Нет вариантов #CHOICES в этапе 3, задание {idx_display}, группа слов {st3['family_idx']}")
        await update.callback_query.edit_message_text(
            "❗️ Ошибка: не указаны варианты ответов для задания. Пожалуйста, сообщите администратору.",
            reply_markup=OK_MENU
        )
        return

    all_choices = [c for c in task['choices'] if c != correct_answer]

    previous_answer = await get_stage3_answer_pg(user_id, st3['family_idx'], orig_idx)
    # --- PATCH: сохраняем и восстанавливаем displayed_choices ---
    if 'displayed_choices' in st3 and st3['displayed_choices']:
        displayed_choices = st3['displayed_choices']
    else:
        all_choices = [c for c in task['choices'] if c != correct_answer]
        random_distractors = random.sample(all_choices, 3)
        displayed_choices = [correct_answer] + random_distractors
        random.shuffle(displayed_choices)
        st3['displayed_choices'] = displayed_choices
        context.user_data['stage3'] = st3
        await update_stage_state(context, 'stage3', family_id, 3, user_id)
    # ГАРАНТИЯ: правильный ответ всегда в displayed_choices
    if correct_answer not in displayed_choices:
        all_choices = [c for c in task['choices'] if c != correct_answer]
        # logger.warning(f"[stage3] correct_answer '{correct_answer}' not in displayed_choices for family {family_id}, task {idx}. Перегенерирую варианты.")
        random_distractors = random.sample(all_choices, 3)
        displayed_choices = [correct_answer] + random_distractors
        random.shuffle(displayed_choices)
        st3['displayed_choices'] = displayed_choices
        context.user_data['stage3'] = st3
        await update_stage_state(context, 'stage3', family_id, 3, user_id)
    # сохраняем порядок
    st3['choices_order'] = list(range(4))  # всегда 4 варианта
    context.user_data['stage3'] = st3

    order_choices = st3['choices_order']
    choices = []
    answered_idx = None
    correct_idx = None

    if previous_answer:
        selected_text = previous_answer[0]
        _, is_correct = previous_answer  # Получаем правильность предыдущего ответа
        
        # Собираем варианты: предыдущий ответ + 3 рандомных из остальных
        choices = []
        choices.append((selected_text, is_correct, True))  # Предыдущий ответ
        
        # Собираем остальные варианты (исключая предыдущий ответ)
        other_choices = []
        for i, orig_idx_c in enumerate(order_choices):
            if displayed_choices[orig_idx_c] != selected_text:
                is_correct_choice = (orig_idx_c == displayed_choices.index(correct_answer))
                other_choices.append((displayed_choices[orig_idx_c], is_correct_choice, False))
        
        # Берем 3 рандомных из остальных
        random.shuffle(other_choices)
        choices.extend(other_choices[:3])
        
        # Смешиваем все 4 варианта
        random.shuffle(choices)
        
        # Находим индекс предыдущего ответа
        answered_idx = None
        for i, (text, _, is_selected) in enumerate(choices):
            if is_selected:
                answered_idx = i
                break
        
        correct_idx = None  # Не показываем правильный ответ, чтобы не путать
    else:
        for i, orig_idx_c in enumerate(order_choices):
            choices.append((displayed_choices[orig_idx_c], False, False))

    await set_last_opened_family_place_pg(user_id, st3['family_idx'], 3, idx_display)

    answered_idx = answered_idx if previous_answer else None
    correct_idx = correct_idx if previous_answer else None
    feedback_text = ''
    if previous_answer:
        _, is_correct = previous_answer
        if is_correct:
            feedback_text = f"\n\n✅ Верно! {task.get('explanation', '')}"
        else:
            feedback_text = f"\n\n❌ Неправильно! Правильный ответ: <b>{task['word']}</b>\n{task.get('explanation', '')}"

    await update.callback_query.edit_message_text(
        get_stage3_text(task, idx_display, len(tasks)) + feedback_text,
        reply_markup=get_stage3_keyboard(choices, answered_idx=answered_idx, correct_idx=correct_idx, task_idx=idx_display, total_tasks=len(tasks), is_final_finish=is_last_stage),
        parse_mode='HTML'
    )

@track_metrics
async def stage3_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    # logger.info(f"[stage3] stage3_start: callback_query.data={getattr(query, 'data', None)}, user_id={getattr(update.effective_user, 'id', None)}")
    # logger.info(f"[stage3] context.user_data['stage3'] (до ensure): {context.user_data.get('stage3')}")
    await query.answer()
    
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    
    success, st3 = await ensure_stage_state(update, context, 'stage3', 3, get_default_stage3_state)
    # logger.info(f"[stage3] context.user_data['stage3'] (после ensure): {context.user_data.get('stage3')}")
    if not success:
        return
    
    await show_stage3_task(update, context)

@track_metrics
async def stage3_answer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    success, st3 = await ensure_stage_state(update, context, 'stage3', 3, get_default_stage3_state)
    if not success:
        await query.answer()
        return
    
    family_id = st3['family_idx']
    tasks = await get_stage3_tasks_pg(family_id)
    # Ensure tasks_order
    order = st3.get('tasks_order') or list(range(len(tasks)))
    if (not order) or (len(order) != len(tasks)) or (sorted(order) != list(range(len(tasks)))):
        order = list(range(len(tasks)))
        random.shuffle(order)
        st3['tasks_order'] = order
        context.user_data['stage3'] = st3
        await update_stage_state(context, 'stage3', family_id, 3, update.effective_user.id)

    task_idx_display = st3['task_idx']
    task = tasks[order[task_idx_display]]
    displayed_choices = st3.get('displayed_choices')
    order_choices = st3.get('choices_order')
    if not displayed_choices or not order_choices:
        await query.edit_message_text('Ошибка: порядок вариантов потерян. Начни этап заново.', reply_markup=OK_MENU)
        return

    answer_idx = int(query.data.replace('stage3_answer_', ''))
    selected_orig_idx = order_choices[answer_idx]
    correct_answer = task['word']
    correct_orig_idx = displayed_choices.index(correct_answer)
    selected_text = displayed_choices[selected_orig_idx]

    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    is_correct = (selected_orig_idx == correct_orig_idx)
    await set_stage3_answer_pg(user_id, family_id, order[task_idx_display], selected_text, is_correct)
    
    feedback_text = ''
    if is_correct:
        await set_task_done_pg(user_id, family_id, 3, order[task_idx_display])
        feedback_text = f'✅ Верно! {task.get("explanation", "")}'
    else:
        feedback_text = f'❌ Неправильно! Правильный ответ: <b>{task["word"]}</b>\n{task.get("explanation", "")}'

    choices = []
    for i, orig_idx_c in enumerate(order_choices):
        choices.append((displayed_choices[orig_idx_c], (orig_idx_c == correct_orig_idx), (i == answer_idx)))
    
    st3['answered'] = True
    context.user_data['stage3'] = st3
    await update_stage_state(context, 'stage3', family_id, 3, user_id)

    # Определяем, является ли текущий этап последним
    from tgteacher_bot.db.families_repo import get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
    has_next = False
    for fetch in (get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
        next_tasks = await fetch(family_id)
        if next_tasks:
            has_next = True
            break
    is_last_stage = not has_next

    await query.edit_message_text(
        get_stage3_text(task, task_idx_display, len(tasks)) + f"\n\n{feedback_text}",
        reply_markup=get_stage3_keyboard(
            choices,
            answered_idx=answer_idx,
            correct_idx=correct_orig_idx,
            task_idx=task_idx_display,
            total_tasks=len(tasks),
            is_final_finish=is_last_stage
        ),
        parse_mode='HTML'
    )

@track_metrics
async def stage3_next_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    st3_before = context.user_data.get('stage3', {}).copy() if context.user_data.get('stage3') else None
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    success, st3 = await ensure_stage_state(update, context, 'stage3', 3, get_default_stage3_state)
    if not success:
        return
    tasks = await get_stage3_tasks_pg(st3['family_idx'])
    if st3['task_idx'] < len(tasks) - 1:
        st3['task_idx'] += 1
        st3['choices_order'] = None
        st3['displayed_choices'] = None  # MCP PATCH
        st3['answered'] = False
        st3['last_feedback_message'] = ''
        context.user_data['stage3'] = st3
        user_id = update.effective_user.id
        await update_stage_state(context, 'stage3', st3['family_idx'], 3, user_id)
        st3_after = context.user_data.get('stage3', {}).copy() if context.user_data.get('stage3') else None
        await show_stage3_task(update, context)
    else:
        await stage3_finish_callback(update, context)

@track_metrics
async def stage3_prev_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    st3_before = context.user_data.get('stage3', {}).copy() if context.user_data.get('stage3') else None
    await query.answer()

    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

    success, st3 = await ensure_stage_state(update, context, 'stage3', 3, get_default_stage3_state)
    if not success:
        return

    if st3['task_idx'] > 0:
        st3['task_idx'] -= 1
        st3['choices_order'] = None
        st3['displayed_choices'] = None  # MCP PATCH
        user_id = update.effective_user.id
        family_id = st3['family_idx']
        prev_answer = await get_stage3_answer_pg(user_id, family_id, st3['task_idx'])
        st3['answered'] = prev_answer is not None
        context.user_data['stage3'] = st3
        await update_stage_state(context, 'stage3', family_id, 3, user_id)
        st3_after = context.user_data.get('stage3', {}).copy() if context.user_data.get('stage3') else None
        await show_stage3_task(update, context)
    else:
        await query.answer('Это первое задание', show_alert=True)

@track_metrics
async def stage3_skip_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    # logger.info(f"[stage3] stage3_skip_confirm_callback: callback_query.data={getattr(query, 'data', None)}, user_id={getattr(update.effective_user, 'id', None)}")
    try:
        await query.answer()
    except BadRequest as e:
        logger.warning(f"Failed to answer callback query in stage3_skip_confirm_callback: {e}")
        pass
    text = ('Ты уверен, что хочешь пропустить этот этап?\nРекомендуем пройти все этапы для лучшего запоминания слов!')
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да, пропустить', callback_data='stage3_skip'),
            InlineKeyboardButton('❌ Отмена', callback_data='stage3_cancel_skip')
        ]
    ])
    await query.edit_message_text(text, reply_markup=keyboard)
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

@track_metrics
async def stage3_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    # logger.info(f"[stage3] stage3_skip_callback: callback_query.data={getattr(query, 'data', None)}, user_id={getattr(update.effective_user, 'id', None)}")
    await query.answer()
    context.user_data.pop('stage3', None)
    await stage4_start(update, context)
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

@track_metrics
async def stage3_cancel_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # logger.info(f"[stage3] stage3_cancel_skip_callback: callback_query.data={getattr(getattr(update, 'callback_query', None), 'data', None)}, user_id={getattr(update.effective_user, 'id', None)}")
    await show_stage3_task(update, context)
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

@track_metrics
async def stage3_finish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    # logger.info(f"[stage3] stage3_finish_callback: callback_query.data={getattr(query, 'data', None)}, user_id={getattr(update.effective_user, 'id', None)}")
    await query.answer()
    st3 = context.user_data.get('stage3')
    # logger.info(f"[stage3] st3 (finish): {st3}")
    if st3:
        user_id = update.effective_user.id
        await set_family_stage_done_pg(user_id, st3['family_idx'], 3)
        family_id = st3['family_idx']
        family_meta = await get_family_data_pg(family_id)
        # Проверяем, есть ли задачи на этапах 4–8
        from tgteacher_bot.db.families_repo import get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
        has_next = False
        for fetch in (get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
            next_tasks = await fetch(family_id)
            if next_tasks:
                has_next = True
                break
        context.user_data.pop('stage3', None)
        if has_next:
            await stage4_start(update, context)
        else:
            from tgteacher_bot.db.user_repo import set_family_finished_pg
            await set_family_finished_pg(user_id, family_id)
            await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
            await update_stage_state(context, 'stage3', family_id, 3, user_id)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{family_id}_0")],
                [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
            ])
            await query.edit_message_text(
                f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
                reply_markup=keyboard
            )
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)

@track_metrics
async def stage3_no_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # logger.info(f"[stage3] stage3_no_action_callback: callback_query.data={getattr(getattr(update, 'callback_query', None), 'data', None)}, user_id={getattr(update.effective_user, 'id', None)}")
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await update.callback_query.answer()

@track_metrics
async def stage3_first_task_alert_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # logger.info(f"[stage3] stage3_first_task_alert_callback: callback_query.data={getattr(getattr(update, 'callback_query', None), 'data', None)}, user_id={getattr(update.effective_user, 'id', None)}")
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await update.callback_query.answer('Это первое задание.', show_alert=False)

async def await_stage3_is_final_finish(family_id: int, task_idx: int, total_tasks: int) -> bool:
    if task_idx != total_tasks - 1:
        return False
    from tgteacher_bot.db.families_repo import (
        get_stage4_tasks_pg,
        get_stage5_tasks_pg,
        get_stage6_tasks_pg,
        get_stage7_tasks_pg,
        get_stage8_tasks_pg,
    )
    for fetch in (get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
        next_tasks = await fetch(family_id)
        if next_tasks:
            return False
    return True

    
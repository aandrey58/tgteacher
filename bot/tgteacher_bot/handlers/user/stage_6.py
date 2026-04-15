from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage6_tasks_pg
from tgteacher_bot.db.user_repo import set_task_done_pg, set_last_opened_family_place_pg, get_last_opened_family_place_pg, set_stage6_answer_pg, get_stage6_answer_pg, set_family_stage_done_pg, set_family_finished_pg, get_user_stage_state_pg, get_current_family_idx_pg, mark_user_active_if_needed
from tgteacher_bot.utils.stage_state_manager import initialize_stage_state, update_stage_state, ensure_stage_state
import logging
import random
from telegram.error import BadRequest
from tgteacher_bot.utils.common import OK_MENU
from tgteacher_bot.handlers.admin.admin_status import track_metrics

logger = logging.getLogger(__name__)

def get_stage6_keyboard(choices, selected_choices, is_answered, correct_synonyms, task_idx, total_tasks, is_final_finish: bool = False):
    keyboard = []
    buttons_row = []
    for i, choice_text in enumerate(choices):
        button_label = choice_text
        is_current_selected = choice_text in selected_choices
        is_correct_synonym = choice_text in correct_synonyms

        if is_answered:
            if is_current_selected and is_correct_synonym:
                button_label += " ✅"
            elif is_current_selected and not is_correct_synonym:
                button_label += " ❌"
            elif not is_current_selected and is_correct_synonym:
                button_label += " ✅"
            callback_data = 'stage6_no_action' # Disable after answer
        else:
            button_label += " ✅" if is_current_selected else ''
            callback_data = f'stage6_toggle_choice_{i}'
        
        buttons_row.append(InlineKeyboardButton(button_label, callback_data=callback_data))
        if len(buttons_row) == 2:
            keyboard.append(buttons_row)
            buttons_row = []
    if buttons_row:
        keyboard.append(buttons_row)

    if not is_answered:
        keyboard.append([InlineKeyboardButton('✅ Подтвердить выбор', callback_data='stage6_confirm')])

    nav_row = []
    if task_idx == 0:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage6_first_task_alert'))
    else:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage6_prev'))
    
    if task_idx < total_tasks - 1:
        nav_row.append(InlineKeyboardButton('Вперёд ➡️', callback_data='stage6_next'))
    else:
        nav_row.append(InlineKeyboardButton('🏁 Завершить' if is_final_finish else '✅ Далее', callback_data='stage6_finish'))
    keyboard.append(nav_row)
    
    if is_final_finish and task_idx == total_tasks - 1:
        keyboard.append([InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')])
    elif is_final_finish:
        keyboard.append([
            InlineKeyboardButton('🏁 Завершить', callback_data='stage6_finish'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    else:
        keyboard.append([
            InlineKeyboardButton('⏩ Пропустить этап', callback_data='stage6_skip_confirm'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    return InlineKeyboardMarkup(keyboard)

def get_stage6_text(task, idx, total, feedback_text=''):
    text = f"Задание {idx+1}/{total}\n<b>Выбери все синонимы к слову:</b>\n{task['word']}"
    if feedback_text:
        text += f'\n\n{feedback_text}'
    return text

async def get_default_stage6_state(user_id, family_id, stage_num):
    family_meta = await get_family_data_pg(family_id)
    if not family_meta:
        logger.error(f"Группа слов {family_id} не найдена при инициализации stage6 состояния.")
        return None

    last_place = await get_last_opened_family_place_pg(user_id, family_id)
    task_idx = 0
    if last_place and last_place[0] == 6:
        tasks = await get_stage6_tasks_pg(family_id)
        if tasks:
            task_idx = min(last_place[1], len(tasks) - 1)

    return {
        'family_idx': family_id,
        'task_idx': task_idx,
        'selected_choices': [],
        'answered': False,
        'choices_order': None,
        'tasks_order': None,
    }

@track_metrics
async def show_stage6_task(update: Update, context: ContextTypes.DEFAULT_TYPE, feedback_message=''):
    success, st6 = await ensure_stage_state(update, context, 'stage6', 6, get_default_stage6_state)
    if not success:
        return
    
    family_id = st6['family_idx']
    
    tasks = await get_stage6_tasks_pg(family_id)
    
    if not tasks:
        # Если этап 6 отсутствует — пробуем перейти на 7, затем на 8
        from tgteacher_bot.db.families_repo import get_stage7_tasks_pg
        stage7_tasks = await get_stage7_tasks_pg(family_id)
        if stage7_tasks and len(stage7_tasks) > 0:
            from tgteacher_bot.handlers.user.stage_7 import stage7_start
            await stage7_start(update, context)
            return
        from tgteacher_bot.db.families_repo import get_stage8_tasks_pg
        stage8_tasks = await get_stage8_tasks_pg(family_id)
        if stage8_tasks and len(stage8_tasks) > 0:
            from tgteacher_bot.handlers.user.stage_8 import stage8_start
            await stage8_start(update, context)
            return
        # Если 7-8 тоже отсутствуют — завершаем семью
        user_id = update.effective_user.id
        await set_family_finished_pg(user_id, family_id)
        await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
        await update_stage_state(context, 'stage6', family_id, 6, user_id)
        family_meta = await get_family_data_pg(family_id)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{family_id}_0")],
            [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
        ])
        await update.callback_query.edit_message_text(
            f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
            reply_markup=keyboard
        )
        return
    
    # Initialize or validate tasks_order
    total = len(tasks)
    order = st6.get('tasks_order')
    if (not order) or (len(order) != total) or (sorted(order) != list(range(total))):
        order = list(range(total))
        random.shuffle(order)
        st6['tasks_order'] = order
        context.user_data['stage6'] = st6
        await update_stage_state(context, 'stage6', family_id, 6, update.effective_user.id)

    idx_display = st6['task_idx']
    
    if idx_display >= len(tasks):
        logger.warning(f"Invalid task_idx {idx_display} for family {st6['family_idx']}, stage 6. Resetting to 0.")
        st6['task_idx'] = 0
        idx_display = 0
        context.user_data['stage6'] = st6
    
    task = tasks[order[idx_display]]
    user_id = update.effective_user.id

    # Определяем, является ли текущий этап последним
    from tgteacher_bot.db.families_repo import get_stage7_tasks_pg, get_stage8_tasks_pg
    has_next = False
    for fetch in (get_stage7_tasks_pg, get_stage8_tasks_pg):
        next_tasks = await fetch(family_id)
        if next_tasks:
            has_next = True
            break
    is_last_stage = not has_next

    # Определяем, нужно ли показывать "Завершить"
    is_final_finish = is_last_stage and (idx_display == len(tasks) - 1)

    synonyms = list(task['synonyms'])
    wrong_synonyms = list(task['wrong_synonyms'])
    n_syn = len(synonyms)
    n_wrong_needed = 4 - n_syn
    if n_wrong_needed < 0:
        logger.error(f"Слишком много синонимов для этапа 6, задание {idx_display}, группа слов {st6['family_idx']}")
        await update.callback_query.edit_message_text(
            "❗️ Ошибка: максимум 4 синонима для одного задания.",
            reply_markup=OK_MENU
        )
        return
    if len(wrong_synonyms) < n_wrong_needed:
        logger.error(f"Недостаточно неправильных вариантов для этапа 6, задание {idx_display}, группа слов {st6['family_idx']}")
        await update.callback_query.edit_message_text(
            "❗️ Ошибка: нужно минимум {n_wrong_needed} неправильных вариантов в #WRONG_SYNONYMS. Обратитесь к администратору.",
            reply_markup=OK_MENU
        )
        return
    # --- PATCH: сохраняем и восстанавливаем displayed_choices ---
    if 'displayed_choices' in st6 and st6['displayed_choices']:
        displayed_choices = st6['displayed_choices']
    else:
        selected_wrong = random.sample(wrong_synonyms, n_wrong_needed) if n_wrong_needed > 0 else []
        displayed_choices = synonyms + selected_wrong
        random.shuffle(displayed_choices)
        st6['displayed_choices'] = displayed_choices
        context.user_data['stage6'] = st6
        await update_stage_state(context, 'stage6', family_id, 6, user_id)
    # ГАРАНТИЯ: все правильные синонимы есть в displayed_choices
    if any(s not in displayed_choices for s in synonyms):
        logger.warning(f"[stage6] not all synonyms in displayed_choices for family {family_id}, task {idx_display}. Перегенерирую варианты.")
        selected_wrong = random.sample(wrong_synonyms, n_wrong_needed) if n_wrong_needed > 0 else []
        displayed_choices = synonyms + selected_wrong
        random.shuffle(displayed_choices)
        st6['displayed_choices'] = displayed_choices
        context.user_data['stage6'] = st6
        await update_stage_state(context, 'stage6', family_id, 6, user_id)

    previous_answer = await get_stage6_answer_pg(user_id, st6['family_idx'], order[idx_display])
    if previous_answer:
        st6['selected_choices'] = previous_answer[0]
        st6['answered'] = True
        st6['last_feedback_message'] = ''
    else:
        st6['selected_choices'] = []
        st6['answered'] = False
        st6['last_feedback_message'] = ''

    context.user_data['stage6'] = st6
    await set_last_opened_family_place_pg(user_id, st6['family_idx'], 6, idx_display)

    if st6['answered'] and not feedback_message:
        correct_synonyms = set(synonyms)
        user_selected_choices_for_feedback = set(previous_answer[0])
        is_correct_recalculated = (user_selected_choices_for_feedback == correct_synonyms)

        if is_correct_recalculated:
            feedback_message = f"✅ Верно! {task.get('explanation', '')}"
        else:
            incorrect_selected = user_selected_choices_for_feedback - correct_synonyms
            missing_correct = correct_synonyms - user_selected_choices_for_feedback
            
            feedback_message = '❌ Неправильно!\n'
            if incorrect_selected:
                feedback_message += f"Ты выбрал лишнее: {', '.join(incorrect_selected)}.\n"
            if missing_correct:
                feedback_message += f"Ты пропустил: {', '.join(missing_correct)}.\n"
            feedback_message += f"{task.get('explanation', '')}"

    await update.callback_query.edit_message_text(
        get_stage6_text(task, idx_display, len(tasks), feedback_message),
        reply_markup=get_stage6_keyboard(
            st6['displayed_choices'],
            st6['selected_choices'],
            st6['answered'],
            synonyms,
            idx_display,
            len(tasks),
            is_final_finish=is_last_stage
        ),
        parse_mode='HTML'
    )

@track_metrics
async def stage6_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    
    success, st6 = await ensure_stage_state(update, context, 'stage6', 6, get_default_stage6_state)
    if not success:
        return
    
    await show_stage6_task(update, context)

@track_metrics
async def stage6_toggle_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await query.answer()
    st6 = context.user_data.get('stage6')
    if not st6 or st6['answered']:
        return

    selected_idx = int(query.data.replace('stage6_toggle_choice_', ''))
    all_choices = st6['displayed_choices']
    choice_text = all_choices[selected_idx]
    
    if choice_text in st6['selected_choices']:
        st6['selected_choices'].remove(choice_text)
    else:
        st6['selected_choices'].append(choice_text)
    context.user_data['stage6'] = st6
    
    family_id = st6['family_idx']
    
    tasks = await get_stage6_tasks_pg(family_id)
    idx_display = st6['task_idx']
    # Ensure tasks_order
    order = st6.get('tasks_order') or list(range(len(tasks)))
    if (not order) or (len(order) != len(tasks)) or (sorted(order) != list(range(len(tasks)))):
        order = list(range(len(tasks)))
        random.shuffle(order)
        st6['tasks_order'] = order
        context.user_data['stage6'] = st6
        await update_stage_state(context, 'stage6', family_id, 6, update.effective_user.id)

    task = tasks[order[idx_display]]

    # Determine last-stage here too
    from tgteacher_bot.db.families_repo import get_stage7_tasks_pg, get_stage8_tasks_pg
    has_next = False
    for fetch in (get_stage7_tasks_pg, get_stage8_tasks_pg):
        next_tasks = await fetch(family_id)
        if next_tasks:
            has_next = True
            break
    is_last_stage = not has_next

    await query.edit_message_reply_markup(
        reply_markup=get_stage6_keyboard(
            all_choices,
            st6['selected_choices'],
            False,
            task['synonyms'],
            idx_display,
            len(tasks),
            is_final_finish=is_last_stage
        )
    )

@track_metrics
async def stage6_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    st6 = context.user_data.get('stage6')
    if not st6 or st6['answered']:
        await query.answer()
        return

    family_id = st6['family_idx']
    
    tasks = await get_stage6_tasks_pg(family_id)
    idx_display = st6['task_idx']
    # Ensure tasks_order
    order = st6.get('tasks_order') or list(range(len(tasks)))
    if (not order) or (len(order) != len(tasks)) or (sorted(order) != list(range(len(tasks)))):
        order = list(range(len(tasks)))
        random.shuffle(order)
        st6['tasks_order'] = order
        context.user_data['stage6'] = st6
        await update_stage_state(context, 'stage6', family_id, 6, user_id)

    task = tasks[order[idx_display]]

    correct_synonyms = set(task['synonyms'])
    user_selected_choices = set(st6['selected_choices'])

    is_correct = (user_selected_choices == correct_synonyms)
    await set_stage6_answer_pg(user_id, st6['family_idx'], order[idx_display], list(user_selected_choices), is_correct)
    await set_task_done_pg(user_id, st6['family_idx'], 6, order[idx_display])

    feedback_message = ''
    if is_correct:
        feedback_message = f'✅ Верно! {task.get("explanation", "")}'
    else:
        incorrect_selected = user_selected_choices - correct_synonyms
        missing_correct = correct_synonyms - user_selected_choices
        
        feedback_message = '❌ Неправильно!\n'
        if incorrect_selected:
            feedback_message += f"Ты выбрал лишнее: {', '.join(incorrect_selected)}.\n"
        if missing_correct:
            feedback_message += f"Ты пропустил: {', '.join(missing_correct)}.\n"
        feedback_message += f"{task.get('explanation', '')}"
    
    st6['answered'] = True
    st6['last_feedback_message'] = feedback_message
    context.user_data['stage6'] = st6
    await update_stage_state(context, 'stage6', family_id, 6, user_id)

    # Determine last-stage
    from tgteacher_bot.db.families_repo import get_stage7_tasks_pg, get_stage8_tasks_pg
    has_next = False
    for fetch in (get_stage7_tasks_pg, get_stage8_tasks_pg):
        next_tasks = await fetch(family_id)
        if next_tasks:
            has_next = True
            break
    is_last_stage = not has_next

    await query.edit_message_text(
        get_stage6_text(task, idx_display, len(tasks), feedback_message),
        reply_markup=get_stage6_keyboard(
            st6['displayed_choices'],
            st6['selected_choices'],
            True,
            task['synonyms'],
            idx_display,
            len(tasks),
            is_final_finish=is_last_stage
        ),
        parse_mode='HTML'
    )

@track_metrics
async def stage6_next_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    success, st6 = await ensure_stage_state(update, context, 'stage6', 6, get_default_stage6_state)
    if not success:
        return
    
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    
    family_id = st6['family_idx']
    
    tasks = await get_stage6_tasks_pg(family_id)
    if st6['task_idx'] < len(tasks) - 1:
        st6['task_idx'] += 1
        st6['choices_order'] = None
        st6['displayed_choices'] = None  # MCP PATCH
        st6['selected_choices'] = []
        st6['answered'] = False
        st6['last_feedback_message'] = ''
        context.user_data['stage6'] = st6
        user_id = update.effective_user.id
        await update_stage_state(context, 'stage6', family_id, 6, user_id)
        await show_stage6_task(update, context)
    else:
        await stage6_finish_callback(update, context)

@track_metrics
async def stage6_prev_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await query.answer()
    
    success, st6 = await ensure_stage_state(update, context, 'stage6', 6, get_default_stage6_state)
    if not success:
        return

    if st6['task_idx'] > 0:
        st6['task_idx'] -= 1
        st6['choices_order'] = None
        st6['displayed_choices'] = None  # MCP PATCH
        user_id = update.effective_user.id
        family_id = st6['family_idx']
        prev_answer_data = await get_stage6_answer_pg(user_id, family_id, st6['task_idx'])
        
        if prev_answer_data:
            st6['selected_choices'] = prev_answer_data[0]
            st6['answered'] = True
            st6['last_feedback_message'] = ''
        else:
            st6['selected_choices'] = []
            st6['answered'] = False
            st6['last_feedback_message'] = ''

        context.user_data['stage6'] = st6
        await update_stage_state(context, 'stage6', family_id, 6, user_id)
        await show_stage6_task(update, context)
    else:
        await query.answer('Это первое задание', show_alert=True)

@track_metrics
async def stage6_skip_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    try:
        await query.answer()
    except BadRequest as e:
        logger.warning(f"Failed to answer callback query in stage6_skip_confirm_callback: {e}")
    text = ('Ты уверен, что хочешь пропустить этот этап?\nРекомендуем пройти все этапы для лучшего запоминания слов!')
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да, пропустить', callback_data='stage6_skip'),
            InlineKeyboardButton('❌ Отмена', callback_data='stage6_cancel_skip')
        ]
    ])
    await query.edit_message_text(text, reply_markup=keyboard)

@track_metrics
async def stage6_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await query.answer()
    st6 = context.user_data.get('stage6')
    if st6:
        family_id = st6['family_idx']
        # Сначала пробуем этап 7, затем 8
        from tgteacher_bot.db.families_repo import get_stage7_tasks_pg
        stage7_tasks = await get_stage7_tasks_pg(family_id)
        context.user_data.pop('stage6', None)
        if stage7_tasks and len(stage7_tasks) > 0:
            from tgteacher_bot.handlers.user.stage_7 import stage7_start
            await stage7_start(update, context)
            return
        from tgteacher_bot.db.families_repo import get_stage8_tasks_pg
        stage8_tasks = await get_stage8_tasks_pg(family_id)
        if stage8_tasks and len(stage8_tasks) > 0:
            from tgteacher_bot.handlers.user.stage_8 import stage8_start
            await stage8_start(update, context)
            return
        # Если нет следующих этапов - завершаем семью
        await set_family_finished_pg(user_id, family_id)
        await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
        await update_stage_state(context, 'stage6', family_id, 6, user_id)
        family_meta = await get_family_data_pg(family_id)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{family_id}_0")],
            [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
        ])
        await query.edit_message_text(
            f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
            reply_markup=keyboard
        )
    else:
        context.user_data.pop('stage6', None)
        await query.edit_message_text('Этапы 7-8 не реализованы для этой группы слов.', reply_markup=OK_MENU)

@track_metrics
async def stage6_cancel_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await show_stage6_task(update, context)

@track_metrics
async def stage6_finish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await query.answer()
    st6 = context.user_data.get('stage6')
    if st6:
        family_id = st6['family_idx']
        await set_family_stage_done_pg(user_id, family_id, 6)
        # Сначала пробуем этап 7, затем 8
        from tgteacher_bot.db.families_repo import get_stage7_tasks_pg
        stage7_tasks = await get_stage7_tasks_pg(family_id)
        context.user_data.pop('stage6', None)
        if stage7_tasks and len(stage7_tasks) > 0:
            from tgteacher_bot.handlers.user.stage_7 import stage7_start
            await stage7_start(update, context)
            return
        from tgteacher_bot.db.families_repo import get_stage8_tasks_pg
        stage8_tasks = await get_stage8_tasks_pg(family_id)
        if stage8_tasks and len(stage8_tasks) > 0:
            from tgteacher_bot.handlers.user.stage_8 import stage8_start
            await stage8_start(update, context)
            return
        # Если нет следующих этапов - завершаем семью
        await set_family_finished_pg(user_id, family_id)
        await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
        await update_stage_state(context, 'stage6', family_id, 6, user_id)
        family_meta = await get_family_data_pg(family_id)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{family_id}_0")],
            [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
        ])
        await query.edit_message_text(
            f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
            reply_markup=keyboard
        )
    else:
        context.user_data.pop('stage6', None)
        await query.edit_message_text('Этапы 7-8 не реализованы для этой группы слов.', reply_markup=OK_MENU)

@track_metrics
async def stage6_no_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await update.callback_query.answer()

@track_metrics
async def stage6_first_task_alert_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    await update.callback_query.answer('Это первое задание.', show_alert=False)
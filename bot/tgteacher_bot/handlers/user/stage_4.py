from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage4_tasks_pg
from tgteacher_bot.db.user_repo import set_task_done_pg, set_last_opened_family_place_pg, get_last_opened_family_place_pg, set_stage4_answer_pg, get_stage4_answer_pg, set_family_stage_done_pg, get_user_stage_state_pg, get_current_family_idx_pg, mark_user_active_if_needed
from tgteacher_bot.utils.stage_state_manager import initialize_stage_state, update_stage_state, ensure_stage_state
from tgteacher_bot.handlers.user.stage_6 import stage6_start
from tgteacher_bot.utils.common import OK_MENU
from tgteacher_bot.handlers.admin.admin_status import track_metrics
import logging
import random

logger = logging.getLogger(__name__)

# Этап 4: полностью аналогичен этапу 2, но использует отдельный набор заданий и stage_num=4


def get_stage4_keyboard(choices, answered_idx=None, correct_idx=None, task_idx=0, total_tasks=1, is_final_finish: bool = False):
    keyboard = []
    row = []
    for i, (text, is_correct, is_selected) in enumerate(choices):
        btn_text = text
        if is_selected:
            btn_text += ' ✅' if is_correct else ' ❌'
        if answered_idx is not None:
            if i == answered_idx:
                cb_data = f'stage4_answer_{i}'
            else:
                cb_data = 'stage4_no_action'
        else:
            cb_data = f'stage4_answer_{i}'
        row.append(InlineKeyboardButton(btn_text, callback_data=cb_data))
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    nav_row = []
    if task_idx == 0:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage4_first_task_alert'))
    else:
        nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage4_prev'))
    if task_idx < total_tasks - 1:
        nav_row.append(InlineKeyboardButton('Вперёд ➡️', callback_data='stage4_next'))
    else:
        nav_row.append(InlineKeyboardButton('🏁 Завершить' if is_final_finish else '✅ Далее', callback_data='stage4_finish'))
    keyboard.append(nav_row)
    # Bottom row
    if is_final_finish and task_idx == total_tasks - 1:
        keyboard.append([InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')])
    elif is_final_finish:
        keyboard.append([
            InlineKeyboardButton('🏁 Завершить', callback_data='stage4_finish'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    else:
        keyboard.append([
            InlineKeyboardButton('⏩ Пропустить этап', callback_data='stage4_skip_confirm'),
            InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
        ])
    return InlineKeyboardMarkup(keyboard)


def get_stage4_text(task, idx, total):
    return f"Задание {idx+1}/{total}\n<b>Вставь подходящее слово:</b>\n{task['sentence']}"


async def get_default_stage4_state(user_id, family_id, stage_num):
    family_meta = await get_family_data_pg(family_id)
    if not family_meta:
        return None

    last_place = await get_last_opened_family_place_pg(user_id, family_id)
    task_idx = 0
    if last_place and last_place[0] == 4:
        tasks = await get_stage4_tasks_pg(family_id)
        if tasks:
            task_idx = min(last_place[1], len(tasks) - 1)

    return {
        'family_idx': family_id,
        'task_idx': task_idx,
        'answered': False,
        'choices_order': None,
        'tasks_order': None,
    }


@track_metrics
async def show_stage4_task(update, context):
    await mark_user_active_if_needed(update.effective_user.id, context)
    success, st4 = await ensure_stage_state(update, context, 'stage4', 4, get_default_stage4_state)
    if not success:
        return

    family_id = st4['family_idx']
    family_meta = await get_family_data_pg(family_id)
    if not family_meta:
        await update.callback_query.edit_message_text('Ошибка: группа слов не найдена.')
        return

    tasks = await get_stage4_tasks_pg(family_id)
    # Initialize or validate tasks_order
    total = len(tasks)
    order = st4.get('tasks_order')
    if (not order) or (len(order) != total) or (sorted(order) != list(range(total))):
        order = list(range(total))
        random.shuffle(order)
        st4['tasks_order'] = order
        context.user_data['stage4'] = st4
        await update_stage_state(context, 'stage4', family_id, 4, update.effective_user.id)

    idx_display = st4['task_idx']

    # Если этап 4 отсутствует — пробуем сразу этап 5, иначе этап 6
    if not tasks:
        from tgteacher_bot.db.families_repo import get_stage5_tasks_pg
        tasks5 = await get_stage5_tasks_pg(family_id)
        if tasks5 and len(tasks5) > 0:
            from tgteacher_bot.handlers.user.stage_5 import stage5_start
            await stage5_start(update, context)
            return
        from tgteacher_bot.handlers.user.stage_6 import stage6_start
        await stage6_start(update, context)
        return

    # Определяем, является ли текущий этап последним
    from tgteacher_bot.db.families_repo import get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
    has_next = False
    for fetch in (get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
        next_tasks = await fetch(family_id)
        if next_tasks:
            has_next = True
            break
    is_last_stage = not has_next

    if idx_display >= len(tasks):
        st4['task_idx'] = 0
        idx_display = 0
        context.user_data['stage4'] = st4

    orig_idx = order[idx_display]
    task = tasks[orig_idx]
    user_id = update.effective_user.id

    correct_answer = task['answer']

    if 'choices' not in task or 'answer' not in task:
        await update.callback_query.edit_message_text(
            'Ошибка в данных задания. Пожалуйста, сообщите администратору.',
            reply_markup=OK_MENU
        )
        return

    all_choices = [c for c in task['choices'] if c != correct_answer]

    # Используем те же функции хранения ответов, что и для этапа 2, но с отличающимся stage_num в last_opened
    previous_answer = await get_stage4_answer_pg(user_id, st4['family_idx'], orig_idx)

    if 'displayed_choices' in st4 and st4['displayed_choices']:
        displayed_choices = st4['displayed_choices']
    else:
        all_choices = [c for c in task['choices'] if c != correct_answer]
        random_distractors = random.sample(all_choices, min(3, len(all_choices)))
        displayed_choices = [correct_answer] + random_distractors
        random.shuffle(displayed_choices)
        st4['displayed_choices'] = displayed_choices
        context.user_data['stage4'] = st4
        await update_stage_state(context, 'stage4', family_id, 4, user_id)

    if correct_answer not in displayed_choices:
        all_choices = [c for c in task['choices'] if c != correct_answer]
        random_distractors = random.sample(all_choices, min(3, len(all_choices)))
        displayed_choices = [correct_answer] + random_distractors
        random.shuffle(displayed_choices)
        st4['displayed_choices'] = displayed_choices
        context.user_data['stage4'] = st4
        await update_stage_state(context, 'stage4', family_id, 4, user_id)

    st4['choices_order'] = list(range(len(displayed_choices)))
    context.user_data['stage4'] = st4

    order_choices = st4['choices_order']
    choices = []
    answered_idx = None
    correct_idx = None

    if previous_answer:
        selected_text, is_correct = previous_answer
        try:
            selected_orig_idx = displayed_choices.index(selected_text)
            correct_orig_idx = displayed_choices.index(correct_answer)
            for i, orig_idx_c in enumerate(order_choices):
                is_selected = (i == order_choices.index(selected_orig_idx))
                is_correct_choice = (orig_idx_c == correct_orig_idx)
                choices.append((displayed_choices[orig_idx_c], is_correct_choice, is_selected))
            answered_idx = order_choices.index(selected_orig_idx)
            correct_idx = order_choices.index(correct_orig_idx)
        except ValueError:
            pass
    else:
        for i, orig_idx_c in enumerate(order_choices):
            choices.append((displayed_choices[orig_idx_c], False, False))

    await set_last_opened_family_place_pg(user_id, st4['family_idx'], 4, idx_display)

    answered_idx = answered_idx if previous_answer else None
    correct_idx = correct_idx if previous_answer else None
    feedback_text = ''
    if previous_answer:
        selected_text, is_correct = previous_answer
        if is_correct:
            feedback_text = f"\n\n✅ Верно! {task.get('explanation', '')}"
        else:
            feedback_text = f"\n\n❌ Неправильно! Правильный ответ: <b>{task['answer']}</b>\n{task.get('explanation', '')}"

    await update.callback_query.edit_message_text(
        get_stage4_text(task, idx_display, len(tasks)) + feedback_text,
        reply_markup=get_stage4_keyboard(choices, answered_idx=answered_idx, correct_idx=correct_idx, task_idx=idx_display, total_tasks=len(tasks), is_final_finish=is_last_stage),
        parse_mode='HTML'
    )


@track_metrics
async def stage4_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await mark_user_active_if_needed(update.effective_user.id, context)

    success, st4 = await ensure_stage_state(update, context, 'stage4', 4, get_default_stage4_state)
    if not success:
        return

    await show_stage4_task(update, context)


@track_metrics
async def stage4_answer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    success, st4 = await ensure_stage_state(update, context, 'stage4', 4, get_default_stage4_state)
    if not success:
        await query.answer()
        return

    family_id = st4['family_idx']
    family_meta = await get_family_data_pg(family_id)

    if not family_meta:
        await query.edit_message_text('Ошибка: группа слов не найдена.')
        return

    tasks = await get_stage4_tasks_pg(family_id)
    # Ensure tasks_order
    order = st4.get('tasks_order') or list(range(len(tasks)))
    if (not order) or (len(order) != len(tasks)) or (sorted(order) != list(range(len(tasks)))):
        order = list(range(len(tasks)))
        random.shuffle(order)
        st4['tasks_order'] = order
        context.user_data['stage4'] = st4
        await update_stage_state(context, 'stage4', family_id, 4, update.effective_user.id)

    task_idx_display = st4['task_idx']
    task = tasks[order[task_idx_display]]
    displayed_choices = st4.get('displayed_choices')
    order_choices = st4.get('choices_order')
    if not displayed_choices or not order_choices:
        await query.edit_message_text('Ошибка: порядок вариантов потерян. Начни этап заново.', reply_markup=OK_MENU)
        return

    answer_idx = int(query.data.replace('stage4_answer_', ''))
    selected_orig_idx = order_choices[answer_idx]
    correct_answer = task['answer']
    correct_orig_idx = displayed_choices.index(correct_answer)
    selected_text = displayed_choices[selected_orig_idx]

    user_id = update.effective_user.id
    await mark_user_active_if_needed(user_id, context)
    is_correct = (selected_orig_idx == correct_orig_idx)

    # Сохраняем ответ для этапа 4
    await set_stage4_answer_pg(user_id, family_id, order[task_idx_display], selected_text, is_correct)

    feedback_text = ''
    if is_correct:
        await set_task_done_pg(user_id, family_id, 4, order[task_idx_display])
        feedback_text = f'✅ Верно! {task.get("explanation", "")}'
    else:
        feedback_text = f'❌ Неправильно! Правильный ответ: <b>{task["answer"]}</b>\n{task.get("explanation", "")}'

    choices = []
    for i, orig_idx_c in enumerate(order_choices):
        choices.append((displayed_choices[orig_idx_c], (orig_idx_c == correct_orig_idx), (i == answer_idx)))

    st4['answered'] = True
    context.user_data['stage4'] = st4
    await update_stage_state(context, 'stage4', family_id, 4, user_id)

    await query.edit_message_text(
        get_stage4_text(task, task_idx_display, len(tasks)) + f"\n\n{feedback_text}",
        reply_markup=get_stage4_keyboard(choices, answered_idx=answer_idx, correct_idx=correct_orig_idx, task_idx=task_idx_display, total_tasks=len(tasks)),
        parse_mode='HTML'
    )


@track_metrics
async def stage4_next_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await mark_user_active_if_needed(update.effective_user.id, context)
    success, st4 = await ensure_stage_state(update, context, 'stage4', 4, get_default_stage4_state)
    if not success:
        return

    family_id = st4['family_idx']

    tasks = await get_stage4_tasks_pg(family_id)
    if st4['task_idx'] < len(tasks) - 1:
        st4['task_idx'] += 1
        st4['choices_order'] = None
        st4['displayed_choices'] = None
        st4['answered'] = False
        st4['last_feedback_message'] = ''
        context.user_data['stage4'] = st4
        user_id = update.effective_user.id
        await update_stage_state(context, 'stage4', family_id, 4, user_id)
        await show_stage4_task(update, context)
    else:
        await update.callback_query.edit_message_text('Этап завершён!', reply_markup=OK_MENU)


@track_metrics
async def stage4_prev_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await mark_user_active_if_needed(update.effective_user.id, context)

    success, st4 = await ensure_stage_state(update, context, 'stage4', 4, get_default_stage4_state)
    if not success:
        return

    if st4['task_idx'] > 0:
        st4['task_idx'] -= 1
        st4['choices_order'] = None
        st4['displayed_choices'] = None
        user_id = update.effective_user.id
        family_id = st4['family_idx']
        prev_answer = await get_stage4_answer_pg(user_id, family_id, st4['task_idx'])
        st4['answered'] = prev_answer is not None
        context.user_data['stage4'] = st4
        await update_stage_state(context, 'stage4', family_id, 4, user_id)
        await show_stage4_task(update, context)
    else:
        await query.answer('Это первое задание', show_alert=True)


@track_metrics
async def stage4_skip_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass
    await mark_user_active_if_needed(update.effective_user.id, context)
    text = (
        'Ты уверен, что хочешь пропустить этот этап?\n'
        'Рекомендуем пройти все этапы для лучшего запоминания слов!'
    )
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton('✅ Да, пропустить', callback_data='stage4_skip'),
            InlineKeyboardButton('❌ Отмена', callback_data='stage4_cancel_skip')
        ]
    ])
    await query.edit_message_text(text, reply_markup=keyboard)


@track_metrics
async def stage4_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await mark_user_active_if_needed(update.effective_user.id, context)
    # Пропускаем этап 4 и переходим к этапу 5, если он есть; иначе к этапу 6
    family_id = context.user_data.get('current_family_idx')
    if family_id:
        from tgteacher_bot.db.families_repo import get_stage5_tasks_pg
        tasks5 = await get_stage5_tasks_pg(family_id)
        context.user_data.pop('stage4', None)
        if tasks5 and len(tasks5) > 0:
            from tgteacher_bot.handlers.user.stage_5 import stage5_start
            await stage5_start(update, context)
            return
    context.user_data.pop('stage4', None)
    await stage6_start(update, context)


@track_metrics
async def stage4_cancel_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await mark_user_active_if_needed(update.effective_user.id, context)
    await show_stage4_task(update, context)


@track_metrics
async def stage4_finish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await mark_user_active_if_needed(update.effective_user.id, context)

    st4 = context.user_data.get('stage4')
    if st4:
        user_id = update.effective_user.id
        await set_family_stage_done_pg(user_id, st4['family_idx'], 4)
        family_id = st4['family_idx']
        family_meta = await get_family_data_pg(family_id)
        # Проверяем, есть ли задачи на этапах 5–8
        from tgteacher_bot.db.families_repo import get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg
        has_next = False
        for fetch in (get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
            next_tasks = await fetch(family_id)
            if next_tasks:
                has_next = True
                break
        context.user_data.pop('stage4', None)
        if has_next:
            from tgteacher_bot.db.families_repo import get_stage5_tasks_pg as _g5
            tasks5 = await _g5(family_id)
            if tasks5 and len(tasks5) > 0:
                from tgteacher_bot.handlers.user.stage_5 import stage5_start
                await stage5_start(update, context)
            else:
                await stage6_start(update, context)
        else:
            from tgteacher_bot.db.user_repo import set_family_finished_pg
            await set_family_finished_pg(user_id, family_id)
            await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
            await update_stage_state(context, 'stage4', family_id, 4, user_id)
            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton('📈 Прогресс', callback_data=f"progress_select_{family_id}_0")],
                [InlineKeyboardButton('🏠 В меню', callback_data='main_menu')]
            ])
            await query.edit_message_text(
                f"✅ Все этапы по группе слов «{family_meta['name']}» пройдены!\nМожно посмотреть прогресс.",
                reply_markup=keyboard
            )


@track_metrics
async def stage4_no_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await mark_user_active_if_needed(update.effective_user.id, context)
    await update.callback_query.answer()


@track_metrics
async def stage4_first_task_alert_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await mark_user_active_if_needed(update.effective_user.id, context)
    await update.callback_query.answer('Это первое задание.', show_alert=False) 
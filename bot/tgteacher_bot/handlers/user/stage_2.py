from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
from tgteacher_bot.db.families_repo import get_family_data_pg, get_stage2_tasks_pg
from tgteacher_bot.db.user_repo import set_task_done_pg, set_last_opened_family_place_pg, get_last_opened_family_place_pg, set_stage2_answer_pg, get_stage2_answer_pg, set_family_stage_done_pg, get_user_stage_state_pg, get_current_family_idx_pg, mark_user_active_if_needed
from tgteacher_bot.utils.stage_state_manager import initialize_stage_state, update_stage_state, ensure_stage_state
from tgteacher_bot.handlers.user.stage_3 import stage3_start
import logging
import random
from telegram.error import BadRequest
from tgteacher_bot.utils.common import OK_MENU
from tgteacher_bot.handlers.admin.admin_status import track_metrics

logger = logging.getLogger(__name__)

def get_stage2_keyboard(choices, answered_idx=None, correct_idx=None, task_idx=0, total_tasks=1, is_final_finish: bool = False):
	keyboard = []
	row = []
	for i, (text, is_correct, is_selected) in enumerate(choices):
		btn_text = text
		if is_selected:
			btn_text += ' ✅' if is_correct else ' ❌'
		if answered_idx is not None:
			if i == answered_idx:
				cb_data = f'stage2_answer_{i}'
			else:
				cb_data = 'stage2_no_action'
		else:
			cb_data = f'stage2_answer_{i}'
		row.append(InlineKeyboardButton(btn_text, callback_data=cb_data))
		if len(row) == 2:
			keyboard.append(row)
			row = []
	if row:
		keyboard.append(row)
	nav_row = []
	if task_idx == 0:
		nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage2_first_task_alert'))
	else:
		nav_row.append(InlineKeyboardButton('⬅️ Назад', callback_data='stage2_prev'))
	if task_idx < total_tasks - 1:
		nav_row.append(InlineKeyboardButton('Вперёд ➡️', callback_data='stage2_next'))
	else:
		nav_row.append(InlineKeyboardButton('🏁 Завершить' if is_final_finish else '✅ Далее', callback_data='stage2_finish'))
	keyboard.append(nav_row)
	# Bottom row: either Skip+Menu, or Finish+Menu if last stage, or Menu only on last task of last stage
	if is_final_finish and task_idx == total_tasks - 1:
		keyboard.append([
			InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
		])
	elif is_final_finish:
		keyboard.append([
			InlineKeyboardButton('🏁 Завершить', callback_data='stage2_finish'),
			InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
		])
	else:
		keyboard.append([
			InlineKeyboardButton('⏩ Пропустить этап', callback_data='stage2_skip_confirm'),
			InlineKeyboardButton('🏠 Выйти в меню', callback_data='main_menu')
		])
	return InlineKeyboardMarkup(keyboard)

def get_stage2_text(task, idx, total):
	return f"Задание {idx+1}/{total}\n<b>Вставь подходящее слово:</b>\n{task['sentence']}"

async def get_default_stage2_state(user_id, family_id, stage_num):
	family_meta = await get_family_data_pg(family_id)
	if not family_meta:
		# logger.error(f"Группа слов {family_id} не найдена при инициализации stage2 состояния.")
		return None

	last_place = await get_last_opened_family_place_pg(user_id, family_id)
	task_idx = 0
	if last_place and last_place[0] == 2:
		tasks = await get_stage2_tasks_pg(family_id)
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
async def show_stage2_task(update, context):
	# logger.info(f"[stage2] show_stage2_task: callback_query.data={getattr(getattr(update, 'callback_query', None), 'data', None)}, user_id={getattr(update.effective_user, 'id', None)}")
	# logger.info(f"[stage2] context.user_data['stage2'] (до ensure): {context.user_data.get('stage2')}")
	success, st2 = await ensure_stage_state(update, context, 'stage2', 2, get_default_stage2_state)
	# logger.info(f"[stage2] context.user_data['stage2'] (после ensure): {context.user_data.get('stage2')}")
	if not success:
		return
	
	family_id = st2['family_idx']
	family_meta = await get_family_data_pg(family_id)

	if not family_meta:
		await update.callback_query.edit_message_text('Ошибка: группа слов не найдена.')
		return

	tasks = await get_stage2_tasks_pg(family_id)
	# Initialize or validate tasks_order
	total = len(tasks)
	order = st2.get('tasks_order')
	if (not order) or (len(order) != total) or (sorted(order) != list(range(total))):
		order = list(range(total))
		random.shuffle(order)
		st2['tasks_order'] = order
		context.user_data['stage2'] = st2
		await update_stage_state(context, 'stage2', family_id, 2, update.effective_user.id)
	idx_display = st2['task_idx']
	if not tasks:
		from tgteacher_bot.handlers.user.stage_3 import stage3_start
		await stage3_start(update, context)
		return

	# Определяем, является ли текущий этап последним доступным этапом (после 2-го нет задач)
	from tgteacher_bot.db.families_repo import get_stage3_tasks_pg, get_stage4_tasks_pg
	from tgteacher_bot.db.families_repo import get_stage5_tasks_pg, get_stage6_tasks_pg
	from tgteacher_bot.db.families_repo import get_stage7_tasks_pg, get_stage8_tasks_pg
	has_next = False
	for fetch in (get_stage3_tasks_pg, get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
		next_tasks = await fetch(family_id)
		if next_tasks:
			has_next = True
			break
	is_last_stage = not has_next

	if idx_display >= len(tasks):
		# logger.warning(f"Invalid task_idx {idx} for family {st2['family_idx']}, stage 2. Resetting to 0.")
		st2['task_idx'] = 0
		idx_display = 0
		context.user_data['stage2'] = st2
	
	orig_idx = order[idx_display]
	task = tasks[orig_idx]
	user_id = update.effective_user.id
	
	correct_answer = task['answer']

	if 'choices' not in task or 'answer' not in task:
		# logger.error(f"Неверный формат задания в группе слов {st2['family_idx']}, этап 2, задание {idx}. Отсутствуют 'choices' или 'answer'.")
		await update.callback_query.edit_message_text(
			"Ошибка в данных задания. Пожалуйста, сообщите администратору.",
			reply_markup=OK_MENU
		)
		return

	all_choices = [c for c in task['choices'] if c != correct_answer]

	previous_answer = await get_stage2_answer_pg(user_id, st2['family_idx'], orig_idx)
	# logger.info(f"[stage2] previous_answer из базы: {previous_answer}")
	# --- PATCH: сохраняем и восстанавливаем displayed_choices ---
	if 'displayed_choices' in st2 and st2['displayed_choices']:
		displayed_choices = st2['displayed_choices']
	else:
		all_choices = [c for c in task['choices'] if c != correct_answer]
		random_distractors = random.sample(all_choices, min(3, len(all_choices)))
		displayed_choices = [correct_answer] + random_distractors
		random.shuffle(displayed_choices)
		st2['displayed_choices'] = displayed_choices
		context.user_data['stage2'] = st2
		await update_stage_state(context, 'stage2', family_id, 2, user_id)
	# ГАРАНТИЯ: правильный ответ всегда в displayed_choices
	if correct_answer not in displayed_choices:
		all_choices = [c for c in task['choices'] if c != correct_answer]
		# logger.warning(f"[stage2] correct_answer '{correct_answer}' not in displayed_choices for family {family_id}, task {idx}. Перегенерирую варианты.")
		random_distractors = random.sample(all_choices, min(3, len(all_choices)))
		displayed_choices = [correct_answer] + random_distractors
		random.shuffle(displayed_choices)
		st2['displayed_choices'] = displayed_choices
		context.user_data['stage2'] = st2
		await update_stage_state(context, 'stage2', family_id, 2, user_id)
	# сохраняем порядок
	st2['choices_order'] = list(range(len(displayed_choices)))
	context.user_data['stage2'] = st2

	order_choices = st2['choices_order']
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
		except ValueError as e:
			# logger.warning(...)
			pass
	else:
		for i, orig_idx_c in enumerate(order_choices):
			choices.append((displayed_choices[orig_idx_c], False, False))

	await set_last_opened_family_place_pg(user_id, st2['family_idx'], 2, idx_display)

	answered_idx = answered_idx if previous_answer else None
	correct_idx = correct_idx if previous_answer else None
	feedback_text = ''
	if previous_answer:
		selected_text, is_correct = previous_answer
		if is_correct:
			feedback_text = f"\n\n✅ Верно! {task.get('explanation', '')}"
		else:
			feedback_text = f"\n\n❌ Неправильно! Правильный ответ: <b>{task['answer']}</b>\n{task.get('explanation', '')}"

	# logger.info(...)
	await update.callback_query.edit_message_text(
		get_stage2_text(task, idx_display, len(tasks)) + feedback_text,
		reply_markup=get_stage2_keyboard(choices, answered_idx=answered_idx, correct_idx=correct_idx, task_idx=idx_display, total_tasks=len(tasks), is_final_finish=is_last_stage),
		parse_mode='HTML'
	)

@track_metrics
async def stage2_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
	query = update.callback_query
	# logger.info(...)
	await query.answer()
	
	user_id = update.effective_user.id
	await mark_user_active_if_needed(user_id, context)
	
	success, st2 = await ensure_stage_state(update, context, 'stage2', 2, get_default_stage2_state)
	# logger.info(...)
	if not success:
		return
	
	await show_stage2_task(update, context)

@track_metrics
async def stage2_answer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
	query = update.callback_query
	# logger.info(...)
	success, st2 = await ensure_stage_state(update, context, 'stage2', 2, get_default_stage2_state)
	# logger.info(...)
	if not success:
		await query.answer()
		return
	
	family_id = st2['family_idx']
	family_meta = await get_family_data_pg(family_id)

	if not family_meta:
		await query.edit_message_text('Ошибка: группа слов не найдена.')
		return

	tasks = await get_stage2_tasks_pg(family_id)
	# Ensure tasks_order
	order = st2.get('tasks_order') or list(range(len(tasks)))
	if (not order) or (len(order) != len(tasks)) or (sorted(order) != list(range(len(tasks)))):
		order = list(range(len(tasks)))
		random.shuffle(order)
		st2['tasks_order'] = order
		context.user_data['stage2'] = st2
		await update_stage_state(context, 'stage2', family_id, 2, update.effective_user.id)

	task_idx_display = st2['task_idx']
	task = tasks[order[task_idx_display]]
	displayed_choices = st2.get('displayed_choices')
	order_choices = st2.get('choices_order')
	if not displayed_choices or not order_choices:
		await query.edit_message_text('Ошибка: порядок вариантов потерян. Начни этап заново.', reply_markup=OK_MENU)
		return

	answer_idx = int(query.data.replace('stage2_answer_', ''))
	selected_orig_idx = order_choices[answer_idx]
	correct_answer = task['answer']
	correct_orig_idx = displayed_choices.index(correct_answer)
	selected_text = displayed_choices[selected_orig_idx]

	user_id = update.effective_user.id
	await mark_user_active_if_needed(user_id, context)
	is_correct = (selected_orig_idx == correct_orig_idx)
	orig_idx_for_db = order[task_idx_display]
	await set_stage2_answer_pg(user_id, family_id, orig_idx_for_db, selected_text, is_correct)
	
	feedback_text = ''
	if is_correct:
		await set_task_done_pg(user_id, family_id, 2, orig_idx_for_db)
		feedback_text = f'✅ Верно! {task.get("explanation", "")}'
	else:
		feedback_text = f'❌ Неправильно! Правильный ответ: <b>{task["answer"]}</b>\n{task.get("explanation", "")}'

	choices = []
	for i, orig_idx_c in enumerate(order_choices):
		choices.append((displayed_choices[orig_idx_c], (orig_idx_c == correct_orig_idx), (i == answer_idx)))
	
	st2['answered'] = True
	context.user_data['stage2'] = st2
	await update_stage_state(context, 'stage2', family_id, 2, user_id)
	
	previous_answer = await get_stage2_answer_pg(user_id, family_id, orig_idx_for_db)
	
	unique_marker = random.random()
	
	# Определяем, является ли текущий этап последним
	from tgteacher_bot.db.families_repo import get_stage3_tasks_pg, get_stage4_tasks_pg
	from tgteacher_bot.db.families_repo import get_stage5_tasks_pg, get_stage6_tasks_pg
	from tgteacher_bot.db.families_repo import get_stage7_tasks_pg, get_stage8_tasks_pg
	has_next = False
	for fetch in (get_stage3_tasks_pg, get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
		next_tasks = await fetch(family_id)
		if next_tasks:
			has_next = True
			break
	is_last_stage = not has_next

	await query.edit_message_text(
		get_stage2_text(task, task_idx_display, len(tasks)) + f"\n\n{feedback_text}" + f" <a href='tg://resolve?domain=null&start={unique_marker}'>&#8;</a>",
		reply_markup=get_stage2_keyboard(choices, answered_idx=answer_idx, correct_idx=correct_orig_idx, task_idx=task_idx_display, total_tasks=len(tasks), is_final_finish=is_last_stage),
		parse_mode='HTML'
	)

@track_metrics
async def stage2_next_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
	# logger.info(...)
	st2_before = context.user_data.get('stage2', {}).copy() if context.user_data.get('stage2') else None
	# logger.info(...)
	success, st2 = await ensure_stage_state(update, context, 'stage2', 2, get_default_stage2_state)
	# logger.info(...)
	if not success:
		return
	
	family_id = st2['family_idx']
	family_meta = await get_family_data_pg(family_id)

	if not family_meta:
		await update.callback_query.edit_message_text('Ошибка: группа слов не найдена.')
		return

	tasks = await get_stage2_tasks_pg(family_id)
	idx_display = st2['task_idx']
	if idx_display < len(tasks)-1:
		st2['task_idx'] += 1
		st2['choices_order'] = None
		st2['displayed_choices'] = None  # MCP PATCH
		st2['answered'] = False
		st2['last_feedback_message'] = ''
		context.user_data['stage2'] = st2
		user_id = update.effective_user.id
		await update_stage_state(context, 'stage2', family_id, 2, user_id)
		st2_after = context.user_data.get('stage2', {}).copy() if context.user_data.get('stage2') else None
		# logger.info(...)
		await show_stage2_task(update, context)
	else:
		await update.callback_query.edit_message_text('Этап завершён!', reply_markup=OK_MENU)
	# logger.info(...)
	user_id = update.effective_user.id
	await mark_user_active_if_needed(user_id, context)

@track_metrics
async def stage2_skip_confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
	query = update.callback_query
	# logger.info(...)
	try:
		await query.answer()
	except BadRequest as e:
		logger.warning(f"Failed to answer callback query in stage2_skip_confirm_callback: {e}")
		pass
	text = (
		'Ты уверен, что хочешь пропустить этот этап?\n'
		'Рекомендуем пройти все этапы для лучшего запоминания слов!'
	)
	keyboard = InlineKeyboardMarkup([
		[
			InlineKeyboardButton('✅ Да, пропустить', callback_data='stage2_skip'),
			InlineKeyboardButton('❌ Отмена', callback_data='stage2_cancel_skip')
		]
	])
	await query.edit_message_text(text, reply_markup=keyboard)
	user_id = update.effective_user.id
	await mark_user_active_if_needed(user_id, context)

@track_metrics
async def stage2_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
	query = update.callback_query
	# logger.info(...)
	await query.answer()
	context.user_data.pop('stage2', None)
	await stage3_start(update, context)
	user_id = update.effective_user.id
	await mark_user_active_if_needed(user_id, context)

@track_metrics
async def stage2_cancel_skip_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
	# logger.info(...)
	await show_stage2_task(update, context)
	user_id = update.effective_user.id
	await mark_user_active_if_needed(user_id, context)

@track_metrics
async def stage2_finish_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
	query = update.callback_query
	# logger.info(...)
	await query.answer()

	st2 = context.user_data.get('stage2')
	# logger.info(f"[stage2] st2 (finish): {st2}")
	if st2:
		user_id = update.effective_user.id
		family_id = st2['family_idx']
		await set_family_stage_done_pg(user_id, family_id, 2)
		# Проверяем, есть ли задачи на этапах 3–8
		from tgteacher_bot.db.families_repo import get_stage3_tasks_pg, get_stage4_tasks_pg
		from tgteacher_bot.db.families_repo import get_stage5_tasks_pg, get_stage6_tasks_pg
		from tgteacher_bot.db.families_repo import get_stage7_tasks_pg, get_stage8_tasks_pg
		has_next = False
		for fetch in (get_stage3_tasks_pg, get_stage4_tasks_pg, get_stage5_tasks_pg, get_stage6_tasks_pg, get_stage7_tasks_pg, get_stage8_tasks_pg):
			next_tasks = await fetch(family_id)
			if next_tasks:
				has_next = True
				break
		context.user_data.pop('stage2', None)
		if has_next:
			await stage3_start(update, context)
		else:
			# Завершаем семью сразу
			from tgteacher_bot.db.user_repo import set_family_finished_pg
			await set_family_finished_pg(user_id, family_id)
			await set_last_opened_family_place_pg(user_id, family_id, 8, 0)
			await update_stage_state(context, 'stage2', family_id, 2, user_id)
			family_meta = await get_family_data_pg(family_id)
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
async def stage2_prev_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
	query = update.callback_query
	# logger.info(...)
	# logger.info(f"[stage2] context.user_data['stage2'] (до ensure): {context.user_data.get('stage2')}")
	st2_before = context.user_data.get('stage2', {}).copy() if context.user_data.get('stage2') else None
	# logger.info(...)
	await query.answer()
	
	success, st2 = await ensure_stage_state(update, context, 'stage2', 2, get_default_stage2_state)
	# logger.info(...)
	if not success:
		return

	if st2['task_idx'] > 0:
		st2['task_idx'] -= 1
		st2['choices_order'] = None
		st2['displayed_choices'] = None  # MCP PATCH
		user_id = update.effective_user.id
		family_id = st2['family_idx']
		previous_answer = await get_stage2_answer_pg(user_id, family_id, st2['task_idx'])
		st2['answered'] = previous_answer is not None
		context.user_data['stage2'] = st2
		await update_stage_state(context, 'stage2', family_id, 2, user_id)
		st2_after = context.user_data.get('stage2', {}).copy() if context.user_data.get('stage2') else None
		# logger.info(...)
		await show_stage2_task(update, context)
	else:
		await query.answer('Это первое задание', show_alert=True)
	# logger.info(...)
	user_id = update.effective_user.id
	await mark_user_active_if_needed(user_id, context)

@track_metrics
async def stage2_no_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
	# logger.info(...)
	user_id = update.effective_user.id
	await mark_user_active_if_needed(user_id, context)
	await update.callback_query.answer()

@track_metrics
async def stage2_first_task_alert_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
	# logger.info(...)
	user_id = update.effective_user.id
	await mark_user_active_if_needed(user_id, context)
	await update.callback_query.answer('Это первое задание.', show_alert=False) 
"""Granular mutations of private drafts with optimistic concurrency.

Positions are zero-based and scoped to a stage and expected draft revision.
Existing published task IDs are never accepted from add/patch operations.
"""
from copy import deepcopy

from tgteacher_bot.db import drafts_repo as drafts, content_versions as versions
from tgteacher_bot.db.content_schema import STAGES, stage_key
from tgteacher_bot.utils.family_parser import parse_family_text
from tgteacher_bot.utils.family_renderer import render_family_txt


def _key(stage):
    if type(stage) is not int or stage not in STAGES:
        raise ValueError("Stage must be an integer from 1 to 8")
    return stage_key(stage)


def _position(index, size, *, insertion=False):
    if type(index) is not int or not 0 <= index < size + int(insertion):
        raise ValueError("Task index out of range for this draft revision")
    return index


def _fields(stage, patch):
    _key(stage)
    if not isinstance(patch, dict) or not patch or set(patch) - set(STAGES[stage][2]):
        raise ValueError("Use only editable fields of this stage; task_id cannot be supplied")


async def _mutate(draft_kind, draft_id, expected_revision, stage, operation):
    key = _key(stage)
    if draft_kind == "new":
        draft = await drafts.get_draft(draft_id)
        conflict = drafts.DraftConflict
    elif draft_kind == "edit":
        draft = await versions.get_edit_draft(draft_id)
        conflict = versions.VersionConflict
    else:
        raise ValueError("draft_kind must be new or edit")
    if draft["status"] != "draft" or draft["revision"] != expected_revision:
        raise conflict("Draft deleted, published, or revision changed")
    if draft_kind == "new":
        content = parse_family_text(draft["family_txt"])
    else:
        content = deepcopy(draft["content"])
    entries = content.setdefault(key, [])
    operation(entries)
    candidate = versions.clean_content(content)
    if draft_kind == "new":
        # The TXT format does not persist IDs or comments. Canonicalize only
        # after the complete proposed lesson has passed the shared validator.
        if any(item["task_id"] is not None for n in STAGES for item in candidate[stage_key(n)]):
            raise ValueError("New drafts cannot reference published task IDs")
        result = await drafts.update_draft(draft_id, expected_revision, render_family_txt(candidate))
    else:
        result = await versions.update_edit_draft(draft_id, expected_revision, candidate)
    # Both repository writes are compare-and-swap. No read/modify/write
    # can overwrite a competing update, publication, or deletion.
    return {"draft_id": draft_id, "draft_kind": draft_kind, "revision": result["revision"],
            "stage": stage, "task_count": len(candidate[key]),
            "task_refs": [{"task_index": i, "task_id": item["task_id"]}
                          for i, item in enumerate(candidate[key])]}


async def add_tasks(draft_kind, draft_id, expected_revision, stage, tasks, insert_at=None):
    _key(stage)
    if not isinstance(tasks, list) or not 1 <= len(tasks) <= 100:
        raise ValueError("Supply between 1 and 100 tasks per call")
    for task in tasks:
        _fields(stage, task)

    def operation(entries):
        index = len(entries) if insert_at is None else _position(insert_at, len(entries), insertion=True)
        entries[index:index] = deepcopy(tasks)

    return await _mutate(draft_kind, draft_id, expected_revision, stage, operation)


async def update_task(draft_kind, draft_id, expected_revision, stage, task_index, patch):
    _fields(stage, patch)

    def operation(entries):
        entries[_position(task_index, len(entries))].update(deepcopy(patch))

    return await _mutate(draft_kind, draft_id, expected_revision, stage, operation)


async def archive_task(draft_kind, draft_id, expected_revision, stage, task_index):
    def operation(entries):
        entries.pop(_position(task_index, len(entries)))

    return await _mutate(draft_kind, draft_id, expected_revision, stage, operation)


async def reorder_tasks(draft_kind, draft_id, expected_revision, stage, order):
    def operation(entries):
        if (not isinstance(order, list) or any(type(i) is not int for i in order)
                or sorted(order) != list(range(len(entries)))):
            raise ValueError("order must be a complete permutation of current zero-based task indices")
        entries[:] = [entries[i] for i in order]

    return await _mutate(draft_kind, draft_id, expected_revision, stage, operation)

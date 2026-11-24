"""Render current version back to the legacy Telegram ZIP interchange format."""
from tgteacher_bot.db.content_schema import STAGES, stage_key

KEYS = {
    1: ("word", "translation", "example", "example_translation", "hint"),
    2: ("sentence", "answer", "choices", "explanation"),
    3: ("word", "definition", "choices", "explanation"),
    4: ("sentence", "answer", "choices", "explanation"),
    5: ("image", "answer", "alternatives", "explanation"),
    6: ("word", "synonyms", "wrong_synonyms", "explanation"),
    7: ("task", "audio", "answer", "choices", "explanation"),
}


def render_family_txt(content):
    lines = [f'#FAMILY "{content["name"]}"', f'#TARGET {content.get("target") or "VIP+FREE"}']
    if content.get("description"):
        lines.append("#DESCRIPTION " + content["description"])
    for stage in STAGES:
        entries = content.get(stage_key(stage), [])
        if not entries:
            continue
        if stage not in (1, 8):
            lines.append(f"#STAGE{stage}")
        for item in entries:
            if stage == 8:
                lines.extend(["#STAGE8", "#TEXT", *item["text"].split("\\n")])
                lines.extend(f"#ANSWER{i} {a}" for i,a in enumerate(item["answers"], 1))
                lines.extend(f"#EXPLANATION{i} {a}" for i,a in enumerate(item.get("explanations") or [], 1))
                if item.get("equal"):
                    lines.append("#EQUAL " + item["equal"])
                continue
            if stage in (2, 4, 5, 7):
                lines.append("#TASK" + (" " + item["task"] if stage == 7 and item.get("task") else ""))
            for key in KEYS[stage]:
                if key == "task":
                    continue
                value = item.get(key)
                if not value:
                    continue
                if isinstance(value, list):
                    delimiter = ", " if key in ("synonyms", "wrong_synonyms") else "; "
                    value = delimiter.join(value)
                lines.append(f"#{key.upper()} {value}")
    return "\n".join(lines) + "\n"

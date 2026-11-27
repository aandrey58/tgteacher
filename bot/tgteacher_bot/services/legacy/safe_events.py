"""Privacy-preserving warning/error log; never store message, args or traceback."""
import asyncio
import json
import logging
import os
import stat
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from tgteacher_bot.core.paths import admins_path

MAX_BYTES = 256 * 1024
CATEGORIES = {"database", "telegram", "http", "bot", "other"}
LEVELS = {"warning", "error", "critical"}


def event_path():
    return admins_path().parent / "safe_events.jsonl"


class SafeFormatter(logging.Formatter):
    def format(self, record):
        name = record.name
        category = ("database" if name.startswith(("asyncpg", "tgteacher_bot.db"))
                    else "telegram" if name.startswith("telegram")
                    else "http" if name.startswith(("httpx", "httpcore"))
                    else "bot" if name.startswith("tgteacher_bot") else "other")
        level = "critical" if record.levelno >= logging.CRITICAL else "error" if record.levelno >= logging.ERROR else "warning"
        return json.dumps({"time": datetime.fromtimestamp(record.created,timezone.utc).isoformat(),
                           "level":level,"category":category}, separators=(",",":"))


def install():
    root = logging.getLogger()
    if any(getattr(h,"_tgteacher_safe_events",False) for h in root.handlers):
        return
    handler = RotatingFileHandler(event_path(),maxBytes=MAX_BYTES,backupCount=1,encoding="utf-8",delay=True)
    handler.setLevel(logging.WARNING)
    handler.setFormatter(SafeFormatter())
    handler._tgteacher_safe_events=True
    root.addHandler(handler)


def read_events(path):
    try:
        if path.is_symlink():
            return []
        fd=os.open(path,os.O_RDONLY | getattr(os,"O_NOFOLLOW",0) | getattr(os,"O_NONBLOCK",0))
    except OSError:
        return []
    with os.fdopen(fd,"rb") as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode):
            return []
        stream.seek(max(0,info.st_size-MAX_BYTES))
        raw=stream.read(MAX_BYTES)
    items=[]
    for line in raw.splitlines():
        if len(line)>256:
            continue
        try:
            entry=json.loads(line)
            if not isinstance(entry,dict) or set(entry)!={"time","level","category"}:
                continue
            if entry["level"] not in LEVELS or entry["category"] not in CATEGORIES:
                continue
            stamp=datetime.fromisoformat(entry["time"])
            if stamp.utcoffset() is None:
                continue
            items.append({"time":stamp.astimezone(timezone.utc).isoformat(),"level":entry["level"],"category":entry["category"]})
        except (ValueError,TypeError,OverflowError):
            continue
    return list(reversed(items[-100:]))


async def recent():
    return {"items":await asyncio.to_thread(read_events,event_path())}

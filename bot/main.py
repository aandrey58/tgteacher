"""
TGTeacher entrypoint.

Run:
    python bot/main.py
"""

from tgteacher_bot.core.app import run_polling


if __name__ == "__main__":
    run_polling()


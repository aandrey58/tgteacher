"""Fail-closed authorization at every registered administrative handler."""
from functools import wraps
from telegram.ext import CallbackQueryHandler, MessageHandler, ApplicationHandlerStop


def is_admin(update):
    from tgteacher_bot.handlers.admin.admin_list import get_admin_ids
    user, chat = update.effective_user, update.effective_chat
    return bool(user and chat and chat.type == "private" and chat.id == user.id and str(user.id) in get_admin_ids())


def guard(callback, callback_query):
    @wraps(callback)
    async def checked(update, context):
        if not is_admin(update):
            if callback_query:
                await update.callback_query.answer("Недостаточно прав.", show_alert=True)
                raise ApplicationHandlerStop
            return
        return await callback(update, context)
    return checked


class AdminRegistration:
    """Only used when registering admin modules, including nested registrations."""
    def __init__(self, app):
        self.app = app

    def add_handler(self, handler, group=0):
        if isinstance(handler, CallbackQueryHandler):
            # noop is shared with harmless pagination in learner menus.
            pattern = getattr(handler.pattern,"pattern",handler.pattern)
            if pattern != "^noop$":
                handler.callback = guard(handler.callback, True)
                handler.block = True
        elif isinstance(handler, MessageHandler):
            handler.callback = guard(handler.callback, False)
        self.app.add_handler(handler,group=group)

    def __getattr__(self,name):
        return getattr(self.app,name)

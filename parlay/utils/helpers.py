import logging
from telegram.error import BadRequest

logger = logging.getLogger(__name__)


async def safe_edit(query, text, **kwargs):
    """Edit a message via CallbackQuery, silently ignoring 'Message is not modified'."""
    try:
        await query.edit_message_text(text, **kwargs)
    except BadRequest as e:
        if "Message is not modified" in str(e):
            pass  # Content identical — ignore
        else:
            raise

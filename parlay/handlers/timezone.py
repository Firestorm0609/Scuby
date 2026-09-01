"""
Timezone handler — lets users set their UTC offset so fixture times display correctly.
"""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from sqlalchemy import select
from database.db import SessionLocal, User
from utils.helpers import safe_edit


# Common timezone offsets with labels
TIMEZONE_OPTIONS = [
    (-12, "UTC-12"), (-11, "UTC-11"), (-10, "UTC-10 (Hawaii)"),
    (-9, "UTC-9 (Alaska)"), (-8, "UTC-8 (Pacific)"), (-7, "UTC-7 (Mountain)"),
    (-6, "UTC-6 (Central)"), (-5, "UTC-5 (Eastern)"), (-4, "UTC-4 (Atlantic)"),
    (-3, "UTC-3 (Buenos Aires)"), (-2, "UTC-2"), (-1, "UTC-1"),
    (0, "UTC+0 (London)"), (1, "UTC+1 (Paris/West Africa)"), (2, "UTC+2 (Cairo/South Africa)"),
    (3, "UTC+3 (Moscow)"), (4, "UTC+4 (Dubai)"), (5, "UTC+5 (Karachi)"),
    (6, "UTC+6 (Dhaka)"), (7, "UTC+7 (Bangkok)"), (8, "UTC+8 (Singapore/HK)"),
    (9, "UTC+9 (Tokyo)"), (10, "UTC+10 (Sydney)"), (11, "UTC+11"),
    (12, "UTC+12 (Auckland)"),
]


def format_time_with_offset(utc_str: str, offset: int) -> str:
    """Convert an ISO UTC time string to local time string with offset.
    Returns formatted string like 'Aug 23, 15:30'."""
    from datetime import datetime, timedelta, timezone
    try:
        dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        local_dt = dt + timedelta(hours=offset)
        sign = "+" if offset >= 0 else ""
        return local_dt.strftime(f"%b %d, %H:%M (UTC{sign}{offset})")
    except (ValueError, TypeError):
        return utc_str[:16]


async def timezone_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show timezone selection menu."""
    q = update.callback_query
    await q.answer()

    user_id = update.effective_user.id
    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()

    current_tz = getattr(user, "tz_offset", 0) if user else 0

    text = (
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🌍 *Select Your Timezone*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Current: *UTC{'+' if current_tz >= 0 else ''}{current_tz}*\n\n"
        f"_Fixture times will display in your local timezone._"
    )

    # Build buttons in rows of 3
    kb = []
    row = []
    for offset, label in TIMEZONE_OPTIONS:
        check = " ✓" if offset == current_tz else ""
        row.append(InlineKeyboardButton(f"{label}{check}", callback_data=f"tz_set_{offset}"))
        if len(row) == 3:
            kb.append(row)
            row = []
    if row:
        kb.append(row)

    kb.append([InlineKeyboardButton("◀️ Back", callback_data="menu_sports")])

    await safe_edit(q, text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))


async def timezone_set_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set user's timezone offset."""
    q = update.callback_query
    offset = int(q.data[len("tz_set_"):])

    user_id = update.effective_user.id
    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()
        if user:
            user.tz_offset = offset
            await s.commit()

    sign = "+" if offset >= 0 else ""
    await q.answer(f"🌍 Timezone set to UTC{sign}{offset}!")

    # Go back to sports menu
    from handlers.sports import sports_menu
    await sports_menu(update, context)

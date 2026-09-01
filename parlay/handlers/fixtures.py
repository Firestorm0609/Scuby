"""
Fixtures handler — shows today's available games with pagination.
"""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime, timedelta
from services.espn_api import ESPNClient
from config import LEAGUES
from handlers.sports import get_user_sports
from database.db import SessionLocal, User, select
from utils.helpers import safe_edit

SPORT_EMOJI = {
    "soccer": "⚽", "basketball": "🏀", "football": "🏈",
    "baseball": "⚾", "hockey": "🏒", "rugby": "🏉", "cricket": "🏏",
}

FIXES_PER_PAGE = 10


async def fixtures_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show today's fixtures with pagination."""
    q = update.callback_query
    await q.answer()

    # Parse page from callback data
    page = 0
    if q.data.startswith("fixtures_page_"):
        try:
            page = int(q.data[len("fixtures_page_"):])
        except ValueError:
            page = 0

    # Cache fixtures in user_data to avoid re-fetching on page turns
    cache_key = "fixtures_cache"
    cache_date_key = "fixtures_date"
    today = datetime.utcnow().strftime("%Y%m%d")

    cached = context.user_data.get(cache_key)
    cached_date = context.user_data.get(cache_date_key)

    # Always fetch user for timezone
    async with SessionLocal() as s:
        res = await s.execute(
            select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()

    if cached and cached_date == today:
        by_sport = cached
    else:
        # Fetch fresh
        user_sports = get_user_sports(user) if user else None

        client = ESPNClient()
        try:
            leagues_to_fetch = [
                lg for lg in LEAGUES.keys()
                if not user_sports or lg.split("/")[0] in user_sports
            ]
            raw = await client.fetch_all_leagues(today, leagues=leagues_to_fetch)

            by_sport = {}
            for league_code, data in raw.items():
                if isinstance(data, Exception) or not data:
                    continue
                sport = league_code.split("/")[0] if "/" in league_code else "soccer"
                fixtures = ESPNClient.parse_events(data, league_code)
                upcoming = [f for f in fixtures if f["status"] == "pre"]
                if upcoming:
                    if sport not in by_sport:
                        by_sport[sport] = []
                    by_sport[sport].extend(upcoming)

            # Cache for page turns
            context.user_data[cache_key] = by_sport
            context.user_data[cache_date_key] = today
        finally:
            await client.close()

    if not by_sport:
        kb = [
            [InlineKeyboardButton("🔁 Refresh", callback_data="menu_fixtures")],
            [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
        ]
        try:
            await q.edit_message_text(
                "📅 *No upcoming fixtures found.*\n\nCheck back later!",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb))
        except Exception:
            pass
        return

    # Flatten all fixtures into a single list with sport labels
    sport_order = ["soccer", "basketball", "football", "baseball", "hockey", "rugby", "cricket"]
    all_fixtures = []
    for sport in sport_order:
        if sport in by_sport:
            for fx in by_sport[sport]:
                all_fixtures.append((sport, fx))

    total = len(all_fixtures)
    total_pages = (total - 1) // FIXES_PER_PAGE if total > 0 else 1
    page = min(page, total_pages)
    start = page * FIXES_PER_PAGE
    end = start + FIXES_PER_PAGE
    page_fixtures = all_fixtures[start:end]

    # Get user timezone offset
    user_tz = 0
    if user:
        user_tz = getattr(user, "tz_offset", 0)

    # Build message
    date_str = datetime.utcnow().strftime("%b %d, %Y")
    sign = "+" if user_tz >= 0 else ""
    lines = [f"📅 *Fixtures — {date_str}*\n"]
    kb = []

    current_sport = None
    for sport, fx in page_fixtures:
        if sport != current_sport:
            emoji = SPORT_EMOJI.get(sport, "🏆")
            lines.append(f"\n{emoji} *{sport.title()}*")
            current_sport = sport

        dt = datetime.fromisoformat(fx["date"].replace("Z", "+00:00"))
        local_dt = dt + timedelta(hours=user_tz)
        time_str = local_dt.strftime("%H:%M")

        odds_str = ""
        if fx.get("odds"):
            o = fx["odds"]
            h = o.get("home_ml")
            a = o.get("away_ml")
            if h and a:
                odds_str = f"  `{h} / {a}`"

        lines.append(
            f"  ⏰ `{time_str}`  {fx['home_team']} vs {fx['away_team']}{odds_str}"
        )

    lines.append(f"\n📄 Page {page + 1}/{total_pages} · {total} matches")
    text = "\n".join(lines)

    # Navigation buttons
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"fixtures_page_{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"fixtures_page_{page + 1}"))
    if len(nav) > 1:
        kb.append(nav)

    kb.append([InlineKeyboardButton("🔁 Refresh", callback_data="menu_fixtures")])
    kb.append([InlineKeyboardButton("🏠 Back", callback_data="menu_main")])

    try:
        await q.edit_message_text(text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb))
    except Exception:
        pass  # Message not modified — ignore

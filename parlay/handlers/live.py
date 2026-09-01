"""
Live features — match viewer and live score display.
"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime, timezone, timedelta
from sqlalchemy import select
from services.espn_api import ESPNClient
from database.db import SessionLocal, User
from config import LEAGUES
from handlers.sports import get_user_sports
from utils.helpers import safe_edit

logger = logging.getLogger(__name__)

SPORT_EMOJI = {
    "soccer": "⚽", "basketball": "🏀", "football": "🏈",
    "baseball": "⚾", "hockey": "🏒", "rugby": "🏉", "cricket": "🏏",
}

PERIOD_NAMES = {
    "soccer": {1: "1st Half", 2: "2nd Half"},
    "basketball": {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4", 5: "OT"},
    "football": {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4", 5: "OT"},
    "baseball": {1: "Top 1st", 2: "Bot 1st", 3: "Top 2nd", 4: "Bot 2nd",
                 5: "Top 3rd", 6: "Bot 3rd", 7: "Top 4th", 8: "Bot 4th",
                 9: "Top 5th", 10: "Bot 5th", 11: "Top 6th", 12: "Bot 6th",
                 13: "Top 7th", 14: "Bot 7th", 15: "Top 8th", 16: "Bot 8th",
                 17: "Top 9th", 18: "Bot 9th", 19: "OT"},
    "hockey": {1: "1st Period", 2: "2nd Period", 3: "3rd Period", 4: "OT", 5: "SO"},
    "rugby": {1: "1st Half", 2: "2nd Half"},
    "cricket": {1: "Inns 1", 2: "Inns 2", 3: "Inns 3", 4: "Inns 4"},
}


def _period_label(sport: str, period: int, clock: str = "", status_detail: str = "") -> str:
    """Human-readable period + clock string."""
    detail_lower = status_detail.lower() if status_detail else ""
    if "halftime" in detail_lower or ("half" in detail_lower and "end" in detail_lower):
        return "⏸ HT"
    if "full" in detail_lower or "final" in detail_lower:
        return "✅ FT"
    if "end of" in detail_lower and "overtime" not in detail_lower:
        return "End of Period"
    if "overtime" in detail_lower or detail_lower.strip() == "ot":
        return "⏱ OT"

    names = PERIOD_NAMES.get(sport, {})
    label = names.get(period, f"P{period}")
    if clock and clock.strip() and clock.strip() != "0:00":
        return f"🔴 {label} · {clock.strip()}"
    if clock and clock.strip() == "0:00":
        return f"⏸ {label} · Break"
    return f"🔴 {label}"


# ─── Live Menu ──────────────────────────────────────────────────────

async def live_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main live menu — goes straight to live matches."""
    q = update.callback_query
    # Don't answer here — live_matches_handler will answer
    await live_matches_handler(update, context)


# ─── Live Matches Viewer ───────────────────────────────────────────

async def live_matches_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show all live games across sports."""
    q = update.callback_query
    await q.answer("🔴 Loading live games...")

    # Get user's preferred sports
    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()
    user_sports = get_user_sports(user) if user else None

    client = ESPNClient()
    try:
        leagues_to_fetch = [
            lg for lg in LEAGUES.keys()
            if not user_sports or lg.split("/")[0] in user_sports
        ]

        et = timezone(timedelta(hours=-4))
        et_date = datetime.now(et).strftime("%Y%m%d")
        utc_date = datetime.utcnow().strftime("%Y%m%d")

        live_by_sport = {}
        all_statuses = {}
        dates_to_try = [None, et_date, utc_date]

        for date_param in dates_to_try:
            if live_by_sport:
                break
            raw = await client.fetch_all_leagues(date=date_param, leagues=leagues_to_fetch)
            for league_code, data in raw.items():
                if isinstance(data, Exception) or not data:
                    continue
                sport = league_code.split("/")[0] if "/" in league_code else "soccer"
                fixtures = ESPNClient.parse_events(data, league_code)
                for fx in fixtures:
                    st = fx["status"]
                    all_statuses[st] = all_statuses.get(st, 0) + 1
                live = [f for f in fixtures if f["status"] == "in"]
                if live:
                    if sport not in live_by_sport:
                        live_by_sport[sport] = []
                    live_by_sport[sport].extend(live)

        logger.info(f"Live check: statuses={all_statuses}, live={sum(len(v) for v in live_by_sport.values())}")
    finally:
        await client.close()

    if not live_by_sport:
        kb = [
            [InlineKeyboardButton("🔁 Refresh", callback_data="live_matches")],
            [InlineKeyboardButton("◀️ Back", callback_data="menu_main")],
        ]
        await safe_edit(q,
            "🔴 *No live games right now.*\n\n_Come back during peak hours!_",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return

    # Build display
    sport_order = ["soccer", "basketball", "football", "baseball", "hockey", "rugby", "cricket"]
    total_live = 0
    lines = ["🔴 *Live Matches*\n"]

    for sport in sport_order:
        if sport not in live_by_sport:
            continue
        games = live_by_sport[sport]
        total_live += len(games)
        emoji = SPORT_EMOJI.get(sport, "🏆")
        lines.append(f"\n{emoji} *{sport.title()}*")

        for fx in sorted(games, key=lambda x: x.get("period", 0)):
            h_score = fx["home_score"]
            a_score = fx["away_score"]
            period_label = _period_label(sport, fx.get("period", 0), fx.get("clock", ""), fx.get("status_detail", ""))
            h_name = fx["home_team"][:22]
            a_name = fx["away_team"][:22]
            lines.append(
                f"  {h_name}  *{h_score}* — *{a_score}*  {a_name}\n"
                f"  {period_label}"
            )

    lines.insert(1, f"_{total_live} game{'s' if total_live != 1 else ''} in progress_\n")
    text = "\n".join(lines)

    kb = [
        [InlineKeyboardButton("🔄 Refresh", callback_data="live_matches")],
        [InlineKeyboardButton("◀️ Back", callback_data="menu_main")],
    ]
    await safe_edit(q, text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))

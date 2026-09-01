"""
Sport selector — choose which sports/leagues to include.
"""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from sqlalchemy import select
from database.db import SessionLocal, User
from config import LEAGUES, SPORT_MARKETS, DEFAULT_MARKET_PREFS
from utils.helpers import safe_edit
import json

SPORT_CATEGORIES = {
    "soccer":     "⚽ Soccer",
    "basketball": "🏀 Basketball",
    "football":   "🏈 Football",
    "baseball":   "⚾ Baseball",
    "hockey":     "🏒 Hockey",
    "rugby":      "🏉 Rugby",
    "cricket":    "🏏 Cricket",
}

SPORT_EMOJI = {
    "soccer": "⚽", "basketball": "🏀", "football": "🏈",
    "baseball": "⚾", "hockey": "🏒", "rugby": "🏉", "cricket": "🏏",
}


def get_user_sports(user: User) -> set:
    if user.preferred_sports is None or user.preferred_sports == "all":
        return set(SPORT_CATEGORIES.keys())
    if not user.preferred_sports:
        return set()
    return set(user.preferred_sports.split(","))


def get_user_markets(user: User) -> dict:
    if user.market_prefs is None or user.market_prefs == "all":
        return dict(DEFAULT_MARKET_PREFS)
    if not user.market_prefs:
        return {}
    try:
        prefs = json.loads(user.market_prefs)
        for sport in SPORT_MARKETS:
            if sport not in prefs:
                prefs[sport] = list(SPORT_MARKETS[sport].keys())
        return prefs
    except (json.JSONDecodeError, TypeError):
        return dict(DEFAULT_MARKET_PREFS)


def count_enabled_markets(market_list):
    return len(market_list)


async def sports_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with SessionLocal() as s:
        res = await s.execute(
            select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()

    if not user:
        kb = [[InlineKeyboardButton("🏠 Back", callback_data="menu_main")]]
        msg = update.message or update.callback_query.message
        await msg.reply_text("⚠️ Please /start first.", reply_markup=InlineKeyboardMarkup(kb))
        return

    selected = get_user_sports(user)
    market_prefs = get_user_markets(user)

    buttons = []
    for sport, label in SPORT_CATEGORIES.items():
        check = "✅ " if sport in selected else ""
        mc = count_enabled_markets(market_prefs.get(sport, []))
        tm = len(SPORT_MARKETS.get(sport, {}))
        buttons.append([
            InlineKeyboardButton(f"{check}{label}", callback_data=f"sport_toggle_{sport}"),
            InlineKeyboardButton(f"Markets: {mc}/{tm}", callback_data=f"sport_config_{sport}"),
        ])

    buttons.append([
        InlineKeyboardButton("✅ All", callback_data="sport_set_all"),
        InlineKeyboardButton("❌ None", callback_data="sport_set_none"),
    ])
    buttons.append([InlineKeyboardButton("🌍 Timezone", callback_data="menu_timezone")])
    buttons.append([InlineKeyboardButton("🏠 Back", callback_data="menu_main")])

    tz_offset = getattr(user, "tz_offset", 0)
    sign = "+" if tz_offset >= 0 else ""
    text = (
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ *Settings*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Sports: *{len(selected)}* active\n"
        f"Timezone: *UTC{sign}{tz_offset}*\n\n"
        f"_Tap sport to toggle · Tap *Markets* to configure_"
    )

    markup = InlineKeyboardMarkup(buttons)
    if update.callback_query:
        await safe_edit(update.callback_query, text, parse_mode="Markdown", reply_markup=markup)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=markup)


async def sport_config_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, sport: str):
    async with SessionLocal() as s:
        res = await s.execute(
            select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()
    if not user:
        return

    market_prefs = get_user_markets(user)
    enabled = set(market_prefs.get(sport, []))

    sport_label = SPORT_CATEGORIES.get(sport, sport)
    sport_emoji = SPORT_EMOJI.get(sport, "🏆")
    markets = SPORT_MARKETS.get(sport, {})

    buttons = []
    for mkt_key, mkt_info in markets.items():
        check = "✅ " if mkt_key in enabled else ""
        buttons.append([
            InlineKeyboardButton(
                f"{check}{mkt_info['emoji']} {mkt_info['label']}",
                callback_data=f"sport_mkt_toggle_{sport}_{mkt_key}")
        ])

    buttons.append([
        InlineKeyboardButton("✅ All", callback_data=f"sport_mkt_all_{sport}"),
        InlineKeyboardButton("❌ None", callback_data=f"sport_mkt_none_{sport}"),
    ])
    buttons.append([InlineKeyboardButton("◀️ Back", callback_data="menu_sports")])

    text = (
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{sport_emoji} *{sport_label}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Enabled: *{len(enabled)}/{len(markets)}*\n\n"
        f"_Tap to toggle_"
    )

    markup = InlineKeyboardMarkup(buttons)
    await safe_edit(update.callback_query, text, parse_mode="Markdown", reply_markup=markup)


async def sport_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    sport = q.data[len("sport_toggle_"):]

    async with SessionLocal() as s:
        res = await s.execute(
            select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()
        if not user:
            return

        selected = get_user_sports(user)
        if sport in selected:
            selected.discard(sport)
        else:
            selected.add(sport)

        user.preferred_sports = ",".join(sorted(selected)) if selected != set(SPORT_CATEGORIES.keys()) else "all"
        await s.commit()

    await sports_menu(update, context)


async def sport_set_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    preset = q.data[len("sport_set_"):]

    # Only handle sport_set_all and sport_set_none
    if preset not in ("all", "none"):
        await q.answer()
        return

    await q.answer()

    async with SessionLocal() as s:
        res = await s.execute(
            select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()
        if not user:
            return

        user.preferred_sports = "all" if preset == "all" else ""
        await s.commit()

    try:
        await sports_menu(update, context)
    except Exception:
        pass


async def sport_config_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    sport = q.data[len("sport_config_"):]

    # Guard: sport_config_ could match sport_config_ with market suffix if pattern is loose
    # But our callback_data is always sport_config_{sport} and sport is a single word
    if "_" in sport:
        return

    await sport_config_menu(update, context, sport)


async def sport_mkt_toggle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    prefix = "sport_mkt_toggle_"
    remainder = q.data[len(prefix):]
    parts = remainder.split("_", 1)
    if len(parts) != 2:
        return
    sport, market = parts

    async with SessionLocal() as s:
        res = await s.execute(
            select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()
        if not user:
            return

        market_prefs = get_user_markets(user)
        enabled = set(market_prefs.get(sport, []))

        if market in enabled:
            enabled.discard(market)
        else:
            enabled.add(market)

        market_prefs[sport] = sorted(enabled)
        user.market_prefs = json.dumps(market_prefs)
        await s.commit()

    try:
        await sport_config_menu(update, context, sport)
    except Exception:
        pass


async def sport_mkt_all_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    sport = q.data[len("sport_mkt_all_"):]

    async with SessionLocal() as s:
        res = await s.execute(
            select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()
        if not user:
            return

        market_prefs = get_user_markets(user)
        market_prefs[sport] = list(SPORT_MARKETS.get(sport, {}).keys())
        user.market_prefs = json.dumps(market_prefs)
        await s.commit()

    try:
        await sport_config_menu(update, context, sport)
    except Exception:
        pass


async def sport_mkt_none_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    sport = q.data[len("sport_mkt_none_"):]

    async with SessionLocal() as s:
        res = await s.execute(
            select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()
        if not user:
            return

        market_prefs = get_user_markets(user)
        market_prefs[sport] = []
        user.market_prefs = json.dumps(market_prefs)
        await s.commit()

    try:
        await sport_config_menu(update, context, sport)
    except Exception:
        pass

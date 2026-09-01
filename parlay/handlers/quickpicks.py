"""
Quick Picks — Parlay builder by target odds.

Flow:
1. User clicks Quick Picks → "What target odds parlay do you want?"
2. User types e.g. 10 → bot shows pick ranges
3. User selects range (or custom odds like 1.5) → bot builds parlay
"""
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime, timedelta, timezone
from math import log
from sqlalchemy import select
from services.espn_api import ESPNClient
from services.analytics import analyze_fixture, correlation_penalty
from database.db import SessionLocal, User
from config import LEAGUES
from handlers.sports import get_user_sports
from utils.helpers import safe_edit

logger = logging.getLogger(__name__)

SPORT_EMOJI = {
    "soccer": "⚽", "basketball": "🏀", "football": "🏈",
    "baseball": "⚾", "hockey": "🏒", "rugby": "🏉", "cricket": "🏏",
}


def _calc_needed_legs(target_odds: float, avg_leg_odds: float) -> int:
    """How many legs at avg_leg_odds to reach target_odds."""
    if avg_leg_odds <= 1.0:
        return 99
    return max(1, int(round(log(target_odds) / log(avg_leg_odds))))


# ─── Handlers ──────────────────────────────────────────────────────

async def quick_picks_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Step 1: Ask user for target parlay odds."""
    q = update.callback_query
    await q.answer()

    context.user_data["qp_target"] = None
    context.user_data["awaiting_qp_target"] = True

    text = (
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ *Quick Picks*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "What *target odds parlay* do you want?\n\n"
        "Type a number — e.g. `2`, `5`, `10`, `20`\n"
        "_I'll build the safest parlay to hit that target._"
    )
    kb = [
        [
            InlineKeyboardButton("2×", callback_data="qp_target_2"),
            InlineKeyboardButton("5×", callback_data="qp_target_5"),
            InlineKeyboardButton("10×", callback_data="qp_target_10"),
        ],
        [
            InlineKeyboardButton("15×", callback_data="qp_target_15"),
            InlineKeyboardButton("20×", callback_data="qp_target_20"),
            InlineKeyboardButton("50×", callback_data="qp_target_50"),
        ],
        [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
    ]
    await safe_edit(q, text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))


async def quick_picks_target_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle preset target odds button."""
    q = update.callback_query
    target = int(q.data[len("qp_target_"):])
    context.user_data["qp_target"] = target
    context.user_data["awaiting_qp_target"] = False
    await _show_range_menu(q, context, target)


async def handle_qp_target_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle typed target odds. Returns True if consumed."""
    if not context.user_data.get("awaiting_qp_target"):
        return False

    context.user_data["awaiting_qp_target"] = False
    raw = update.message.text.strip().replace("×", "").replace("x", "").replace("X", "")

    try:
        target = float(raw)
        if target < 1.1 or target > 500:
            raise ValueError
    except ValueError:
        kb = [[InlineKeyboardButton("❌ Cancel", callback_data="menu_quickpicks")]]
        await update.message.reply_text(
            "❌ Enter a number between 1.1 and 500.\n_e.g. 10 for a 10× parlay_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb))
        return True

    context.user_data["qp_target"] = target
    text = _build_range_text(target)
    kb = _build_range_keyboard(target)
    await update.message.reply_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))
    return True


def _build_range_text(target: float) -> str:
    return (
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ *Target: ×{target:.1f}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Pick a range for each leg:\n"
        f"_Lower odds = more legs · Higher odds = fewer legs_\n\n"
        f"Or type a custom number like `1.5`"
    )


def _build_range_keyboard(target: float) -> list:
    return [
        [InlineKeyboardButton("🟢 1.1 – 1.5  Safe, many legs", callback_data="qp_range_1.1_1.5")],
        [InlineKeyboardButton("🟢 1.1 – 2.0  Balanced", callback_data="qp_range_1.1_2.0")],
        [InlineKeyboardButton("🟡 1.5 – 3.0  Moderate", callback_data="qp_range_1.5_3.0")],
        [InlineKeyboardButton("🔴 2.0 – 3.0  Aggressive", callback_data="qp_range_2.0_3.0")],
        [InlineKeyboardButton("🔥 3.0 – 5.0  High risk", callback_data="qp_range_3.0_5.0")],
        [InlineKeyboardButton("✏️ Custom  Type your own odds", callback_data="qp_range_custom")],
        [InlineKeyboardButton("🏠 Back", callback_data="menu_quickpicks")],
    ]


async def _show_range_menu(q, context, target: float):
    text = _build_range_text(target)
    kb = _build_range_keyboard(target)
    await safe_edit(q, text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))


async def quick_picks_range_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle range selection or custom prompt."""
    q = update.callback_query
    data = q.data
    target = context.user_data.get("qp_target", 10)

    if data == "qp_range_custom":
        await q.answer()
        context.user_data["awaiting_qp_custom_range"] = True
        text = (
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"✏️ *Custom Range*\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Type the odds you want each leg to be around.\n\n"
            f"`1.5` — builds ×{target:.1f} using ~1.5 odds picks\n"
            f"`2.0` — builds ×{target:.1f} using ~2.0 odds picks"
        )
        kb = [[InlineKeyboardButton("❌ Cancel", callback_data="menu_quickpicks")]]
        await safe_edit(q, text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb))
        return

    await q.answer("🔍 Building parlay...")

    parts = data[len("qp_range_"):].split("_")
    min_odds = float(parts[0])
    max_odds = float(parts[1])
    mid_odds = (min_odds + max_odds) / 2

    await _build_parlay(q, context, target, min_odds, max_odds, mid_odds)


async def handle_qp_custom_range_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle custom odds range typed input. Returns True if consumed."""
    if not context.user_data.get("awaiting_qp_custom_range"):
        return False

    context.user_data["awaiting_qp_custom_range"] = False
    raw = update.message.text.strip().replace("×", "").replace("x", "")

    try:
        custom_odds = float(raw)
        if custom_odds < 1.05 or custom_odds > 50:
            raise ValueError
    except ValueError:
        kb = [[InlineKeyboardButton("❌ Cancel", callback_data="menu_quickpicks")]]
        await update.message.reply_text(
            "❌ Enter odds between 1.05 and 50.\n_e.g. 1.5 or 2.0_",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb))
        return True

    target = context.user_data.get("qp_target", 10)
    min_odds = max(1.05, custom_odds - 0.3)
    max_odds = custom_odds + 0.3

    status_msg = await update.message.reply_text(
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ *Quick Picks*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"_Building ×{target:.1f} parlay with ~{custom_odds} odds picks..._",
        parse_mode="Markdown")

    await _build_parlay_from_message(status_msg, context, target, min_odds, max_odds, custom_odds)
    return True


async def _build_parlay(q, context, target, min_odds, max_odds, mid_odds):
    """Build parlay from callback query."""
    range_label = f"×{min_odds:.1f} – ×{max_odds:.1f}"
    needed = _calc_needed_legs(target, mid_odds)

    status_msg = await q.message.reply_text(
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ *Quick Picks*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"_Building ×{target:.1f} parlay ({range_label})..._\n"
        f"_{needed} legs at ~×{mid_odds:.1f} each_",
        parse_mode="Markdown")

    await _generate_parlay(status_msg, context, target, min_odds, max_odds, mid_odds, range_label)


async def _build_parlay_from_message(msg, context, target, min_odds, max_odds, mid_odds):
    """Build parlay from text message."""
    range_label = f"~×{mid_odds:.1f}"
    needed = _calc_needed_legs(target, mid_odds)

    status_msg = await msg.reply_text(
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ *Quick Picks*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"_Building ×{target:.1f} parlay ({range_label})..._\n"
        f"_{needed} legs needed_",
        parse_mode="Markdown")

    await _generate_parlay(status_msg, context, target, min_odds, max_odds, mid_odds, range_label)


async def _generate_parlay(status_msg, context, target, min_odds, max_odds, mid_odds, range_label):
    """Core parlay building logic using advanced analytics."""
    user_id = status_msg.chat.id
    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()
    user_sports = get_user_sports(user) if user else None
    user_tz = getattr(user, "tz_offset", 0) if user else 0

    client = ESPNClient()
    try:
        et = datetime.now(timezone(timedelta(hours=-4))).strftime("%Y%m%d")
        utc = datetime.utcnow().strftime("%Y%m%d")

        leagues_to_fetch = [
            lg for lg in LEAGUES.keys()
            if not user_sports or lg.split("/")[0] in user_sports
        ]

        all_picks = []
        seen = set()

        for date in [et, utc]:
            if all_picks:
                break
            raw = await client.fetch_all_leagues(date=date, leagues=leagues_to_fetch)
            for league_code, data in raw.items():
                if isinstance(data, Exception) or not data:
                    continue
                fixtures = ESPNClient.parse_events(data, league_code)
                for fx in fixtures:
                    if fx["status"] != "pre":
                        continue

                    # Use advanced analytics engine
                    picks = analyze_fixture(fx, league_code)
                    for p in picks:
                        key = (fx["id"], p["market"])
                        if key in seen:
                            continue
                        seen.add(key)
                        if not (min_odds <= p["odds"] <= max_odds):
                            continue
                        p["home"] = fx["home_team"]
                        p["away"] = fx["away_team"]
                        p["league"] = league_code
                        p["sport"] = fx.get("sport", league_code.split("/")[0])
                        try:
                            game_dt = datetime.fromisoformat(fx["date"].replace("Z", "+00:00"))
                            local_dt = game_dt + timedelta(hours=user_tz)
                            p["kickoff"] = local_dt.strftime("%H:%M")
                        except (ValueError, TypeError):
                            p["kickoff"] = ""
                        all_picks.append(p)

        if not all_picks:
            kb = [
                [InlineKeyboardButton("🔄 Try Another Range", callback_data="menu_quickpicks")],
                [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
            ]
            await status_msg.edit_text(
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "⚡ *Quick Picks*\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"No picks found in the {range_label} range for ×{target:.1f}.\n"
                "_Try a different range or check back closer to match times._",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb))
            return

        # Sort by confidence (safest first)
        all_picks.sort(key=lambda x: x["confidence"], reverse=True)

        # Build parlay: pick best legs, one per fixture, toward target odds
        parlay = []
        combined_odds = 1.0
        used_fids = set()

        for p in all_picks:
            if combined_odds >= target * 0.90:
                break
            if len(parlay) >= 12:
                break
            fid_key = f"{p.get('home', '')}-{p.get('away', '')}"
            if fid_key in used_fids:
                continue

            new_odds = combined_odds * p["odds"]
            if new_odds > target * 1.30:
                continue

            parlay.append(p)
            combined_odds = new_odds
            used_fids.add(fid_key)

        if not parlay:
            kb = [
                [InlineKeyboardButton("🔄 Try Another Range", callback_data="menu_quickpicks")],
                [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
            ]
            await status_msg.edit_text(
                "━━━━━━━━━━━━━━━━━━━━━\n"
                "⚡ *Quick Picks*\n"
                "━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"Could not build a ×{target:.1f} parlay with {range_label} picks.\n"
                "_Try a different range._",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb))
            return

        # Recalculate combined probability and apply correlation penalty
        combined_prob = 1.0
        for p in parlay:
            combined_prob *= p["probability"]

        corr_penalty = correlation_penalty(parlay)
        adjusted_prob = combined_prob * corr_penalty

        # Confidence label based on adjusted probability
        if adjusted_prob >= 0.50:
            conf_emoji = "🟢"
            conf_label = "Very High"
        elif adjusted_prob >= 0.30:
            conf_emoji = "🟡"
            conf_label = "High"
        elif adjusted_prob >= 0.15:
            conf_emoji = "🟠"
            conf_label = "Moderate"
        else:
            conf_emoji = "⚠️"
            conf_label = "Lower"

        # Build clean display
        date_str = datetime.utcnow().strftime("%b %d, %Y")
        lines = [
            f"━━━━━━━━━━━━━━━━━━━━━",
            f"⚡ *QUICK PICKS — {date_str}*",
            f"━━━━━━━━━━━━━━━━━━━━━\n",
            f"{conf_emoji} *Confidence: {conf_label} ({adjusted_prob*100:.0f}%)*",
            f"🎯 *Target: ×{target:.1f} | Actual: ×{combined_odds:.2f}*\n",
        ]

        for i, p in enumerate(parlay, 1):
            time_str = f"\n   ⏰ {p['kickoff']}" if p.get("kickoff") else ""
            lines.append(
                f"*{i}. {p['home']} vs {p['away']}*{time_str}\n"
                f"   {p['label']} @ *{p['odds']}*"
            )

        lines.extend([
            "",
            f"*{len(parlay)} selections | ×{combined_odds:.2f}*",
            "",
            "🤖 _@fireparlays_",
        ])

        text = "\n".join(lines)

        kb = [
            [InlineKeyboardButton("🔄 New Parlay", callback_data="menu_quickpicks")],
            [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
        ]
        await status_msg.edit_text(text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb))

    finally:
        await client.close()

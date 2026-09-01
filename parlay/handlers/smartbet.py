"""
Smart Bet — AI-powered daily parlay pick with subscription model.

Flow:
1. User clicks Smart Bet → sees today's pick + subscribe option
2. User subscribes (free) → gets daily notifications when new pick is generated
3. Background job checks results when last game settles → sends win/loss notification
"""
import json
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime, timedelta, timezone
from sqlalchemy import select, func, Integer
from services.smart_bet import smart_bet_engine
from services.espn_api import ESPNClient
from database.db import SessionLocal, User, SmartBetPick, SmartBetStreak, UltraPick
from utils.helpers import safe_edit

logger = logging.getLogger(__name__)




async def smart_bet_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show Smart Bet — generate today's pick and show subscribe option."""
    user_id = update.effective_user.id
    q = update.callback_query

    msg = update.message or (q.message if q else None)
    if not msg:
        return

    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()

    if not user:
        kb = [[InlineKeyboardButton("🏠 Back", callback_data="menu_main")]]
        if update.message:
            await update.message.reply_text("⚠️ Please /start first.", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await safe_edit(q, "⚠️ Please /start first.", reply_markup=InlineKeyboardMarkup(kb))
        return

    is_subscribed = getattr(user, "smart_bet_subscribed", False)

    status_msg = await msg.reply_text(
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 *Smart Bet*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "_Analyzing all fixtures..._\n"
        "_Building the safest parlay..._\n"
        "_This may take a moment..._",
        parse_mode="Markdown")

    from handlers.sports import get_user_sports
    user_sports = get_user_sports(user)
    tz_offset = getattr(user, "tz_offset", 0) if user else 0
    pick = await smart_bet_engine.generate_daily_pick(user_sports=user_sports, tz_offset=tz_offset)

    if pick.get("has_pick"):
        # Store pick in DB for result tracking
        async with SessionLocal() as s:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            existing = await s.execute(
                select(SmartBetPick).where(SmartBetPick.date == today))
            existing_pick = existing.scalar_one_or_none()

            selections_json = json.dumps([
                {
                    "home": p.get("home", ""),
                    "away": p.get("away", ""),
                    "label": p.get("label", ""),
                    "odds": p.get("odds", 0),
                    "market": p.get("market", ""),
                    "league": p.get("league", ""),
                    "fixture_id": p.get("fixture", {}).get("id", ""),
                    "kickoff_iso": p.get("fixture", {}).get("date", ""),
                    "confidence": p.get("confidence", 0),
                    "edge": p.get("edge", 0),
                    "probability": p.get("probability", 0),
                }
                for p in pick.get("selections", [])
            ])

            if existing_pick:
                existing_pick.selections_json = selections_json
                existing_pick.total_odds = pick.get("total_odds", 0)
                existing_pick.combined_prob = pick.get("combined_prob", 0)
                existing_pick.num_legs = pick.get("num_legs", 0)
                existing_pick.message = pick.get("message", "")
                existing_pick.result = "pending"
                existing_pick.won_count = 0
                existing_pick.notified = False
            else:
                new_pick = SmartBetPick(
                    date=today,
                    selections_json=selections_json,
                    total_odds=pick.get("total_odds", 0),
                    combined_prob=pick.get("combined_prob", 0),
                    num_legs=pick.get("num_legs", 0),
                    message=pick.get("message", ""),
                    result="pending",
                )
                s.add(new_pick)
            await s.commit()

        # Build keyboard with subscribe/unsubscribe
        if is_subscribed:
            kb = [
                [InlineKeyboardButton("🔕 Unsubscribe from Daily Picks", callback_data="smartbet_unsubscribe")],
                [InlineKeyboardButton("🔄 Regenerate", callback_data="menu_smartbet")],
                [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
            ]
            sub_text = "\n✅ *You are subscribed!* You will get notified when results come in."
        else:
            kb = [
                [InlineKeyboardButton("🔔 Subscribe for Daily Picks (FREE)", callback_data="smartbet_subscribe")],
                [InlineKeyboardButton("🔄 Regenerate", callback_data="menu_smartbet")],
                [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
            ]
            sub_text = "\n\n💡 _Subscribe to get daily notifications and results automatically!_"

        await status_msg.edit_text(
            pick["message"] + sub_text,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb))
    else:
        kb = [
            [InlineKeyboardButton("🔄 Try Again", callback_data="menu_smartbet")],
            [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
        ]
        await status_msg.edit_text(
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🤖 *Smart Bet*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{pick.get('message', 'No pick available today.')}",
            parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


async def smart_bet_proceed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Compatibility stub — answers callback then redirects to handler."""
    q = update.callback_query
    if q:
        await q.answer("🤖 Analyzing fixtures...")
    await smart_bet_handler(update, context)


async def smart_bet_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Subscribe user to daily smart bet notifications."""
    q = update.callback_query
    await q.answer("✅ Subscribed!")
    user_id = q.from_user.id

    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()
        if user:
            user.smart_bet_subscribed = True
            user.smart_bet_subscribed_at = datetime.now(timezone.utc)
            await s.commit()

    kb = [
        [InlineKeyboardButton("🤖 Smart Bet", callback_data="menu_smartbet")],
        [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
    ]
    await q.edit_message_text(
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ *Subscribed to Smart Bet!*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "You'll now receive:\n"
        "• 📋 Daily Smart Bet picks\n"
        "• 📊 Win/Loss results after games conclude\n"
        "• 🔥 Streak tracking\n\n"
        "_Notifications are sent when picks are generated and when results settle._",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))


async def smart_bet_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unsubscribe user from daily smart bet notifications."""
    q = update.callback_query
    await q.answer("🔕 Unsubscribed")
    user_id = q.from_user.id

    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()
        if user:
            user.smart_bet_subscribed = False
            await s.commit()

    kb = [
        [InlineKeyboardButton("🔔 Re-subscribe (FREE)", callback_data="smartbet_subscribe")],
        [InlineKeyboardButton("🤖 Smart Bet", callback_data="menu_smartbet")],
        [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
    ]
    await q.edit_message_text(
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🔕 *Unsubscribed from Smart Bet*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "You will not receive daily notifications anymore.\n"
        "You can still view picks manually from the Smart Bet menu.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))


# ─── Ultra Mode (Continuous Chain) ───────────────────────────────

async def ultra_mode_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Dedicated Ultra Mode menu — shows status and controls."""
    user_id = update.effective_user.id
    q = update.callback_query

    msg = update.message or (q.message if q else None)
    if not msg:
        return

    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()

    if not user:
        kb = [[InlineKeyboardButton("🏠 Back", callback_data="menu_main")]]
        if update.message:
            await update.message.reply_text("⚠️ Please /start first.", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await safe_edit(q, "⚠️ Please /start first.", reply_markup=InlineKeyboardMarkup(kb))
        return

    is_ultra = getattr(user, "ultra_mode_subscribed", False)

    # Get chain stats
    async with SessionLocal() as s:
        chain_res = await s.execute(
            select(UltraPick).where(UltraPick.user_id == user_id)
            .order_by(UltraPick.chain_number.desc()).limit(1))
        last_pick = chain_res.scalar_one_or_none()

        # Count wins and total
        stats_res = await s.execute(
            select(
                func.count().label("total"),
                func.sum(func.cast(UltraPick.status == "won", Integer)).label("wins")
            )
            .where(UltraPick.user_id == user_id)
            .where(UltraPick.status.in_(["won", "lost", "partial"]))
        )
        stats_row = stats_res.one()
        total_picks = stats_row.total or 0
        total_wins = stats_row.wins or 0

    chain_num = last_pick.chain_number if last_pick else 0
    win_rate = (total_wins / total_picks * 100) if total_picks > 0 else 0

    if is_ultra:
        status_text = "✅ *Active*"
        chain_text = f"Current chain: *#{chain_num}*"
        kb = [
            [InlineKeyboardButton("🔄 Reset Chain to #1", callback_data="ultra_reset")],
            [InlineKeyboardButton("⚡ Stop Ultra Mode", callback_data="ultra_unsubscribe")],
            [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
        ]
    else:
        status_text = "⬜ *Inactive*"
        chain_text = "Start a new chain!"
        kb = [
            [InlineKeyboardButton("⚡ Start Ultra Mode", callback_data="ultra_subscribe")],
            [InlineKeyboardButton("🔄 Reset Chain to #1", callback_data="ultra_reset")],
            [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
        ]

    text = (
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ *ULTRA MODE*\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Status: {status_text}\n"
        f"{chain_text}\n\n"
        f"📊 *Stats:*\n"
        f"  Total picks: *{total_picks}*\n"
        f"  Wins: *{total_wins}* ({win_rate:.0f}% win rate)\n\n"
        f"_Continuous ~2x parlays. Win and the chain continues!_\n"
        f"_Lose and the chain stops. Restart anytime._"
    )

    if q:
        await safe_edit(q, text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb))
    else:
        await msg.reply_text(text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb))


# Shared fixture cache for ultra mode (avoids re-fetching from ESPN)
_ultra_fixture_cache = None
_ultra_cache_time = None
_ULTRA_CACHE_TTL = 600  # 10 minutes


async def _get_cached_fixtures(user_sports=None):
    """Get fixtures from cache or fetch fresh. Shared across all ultra picks."""
    global _ultra_fixture_cache, _ultra_cache_time
    import time

    now = time.time()
    if _ultra_fixture_cache and _ultra_cache_time and (now - _ultra_cache_time) < _ULTRA_CACHE_TTL:
        return _ultra_fixture_cache

    from services.espn_api import ESPNClient
    from services.analytics import analyze_fixture
    from config import LEAGUES
    from datetime import datetime, timedelta, timezone

    client = ESPNClient()
    try:
        et = datetime.now(timezone(timedelta(hours=-4))).strftime("%Y%m%d")
        utc_now = datetime.now(timezone.utc).strftime("%Y%m%d")

        leagues_to_fetch = [
            lg for lg in LEAGUES.keys()
            if not user_sports or lg.split("/")[0] in user_sports
        ]

        all_picks = []
        seen = set()

        for date in [et, utc_now]:
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
                    picks = analyze_fixture(fx, league_code)
                    for p in picks:
                        key = (fx["id"], p["market"])
                        if key in seen:
                            continue
                        seen.add(key)
                        p["home"] = fx["home_team"]
                        p["away"] = fx["away_team"]
                        p["league"] = league_code
                        p["sport"] = fx.get("sport", league_code.split("/")[0])
                        p["fixture"] = fx
                        try:
                            game_dt = datetime.fromisoformat(fx["date"].replace("Z", "+00:00"))
                            now_utc = datetime.now(timezone.utc)
                            hours_until = (game_dt - now_utc).total_seconds() / 3600
                            p["hours_until"] = hours_until
                            p["kickoff"] = game_dt.strftime("%b %d, %H:%M")
                        except (ValueError, TypeError):
                            p["hours_until"] = 999
                            p["kickoff"] = ""
                        all_picks.append(p)

        # Sort by kickoff time (soonest first)
        all_picks.sort(key=lambda x: x.get("hours_until", 999))

        _ultra_fixture_cache = all_picks
        _ultra_cache_time = now
        return all_picks
    finally:
        await client.close()


async def ultra_mode_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Subscribe to ultra mode — continuous chain of ~2x parlays."""
    q = update.callback_query
    await q.answer("⚡ Ultra Mode activated!")
    user_id = q.from_user.id

    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()
        if user:
            user.ultra_mode_subscribed = True
            user.ultra_mode_subscribed_at = datetime.now(timezone.utc)
            await s.commit()

    # Generate first ultra pick immediately
    await _generate_ultra_pick(user_id, app=context.application)

    kb = [
        [InlineKeyboardButton("⚡ Ultra Mode", callback_data="menu_smartbet")],
        [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
    ]
    await q.edit_message_text(
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ *ULTRA MODE ACTIVATED*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "You will now receive:\n"
        "• ⚡ Continuous ~2x parlay picks\n"
        "• 🔄 New pick sent immediately when the current one settles\n"
        "• 📊 Win/Loss results for each pick\n\n"
        "_Your first pick is being generated now..._",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))


async def ultra_mode_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset ultra mode chain to #1 — clears all previous picks."""
    q = update.callback_query
    await q.answer("🔄 Chain reset!")
    user_id = q.from_user.id

    async with SessionLocal() as s:
        # Delete all previous ultra picks for this user
        from sqlalchemy import delete
        await s.execute(
            delete(UltraPick).where(UltraPick.user_id == user_id))
        await s.commit()

    # Re-show the menu with chain #1
    await ultra_mode_menu(update, context)


async def ultra_mode_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unsubscribe from ultra mode."""
    q = update.callback_query
    await q.answer("⚡ Ultra Mode deactivated")
    user_id = q.from_user.id

    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()
        if user:
            user.ultra_mode_subscribed = False
            # Cancel any active ultra pick
            active = await s.execute(
                select(UltraPick).where(
                    UltraPick.user_id == user_id,
                    UltraPick.status == "active"))
            for pick in active.scalars().all():
                pick.status = "cancelled"
            await s.commit()

    kb = [
        [InlineKeyboardButton("⚡ Re-enable Ultra Mode", callback_data="ultra_subscribe")],
        [InlineKeyboardButton("🤖 Smart Bet", callback_data="menu_smartbet")],
        [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
    ]
    await q.edit_message_text(
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ *Ultra Mode Deactivated*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "You will no longer receive continuous picks.\n"
        "Your active chain has been stopped.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(kb))


async def _generate_ultra_pick(user_id: int, app=None):
    """Generate a ~2x parlay pick for ultra mode using cached fixtures (fast)."""
    try:
        import json

        # Get user sports preferences
        async with SessionLocal() as s:
            res = await s.execute(select(User).where(User.tg_id == user_id))
            user = res.scalar_one_or_none()
            if not user or not user.ultra_mode_subscribed:
                return

            # Get chain number
            chain_res = await s.execute(
                select(UltraPick).where(UltraPick.user_id == user_id)
                .order_by(UltraPick.chain_number.desc()).limit(1))
            last_pick = chain_res.scalar_one_or_none()
            chain_number = (last_pick.chain_number + 1) if last_pick else 1

        from handlers.sports import get_user_sports
        user_sports = get_user_sports(user)

        # Use cached fixtures (fast, no re-fetch from ESPN)
        all_picks = await _get_cached_fixtures(user_sports)

        if not all_picks:
            if app:
                try:
                    await app.bot.send_message(
                        chat_id=user_id,
                        text="⚡ *Ultra Mode*\n\n_No suitable fixtures found right now._",
                        parse_mode="Markdown")
                except Exception:
                    pass
            return

        # Ultra Mode = SINGLE GAME picks at ~2x odds (not parlays)
        # Find single games with odds close to 2.0x and positive edge
        from config import LEAGUES as LG

        # Get previously picked fixture IDs for this user to avoid repeats
        async with SessionLocal() as s:
            prev_res = await s.execute(
                select(UltraPick.selections_json).where(
                    UltraPick.user_id == user_id,
                    UltraPick.status.in_(["won", "lost", "partial", "active"])))
            picked_fids = set()
            for row in prev_res.scalars().all():
                try:
                    sels = json.loads(row)
                    for sel in sels:
                        picked_fids.add(sel.get("fixture_id", ""))
                except (json.JSONDecodeError, TypeError, KeyError):
                    pass

        # Filter: single game picks near 2.0x odds, exclude already picked
        candidates = [
            p for p in all_picks
            if p["confidence"] >= 15
            and p["odds"] >= 1.60
            and p["odds"] <= 2.50
            and p["probability"] >= 0.38
            and p["edge"] >= -0.03
            and p["fixture"]["id"] not in picked_fids
        ]

        if not candidates:
            # Widen if no fresh picks available
            candidates = [
                p for p in all_picks
                if p["confidence"] >= 10
                and p["odds"] >= 1.50
                and p["odds"] <= 2.80
                and p["probability"] >= 0.35
                and p["edge"] >= -0.05
            ]

        if not candidates:
            if app:
                try:
                    await app.bot.send_message(
                        chat_id=user_id,
                        text="⚡ *Ultra Mode*\n\n_No ~2x picks available right now. Will try again later._",
                        parse_mode="Markdown")
                except Exception:
                    pass
            return

        # Score: prefer odds closest to 2.0x + proximity boost
        def _ultra_score(p):
            # How close to 2.0x odds (penalize far from target)
            odds_dist = abs(p["odds"] - 2.0)
            odds_score = max(0, 1.0 - odds_dist * 0.5)  # 1.0 at exactly 2.0x, 0 at 4.0x
            # Edge bonus
            edge_bonus = max(0, p["edge"]) * 5
            # Proximity boost
            hours = p.get("hours_until", 999)
            if hours <= 1:
                prox = 2.0
            elif hours <= 3:
                prox = 1.5
            elif hours <= 6:
                prox = 1.2
            else:
                prox = 1.0
            return (odds_score + edge_bonus) * prox

        candidates.sort(key=_ultra_score, reverse=True)

        # Pick the BEST single game
        best_pick = candidates[0]
        single_odds = best_pick["odds"]
        single_prob = best_pick["probability"]
        league_name = LG.get(best_pick.get("league", ""), best_pick.get("league", ""))
        hours = best_pick.get("hours_until", 999)

        # Build message
        if hours <= 1:
            time_badge = "🔴 STARTING SOON"
        elif hours <= 3:
            time_badge = "🟡 KICKOFF IN {:.0f}H".format(hours)
        else:
            time_badge = "🟢 KICKOFF IN {:.0f}H".format(hours)

        lines = [
            f"━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"⚡ *ULTRA MODE — Chain #{chain_number}*",
            f"━━━━━━━━━━━━━━━━━━━━━━━━━\n",
            f"{time_badge}\n",
            f"*{best_pick['home']} vs {best_pick['away']}*",
            f"{best_pick['label']} @ *×{single_odds}*",
            f"League: {league_name}\n",
            f"📊 Probability: *{single_prob*100:.1f}%*",
            f"📈 Edge: *{best_pick['edge']*100:+.1f}%*",
            f"🎯 Confidence: *{best_pick['confidence']:.0f}*\n",
            "━━━━━━━━━━━━━━━━━━━━━━━━━",
            "_Single game pick. Win and the chain continues!_",
        ]

        message = "\n".join(lines)

        # Build pick dict for storage
        pick = {
            "has_pick": True,
            "selections": [best_pick],
            "total_odds": round(single_odds, 2),
            "combined_prob": round(single_prob, 4),
            "confidence": round(best_pick["confidence"], 1),
            "num_legs": 1,
            "message": message,
        }

        # Store ultra pick
        selections_json = json.dumps([
            {
                "home": p.get("home", ""),
                "away": p.get("away", ""),
                "label": p.get("label", ""),
                "odds": p.get("odds", 0),
                "market": p.get("market", ""),
                "league": p.get("league", ""),
                "fixture_id": p.get("fixture", {}).get("id", ""),
                "kickoff_iso": p.get("fixture", {}).get("date", ""),
                "confidence": p.get("confidence", 0),
                "edge": p.get("edge", 0),
                "probability": p.get("probability", 0),
            }
            for p in pick.get("selections", [])
        ])

        async with SessionLocal() as s:
            ultra_pick = UltraPick(
                user_id=user_id,
                selections_json=selections_json,
                total_odds=pick.get("total_odds", 0),
                combined_prob=pick.get("combined_prob", 0),
                num_legs=pick.get("num_legs", 0),
                message=pick.get("message", ""),
                chain_number=chain_number,
                status="active",
            )
            s.add(ultra_pick)
            await s.commit()

        # Send to user
        if app:
            chain_text = f"\n\n_⚡ Chain #{chain_number} | Send /ultra to stop_"
            kb = [[InlineKeyboardButton("⚡ Stop Ultra Mode", callback_data="ultra_unsubscribe")]]
            from telegram import InlineKeyboardButton as IKButton, InlineKeyboardMarkup as IKMarkup
            try:
                await app.bot.send_message(
                    chat_id=user_id,
                    text=pick["message"] + chain_text,
                    parse_mode="Markdown",
                    reply_markup=IKMarkup(kb))
            except Exception:
                pass

        logger.info(f"Ultra pick #{chain_number} sent to user {user_id}")

    except Exception as e:
        logger.error(f"Ultra pick generation error for user {user_id}: {e}")


async def check_ultra_picks(app):
    """Check if any active ultra picks have settled. If so, send result and generate next pick."""
    try:
        async with SessionLocal() as s:
            # Get all active ultra picks
            res = await s.execute(
                select(UltraPick).where(UltraPick.status == "active"))
            active_picks = res.scalars().all()

            if not active_picks:
                return

            for ultra_pick in active_picks:
                selections = json.loads(ultra_pick.selections_json)
                if not selections:
                    continue

                # Check each selection
                all_settled = True
                all_won = True
                results = []

                client = ESPNClient()
                try:
                    # Pass today's date explicitly so ESPN returns finished games
                    today_date = datetime.now(timezone.utc).strftime("%Y%m%d")
                    yesterday_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")

                    for sel in selections:
                        fixture_id = sel.get("fixture_id", "")
                        league = sel.get("league", "")

                        if not fixture_id or not league:
                            all_settled = False
                            continue

                        # Try today first, then yesterday (late games may cross midnight)
                        data = await client.fetch_scoreboard(league, date=today_date)
                        if not data:
                            all_settled = False
                            continue

                        fixtures = ESPNClient.parse_events(data, league)
                        current_fx = next(
                            (f for f in fixtures if str(f["id"]) == str(fixture_id)), None)

                        # If not found today, check yesterday
                        if not current_fx:
                            data_y = await client.fetch_scoreboard(league, date=yesterday_date)
                            if data_y:
                                fixtures_y = ESPNClient.parse_events(data_y, league)
                                current_fx = next(
                                    (f for f in fixtures_y if str(f["id"]) == str(fixture_id)), None)

                        if not current_fx or current_fx["status"] == "pre":
                            all_settled = False
                            continue

                        if current_fx["status"] == "in":
                            all_settled = False
                            continue

                        won = _check_selection_won(sel, current_fx)
                        if won is None:
                            all_settled = False
                            continue

                        results.append({
                            "home": current_fx["home_team"],
                            "away": current_fx["away_team"],
                            "home_score": current_fx["home_score"],
                            "away_score": current_fx["away_score"],
                            "label": sel.get("label", ""),
                            "won": won,
                            "odds": sel.get("odds", 0),
                        })

                        if not won:
                            all_won = False
                finally:
                    await client.close()

                if not all_settled or not results:
                    continue

                # Settled — update status
                won_count = sum(1 for r in results if r["won"])
                total = len(results)
                if all_won:
                    ultra_pick.status = "won"
                elif won_count == 0:
                    ultra_pick.status = "lost"
                else:
                    ultra_pick.status = "partial"
                ultra_pick.won_count = won_count
                ultra_pick.settled_at = datetime.now(timezone.utc)
                await s.commit()

                # Send result to user
                if all_won:
                    emoji = "🎉"
                    status_text = "✅ *WON!*"
                elif won_count == 0:
                    emoji = "😞"
                    status_text = "❌ *LOST*"
                else:
                    emoji = "🤔"
                    status_text = f"⚠️ *PARTIAL* ({won_count}/{total})"

                lines = [
                    f"{emoji} *ULTRA MODE RESULT — Chain #{ultra_pick.chain_number}*",
                    "━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                    f"{status_text}\n",
                ]
                for i, r in enumerate(results, 1):
                    check = "✅" if r["won"] else "❌"
                    lines.append(
                        f"*{i}. {check}* {r['home']} *{r['home_score']}* - "
                        f"*{r['away_score']}* {r['away']}\n"
                        f"   {r['label']} @ ×{r['odds']}"
                    )
                lines.extend([
                    "",
                    f"*{won_count}/{total} selections won*",
                    f"Odds: ×{ultra_pick.total_odds:.2f}",
                ])

                text = "\n".join(lines)
                kb = [[InlineKeyboardButton("⚡ Stop Ultra Mode", callback_data="ultra_unsubscribe")]]

                try:
                    from telegram import InlineKeyboardButton as IKButton, InlineKeyboardMarkup as IKMarkup
                    await app.bot.send_message(
                        chat_id=ultra_pick.user_id,
                        text=text,
                        parse_mode="Markdown",
                        reply_markup=IKMarkup(kb))
                except Exception:
                    pass

                # If won, generate next pick immediately
                if all_won:
                    logger.info(f"Ultra pick #{ultra_pick.chain_number} won for user {ultra_pick.user_id}, generating next...")
                    await _generate_ultra_pick(ultra_pick.user_id, app=app)
                else:
                    # Lost or partial — stop the chain, notify user
                    try:
                        from telegram import InlineKeyboardButton as IKButton, InlineKeyboardMarkup as IKMarkup
                        await app.bot.send_message(
                            chat_id=ultra_pick.user_id,
                            text="_⚡ Ultra Mode chain ended. Subscribe again to restart._",
                            parse_mode="Markdown",
                            reply_markup=IKMarkup([[IKButton("⚡ Restart Ultra Mode", callback_data="ultra_subscribe")]]))
                    except Exception:
                        pass
                    # Deactivate user
                    async with SessionLocal() as s2:
                        user_res = await s2.execute(
                            select(User).where(User.tg_id == ultra_pick.user_id))
                        user = user_res.scalar_one_or_none()
                        if user:
                            user.ultra_mode_subscribed = False
                            await s2.commit()

        logger.info(f"Ultra picks check complete. {len(active_picks)} active picks processed.")

    except Exception as e:
        logger.error(f"Ultra picks check error: {e}")


async def handle_passkey_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """No longer needed — always returns False."""
    return False


# ─── Background: Check Smart Bet Results ───────────────────────────

async def check_smart_bet_results(app):
    """
    Background job: check if today's smart bet picks have settled.
    Notifies ALL subscribed users when results are ready.
    Sends one notification per user with detailed win/loss breakdown.
    """
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        async with SessionLocal() as s:
            # Get today's pick
            res = await s.execute(
                select(SmartBetPick).where(SmartBetPick.date == today))
            pick_record = res.scalar_one_or_none()

            if not pick_record or pick_record.notified:
                return
            if pick_record.result != "pending":
                return

            selections = json.loads(pick_record.selections_json)
            if not selections:
                return

            # SAFETY BUFFER: Don't check until at least 2 hours after the earliest
            # kickoff. This avoids wasting ESPN calls on games that just started.
            # The actual settlement check (status == "in") handles the rest.
            now_utc = datetime.now(timezone.utc)
            earliest_kickoff = None
            for sel in selections:
                kick_str = sel.get("kickoff_iso", "")
                if kick_str:
                    try:
                        kick_str_clean = kick_str.replace("Z", "+00:00")
                        kdt = datetime.fromisoformat(kick_str_clean)
                        if earliest_kickoff is None or kdt < earliest_kickoff:
                            earliest_kickoff = kdt
                    except (ValueError, TypeError):
                        pass

            # Wait at least 2h after earliest kickoff (no point checking too soon)
            if earliest_kickoff:
                earliest_safe = earliest_kickoff + timedelta(hours=2)
                if now_utc < earliest_safe:
                    return  # Too early — games definitely still in progress

            # Check each selection
            all_settled = True
            all_won = True
            results = []

            client = ESPNClient()
            try:
                # Pass today's date explicitly so ESPN returns finished games too
                today_date = datetime.now(timezone.utc).strftime("%Y%m%d")
                yesterday_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")

                for sel in selections:
                    fixture_id = sel.get("fixture_id", "")
                    league = sel.get("league", "")

                    # BUG FIX: missing data means unsettled, not skip
                    if not fixture_id or not league:
                        all_settled = False
                        continue

                    # Try today first, then yesterday (late games may cross midnight)
                    data = await client.fetch_scoreboard(league, date=today_date)
                    if not data:
                        all_settled = False
                        continue

                    fixtures = ESPNClient.parse_events(data, league)
                    current_fx = next(
                        (f for f in fixtures if str(f["id"]) == str(fixture_id)), None)

                    # If not found today, check yesterday
                    if not current_fx:
                        data_y = await client.fetch_scoreboard(league, date=yesterday_date)
                        if data_y:
                            fixtures_y = ESPNClient.parse_events(data_y, league)
                            current_fx = next(
                                (f for f in fixtures_y if str(f["id"]) == str(fixture_id)), None)

                    if not current_fx or current_fx["status"] == "pre":
                        all_settled = False
                        continue

                    if current_fx["status"] == "in":
                        all_settled = False
                        continue

                    # Post-match — check if selection won
                    won = _check_selection_won(sel, current_fx)

                    # None = undetermined market type, treat as not settled
                    if won is None:
                        all_settled = False
                        continue

                    results.append({
                        "home": current_fx["home_team"],
                        "away": current_fx["away_team"],
                        "home_score": current_fx["home_score"],
                        "away_score": current_fx["away_score"],
                        "label": sel.get("label", ""),
                        "won": won,
                        "odds": sel.get("odds", 0),
                        "edge": sel.get("edge", 0),
                    })

                    if not won:
                        all_won = False
            finally:
                await client.close()

            if not all_settled:
                return

            if not results:
                return

            # All settled — update pick record
            won_count = sum(1 for r in results if r["won"])
            total = len(results)

            if all_won:
                pick_record.result = "won"
            elif won_count == 0:
                pick_record.result = "lost"
            else:
                pick_record.result = "partial"
            pick_record.won_count = won_count
            pick_record.notified = True
            await s.commit()

            # BUG FIX: collect user IDs inside session to avoid detached access
            user_res = await s.execute(
                select(User.tg_id).where(User.smart_bet_subscribed == True))
            subscribed_user_ids = [row[0] for row in user_res.all()]

        # Send result to all subscribed users (session closed, using plain IDs)
        for user_id in subscribed_user_ids:

            # Update streak (skip if no results)
            if not results:
                continue

            # Load streak from DB
            async with SessionLocal() as streak_s:
                streak_res = await streak_s.execute(
                    select(SmartBetStreak).where(SmartBetStreak.user_id == user_id))
                streak_record = streak_res.scalar_one_or_none()
                if not streak_record:
                    streak_record = SmartBetStreak(user_id=user_id)
                    streak_s.add(streak_record)

                streak_record.total_days += 1
                if all_won:
                    streak_record.streak += 1
                    streak_record.won_days += 1
                    streak_record.last_result = "won"
                    status = "✅ *WON!*"
                    emoji = "🎉"
                elif won_count == 0:
                    streak_record.streak = 0
                    streak_record.last_result = "lost"
                    status = "❌ *LOST*"
                    emoji = "😞"
                else:
                    streak_record.streak = 0
                    streak_record.last_result = "partial"
                    status = f"⚠️ *PARTIAL* ({won_count}/{total})"
                    emoji = "🤔"
                streak_record.last_updated = datetime.now(timezone.utc)
                await streak_s.commit()

                # Read back for message
                current_streak = streak_record.streak
                total_days = streak_record.total_days
                won_days = streak_record.won_days

            # Win rate
            win_rate = (won_days / total_days * 100) if total_days > 0 else 0
            streak_str = (
                f"🔥 *{current_streak} day streak!*"
                if current_streak > 0 else ""
            )
            rate_str = (
                f"📊 *{win_rate:.0f}% win rate* "
                f"({won_days}/{total_days} days)"
            )

            # Build result message
            lines = [
                f"{emoji} *SMART BET RESULT*",
                "━━━━━━━━━━━━━━━━━━━━━━━━━\n",
                f"{status}\n",
            ]

            for i, r in enumerate(results, 1):
                check = "✅" if r["won"] else "❌"
                lines.append(
                    f"*{i}. {check}* {r['home']} *{r['home_score']}* - "
                    f"*{r['away_score']}* {r['away']}\n"
                    f"   {r['label']} @ ×{r['odds']}"
                )

            lines.extend([
                "",
                f"*{won_count}/{total} selections won*",
                f"Odds: ×{pick_record.total_odds:.2f}",
                "",
                rate_str,
                streak_str,
            ])

            text = "\n".join(lines)

            kb = [
                [InlineKeyboardButton("🤖 Smart Bet", callback_data="menu_smartbet")],
                [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
            ]

            try:
                await app.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(kb))
            except Exception:
                pass  # User may have blocked bot

        logger.info(
            f"Smart bet results sent to {len(subscribed_user_ids)} subscribers. "
            f"Result: {pick_record.result} ({won_count}/{total})")

    except Exception as e:
        logger.error(f"Smart bet result check error: {e}")


# ─── Daily Summary ───────────────────────────────────────────────

async def send_daily_summary(app):
    """Send end-of-day summary to all subscribed users.
    Shows yesterday's pick result (all games finished by now),
    streak stats, and overall performance."""
    try:
        # Summary runs at 07:00 UTC — look for YESTERDAY's pick
        # (today's games haven't started yet at 07:00 UTC)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        async with SessionLocal() as s:
            res = await s.execute(
                select(SmartBetPick).where(SmartBetPick.date == yesterday))
            pick_record = res.scalar_one_or_none()

            if not pick_record:
                return  # No pick yesterday

            selections = json.loads(pick_record.selections_json) if pick_record.selections_json else []

            # Get all-time stats
            from sqlalchemy import func as sqlfunc
            total_picks_res = await s.execute(
                select(sqlfunc.count()).select_from(SmartBetPick).where(
                    SmartBetPick.result.in_(["won", "lost", "partial"])))
            total_picks = total_picks_res.scalar() or 0

            wins_res = await s.execute(
                select(sqlfunc.count()).select_from(SmartBetPick).where(
                    SmartBetPick.result == "won"))
            total_wins = wins_res.scalar() or 0

            # Get subscriber count
            sub_res = await s.execute(
                select(sqlfunc.count()).select_from(User).where(
                    User.smart_bet_subscribed == True))
            sub_count = sub_res.scalar() or 0

            # Collect subscriber IDs
            user_res = await s.execute(
                select(User.tg_id).where(User.smart_bet_subscribed == True))
            subscriber_ids = [row[0] for row in user_res.all()]

        if not subscriber_ids:
            return

        # If yesterday's pick is still pending, try to resolve it now
        if pick_record.result == "pending" and selections:
            try:
                now_utc = datetime.now(timezone.utc)
                yesterday_date = (now_utc - timedelta(days=1)).strftime("%Y%m%d")
                two_days_ago = (now_utc - timedelta(days=2)).strftime("%Y%m%d")

                client = ESPNClient()
                all_settled = True
                all_won = True
                resolved_results = []

                try:
                    for sel in selections:
                        fixture_id = sel.get("fixture_id", "")
                        league = sel.get("league", "")
                        if not fixture_id or not league:
                            all_settled = False
                            continue

                        data = await client.fetch_scoreboard(league, date=yesterday_date)
                        fixtures = ESPNClient.parse_events(data, league) if data else []
                        fx = next((f for f in fixtures if str(f["id"]) == str(fixture_id)), None)

                        if not fx:
                            data_y = await client.fetch_scoreboard(league, date=two_days_ago)
                            if data_y:
                                fixtures_y = ESPNClient.parse_events(data_y, league)
                                fx = next((f for f in fixtures_y if str(f["id"]) == str(fixture_id)), None)

                        if not fx or fx["status"] == "pre" or fx["status"] == "in":
                            all_settled = False
                            continue

                        won = _check_selection_won(sel, fx)
                        if won is None:
                            all_settled = False
                            continue
                        resolved_results.append(won)
                        if not won:
                            all_won = False
                finally:
                    await client.close()

                if all_settled and resolved_results:
                    won_count_resolved = sum(1 for w in resolved_results if w)
                    total_resolved = len(resolved_results)
                    new_result = "won" if all_won else ("lost" if won_count_resolved == 0 else "partial")
                    async with SessionLocal() as s2:
                        pick_date = pick_record.date
                        fresh = await s2.execute(
                            select(SmartBetPick).where(SmartBetPick.date == pick_date))
                        fresh_pick = fresh.scalar_one_or_none()
                        if fresh_pick:
                            fresh_pick.result = new_result
                            fresh_pick.won_count = won_count_resolved
                            fresh_pick.notified = True
                            await s2.commit()
                    pick_record.result = new_result
                    pick_record.won_count = won_count_resolved
            except Exception as e:
                logger.error(f"Summary result resolution error: {e}")

        # Determine result
        result = pick_record.result
        won_count = pick_record.won_count or 0
        total_legs = pick_record.num_legs or len(selections)

        if result == "won":
            result_emoji = "🎉"
            result_text = "✅ *WON!*"
        elif result == "lost":
            result_emoji = "😞"
            result_text = "❌ *LOST*"
        elif result == "partial":
            result_emoji = "🤔"
            result_text = f"⚠️ *PARTIAL* ({won_count}/{total_legs})"
        else:
            result_emoji = "⏳"
            result_text = "_Still pending — results not yet available._"

        # Build summary
        all_time_win_rate = (total_wins / total_picks * 100) if total_picks > 0 else 0

        lines = [
            f"{result_emoji} *DAILY SUMMARY — {yesterday}*",
            "━━━━━━━━━━━━━━━━━━━━━━━━━\n",
            f"*Yesterday's Pick:* {result_text}\n",
        ]

        # Show each leg
        if selections:
            lines.append("*Selections:*")
            for i, sel in enumerate(selections, 1):
                # Check if this leg's result is known from pick_record
                lines.append(
                    f"  {i}. {sel.get('home', '?')} vs {sel.get('away', '?')}\n"
                    f"     {sel.get('label', '?')} @ ×{sel.get('odds', 0)}"
                )
            lines.append("")

        lines.extend([
            f"*{won_count}/{total_legs} legs won*",
            f"Odds: ×{pick_record.total_odds:.2f}\n",
            "━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"📈 *All-Time Stats:*",
            f"  Win Rate: *{all_time_win_rate:.0f}%* ({total_wins}/{total_picks} days)",
            f"  Subscribers: *{sub_count}*\n",
            "━━━━━━━━━━━━━━━━━━━━━━━━━",
            "_See you tomorrow for the next pick!_",
            "",
            "🤖 _@fireparlays_",
        ])

        text = "\n".join(lines)

        kb = [
            [InlineKeyboardButton("🤖 Smart Bet", callback_data="menu_smartbet")],
            [InlineKeyboardButton("🏆 Leaderboard", callback_data="menu_leaderboard")],
            [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
        ]

        sent = 0
        for uid in subscriber_ids:
            try:
                await app.bot.send_message(
                    chat_id=uid,
                    text=text,
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(kb))
                sent += 1
            except Exception:
                pass

        logger.info(f"Daily summary sent to {sent}/{len(subscriber_ids)} subscribers. Result: {result}")

    except Exception as e:
        logger.error(f"Daily summary error: {e}")


def _check_selection_won(selection: dict, fixture: dict) -> bool | None:
    """Check if a selection won based on fixture result.
    Returns True (won), False (lost), or None (undetermined)."""
    label = selection.get("label", "")
    home_team = fixture.get("home_team", "")
    away_team = fixture.get("away_team", "")
    home_score = fixture.get("home_score", 0)
    away_score = fixture.get("away_score", 0)

    label_lower = label.lower()
    home_lower = home_team.lower()
    away_lower = away_team.lower()

    # Double Chance: Home or Draw (check BEFORE plain Draw)
    if "or draw" in label_lower and home_lower in label_lower:
        return home_score >= away_score

    # Double Chance: Away or Draw
    if "or draw" in label_lower and away_lower in label_lower:
        return away_score >= home_score

    # Double Chance: Home or Away (no draw possible)
    if "or " in label_lower and "draw" not in label_lower and "dnb" not in label_lower:
        if home_lower in label_lower:
            return home_score >= away_score
        elif away_lower in label_lower:
            return away_score >= home_score

    # Home win
    if f"{home_lower} win" in label_lower:
        return home_score > away_score

    # Away win
    if f"{away_lower} win" in label_lower:
        return away_score > home_score

    # Draw (plain draw, not double chance)
    if "draw" in label_lower and "dnb" not in label_lower:
        return home_score == away_score

    # DNB (Draw No Bet)
    if "dnb" in label_lower:
        if home_lower in label_lower:
            return home_score > away_score
        elif away_lower in label_lower:
            return away_score > home_score

    # Over/Under
    total_goals = home_score + away_score
    if "over" in label_lower:
        if "1.5" in label_lower:
            return total_goals > 1.5
        elif "2.5" in label_lower:
            return total_goals > 2.5
        elif "3.5" in label_lower:
            return total_goals > 3.5
        elif "0.5" in label_lower:
            return total_goals > 0.5
    if "under" in label_lower:
        if "1.5" in label_lower:
            return total_goals < 1.5
        elif "2.5" in label_lower:
            return total_goals < 2.5
        elif "3.5" in label_lower:
            return total_goals < 3.5
        elif "0.5" in label_lower:
            return total_goals < 0.5

    # Spread / Handicap
    if "spread" in label_lower or "handicap" in label_lower:
        try:
            import re
            spread_match = re.search(r'[-+]?\d+\.?\d*', label)
            if spread_match:
                spread_val = float(spread_match.group())
                if home_lower in label_lower:
                    return (home_score + spread_val) > away_score
                elif away_lower in label_lower:
                    return (away_score + spread_val) > home_score
        except (ValueError, TypeError):
            pass

    # Default: can't determine market type
    return None

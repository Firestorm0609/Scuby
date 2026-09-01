import logging
import asyncio
from datetime import datetime
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from config import BOT_TOKEN
from database.db import init_db, touch_user, SessionLocal, User
from handlers.start import start, help_handler, accept_terms_callback
from handlers.stats import stats_handler, leaderboard_handler
from handlers.smartbet import (
    smart_bet_handler, smart_bet_proceed, handle_passkey_input,
    check_smart_bet_results, smart_bet_subscribe, smart_bet_unsubscribe,
    send_daily_summary, ultra_mode_subscribe, ultra_mode_unsubscribe,
    check_ultra_picks, ultra_mode_menu, ultra_mode_reset,
)
from handlers.quickpicks import (
    quick_picks_menu, quick_picks_target_callback, quick_picks_range_callback,
    handle_qp_target_input, handle_qp_custom_range_input,
)
from handlers.sports import (
    sports_menu, sport_toggle_callback, sport_set_callback,
    sport_config_callback, sport_mkt_toggle_callback,
    sport_mkt_all_callback, sport_mkt_none_callback,
)
from handlers.fixtures import fixtures_handler
from handlers.live import (
    live_menu, live_matches_handler,
)
from handlers.timezone import timezone_menu, timezone_set_callback
from handlers.admin import (
    bot_stats_handler, admin_menu,
    admin_genkey_menu, admin_gendone, admin_viewkeys, admin_users,
    admin_togglepremium_prompt, handle_admin_input, admin_sendpicks,
)
from services.smart_bet import smart_bet_engine

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)


# ─── Presence middleware ───────────────────────────────────────────────

async def presence_middleware(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Update last_seen on every interaction so online-count stays accurate."""
    if update.effective_user:
        await touch_user(update.effective_user.id)


# ─── Menu router ──────────────────────────────────────────────────────

async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    if data == "menu_main":
        await start(update, context)
    elif data == "menu_stats":
        await stats_handler(update, context)
    elif data == "menu_help":
        await help_handler(update, context)
    elif data == "menu_botstats":
        await bot_stats_handler(update, context)
    elif data == "menu_leaderboard":
        await leaderboard_handler(update, context)
    elif data == "menu_smartbet":
        await smart_bet_handler(update, context)
    elif data == "menu_ultra":
        await ultra_mode_menu(update, context)
    elif data == "menu_quickpicks":
        await quick_picks_menu(update, context)
    elif data == "menu_sports":
        await sports_menu(update, context)
    elif data == "menu_timezone":
        await timezone_menu(update, context)
    elif data == "menu_live":
        await live_menu(update, context)
    elif data == "menu_fixtures":
        await fixtures_handler(update, context)


# ─── Text input router ────────────────────────────────────────────────

async def text_input_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Route typed messages to the active input handler."""
    if await handle_admin_input(update, context):
        return
    if await handle_passkey_input(update, context):
        return
    if await handle_qp_target_input(update, context):
        return
    if await handle_qp_custom_range_input(update, context):
        return


# ─── Background Jobs ─────────────────────────────────────────────────

async def background_result_checker(app):
    """Background job: check smart bet + ultra picks every 5 minutes."""
    while True:
        try:
            await check_smart_bet_results(app)
            await check_ultra_picks(app)
            await asyncio.sleep(300)  # Check every 5 minutes (faster for ultra mode)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"Background result checker error: {e}")
            await asyncio.sleep(60)


async def send_daily_smart_bet(app):
    """Generate daily Smart Bet pick and send to SUBSCRIBED users only."""
    try:
        pick = await smart_bet_engine.generate_daily_pick()
        if not pick.get("has_pick"):
            logging.info("No Smart Bet pick available today.")
            return

        # Store pick in DB
        from database.db import SmartBetPick
        import json
        async with SessionLocal() as s:
            from sqlalchemy import select as sel
            from datetime import datetime as dt, timezone
            today = dt.now(timezone.utc).strftime("%Y-%m-%d")
            existing = await s.execute(
                sel(SmartBetPick).where(SmartBetPick.date == today))
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

        # Send ONLY to subscribed users
        async with SessionLocal() as s:
            from sqlalchemy import select as sel
            res = await s.execute(
                sel(User).where(User.smart_bet_subscribed == True))
            subscribed_users = res.scalars().all()

        if not subscribed_users:
            logging.info("No subscribers for daily Smart Bet.")
            return

        from telegram import InlineKeyboardButton, InlineKeyboardMarkup
        kb = [
            [InlineKeyboardButton("🔔 Results will come automatically!", callback_data="noop")],
            [InlineKeyboardButton("🤖 Smart Bet", callback_data="menu_smartbet")],
        ]

        sent = 0
        for user in subscribed_users:
            try:
                await app.bot.send_message(
                    chat_id=user.tg_id,
                    text=pick["message"],
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(kb))
                sent += 1
            except Exception:
                pass

        logging.info(f"Daily Smart Bet sent to {sent}/{len(subscribed_users)} subscribers.")
    except Exception as e:
        logging.error(f"Daily Smart Bet error: {e}")


# ─── Startup ──────────────────────────────────────────────────────────

async def post_init(app):
    await init_db()
    logging.info("DB initialized")

    # Background result checker — every 10 minutes
    app.bot_data["result_checker_task"] = asyncio.create_task(background_result_checker(app))

    # APScheduler: daily smart bet at 08:00 UTC + summary at 23:30 UTC
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        send_daily_smart_bet, "cron", hour=8, minute=0,
        args=[app], id="daily_smart_bet")
    scheduler.add_job(
        send_daily_summary, "cron", hour=7, minute=0,
        args=[app], id="daily_summary")
    scheduler.start()
    app.bot_data["scheduler"] = scheduler
    logging.info("Scheduler started — Smart Bet 08:00 UTC, Summary 07:00 UTC")


def main():
    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    # ── Presence (runs for all updates) ──────────────────────────────────
    app.add_handler(
        CallbackQueryHandler(presence_middleware, pattern=r".*"), group=-1)

    # ── Commands ──────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",    start))
    app.add_handler(CommandHandler("botstats", bot_stats_handler))
    app.add_handler(CommandHandler("smartbet", smart_bet_handler))
    app.add_handler(CommandHandler("sendpicks", admin_sendpicks))
    app.add_handler(CommandHandler("admin", admin_menu))

    # ── Terms acceptance ──────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(accept_terms_callback,  pattern=r"^accept_terms$"))

    # ── Admin ─────────────────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(admin_menu,               pattern=r"^admin_menu$"))
    app.add_handler(CallbackQueryHandler(admin_genkey_menu,        pattern=r"^admin_genkey$"))
    app.add_handler(CallbackQueryHandler(admin_gendone,            pattern=r"^admin_gendone_"))
    app.add_handler(CallbackQueryHandler(admin_viewkeys,           pattern=r"^admin_viewkeys$"))
    app.add_handler(CallbackQueryHandler(admin_users,              pattern=r"^admin_users$"))
    app.add_handler(CallbackQueryHandler(admin_togglepremium_prompt, pattern=r"^admin_togglepremium$"))

    # ── Smart Bet ──────────────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(smart_bet_proceed,        pattern=r"^smartbet_proceed$"))
    app.add_handler(CallbackQueryHandler(smart_bet_subscribe,     pattern=r"^smartbet_subscribe$"))
    app.add_handler(CallbackQueryHandler(smart_bet_unsubscribe,   pattern=r"^smartbet_unsubscribe$"))
    app.add_handler(CallbackQueryHandler(ultra_mode_subscribe,    pattern=r"^ultra_subscribe$"))
    app.add_handler(CallbackQueryHandler(ultra_mode_unsubscribe,  pattern=r"^ultra_unsubscribe$"))
    app.add_handler(CallbackQueryHandler(ultra_mode_reset,       pattern=r"^ultra_reset$"))

    # ── Timezone ───────────────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(timezone_set_callback,    pattern=r"^tz_set_"))

    # ── Live ──────────────────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(live_matches_handler,  pattern=r"^live_matches$"))

    # ── Menu routing ──────────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(menu_router,            pattern=r"^menu_"))

    # ── Fixtures pagination ──────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(fixtures_handler, pattern=r"^fixtures_page_"))

    # ── Quick Picks
    app.add_handler(CallbackQueryHandler(quick_picks_menu, pattern=r"^menu_quickpicks$"))
    app.add_handler(CallbackQueryHandler(quick_picks_target_callback, pattern=r"^qp_target_"))
    app.add_handler(CallbackQueryHandler(quick_picks_range_callback, pattern=r"^qp_range_"))

    # ── Noop (page indicators) ───────────────────────────────────────────
    async def noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.callback_query.answer()
    app.add_handler(CallbackQueryHandler(noop, pattern=r"^noop$"))

    # ── Sports ────────────────────────────────────────────────────────────
    # sport_set_all and sport_set_none before sport_set_ to avoid wrong match
    app.add_handler(CallbackQueryHandler(sport_mkt_all_callback,    pattern=r"^sport_mkt_all_"))
    app.add_handler(CallbackQueryHandler(sport_mkt_none_callback,   pattern=r"^sport_mkt_none_"))
    app.add_handler(CallbackQueryHandler(sport_set_callback,        pattern=r"^sport_set_"))
    app.add_handler(CallbackQueryHandler(sport_toggle_callback,     pattern=r"^sport_toggle_"))
    app.add_handler(CallbackQueryHandler(sport_config_callback,     pattern=r"^sport_config_"))
    app.add_handler(CallbackQueryHandler(sport_mkt_toggle_callback, pattern=r"^sport_mkt_toggle_"))

    # ── Text input ────────────────────────────────────────────────────────
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_input_router))

    logging.info("Bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

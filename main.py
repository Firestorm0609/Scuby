"""
main.py — entry point. Wires together handlers, jobs, and lifecycle.
"""

import logging
import os

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from utils import (
    make_http_client,
    load_scan_prices, save_scan_prices,
    load_stats,       save_stats,
    load_alerts,      save_alerts,
    load_monitors,    save_monitors,
    load_watchlist,   save_watchlist,
)
from handlers import (
    handle_start, handle_help, handle_sniff, handle_message,
    handle_leaderboard,
    handle_alert, handle_myalerts,
    handle_monitor, handle_monitoring, handle_unmonitor,
    handle_feed, handle_feeds, handle_unfeed, handle_screener,
    handle_screener_page,
    handle_watch, handle_watching, handle_unwatch,
    handle_stats,
    handle_og_button, handle_versions_button,
    handle_dual_refresh, handle_refresh, handle_refreshpair,
    handle_movers, handle_noop,
    handle_menu_button,
    handle_delalert_button,
    handle_delmonitor_button,
    handle_delfeed_button,
    handle_delwatch_button,
    handle_portfolio_refresh,
    handle_patterns,
    handle_filterstats,
    handle_memory,
    handle_teach,
    handle_clearmemory_button,
)
from handlers_ai_addition import (
    handle_ask,
    handle_clearchat,
    handle_clearchat_button,
    handle_smartwatch,
    handle_smartwatches,
    handle_unsmartwatch,
    handle_delsmartwatch_button,
    handle_autofilter_button,
)
from wallet_tracker import (
    handle_trackwallet,
    handle_wallets,
    handle_untrackwallet,
    handle_delwallet_button,
    wallet_scan_job,
    load_wallets,
    WALLET_POLL_INTERVAL,
)
from reminders import (
    handle_remindme, handle_reminders, handle_unremind,
    handle_delreminder_button, reminder_check_job,
    load_reminders, REMINDER_CHECK_SECS,
)
from jobs import check_alerts_job, monitor_ping_job, watch_scan_job
from feeds import feed_scan_job, load_feeds, save_feeds, FEED_POLL_INTERVAL
from smart_filters import (
    smart_filter_scan_job,
    load_smart_filters, save_smart_filters,
    SMART_FILTER_POLL_INTERVAL,
)
from portfolio import load_portfolios, save_portfolios, daily_briefing_job
from proactive import rug_watchdog_job, whale_alert_job, trending_job
from memory import (
    load_user_memory, load_token_perf, load_filter_scores, load_pattern_cache,
    save_user_memory_async, save_token_perf_async, save_filter_scores_async,
    learning_job,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
OWNER_ID       = int(os.environ["OWNER_ID"]) if os.environ.get("OWNER_ID") else None

ALERT_POLL_INTERVAL   = 60
MONITOR_POLL_INTERVAL = 60
WATCH_POLL_INTERVAL   = 120


# ─── Lifecycle ────────────────────────────────────────────────────────────────

async def post_init(application: Application) -> None:
    application.bot_data["http"]            = make_http_client()
    application.bot_data["versions_cache"]  = {}
    application.bot_data["scan_prices"]     = load_scan_prices()
    application.bot_data["stats"]           = load_stats()
    application.bot_data["alerts"]          = load_alerts()
    application.bot_data["monitors"]        = load_monitors()
    application.bot_data["watchlist"]       = load_watchlist()
    application.bot_data["feeds"]           = load_feeds()
    application.bot_data["smart_filters"]   = load_smart_filters()
    application.bot_data["portfolios"]      = load_portfolios()
    application.bot_data["fired_alerts"]    = set()
    application.bot_data["liq_cache"]       = {}
    application.bot_data["vol_cache"]       = {}
    application.bot_data["whale_fired"]     = set()
    application.bot_data["user_memory"]     = load_user_memory()
    application.bot_data["token_perf"]      = load_token_perf()
    application.bot_data["filter_scores"]   = load_filter_scores()
    application.bot_data["pattern_cache"]   = load_pattern_cache()
    application.bot_data["tracked_wallets"] = load_wallets()
    application.bot_data["global_pool"]     = {"tokens": {}, "contributed_tokens": 0}
    application.bot_data["reminders"]       = load_reminders()

    logger.info(f"Loaded {len(application.bot_data['scan_prices'])} scan price entries.")
    logger.info(f"Loaded {len(application.bot_data['portfolios'])} portfolios.")

    jq = application.job_queue
    jq.run_repeating(check_alerts_job,      interval=ALERT_POLL_INTERVAL,         first=ALERT_POLL_INTERVAL)
    jq.run_repeating(monitor_ping_job,       interval=MONITOR_POLL_INTERVAL,       first=10)
    jq.run_repeating(watch_scan_job,         interval=WATCH_POLL_INTERVAL,         first=30)
    jq.run_repeating(feed_scan_job,          interval=FEED_POLL_INTERVAL,          first=60)
    jq.run_repeating(smart_filter_scan_job,  interval=SMART_FILTER_POLL_INTERVAL,  first=45)
    jq.run_repeating(rug_watchdog_job,       interval=180,                         first=90)
    jq.run_repeating(whale_alert_job,        interval=300,                         first=120)
    # trending_job disabled — feature turned off
    # jq.run_repeating(trending_job,         interval=14400,                       first=300)
    jq.run_repeating(wallet_scan_job,        interval=WALLET_POLL_INTERVAL,        first=90)
    jq.run_daily(daily_briefing_job,         time=__import__("datetime").time(9, 0, 0))
    jq.run_repeating(reminder_check_job,     interval=REMINDER_CHECK_SECS,             first=10)
    jq.run_repeating(learning_job,           interval=1800,                        first=600)


async def post_shutdown(application: Application) -> None:
    import httpx as _httpx
    http: _httpx.AsyncClient = application.bot_data.get("http")
    if http:
        await http.aclose()

    save_scan_prices(application.bot_data.get("scan_prices", {}))
    save_stats(application.bot_data.get("stats", {"chats": set(), "users": set()}))
    save_alerts(application.bot_data.get("alerts", {}))
    save_monitors(application.bot_data.get("monitors", {}))
    save_watchlist(application.bot_data.get("watchlist", {}))
    save_feeds(application.bot_data.get("feeds", {}))
    save_smart_filters(application.bot_data.get("smart_filters", {}))
    save_portfolios(application.bot_data.get("portfolios", {}))

    from memory import TOKEN_PERF_FILE as _TPF, FILTER_SCORES_FILE as _FSF, USER_MEMORY_FILE as _UMF
    import json as _json
    for _path, _key in [(_UMF, "user_memory"), (_TPF, "token_perf"), (_FSF, "filter_scores")]:
        try:
            with open(_path, "w") as _f:
                _json.dump(application.bot_data.get(_key, {}), _f)
        except Exception as _e:
            logger.warning(f"Could not save {_path}: {_e}")
    from reminders import save_reminders
    save_reminders(application.bot_data.get("reminders", {}))
    logger.info("All data saved on shutdown.")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not TELEGRAM_TOKEN:
        raise ValueError("TELEGRAM_BOT_TOKEN not set!")

    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .build()
    )

    # ── Commands ──────────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("start",          handle_start))
    app.add_handler(CommandHandler("help",           handle_help))
    app.add_handler(CommandHandler("sniff",          handle_sniff))
    app.add_handler(CommandHandler("leaderboard",    handle_leaderboard))
    app.add_handler(CommandHandler("alert",          handle_alert))
    app.add_handler(CommandHandler("myalerts",       handle_myalerts))
    app.add_handler(CommandHandler("monitor",        handle_monitor))
    app.add_handler(CommandHandler("monitoring",     handle_monitoring))
    app.add_handler(CommandHandler("unmonitor",      handle_unmonitor))
    app.add_handler(CommandHandler("feed",           handle_feed))
    app.add_handler(CommandHandler("feeds",          handle_feeds))
    app.add_handler(CommandHandler("unfeed",         handle_unfeed))
    app.add_handler(CommandHandler("screener",       handle_screener))
    app.add_handler(CommandHandler("watch",          handle_watch))
    app.add_handler(CommandHandler("watching",       handle_watching))
    app.add_handler(CommandHandler("unwatch",        handle_unwatch))
    app.add_handler(CommandHandler("stats",          handle_stats))
    app.add_handler(CommandHandler("ask",            handle_ask))
    app.add_handler(CommandHandler("clearchat",      handle_clearchat))
    app.add_handler(CommandHandler("smartwatch",     handle_smartwatch))
    app.add_handler(CommandHandler("smartwatches",   handle_smartwatches))
    app.add_handler(CommandHandler("unsmartwatch",   handle_unsmartwatch))
    app.add_handler(CommandHandler("patterns",       handle_patterns))
    app.add_handler(CommandHandler("filterstats",    handle_filterstats))
    app.add_handler(CommandHandler("memory",         handle_memory))
    app.add_handler(CommandHandler("teach",          handle_teach))
    app.add_handler(CommandHandler("trackwallet",    handle_trackwallet))
    app.add_handler(CommandHandler("wallets",        handle_wallets))
    app.add_handler(CommandHandler("untrackwallet",  handle_untrackwallet))
    app.add_handler(CommandHandler("remindme",       handle_remindme))
    app.add_handler(CommandHandler("reminders",      handle_reminders))
    app.add_handler(CommandHandler("unremind",       handle_unremind))

    # ── Message handler ───────────────────────────────────────────────────────
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # ── Callbacks ─────────────────────────────────────────────────────────────
    app.add_handler(CallbackQueryHandler(handle_menu_button,          pattern=r"^menu\|"))
    app.add_handler(CallbackQueryHandler(handle_screener_page,        pattern=r"^scr\|"))
    app.add_handler(CallbackQueryHandler(handle_delalert_button,      pattern=r"^delalert\|"))
    app.add_handler(CallbackQueryHandler(handle_delmonitor_button,    pattern=r"^delmonitor\|"))
    app.add_handler(CallbackQueryHandler(handle_delfeed_button,       pattern=r"^delfeed\|"))
    app.add_handler(CallbackQueryHandler(handle_delwatch_button,      pattern=r"^delwatch\|"))
    app.add_handler(CallbackQueryHandler(handle_og_button,            pattern=r"^og\|"))
    app.add_handler(CallbackQueryHandler(handle_dual_refresh,         pattern=r"^dualrefresh\|"))
    app.add_handler(CallbackQueryHandler(handle_versions_button,      pattern=r"^ver\|"))
    app.add_handler(CallbackQueryHandler(handle_refresh,              pattern=r"^(ro|rv)\|"))
    app.add_handler(CallbackQueryHandler(handle_refreshpair,          pattern=r"^refreshpair\|"))
    app.add_handler(CallbackQueryHandler(handle_movers,               pattern=r"^mov\|"))
    app.add_handler(CallbackQueryHandler(handle_noop,                 pattern=r"^noop$"))
    app.add_handler(CallbackQueryHandler(handle_clearchat_button,     pattern=r"^clearchat\|"))
    app.add_handler(CallbackQueryHandler(handle_delsmartwatch_button, pattern=r"^delsmartwatch\|"))
    app.add_handler(CallbackQueryHandler(handle_portfolio_refresh,    pattern=r"^portfolio\|"))
    app.add_handler(CallbackQueryHandler(handle_clearmemory_button,   pattern=r"^clearmemory\|"))
    app.add_handler(CallbackQueryHandler(handle_delwallet_button,     pattern=r"^delwallet\|"))
    app.add_handler(CallbackQueryHandler(handle_autofilter_button,    pattern=r"^autofilter\|"))
    app.add_handler(CallbackQueryHandler(handle_delreminder_button,   pattern=r"^delreminder\|"))

    logger.info("🐾 Scuby OG Finder is live! Scuby-Duby-Doo!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

"""
handlers.py — all Telegram command handlers and callback query handlers.
Imports from utils.py only (never from jobs.py or main.py).
"""

import asyncio
import logging
import os
import re
import time
from datetime import datetime, timezone

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from feeds import (
    parse_feed_args, feed_label, build_mover_message, run_screener,
    save_feeds_async, VALID_TIMEFRAMES, MAX_FEEDS_PER_CHAT,
    fmt_mcap, fmt_price,
)

from utils import (
    VALID_CA, VALID_TICKER, SOLANA_CA_PATTERN, TICKER_PATTERN,
    CHAT_COOLDOWN, USER_COOLDOWN, CALLBACK_USER_COOLDOWN,
    MAX_ALERTS_PER_USER, MAX_MONITORS_PER_CHAT, MAX_WATCHES_PER_CHAT,
    MONITOR_MIN_INTERVAL, MONITOR_MAX_INTERVAL,
    WATCH_MIN_LIQUIDITY, WATCH_MAX_AGE_HOURS,
    escape_md, safe_float,
    save_stats_async, save_scan_prices_async,
    save_alerts_async, save_monitors_async, save_watchlist_async,
    fetch_current_prices, resolve_symbol_for_ca,
    fetch_rugcheck, parse_risk_badge,
    fetch_pairs_and_cache, _bust_cache, _load_versions,
    find_og, format_og_response, format_version_card, format_movers_report,
    _calc_perf_label, build_leaderboard_text,
    parse_interval, format_monitor_ping,
    expand_keywords, watch_confirm_text,
    main_menu_keyboard, leaderboard_keyboard, og_keyboard,
    versions_keyboard, movers_keyboard, my_alerts_keyboard,
    monitoring_keyboard, watchlist_keyboard,
)

logger = logging.getLogger(__name__)


# ─── Rate limiting & stat tracking ───────────────────────────────────────────

async def _track_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    stats: dict = context.bot_data.setdefault("stats", {"chats": set(), "users": set()})
    changed = False
    if update.effective_chat and update.effective_chat.id not in stats["chats"]:
        stats["chats"].add(update.effective_chat.id)
        changed = True
    if update.effective_user and update.effective_user.id not in stats["users"]:
        stats["users"].add(update.effective_user.id)
        changed = True
    if changed:
        await save_stats_async(stats)


async def _check_rate_limits(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    now = time.time()
    await _track_stats(update, context)

    last_chat = context.chat_data.get("last_call", 0)
    if now - last_chat < CHAT_COOLDOWN:
        logger.info("Rate limited — chat cooldown active.")
        return False
    context.chat_data["last_call"] = now

    last_user = context.user_data.get("last_call", 0)
    remaining = USER_COOLDOWN - (now - last_user)
    if remaining > 0:
        await update.message.reply_text(
            f"Ruh-roh! Scuby needs {int(remaining) + 1}s to catch his breath before the next sniff! \U0001f43e"
        )
        return False
    context.user_data["last_call"] = now
    return True


async def _check_callback_rate_limit(query, context: ContextTypes.DEFAULT_TYPE) -> bool:
    stats: dict = context.bot_data.setdefault("stats", {"chats": set(), "users": set()})
    changed = False
    if query.from_user and query.from_user.id not in stats["users"]:
        stats["users"].add(query.from_user.id)
        changed = True
    if query.message and query.message.chat and query.message.chat.id not in stats["chats"]:
        stats["chats"].add(query.message.chat.id)
        changed = True
    if changed:
        await save_stats_async(stats)

    now       = time.time()
    last      = context.user_data.get("last_callback", 0)
    remaining = CALLBACK_USER_COOLDOWN - (now - last)
    if remaining > 0:
        await query.answer(f"Slow down, raggy! Wait {int(remaining) + 1}s 🐾", show_alert=False)
        return False
    context.user_data["last_callback"] = now
    await query.answer()
    return True


# ─── Dual card keyboard ───────────────────────────────────────────────────────

def dual_keyboard(query_type: str, query_value: str, show_versions: bool = True) -> InlineKeyboardMarkup:
    rows = []
    row1 = [InlineKeyboardButton("🔄 Refresh", callback_data=f"dualrefresh|{query_type}|{query_value}")]
    if show_versions:
        row1.append(InlineKeyboardButton("📋 All versions", callback_data=f"ver|{query_type}|{query_value}|0"))
    rows.append(row1)
    rows.append([InlineKeyboardButton("📊 Movers", callback_data=f"mov|{query_type}|{query_value}")])
    return InlineKeyboardMarkup(rows)


# ─── Dual card formatter ──────────────────────────────────────────────────────

def _token_block(pair: dict, scan_prices: dict, label: str, risk_badge: str = "", live_prices: dict | None = None) -> str:
    e      = escape_md
    ca     = pair.get("baseToken", {}).get("address", "?")
    symbol = pair.get("baseToken", {}).get("symbol", "?")
    name   = pair.get("baseToken", {}).get("name",   "?")
    price  = pair.get("priceUsd") or "?"
    mcap   = safe_float(pair.get("marketCap") or pair.get("fdv") or 0)
    liq    = safe_float((pair.get("liquidity") or {}).get("usd", 0))
    h1     = safe_float((pair.get("priceChange") or {}).get("h1",  0))
    h24    = safe_float((pair.get("priceChange") or {}).get("h24", 0))
    dex_url = f"https://dexscreener.com/solana/{ca}"

    created_ts = pair.get("pairCreatedAt")
    if created_ts:
        created_dt  = datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc)
        date_str    = created_dt.strftime("%b %d, %Y  %H:%M:%S UTC")
        age_days    = (datetime.now(timezone.utc) - created_dt).days
        launch_line = f"📅 {e(date_str)} _\\({e(str(age_days))}d ago\\)_\n"
    else:
        launch_line = ""

    entry = scan_prices.get(ca)
    if entry and entry.get("price", 0) > 0:
        live_price = (live_prices or {}).get(ca, 0) or safe_float(price)
        if live_price > 0:
            multiple   = live_price / entry["price"]
            perf_label = _calc_perf_label(multiple)
            perf_line  = f"📊 Since sniff: *{e(perf_label)}*\n"
        else:
            perf_line = ""
    else:
        perf_line = ""

    from feeds import fmt_mcap
    mcap_str   = fmt_mcap(mcap) if mcap > 0 else "N/A"
    badge_line = f"{risk_badge}\n" if risk_badge else ""

    return (
        f"*{label}*\n"
        f"*{e(symbol)}* — {e(name)}\n"
        f"`{e(ca)}`\n"
        f"{launch_line}"
        f"💰 Price: ${e(str(price))}\n"
        f"💎 MCap: {e(mcap_str)}\n"
        f"💧 Liq: ${e(f'{liq:,.0f}')}\n"
        f"📈 1h {e(f'{h1:+.1f}')}%  \\|  24h {e(f'{h24:+.1f}')}%\n"
        f"{perf_line}"
        f"{badge_line}"
        f"[DexScreener]({dex_url})"
    )


def format_dual_card(
    scanned_pair: dict,
    og_pair: dict | None,
    total_versions: int,
    scanned_risk_badge: str,
    og_risk_badge: str,
    scan_prices: dict,
    live_prices: dict | None = None,
) -> str:
    e          = escape_md
    scanned_ca = scanned_pair.get("baseToken", {}).get("address", "")
    og_ca      = og_pair.get("baseToken", {}).get("address", "") if og_pair else ""
    is_og      = (not og_pair) or (scanned_ca == og_ca)
    versions_note = f"\n\n_🐾 Scuby found {e(str(total_versions))} version\\(s\\) on Solana_"

    if is_og:
        block  = _token_block(scanned_pair, scan_prices, "🏆 OG Token", risk_badge=og_risk_badge, live_prices=live_prices)
        header = f"🐾 *Scuby\\-Dooby\\-Doo\\!* _You posted the OG\\!_\n\n"
        return header + block + versions_note
    else:
        scanned_block = _token_block(scanned_pair, scan_prices, "🔍 Scanned Token", risk_badge=scanned_risk_badge, live_prices=live_prices)
        og_block      = _token_block(og_pair,      scan_prices, "🏆 OG Token",      risk_badge=og_risk_badge,      live_prices=live_prices)
        divider       = "\n\n━━━━━━━━━━━━━━━━━━━\n\n"
        return scanned_block + divider + og_block + versions_note


# ─── Auto-display helper ──────────────────────────────────────────────────────

async def _show_dual_card(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    query_type: str,
    query_value: str,
) -> None:
    http        = context.bot_data["http"]
    scan_prices = context.bot_data.setdefault("scan_prices", {})
    chat_id     = str(update.effective_chat.id) if update.effective_chat else ""
    user        = update.effective_user
    scanned_by  = f"@{user.username}" if user and user.username else (user.first_name if user else "")

    msg = await update.message.reply_text(
        "🐾 Scuby's on the trail\\.\\.\\. sniffing through the blockchain\\!",
        parse_mode="MarkdownV2",
    )

    try:
        pairs = await fetch_pairs_and_cache(
            query_type, query_value, http, context.bot_data,
            scanned_by=scanned_by, chat_id=chat_id,
        )

        if not pairs:
            await msg.edit_text(
                "Ruh\\-roh\\! 🐾 Scuby couldn't find that token on Solana\\.",
                parse_mode="MarkdownV2",
            )
            return

        og = find_og(pairs)
        if not og:
            await msg.edit_text(
                "Jeepers\\! Trail went cold — no launch date found\\. 🔍",
                parse_mode="MarkdownV2",
            )
            return

        scanned_pair = pairs[0]
        if query_type == "ca":
            scanned_pair = next(
                (p for p in pairs if p.get("baseToken", {}).get("address", "") == query_value),
                pairs[0],
            )

        scanned_ca = scanned_pair.get("baseToken", {}).get("address", "")
        og_ca      = og.get("baseToken", {}).get("address", "")
        is_same    = scanned_ca == og_ca

        cas_to_fetch = [scanned_ca] if is_same else list({scanned_ca, og_ca})
        if is_same:
            og_report, live_prices = await asyncio.gather(
                fetch_rugcheck(og_ca, http),
                fetch_current_prices(cas_to_fetch, http),
            )
            scanned_report = og_report
        else:
            scanned_report, og_report, live_prices = await asyncio.gather(
                fetch_rugcheck(scanned_ca, http),
                fetch_rugcheck(og_ca, http),
                fetch_current_prices(cas_to_fetch, http),
            )

        scanned_badge = parse_risk_badge(scanned_report)
        og_badge      = parse_risk_badge(og_report)

        text     = format_dual_card(scanned_pair, og, len(pairs), scanned_badge, og_badge, scan_prices, live_prices)

        # Append GemScore for the OG token
        try:
            from gemscore import calculate_gem_score, format_gem_score
            token_perf = context.bot_data.get("token_perf", {})
            gem_result = calculate_gem_score(og, og_report, token_perf, chat_id)
            og_sym     = og.get("baseToken", {}).get("symbol", "?")
            text      += format_gem_score(gem_result, og_sym, og_ca)
        except Exception as gs_err:
            logger.debug(f"GemScore error: {gs_err}")

        keyboard = dual_keyboard(query_type, query_value, show_versions=len(pairs) > 1)

        await msg.edit_text(
            text,
            parse_mode="MarkdownV2",
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )

    except Exception as ex:
        logger.error(f"_show_dual_card error: {ex}", exc_info=True)
        try:
            await msg.edit_text(
                "Zoinks\\! 🐾 Something went wrong\\. Try again\\!",
                parse_mode="MarkdownV2",
            )
        except Exception:
            pass


# ─── /start ───────────────────────────────────────────────────────────────────

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    await _track_stats(update, context)
    await update.message.reply_text(
        "\U0001f43e *Scuby\\-Dooby\\-Doo\\! Welcome\\!*\n\n"
        "I sniff out the *OG token* on Solana for any ticker or CA, "
        "track movers, and alert you when a token hits your target\\.\n\n"
        "Drop a `$TICKER` or contract address in chat anytime, "
        "or use the menu below\\:",
        parse_mode="MarkdownV2",
        reply_markup=main_menu_keyboard(),
    )


# ─── /help ────────────────────────────────────────────────────────────────────

async def handle_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    await _track_stats(update, context)

    sections = [
        (
            "\U0001f43e *Scuby OG Finder \u2014 Full Help*\n\n"
            "_Drop a `$TICKER` or contract address anywhere in chat and Scuby gets to work\\. "
            "Or use the commands below\\._"
        ),
        (
            "\U0001f50d *Sniffing Tokens*\n"
            "\u2022 Just type `$BONK` or paste a CA in chat\n"
            "\u2022 `/sniff $TICKER` \u2014 e\\.g\\. `/sniff $WIF`\n"
            "\u2022 `/sniff <CA>` \u2014 e\\.g\\. `/sniff 7xKXtg\\.\\.\\.`\n\n"
            "_What you get:_\n"
            "\u2022 The *OG token* \u2014 earliest launched version on Solana\n"
            "\u2022 Price, FDV, liquidity, 1h \\& 24h volume\n"
            "\u2022 Rugcheck rug risk badge \\(HIGH / MEDIUM / LOW\n"
            "  \u2014 rug risk only, not a buy signal\\)\n"
            "\u2022 Performance since Scuby first sniffed it\n"
            "\u2022 DexScreener \\+ X search links\n\n"
            "_Buttons on the result:_\n"
            "\u2022 *See all versions* \u2014 page through every copycat\n"
            "\u2022 *Scan movers* \u2014 rank all versions by performance\n"
            "\u2022 *Refresh* \u2014 pull latest prices \\(your baseline stays fixed\\)"
        ),
        (
            "\U0001f916 *AI Scuby*\n"
            "\u2022 Just talk to Scuby naturally in chat\\!\n"
            "\u2022 `/ask <question>` \u2014 ask anything about crypto\n"
            "\u2022 `/clearchat` \u2014 reset conversation history\n\n"
            "_Examples:_\n"
            "\u2022 `scuby what is mcap?`\n"
            "\u2022 `scuby price of bonk`\n"
            "\u2022 `scuby how do I spot a rug?`"
        ),
        (
            "\U0001f3c6 *Leaderboard*\n"
            "\u2022 `/leaderboard` \u2014 top 10 movers across all tokens\n"
            "  Scuby has sniffed in this chat, ranked by\n"
            "  performance since first scan\\.\n\n"
            "_Tip: the leaderboard is chat\\-specific \u2014 only tokens\n"
            "sniffed here show up\\._"
        ),
        (
            "\U0001f514 *Price Alerts*\n"
            "\u2022 `/alert <CA> <Nx>` \u2014 get a DM when a token\n"
            "  hits your target multiple from its scan price\n"
            "  e\\.g\\. `/alert 7xKXtg\\.\\.\\. 2` fires at 2x\n\n"
            "\u2022 `/myalerts` \u2014 view all your active alerts\n\n"
            "_Max 10 alerts per user\\. You must DM the bot first\\!_"
        ),
        (
            "📡 *MCap Monitor*\n"
            "\u2022 `/monitor <CA> <interval>` \u2014 e\\.g\\. `/monitor 7xKXtg\\.\\.\\. 3m`\n"
            "\u2022 `/monitoring` \u2014 view active monitors\n"
            "\u2022 `/unmonitor <CA or symbol>` \u2014 stop a monitor\n\n"
            "_Min: 1m \\| Max: 60m \\| Up to 5 monitors per chat_"
        ),
        (
            "📡 *Momentum Feed*\n"
            "\u2022 `/feed up 20% 1h` \u2014 post tokens pumping 20%\\+ in 1h\n"
            "\u2022 `/feed down 30% 24h` \u2014 post tokens dumping 30%\\+\n"
            "\u2022 `/feeds` \u2014 view and cancel active feeds\n"
            "\u2022 `/screener up 50% 1h` \u2014 one\\-off manual snapshot\n\n"
            "_Timeframes: `5m` \\| `1h` \\| `6h` \\| `24h`_"
        ),
        (
            "🎯 *Smart Filters*\n"
            "Auto\\-alert when a new token matches your criteria\\.\n\n"
            "\u2022 `/smartwatch mcap between 10k and 20k`\n"
            "\u2022 `/smartwatch new tokens up 50% in 1h`\n"
            "\u2022 `/smartwatch gems under 50k liq over 5k`\n"
            "\u2022 `/smartwatches` \u2014 view active filters\n"
            "\u2022 `/unsmartwatch <label>` \u2014 remove a filter\n\n"
            "_Scans every 2 minutes\\._"
        ),
        (
            "👁 *Token Watcher*\n"
            "\u2022 `/watch dog` \u2014 alert on new dog\\-themed coins\n"
            "\u2022 `/watch cat meme` \u2014 cat or meme coins\n"
            "\u2022 `/watching` \u2014 view active watches\n"
            "\u2022 `/unwatch <keyword>` \u2014 stop a watch\n\n"
            "_Max 10 watches per chat\\._"
        ),
        (
            "_\u26a0\ufe0f Always DYOR, raggy\\. "
            "Scuby is not a financial advisor \u2014 "
            "nothing here is financial advice\\._"
        ),
    ]

    await update.message.reply_text(
        "\n\n".join(sections),
        parse_mode="MarkdownV2",
        reply_markup=main_menu_keyboard(),
    )


# ─── /sniff ───────────────────────────────────────────────────────────────────

async def handle_sniff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not context.args:
        await _track_stats(update, context)
        await update.message.reply_text(
            "Zoinks! Scuby needs something to sniff! \U0001f43e\n\n"
            "Usage:\n"
            "  /sniff $TICKER \u2014 e.g. /sniff $BONK\n"
            "  /sniff <CA>    \u2014 e.g. /sniff 7xKXtg..."
        )
        return
    if not await _check_rate_limits(update, context):
        return

    arg  = context.args[0].strip()
    http: httpx.AsyncClient = context.bot_data["http"]

    if VALID_CA.match(arg):
        query_type  = "ca"
        query_value = arg
    elif re.match(r'^\$?([A-Za-z]{2,10})$', arg):
        query_type  = "ticker"
        query_value = re.match(r'^\$?([A-Za-z]{2,10})$', arg).group(1).upper()
    else:
        await update.message.reply_text(
            "Jinkies! That doesn't look like a valid ticker or CA. \U0001f50d\n"
            "Try /sniff $BONK or /sniff <contract address>"
        )
        return

    await _show_dual_card(update, context, query_type, query_value)


# ─── Message handler ──────────────────────────────────────────────────────────

_TOKEN_INTENT_PATTERN = re.compile(
    r"""
    (?:
        (?:mcap|market\s*cap|price|chart|check|sniff|
           volume|vol|liq|liquidity|fdv|info|data|
           what(?:'s|\s+is)?\s+(?:the\s+)?(?:mcap|price|chart))
        \s+(?:of|for|on)?\s*
        (?:\$?([A-Za-z]{2,10}))
    )
    |
    (?:
        (?:\$?([A-Za-z]{2,10}))
        \s+(?:mcap|market\s*cap|price|chart|volume|liq|info)
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_NOT_TICKERS = {
    "the", "for", "of", "on", "in", "is", "it", "to", "a", "an",
    "me", "my", "us", "do", "get", "can", "and", "or", "but",
    "what", "how", "why", "who", "when", "where", "are", "you",
    "today", "hey", "hi", "hello", "yo", "sup", "good", "bad",
    "mcap", "market", "price", "chart", "check", "sniff",
    "volume", "vol", "liq", "info", "data", "fdv",
}

# Fast-path regex: short conversational messages skip intent classification
# and go straight to scuby_chat — saves one Groq call per message.
_CHAT_FAST_PATH = re.compile(
    r"^\s*(?:what|how|why|explain|tell\s+me|is|are|can\s+you|help|"
    r"hi|hello|hey|thanks|thank\s+you|good|bad|when|who|does|do|"
    r"scuby|ruh|zoinks|yo|sup|lol|haha|ok|okay|sure|nice|cool|"
    r"what'?s|whats|hows|just|any|some)\b",
    re.IGNORECASE,
)

# If any of these action keywords appear, always run intent classification.
_ACTION_KEYWORDS = re.compile(
    r"\b(watch\s+for|alert|remind|monitor|feed|screener|portfolio|"
    r"smart\s*watch|track|cancel|stop|remove|filter|notify|ping\s+me|"
    r"send\s+me|look\s+for|scan\s+for|find\s+pairs|whenever|each\s+time)\b",
    re.IGNORECASE,
)


def _extract_bare_ticker(text: str) -> str | None:
    m = _TOKEN_INTENT_PATTERN.search(text)
    if m:
        ticker = m.group(1) or m.group(2)
        if ticker and ticker.lower() not in _NOT_TICKERS:
            return ticker.upper()
    return None


def _is_scuby_addressed(text: str, bot_username: str) -> bool:
    if bot_username and f"@{bot_username}".lower() in text.lower():
        return True
    if re.search(r'\bscuby\b', text, re.IGNORECASE):
        return True
    return False


async def _send_reply(update: Update, text: str) -> None:
    """Send a reply, trying MarkdownV2 then plain text fallback."""
    try:
        await update.message.reply_text(escape_md(text), parse_mode="MarkdownV2")
    except Exception:
        try:
            await update.message.reply_text(text)
        except Exception as ex:
            logger.warning(f"_send_reply failed: {ex}")


async def _execute_intent(update: Update, context: ContextTypes.DEFAULT_TYPE, intent_data: dict, clean: str) -> None:
    """
    Execute an AI-classified intent — create filters, alerts, monitors, etc.
    This is where natural language becomes real bot actions.
    """
    from ai import scuby_chat, parse_smart_filter, understand_intent
    from smart_filters import save_smart_filters_async, MAX_SMART_FILTERS_PER_CHAT
    from feeds import parse_feed_args, save_feeds_async, VALID_TIMEFRAMES

    intent  = intent_data.get("intent", "chat")
    params  = intent_data.get("params", {})
    reply   = intent_data.get("scuby_reply")
    user_id = update.effective_user.id if update.effective_user else 0
    chat_id = str(update.effective_chat.id) if update.effective_chat else ""
    http: httpx.AsyncClient = context.bot_data["http"]

    # ── smart_filter ──────────────────────────────────────────────────────────
    if intent == "smart_filter":
        smart_filters = context.bot_data.setdefault("smart_filters", {})
        chat_sf       = smart_filters.setdefault(chat_id, [])

        if len(chat_sf) >= MAX_SMART_FILTERS_PER_CHAT:
            await _send_reply(update, f"Ruh-roh! 🐾 Already have {MAX_SMART_FILTERS_PER_CHAT} smart filters running here! Use /smartwatches to cancel some first.")
            return

        # Build filter from intent params
        flt = {
            "mcap_min":        params.get("mcap_min"),
            "mcap_max":        params.get("mcap_max"),
            "liq_min":         params.get("liq_min"),
            "liq_max":         params.get("liq_max"),
            "age_max_minutes": params.get("age_max_minutes", 60),
            "pct_change_min":  params.get("pct_change_min"),
            "pct_change_max":  params.get("pct_change_max"),
            "vol_min_1h":      params.get("vol_min_1h"),
            "label":           params.get("label", clean[:50]),
        }

        import time as _time, os as _os
        flt["id"]       = f"{int(_time.time() * 1000)}_{_os.urandom(3).hex()}"
        flt["raw"]      = clean
        flt["added_ts"] = _time.time()
        flt["seen_cas"] = []
        chat_sf.append(flt)
        await save_smart_filters_async(smart_filters)

        await _send_reply(update, reply or f"Scuby-Duby-Doo! 🎯 Smart filter set! I'll send you every new Solana pair matching your criteria. Use /smartwatches to manage. 🐾")
        return

    # ── feed ─────────────────────────────────────────────────────────────────
    if intent == "feed":
        direction = params.get("direction", "up")
        threshold = float(params.get("threshold", 20))
        timeframe = params.get("timeframe", "1h")

        if timeframe not in VALID_TIMEFRAMES:
            timeframe = "1h"

        feeds: dict = context.bot_data.setdefault("feeds", {})
        chat_feeds  = feeds.setdefault(chat_id, [])

        from feeds import MAX_FEEDS_PER_CHAT, feed_label as _feed_label
        if len(chat_feeds) >= MAX_FEEDS_PER_CHAT:
            await _send_reply(update, f"Ruh-roh! 🐾 Already have {MAX_FEEDS_PER_CHAT} feeds! Use /feeds to cancel some first.")
            return

        existing = next((f for f in chat_feeds if f["direction"] == direction and f["timeframe"] == timeframe), None)
        import time as _time, os as _os
        if existing:
            existing["threshold"] = threshold
            existing["seen_cas"]  = []
        else:
            chat_feeds.append({
                "id":        f"{int(_time.time() * 1000)}_{_os.urandom(3).hex()}",
                "direction": direction,
                "threshold": threshold,
                "timeframe": timeframe,
                "added_ts":  _time.time(),
                "seen_cas":  [],
            })
        await save_feeds_async(feeds)
        await _send_reply(update, reply or f"📡 Feed set! Scuby will post tokens {'up' if direction == 'up' else 'down'} {threshold:.0f}%+ in {timeframe} every 10 minutes! 🐾")
        return

    # ── keyword_watch ─────────────────────────────────────────────────────────
    if intent == "keyword_watch":
        keywords = params.get("keywords", [])
        if not keywords and clean:
            keywords = [clean.strip()]

        from utils import expand_keywords, save_watchlist_async, MAX_WATCHES_PER_CHAT
        raw_kw = " ".join(keywords)
        terms, patterns = expand_keywords(raw_kw)
        if not terms:
            terms    = keywords
            patterns = keywords

        watchlist:  dict = context.bot_data.setdefault("watchlist", {})
        chat_watches     = watchlist.setdefault(chat_id, [])

        if len(chat_watches) >= MAX_WATCHES_PER_CHAT:
            await _send_reply(update, f"Ruh-roh! 🐾 Already have {MAX_WATCHES_PER_CHAT} watches! Use /watching to cancel some first.")
            return

        import time as _time, os as _os
        chat_watches.append({
            "id":       f"{int(_time.time() * 1000)}_{_os.urandom(4).hex()}",
            "terms":    terms,
            "patterns": patterns,
            "raw":      raw_kw,
            "added_ts": _time.time(),
            "seen_cas": [],
        })
        await save_watchlist_async(watchlist)
        await _send_reply(update, reply or f"Ruh-roh! 🐾 Scuby's nose is tuned to {', '.join(f'#{t}' for t in terms)}! I'll bark the moment a new one launches! Use /watching to manage.")
        return

    # ── monitor ───────────────────────────────────────────────────────────────
    if intent == "monitor":
        ticker_or_ca  = params.get("ticker_or_ca", "")
        interval_mins = int(params.get("interval_minutes", 5))
        interval_mins = max(1, min(60, interval_mins))
        interval_secs = interval_mins * 60

        if not ticker_or_ca:
            await _send_reply(update, "Ruh-roh! 🐾 Scuby needs a token name or CA to monitor! Try: 'monitor BONK every 5 minutes'")
            return

        from utils import resolve_symbol_for_ca, save_monitors_async, MAX_MONITORS_PER_CHAT, VALID_CA as _VALID_CA
        monitors: dict  = context.bot_data.setdefault("monitors", {})
        chat_monitors   = monitors.setdefault(chat_id, [])

        if len(chat_monitors) >= MAX_MONITORS_PER_CHAT:
            await _send_reply(update, f"Ruh-roh! 🐾 Already monitoring {MAX_MONITORS_PER_CHAT} tokens! Use /monitoring to cancel some first.")
            return

        if _VALID_CA.match(ticker_or_ca):
            ca     = ticker_or_ca
            symbol = await resolve_symbol_for_ca(ca, http) or ca[:8]
        else:
            ticker = ticker_or_ca.upper().lstrip("$")
            from utils import get_all_by_ticker
            pairs  = await get_all_by_ticker(ticker, http)
            if not pairs:
                await _send_reply(update, f"Ruh-roh! 🐾 Scuby couldn't find ${ticker} on Solana!")
                return
            from utils import find_og
            og     = find_og(pairs) or pairs[0]
            ca     = og.get("baseToken", {}).get("address", "")
            symbol = og.get("baseToken", {}).get("symbol", ticker)

        existing = next((m for m in chat_monitors if m["ca"] == ca), None)
        import time as _time, os as _os
        if existing:
            existing["interval_secs"] = interval_secs
            existing["last_sent_ts"]  = 0
        else:
            chat_monitors.append({
                "id":            f"{int(_time.time() * 1000)}_{_os.urandom(3).hex()}",
                "ca":            ca,
                "symbol":        symbol,
                "interval_secs": interval_secs,
                "chat_id":       chat_id,
                "added_ts":      _time.time(),
                "last_sent_ts":  0,
            })
        await save_monitors_async(monitors)
        await _send_reply(update, reply or f"📡 Zoinks! Scuby is now watching {symbol} every {interval_mins}m, Raggy! 🐾 Use /monitoring to manage.")
        return

    # ── price_alert ───────────────────────────────────────────────────────────
    if intent == "price_alert":
        ticker_or_ca  = params.get("ticker_or_ca", "")
        target_multiple = float(params.get("target_multiple", 2))

        if not ticker_or_ca or target_multiple <= 1:
            await _send_reply(update, "Ruh-roh! 🐾 Scuby needs a token and a target! Try: 'alert me when BONK hits 2x'")
            return

        from utils import fetch_current_prices, resolve_symbol_for_ca, save_alerts_async, MAX_ALERTS_PER_USER, VALID_CA as _VALID_CA
        alerts: dict = context.bot_data.setdefault("alerts", {})
        user_alert_count = sum(1 for v in alerts.values() if v.get("user_id") == user_id)
        if user_alert_count >= MAX_ALERTS_PER_USER:
            await _send_reply(update, f"Ruh-roh! 🐾 You already have {MAX_ALERTS_PER_USER} alerts! Use /myalerts to cancel some.")
            return

        if _VALID_CA.match(ticker_or_ca):
            ca     = ticker_or_ca
            symbol = await resolve_symbol_for_ca(ca, http) or ca[:8]
        else:
            ticker = ticker_or_ca.upper().lstrip("$")
            from utils import get_all_by_ticker
            pairs  = await get_all_by_ticker(ticker, http)
            if not pairs:
                await _send_reply(update, f"Ruh-roh! 🐾 Scuby couldn't find ${ticker} on Solana!")
                return
            from utils import find_og
            og     = find_og(pairs) or pairs[0]
            ca     = og.get("baseToken", {}).get("address", "")
            symbol = og.get("baseToken", {}).get("symbol", ticker)

        prices   = await fetch_current_prices([ca], http)
        baseline = prices.get(ca, 0)
        if baseline <= 0:
            await _send_reply(update, f"Ruh-roh! 🐾 Scuby couldn't get a price for {symbol} right now. Try again in a moment!")
            return

        import time as _time
        alert_key = f"{user_id}:{ca}"
        alerts[alert_key] = {
            "user_id":         user_id,
            "ca":              ca,
            "symbol":          symbol,
            "target_multiple": target_multiple,
            "baseline_price":  baseline,
            "created_ts":      _time.time(),
        }
        await save_alerts_async(alerts)
        await _send_reply(update, reply or f"🔔 Zoinks! Alert set! Scuby will DM you when {symbol} hits {target_multiple:.1f}x from ${baseline:.6g}! Make sure you've started a DM with me first! 🐾")
        return

    # ── sniff ─────────────────────────────────────────────────────────────────
    if intent == "sniff":
        ticker_or_ca = params.get("ticker_or_ca", "")
        if not ticker_or_ca:
            await _send_reply(update, reply or "Ruh-roh! 🐾 Which token should Scuby sniff?")
            return
        if reply:
            await _send_reply(update, reply)
        from utils import VALID_CA as _VALID_CA
        if _VALID_CA.match(ticker_or_ca):
            await _show_dual_card(update, context, "ca", ticker_or_ca)
        else:
            await _show_dual_card(update, context, "ticker", ticker_or_ca.upper().lstrip("$"))
        return

    # ── screener ─────────────────────────────────────────────────────────────
    if intent == "screener":
        direction = params.get("direction", "up")
        threshold = float(params.get("threshold", 20))
        timeframe = params.get("timeframe", "1h")
        if timeframe not in VALID_TIMEFRAMES:
            timeframe = "1h"

        if reply:
            await _send_reply(update, reply)

        from feeds import run_screener
        arrow = "▲" if direction == "up" else "▼"
        msg = await update.message.reply_text(
            f"🔍 Scanning for tokens {arrow} {threshold:.0f}%+ in {timeframe}... hold on, Raggy! 🐾"
        )
        try:
            movers = await run_screener(http, direction, threshold, timeframe)
            if not movers:
                await msg.edit_text(f"🐾 No tokens found moving {arrow} {threshold:.0f}%+ in {timeframe} right now. Try a lower threshold!")
                return
            _store_screener_cache(context.bot_data, direction, threshold, timeframe, movers)
            text, keyboard = format_screener_page(movers, 0, direction, threshold, timeframe)
            await msg.edit_text(text, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=keyboard)
        except Exception as ex:
            logger.error(f"_execute_intent screener: {ex}", exc_info=True)
            await msg.edit_text("Zoinks! 🐾 Scan failed. Try again!")
        return

    # ── cancel ────────────────────────────────────────────────────────────────
    if intent == "cancel":
        target     = params.get("target", "all")
        identifier = (params.get("identifier") or "").lower()
        cancelled  = []

        if target in ("smart_filter", "all"):
            sf = context.bot_data.get("smart_filters", {})
            before = len(sf.get(chat_id, []))
            if identifier:
                sf[chat_id] = [f for f in sf.get(chat_id, []) if identifier not in f.get("label","").lower() and identifier not in f.get("raw","").lower()]
            else:
                sf[chat_id] = []
            removed = before - len(sf.get(chat_id, []))
            if removed:
                cancelled.append(f"{removed} smart filter(s)")
            from smart_filters import save_smart_filters_async
            await save_smart_filters_async(sf)

        if target in ("watch", "all"):
            from utils import save_watchlist_async
            wl = context.bot_data.get("watchlist", {})
            before = len(wl.get(chat_id, []))
            if identifier:
                wl[chat_id] = [w for w in wl.get(chat_id, []) if not any(identifier in t.lower() for t in w["terms"])]
            else:
                wl[chat_id] = []
            removed = before - len(wl.get(chat_id, []))
            if removed:
                cancelled.append(f"{removed} keyword watch(es)")
            await save_watchlist_async(wl)

        if target in ("monitor", "all"):
            from utils import save_monitors_async
            mo = context.bot_data.get("monitors", {})
            before = len(mo.get(chat_id, []))
            if identifier:
                mo[chat_id] = [m for m in mo.get(chat_id, []) if identifier.upper() != m.get("symbol","").upper() and identifier not in m["ca"].lower()]
            else:
                mo[chat_id] = []
            removed = before - len(mo.get(chat_id, []))
            if removed:
                cancelled.append(f"{removed} monitor(s)")
            await save_monitors_async(mo)

        if target in ("feed", "all"):
            fe = context.bot_data.get("feeds", {})
            before = len(fe.get(chat_id, []))
            if identifier:
                fe[chat_id] = [f for f in fe.get(chat_id, []) if f["direction"] != identifier and f["timeframe"] != identifier]
            else:
                fe[chat_id] = []
            removed = before - len(fe.get(chat_id, []))
            if removed:
                cancelled.append(f"{removed} feed(s)")
            await save_feeds_async(fe)

        if target in ("alert", "all"):
            from utils import save_alerts_async
            al = context.bot_data.get("alerts", {})
            before = sum(1 for v in al.values() if v.get("user_id") == user_id)
            if identifier:
                to_del = [k for k, v in al.items() if v.get("user_id") == user_id and identifier.upper() in v.get("symbol","").upper()]
            else:
                to_del = [k for k, v in al.items() if v.get("user_id") == user_id]
            for k in to_del:
                del al[k]
            removed = before - sum(1 for v in al.values() if v.get("user_id") == user_id)
            if removed:
                cancelled.append(f"{removed} alert(s)")
            await save_alerts_async(al)

        if cancelled:
            await _send_reply(update, f"✅ Scuby-Duby-Doo! Cancelled: {', '.join(cancelled)}. 🐾")
        else:
            await _send_reply(update, f"Ruh-roh! 🐾 Nothing found to cancel{(' matching ' + identifier) if identifier else ''}. Use /smartwatches, /watching, /monitoring, or /feeds to see what's active.")
        return

    # ── status ────────────────────────────────────────────────────────────────
    if intent == "status":
        target = params.get("target", "all")
        lines  = ["🐾 *Scuby's active tasks:*\n"]

        if target in ("smart_filters", "all"):
            sf = context.bot_data.get("smart_filters", {}).get(chat_id, [])
            lines.append(f"🎯 Smart filters: *{len(sf)}*")
            for f in sf[:3]:
                lines.append(f"  • {f.get('label','?')}")

        if target in ("watches", "all"):
            wl = context.bot_data.get("watchlist", {}).get(chat_id, [])
            lines.append(f"👁 Keyword watches: *{len(wl)}*")
            for w in wl[:3]:
                lines.append(f"  • {', '.join('#'+t for t in w['terms'][:3])}")

        if target in ("monitors", "all"):
            mo = context.bot_data.get("monitors", {}).get(chat_id, [])
            lines.append(f"📡 Monitors: *{len(mo)}*")
            for m in mo[:3]:
                lines.append(f"  • {m.get('symbol','?')} every {m['interval_secs']//60}m")

        if target in ("feeds", "all"):
            fe = context.bot_data.get("feeds", {}).get(chat_id, [])
            lines.append(f"📶 Feeds: *{len(fe)}*")
            for f in fe[:3]:
                arrow = "▲" if f["direction"] == "up" else "▼"
                thr   = f"{f['threshold']:.0f}"
                lines.append(f"  • {arrow} {thr}%+ in {f['timeframe']}")

        if target in ("alerts", "all"):
            al = context.bot_data.get("alerts", {})
            user_alerts = [v for v in al.values() if v.get("user_id") == user_id]
            lines.append(f"🔔 Price alerts: *{len(user_alerts)}*")
            for a in user_alerts[:3]:
                lines.append(f"  • {a.get('symbol','?')} @ {a['target_multiple']:.1f}x")

        lines.append("\n_Use /smartwatches /watching /monitoring /feeds /myalerts for full lists_")
        await _send_reply(update, "\n".join(lines))
        return


    # ── portfolio ─────────────────────────────────────────────────────────────
    if intent == "portfolio":
        from portfolio import build_portfolio_snapshot, portfolio_keyboard
        user_id = update.effective_user.id if update.effective_user else 0
        msg = await update.message.reply_text("🐾 Fetching your bag, Raggy\.\.\.", parse_mode="MarkdownV2")
        try:
            text = await build_portfolio_snapshot(user_id, http, context.bot_data)
            await msg.edit_text(text, parse_mode="MarkdownV2",
                                disable_web_page_preview=True,
                                reply_markup=portfolio_keyboard(user_id))
        except Exception as ex:
            logger.error(f"portfolio intent: {ex}", exc_info=True)
            await msg.edit_text("Zoinks! 🐾 Couldn't load your portfolio. Try again!")
        return

    # ── add_to_portfolio ──────────────────────────────────────────────────────
    if intent == "add_to_portfolio":
        ticker_or_ca = params.get("ticker_or_ca", "")
        qty          = safe_float(params.get("qty", 0))
        avg_price    = safe_float(params.get("avg_price", 0))

        if not ticker_or_ca or qty <= 0:
            await _send_reply(update, "Ruh-roh! 🐾 Scuby needs a token and quantity! Try: 'add 1000 BONK to my portfolio'")
            return

        from utils import VALID_CA as _VALID_CA, find_og
        if _VALID_CA.match(ticker_or_ca):
            ca     = ticker_or_ca
            symbol = await resolve_symbol_for_ca(ca, http) or ca[:8]
        else:
            ticker = ticker_or_ca.upper().lstrip("$")
            from utils import get_all_by_ticker
            pairs  = await get_all_by_ticker(ticker, http)
            if not pairs:
                await _send_reply(update, f"Ruh-roh! 🐾 Couldn't find ${ticker} on Solana!")
                return
            og     = find_og(pairs) or pairs[0]
            ca     = og.get("baseToken", {}).get("address", "")
            symbol = og.get("baseToken", {}).get("symbol", ticker)

        import time as _time
        portfolios: dict = context.bot_data.setdefault("portfolios", {})
        user_portfolio   = portfolios.setdefault(str(user_id), {})
        user_portfolio[symbol] = {
            "ca":        ca,
            "symbol":    symbol,
            "qty":       qty,
            "avg_price": avg_price,
            "added_ts":  _time.time(),
        }
        from portfolio import save_portfolios_async
        await save_portfolios_async(portfolios)

        avg_str = f" at avg ${avg_price:.6g}" if avg_price > 0 else ""
        await _send_reply(update, f"Scuby-Duby-Doo! ✅ Added {qty:,.2f} {symbol}{avg_str} to your portfolio! 🐾 Say 'show my portfolio' anytime to check your bag!")
        return

    # ── remove_from_portfolio ─────────────────────────────────────────────────
    if intent == "remove_from_portfolio":
        ticker_or_ca = params.get("ticker_or_ca", "").upper().lstrip("$")
        if not ticker_or_ca:
            await _send_reply(update, "Ruh-roh! 🐾 Which token should Scuby remove?")
            return

        portfolios: dict = context.bot_data.setdefault("portfolios", {})
        user_portfolio   = portfolios.get(str(user_id), {})

        # Match by symbol or CA fragment
        to_remove = [k for k, v in user_portfolio.items()
                     if k.upper() == ticker_or_ca or v.get("ca","").startswith(ticker_or_ca.lower())]
        if not to_remove:
            await _send_reply(update, f"Ruh-roh! 🐾 {ticker_or_ca} isn't in your portfolio! Say 'show my portfolio' to see what's there.")
            return

        for k in to_remove:
            del user_portfolio[k]
        from portfolio import save_portfolios_async
        await save_portfolios_async(portfolios)
        await _send_reply(update, f"✅ Removed {', '.join(to_remove)} from your portfolio, Raggy! 🐾")
        return

    # ── chat (fallback) ───────────────────────────────────────────────────────
    user_memory = context.bot_data.get("user_memory", {})
    reply_text  = await scuby_chat(user_id, clean, user_memory=user_memory)
    from memory import save_user_memory_async
    await save_user_memory_async(user_memory)
    await _send_reply(update, reply_text)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text         = update.message.text
    chat_type    = update.effective_chat.type if update.effective_chat else "private"
    bot_username = context.bot.username or ""

    # ── Group chat gate ───────────────────────────────────────────────────────
    # In groups Scuby only wakes up when explicitly summoned (@mention, the
    # word "scuby", or a direct reply to one of his messages).
    # Raw token mentions are intentionally ignored to avoid cluttering chats.
    is_private = (chat_type == "private")
    if not is_private:
        is_reply_to_bot = (
            update.message.reply_to_message is not None
            and update.message.reply_to_message.from_user is not None
            and update.message.reply_to_message.from_user.username == bot_username
        )
        summoned = _is_scuby_addressed(text, bot_username) or is_reply_to_bot
        if not summoned:
            return

    if not await _check_rate_limits(update, context):
        return

    await _track_stats(update, context)

    # Strip "scuby" and @mention before processing
    clean = re.sub(rf"@{re.escape(bot_username)}", "", text, flags=re.IGNORECASE) if bot_username else text
    clean = re.sub(r'\bscuby\b', "", clean, flags=re.IGNORECASE).strip()

    # If the user only said "@scuby" with no other text, check whether they
    # replied to someone's message. If yes, carry on — the replied content is
    # the subject. If there's truly nothing to work with, exit.
    if not clean:
        if not (update.message.reply_to_message and
                (update.message.reply_to_message.text or
                 update.message.reply_to_message.caption)):
            return
        # clean stays empty here; ai_input will be built from replied_text below

    http: httpx.AsyncClient = context.bot_data["http"]

    # ── Fast paths: auto-sniff on raw CA / $TICKER / bare ticker ─────────────
    # Private chat  → fire immediately on any token mention (unchanged).
    # Group chat    → user already summoned Scuby; only auto-sniff when the
    #                 cleaned message is *purely* a token with no other words.
    #                 Mixed messages go through the AI router so Scuby can
    #                 reply naturally instead of dumping an unsolicited card.

    def _is_token_only(s: str) -> bool:
        return bool(VALID_CA.match(s.strip()) or re.match(r'^\$?[A-Za-z]{2,10}$', s.strip()))

    # Fast path 1: explicit CA
    ca_match = SOLANA_CA_PATTERN.search(clean)
    if ca_match and (is_private or _is_token_only(clean)):
        await _show_dual_card(update, context, "ca", ca_match.group(0))
        return

    # Fast path 2: explicit $TICKER
    tick_match = TICKER_PATTERN.search(clean)
    if tick_match and (is_private or _is_token_only(clean)):
        await _show_dual_card(update, context, "ticker", tick_match.group(1).upper())
        return

    # Fast path 3: bare ticker in natural phrasing (private chat only)
    if is_private:
        bare = _extract_bare_ticker(clean)
        if bare:
            await _show_dual_card(update, context, "ticker", bare)
            return

    # ── AI intent routing ─────────────────────────────────────────────────────
    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    except Exception:
        pass

    try:
        from ai import understand_intent, scuby_chat
    except ImportError:
        await update.message.reply_text("Ruh-roh! 🐾 ai.py is missing from the project folder!")
        return

    # Fast-path: short conversational messages skip intent classification
    # entirely — saves a full Groq round-trip for the most common case.
    word_count = len(clean.split())
    is_conversational = (
        word_count <= 10
        and _CHAT_FAST_PATH.match(clean)
        and not _ACTION_KEYWORDS.search(clean)
    )
    if is_conversational:
        user_memory = context.bot_data.get("user_memory", {})
        reply_text  = await scuby_chat(
            update.effective_user.id, clean, user_memory=user_memory
        )
        try:
            from memory import save_user_memory_async
            await save_user_memory_async(user_memory)
        except Exception:
            pass
        await _send_reply(update, reply_text)
        return

    intent_data = await understand_intent(clean)
    await _execute_intent(update, context, intent_data, clean)


# ─── /leaderboard ─────────────────────────────────────────────────────────────

async def handle_memory(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show what Scuby remembers about the user."""
    if not update.message or not update.effective_user:
        return
    await _track_stats(update, context)

    user_id     = update.effective_user.id
    user_memory = context.bot_data.get("user_memory", {})

    try:
        from memory import get_user_profile, infer_preferences
        profile = get_user_profile(user_memory, user_id)
        prefs   = infer_preferences(user_memory, user_id)
        e       = escape_md

        first_seen = profile.get("first_seen", 0)
        last_seen  = profile.get("last_seen",  0)
        msg_count  = profile.get("message_count", 0)
        conv_count = len(profile.get("conversation", []))
        search_hist = profile.get("search_history", [])
        name        = profile.get("name") or update.effective_user.first_name or "Unknown"

        # Top searched tokens
        from collections import Counter
        tickers     = [s["ticker"] for s in search_hist[-100:]]
        top_tickers = Counter(tickers).most_common(5)

        scuby_name = e(name)
        first_dt    = datetime.fromtimestamp(first_seen, tz=timezone.utc).strftime("%b %d, %Y")
        lines = [
            f"\U0001f43e *Scuby's Memory \u2014 {scuby_name}*\n",
            f"\U0001f464 First seen: {e(first_dt)}",
            f"\U0001f4ac Messages: {msg_count} \\| Conversation turns: {conv_count}",
        ]

        if prefs.get("themes"):
            lines.append(f"🎯 Favourite themes: {e(', '.join(prefs['themes']))}")
        if prefs.get("risk_style"):
            style_map = {"degen": "🎲 Degen", "cautious": "🛡️ Cautious", "balanced": "⚖️ Balanced"}
            lines.append(f"📊 Trading style: {e(style_map.get(prefs['risk_style'], prefs['risk_style']))}")
        if top_tickers:
            tk_str = ", ".join(f"${t} \({c}x\)" for t, c in top_tickers)
            lines.append(f"🔍 Most searched: {tk_str}")
        if prefs.get("fav_tokens"):
            lines.append(f"⭐ Top tokens: {e(', '.join('$'+t for t in prefs['fav_tokens'][:5]))}")

        lines.append("_Use /teach to tell Scuby things about yourself._")
        lines.append("_Use /clearchat to reset conversation history\._")

        await update.message.reply_text(
            "\n".join(lines),
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🧹 Clear memory", callback_data=f"clearmemory|{user_id}"),
                InlineKeyboardButton("🏠 Menu",         callback_data="menu|home"),
            ]])
        )
    except Exception as ex:
        logger.error(f"handle_memory: {ex}", exc_info=True)
        await update.message.reply_text("Zoinks! 🐾 Couldn't load memory. Try again!")


async def handle_teach(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Explicitly teach Scuby something about the user.
    /teach I prefer dog coins under 20k mcap
    /teach my name is Alex
    /teach I am a degen trader
    """
    if not update.message or not update.effective_user:
        return
    if not await _check_rate_limits(update, context):
        return

    raw = " ".join(context.args or []).strip()
    if not raw:
        await update.message.reply_text(
            "\U0001f43e Tell Scuby something about yourself, Raggy!\n\n"
            "Examples:\n"
            "  /teach my name is Alex\n"
            "  /teach I prefer dog coins\n"
            "  /teach I am a degen trader\n"
            "  /teach I only trade under 20k mcap"
        )
        return

    user_id     = update.effective_user.id
    user_memory = context.bot_data.get("user_memory", {})

    try:
        from memory import get_user_profile, push_conversation, save_user_memory_async
        from ai import scuby_chat

        profile = get_user_profile(user_memory, user_id)

        # Check for name teaching
        import re as _re
        name_match = _re.search(r"(?:my name is|call me|i am|i'm)\s+([A-Za-z]+)", raw, _re.IGNORECASE)
        if name_match:
            profile["name"] = name_match.group(1).capitalize()

        # Check for risk style
        if any(w in raw.lower() for w in ["degen", "yolo", "ape", "100x", "high risk"]):
            profile["preferences"]["risk_style"] = "degen"
        elif any(w in raw.lower() for w in ["safe", "careful", "low risk", "cautious"]):
            profile["preferences"]["risk_style"] = "cautious"

        # Check for theme preferences
        theme_map = {
            "dog": ["dog", "doge", "shiba", "inu"],
            "cat": ["cat", "kitty"],
            "ai":  ["ai", "artificial intelligence", "gpt"],
            "pepe": ["pepe", "frog"],
            "meme": ["meme", "memecoin"],
        }
        for theme, keywords in theme_map.items():
            if any(kw in raw.lower() for kw in keywords):
                themes = profile["preferences"].setdefault("themes", [])
                if theme not in themes:
                    themes.insert(0, theme)
                    themes[:] = themes[:5]

        # Push as a memory note and get Scuby's acknowledgement
        push_conversation(user_memory, user_id, "user", f"[User taught me]: {raw}")
        reply = await scuby_chat(user_id, f"The user just told you: {raw}. Acknowledge this warmly in character, tell them you'll remember it, and confirm what you learned.", user_memory=user_memory)
        await save_user_memory_async(user_memory)

        await update.message.reply_text(reply)

    except Exception as ex:
        logger.error(f"handle_teach: {ex}", exc_info=True)
        await update.message.reply_text("Zoinks! 🐾 Scuby's brain glitched. Try again!")


async def handle_clearmemory_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clear a user's full memory profile."""
    query = update.callback_query
    await query.answer()
    parts = query.data.split("|", 1)
    if len(parts) != 2:
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        return

    user_memory = context.bot_data.get("user_memory", {})
    if str(user_id) in user_memory:
        del user_memory[str(user_id)]
        from memory import save_user_memory_async
        await save_user_memory_async(user_memory)

    from ai import clear_history
    clear_history(user_id)

    try:
        await query.edit_message_text(
            "🧹 Done, Raggy! Scuby wiped the slate completely clean. "
            "Fresh start — Scuby doesn't remember a thing! 🐾"
        )
    except Exception:
        pass


async def handle_patterns(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show alpha patterns learned from sniff history — local + global pooled."""
    if not update.message or not update.effective_chat:
        return
    await _track_stats(update, context)
    chat_id    = str(update.effective_chat.id)
    token_perf = context.bot_data.get("token_perf", {})
    global_pool = context.bot_data.get("global_pool", {})
    try:
        from memory import (
            analyse_patterns, patterns_to_text,
            analyse_global_patterns, patterns_to_text_merged,
        )
        local_patterns  = analyse_patterns(token_perf, chat_id)
        global_patterns = analyse_global_patterns(global_pool)
        global_count    = global_pool.get("contributed_tokens", 0)

        # Use merged view if we have global data, otherwise local only
        if global_patterns or local_patterns:
            text = patterns_to_text_merged(local_patterns, global_patterns, global_count)
        else:
            text = ""

        if not text:
            count = len(token_perf)
            global_str = f" | {global_count} in global pool" if global_count else ""
            global_str2 = f" | {global_count} in global pool" if global_count else ""
            await update.message.reply_text(
                "\U0001f9e0 Scuby's still learning, Raggy!\n\n"
                "I need more sniff data to detect patterns. "
                "Keep sniffing tokens and I'll find what works! \U0001f43e\n\n"
                f"Currently tracking {count} tokens{global_str2}.",
            )
        else:
            await update.message.reply_text(text, parse_mode="MarkdownV2",
                                             disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"handle_patterns: {e}", exc_info=True)
        await update.message.reply_text("Zoinks! 🐾 Couldn't load patterns. Try again!")


async def handle_filterstats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show performance stats for all active smart filters."""
    if not update.message or not update.effective_chat:
        return
    await _track_stats(update, context)
    chat_id       = str(update.effective_chat.id)
    filter_scores = context.bot_data.get("filter_scores", {})
    smart_filters = context.bot_data.get("smart_filters", {})
    chat_sf       = smart_filters.get(chat_id, [])

    if not chat_sf:
        await update.message.reply_text(
            "🎯 No smart filters active. Set one with /smartwatch to start tracking performance!",
        )
        return

    try:
        from memory import get_filter_report
        lines = ["📊 *Smart Filter Performance*\n"]
        for flt in chat_sf:
            fid    = flt.get("id", "")
            report = get_filter_report(filter_scores, fid)
            lines.append(report)
        await update.message.reply_text(
            "\n\n".join(lines), parse_mode="MarkdownV2",
            disable_web_page_preview=True,
        )
    except Exception as ex:
        logger.error(f"handle_filterstats: {ex}", exc_info=True)
        await update.message.reply_text("Zoinks! 🐾 Couldn't load filter stats. Try again!")


async def handle_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    if not await _check_rate_limits(update, context):
        return
    http: httpx.AsyncClient = context.bot_data["http"]
    scan_prices: dict       = context.bot_data.get("scan_prices", {})
    chat_id                 = str(update.effective_chat.id) if update.effective_chat else ""
    msg = await update.message.reply_text("\U0001f3c6 Building the leaderboard... hold on, raggy!")
    try:
        text = await build_leaderboard_text(scan_prices, http, context.bot_data, chat_id)
        await msg.edit_text(text, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=leaderboard_keyboard())
    except Exception as e:
        logger.error(f"handle_leaderboard error: {e}", exc_info=True)
        try:
            await msg.edit_text("Zoinks! Scuby couldn't build the leaderboard. Try again! \U0001f47b")
        except Exception:
            pass


# ─── /alert ───────────────────────────────────────────────────────────────────

async def handle_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    if not await _check_rate_limits(update, context):
        return

    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /alert <CA> <multiple>\n"
            "Example: /alert 7xKXtg... 2  \u2014 alerts you when the token 2x\u2019s from scan price\n\n"
            "You must have started a DM with me first so I can reach you!"
        )
        return

    ca_arg = args[0].strip()
    if not VALID_CA.match(ca_arg):
        await update.message.reply_text(
            "Jinkies! That doesn\u2019t look like a valid contract address. \U0001f50d\n"
            "Alerts need a specific CA, not a ticker."
        )
        return

    try:
        target = float(args[1])
        if target <= 1.0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Ruh-roh! The target multiple must be a number greater than 1.\nExample: /alert <CA> 2.5"
        )
        return

    user_id     = update.effective_user.id
    http        = context.bot_data["http"]
    scan_prices = context.bot_data.get("scan_prices", {})

    entry = scan_prices.get(ca_arg)
    if entry and entry.get("price", 0) > 0:
        baseline = entry["price"]
        symbol   = entry.get("symbol", ca_arg[:8])
    else:
        prices   = await fetch_current_prices([ca_arg], http)
        baseline = prices.get(ca_arg, 0)
        symbol   = await resolve_symbol_for_ca(ca_arg, http) or ca_arg[:8]

    if baseline <= 0:
        await update.message.reply_text(
            "Ruh-roh! Scuby couldn\u2019t find a price for that token. Make sure it\u2019s a valid Solana CA. \U0001f43e"
        )
        return

    alert_key = f"{user_id}:{ca_arg}"
    alerts: dict = context.bot_data.setdefault("alerts", {})

    user_alert_count = sum(1 for k, v in alerts.items() if v.get("user_id") == user_id and k != alert_key)
    if user_alert_count >= MAX_ALERTS_PER_USER:
        await update.message.reply_text(
            f"\U0001f514 You already have *{MAX_ALERTS_PER_USER}* active alerts — the maximum allowed\\.\n\n"
            f"Cancel one with `/myalerts` before adding a new one\\.",
            parse_mode="MarkdownV2",
        )
        return

    alerts[alert_key] = {
        "user_id":        user_id,
        "ca":             ca_arg,
        "symbol":         symbol,
        "target_multiple": target,
        "baseline_price": baseline,
        "created_ts":     time.time(),
    }
    await save_alerts_async(alerts)

    e = escape_md
    await update.message.reply_text(
        f"\U0001f514 Alert set\\!\n\n"
        f"I\u2019ll DM you when *{e(symbol)}* reaches *{e(f'{target:.1f}')}x* "
        f"from its baseline price of ${e(f'{baseline:.8g}')}\\.\n\n"
        f"_Make sure you\u2019ve started a DM with me so I can reach you\\!_",
        parse_mode="MarkdownV2",
    )


# ─── /myalerts ────────────────────────────────────────────────────────────────

async def handle_myalerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    await _track_stats(update, context)

    user_id     = update.effective_user.id
    alerts: dict = context.bot_data.get("alerts", {})
    user_alerts  = [v for v in alerts.values() if v.get("user_id") == user_id]

    if not user_alerts:
        await update.message.reply_text(
            "\U0001f514 You have no active alerts\\.\n\nSet one with:\n`/alert <CA> <multiple>`\n"
            "e\\.g\\. `/alert 7xKXtg\\.\\.\\. 2`",
            parse_mode="MarkdownV2",
            reply_markup=main_menu_keyboard(),
        )
        return

    e     = escape_md
    lines = ["\U0001f514 *Your active alerts:*\n"]
    for alert in user_alerts:
        symbol  = alert.get("symbol", "?")
        ca      = alert["ca"]
        target  = alert["target_multiple"]
        baseline = alert["baseline_price"]
        dt_str  = datetime.fromtimestamp(alert["created_ts"], tz=timezone.utc).strftime("%b %d, %H:%M UTC")
        lines.append(
            f"\u2022 *{e(symbol)}* \u2014 alert at *{e(f'{target:.1f}')}x* "
            f"\\(baseline: ${e(f'{baseline:.8g}')}\\)\n"
            f"  `{e(ca)}`\n  _Set {e(dt_str)}_"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2", reply_markup=my_alerts_keyboard(user_alerts))


# ─── /monitor ─────────────────────────────────────────────────────────────────

async def handle_monitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if not await _check_rate_limits(update, context):
        return

    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text(
            "🐾 *Usage:* `/monitor <CA> <interval>`\n\n"
            "Examples:\n"
            "  `/monitor 7xKXtg\\.\\.\\. 3m` — ping every 3 minutes\n"
            "  `/monitor 7xKXtg\\.\\.\\. 10m` — ping every 10 minutes\n\n"
            f"_Min: 1m \\| Max: 60m \\| Up to {MAX_MONITORS_PER_CHAT} monitors per chat_",
            parse_mode="MarkdownV2",
        )
        return

    ca_arg = args[0].strip()
    if not VALID_CA.match(ca_arg):
        await update.message.reply_text(
            "Jinkies\\! 🔍 That doesn't look like a valid contract address\\.\nUsage: `/monitor <CA> <interval>`",
            parse_mode="MarkdownV2",
        )
        return

    interval_secs = parse_interval(args[1])
    if interval_secs is None:
        await update.message.reply_text(
            f"Ruh\\-roh\\! ⏱ Invalid interval\\.\nUse a number of minutes, e\\.g\\. `3m`, `10m`, `30m`\\.\n_Min: 1m \\| Max: 60m_",
            parse_mode="MarkdownV2",
        )
        return

    chat_id       = str(update.effective_chat.id)
    http          = context.bot_data["http"]
    monitors: dict = context.bot_data.setdefault("monitors", {})
    chat_monitors  = monitors.setdefault(chat_id, [])

    if len(chat_monitors) >= MAX_MONITORS_PER_CHAT:
        await update.message.reply_text(
            f"🐾 Ruh\\-roh\\! Already monitoring *{MAX_MONITORS_PER_CHAT}* tokens here\\.\nUse `/monitoring` to see and cancel some first\\!",
            parse_mode="MarkdownV2",
        )
        return

    symbol = await resolve_symbol_for_ca(ca_arg, http)
    if not symbol:
        await update.message.reply_text(
            "Ruh\\-roh\\! 🐾 Scuby couldn't find that token on Solana\\. Double\\-check the CA and try again\\.",
            parse_mode="MarkdownV2",
        )
        return

    existing = next((m for m in chat_monitors if m["ca"] == ca_arg), None)
    if existing:
        existing["interval_secs"] = interval_secs
        existing["last_sent_ts"]  = 0
        await save_monitors_async(monitors)
        e = escape_md
        await update.message.reply_text(
            f"✅ Updated\\! Scuby will now ping *{e(symbol)}* every *{e(str(interval_secs // 60))}m* 📡",
            parse_mode="MarkdownV2",
        )
        return

    monitor_id = f"{int(time.time() * 1000)}_{os.urandom(3).hex()}"
    chat_monitors.append({
        "id":            monitor_id,
        "ca":            ca_arg,
        "symbol":        symbol,
        "interval_secs": interval_secs,
        "chat_id":       chat_id,
        "added_ts":      time.time(),
        "last_sent_ts":  0,
    })
    await save_monitors_async(monitors)

    e = escape_md
    await update.message.reply_text(
        f"📡 *Monitor set\\!*\n\n"
        f"Scuby will ping *{e(symbol)}* mcap updates every *{e(str(interval_secs // 60))}m* right here 🐾\n\n"
        f"_Use /monitoring to see all active monitors\\._\n_Use /unmonitor to stop\\._",
        parse_mode="MarkdownV2",
    )


# ─── /monitoring ──────────────────────────────────────────────────────────────

async def handle_monitoring(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    await _track_stats(update, context)

    chat_id        = str(update.effective_chat.id)
    monitors: dict = context.bot_data.get("monitors", {})
    chat_monitors  = monitors.get(chat_id, [])

    if not chat_monitors:
        await update.message.reply_text(
            "📡 Scuby isn't monitoring anything here yet\\!\n\nStart with: `/monitor <CA> <interval>`",
            parse_mode="MarkdownV2",
            reply_markup=main_menu_keyboard(),
        )
        return

    e     = escape_md
    lines = ["📡 *Active monitors in this chat:*\n"]
    for i, m in enumerate(chat_monitors, 1):
        symbol   = m.get("symbol", "?")
        ca       = m["ca"]
        interval = m["interval_secs"] // 60
        added    = datetime.fromtimestamp(m["added_ts"], tz=timezone.utc).strftime("%b %d, %H:%M UTC")
        last_ts  = m.get("last_sent_ts", 0)
        last_str = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%H:%M UTC") if last_ts else "not yet"
        lines.append(
            f"*{i}\\.* *{e(symbol)}* — every *{e(str(interval))}m*\n"
            f"   `{e(ca)}`\n"
            f"   _Added {e(added)}, last ping {e(last_str)}_"
        )

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="MarkdownV2",
        reply_markup=monitoring_keyboard(int(chat_id), chat_monitors),
    )


# ─── /unmonitor ───────────────────────────────────────────────────────────────

async def handle_unmonitor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    await _track_stats(update, context)

    chat_id        = str(update.effective_chat.id)
    monitors: dict = context.bot_data.get("monitors", {})
    chat_monitors  = monitors.get(chat_id, [])

    if not chat_monitors:
        await update.message.reply_text("🐾 Nothing to stop — Scuby isn't monitoring anything here\\!", parse_mode="MarkdownV2")
        return

    args = context.args or []
    if not args:
        await handle_monitoring(update, context)
        return

    fragment = args[0].strip().lower()
    before   = len(chat_monitors)
    monitors[chat_id] = [
        m for m in chat_monitors
        if not m["ca"].lower().startswith(fragment)
        and fragment not in m["ca"].lower()
        and fragment.upper() != m.get("symbol", "").upper()
    ]
    removed = before - len(monitors[chat_id])
    await save_monitors_async(monitors)

    e = escape_md
    if removed:
        await update.message.reply_text(
            f"✅ Done\\! Scuby stopped monitoring *{e(fragment)}* \\({removed} monitor\\(s\\) removed\\) 🐾",
            parse_mode="MarkdownV2",
        )
    else:
        await update.message.reply_text(
            f"Hmm\\! 🐾 No monitor found matching `{e(fragment)}`\\.\nUse /monitoring to see what's active\\.",
            parse_mode="MarkdownV2",
        )


# ─── /watch ───────────────────────────────────────────────────────────────────

async def handle_watch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if not await _check_rate_limits(update, context):
        return

    raw = " ".join(context.args or []).strip()
    if not raw:
        await update.message.reply_text(
            "🐾 *Ruh-roh\\!* Tell Scuby what to watch for\\!\n\n"
            "Try:\n"
            "  `/watch dog` — new dog\\-themed coins\n"
            "  `/watch cat meme` — cat or meme coins\n"
            "  `/watch animal` — ALL animal memecoins\n"
            "  `/watch trump` — political memecoins\n\n"
            "_Just describe it in plain English, raggy\\!_",
            parse_mode="MarkdownV2",
        )
        return

    terms, patterns = expand_keywords(raw)
    if not terms:
        await update.message.reply_text(
            "Jinkies\\! 🔍 Scuby couldn't figure out what to watch for\\.\n"
            "Try `/watch dog coin` or `/watch animal meme`\\.",
            parse_mode="MarkdownV2",
        )
        return

    chat_id        = str(update.effective_chat.id)
    watchlist: dict = context.bot_data.setdefault("watchlist", {})
    chat_watches    = watchlist.setdefault(chat_id, [])

    if len(chat_watches) >= MAX_WATCHES_PER_CHAT:
        await update.message.reply_text(
            f"🐾 Ruh\\-roh\\! Already watching *{MAX_WATCHES_PER_CHAT}* things here\\.\nUse `/watching` to cancel some first\\!",
            parse_mode="MarkdownV2",
        )
        return

    watch_id = f"{int(time.time() * 1000)}_{os.urandom(4).hex()}"
    chat_watches.append({
        "id":       watch_id,
        "terms":    terms,
        "patterns": patterns,
        "raw":      raw,
        "added_ts": time.time(),
        "seen_cas": [],
    })
    await save_watchlist_async(watchlist)

    e               = escape_md
    term_tags       = e(", ".join(f"#{t}" for t in terms))
    pattern_preview = e(", ".join(patterns[:8]) + ("…" if len(patterns) > 8 else ""))

    await update.message.reply_text(
        f"{watch_confirm_text(terms)}\n\n"
        f"📡 *Watching for:* {term_tags}\n"
        f"🔍 *Patterns:* `{pattern_preview}`\n"
        f"💧 Min liquidity: ${WATCH_MIN_LIQUIDITY:,} \\| Max age: {WATCH_MAX_AGE_HOURS}h\n\n"
        f"_Use /watching to see all active watches or /unwatch to stop\\._",
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("👁 What I'm watching", callback_data="menu|watching"),
        ]]),
    )


# ─── /watching ────────────────────────────────────────────────────────────────

async def handle_watching(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    await _track_stats(update, context)

    chat_id        = str(update.effective_chat.id)
    watchlist: dict = context.bot_data.get("watchlist", {})
    chat_watches    = watchlist.get(chat_id, [])

    if not chat_watches:
        await update.message.reply_text(
            "🐾 Scuby isn't watching for anything here yet\\!\nStart with `/watch dog` or `/watch animal meme`\\.",
            parse_mode="MarkdownV2",
            reply_markup=main_menu_keyboard(),
        )
        return

    e     = escape_md
    lines = ["🔭 *Active watches in this chat:*\n"]
    for i, w in enumerate(chat_watches, 1):
        term_str   = e(", ".join(f"#{t}" for t in w["terms"]))
        seen_count = len(w.get("seen_cas", []))
        added      = datetime.fromtimestamp(w["added_ts"], tz=timezone.utc).strftime("%b %d, %H:%M UTC")
        lines.append(f"*{i}\\.* {term_str}\n   _Added {e(added)} \\- {seen_count} alert\\(s\\) sent_")

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="MarkdownV2",
        reply_markup=watchlist_keyboard(int(chat_id), chat_watches),
    )


# ─── /unwatch ─────────────────────────────────────────────────────────────────

async def handle_unwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    await _track_stats(update, context)

    chat_id        = str(update.effective_chat.id)
    watchlist: dict = context.bot_data.get("watchlist", {})
    chat_watches    = watchlist.get(chat_id, [])

    if not chat_watches:
        await update.message.reply_text("🐾 Nothing to stop — Scuby isn't watching anything here\\!", parse_mode="MarkdownV2")
        return

    raw = " ".join(context.args or []).strip().lower()
    if not raw:
        await handle_watching(update, context)
        return

    raw_pattern = re.compile(rf'\b{re.escape(raw)}\b')
    before      = len(chat_watches)
    watchlist[chat_id] = [
        w for w in chat_watches
        if not any(raw_pattern.search(t.lower()) for t in w["terms"])
        and not raw_pattern.search(w.get("raw", "").lower())
    ]
    removed = before - len(watchlist[chat_id])
    await save_watchlist_async(watchlist)

    e = escape_md
    if removed:
        await update.message.reply_text(
            f"✅ Done, raggy\\! Scuby stopped watching *{e(raw)}* \\({removed} watch\\(es\\) removed\\)\\. 🐾",
            parse_mode="MarkdownV2",
        )
    else:
        await update.message.reply_text(
            f"Hmm\\! 🐾 No watch matching *{e(raw)}*\\. Use /watching to see what's active\\.",
            parse_mode="MarkdownV2",
        )


# ─── Feed keyboard ────────────────────────────────────────────────────────────

def feeds_keyboard(chat_id: int, chat_feeds: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for feed in chat_feeds:
        label = feed_label(feed["direction"], feed["threshold"], feed["timeframe"])
        rows.append([InlineKeyboardButton(
            f"❌ Stop: {label}",
            callback_data=f"delfeed|{feed['id']}",
        )])
    rows.append([InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")])
    return InlineKeyboardMarkup(rows)


# ─── /feed ────────────────────────────────────────────────────────────────────

async def handle_feed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if not await _check_rate_limits(update, context):
        return

    args = context.args or []

    if not args:
        await update.message.reply_text(
            "📡 *Usage:* `/feed <up|down> <threshold%> <timeframe>`\n\n"
            "*Examples:*\n"
            "  `/feed up 20% 1h` — tokens pumping 20%\\+ in 1h\n"
            "  `/feed down 30% 24h` — tokens dumping 30%\\+ in 24h\n"
            "  `/feed up 50% 5m` — anything spiking 50%\\+ in 5 minutes\n\n"
            f"*Timeframes:* `5m` \\| `1h` \\| `6h` \\| `24h`\n\n"
            "_Scuby scans every 10 minutes and posts matching tokens here\\.\n"
            "Nothing posted if no tokens hit the threshold that scan\\._",
            parse_mode="MarkdownV2",
        )
        return

    parsed = parse_feed_args(args)
    if not parsed:
        e = escape_md
        tfs = e(" | ".join(VALID_TIMEFRAMES))
        await update.message.reply_text(
            f"Ruh\\-roh\\! 🐾 Couldn't parse that\\.\n\n"
            f"Usage: `/feed up 20% 1h`\n"
            f"Timeframes: `{tfs}`",
            parse_mode="MarkdownV2",
        )
        return

    direction, threshold, timeframe = parsed
    chat_id  = str(update.effective_chat.id)
    feeds: dict = context.bot_data.setdefault("feeds", {})
    chat_feeds  = feeds.setdefault(chat_id, [])

    if len(chat_feeds) >= MAX_FEEDS_PER_CHAT:
        await update.message.reply_text(
            f"🐾 Already have *{MAX_FEEDS_PER_CHAT}* feeds running here\\.\n"
            "Use `/feeds` to cancel some first\\!",
            parse_mode="MarkdownV2",
        )
        return

    existing = next(
        (f for f in chat_feeds
         if f["direction"] == direction and f["timeframe"] == timeframe),
        None,
    )
    if existing:
        existing["threshold"]  = threshold
        existing["seen_cas"]   = []
        await save_feeds_async(feeds)
        e = escape_md
        arrow = "▲" if direction == "up" else "▼"
        await update.message.reply_text(
            f"✅ Updated\\! Scuby will now alert on "
            f"*{arrow} {e(f'{threshold:.0f}')}%\\+* moves in *{e(timeframe)}* 📡",
            parse_mode="MarkdownV2",
        )
        return

    feed_id = f"{int(time.time() * 1000)}_{os.urandom(3).hex()}"
    chat_feeds.append({
        "id":        feed_id,
        "direction": direction,
        "threshold": threshold,
        "timeframe": timeframe,
        "added_ts":  time.time(),
        "seen_cas":  [],
    })
    await save_feeds_async(feeds)

    e     = escape_md
    arrow = "▲" if direction == "up" else "▼"
    emoji = "🚀" if direction == "up" else "📉"
    await update.message.reply_text(
        f"📡 *Feed set\\!*\n\n"
        f"{emoji} Scuby will scan every *10 minutes* and post any Solana token\n"
        f"moving *{arrow} {e(f'{threshold:.0f}')}%\\+* in *{e(timeframe)}* right here\\.\n\n"
        f"_Nothing will be posted if no tokens hit the threshold\\._\n"
        f"_Use /feeds to see active feeds or tap ❌ on any alert to stop it\\._",
        parse_mode="MarkdownV2",
    )


# ─── /feeds ───────────────────────────────────────────────────────────────────

async def handle_feeds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    await _track_stats(update, context)

    chat_id    = str(update.effective_chat.id)
    feeds: dict = context.bot_data.get("feeds", {})
    chat_feeds  = feeds.get(chat_id, [])

    if not chat_feeds:
        await update.message.reply_text(
            "📡 No active feeds here yet\\.\n\n"
            "Start one with:\n"
            "`/feed up 20% 1h` — tokens pumping 20%\\+ in 1h\n"
            "`/feed down 30% 24h` — tokens dumping 30%\\+ in 24h",
            parse_mode="MarkdownV2",
            reply_markup=main_menu_keyboard(),
        )
        return

    e     = escape_md
    lines = ["📡 *Active feeds in this chat:*\n"]
    for i, feed in enumerate(chat_feeds, 1):
        arrow    = "▲" if feed["direction"] == "up" else "▼"
        added    = datetime.fromtimestamp(feed["added_ts"], tz=timezone.utc).strftime("%b %d, %H:%M UTC")
        seen_cnt = len(feed.get("seen_cas", []))
        thr      = f"{feed['threshold']:.0f}"
        lines.append(
            f"*{i}\\.* {arrow} *{e(thr)}%\\+* in *{e(feed['timeframe'])}*\n"
            f"   _Added {e(added)} — {seen_cnt} alert\\(s\\) sent_"
        )

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="MarkdownV2",
        reply_markup=feeds_keyboard(int(chat_id), chat_feeds),
    )


# ─── /unfeed ──────────────────────────────────────────────────────────────────

async def handle_unfeed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    await _track_stats(update, context)

    chat_id    = str(update.effective_chat.id)
    feeds: dict = context.bot_data.get("feeds", {})
    chat_feeds  = feeds.get(chat_id, [])

    if not chat_feeds:
        await update.message.reply_text("🐾 No active feeds to stop here\\!", parse_mode="MarkdownV2")
        return

    args = context.args or []
    if not args:
        await handle_feeds(update, context)
        return

    raw = " ".join(args).lower().strip()
    before = len(chat_feeds)
    feeds[chat_id] = [
        f for f in chat_feeds
        if f["direction"] != raw and f["timeframe"] != raw
    ]
    removed = before - len(feeds[chat_id])
    await save_feeds_async(feeds)

    e = escape_md
    if removed:
        await update.message.reply_text(
            f"✅ Stopped *{removed}* feed\\(s\\) matching *{e(raw)}* 🐾",
            parse_mode="MarkdownV2",
        )
    else:
        await update.message.reply_text(
            f"Hmm\\! 🐾 No feed matched *{e(raw)}*\\. Use /feeds to see what's active\\.",
            parse_mode="MarkdownV2",
        )


# ─── /screener ────────────────────────────────────────────────────────────────

def screener_keyboard(direction, threshold, timeframe, page, total):
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("« Prev", callback_data=f"scr|{direction}|{threshold}|{timeframe}|{page - 1}"))
    nav.append(InlineKeyboardButton(f"{page + 1} / {total}", callback_data="noop"))
    if page < total - 1:
        nav.append(InlineKeyboardButton("Next »", callback_data=f"scr|{direction}|{threshold}|{timeframe}|{page + 1}"))
    action_row = [InlineKeyboardButton("🔄 Refresh", callback_data=f"scr|{direction}|{threshold}|{timeframe}|{page}|refresh")]
    return InlineKeyboardMarkup([nav, action_row])


def _screener_cache_key(direction, threshold, timeframe):
    return f"screener:{direction}:{threshold:.2f}:{timeframe}"


def _store_screener_cache(bot_data, direction, threshold, timeframe, pairs):
    cache = bot_data.setdefault("screener_cache", {})
    cache[_screener_cache_key(direction, threshold, timeframe)] = {"pairs": pairs, "ts": time.monotonic()}


def _load_screener_cache(bot_data, direction, threshold, timeframe):
    cache = bot_data.get("screener_cache", {})
    entry = cache.get(_screener_cache_key(direction, threshold, timeframe))
    if not entry:
        return None
    if time.monotonic() - entry["ts"] > 300:
        return None
    return entry["pairs"]


def format_screener_page(pairs, page, direction, threshold, timeframe):
    from feeds import TIMEFRAME_FIELD
    total  = len(pairs)
    pair   = pairs[page]
    e      = escape_md
    arrow  = "▲" if direction == "up" else "▼"
    field  = TIMEFRAME_FIELD.get(timeframe, "h1")
    pct    = safe_float((pair.get("priceChange") or {}).get(field, 0))
    sign   = "+" if pct >= 0 else ""
    ca     = pair.get("baseToken", {}).get("address", "?")
    symbol = pair.get("baseToken", {}).get("symbol", "?")
    name   = pair.get("baseToken", {}).get("name",   "?")
    price  = pair.get("priceUsd") or "?"
    mcap   = safe_float(pair.get("marketCap") or pair.get("fdv") or 0)
    liq    = safe_float((pair.get("liquidity") or {}).get("usd", 0))
    vol1h  = safe_float((pair.get("volume")    or {}).get("h1",  0))
    vol24h = safe_float((pair.get("volume")    or {}).get("h24", 0))
    h1     = safe_float((pair.get("priceChange") or {}).get("h1",  0))
    h24    = safe_float((pair.get("priceChange") or {}).get("h24", 0))
    dex_url = f"https://dexscreener.com/solana/{ca}"
    x_url   = f"https://x.com/search?q={ca}"
    emoji = "🌙" if abs(pct) >= 100 else ("🚀" if abs(pct) >= 50 else "📈") if direction == "up" else ("💀" if abs(pct) >= 70 else ("🩸" if abs(pct) >= 40 else "📉"))
    header = f"🔍 *Screener — {arrow} {e(f'{threshold:.0f}')}%\\+ in {e(timeframe)}*\n_{e(str(total))} result\\(s\\) found_\n\n"
    body = (
        f"{emoji} *{e(symbol)}* — {e(name)}\n`{e(ca)}`\n\n"
        f"{'📈' if direction=='up' else '📉'} *{e(f'{sign}{pct:.1f}%')}* in {e(timeframe)}\n"
        f"💰 Price:   ${e(str(price))}\n"
        f"💎 MCap:    {e(fmt_mcap(mcap))}\n"
        f"💧 Liq:     ${e(f'{liq:,.0f}')}\n"
        f"📊 Vol 1h:  ${e(f'{vol1h:,.0f}')}\n"
        f"📊 Vol 24h: ${e(f'{vol24h:,.0f}')}\n"
        f"📈 1h: {e(f'{h1:+.2f}')}%  \\|  24h: {e(f'{h24:+.2f}')}%\n\n"
        f"[DexScreener]({dex_url}) \\| [Search on X]({x_url})\n\n"
        f"_⚠️ DYOR\\. Not financial advice\\._"
    )
    nav_keyboard = screener_keyboard(direction, threshold, timeframe, page, total)
    rows = list(nav_keyboard.inline_keyboard)
    rows.append([InlineKeyboardButton("🔍 Sniff OG", callback_data=f"og|ca|{ca}")])
    return header + body, InlineKeyboardMarkup(rows)


async def handle_screener(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    if not await _check_rate_limits(update, context):
        return

    args = context.args or []
    parsed = parse_feed_args(args)
    if not parsed:
        tfs = escape_md(" | ".join(VALID_TIMEFRAMES))
        await update.message.reply_text(
            "🔍 *Usage:* `/screener <up|down> <threshold%> <timeframe>`\n\n"
            "*Examples:*\n"
            "  `/screener up 20% 1h`\n"
            "  `/screener down 30% 24h`\n\n"
            f"*Timeframes:* `{tfs}`",
            parse_mode="MarkdownV2",
        )
        return

    direction, threshold, timeframe = parsed
    arrow = "▲" if direction == "up" else "▼"
    e     = escape_md
    msg = await update.message.reply_text(
        f"🔍 Scanning for tokens *{arrow} {e(f'{threshold:.0f}')}%\\+* in *{e(timeframe)}*\\.\\.\\. hold on, raggy\\!",
        parse_mode="MarkdownV2",
    )

    http: httpx.AsyncClient = context.bot_data["http"]
    try:
        movers = await run_screener(http, direction, threshold, timeframe)
    except Exception as ex:
        logger.error(f"handle_screener error: {ex}", exc_info=True)
        await msg.edit_text("Zoinks\\! 🐾 Scan failed\\. Try again\\!", parse_mode="MarkdownV2")
        return

    if not movers:
        await msg.edit_text(
            f"🐾 No tokens found moving *{arrow} {e(f'{threshold:.0f}')}%\\+* in *{e(timeframe)}* right now\\.\n\n"
            "_Try a lower threshold or different timeframe\\._",
            parse_mode="MarkdownV2",
        )
        return

    _store_screener_cache(context.bot_data, direction, threshold, timeframe, movers)
    text, keyboard = format_screener_page(movers, 0, direction, threshold, timeframe)
    await msg.edit_text(text, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=keyboard)


async def handle_screener_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return

    parts = query.data.split("|")
    if len(parts) < 5 or parts[0] != "scr":
        return

    _, direction, threshold_str, timeframe, page_str, *flags = parts
    refresh = "refresh" in flags

    try:
        threshold = float(threshold_str)
        page      = int(page_str)
    except ValueError:
        return

    http: httpx.AsyncClient = context.bot_data["http"]

    if refresh:
        cache = context.bot_data.get("screener_cache", {})
        cache.pop(_screener_cache_key(direction, threshold, timeframe), None)
        arrow = "▲" if direction == "up" else "▼"
        e     = escape_md
        await query.edit_message_text(
            f"🔍 Refreshing\\.\\.\\. scanning for *{arrow} {e(f'{threshold:.0f}')}%\\+* in *{e(timeframe)}* \\.\\.\\.",
            parse_mode="MarkdownV2",
        )
        try:
            movers = await run_screener(http, direction, threshold, timeframe)
        except Exception as ex:
            logger.error(f"handle_screener_page refresh error: {ex}", exc_info=True)
            await query.edit_message_text("Zoinks\\! 🐾 Refresh failed\\.", parse_mode="MarkdownV2")
            return
        if not movers:
            await query.edit_message_text(f"🐾 No tokens found right now\\.", parse_mode="MarkdownV2")
            return
        _store_screener_cache(context.bot_data, direction, threshold, timeframe, movers)
        page = 0
    else:
        movers = _load_screener_cache(context.bot_data, direction, threshold, timeframe)
        if movers is None:
            try:
                movers = await run_screener(http, direction, threshold, timeframe)
                if movers:
                    _store_screener_cache(context.bot_data, direction, threshold, timeframe, movers)
            except Exception as ex:
                await query.edit_message_text("Zoinks\\! 🐾 Results expired\\. Run `/screener` again\\!", parse_mode="MarkdownV2")
                return
            if not movers:
                await query.edit_message_text("🐾 Results expired\\. Try `/screener` again\\!", parse_mode="MarkdownV2")
                return

    total = len(movers)
    page  = max(0, min(page, total - 1))
    text, keyboard = format_screener_page(movers, page, direction, threshold, timeframe)
    try:
        await query.edit_message_text(text, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=keyboard)
    except Exception as ex:
        logger.warning(f"handle_screener_page edit failed: {ex}")


# ─── Delete feed callback ─────────────────────────────────────────────────────

async def handle_delfeed_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return

    parts = query.data.split("|", 1)
    if len(parts) != 2 or parts[0] != "delfeed":
        return

    feed_id = parts[1]
    chat_id = str(query.message.chat.id) if query.message else None
    if not chat_id:
        return

    feeds: dict = context.bot_data.setdefault("feeds", {})
    before  = feeds.get(chat_id, [])
    removed = [f for f in before if f["id"] == feed_id]
    feeds[chat_id] = [f for f in before if f["id"] != feed_id]
    await save_feeds_async(feeds)

    remaining = feeds.get(chat_id, [])
    label     = feed_label(removed[0]["direction"], removed[0]["threshold"], removed[0]["timeframe"]) if removed else "that feed"

    e = escape_md
    try:
        if not remaining:
            await query.edit_message_text(
                f"✅ Stopped feed *{e(label)}*\\.\n\n🐾 No active feeds here now\\.",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")]]),
            )
        else:
            lines = [f"✅ Stopped feed *{e(label)}*\\.\n\n📡 *Still active:*\n"]
            for fd in remaining:
                arrow   = "▲" if fd["direction"] == "up" else "▼"
                thr_str = f"{fd['threshold']:.0f}"
                tf_str  = fd['timeframe']
                lines.append(f"• {arrow} *{e(thr_str)}%\\+* in *{e(tf_str)}*")
            await query.edit_message_text("\n".join(lines), parse_mode="MarkdownV2", reply_markup=feeds_keyboard(int(chat_id), remaining))
    except Exception as ex:
        logger.warning(f"handle_delfeed_button edit failed: {ex}")


# ─── /stats ───────────────────────────────────────────────────────────────────

async def handle_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    import os as _os
    owner_id = int(_os.environ["OWNER_ID"]) if _os.environ.get("OWNER_ID") else None
    if owner_id is not None and (not update.effective_user or update.effective_user.id != owner_id):
        await update.message.reply_text("Ruh-roh! That command is for Scuby's owner only. \U0001f43e")
        return
    await _track_stats(update, context)

    stats       = context.bot_data.get("stats", {"chats": set(), "users": set()})
    scan_prices = context.bot_data.get("scan_prices", {})
    monitors    = context.bot_data.get("monitors", {})
    feeds       = context.bot_data.get("feeds", {})

    await update.message.reply_text(
        f"\U0001f4ca *Scuby Stats*\n\n"
        f"\U0001f465 Unique users: *{len(stats.get('users', set()))}*\n"
        f"\U0001f4ac Unique chats: *{len(stats.get('chats', set()))}*\n"
        f"\U0001f4b0 Tokens with scan price: *{len(scan_prices)}*\n"
        f"📡 Active monitors: *{sum(len(v) for v in monitors.values())}*\n"
        f"📶 Active feeds: *{sum(len(v) for v in feeds.values())}*",
        parse_mode="MarkdownV2",
    )


# ─── OG display ───────────────────────────────────────────────────────────────

async def _show_og(query, context, query_type, query_value, force=False):
    http        = context.bot_data["http"]
    scan_prices = context.bot_data.setdefault("scan_prices", {})
    retry_kb    = InlineKeyboardMarkup([[InlineKeyboardButton("\U0001f43e Sniff again", callback_data=f"og|{query_type}|{query_value}")]])
    user        = query.from_user
    scanned_by  = f"@{user.username}" if user and user.username else (user.first_name if user else "")
    chat_id     = str(query.message.chat.id) if query.message and query.message.chat else ""

    try:
        pairs = await fetch_pairs_and_cache(query_type, query_value, http, context.bot_data, force=force, scanned_by=scanned_by, chat_id=chat_id)
        if not pairs:
            await query.edit_message_text("Ruh-roh! Scuby couldn't find any trace of that token on Solana. \U0001f43e", reply_markup=retry_kb)
            return
        og = find_og(pairs)
        if not og:
            await query.edit_message_text("Jeepers! The trail went cold. \U0001f50d", reply_markup=retry_kb)
            return
        og_ca       = og.get("baseToken", {}).get("address", "?")
        risk_report = await fetch_rugcheck(og_ca, http)
        risk_badge  = parse_risk_badge(risk_report)
        result      = format_og_response(og, len(pairs), risk_badge, scan_prices)
        keyboard    = og_keyboard(query_type, query_value, show_versions=len(pairs) > 1)
        await query.edit_message_text(result, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error in _show_og: {e}", exc_info=True)
        try:
            await query.edit_message_text("Zoinks! Something spooked Scuby. Try again! \U0001f47b", reply_markup=retry_kb)
        except Exception:
            pass


async def handle_og_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return
    parts = query.data.split("|", 2)
    if len(parts) != 3 or parts[0] != "og":
        return
    _, query_type, query_value = parts
    if query_type == "ca" and not VALID_CA.match(query_value):
        await query.edit_message_text("Ruh-roh! That contract address looks sus. \U0001f43e")
        return
    if query_type not in ("ca", "ticker"):
        await query.edit_message_text("Zoinks! Scuby doesn't know what to do with that. \U0001f43e")
        return
    await query.edit_message_text("\U0001f436 Scuby's on the trail... sniffing through the blockchain...")
    await _show_og(query, context, query_type, query_value, force=False)


# ─── Versions display ─────────────────────────────────────────────────────────

async def _show_versions_page(query, context, query_type, query_value, page, force=False):
    http        = context.bot_data["http"]
    scan_prices = context.bot_data.setdefault("scan_prices", {})
    retry_kb    = InlineKeyboardMarkup([[InlineKeyboardButton("\U0001f43e Sniff again", callback_data=f"og|{query_type}|{query_value}")]])

    try:
        pairs = await fetch_pairs_and_cache(query_type, query_value, http, context.bot_data, force=force)
        if not pairs:
            await query.edit_message_text("Ruh-roh! The versions vanished! \U0001f43e", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\U0001f3c6 Back to OG", callback_data=f"og|{query_type}|{query_value}")]]))
            return
        total    = len(pairs)
        page     = max(0, min(page, total - 1))
        card     = format_version_card(pairs[page], rank=page + 1, total=total, scan_prices=scan_prices)
        keyboard = versions_keyboard(query_type, query_value, page, total)
        await query.edit_message_text(card, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error in _show_versions_page: {e}", exc_info=True)
        try:
            await query.edit_message_text("Zoinks! Scuby lost the scent. Try again! \U0001f47b", reply_markup=retry_kb)
        except Exception:
            pass


async def handle_versions_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return
    parts = query.data.split("|", 3)
    if len(parts) != 4 or parts[0] != "ver":
        return
    _, query_type, query_value, page_str = parts
    try:
        page = int(page_str)
    except ValueError:
        return
    if _load_versions(context.bot_data, query_type, query_value) is None:
        await query.edit_message_text("\U0001f436 Fetching all versions... hold on, raggy!")
    await _show_versions_page(query, context, query_type, query_value, page)


# ─── Refresh ──────────────────────────────────────────────────────────────────

async def handle_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return
    parts = query.data.split("|")
    if parts[0] not in ("ro", "rv") or len(parts) < 3:
        return
    view        = "og" if parts[0] == "ro" else "versions"
    query_type  = parts[1]
    query_value = parts[2]
    _bust_cache(context.bot_data, query_type, query_value)
    if view == "og":
        await query.edit_message_text("\U0001f436 Refreshing... sniffing for the latest data!")
        await _show_og(query, context, query_type, query_value, force=False)
    else:
        try:
            page = int(parts[3]) if len(parts) > 3 else 0
        except ValueError:
            page = 0
        await query.edit_message_text("\U0001f436 Refreshing versions... hold on, raggy!")
        await _show_versions_page(query, context, query_type, query_value, page, force=False)


# ─── Movers ───────────────────────────────────────────────────────────────────

async def handle_movers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return
    parts = query.data.split("|", 2)
    if len(parts) != 3 or parts[0] != "mov":
        return
    _, query_type, query_value = parts
    await query.edit_message_text("\U0001f4ca Scuby's scanning all versions for movers...")

    http        = context.bot_data["http"]
    scan_prices = context.bot_data.setdefault("scan_prices", {})
    try:
        pairs = await fetch_pairs_and_cache(query_type, query_value, http, context.bot_data)
        if not pairs:
            await query.edit_message_text("Ruh-roh! No versions found to scan. \U0001f43e", reply_markup=movers_keyboard(query_type, query_value))
            return
        symbol = pairs[0].get("baseToken", {}).get("symbol", query_value)
        report = format_movers_report(pairs, symbol, scan_prices)

        def _m(p):
            ca      = p.get("baseToken", {}).get("address", "")
            current = safe_float(p.get("priceUsd") or 0)
            entry   = scan_prices.get(ca)
            if entry and entry.get("price", 0) > 0 and current > 0:
                return current / entry["price"]
            return 1 + safe_float(p.get("priceChange", {}).get("h24", 0)) / 100

        sorted_indices = sorted(range(len(pairs)), key=lambda i: _m(pairs[i]), reverse=True)
        top_movers     = [(rank + 1, idx) for rank, idx in enumerate(sorted_indices[:5])]
        await query.edit_message_text(report, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=movers_keyboard(query_type, query_value, top_movers))
    except Exception as e:
        logger.error(f"Error in handle_movers: {e}", exc_info=True)
        try:
            await query.edit_message_text("Zoinks! Something went wrong. Try again! \U0001f47b", reply_markup=movers_keyboard(query_type, query_value))
        except Exception:
            pass


# ─── Refresh pair callback ────────────────────────────────────────────────────

async def handle_refreshpair(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return

    parts = query.data.split("|", 1)
    if len(parts) != 2 or parts[0] != "refreshpair":
        return

    ca   = parts[1]
    http = context.bot_data["http"]
    await query.edit_message_text("🔄 Refreshing\\.\\.\\.", parse_mode="MarkdownV2")

    try:
        from utils import dex_get as _dex_get, _calc_perf_label
        from feeds import fmt_mcap
        data  = await _dex_get(http, f"https://api.dexscreener.com/latest/dex/tokens/{ca}")
        pairs = [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
        if not pairs:
            await query.edit_message_text(
                "Ruh\\-roh\\! 🐾 Couldn't fetch current data\\.",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try again", callback_data=f"refreshpair|{ca}"), InlineKeyboardButton("🔍 Sniff OG", callback_data=f"og|ca|{ca}")]]),
            )
            return

        pair    = pairs[0]
        e       = escape_md
        symbol  = pair.get("baseToken", {}).get("symbol", "?")
        name    = pair.get("baseToken", {}).get("name",   "?")
        price   = pair.get("priceUsd") or "?"
        mcap    = safe_float(pair.get("marketCap") or pair.get("fdv") or 0)
        liq     = safe_float((pair.get("liquidity") or {}).get("usd",  0))
        vol1h   = safe_float((pair.get("volume")    or {}).get("h1",   0))
        vol24h  = safe_float((pair.get("volume")    or {}).get("h24",  0))
        h1      = safe_float((pair.get("priceChange") or {}).get("h1",  0))
        h6      = safe_float((pair.get("priceChange") or {}).get("h6",  0))
        h24     = safe_float((pair.get("priceChange") or {}).get("h24", 0))
        dex_url = f"https://dexscreener.com/solana/{ca}"
        x_url   = f"https://x.com/search?q={ca}"
        now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")

        scan_prices: dict = context.bot_data.get("scan_prices", {})
        entry = scan_prices.get(ca)
        if entry and entry.get("price", 0) > 0 and safe_float(price) > 0:
            multiple   = safe_float(price) / entry["price"]
            perf_label = _calc_perf_label(multiple)
            scan_dt    = datetime.fromtimestamp(entry["ts"], tz=timezone.utc).strftime("%b %d, %H:%M UTC")
            perf_line  = f"📊 Since first sniff \\({e(scan_dt)}\\): *{e(perf_label)}*\n"
        else:
            perf_line = ""

        text = (
            f"🔄 *{e(symbol)}* — {e(name)}\n`{e(ca)}`\n\n"
            f"💰 Price:   ${e(str(price))}\n"
            f"💎 MCap:    {e(fmt_mcap(mcap))}\n"
            f"💧 Liq:     ${e(f'{liq:,.0f}')}\n"
            f"📊 Vol 1h:  ${e(f'{vol1h:,.0f}')}\n"
            f"📊 Vol 24h: ${e(f'{vol24h:,.0f}')}\n"
            f"📈 1h: {e(f'{h1:+.2f}')}%  \\|  6h: {e(f'{h6:+.2f}')}%  \\|  24h: {e(f'{h24:+.2f}')}%\n"
            f"{perf_line}\n"
            f"🕐 {e(now_str)}\n"
            f"[DexScreener]({dex_url}) \\| [Search on X]({x_url})\n\n"
            f"_⚠️ DYOR\\. Not financial advice\\._"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Refresh",  callback_data=f"refreshpair|{ca}"),
            InlineKeyboardButton("🔍 Sniff OG", callback_data=f"og|ca|{ca}"),
        ]])
        await query.edit_message_text(text, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=keyboard)

    except Exception as ex:
        logger.error(f"handle_refreshpair: {ex}", exc_info=True)
        try:
            await query.edit_message_text("Zoinks\\! 🐾 Refresh failed\\.", parse_mode="MarkdownV2", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try again", callback_data=f"refreshpair|{ca}")]]))
        except Exception:
            pass



# ─── Portfolio refresh callback ───────────────────────────────────────────────

async def handle_portfolio_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return
    parts = query.data.split("|")
    if len(parts) < 3 or parts[0] != "portfolio":
        return
    try:
        user_id = int(parts[2])
    except (ValueError, IndexError):
        user_id = query.from_user.id if query.from_user else 0

    await query.edit_message_text("🔄 Refreshing your bag\.\.\.", parse_mode="MarkdownV2")
    from portfolio import build_portfolio_snapshot, portfolio_keyboard
    http = context.bot_data["http"]
    try:
        text = await build_portfolio_snapshot(user_id, http, context.bot_data)
        await query.edit_message_text(text, parse_mode="MarkdownV2",
                                       disable_web_page_preview=True,
                                       reply_markup=portfolio_keyboard(user_id))
    except Exception as ex:
        logger.error(f"handle_portfolio_refresh: {ex}", exc_info=True)
        try:
            await query.edit_message_text("Zoinks! 🐾 Couldn't refresh portfolio. Try again!")
        except Exception:
            pass


# ─── Noop ─────────────────────────────────────────────────────────────────────

async def handle_noop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()


# ─── Menu callback ────────────────────────────────────────────────────────────

async def handle_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return
    parts = query.data.split("|", 1)
    if len(parts) != 2 or parts[0] != "menu":
        return
    action = parts[1]

    if action == "home":
        await query.edit_message_text(
            "\U0001f43e *Scuby OG Finder*\n\nDrop a `$TICKER` or contract address in chat, or pick an option below:",
            parse_mode="MarkdownV2",
            reply_markup=main_menu_keyboard(),
        )
    elif action == "sniff":
        await query.edit_message_text(
            "\U0001f50d *How to sniff a token:*\n\n\u2022 Just drop `$BONK` or a CA in this chat\n\u2022 Or type `/sniff $BONK`\n\nScuby will sniff out the OG and show you all versions\\!",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\U0001f3e0 Main Menu", callback_data="menu|home")]]),
        )
    elif action == "leaderboard":
        await query.edit_message_text("\U0001f3c6 Building the leaderboard\\.\\.\\. hold on, raggy\\!", parse_mode="MarkdownV2")
        http        = context.bot_data["http"]
        scan_prices = context.bot_data.get("scan_prices", {})
        chat_id     = str(query.message.chat.id) if query.message else ""
        try:
            text = await build_leaderboard_text(scan_prices, http, context.bot_data, chat_id)
            await query.edit_message_text(text, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=leaderboard_keyboard())
        except Exception as e:
            logger.error(f"menu leaderboard error: {e}", exc_info=True)
            try:
                await query.edit_message_text("Zoinks! Scuby couldn't build the leaderboard. Try again!")
            except Exception:
                pass
    elif action == "alerts":
        user_id     = query.from_user.id if query.from_user else None
        alerts: dict = context.bot_data.get("alerts", {})
        user_alerts  = [v for v in alerts.values() if v.get("user_id") == user_id]
        if not user_alerts:
            await query.edit_message_text(
                "\U0001f514 *Your active alerts*\n\nYou have no active alerts\\.\n\nSet one with:\n`/alert <CA> <multiple>`",
                parse_mode="MarkdownV2",
                reply_markup=my_alerts_keyboard([]),
            )
        else:
            e     = escape_md
            lines = ["\U0001f514 *Your active alerts:*\n"]
            for alert in user_alerts:
                sym = alert.get('symbol', '?')
                tgt = f"{alert['target_multiple']:.1f}"
                ca_ = alert['ca']
                lines.append(f"\u2022 *{e(sym)}* \u2014 *{e(tgt)}x*\n  `{e(ca_)}`")
            await query.edit_message_text("\n".join(lines), parse_mode="MarkdownV2", reply_markup=my_alerts_keyboard(user_alerts))
    elif action == "monitoring":
        chat_id        = str(query.message.chat.id) if query.message else None
        monitors: dict = context.bot_data.get("monitors", {})
        chat_monitors  = monitors.get(chat_id, []) if chat_id else []
        if not chat_monitors:
            await query.edit_message_text(
                "📡 *Monitoring*\n\nScuby isn't monitoring anything here yet\\!\n\nStart with:\n`/monitor <CA> 3m`",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")]]),
            )
        else:
            e     = escape_md
            lines = ["📡 *Active monitors:*\n"]
            for i, m in enumerate(chat_monitors, 1):
                lines.append(f"*{i}\\.* *{e(m.get('symbol','?'))}* — every {m['interval_secs']//60}m")
            await query.edit_message_text("\n".join(lines), parse_mode="MarkdownV2", reply_markup=monitoring_keyboard(int(chat_id), chat_monitors))
    elif action == "feed":
        chat_id    = str(query.message.chat.id) if query.message else None
        feeds: dict = context.bot_data.get("feeds", {})
        chat_feeds  = feeds.get(chat_id, []) if chat_id else []
        e = escape_md
        if not chat_feeds:
            await query.edit_message_text(
                "📶 *Momentum Feed*\n\nNo active feeds here yet\\.\n\n"
                "Start one:\n`/feed up 20% 1h`\n`/feed down 30% 24h`",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")]]),
            )
        else:
            lines = ["📶 *Active feeds:*\n"]
            for i, feed in enumerate(chat_feeds, 1):
                arrow   = "▲" if feed["direction"] == "up" else "▼"
                thr_str = f"{feed['threshold']:.0f}"
                tf_str  = feed['timeframe']
                lines.append(f"*{i}\\.* {arrow} *{e(thr_str)}%\\+* in *{e(tf_str)}*")
            await query.edit_message_text("\n".join(lines), parse_mode="MarkdownV2", reply_markup=feeds_keyboard(int(chat_id), chat_feeds))
    elif action == "watching":
        chat_id         = str(query.message.chat.id) if query.message else None
        watchlist: dict = context.bot_data.get("watchlist", {})
        chat_watches    = watchlist.get(chat_id, []) if chat_id else []
        if not chat_watches:
            await query.edit_message_text(
                "👁 *Watching*\n\nScuby isn't watching for anything here yet\\!\n\nStart with:\n`/watch dog`",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")]]),
            )
        else:
            e     = escape_md
            lines = ["🔭 *Active watches:*\n"]
            for i, w in enumerate(chat_watches, 1):
                lines.append(f"*{i}\\.* {e(', '.join(f'#{t}' for t in w['terms']))} _{len(w.get('seen_cas',[]))} alert\\(s\\)_")
            await query.edit_message_text("\n".join(lines), parse_mode="MarkdownV2", reply_markup=watchlist_keyboard(int(chat_id), chat_watches))
    elif action == "help":
        await query.edit_message_text(
            "🐾 *Scuby OG Finder \u2014 Help*\n\n"
            "*Sniffing:* Drop `$TICKER` or CA in chat\n\n"
            "*AI Chat:* Just talk to Scuby\\! Or `/ask <question>`\n\n"
            "*Leaderboard:* `/leaderboard`\n\n"
            "*Alerts:* `/alert <CA> <Nx>` \\| `/myalerts`\n\n"
            "*Monitor:* `/monitor <CA> 5m` \\| `/monitoring`\n\n"
            "*Feed:* `/feed up 20% 1h` \\| `/screener down 30% 24h`\n\n"
            "*Smart Filter:* `/smartwatch mcap between 10k and 20k`\n\n"
            "*Watcher:* `/watch dog` \\| `/watching`\n\n"
            "_⚠️ DYOR, raggy\\. Not financial advice\\._",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")]]),
        )


# ─── Dual card refresh callback ───────────────────────────────────────────────

async def handle_dual_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return

    parts = query.data.split("|", 2)
    if len(parts) != 3 or parts[0] != "dualrefresh":
        return

    _, query_type, query_value = parts
    await query.edit_message_text("🔄 Refreshing\\.\\.\\. sniffing for the latest data\\!", parse_mode="MarkdownV2")

    http        = context.bot_data["http"]
    scan_prices = context.bot_data.setdefault("scan_prices", {})
    _bust_cache(context.bot_data, query_type, query_value)

    try:
        pairs = await fetch_pairs_and_cache(query_type, query_value, http, context.bot_data)
        if not pairs:
            await query.edit_message_text("Ruh\\-roh\\! 🐾 Couldn't find that token\\.", parse_mode="MarkdownV2", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try again", callback_data=f"dualrefresh|{query_type}|{query_value}")]]))
            return

        og = find_og(pairs)
        if not og:
            await query.edit_message_text("Jeepers\\! Trail went cold\\. 🔍", parse_mode="MarkdownV2")
            return

        scanned_pair = pairs[0]
        if query_type == "ca":
            scanned_pair = next((p for p in pairs if p.get("baseToken", {}).get("address", "") == query_value), pairs[0])

        og_ca      = og.get("baseToken", {}).get("address", "")
        scanned_ca = scanned_pair.get("baseToken", {}).get("address", "")
        is_same    = scanned_ca == og_ca
        cas_to_fetch = [scanned_ca] if is_same else list({scanned_ca, og_ca})

        if is_same:
            og_report, live_prices = await asyncio.gather(fetch_rugcheck(og_ca, http), fetch_current_prices(cas_to_fetch, http))
            scanned_report = og_report
        else:
            scanned_report, og_report, live_prices = await asyncio.gather(fetch_rugcheck(scanned_ca, http), fetch_rugcheck(og_ca, http), fetch_current_prices(cas_to_fetch, http))

        text     = format_dual_card(scanned_pair, og, len(pairs), parse_risk_badge(scanned_report), parse_risk_badge(og_report), scan_prices, live_prices)
        keyboard = dual_keyboard(query_type, query_value, show_versions=len(pairs) > 1)
        await query.edit_message_text(text, parse_mode="MarkdownV2", disable_web_page_preview=True, reply_markup=keyboard)

    except Exception as ex:
        logger.error(f"handle_dual_refresh error: {ex}", exc_info=True)
        try:
            await query.edit_message_text("Zoinks\\! 🐾 Refresh failed\\.", parse_mode="MarkdownV2", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try again", callback_data=f"dualrefresh|{query_type}|{query_value}")]]))
        except Exception:
            pass


# ─── Delete alert callback ────────────────────────────────────────────────────

async def handle_delalert_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return
    parts = query.data.split("|", 1)
    if len(parts) != 2 or parts[0] != "delalert":
        return

    ca        = parts[1]
    user_id   = query.from_user.id if query.from_user else None
    alert_key = f"{user_id}:{ca}"
    alerts: dict = context.bot_data.setdefault("alerts", {})

    if alert_key in alerts:
        symbol = alerts[alert_key].get("symbol", ca[:8])
        del alerts[alert_key]
        await save_alerts_async(alerts)
        user_alerts = [v for v in alerts.values() if v.get("user_id") == user_id]
        e = escape_md
        try:
            if not user_alerts:
                await query.edit_message_text(f"\u2705 Alert for *{e(symbol)}* cancelled\\.\n\nNo more active alerts\\.", parse_mode="MarkdownV2", reply_markup=my_alerts_keyboard([]))
            else:
                lines = [f"\u2705 Alert for *{e(symbol)}* cancelled\\.\n\n\U0001f514 *Remaining:*\n"]
                for alert in user_alerts:
                    sym = alert.get('symbol', '?')
                    tgt = f"{alert['target_multiple']:.1f}"
                    lines.append(f"\u2022 *{e(sym)}* \u2014 *{e(tgt)}x*")
                await query.edit_message_text("\n".join(lines), parse_mode="MarkdownV2", reply_markup=my_alerts_keyboard(user_alerts))
        except Exception as ex:
            logger.warning(f"handle_delalert_button edit failed: {ex}")
    else:
        try:
            await query.edit_message_text("Ruh-roh! That alert wasn't found. \U0001f43e", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("\U0001f3e0 Main Menu", callback_data="menu|home")]]))
        except Exception:
            pass


# ─── Delete monitor callback ──────────────────────────────────────────────────

async def handle_delmonitor_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return
    parts = query.data.split("|", 1)
    if len(parts) != 2 or parts[0] != "delmonitor":
        return

    monitor_id = parts[1]
    chat_id    = str(query.message.chat.id) if query.message else None
    if not chat_id:
        return

    monitors: dict = context.bot_data.setdefault("monitors", {})
    before    = monitors.get(chat_id, [])
    removed   = [m for m in before if m["id"] == monitor_id]
    monitors[chat_id] = [m for m in before if m["id"] != monitor_id]
    await save_monitors_async(monitors)

    remaining = monitors.get(chat_id, [])
    label     = removed[0].get("symbol", "that monitor") if removed else "that monitor"
    e = escape_md
    try:
        if not remaining:
            await query.edit_message_text(f"✅ Stopped monitoring *{e(label)}*\\.\n\n🐾 No active monitors here now\\.", parse_mode="MarkdownV2", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")]]))
        else:
            lines = [f"✅ Stopped monitoring *{e(label)}*\\.\n\n📡 *Still active:*\n"]
            for m in remaining:
                lines.append(f"• *{e(m.get('symbol','?'))}* — every {m['interval_secs']//60}m")
            await query.edit_message_text("\n".join(lines), parse_mode="MarkdownV2", reply_markup=monitoring_keyboard(int(chat_id), remaining))
    except Exception as ex:
        logger.warning(f"handle_delmonitor_button edit failed: {ex}")


# ─── Delete watch callback ────────────────────────────────────────────────────

async def handle_delwatch_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await _check_callback_rate_limit(query, context):
        return
    parts = query.data.split("|", 1)
    if len(parts) != 2 or parts[0] != "delwatch":
        return

    watch_id  = parts[1]
    chat_id   = str(query.message.chat.id) if query.message else None
    if not chat_id:
        return

    watchlist: dict = context.bot_data.setdefault("watchlist", {})
    before    = watchlist.get(chat_id, [])
    removed   = [w for w in before if w["id"] == watch_id]
    watchlist[chat_id] = [w for w in before if w["id"] != watch_id]
    await save_watchlist_async(watchlist)

    remaining = watchlist.get(chat_id, [])
    term_str  = ", ".join(f"#{t}" for t in removed[0]["terms"]) if removed else "that watch"
    e = escape_md
    try:
        if not remaining:
            await query.edit_message_text(f"✅ Stopped watching *{e(term_str)}*\\.\n\n🐾 Not watching anything now\\.", parse_mode="MarkdownV2", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")]]))
        else:
            lines = [f"✅ Stopped watching *{e(term_str)}*\\.\n\n🔭 *Still active:*\n"]
            for w in remaining:
                lines.append(f"• {e(', '.join(f'#{t}' for t in w['terms']))}")
            await query.edit_message_text("\n".join(lines), parse_mode="MarkdownV2", reply_markup=watchlist_keyboard(int(chat_id), remaining))
    except Exception as ex:
        logger.warning(f"handle_delwatch_button edit failed: {ex}")

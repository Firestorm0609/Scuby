"""
jobs.py — background jobs: price alerts, mcap monitor pings, keyword watcher.
All jobs import from utils.py only.
"""

import logging
import time
import re
from datetime import datetime, timezone

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from utils import (
    escape_md,
    safe_float,
    fetch_current_prices,
    save_alerts_async,
    save_monitors_async,
    save_watchlist_async,
    dex_get,
    token_matches_patterns,
    watch_alert_intro,
    format_monitor_ping,
    _calc_perf_label,
    WATCH_MIN_LIQUIDITY,
    WATCH_MAX_AGE_HOURS,
)

logger = logging.getLogger(__name__)


async def check_alerts_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    alerts: dict = context.bot_data.get("alerts", {})
    if not alerts:
        return

    already_fired: set = context.bot_data.setdefault("fired_alerts", set())
    http: httpx.AsyncClient = context.bot_data["http"]
    cas = list({v["ca"] for v in alerts.values()})

    try:
        current_prices = await fetch_current_prices(cas, http)
    except Exception as e:
        logger.warning(f"check_alerts_job: price fetch failed: {e}")
        return

    triggered_keys = []
    for key, alert in list(alerts.items()):
        if key in already_fired:
            triggered_keys.append(key)
            continue

        ca       = alert["ca"]
        current  = current_prices.get(ca, 0)
        baseline = alert.get("baseline_price", 0)
        if baseline <= 0 or current <= 0:
            continue

        multiple = current / baseline
        if multiple >= alert["target_multiple"]:
            symbol  = alert.get("symbol", ca[:8])
            label   = _calc_perf_label(multiple)
            dex_url = f"https://dexscreener.com/solana/{ca}"
            e       = escape_md
            try:
                await context.bot.send_message(
                    chat_id=alert["user_id"],
                    text=(
                        f"\U0001f514 *Price alert triggered\\!*\n\n"
                        f"[{e(symbol)}]({dex_url}) just hit "
                        f"*{e(f'{multiple:.2f}')}x* from your baseline of "
                        f"${e(f'{baseline:.8g}')} "
                        f"\\({e(label)}\\)\\.\n\n"
                        f"_\u26a0\ufe0f DYOR\\. Not financial advice\\._"
                    ),
                    parse_mode="MarkdownV2",
                    disable_web_page_preview=True,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔍 Sniff OG",  callback_data=f"og|ca|{ca}"),
                        InlineKeyboardButton("🔄 Refresh",   callback_data=f"refreshpair|{ca}"),
                    ]]),
                )
                already_fired.add(key)
                triggered_keys.append(key)
                logger.info(f"Alert fired for user {alert['user_id']}: {symbol} @ {multiple:.2f}x")
            except Exception as send_err:
                logger.warning(
                    f"check_alerts_job: failed to DM user {alert['user_id']} — "
                    f"alert kept active for retry: {send_err}"
                )

    if triggered_keys:
        fired_snapshots = {k: alerts[k] for k in triggered_keys if k in alerts}
        for key in triggered_keys:
            alerts.pop(key, None)
        try:
            await save_alerts_async(alerts)
            already_fired.difference_update(triggered_keys)
        except Exception as e:
            logger.error(
                f"check_alerts_job: failed to save after firing — "
                f"restoring {len(triggered_keys)} alert(s). "
                f"fired_alerts guard prevents duplicate DMs: {e}",
                exc_info=True,
            )
            alerts.update(fired_snapshots)


async def monitor_ping_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    monitors: dict = context.bot_data.get("monitors", {})
    if not monitors:
        return

    http: httpx.AsyncClient = context.bot_data["http"]
    now     = time.time()
    changed = False

    due: list[tuple[str, dict]] = [
        (chat_id_str, m)
        for chat_id_str, chat_monitors in monitors.items()
        for m in chat_monitors
        if now - m.get("last_sent_ts", 0) >= m["interval_secs"]
    ]
    if not due:
        return

    due_cas = list({m["ca"] for _, m in due})
    try:
        resp = await http.get(
            f"https://api.dexscreener.com/latest/dex/tokens/{','.join(due_cas)}"
        )
        resp.raise_for_status()
        raw_json = resp.json()
        raw = raw_json if isinstance(raw_json, dict) else {}
        pairs_by_ca: dict[str, dict] = {}
        for pair in (raw.get("pairs") or []):
            if pair.get("chainId") != "solana":
                continue
            ca = pair.get("baseToken", {}).get("address", "")
            if ca and ca not in pairs_by_ca:
                pairs_by_ca[ca] = pair
    except Exception as e:
        logger.warning(f"monitor_ping_job: price fetch failed: {e}")
        return

    for chat_id_str, m in due:
        ca     = m["ca"]
        pair   = pairs_by_ca.get(ca)
        symbol = m.get("symbol", ca[:8])

        if not pair:
            logger.info(f"monitor_ping_job: no pair data for {ca}, skipping")
            continue

        text     = format_monitor_ping(pair, symbol, m["interval_secs"])
        keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("🔄 Refresh",    callback_data=f"refreshpair|{ca}"),
                InlineKeyboardButton("🔍 Sniff OG",   callback_data=f"og|ca|{ca}"),
            ],
            [
                InlineKeyboardButton(f"❌ Stop monitoring {symbol}", callback_data=f"delmonitor|{m['id']}"),
            ],
        ])

        try:
            await context.bot.send_message(
                chat_id=int(chat_id_str),
                text=text,
                parse_mode="MarkdownV2",
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )
            m["last_sent_ts"] = now
            changed = True
            logger.info(f"Monitor ping sent for {symbol} to chat {chat_id_str}")
        except Exception as send_err:
            logger.warning(f"monitor_ping_job: failed to ping chat {chat_id_str} for {symbol}: {send_err}")

    if changed:
        await save_monitors_async(monitors)


async def watch_scan_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    watchlist: dict = context.bot_data.get("watchlist", {})
    if not watchlist:
        return

    http: httpx.AsyncClient = context.bot_data["http"]

    try:
        data  = await dex_get(http, "https://api.dexscreener.com/latest/dex/search", params={"q": "solana"})
        pairs = [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
    except Exception as e:
        logger.warning(f"watch_scan_job: failed to fetch pairs: {e}")
        return

    if not pairs:
        return

    e             = escape_md
    now_ms        = time.time() * 1000
    age_cutoff_ms = now_ms - WATCH_MAX_AGE_HOURS * 3600 * 1000
    changed       = False

    for chat_id_str, chat_watches in list(watchlist.items()):
        if not chat_watches:
            continue

        for watch in chat_watches:
            patterns     = watch.get("patterns", [])
            seen_cas: list = watch.setdefault("seen_cas", [])
            seen_cas_set   = set(seen_cas)
            terms          = watch.get("terms", [])

            for pair in pairs:
                ca = pair.get("baseToken", {}).get("address", "")
                if not ca or ca in seen_cas_set:
                    continue

                created_ts = pair.get("pairCreatedAt") or 0
                if created_ts and created_ts < age_cutoff_ms:
                    continue

                liq = safe_float((pair.get("liquidity") or {}).get("usd", 0))
                if liq < WATCH_MIN_LIQUIDITY:
                    continue

                name   = pair.get("baseToken", {}).get("name",   "")
                symbol = pair.get("baseToken", {}).get("symbol", "")
                if not token_matches_patterns(name, symbol, patterns):
                    continue

                fdv      = safe_float(pair.get("fdv"))
                liq_usd  = safe_float((pair.get("liquidity") or {}).get("usd", 0))
                vol24    = safe_float((pair.get("volume")    or {}).get("h24", 0))
                price    = pair.get("priceUsd") or "?"
                dex_url  = f"https://dexscreener.com/solana/{ca}"
                x_url    = f"https://x.com/search?q={ca}"
                matched_term = e(", ".join(f"#{t}" for t in terms))

                launched_line = ""
                if created_ts:
                    launched_str  = datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc).strftime("%b %d, %H:%M UTC")
                    launched_line = f"🕐 Just launched: {e(launched_str)}\n"

                text = (
                    f"🐾 *{watch_alert_intro()}*\n\n"
                    f"👀 Matched watch: {matched_term}\n\n"
                    f"🏷 *{e(symbol)}* — {e(name)}\n"
                    f"`{e(ca)}`\n\n"
                    f"{launched_line}"
                    f"💰 Price: ${e(price)}\n"
                    f"💎 FDV: ${e(f'{fdv:,.0f}')}\n"
                    f"💧 Liq: ${e(f'{liq_usd:,.0f}')}\n"
                    f"📊 Vol 24h: ${e(f'{vol24:,.0f}')}\n\n"
                    f"[DexScreener]({dex_url}) \\| [X Search]({x_url})\n\n"
                    f"_⚠️ DYOR, raggy\\. Not financial advice\\._"
                )
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton(f"🔍 Sniff the OG ${symbol}", callback_data=f"og|ca|{ca}"),
                    InlineKeyboardButton("🔄 Refresh",                 callback_data=f"refreshpair|{ca}"),
                ]])

                try:
                    await context.bot.send_message(
                        chat_id=int(chat_id_str),
                        text=text,
                        parse_mode="MarkdownV2",
                        disable_web_page_preview=True,
                        reply_markup=keyboard,
                    )
                    seen_cas.append(ca)
                    if len(seen_cas) > 500:
                        watch["seen_cas"] = seen_cas[-500:]
                    changed = True
                    logger.info(f"Watch alert sent to {chat_id_str}: {symbol} matched {terms}")
                except Exception as send_err:
                    logger.warning(f"watch_scan_job: failed to alert chat {chat_id_str}: {send_err}")

    if changed:
        await save_watchlist_async(watchlist)

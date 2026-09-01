"""proactive.py — Scuby's proactive alpha engine.

Jobs that run in the background and push insights without being asked:
  • Whale alert   — detects sudden large volume spikes on tracked tokens
  • Rug watchdog  — if a tracked token's liquidity drops >50% suddenly, warn the chat
"""

import asyncio
import logging
import time
from datetime import datetime, timezone

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from utils import escape_md, safe_float, dex_get

logger = logging.getLogger(__name__)

PROACTIVE_FILE         = "proactive_settings.json"
WHALE_VOL_MULTIPLIER   = 5.0
WHALE_MIN_VOL_USD      = 50_000
LIQ_DROP_THRESHOLD     = 0.45

import json, copy

def load_proactive_settings() -> dict:
    try:
        with open(PROACTIVE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_proactive_settings(s: dict) -> None:
    try:
        with open(PROACTIVE_FILE, "w") as f:
            json.dump(s, f)
    except Exception as e:
        logger.warning(f"Could not save proactive settings: {e}")

async def save_proactive_settings_async(s: dict) -> None:
    await asyncio.to_thread(save_proactive_settings, copy.deepcopy(s))


# ─── Shared helper: fetch monitored token pairs ──────────────────────────────

async def _fetch_monitored_pairs(
    http: httpx.AsyncClient,
    monitors: dict,
) -> tuple[list[dict], dict[str, list[str]]]:
    """
    Collect all monitored CAs and batch-fetch their pair data.
    Returns (pairs, all_cas) where all_cas maps CA → list of chat_id_str.
    Shared by rug_watchdog_job and whale_alert_job to avoid duplicate API calls.
    """
    all_cas: dict[str, list[str]] = {}
    for chat_id_str, chat_monitors in monitors.items():
        for m in chat_monitors:
            all_cas.setdefault(m["ca"], []).append(chat_id_str)

    if not all_cas:
        return [], all_cas

    cas_str = ",".join(list(all_cas.keys())[:30])
    data    = await dex_get(http, f"https://api.dexscreener.com/latest/dex/tokens/{cas_str}")
    pairs   = [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
    return pairs, all_cas


# ─── Rug watchdog ─────────────────────────────────────────────────────────────

async def rug_watchdog_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    monitors: dict          = context.bot_data.get("monitors", {})
    http: httpx.AsyncClient = context.bot_data["http"]
    liq_cache: dict         = context.bot_data.setdefault("liq_cache", {})

    try:
        pairs, all_cas = await _fetch_monitored_pairs(http, monitors)
    except Exception as ex:
        logger.debug(f"rug_watchdog_job fetch failed: {ex}")
        return

    if not pairs:
        return

    e = escape_md
    for pair in pairs:
        ca      = pair.get("baseToken", {}).get("address", "")
        symbol  = pair.get("baseToken", {}).get("symbol", "?")
        liq     = safe_float((pair.get("liquidity") or {}).get("usd", 0))
        prev    = liq_cache.get(ca)

        rug_confirm: dict = context.bot_data.setdefault("rug_confirm", {})
        if prev and prev > 5000 and liq > 500:
            drop_pct = (prev - liq) / prev
            drop_abs = prev - liq
            if drop_pct >= LIQ_DROP_THRESHOLD and drop_abs >= 2000:
                rug_confirm[ca] = rug_confirm.get(ca, 0) + 1
            else:
                rug_confirm[ca] = 0

            if rug_confirm.get(ca, 0) < 2:
                if liq > 0:
                    liq_cache[ca] = liq
                continue

            if rug_confirm.get(ca, 0) >= 2:
                rug_confirm[ca] = 0
                dex_url = f"https://dexscreener.com/solana/{ca}"
                text = (
                    f"🚨 *RUG ALERT — {e(symbol)}*\n\n"
                    f"Liquidity just dropped *{e(f'{drop_pct*100:.0f}')}%* in the last few minutes\\!\n\n"
                    f"💧 Was: ${e(f'{prev:,.0f}')} → Now: ${e(f'{liq:,.0f}')}\n\n"
                    f"Something looks suspicious\\! This could be a rug\\._\n\n"
                    f"[Check DexScreener]({dex_url})\n\n"
                    f"_⚠️ DYOR\\. Not financial advice\\._"
                )
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔍 Check token", callback_data=f"og|ca|{ca}"),
                    InlineKeyboardButton("🔄 Refresh liq", callback_data=f"refreshpair|{ca}"),
                ]])
                for chat_id_str in all_cas.get(ca, []):
                    try:
                        await context.bot.send_message(
                            chat_id=int(chat_id_str),
                            text=text,
                            parse_mode="MarkdownV2",
                            disable_web_page_preview=True,
                            reply_markup=keyboard,
                        )
                        logger.warning(f"Rug alert sent: {symbol} liq drop {drop_pct*100:.0f}% in chat {chat_id_str}")
                    except Exception as send_err:
                        logger.debug(f"rug_watchdog send failed: {send_err}")

        if liq > 500:
            liq_cache[ca] = liq


# ─── Whale alert ──────────────────────────────────────────────────────────────

async def whale_alert_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    monitors: dict          = context.bot_data.get("monitors", {})
    http: httpx.AsyncClient = context.bot_data["http"]
    vol_cache: dict         = context.bot_data.setdefault("vol_cache", {})
    whale_fired: set        = context.bot_data.setdefault("whale_fired", set())

    try:
        pairs, all_cas = await _fetch_monitored_pairs(http, monitors)
    except Exception as ex:
        logger.debug(f"whale_alert_job fetch failed: {ex}")
        return

    if not pairs:
        return

    e   = escape_md
    now = time.time()

    for pair in pairs:
        ca      = pair.get("baseToken", {}).get("address", "")
        symbol  = pair.get("baseToken", {}).get("symbol", "?")
        vol_5m  = safe_float((pair.get("volume") or {}).get("m5", 0))
        vol_1h  = safe_float((pair.get("volume") or {}).get("h1", 0))
        price   = pair.get("priceUsd") or "?"
        mcap    = safe_float(pair.get("marketCap") or pair.get("fdv") or 0)
        h1      = safe_float((pair.get("priceChange") or {}).get("h1", 0))

        expected_5m = vol_1h / 12 if vol_1h > 0 else 0
        fire_key    = f"whale:{ca}:{int(now // 300)}"

        if (
            vol_5m >= WHALE_MIN_VOL_USD
            and expected_5m > 0
            and vol_5m >= expected_5m * WHALE_VOL_MULTIPLIER
            and fire_key not in whale_fired
        ):
            dex_url = f"https://dexscreener.com/solana/{ca}"
            text = (
                f"🐋 *Whale Alert — {e(symbol)}\\!*\n\n"
                f"  Huge volume spike detected in the last 5 minutes\\!\n\n"
                f"📊 5m Vol: *${e(f'{vol_5m:,.0f}')}* "
                f"\\({e(f'{vol_5m/expected_5m:.1f}')}x normal\\)\n"
                f"💰 Price: ${e(str(price))}\n"
                f"📈 1h change: {e(f'{h1:+.1f}')}%\n"
                f"💎 MCap: ${e(f'{mcap:,.0f}')}\n\n"
                f"Something big is happening\\!_\n\n"
                f"[DexScreener]({dex_url})\n\n"
                f"_⚠️ DYOR\\. Not financial advice\\._"
            )
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔍 Sniff OG", callback_data=f"og|ca|{ca}"),
                InlineKeyboardButton("🔄 Refresh",  callback_data=f"refreshpair|{ca}"),
            ]])
            for chat_id_str in all_cas.get(ca, []):
                try:
                    await context.bot.send_message(
                        chat_id=int(chat_id_str),
                        text=text,
                        parse_mode="MarkdownV2",
                        disable_web_page_preview=True,
                        reply_markup=keyboard,
                    )
                    whale_fired.add(fire_key)
                    logger.info(f"Whale alert: {symbol} {vol_5m/expected_5m:.1f}x vol spike in {chat_id_str}")
                except Exception as send_err:
                    logger.debug(f"whale_alert send failed: {send_err}")

        vol_cache[ca] = {"vol_1h": vol_1h, "ts": now}

    context.bot_data["whale_fired"] = {k for k in whale_fired if int(k.split(":")[-1]) > int(now // 300) - 12}

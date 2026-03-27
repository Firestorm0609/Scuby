"""
feeds.py — momentum feed scanner for Scuby OG Finder.

Every FEED_POLL_INTERVAL seconds, polls DexScreener for active Solana pairs
and posts any that cross a user-configured price-change threshold into the
chat where the feed was set up.

Also exposes run_screener() for the one-off /screener command.
"""

import asyncio
import copy
import json
import logging
import time
from datetime import datetime, timezone

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

try:
    from rate_limiter import dex_get_safe as _dex_get_rl, dex_search_many
    _RATE_LIMITER_AVAILABLE = True
except ImportError:
    _RATE_LIMITER_AVAILABLE = False
    dex_search_many = None
from utils import (
    escape_md,
    safe_float,
    dex_get,
    WATCH_MIN_LIQUIDITY,
)

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
FEEDS_FILE         = "feeds.json"
FEED_POLL_INTERVAL = 600          # 10 minutes
MAX_FEEDS_PER_CHAT = 10
FEED_SEEN_CAP      = 1000         # max CAs remembered per feed before pruning

# DexScreener priceChange object field names (what the API actually returns)
TIMEFRAME_FIELD = {
    "5m":  "m5",
    "1h":  "h1",
    "6h":  "h6",
    "24h": "h24",
}
VALID_TIMEFRAMES = list(TIMEFRAME_FIELD.keys())

# How many results to show per scan (cap to avoid chat spam in feed)
MAX_ALERTS_PER_SCAN  = 8
# How many results to show for a one-off /screener command
MAX_SCREENER_RESULTS = 20

# Minimum liquidity for a token to appear in feed results
FEED_MIN_LIQUIDITY = 1000


# ── Persistence ───────────────────────────────────────────────────────────────

def load_feeds() -> dict:
    try:
        with open(FEEDS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_feeds(feeds: dict) -> None:
    try:
        with open(FEEDS_FILE, "w") as f:
            json.dump(feeds, f)
    except Exception as e:
        logger.warning(f"Could not save feeds: {e}")


async def save_feeds_async(feeds: dict) -> None:
    await asyncio.to_thread(save_feeds, copy.deepcopy(feeds))


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_feed_args(args: list[str]) -> tuple[str, float, str] | None:
    """
    Parse direction, threshold, timeframe from command args.
    Accepts forms like: up 20% 1h  /  down 30 24h  /  up 50% 5m
    Returns (direction, threshold, timeframe) or None if invalid.
    """
    if len(args) < 3:
        return None

    direction = args[0].lower()
    if direction not in ("up", "down"):
        return None

    # threshold — strip % if present
    try:
        threshold = float(args[1].rstrip("%"))
        if threshold <= 0:
            return None
    except ValueError:
        return None

    timeframe = args[2].lower().replace("m5", "5m")
    if timeframe not in VALID_TIMEFRAMES:
        return None

    return direction, threshold, timeframe


def fmt_mcap(v: float) -> str:
    if v >= 1e9: return f"${v/1e9:.2f}B"
    if v >= 1e6: return f"${v/1e6:.2f}M"
    if v >= 1e3: return f"${v/1e3:.1f}K"
    return f"${v:.0f}"


def fmt_price(p: float) -> str:
    if p == 0:      return "$0"
    if p >= 100:    return f"${p:,.2f}"
    if p >= 1:      return f"${p:.4f}"
    if p >= 0.001:  return f"${p:.6f}"
    return f"${p:.10f}".rstrip("0")


def direction_emoji(direction: str, pct: float) -> str:
    if direction == "up":
        if abs(pct) >= 100: return "🌙"
        if abs(pct) >= 50:  return "🚀"
        return "📈"
    else:
        if abs(pct) >= 70:  return "💀"
        if abs(pct) >= 40:  return "🩸"
        return "📉"


def feed_label(direction: str, threshold: float, timeframe: str) -> str:
    arrow = "▲" if direction == "up" else "▼"
    return f"{arrow} {threshold:.0f}%+ / {timeframe}"


# ── Core scanner ──────────────────────────────────────────────────────────────

async def _fetch_token_profiles(http: httpx.AsyncClient) -> list[dict]:
    """
    Try DexScreener's token-profiles endpoint which returns recently active
    tokens on all chains — filter to Solana. Falls back to empty list.
    """
    try:
        resp = await http.get(
            "https://api.dexscreener.com/token-profiles/latest/v1",
            timeout=10,
        )
        resp.raise_for_status()
        items = resp.json() if isinstance(resp.json(), list) else []
        return [i for i in items if i.get("chainId") == "solana"]
    except Exception as e:
        logger.debug(f"_fetch_token_profiles: {e}")
        return []


async def _fetch_boosted_tokens(http: httpx.AsyncClient) -> list[dict]:
    """
    Try DexScreener's boosted-tokens endpoint — these are actively traded.
    """
    try:
        resp = await http.get(
            "https://api.dexscreener.com/token-boosts/latest/v1",
            timeout=10,
        )
        resp.raise_for_status()
        items = resp.json() if isinstance(resp.json(), list) else []
        return [i for i in items if i.get("chainId") == "solana"]
    except Exception as e:
        logger.debug(f"_fetch_boosted_tokens: {e}")
        return []


async def scan_movers(
    http: httpx.AsyncClient,
    direction: str,
    threshold: float,
    timeframe: str,
    exclude_cas: set[str] | None = None,
    limit: int = MAX_ALERTS_PER_SCAN,
) -> list[dict]:
    """
    Query DexScreener for Solana pairs that have moved >= threshold %
    in the given direction and timeframe.

    Strategy (layered, most-to-least diverse):
    1. Try token-profiles + boosted-tokens endpoints for recently active CAs,
       then batch-fetch their pair data via /tokens/<ca1,ca2,...>
    2. Fire 40 parallel diverse search queries covering different token themes
       to pool as many unique Solana pairs as possible
    3. Deduplicate the combined pool and filter by price change threshold

    Returns a list of matching pair dicts, up to MAX_ALERTS_PER_SCAN.
    """
    field   = TIMEFRAME_FIELD.get(timeframe, "h1")
    exclude = exclude_cas if exclude_cas is not None else set()

    # ── Layer 1: token profiles + boosted ────────────────────────────────────
    profile_pairs: list[dict] = []
    try:
        profiles, boosted = await asyncio.gather(
            _fetch_token_profiles(http),
            _fetch_boosted_tokens(http),
        )
        # Collect unique CAs from both sources
        profile_cas: list[str] = []
        seen_profile: set[str] = set()
        for item in (profiles + boosted):
            ca = item.get("tokenAddress") or item.get("address") or ""
            if ca and ca not in seen_profile:
                seen_profile.add(ca)
                profile_cas.append(ca)

        # Batch-fetch pair data for these CAs (30 per request)
        batch_size = 30
        batch_tasks = []
        for i in range(0, min(len(profile_cas), 300), batch_size):
            batch = profile_cas[i:i + batch_size]
            batch_tasks.append(
                dex_get(http, f"https://api.dexscreener.com/latest/dex/tokens/{','.join(batch)}")
            )
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        for result in batch_results:
            if isinstance(result, Exception):
                continue
            for pair in (result.get("pairs") or []):
                if pair.get("chainId") == "solana":
                    profile_pairs.append(pair)
    except Exception as e:
        logger.debug(f"scan_movers layer1 failed: {e}")

    # ── Layer 2: broad parallel keyword search ────────────────────────────────
    # Note: single-character queries return 400 from DexScreener — that's
    # expected and handled silently. They still expand the pool when they work.
    SEARCH_TERMS = [
        # animal memes
        "inu", "cat", "frog", "bird", "bear", "wolf", "fish", "pig",
        # culture / people
        "pepe", "trump", "elon", "grok", "chad", "based",
        # generic crypto terms that surface different tokens each
        "pump", "moon", "fire", "king", "god", "rich", "gem",
        # short queries — some 400, some work, all handled silently
        "a", "b", "c", "x", "z",
        # numeric / tech themed
        "404", "420", "1000", "ai", "gpt",
        # sol ecosystem terms
        "jup", "bonk", "wif", "myro", "bome",
    ]

    async def _safe_search(q: str) -> list[dict]:
        try:
            data = await dex_get(
                http,
                "https://api.dexscreener.com/latest/dex/search",
                params={"q": q},
            )
            return [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
        except Exception as e:
            # 400 on short queries is expected — DexScreener requires min length
            # Log at debug so it doesn't flood production logs
            logger.debug(f"scan_movers: query {q!r} skipped: {e}")
            return []

    # Use rate-limited chunked search if available, otherwise fall back to parallel
    if _RATE_LIMITER_AVAILABLE and dex_search_many is not None:
        _flat = await dex_search_many(http, SEARCH_TERMS, chunk_size=6)
        search_results = [_flat]
    else:
        search_results = await asyncio.gather(*[_safe_search(q) for q in SEARCH_TERMS])

    # ── Merge + deduplicate all sources ───────────────────────────────────────
    seen_ca: set[str] = set()
    all_pairs: list[dict] = []

    for pair in profile_pairs:
        ca = pair.get("baseToken", {}).get("address", "")
        if ca and ca not in seen_ca:
            seen_ca.add(ca)
            all_pairs.append(pair)

    for pairs in search_results:
        for pair in pairs:
            ca = pair.get("baseToken", {}).get("address", "")
            if ca and ca not in seen_ca:
                seen_ca.add(ca)
                all_pairs.append(pair)

    logger.info(
        f"scan_movers: {len(all_pairs)} unique pairs pooled "
        f"({len(profile_pairs)} from profiles, rest from search)"
    )

    # ── Filter by threshold ───────────────────────────────────────────────────
    matched: list[tuple[float, dict]] = []
    for pair in all_pairs:
        ca = pair.get("baseToken", {}).get("address", "")
        if not ca or ca in exclude:
            continue

        liq = safe_float((pair.get("liquidity") or {}).get("usd", 0))
        if liq < FEED_MIN_LIQUIDITY:
            continue

        pct = safe_float((pair.get("priceChange") or {}).get(field, 0))
        if direction == "up"   and pct >= threshold:
            matched.append((pct, pair))
        elif direction == "down" and pct <= -threshold:
            matched.append((pct, pair))

    matched.sort(key=lambda x: x[0], reverse=(direction == "up"))
    return [pair for _, pair in matched[:limit]]


def build_mover_message(
    pair: dict,
    direction: str,
    timeframe: str,
    feed_id: str | None = None,
) -> tuple[str, "InlineKeyboardMarkup"]:
    """Build the alert message + keyboard for a single mover."""
    e       = escape_md
    field   = TIMEFRAME_FIELD.get(timeframe, "h1")
    ca      = pair.get("baseToken", {}).get("address", "?")
    symbol  = pair.get("baseToken", {}).get("symbol", "?")
    name    = pair.get("baseToken", {}).get("name",   "?")
    pct     = safe_float((pair.get("priceChange") or {}).get(field, 0))
    price   = pair.get("priceUsd") or "?"
    mcap    = safe_float(pair.get("marketCap") or pair.get("fdv") or 0)
    liq     = safe_float((pair.get("liquidity") or {}).get("usd", 0))
    vol1h   = safe_float((pair.get("volume")    or {}).get("h1",  0))
    dex_url = f"https://dexscreener.com/solana/{ca}"
    sign    = "+" if pct >= 0 else ""
    emoji   = direction_emoji(direction, pct)
    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")

    text = (
        f"{emoji} *{e(symbol)}* — {e(name)}\n"
        f"`{e(ca)}`\n\n"
        f"{'📈' if direction=='up' else '📉'} *{e(f'{sign}{pct:.1f}%')}* in {e(timeframe)}\n"
        f"💰 Price: ${e(str(price))}\n"
        f"💎 MCap:  {e(fmt_mcap(mcap))}\n"
        f"💧 Liq:   ${e(f'{liq:,.0f}')}\n"
        f"📊 Vol 1h: ${e(f'{vol1h:,.0f}')}\n\n"
        f"🕐 {e(now_str)}\n"
        f"[DexScreener]({dex_url})\n\n"
        f"_⚠️ DYOR\\. Not financial advice\\._"
    )

    buttons = [
        [
            InlineKeyboardButton("🔍 Sniff OG", callback_data=f"og|ca|{ca}"),
            InlineKeyboardButton("🔄 Refresh",  callback_data=f"refreshpair|{ca}"),
        ]
    ]
    if feed_id:
        buttons.append([
            InlineKeyboardButton("❌ Stop this feed", callback_data=f"delfeed|{feed_id}")
        ])

    return text, InlineKeyboardMarkup(buttons)


# ── Background job ────────────────────────────────────────────────────────────

async def feed_scan_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Runs every FEED_POLL_INTERVAL seconds.

    For each active feed:
    1. Scan ALL pairs currently above the threshold (no exclude).
    2. Compute new_cas = above_threshold - seen_cas (tokens to alert on).
    3. Update seen_cas = above_threshold ∩ seen_cas  (prune tokens that
       dropped below threshold so they can refire if they pump again).
    4. Send alerts for new_cas and add them to seen_cas.
    """
    feeds: dict = context.bot_data.get("feeds", {})
    if not feeds:
        return

    http: httpx.AsyncClient = context.bot_data["http"]
    changed = False

    for chat_id_str, chat_feeds in list(feeds.items()):
        if not chat_feeds:
            continue

        for feed in chat_feeds:
            direction = feed["direction"]
            threshold = feed["threshold"]
            timeframe = feed["timeframe"]
            seen_cas: list = feed.setdefault("seen_cas", [])
            seen_set:  set = set(seen_cas)

            # Step 1 — scan ALL pairs currently above threshold
            all_above = await scan_movers(
                http, direction, threshold, timeframe,
                exclude_cas=None,   # no exclude — get everything above threshold
            )
            above_cas = {
                p.get("baseToken", {}).get("address", "")
                for p in all_above
                if p.get("baseToken", {}).get("address", "")
            }

            # Step 2 — prune seen_cas: remove tokens that are no longer above threshold
            # This allows them to refire the next time they cross the threshold
            still_above = seen_set & above_cas
            dropped_out = seen_set - above_cas
            if dropped_out:
                feed["seen_cas"] = [ca for ca in seen_cas if ca in still_above]
                seen_cas = feed["seen_cas"]
                seen_set = still_above
                changed = True
                logger.debug(
                    f"feed_scan_job: pruned {len(dropped_out)} token(s) from seen_cas "
                    f"(dropped below threshold) for chat {chat_id_str}"
                )

            # Step 3 — new tokens = above threshold but not yet seen
            new_pairs = [
                p for p in all_above
                if p.get("baseToken", {}).get("address", "") not in seen_set
            ][:MAX_ALERTS_PER_SCAN]

            if not new_pairs:
                logger.debug(
                    f"feed_scan_job: no new movers for {chat_id_str} "
                    f"({direction} {threshold}% {timeframe})"
                )
                continue

            # Step 4 — send alerts
            for pair in new_pairs:
                ca = pair.get("baseToken", {}).get("address", "")
                if not ca:
                    continue

                text, keyboard = build_mover_message(
                    pair, direction, timeframe, feed_id=feed["id"]
                )
                try:
                    await context.bot.send_message(
                        chat_id=int(chat_id_str),
                        text=text,
                        parse_mode="MarkdownV2",
                        disable_web_page_preview=True,
                        reply_markup=keyboard,
                    )
                    seen_cas.append(ca)
                    seen_set.add(ca)
                    changed = True
                    logger.info(
                        f"Feed alert → chat {chat_id_str}: "
                        f"{pair.get('baseToken', {}).get('symbol', '?')} "
                        f"{direction} {threshold}% {timeframe}"
                    )
                except Exception as send_err:
                    logger.warning(
                        f"feed_scan_job: send failed to {chat_id_str}: {send_err}"
                    )

            # Prune seen_cas to cap
            if len(seen_cas) > FEED_SEEN_CAP:
                feed["seen_cas"] = seen_cas[-FEED_SEEN_CAP:]

    if changed:
        await save_feeds_async(feeds)


# ── One-off screener ──────────────────────────────────────────────────────────

async def run_screener(
    http: httpx.AsyncClient,
    direction: str,
    threshold: float,
    timeframe: str,
) -> list[dict]:
    """Run a one-off scan with no dedup — for the /screener command."""
    return await scan_movers(
        http, direction, threshold, timeframe,
        exclude_cas=set(),
        limit=MAX_SCREENER_RESULTS,
    )

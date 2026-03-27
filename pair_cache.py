"""
pair_cache.py — Shared DexScreener pair pool cache for Scuby.

PROBLEM: feeds.py, smart_filters.py, and screener each fire 40+ parallel
DexScreener queries independently. When all three run within the same minute
(common at startup and during active trading), you're making 120+ redundant
calls for the same data. This burns your 429 budget fast.

SOLUTION: One shared pair pool, refreshed every 90 seconds. All three jobs
read from the same cache. ~60% fewer DexScreener calls in practice.

Usage:
    from pair_cache import get_solana_pairs

    pairs = await get_solana_pairs(http)
    # returns deduplicated list of Solana pair dicts, max 90s stale

Force a refresh (e.g. for the /screener command):
    pairs = await get_solana_pairs(http, force=True)
"""

import asyncio
import logging
import time

import httpx

from utils import safe_float, dex_get

logger = logging.getLogger(__name__)

CACHE_TTL_SECS = 90
_cache_lock    = asyncio.Lock()
_cached_pairs: list[dict] = []
_cached_at:    float      = 0.0

# Broad queries that pool diverse tokens
_POOL_QUERIES = [
    # Animal memes (highest volume tier)
    "inu", "cat", "frog", "dog", "bird", "bear", "wolf", "fish", "pig",
    # Culture / people
    "pepe", "trump", "elon", "grok", "chad", "based",
    # Generic crypto terms
    "pump", "moon", "fire", "king", "god", "rich", "gem",
    # Short (some 400, handled silently)
    "a", "b", "c", "x", "z",
    # Numeric / tech
    "404", "420", "ai", "gpt",
    # Sol ecosystem
    "jup", "bonk", "wif", "myro", "bome",
]

# Minimum liquidity to enter the pool at all
_POOL_MIN_LIQ = 500


async def _fetch_profiles(http: httpx.AsyncClient) -> list[dict]:
    """Pull recently active CAs from the token-profiles endpoint."""
    try:
        resp = await http.get(
            "https://api.dexscreener.com/token-profiles/latest/v1", timeout=10
        )
        resp.raise_for_status()
        items = resp.json() if isinstance(resp.json(), list) else []
        cas = [
            i.get("tokenAddress") or i.get("address")
            for i in items if i.get("chainId") == "solana"
        ]
        cas = [c for c in cas if c][:60]
        if not cas:
            return []
        data = await dex_get(
            http,
            f"https://api.dexscreener.com/latest/dex/tokens/{','.join(cas[:30])}",
        )
        return [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
    except Exception as ex:
        logger.debug(f"pair_cache._fetch_profiles: {ex}")
        return []


async def _fetch_boosted(http: httpx.AsyncClient) -> list[dict]:
    """Pull boosted tokens — actively traded right now."""
    try:
        resp = await http.get(
            "https://api.dexscreener.com/token-boosts/latest/v1", timeout=10
        )
        resp.raise_for_status()
        items = resp.json() if isinstance(resp.json(), list) else []
        cas = [
            i.get("tokenAddress") or i.get("address")
            for i in items if i.get("chainId") == "solana"
        ]
        cas = [c for c in cas if c][:30]
        if not cas:
            return []
        data = await dex_get(
            http,
            f"https://api.dexscreener.com/latest/dex/tokens/{','.join(cas)}",
        )
        return [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
    except Exception as ex:
        logger.debug(f"pair_cache._fetch_boosted: {ex}")
        return []


async def _search_chunk(
    http: httpx.AsyncClient, queries: list[str]
) -> list[dict]:
    """Run a small batch of search queries concurrently."""
    async def _one(q: str) -> list[dict]:
        try:
            data = await dex_get(
                http,
                "https://api.dexscreener.com/latest/dex/search",
                params={"q": q},
            )
            return [p for p in (data.get("pairs") or []) if p.get("chainId") == "solana"]
        except Exception:
            return []

    results = await asyncio.gather(*[_one(q) for q in queries], return_exceptions=True)
    out = []
    for r in results:
        if not isinstance(r, Exception):
            out.extend(r)
    return out


async def _build_pool(http: httpx.AsyncClient) -> list[dict]:
    """Fetch a fresh deduplicated pair pool from all sources."""

    # Layer 1: profiles + boosted (most current data)
    profile_pairs, boosted_pairs = await asyncio.gather(
        _fetch_profiles(http),
        _fetch_boosted(http),
    )

    # Layer 2: chunked keyword search (6 queries at a time, 300ms pause between)
    search_pairs: list[dict] = []
    chunk_size = 6
    for i in range(0, len(_POOL_QUERIES), chunk_size):
        chunk = _POOL_QUERIES[i : i + chunk_size]
        batch = await _search_chunk(http, chunk)
        search_pairs.extend(batch)
        if i + chunk_size < len(_POOL_QUERIES):
            await asyncio.sleep(0.3)

    # Deduplicate by CA, filter dust
    seen:     set[str]   = set()
    all_pairs: list[dict] = []
    for pair in profile_pairs + boosted_pairs + search_pairs:
        ca  = pair.get("baseToken", {}).get("address", "")
        liq = safe_float((pair.get("liquidity") or {}).get("usd", 0))
        if ca and ca not in seen and liq >= _POOL_MIN_LIQ:
            seen.add(ca)
            all_pairs.append(pair)

    logger.info(
        f"pair_cache: pool refreshed — {len(all_pairs)} unique pairs "
        f"({len(profile_pairs)} profiles, {len(boosted_pairs)} boosted, "
        f"{len(search_pairs)} search)"
    )
    return all_pairs


async def get_solana_pairs(
    http: httpx.AsyncClient,
    force: bool = False,
) -> list[dict]:
    """
    Return the shared Solana pair pool, refreshing if stale (> 90s).
    All callers (feeds, smart_filters, screener) share the same data.

    Args:
        http:  the shared httpx client from bot_data
        force: skip the cache and fetch fresh data immediately

    Returns:
        List of DexScreener pair dicts for Solana.
    """
    global _cached_pairs, _cached_at

    async with _cache_lock:
        age = time.monotonic() - _cached_at
        if not force and _cached_pairs and age < CACHE_TTL_SECS:
            logger.debug(f"pair_cache: serving {len(_cached_pairs)} pairs from cache ({age:.0f}s old)")
            return list(_cached_pairs)

        fresh = await _build_pool(http)
        if fresh:
            _cached_pairs = fresh
            _cached_at    = time.monotonic()
        elif _cached_pairs:
            logger.warning("pair_cache: refresh returned empty — serving stale data")

        return list(_cached_pairs)


def invalidate() -> None:
    """Force the next call to fetch fresh data. Use after /screener refresh."""
    global _cached_at
    _cached_at = 0.0

"""
solscan.py — On-chain token creation time lookup. No API key required.

Uses the public Solana RPC to find when a token mint account was first
created — the definitive source for OG detection.

HOW IT WORKS:
  1. Call getSignaturesForAddress on the mint account
     → mint accounts have very few txns (create, mint, freeze/revoke)
     → paginate until the last (oldest) signature is found
  2. Call getTransaction on that oldest signature
     → blockTime field = Unix timestamp of mint creation

No API keys, no rate limit subscriptions, completely free.

OPTIONAL SPEEDUP:
  Set HELIUS_RPC_URL in .env for higher rate limits on the RPC calls.
  The wallet_tracker.py already uses this — same key works here.
  Falls back to the public mainnet RPC if not set.
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

_RPC_URL = os.environ.get(
    "HELIUS_RPC_URL",
    "https://api.mainnet-beta.solana.com",
)

SOLSCAN_CACHE_FILE = "solscan_cache.json"

_SIG_PAGE_SIZE  = 1000
_MAX_PAGES      = 3
_MAX_CONCURRENT = 4
_MIN_DELAY      = 0.25

_semaphore: Optional[asyncio.Semaphore] = None
_last_req:  float = 0.0
_req_lock = asyncio.Lock()

_creation_cache: dict[str, float] = {}
_cache_loaded   = False
_cache_dirty    = False


# ─── Cache ────────────────────────────────────────────────────────────────────

def _load_cache() -> None:
    global _creation_cache, _cache_loaded
    if _cache_loaded:
        return
    try:
        with open(SOLSCAN_CACHE_FILE) as f:
            data = json.load(f)
            _creation_cache = {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}
        logger.info(f"token_creation_cache: loaded {len(_creation_cache)} entries")
    except FileNotFoundError:
        _creation_cache = {}
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(f"token_creation_cache: corrupt, starting fresh: {e}")
        _creation_cache = {}
    _cache_loaded = True


def _save_cache_sync() -> None:
    try:
        with open(SOLSCAN_CACHE_FILE, "w") as f:
            json.dump(_creation_cache, f)
    except Exception as e:
        logger.warning(f"token_creation_cache: save failed: {e}")


async def flush_cache() -> None:
    global _cache_dirty
    if _cache_dirty:
        await asyncio.to_thread(_save_cache_sync)
        _cache_dirty = False


def _get_semaphore() -> asyncio.Semaphore:
    global _semaphore
    if _semaphore is None:
        _semaphore = asyncio.Semaphore(_MAX_CONCURRENT)
    return _semaphore


# ─── Solana RPC helpers ───────────────────────────────────────────────────────

async def _rpc(http: httpx.AsyncClient, method: str, params: list) -> Optional[dict]:
    global _last_req
    async with _get_semaphore():
        async with _req_lock:
            gap = time.monotonic() - _last_req
            if gap < _MIN_DELAY:
                await asyncio.sleep(_MIN_DELAY - gap)
            _last_req = time.monotonic()

        try:
            resp = await http.post(
                _RPC_URL,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                headers={"Content-Type": "application/json"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if "error" in data:
                logger.debug(f"RPC {method} error: {data['error']}")
                return None
            return data.get("result")
        except Exception as e:
            logger.debug(f"RPC {method} failed: {e}")
            return None


async def _get_oldest_signature(mint_ca: str, http: httpx.AsyncClient) -> Optional[str]:
    """
    Walk getSignaturesForAddress pages to find the very first transaction
    on the mint account. For memecoins this is almost always one page.
    """
    before: Optional[str] = None

    for _page in range(_MAX_PAGES):
        opts: dict = {"limit": _SIG_PAGE_SIZE, "commitment": "finalized"}
        if before:
            opts["before"] = before

        result = await _rpc(http, "getSignaturesForAddress", [mint_ca, opts])

        if not result or not isinstance(result, list):
            break

        if len(result) < _SIG_PAGE_SIZE:
            # Got all signatures — last one is the oldest (creation tx)
            return result[-1]["signature"]

        # Full page — paginate further back
        before = result[-1]["signature"]

    return before


async def _get_block_time(signature: str, http: httpx.AsyncClient) -> Optional[float]:
    """Get the Unix timestamp of a transaction by its signature."""
    result = await _rpc(
        http,
        "getTransaction",
        [signature, {"encoding": "json", "commitment": "finalized", "maxSupportedTransactionVersion": 0}],
    )
    if result and isinstance(result, dict):
        bt = result.get("blockTime")
        if bt:
            return float(bt)
    return None


# ─── Public API ───────────────────────────────────────────────────────────────

async def get_creation_time(ca: str, http: httpx.AsyncClient) -> Optional[float]:
    """
    Get the on-chain creation timestamp (Unix seconds) for a token mint.
    Returns None if lookup fails.
    """
    _load_cache()
    if ca in _creation_cache:
        return _creation_cache[ca]

    global _cache_dirty
    try:
        sig = await _get_oldest_signature(ca, http)
        if not sig:
            return None
        ts = await _get_block_time(sig, http)
        if ts:
            _creation_cache[ca] = ts
            _cache_dirty = True
            if len(_creation_cache) % 10 == 0:
                await asyncio.to_thread(_save_cache_sync)
                _cache_dirty = False
            logger.debug(f"token_creation: {ca[:8]}... created at {ts}")
            return ts
    except Exception as e:
        logger.debug(f"get_creation_time({ca[:8]}...): {e}")
    return None


async def get_creation_times_batch(cas: list[str], http: httpx.AsyncClient) -> dict[str, float]:
    """Get creation timestamps for multiple CAs concurrently."""
    _load_cache()
    result: dict[str, float] = {}
    to_fetch: list[str] = []

    for ca in cas:
        if ca in _creation_cache:
            result[ca] = _creation_cache[ca]
        else:
            to_fetch.append(ca)

    if not to_fetch:
        return result

    fetched = await asyncio.gather(
        *[get_creation_time(ca, http) for ca in to_fetch],
        return_exceptions=True,
    )
    for ca, ts in zip(to_fetch, fetched):
        if isinstance(ts, float):
            result[ca] = ts

    logger.info(
        f"token_creation batch: {len(result)}/{len(cas)} resolved "
        f"({len(cas)-len(to_fetch)} cached, "
        f"{len([t for t in fetched if isinstance(t, float)])} from RPC)"
    )
    return result


async def enrich_pairs(pairs: list[dict], http: httpx.AsyncClient) -> list[dict]:
    """Add _solscan_created_at (on-chain mint time) to each pair dict."""
    if not pairs:
        return pairs

    cas = list({
        p.get("baseToken", {}).get("address", "")
        for p in pairs
        if p.get("baseToken", {}).get("address")
    })

    creation_times = await get_creation_times_batch(cas, http)

    for pair in pairs:
        ca = pair.get("baseToken", {}).get("address", "")
        if ca and ca in creation_times:
            pair["_solscan_created_at"] = creation_times[ca]

    return pairs


def is_configured() -> bool:
    """Always True — public RPC needs no API key."""
    return True

"""
Free mcap poller.

Fallback data source for when you don't have a funded PumpPortal API key.
Polls pump.fun's public (unofficial) frontend API on a timer for every
actively-tracked token and records its mcap -- zero cost, just lower
resolution than the live trade stream.

Run with: python -m src.poll
"""

import asyncio
import logging
from datetime import datetime, timezone

import aiohttp

from src.config import (
    PUMP_FRONTEND_API_BASE,
    POLL_INTERVAL_SECONDS,
    POLL_CONCURRENCY,
    POLL_TRACK_SECONDS,
    MAX_SANE_MCAP_SOL,
)
from src import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("poll")

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "application/json",
}


def _extract_mcap(data: dict):
    mcap_sol = data.get("market_cap") or data.get("marketCapSol") or data.get("mcap_sol")
    mcap_usd = data.get("usd_market_cap") or data.get("usdMarketCap") or data.get("mcap_usd")
    return mcap_sol, mcap_usd


def _extract_ath(data: dict):
    ath_sol = data.get("ath_market_cap")
    ath_ts_ms = data.get("ath_market_cap_timestamp")
    ath_ts = None
    if ath_ts_ms:
        try:
            ath_ts = datetime.fromtimestamp(ath_ts_ms / 1000, tz=timezone.utc)
        except (ValueError, OSError):
            ath_ts = None
    return ath_sol, ath_ts


async def _poll_one(session: aiohttp.ClientSession, mint: str, sem: asyncio.Semaphore, stats: dict):
    url = f"{PUMP_FRONTEND_API_BASE}/coins/{mint}"
    async with sem:
        data = None
        for attempt in range(3):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 429:
                        stats["rate_limited"] += 1
                        retry_after = float(resp.headers.get("Retry-After", 1))
                        await asyncio.sleep(min(retry_after, 5))
                        continue
                    if resp.status != 200:
                        stats["other_errors"] += 1
                        return
                    data = await resp.json(content_type=None)
                    break
            except Exception:
                stats["exceptions"] += 1
                return
        if data is None:
            return

    mcap_sol, mcap_usd = _extract_mcap(data)
    if mcap_sol is not None and mcap_sol > MAX_SANE_MCAP_SOL:
        log.warning("dropping implausible mcap_sol=%.1f for %s", mcap_sol, mint)
        mcap_sol = None
    if mcap_sol is not None or mcap_usd is not None:
        await db.insert_price_poll(mint, mcap_sol, mcap_usd)
        stats["ok"] += 1

    ath_sol, ath_ts = _extract_ath(data)
    if ath_sol is not None and ath_sol > MAX_SANE_MCAP_SOL:
        log.warning("dropping implausible ath_market_cap=%.1f for %s", ath_sol, mint)
        ath_sol = None
    if ath_sol is not None and ath_ts is not None:
        await db.update_ath_if_higher(mint, ath_sol, ath_ts)


async def run_once():
    mints = await db.get_pollable_mints(POLL_TRACK_SECONDS)
    if not mints:
        return
    log.info("polling %d tokens", len(mints))
    sem = asyncio.Semaphore(POLL_CONCURRENCY)
    stats = {"ok": 0, "rate_limited": 0, "other_errors": 0, "exceptions": 0}
    async with aiohttp.ClientSession(headers=_HEADERS) as session:
        await asyncio.gather(*[_poll_one(session, m, sem, stats) for m in mints])
    if stats["rate_limited"] or stats["other_errors"] or stats["exceptions"]:
        log.warning("poll cycle stats: %s", stats)
    else:
        log.info("poll cycle stats: %s", stats)


async def run_forever():
    while True:
        try:
            await run_once()
        except Exception:
            log.exception("poll cycle failed")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(run_forever())

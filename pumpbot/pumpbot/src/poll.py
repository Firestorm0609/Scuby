"""
Free mcap poller.

Fallback data source for when you don't have a funded PumpPortal API key
(the metered subscribeTokenTrade stream requires a wallet with >=0.02 SOL).
This polls pump.fun's public (unofficial) frontend API on a timer for every
actively-tracked token and records its mcap -- zero cost, just lower
resolution than the live trade stream.

Once you can afford to fund a PumpPortal API key, keep this running
alongside src.ingest for redundancy, or drop it -- your call.

Run with: python -m src.poll
"""

import asyncio
import logging

import aiohttp

from src.config import (
    PUMP_FRONTEND_API_BASE,
    POLL_INTERVAL_SECONDS,
    POLL_CONCURRENCY,
    POLL_TRACK_SECONDS,
)
from src import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("poll")


def _extract_mcap(data: dict):
    """pump.fun's frontend API has changed field names across versions in
    the wild; check a few plausible keys defensively rather than assuming
    one exact shape."""
    mcap_sol = data.get("market_cap") or data.get("marketCapSol") or data.get("mcap_sol")
    mcap_usd = data.get("usd_market_cap") or data.get("usdMarketCap") or data.get("mcap_usd")
    return mcap_sol, mcap_usd


async def _poll_one(session: aiohttp.ClientSession, mint: str, sem: asyncio.Semaphore):
    url = f"{PUMP_FRONTEND_API_BASE}/coins/{mint}"
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    log.debug("poll %s -> HTTP %s", mint, resp.status)
                    return
                data = await resp.json(content_type=None)
        except Exception:
            log.debug("poll failed for %s", mint, exc_info=True)
            return

    mcap_sol, mcap_usd = _extract_mcap(data)
    if mcap_sol is None and mcap_usd is None:
        return
    await db.insert_price_poll(mint, mcap_sol, mcap_usd)


async def run_once():
    mints = await db.get_pollable_mints(POLL_TRACK_SECONDS)
    if not mints:
        return
    log.info("polling %d tokens", len(mints))
    sem = asyncio.Semaphore(POLL_CONCURRENCY)
    async with aiohttp.ClientSession(headers={"User-Agent": "pumpbot/0.1"}) as session:
        await asyncio.gather(*[_poll_one(session, m, sem) for m in mints])


async def run_forever():
    while True:
        try:
            await run_once()
        except Exception:
            log.exception("poll cycle failed")
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(run_forever())

"""
Labeling job.

For every token whose creation is older than LABEL_WINDOW_SECONDS and hasn't
been labeled yet: look at its trade history, find the peak market cap reached
within the window, and record whether it did >=2x from its initial mcap.

Run periodically (e.g. every 10 min) via cron or a simple sleep loop:
    python -m src.label
"""

import asyncio
import logging

from src.config import LABEL_WINDOW_SECONDS
from src import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("label")


async def label_one(mint: str, initial_mcap_sol: float | None):
    pool = await db.get_pool()

    # peak mcap seen via live trades (if any -- requires funded PumpPortal key)
    trade_row = await pool.fetchrow(
        """
        SELECT mcap_sol, ts FROM trades
        WHERE mint = $1 AND mcap_sol IS NOT NULL
        ORDER BY mcap_sol DESC
        LIMIT 1
        """,
        mint,
    )
    # peak mcap seen via the free REST poller
    poll_row = await pool.fetchrow(
        """
        SELECT mcap_sol, ts FROM price_polls
        WHERE mint = $1 AND mcap_sol IS NOT NULL
        ORDER BY mcap_sol DESC
        LIMIT 1
        """,
        mint,
    )

    candidates = [r for r in (trade_row, poll_row) if r is not None]
    if not candidates or not initial_mcap_sol or initial_mcap_sol <= 0:
        # no data or no baseline mcap -> can't label meaningfully
        await db.label_token(mint, None, None, None, False)
        return

    best = max(candidates, key=lambda r: r["mcap_sol"])
    max_mcap = best["mcap_sol"]
    max_mult = max_mcap / initial_mcap_sol
    label_2x = max_mult >= 2.0
    await db.label_token(mint, max_mcap, best["ts"], max_mult, label_2x)
    log.info("labeled %s: max_mult=%.2fx 2x=%s", mint, max_mult, label_2x)


async def run_once():
    tokens = await db.get_unlabeled_tokens(LABEL_WINDOW_SECONDS)
    log.info("labeling %d tokens", len(tokens))
    for t in tokens:
        try:
            await label_one(t["mint"], t["initial_mcap_sol"])
        except Exception:
            log.exception("failed to label %s", t["mint"])


async def run_forever(interval_seconds: int = 600):
    while True:
        await run_once()
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(run_forever())

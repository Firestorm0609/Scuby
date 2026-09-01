"""
Snapshot builder.

For each tracked token, at each checkpoint (30s, 60s, 5min, ... after creation),
compute a feature row from the trades seen so far: mcap, mult from initial,
buy/sell counts, unique buyers/sellers, buy/sell ratio, volume.

This is what feeds both model training (joined with the eventual `label_2x`)
and live inference (same features, computed on a token that's still young).

Run periodically: python -m src.snapshot
"""

import asyncio
import logging

from src.config import SNAPSHOT_CHECKPOINTS
from src import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("snapshot")


async def _tokens_due_for_snapshot(checkpoint_seconds: int):
    """Tokens old enough to have reached this checkpoint but without a
    snapshot row for it yet."""
    pool = await db.get_pool()
    rows = await pool.fetch(
        """
        SELECT t.mint, t.initial_mcap_sol, t.created_at
        FROM tokens t
        WHERE t.created_at < now() - interval '1 second' * $1
          AND NOT EXISTS (
              SELECT 1 FROM snapshots s
              WHERE s.mint = t.mint AND s.checkpoint_seconds = $1
          )
        """,
        checkpoint_seconds,
    )
    return rows


async def build_snapshot(mint: str, initial_mcap_sol, created_at, checkpoint_seconds: int):
    pool = await db.get_pool()
    cutoff = created_at
    rows = await pool.fetch(
        """
        SELECT tx_type, trader, sol_amount, mcap_sol, ts
        FROM trades
        WHERE mint = $1 AND ts <= $2::timestamptz + (interval '1 second' * $3::int)
        ORDER BY ts ASC
        """,
        mint, cutoff, checkpoint_seconds,
    )

    buys = [r for r in rows if r["tx_type"] == "buy"]
    sells = [r for r in rows if r["tx_type"] == "sell"]
    unique_buyers = len({r["trader"] for r in buys if r["trader"]})
    unique_sellers = len({r["trader"] for r in sells if r["trader"]})
    volume_sol = sum((r["sol_amount"] or 0) for r in rows)

    # latest known mcap as of this checkpoint -- prefer trades, fall back to
    # the free REST poller if no trade data exists (e.g. no funded PumpPortal key)
    mcap_rows = [r for r in rows if r["mcap_sol"] is not None]
    if mcap_rows:
        mcap_sol = mcap_rows[-1]["mcap_sol"]
    else:
        poll_row = await pool.fetchrow(
            """
            SELECT mcap_sol FROM price_polls
            WHERE mint = $1 AND ts <= $2::timestamptz + (interval '1 second' * $3::int)
            ORDER BY ts DESC
            LIMIT 1
            """,
            mint, cutoff, checkpoint_seconds,
        )
        mcap_sol = poll_row["mcap_sol"] if poll_row else initial_mcap_sol
    mult = (mcap_sol / initial_mcap_sol) if (initial_mcap_sol and mcap_sol) else None
    buy_sell_ratio = (len(buys) / len(sells)) if sells else float(len(buys)) if buys else 0.0

    pool_ = await db.get_pool()
    await pool_.execute(
        """
        INSERT INTO snapshots (mint, checkpoint_seconds, mcap_sol, mult_from_initial,
                                num_buys, num_sells, unique_buyers, unique_sellers,
                                buy_sell_ratio, volume_sol)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (mint, checkpoint_seconds) DO NOTHING
        """,
        mint, checkpoint_seconds, mcap_sol, mult, len(buys), len(sells),
        unique_buyers, unique_sellers, buy_sell_ratio, volume_sol,
    )


async def run_once():
    for cp in SNAPSHOT_CHECKPOINTS:
        due = await _tokens_due_for_snapshot(cp)
        if not due:
            continue
        log.info("building %d snapshots at checkpoint=%ds", len(due), cp)
        for row in due:
            try:
                await build_snapshot(row["mint"], row["initial_mcap_sol"], row["created_at"], cp)
            except Exception:
                log.exception("snapshot failed for %s @ %ds", row["mint"], cp)


async def run_forever(interval_seconds: int = 30):
    while True:
        await run_once()
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(run_forever())

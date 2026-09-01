import asyncpg
from src.config import DATABASE_URL

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    return _pool


async def insert_token(token: dict):
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO tokens (mint, name, symbol, creator, created_at,
                             initial_mcap_sol, initial_vsol, initial_vtokens,
                             bonding_curve_key, uri)
        VALUES ($1,$2,$3,$4, to_timestamp($5), $6,$7,$8,$9,$10)
        ON CONFLICT (mint) DO NOTHING
        """,
        token["mint"], token.get("name"), token.get("symbol"), token.get("creator"),
        token["created_at_epoch"], token.get("initial_mcap_sol"),
        token.get("initial_vsol"), token.get("initial_vtokens"),
        token.get("bonding_curve_key"), token.get("uri"),
    )


async def insert_trade(trade: dict):
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO trades (mint, signature, trader, tx_type, sol_amount, token_amount,
                             vsol_in_curve, vtokens_in_curve, mcap_sol, ts)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9, now())
        """,
        trade["mint"], trade.get("signature"), trade.get("trader"), trade.get("tx_type"),
        trade.get("sol_amount"), trade.get("token_amount"),
        trade.get("vsol_in_curve"), trade.get("vtokens_in_curve"), trade.get("mcap_sol"),
    )


async def mark_migrated(mint: str):
    pool = await get_pool()
    await pool.execute(
        "UPDATE tokens SET migrated = TRUE, migrated_at = now() WHERE mint = $1", mint
    )


async def get_active_mints(since_seconds: int) -> list[str]:
    """Mints created within the last `since_seconds` that we should still be
    actively subscribed to for trade events."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT mint FROM tokens
        WHERE created_at > now() - interval '1 second' * $1
        ORDER BY created_at DESC
        """,
        since_seconds,
    )
    return [r["mint"] for r in rows]


async def get_pollable_mints(since_seconds: int) -> list[str]:
    """Same idea as get_active_mints, but for the free REST poller."""
    return await get_active_mints(since_seconds)


async def insert_price_poll(mint: str, mcap_sol: float | None, mcap_usd: float | None):
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO price_polls (mint, mcap_sol, mcap_usd, ts) VALUES ($1,$2,$3, now())",
        mint, mcap_sol, mcap_usd,
    )


async def get_unlabeled_tokens(window_seconds: int) -> list[dict]:
    """Tokens whose label window has closed but haven't been labeled yet."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT mint, initial_mcap_sol FROM tokens
        WHERE label_window_closed = FALSE
          AND created_at < now() - interval '1 second' * $1
        """,
        window_seconds,
    )
    return [dict(r) for r in rows]


async def label_token(mint: str, max_mcap_sol: float, max_mcap_at, max_mult: float, label_2x: bool):
    pool = await get_pool()
    await pool.execute(
        """
        UPDATE tokens
        SET max_mcap_sol = $2, max_mcap_at = $3, max_mult = $4,
            label_2x = $5, label_window_closed = TRUE
        WHERE mint = $1
        """,
        mint, max_mcap_sol, max_mcap_at, max_mult, label_2x,
    )


async def count_labeled_tokens() -> int:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT count(*) AS n FROM tokens WHERE label_window_closed = TRUE AND label_2x IS NOT NULL"
    )
    return row["n"]


async def get_training_rows(checkpoint_seconds: int) -> list[dict]:
    """Join snapshots at a given checkpoint with the final label + created_at
    (used for recency weighting). One row per token that both reached this
    checkpoint and has a closed label window."""
    pool = await get_pool()
    rows = await pool.fetch(
        """
        SELECT s.mint, s.mcap_sol, s.mult_from_initial, s.num_buys, s.num_sells,
               s.unique_buyers, s.unique_sellers, s.buy_sell_ratio, s.volume_sol,
               s.top10_holder_pct, s.dev_holding_pct, s.dev_sold,
               t.label_2x, t.created_at
        FROM snapshots s
        JOIN tokens t ON t.mint = s.mint
        WHERE s.checkpoint_seconds = $1
          AND t.label_window_closed = TRUE
          AND t.label_2x IS NOT NULL
        """,
        checkpoint_seconds,
    )
    return [dict(r) for r in rows]


async def get_training_state(checkpoint_seconds: int) -> dict | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT * FROM training_state WHERE checkpoint_seconds = $1", checkpoint_seconds
    )
    return dict(row) if row else None


async def upsert_training_state(checkpoint_seconds: int, count: int, auc: float | None, model_path: str):
    pool = await get_pool()
    await pool.execute(
        """
        INSERT INTO training_state (checkpoint_seconds, last_trained_at, last_trained_count, last_auc, model_path)
        VALUES ($1, now(), $2, $3, $4)
        ON CONFLICT (checkpoint_seconds) DO UPDATE
        SET last_trained_at = now(), last_trained_count = $2, last_auc = $3, model_path = $4
        """,
        checkpoint_seconds, count, auc, model_path,
    )

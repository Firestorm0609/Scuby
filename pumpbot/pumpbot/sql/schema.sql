-- PumpBot schema
-- Run with: psql "$DATABASE_URL" -f sql/schema.sql

CREATE TABLE IF NOT EXISTS tokens (
    mint                TEXT PRIMARY KEY,
    name                TEXT,
    symbol              TEXT,
    creator             TEXT,
    created_at          TIMESTAMPTZ NOT NULL,
    initial_mcap_sol    DOUBLE PRECISION,
    initial_vsol        DOUBLE PRECISION,
    initial_vtokens     DOUBLE PRECISION,
    bonding_curve_key   TEXT,
    uri                 TEXT,              -- metadata uri (image/socials live behind this)
    twitter             TEXT,
    telegram            TEXT,
    website             TEXT,
    migrated            BOOLEAN DEFAULT FALSE,
    migrated_at         TIMESTAMPTZ,
    -- filled in later once labeling window has passed
    max_mcap_sol        DOUBLE PRECISION,
    max_mcap_at          TIMESTAMPTZ,
    max_mult            DOUBLE PRECISION,  -- max_mcap_sol / initial_mcap_sol
    label_2x            BOOLEAN,           -- max_mult >= 2 within label window
    label_window_closed BOOLEAN DEFAULT FALSE,
    inserted_at         TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tokens_created_at ON tokens (created_at);
CREATE INDEX IF NOT EXISTS idx_tokens_creator ON tokens (creator);
CREATE INDEX IF NOT EXISTS idx_tokens_label_window_closed ON tokens (label_window_closed);

-- Raw trade events, one row per buy/sell seen on the websocket.
-- This is the ground truth we derive mcap-over-time curves and features from.
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL PRIMARY KEY,
    mint            TEXT NOT NULL REFERENCES tokens(mint) ON DELETE CASCADE,
    signature       TEXT,
    trader          TEXT,
    tx_type         TEXT,               -- 'buy' | 'sell' | 'create'
    sol_amount      DOUBLE PRECISION,
    token_amount    DOUBLE PRECISION,
    vsol_in_curve   DOUBLE PRECISION,
    vtokens_in_curve DOUBLE PRECISION,
    mcap_sol        DOUBLE PRECISION,
    ts              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trades_mint_ts ON trades (mint, ts);
CREATE INDEX IF NOT EXISTS idx_trades_trader ON trades (trader);

-- Free-tier fallback for mcap tracking when we don't have a funded PumpPortal
-- API key to stream live trades. Polled periodically via pump.fun's public
-- frontend API (no key required, no cost).
CREATE TABLE IF NOT EXISTS price_polls (
    id          BIGSERIAL PRIMARY KEY,
    mint        TEXT NOT NULL REFERENCES tokens(mint) ON DELETE CASCADE,
    mcap_sol    DOUBLE PRECISION,
    mcap_usd    DOUBLE PRECISION,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_price_polls_mint_ts ON price_polls (mint, ts);

-- Periodic derived snapshots per token (used directly as ML feature rows).
-- One row per (mint, checkpoint_seconds) e.g. 30s, 60s, 300s, 900s, 3600s after creation.
CREATE TABLE IF NOT EXISTS snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    mint                TEXT NOT NULL REFERENCES tokens(mint) ON DELETE CASCADE,
    checkpoint_seconds  INTEGER NOT NULL,
    mcap_sol            DOUBLE PRECISION,
    mult_from_initial   DOUBLE PRECISION,
    num_buys            INTEGER,
    num_sells           INTEGER,
    unique_buyers       INTEGER,
    unique_sellers      INTEGER,
    buy_sell_ratio      DOUBLE PRECISION,
    volume_sol          DOUBLE PRECISION,
    top10_holder_pct    DOUBLE PRECISION,   -- filled in by Helius enrichment job
    dev_holding_pct     DOUBLE PRECISION,
    dev_sold            BOOLEAN,
    created_at          TIMESTAMPTZ DEFAULT now(),
    UNIQUE (mint, checkpoint_seconds)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_mint ON snapshots (mint);

-- Alerts the bot has actually sent, so we can track our own hit rate over time.
CREATE TABLE IF NOT EXISTS alerts (
    id              BIGSERIAL PRIMARY KEY,
    mint            TEXT NOT NULL REFERENCES tokens(mint) ON DELETE CASCADE,
    score           DOUBLE PRECISION,
    features        JSONB,
    sent_at         TIMESTAMPTZ DEFAULT now(),
    outcome_mult    DOUBLE PRECISION,   -- filled in later once outcome is known
    outcome_checked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_alerts_mint ON alerts (mint);

-- One row per checkpoint (e.g. 300, 900...), tracking the last time we
-- trained a model for that checkpoint and how many labeled rows it used.
-- Lets train.py decide "has enough changed to justify a full retrain?"
CREATE TABLE IF NOT EXISTS training_state (
    checkpoint_seconds  INTEGER PRIMARY KEY,
    last_trained_at     TIMESTAMPTZ,
    last_trained_count  INTEGER DEFAULT 0,
    last_auc            DOUBLE PRECISION,
    model_path           TEXT
);

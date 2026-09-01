# PumpBot — Data Foundation (Phase 1)

This is the data-ingestion layer for the pump.fun pattern-learning Telegram bot.
It does **not** trade or alert yet — it just builds the clean historical
dataset everything else depends on. Get this running and stable for a few
days before touching modeling or Telegram.

## Pieces

- `sql/schema.sql` — Postgres schema: `tokens`, `trades`, `snapshots`, `alerts`
- `src/ingest.py` — connects to PumpPortal websocket, records every new
  launch + migration, and dynamically subscribes to trade events for
  recently-created tokens (capped + culled so you don't blow through the
  metered trade-stream quota)
- `src/label.py` — once a token's 24h (configurable) window has passed,
  computes its peak market cap and whether it hit 2x — this is your ML target
- `src/snapshot.py` — turns raw trades into fixed-checkpoint feature rows
  (30s, 60s, 5min, 1h, ... after creation) — this is what both training and
  live inference will read from

## Setup

```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

cp .env.example .env   # fill in DATABASE_URL at minimum

createdb pumpbot        # or use a hosted Postgres/Timescale instance
psql "$DATABASE_URL" -f sql/schema.sql
```

## Running (long-lived processes)

```bash
python -m src.ingest      # websocket listener -- keep this running 24/7
python -m src.poll        # FREE fallback: polls mcap via pump.fun's public API
python -m src.snapshot    # builds feature rows every 30s
python -m src.label       # labels tokens every 10 min once their window closes
python -m src.train       # checks every 30 min whether it's time to (re)train
```

**No money needed to start.** `subscribeNewToken` / `subscribeMigration` (used
by `ingest.py`) are free forever. The only thing that costs anything is
`subscribeTokenTrade` (live buy/sell flow), which needs a PumpPortal API key
tied to a wallet with >=0.02 SOL. Until you can fund that, run `src.poll`
alongside `src.ingest` -- it polls pump.fun's public (unofficial) frontend
API on a timer (`POLL_INTERVAL_SECONDS`, default 15s) for every actively
tracked token and writes to a separate `price_polls` table. Both `label.py`
and `snapshot.py` already check `price_polls` as a fallback whenever trade
data isn't available, so the whole pipeline works end-to-end at zero cost --
just at lower time resolution than the live trade stream. Once you can fund
a PumpPortal key, you can run both side by side (trades win when present)
or drop the poller.

In production, run each of these as a separate systemd service / Docker
container / tmux session, not in one script -- if one crashes and restarts,
you don't want to lose the others too.

## Notes on PumpPortal limits

- `subscribeNewToken` and `subscribeMigration` are free, no API key needed.
- `subscribeTokenTrade` is metered (0.01 SOL per 10,000 events) and requires
  an API key tied to a wallet with >=0.02 SOL. `MAX_ACTIVE_SUBSCRIPTIONS` in
  `.env` caps how many tokens you're paying to watch trades on at once —
  tune this against your budget. Tokens get "culled" (unsubscribed) after
  `ACTIVE_TRACK_SECONDS`, since almost all the signal happens early.
- Without an API key, `ingest.py` will still capture every token creation
  and migration, just not live trades — useful for testing the pipeline for
  free before you commit any SOL.

## Training (`src/train.py`)

Trains one LightGBM classifier **per snapshot checkpoint** (a "5-minute
model," a "1-hour model," etc.) predicting `label_2x`.

**When it trains — driven by sample count, not wall-clock time:**
- Won't train at all until `MIN_LABELED_TOKENS_TO_TRAIN` (default 5000)
  labeled tokens exist. Below that, a GBM will mostly memorize noise.
- Once past that floor, retrains whenever `RETRAIN_INTERVAL_HOURS` has
  passed OR the labeled-token count has grown by `RETRAIN_ON_NEW_LABELS`
  since the last run — whichever comes first. Run `src/train.py` on a timer
  (every 30-60 min via cron); it's a no-op unless one of those conditions
  is met.
- All three thresholds are in `.env`, so you can lower
  `MIN_LABELED_TOKENS_TO_TRAIN` to get a smoke-test model running sooner,
  then raise it back up once you trust the pipeline.

**Does it retrain on every new coin?** No — and deliberately not. LightGBM
(like most gradient-boosted tree models) has no clean "update the existing
model with just this one new row" operation the way an online linear model
does; the trees are built from a fixed training set. So "retraining" here
means a **full retrain from scratch** on all accumulated labeled data each
time the trigger fires, not incremental fine-tuning. This is normal practice
for tree models and is arguably better anyway: pump.fun's meta (what
actually predicts a pump) shifts over weeks, so `RECENCY_HALF_LIFE_DAYS`
exponentially downweights older rows in each retrain rather than treating a
coin from 2 months ago the same as one from yesterday.

Validation uses a time-based split (train on the older 80%, validate on the
most recent 20%) rather than a random split, since a random split would let
future meta leak into training and make the model look better than it is.

Each retrain logs validation AUC and top-5 features by importance to
`training_state`, so you can watch both accuracy and *what's driving it*
evolve over time.

## What's next (not built yet)

1. **Helius enrichment job** — for each snapshot checkpoint, pull holder
   distribution (top-10 %, dev wallet %) via Helius RPC/enhanced API and
   backfill the `snapshots` table's `top10_holder_pct` / `dev_holding_pct`
   columns. This is one of the strongest predictive signals and PumpPortal
   doesn't give it to you directly. `train.py` already has columns wired up
   for it — they'll just be all-NaN until this job fills them in.
2. **Telegram bot** — runs the trained model on live tokens as they pass
   each checkpoint, sends alerts above a score threshold, and logs outcomes
   back into the `alerts` table so you can track real hit-rate over time.

## Reality check

Pump.fun is adversarial: wash trading, sniper bots, and coordinated groups
routinely fake the exact signals (buyer count, volume, holder spread) a
naive model would key on. Expect to iterate a lot on feature engineering,
validate on out-of-time data (not just random train/test splits — meta
shifts week to week), and treat any model score as a probability edge, not
a certainty. Paper-trade the alerts for a while before wiring in real
capital via PumpPortal's trade-local/lightning API.

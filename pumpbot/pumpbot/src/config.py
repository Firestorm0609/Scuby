import os
from dotenv import load_dotenv

load_dotenv()

# --- Database ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost/pumpbot")

# --- PumpPortal ---
PUMPPORTAL_WS_URL = os.getenv("PUMPPORTAL_WS_URL", "wss://pumpportal.fun/api/data")
PUMPPORTAL_API_KEY = os.getenv("PUMPPORTAL_API_KEY", "")  # only needed for trade streams

# --- Helius ---
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# --- Labeling / tracking windows ---
LABEL_WINDOW_SECONDS = int(os.getenv("LABEL_WINDOW_SECONDS", 24 * 3600))  # 24h window to check for 2x
SNAPSHOT_CHECKPOINTS = [30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 86400]  # seconds after creation

# How long to keep a token "actively tracked" (subscribed to trade events) before
# we stop bothering (most life-or-death happens in the first hour or two)
ACTIVE_TRACK_SECONDS = int(os.getenv("ACTIVE_TRACK_SECONDS", 4 * 3600))

# Max number of tokens to hold live trade-subscriptions on at once (PumpPortal trade
# streams are metered, so we cap concurrent subscriptions and cull old ones)
MAX_ACTIVE_SUBSCRIPTIONS = int(os.getenv("MAX_ACTIVE_SUBSCRIPTIONS", 300))

# --- Training ---
# Don't bother training until we have at least this many labeled tokens.
# 5000 is a reasonable floor for a first pass -- below that the model will
# just memorize noise. Lower it to experiment sooner, but treat anything
# trained on <2-3k rows as a smoke test, not a real model.
MIN_LABELED_TOKENS_TO_TRAIN = int(os.getenv("MIN_LABELED_TOKENS_TO_TRAIN", 5000))

# Once the minimum is hit, retrain on this schedule using ALL labeled data
# accumulated so far each time. LightGBM doesn't support clean incremental
# updates, so "retrain" means a fresh full retrain, not fine-tuning.
RETRAIN_INTERVAL_HOURS = float(os.getenv("RETRAIN_INTERVAL_HOURS", 12))

# Also force a retrain if the labeled-token count grows by this much since
# the last training run, even if the time interval hasn't elapsed -- useful
# early on when data accumulates fast relative to the clock.
RETRAIN_ON_NEW_LABELS = int(os.getenv("RETRAIN_ON_NEW_LABELS", 1000))

# Recency half-life in days: rows older than this count for progressively
# less in training, since pump.fun's meta drifts over weeks. Set to 0 to
# disable and weight all historical data equally.
RECENCY_HALF_LIFE_DAYS = float(os.getenv("RECENCY_HALF_LIFE_DAYS", 30))

MODEL_DIR = os.getenv("MODEL_DIR", "models")

# --- Free polling fallback (no PumpPortal API key / no SOL required) ---
# Uses pump.fun's public (unofficial) frontend API to poll mcap for tracked
# tokens on a timer. Much lower resolution than the live trade stream, but
# costs nothing. Use this until you can fund a PumpPortal API key, then
# either keep both running or switch over.
PUMP_FRONTEND_API_BASE = os.getenv("PUMP_FRONTEND_API_BASE", "https://frontend-api.pump.fun")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", 15))
POLL_CONCURRENCY = int(os.getenv("POLL_CONCURRENCY", 10))
# how long after creation to keep polling a token (same idea as ACTIVE_TRACK_SECONDS)
POLL_TRACK_SECONDS = int(os.getenv("POLL_TRACK_SECONDS", 4 * 3600))

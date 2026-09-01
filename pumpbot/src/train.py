"""
Training job.

For each snapshot checkpoint (30s, 60s, 5min, ...), trains a LightGBM
classifier predicting label_2x from that checkpoint's features.

Retrain trigger logic (checked each time this runs):
  - never trained yet, AND total labeled tokens >= MIN_LABELED_TOKENS_TO_TRAIN
  - OR last trained > RETRAIN_INTERVAL_HOURS ago
  - OR labeled-token count has grown by >= RETRAIN_ON_NEW_LABELS since last train

This is a full retrain each time, not incremental fine-tuning -- LightGBM
trees are built from a fixed training set, there's no clean "update with just
the new rows" operation the way there is for e.g. an online linear model.
Retraining periodically on the accumulated (recency-weighted) dataset is the
standard, simpler, and more robust approach here, especially since pump.fun's
meta drifts and older coins should count for less anyway.

Run this on a timer (e.g. every 30-60 min via cron); it's a no-op if nothing
needs retraining yet.

    python -m src.train
"""

import asyncio
import logging
import math
import os
from datetime import datetime, timezone

import lightgbm as lgb
import numpy as np
from sklearn.metrics import roc_auc_score

from src.config import (
    SNAPSHOT_CHECKPOINTS,
    MIN_LABELED_TOKENS_TO_TRAIN,
    RETRAIN_INTERVAL_HOURS,
    RETRAIN_ON_NEW_LABELS,
    RECENCY_HALF_LIFE_DAYS,
    MODEL_DIR,
)
from src import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("train")

FEATURE_COLS = [
    "mcap_sol", "mult_from_initial", "num_buys", "num_sells",
    "unique_buyers", "unique_sellers", "buy_sell_ratio", "volume_sol",
    "top10_holder_pct", "dev_holding_pct", "dev_sold",
]


def _should_retrain(state: dict | None, total_labeled: int) -> bool:
    if total_labeled < MIN_LABELED_TOKENS_TO_TRAIN:
        return False
    if state is None:
        return True  # never trained, and we've hit the minimum
    if state["last_trained_count"] and (total_labeled - state["last_trained_count"] >= RETRAIN_ON_NEW_LABELS):
        return True
    if state["last_trained_at"]:
        hours_since = (datetime.now(timezone.utc) - state["last_trained_at"]).total_seconds() / 3600
        if hours_since >= RETRAIN_INTERVAL_HOURS:
            return True
    return False


def _recency_weights(created_ats) -> np.ndarray:
    if RECENCY_HALF_LIFE_DAYS <= 0:
        return np.ones(len(created_ats))
    now = datetime.now(timezone.utc)
    ages_days = np.array([(now - c).total_seconds() / 86400 for c in created_ats])
    decay = math.log(2) / RECENCY_HALF_LIFE_DAYS
    return np.exp(-decay * ages_days)


def _build_matrix(rows: list[dict]):
    X = np.array([[float(r.get(c) if r.get(c) is not None else np.nan) for c in FEATURE_COLS] for r in rows])
    y = np.array([1 if r["label_2x"] else 0 for r in rows])
    created_ats = [r["created_at"] for r in rows]
    return X, y, created_ats


async def train_checkpoint(checkpoint_seconds: int):
    rows = await db.get_training_rows(checkpoint_seconds)
    if len(rows) < MIN_LABELED_TOKENS_TO_TRAIN:
        log.info("checkpoint=%ds: only %d labeled rows, skipping", checkpoint_seconds, len(rows))
        return

    X, y, created_ats = _build_matrix(rows)
    weights = _recency_weights(created_ats)

    # time-based split: train on the older 80%, validate on the most recent 20%
    # (a random split would leak future meta into training and overstate accuracy)
    order = np.argsort(created_ats)
    X, y, weights = X[order], y[order], weights[order]
    split = int(len(X) * 0.8)
    X_train, X_val = X[:split], X[split:]
    y_train, y_val = y[:split], y[split:]
    w_train = weights[:split]

    train_set = lgb.Dataset(X_train, label=y_train, weight=w_train, feature_name=FEATURE_COLS)
    val_set = lgb.Dataset(X_val, label=y_val, reference=train_set)

    params = {
        "objective": "binary",
        "metric": "auc",
        "verbosity": -1,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "min_data_in_leaf": 30,
    }
    model = lgb.train(
        params, train_set,
        num_boost_round=500,
        valid_sets=[val_set],
        callbacks=[lgb.early_stopping(30, verbose=False)],
    )

    preds = model.predict(X_val, num_iteration=model.best_iteration)
    auc = roc_auc_score(y_val, preds) if len(set(y_val)) > 1 else None

    os.makedirs(MODEL_DIR, exist_ok=True)
    model_path = os.path.join(MODEL_DIR, f"model_{checkpoint_seconds}s.txt")
    model.save_model(model_path)

    await db.upsert_training_state(checkpoint_seconds, len(rows), auc, model_path)
    log.info(
        "checkpoint=%ds: trained on %d rows (val AUC=%s), saved to %s",
        checkpoint_seconds, len(rows), f"{auc:.3f}" if auc else "n/a", model_path,
    )
    if auc is not None:
        importances = dict(zip(FEATURE_COLS, model.feature_importance(importance_type="gain")))
        top = sorted(importances.items(), key=lambda kv: kv[1], reverse=True)[:5]
        log.info("checkpoint=%ds top features: %s", checkpoint_seconds, top)


async def run_once():
    total_labeled = await db.count_labeled_tokens()
    log.info("total labeled tokens: %d (need >= %d to train)", total_labeled, MIN_LABELED_TOKENS_TO_TRAIN)

    for cp in SNAPSHOT_CHECKPOINTS:
        state = await db.get_training_state(cp)
        if _should_retrain(state, total_labeled):
            log.info("checkpoint=%ds: retrain triggered", cp)
            try:
                await train_checkpoint(cp)
            except Exception:
                log.exception("training failed for checkpoint=%ds", cp)
        else:
            log.info("checkpoint=%ds: no retrain needed yet", cp)


async def run_forever(interval_seconds: int = 1800):
    while True:
        await run_once()
        await asyncio.sleep(interval_seconds)


if __name__ == "__main__":
    asyncio.run(run_forever())

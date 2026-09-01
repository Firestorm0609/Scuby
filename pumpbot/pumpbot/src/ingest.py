"""
PumpPortal ingestion service.

Connects to the PumpPortal websocket and:
  1. Subscribes to all new token creations -> inserts into `tokens`
  2. Subscribes to all migrations -> marks tokens as migrated
  3. Dynamically subscribes to trade events for recently-created tokens
     (capped at MAX_ACTIVE_SUBSCRIPTIONS, culled after ACTIVE_TRACK_SECONDS)
     -> inserts into `trades`

Run with: python -m src.ingest
"""

import asyncio
import json
import logging
import time

import websockets

from src.config import (
    PUMPPORTAL_WS_URL,
    PUMPPORTAL_API_KEY,
    ACTIVE_TRACK_SECONDS,
    MAX_ACTIVE_SUBSCRIPTIONS,
)
from src import db

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")

# in-memory set of mints we currently hold a trade subscription for
_subscribed: set[str] = set()
_subscribed_at: dict[str, float] = {}


def _ws_url() -> str:
    if PUMPPORTAL_API_KEY:
        return f"{PUMPPORTAL_WS_URL}?api-key={PUMPPORTAL_API_KEY}"
    return PUMPPORTAL_WS_URL


async def _send(ws, payload: dict):
    await ws.send(json.dumps(payload))


async def _handle_new_token(ws, msg: dict):
    mint = msg.get("mint")
    if not mint:
        return

    vsol = msg.get("vSolInBondingCurve")
    vtokens = msg.get("vTokensInBondingCurve")
    mcap = msg.get("marketCapSol")

    token = {
        "mint": mint,
        "name": msg.get("name"),
        "symbol": msg.get("symbol"),
        "creator": msg.get("traderPublicKey"),
        "created_at_epoch": time.time(),
        "initial_mcap_sol": mcap,
        "initial_vsol": vsol,
        "initial_vtokens": vtokens,
        "bonding_curve_key": msg.get("bondingCurveKey"),
        "uri": msg.get("uri"),
    }
    await db.insert_token(token)
    log.info("new token %s (%s) mcap=%.3f SOL", mint, msg.get("symbol"), mcap or 0)

    # also log the create event itself as a trade row (useful for the t=0 datapoint)
    await db.insert_trade({
        "mint": mint,
        "signature": msg.get("signature"),
        "trader": msg.get("traderPublicKey"),
        "tx_type": "create",
        "sol_amount": None,
        "token_amount": msg.get("initialBuy"),
        "vsol_in_curve": vsol,
        "vtokens_in_curve": vtokens,
        "mcap_sol": mcap,
    })

    await _maybe_subscribe_trade(ws, mint)


async def _handle_migration(msg: dict):
    mint = msg.get("mint")
    if mint:
        await db.mark_migrated(mint)
        log.info("migrated: %s", mint)


async def _handle_trade(msg: dict):
    mint = msg.get("mint")
    if not mint:
        return
    await db.insert_trade({
        "mint": mint,
        "signature": msg.get("signature"),
        "trader": msg.get("traderPublicKey"),
        "tx_type": msg.get("txType"),
        "sol_amount": msg.get("solAmount"),
        "token_amount": msg.get("tokenAmount"),
        "vsol_in_curve": msg.get("vSolInBondingCurve"),
        "vtokens_in_curve": msg.get("vTokensInBondingCurve"),
        "mcap_sol": msg.get("marketCapSol"),
    })


async def _maybe_subscribe_trade(ws, mint: str):
    if mint in _subscribed:
        return
    if len(_subscribed) >= MAX_ACTIVE_SUBSCRIPTIONS:
        await _cull_old_subscriptions(ws)
        if len(_subscribed) >= MAX_ACTIVE_SUBSCRIPTIONS:
            return  # still full even after culling; skip this one
    await _send(ws, {"method": "subscribeTokenTrade", "keys": [mint]})
    _subscribed.add(mint)
    _subscribed_at[mint] = time.time()


async def _cull_old_subscriptions(ws):
    now = time.time()
    stale = [m for m, t in _subscribed_at.items() if now - t > ACTIVE_TRACK_SECONDS]
    if not stale:
        return
    await _send(ws, {"method": "unsubscribeTokenTrade", "keys": stale})
    for m in stale:
        _subscribed.discard(m)
        _subscribed_at.pop(m, None)
    log.info("culled %d stale trade subscriptions", len(stale))


async def _periodic_cull(ws):
    while True:
        await asyncio.sleep(60)
        try:
            await _cull_old_subscriptions(ws)
        except Exception:
            log.exception("cull failed")


async def run():
    backoff = 1
    while True:
        try:
            async with websockets.connect(_ws_url(), ping_interval=20, ping_timeout=20) as ws:
                log.info("connected to PumpPortal")
                backoff = 1
                await _send(ws, {"method": "subscribeNewToken"})
                await _send(ws, {"method": "subscribeMigration"})

                cull_task = asyncio.create_task(_periodic_cull(ws))
                try:
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        tx_type = msg.get("txType")
                        if tx_type == "create":
                            await _handle_new_token(ws, msg)
                        elif tx_type in ("buy", "sell"):
                            await _handle_trade(msg)
                        elif "migration" in str(msg.get("method", "")).lower() or msg.get("pool"):
                            # migration events vary in shape; handle defensively
                            if msg.get("mint"):
                                await _handle_migration(msg)
                finally:
                    cull_task.cancel()
        except (websockets.ConnectionClosed, OSError) as e:
            log.warning("websocket disconnected (%s), reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)
        except Exception:
            log.exception("unexpected error, reconnecting in %ss", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    asyncio.run(run())

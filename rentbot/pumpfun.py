"""
Pump.fun Scanner — Finds new token launches on pump.fun.
Uses DexScreener API (free, no key) to find hot Solana tokens.
"""
import httpx
import time
from datetime import datetime

# Pump.fun bonding curve program
PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
DEXSCREENER = "https://api.dexscreener.com"

# Popular pump.fun tokens (with their mints for reference)
KNOWN_PUMPFUN = {
    "FARTCOIN": "9BB6NFEcjBCtnNLFko2FqVQBq8HHM13kCyYcdQbgpump",
    "BONK":     "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "WIF":      "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    "POPCAT":   "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
}


def search_pumpfun_tokens(limit=10):
    """Search DexScreener for pump.fun Solana tokens, sorted by recent activity."""
    try:
        # Search for pump.fun tokens on Solana
        r = httpx.get(
            f"{DEXSCREENER}/token-boosts/latest/v1",
            timeout=10,
        )
        if r.status_code == 200:
            tokens = r.json()
            results = []
            for t in tokens[:50]:
                if t.get("chainId") == "solana":
                    results.append({
                        "symbol": t.get("tokenAddress", "")[:8],
                        "mint": t.get("tokenAddress", ""),
                        "icon": t.get("icon", ""),
                        "boost": True,
                    })
            return results[:limit]
    except Exception as e:
        print(f"  PumpFun search error: {e}")
    return []


def get_trending_tokens(limit=10):
    """Get trending Solana tokens from DexScreener (includes pump.fun launches)."""
    try:
        r = httpx.get(
            f"{DEXSCREENER}/token-boosts/top/v1",
            timeout=10,
        )
        if r.status_code == 200:
            tokens = r.json()
            results = []
            for t in tokens:
                if t.get("chainId") == "solana":
                    results.append({
                        "symbol": t.get("tokenAddress", "")[:8],
                        "mint": t.get("tokenAddress", ""),
                        "icon": t.get("icon", ""),
                    })
            return results[:limit]
    except Exception as e:
        print(f"  Trending search error: {e}")
    return []


def get_token_pair(mint):
    """Get detailed pair data for a token from DexScreener."""
    try:
        r = httpx.get(
            f"{DEXSCREENER}/tokens/v1/solana/{mint}",
            timeout=10,
        )
        if r.status_code == 200:
            pairs = r.json()
            if pairs and len(pairs) > 0:
                pair = pairs[0]
                return {
                    "symbol": pair.get("baseToken", {}).get("symbol", "?"),
                    "name": pair.get("baseToken", {}).get("name", "?"),
                    "mint": mint,
                    "price_usd": float(pair.get("priceUsd", 0)),
                    "price_native": float(pair.get("priceNative", 0)),
                    "volume_24h": float(pair.get("volume", {}).get("h24", 0)),
                    "volume_6h": float(pair.get("volume", {}).get("h6", 0)),
                    "volume_1h": float(pair.get("volume", {}).get("h1", 0)),
                    "volume_5m": float(pair.get("volume", {}).get("m5", 0)),
                    "price_change_24h": float(pair.get("priceChange", {}).get("h24", 0)),
                    "price_change_1h": float(pair.get("priceChange", {}).get("h1", 0)),
                    "price_change_5m": float(pair.get("priceChange", {}).get("m5", 0)),
                    "liquidity_usd": float(pair.get("liquidity", {}).get("usd", 0)),
                    "fdv": float(pair.get("fdv", 0)),
                    "pair_created": pair.get("pairCreatedAt", 0),
                    "dex": pair.get("dexId", ""),
                    "pair_address": pair.get("pairAddress", ""),
                    "url": pair.get("url", ""),
                }
    except Exception as e:
        print(f"  Token pair error: {e}")
    return None


def get_multiple_tokens(mints):
    """Get price data for multiple tokens at once."""
    if not mints:
        return {}
    # DexScreener allows comma-separated mints (up to 30)
    mint_str = ",".join(mints[:30])
    try:
        r = httpx.get(
            f"{DEXSCREENER}/tokens/v1/solana/{mint_str}",
            timeout=10,
        )
        if r.status_code == 200:
            pairs = r.json()
            results = {}
            for pair in pairs:
                mint = pair.get("baseToken", {}).get("address", "")
                if mint:
                    results[mint] = {
                        "symbol": pair.get("baseToken", {}).get("symbol", "?"),
                        "name": pair.get("baseToken", {}).get("name", "?"),
                        "price_usd": float(pair.get("priceUsd", 0)),
                        "volume_1h": float(pair.get("volume", {}).get("h1", 0)),
                        "volume_24h": float(pair.get("volume", {}).get("h24", 0)),
                        "price_change_5m": float(pair.get("priceChange", {}).get("m5", 0)),
                        "price_change_1h": float(pair.get("priceChange", {}).get("h1", 0)),
                        "price_change_24h": float(pair.get("priceChange", {}).get("h24", 0)),
                        "liquidity_usd": float(pair.get("liquidity", {}).get("usd", 0)),
                        "fdv": float(pair.get("fdv", 0)),
                        "url": pair.get("url", ""),
                    }
            return results
    except Exception as e:
        print(f"  Multi-token error: {e}")
    return {}


def get_new_launches():
    """Find recently created pump.fun tokens (hot new launches)."""
    try:
        # DexScreener new pairs on Solana, sorted by creation time
        r = httpx.get(
            f"{DEXSCREENER}/token-boosts/latest/v1",
            timeout=10,
        )
        if r.status_code == 200:
            tokens = r.json()
            results = []
            for t in tokens[:30]:
                if t.get("chainId") == "solana":
                    # Get the pair data for each
                    mint = t.get("tokenAddress", "")
                    if mint:
                        pair = get_token_pair(mint)
                        if pair and pair["liquidity_usd"] > 50:
                            results.append(pair)
            return results[:5]
    except Exception as e:
        print(f"  New launches error: {e}")
    return []


def scan_for_opportunities():
    """Main scan — returns a list of tokens worth considering for trades.
    Enhanced scoring with multiple signals:
    - Volume momentum (1h vs 24h ratio)
    - Liquidity depth
    - Price momentum (5m, 1h, 24h)
    - Token age (fresh launches get bonus)
    - FDV vs liquidity ratio (hidden gems)
    """
    opportunities = []

    # 1. Get trending boosted tokens
    trending = get_trending_tokens(15)
    mints = [t["mint"] for t in trending if t["mint"]]

    # Also get latest launches for fresh tokens
    latest = search_pumpfun_tokens(10)
    for t in latest:
        if t["mint"] and t["mint"] not in mints:
            mints.append(t["mint"])

    # 2. Get their price data
    if mints:
        prices = get_multiple_tokens(mints)
        now_ms = int(datetime.now().timestamp() * 1000)

        for mint, data in prices.items():
            vol_1h = data.get("volume_1h", 0)
            vol_24h = data.get("volume_24h", 0)
            liq = data.get("liquidity_usd", 0)
            chg_5m = data.get("price_change_5m", 0)
            chg_1h = data.get("price_change_1h", 0)
            chg_24h = data.get("price_change_24h", 0)
            fdv = data.get("fdv", 0)
            pair_created = data.get("pair_created", 0)

            score = 0

            # ── Volume scoring ──
            if vol_1h > 5000: score += 1     # any activity
            if vol_1h > 20000: score += 2    # decent volume
            if vol_1h > 100000: score += 3   # hot volume
            if vol_1h > 500000: score += 2   # extremely hot

            # Volume momentum: 1h vs 24h ratio (normalized)
            if vol_24h > 0:
                vol_ratio = vol_1h / (vol_24h / 24 + 1)
                if vol_ratio > 2: score += 2   # volume accelerating
                if vol_ratio > 5: score += 3   # parabolic volume

            # ── Liquidity scoring ──
            if liq > 1000: score += 1
            if liq > 10000: score += 2
            if liq > 50000: score += 2

            # FDV/Liquidity ratio — lower = more room to grow
            if fdv > 0 and liq > 0:
                fdv_liq_ratio = fdv / liq
                if fdv_liq_ratio < 10: score += 2   # low ratio = gem potential
                if fdv_liq_ratio < 5: score += 1    # very low = moonshot

            # ── Price momentum ──
            if chg_5m > 5: score += 1          # short-term pump
            if chg_5m > 20: score += 2         # strong short-term pump
            if chg_1h > 10: score += 2         # 1h momentum
            if chg_1h > 50: score += 2         # massive 1h pump
            if chg_1h < -20: score += 1        # dip buy potential
            if chg_1h < -40: score += 2        # deep dip = opportunity

            # Multi-timeframe alignment
            if chg_5m > 0 and chg_1h > 0 and chg_24h > 0:
                score += 2  # all timeframes green = strong trend

            # ── Token age bonus ──
            if pair_created > 0:
                age_hours = (now_ms - pair_created) / (1000 * 3600)
                if age_hours < 1: score += 3    # brand new = moonshot potential
                elif age_hours < 6: score += 2  # fresh
                elif age_hours < 24: score += 1 # newish

            # ── Risk-adjusted scoring ──
            # High volume + low liquidity = danger (but also opportunity)
            if vol_1h > 50000 and liq < 5000:
                score += 1  # volatile but tradeable

            # Only include if meets minimum threshold
            if score >= 3:
                data["score"] = score
                data["mint"] = mint
                data["age_hours"] = round((now_ms - pair_created) / (1000 * 3600), 1) if pair_created > 0 else -1
                data["vol_momentum"] = round(vol_1h / (vol_24h / 24 + 1), 2) if vol_24h > 0 else 0
                opportunities.append(data)

    # Sort by score then volume momentum
    opportunities.sort(key=lambda x: (x.get("score", 0), x.get("vol_momentum", 0)), reverse=True)
    return opportunities[:10]


def get_sol_price():
    """Get current SOL price in USD."""
    try:
        r = httpx.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "solana", "vs_currencies": "usd"},
            timeout=5,
        )
        if r.status_code == 200:
            return r.json().get("solana", {}).get("usd", 150.0)
    except:
        pass
    return 150.0  # fallback estimate


if __name__ == "__main__":
    print("=== Pump.fun Scanner ===")
    print("\nSearching for opportunities...")
    opps = scan_for_opportunities()
    for o in opps:
        print(f"\n  {o['symbol']} ({o['name']})")
        print(f"    Price: ${o['price_usd']:.8f}")
        print(f"    1h Vol: ${o['volume_1h']:,.0f} | Liq: ${o['liquidity_usd']:,.0f}")
        print(f"    1h Change: {o['price_change_1h']:+.1f}% | Score: {o['score']}")

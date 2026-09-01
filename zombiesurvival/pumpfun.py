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
    """Main scan — returns a list of tokens worth considering for trades."""
    opportunities = []
    seen_mints = set()

    # 1. Get trending boosted tokens
    trending = get_trending_tokens(30)
    mints = [t["mint"] for t in trending if t["mint"]]

    # 2. Also search DexScreener for pump.fun Solana tokens
    try:
        r = httpx.get(f"{DEXSCREENER}/latest/dex/search?q=pump", timeout=10)
        if r.status_code == 200:
            for pair in r.json().get("pairs", [])[:20]:
                mint = pair.get("baseToken", {}).get("address", "")
                if mint and mint not in seen_mints:
                    mints.append(mint)
    except:
        pass

    # 3. Get price data for all mints
    if mints:
        prices = get_multiple_tokens(mints[:30])
        for mint, data in prices.items():
            if mint in seen_mints:
                continue
            seen_mints.add(mint)
            vol = data.get("volume_1h", 0)
            liq = data.get("liquidity_usd", 0)
            chg = data.get("price_change_1h", 0)
            fdv = data.get("fdv", 0)

            score = 0
            if vol > 5000: score += 1        # any volume
            if vol > 10000: score += 2        # decent volume
            if vol > 50000: score += 2        # great volume
            if liq > 2000: score += 1         # has liquidity
            if liq > 10000: score += 1        # good liquidity
            if abs(chg) > 5: score += 1       # moving
            if chg > 10: score += 2           # pumping
            if chg < -15: score += 1          # dip buy potential
            if fdv > 50000 and fdv < 5000000: score += 1  # sweet spot mcap

            if score >= 1:
                data["score"] = score
                data["mint"] = mint
                opportunities.append(data)

    # Sort by score then volume
    opportunities.sort(key=lambda x: (x.get("score", 0), x.get("volume_1h", 0)), reverse=True)
    return opportunities[:12]


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

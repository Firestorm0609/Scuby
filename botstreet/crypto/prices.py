"""
Price Module — DexScreener for crypto prices
"""
import httpx
from datetime import datetime


def get_current_price(token_address: str, chain: str = "solana") -> dict:
    """Get current token price from DexScreener."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{token_address}"
    with httpx.Client(timeout=10) as client:
        resp = client.get(url)
        if resp.status_code == 200:
            data = resp.json()
            pairs = data.get("pairs", [])
            if pairs:
                pair = pairs[0]
                return {
                    "ticker": pair.get("baseToken", {}).get("symbol", "???"),
                    "name": pair.get("baseToken", {}).get("name", "Unknown"),
                    "price": float(pair.get("priceUsd", "0")),
                    "change_24h": float(pair.get("priceChange", {}).get("h24", "0")),
                    "volume_24h": float(pair.get("volume", {}).get("h24", 0)),
                    "liquidity": float(pair.get("liquidity", {}).get("usd", 0)),
                    "address": token_address,
                    "chain": pair.get("chainId", chain),
                    "timestamp": datetime.now().isoformat(),
                }
    return {"error": f"No price data for {token_address}"}


def get_current_prices(token_addresses: list) -> list:
    """Get prices for multiple tokens."""
    results = []
    for addr in token_addresses:
        try:
            result = get_current_price(addr)
            results.append(result)
        except Exception as e:
            results.append({"address": addr, "error": str(e)})
    return results


def search_token(query: str) -> list:
    """Search for a token by name or symbol."""
    url = f"https://api.dexscreener.com/latest/dex/search?q={query}"
    with httpx.Client(timeout=10) as client:
        resp = client.get(url)
        if resp.status_code == 200:
            data = resp.json()
            pairs = data.get("pairs", [])[:5]
            return [
                {
                    "symbol": p.get("baseToken", {}).get("symbol", "?"),
                    "name": p.get("baseToken", {}).get("name", "?"),
                    "price": float(p.get("priceUsd", "0")),
                    "change_24h": float(p.get("priceChange", {}).get("h24", "0")),
                    "address": p.get("baseToken", {}).get("address", ""),
                    "chain": p.get("chainId", "?"),
                    "liquidity": float(p.get("liquidity", {}).get("usd", 0)),
                }
                for p in pairs
            ]
    return []


def get_trending(chain: str = "solana") -> list:
    """Get trending tokens on a chain."""
    url = f"https://api.dexscreener.com/latest/dex/search?q=memecoin&chainId={chain}"
    with httpx.Client(timeout=10) as client:
        resp = client.get(url)
        if resp.status_code == 200:
            data = resp.json()
            pairs = data.get("pairs", [])[:10]
            return [
                {
                    "symbol": p.get("baseToken", {}).get("symbol", "?"),
                    "name": p.get("baseToken", {}).get("name", "?"),
                    "price": float(p.get("priceUsd", "0")),
                    "change_24h": float(p.get("priceChange", {}).get("h24", "0")),
                    "volume": float(p.get("volume", {}).get("h24", 0)),
                    "address": p.get("baseToken", {}).get("address", ""),
                }
                for p in pairs
            ]
    return []

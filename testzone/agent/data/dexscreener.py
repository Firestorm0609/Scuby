"""DexScreener API client — token price data and market info."""

import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class DexScreenerClient:
    """Client for DexScreener's free API."""

    BASE_URL = "https://api.dexscreener.com/latest"

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=10.0)

    async def get_token_price(self, address: str) -> Optional[dict]:
        """Get price and market data for a Solana token."""
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/dex/tokens/{address}",
            )
            resp.raise_for_status()
            data = resp.json()

            pairs = data.get("pairs", [])
            if not pairs:
                return None

            # Get the pair with highest liquidity
            best_pair = max(pairs, key=lambda p: p.get("liquidity", {}).get("usd", 0))

            return {
                "price_usd": float(best_pair.get("priceUsd", 0)),
                "price_native": float(best_pair.get("priceNative", 0)),
                "market_cap_usd": float(best_pair.get("marketCap", 0)),
                "fdv": float(best_pair.get("fdv", 0)),
                "liquidity_usd": float(best_pair.get("liquidity", {}).get("usd", 0)),
                "volume_24h": float(best_pair.get("volume", {}).get("h24", 0)),
                "volume_6h": float(best_pair.get("volume", {}).get("h6", 0)),
                "volume_1h": float(best_pair.get("volume", {}).get("h1", 0)),
                "price_change_5m": float(best_pair.get("priceChange", {}).get("m5", 0)),
                "price_change_1h": float(best_pair.get("priceChange", {}).get("h1", 0)),
                "price_change_24h": float(best_pair.get("priceChange", {}).get("h24", 0)),
                "txns_5m_buys": best_pair.get("txns", {}).get("m5", {}).get("buys", 0),
                "txns_5m_sells": best_pair.get("txns", {}).get("m5", {}).get("sells", 0),
                "pair_address": best_pair.get("pairAddress", ""),
                "dex": best_pair.get("dexId", ""),
            }

        except Exception as e:
            logger.error(f"Failed to fetch DexScreener data for {address}: {e}")
            return None

    async def search_tokens(self, query: str) -> list[dict]:
        """Search for tokens by name/symbol."""
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/dex/search",
                params={"q": query},
            )
            resp.raise_for_status()
            return resp.json().get("pairs", [])
        except Exception as e:
            logger.error(f"Failed to search DexScreener: {e}")
            return []

    async def close(self):
        await self.client.aclose()

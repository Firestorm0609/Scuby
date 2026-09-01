"""pump.fun API client — fetches new token launches."""

import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class PumpFunClient:
    """Client for pump.fun's API endpoints."""

    BASE_URL = "https://frontend-api-v3.pump.fun"
    
    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=15.0,
            headers={"Accept": "application/json"},
        )

    async def get_new_tokens(self, limit: int = 20, offset: int = 0) -> list[dict]:
        """Fetch recently created tokens from pump.fun."""
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/coins",
                params={
                    "limit": limit,
                    "offset": offset,
                    "sort": "created_timestamp",
                    "order": "DESC",
                    "includeNsfw": "false",
                },
            )
            resp.raise_for_status()
            data = resp.json()
            
            tokens = []
            for item in data:
                tokens.append({
                    "address": item.get("mint"),
                    "name": item.get("name", ""),
                    "symbol": item.get("symbol", ""),
                    "description": item.get("description", ""),
                    "image_uri": item.get("image_uri", ""),
                    "website": item.get("website", ""),
                    "twitter": item.get("twitter", ""),
                    "telegram": item.get("telegram", ""),
                    "market_cap_sol": item.get("market_cap_sol", 0),
                    "usd_market_cap": item.get("usd_market_cap", 0),
                    "reply_count": item.get("reply_count", 0),
                    "creator": item.get("creator", ""),
                    "created_timestamp": item.get("created_timestamp", 0),
                    "complete": item.get("complete", False),
                })
            
            logger.info(f"Fetched {len(tokens)} new tokens from pump.fun")
            return tokens

        except Exception as e:
            logger.error(f"Failed to fetch pump.fun tokens: {e}")
            return []

    async def get_token(self, address: str) -> Optional[dict]:
        """Fetch a specific token by address."""
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/coins/{address}",
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch token {address}: {e}")
            return None

    async def get_token_trades(self, address: str, limit: int = 50) -> list[dict]:
        """Fetch recent trades for a token."""
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/coins/{address}/trades",
                params={"limit": limit},
            )
            resp.raise_for_status()
            return resp.json().get("trades", [])
        except Exception as e:
            logger.error(f"Failed to fetch trades for {address}: {e}")
            return []

    async def close(self):
        await self.client.aclose()

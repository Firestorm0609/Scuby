"""Solscan API client — wallet data and token holder info."""

import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class SolscanClient:
    """Client for Solscan's public API."""

    BASE_URL = "https://public-api.solscan.io"

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=10.0,
            headers={"Accept": "application/json"},
        )

    async def get_wallet_balance(self, wallet_address: str) -> Optional[dict]:
        """Get SOL balance for a wallet."""
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/wallet/getBalance",
                params={"wallet": wallet_address},
            )
            resp.raise_for_status()
            data = resp.json().get("data", {})
            return {
                "lamports": data.get("lamports", 0),
                "sol_balance": data.get("lamports", 0) / 1e9,
            }
        except Exception as e:
            logger.error(f"Failed to fetch wallet balance: {e}")
            return None

    async def get_token_holders(self, token_address: str) -> Optional[dict]:
        """Get holder distribution for a token."""
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/token/holders",
                params={
                    "tokenAddress": token_address,
                    "limit": 20,
                    "offset": 0,
                },
            )
            resp.raise_for_status()
            data = resp.json()

            holders = data.get("data", [])
            total_holders = data.get("total", 0)

            # Calculate top 10 concentration
            if holders:
                top_10_amount = sum(
                    float(h.get("amount", 0)) for h in holders[:10]
                )
                total_amount = sum(
                    float(h.get("amount", 0)) for h in holders
                )
                top_10_pct = top_10_amount / total_amount if total_amount > 0 else 1.0
            else:
                top_10_pct = 1.0

            return {
                "total_holders": total_holders,
                "top_10_concentration": top_10_pct,
                "holders": holders[:10],  # top 10
            }

        except Exception as e:
            logger.error(f"Failed to fetch token holders: {e}")
            return None

    async def get_wallet_tokens(self, wallet_address: str) -> list[dict]:
        """Get all token holdings for a wallet."""
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/wallet/tokenList",
                params={"wallet": wallet_address},
            )
            resp.raise_for_status()
            return resp.json().get("data", [])
        except Exception as e:
            logger.error(f"Failed to fetch wallet tokens: {e}")
            return []

    async def get_token_creation_info(self, token_address: str) -> Optional[dict]:
        """Get token creation/mint info."""
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/token/meta",
                params={"tokenAddress": token_address},
            )
            resp.raise_for_status()
            return resp.json().get("data", {})
        except Exception as e:
            logger.error(f"Failed to fetch token creation info: {e}")
            return None

    async def close(self):
        await self.client.aclose()

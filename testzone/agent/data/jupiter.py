"""Jupiter API client — swap quotes and token routing."""

import httpx
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# SOL and USDC on Solana
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


class JupiterClient:
    """Client for Jupiter Aggregator API (v6)."""

    BASE_URL = "https://quote-api.jup.ag/v6"

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=15.0)

    async def get_quote(self, input_mint: str, output_mint: str,
                        amount: int, slippage_bps: int = 500) -> Optional[dict]:
        """Get a swap quote.
        
        Args:
            input_mint: Token to sell (SOL mint for buying with SOL)
            output_mint: Token to buy
            amount: Amount in smallest unit (lamports for SOL)
            slippage_bps: Max slippage in basis points (500 = 5%)
        """
        try:
            resp = await self.client.get(
                f"{self.BASE_URL}/quote",
                params={
                    "inputMint": input_mint,
                    "outputMint": output_mint,
                    "amount": str(amount),
                    "slippageBps": slippage_bps,
                    "onlyDirectRoutes": "false",
                    "asLegacyTransaction": "false",
                },
            )
            resp.raise_for_status()
            data = resp.json()

            return {
                "input_mint": data.get("inputMint"),
                "output_mint": data.get("outputMint"),
                "in_amount": int(data.get("inAmount", 0)),
                "out_amount": int(data.get("outAmount", 0)),
                "price_impact_pct": float(data.get("priceImpactPct", 0)),
                "route_plan": data.get("routePlan", []),
                "raw_quote": data,
            }

        except Exception as e:
            logger.error(f"Failed to get Jupiter quote: {e}")
            return None

    async def get_sol_to_token_quote(self, token_address: str,
                                      sol_amount: float,
                                      slippage_pct: float = 5.0) -> Optional[dict]:
        """Convenience: get quote for buying a token with SOL.
        
        Args:
            token_address: Token mint address
            sol_amount: Amount of SOL to spend
            slippage_pct: Max slippage percentage
        """
        lamports = int(sol_amount * 1e9)
        slippage_bps = int(slippage_pct * 100)

        return await self.get_quote(
            input_mint=SOL_MINT,
            output_mint=token_address,
            amount=lamports,
            slippage_bps=slippage_bps,
        )

    async def get_token_to_sol_quote(self, token_address: str,
                                      token_amount: int,
                                      slippage_pct: float = 5.0) -> Optional[dict]:
        """Convenience: get quote for selling a token for SOL."""
        slippage_bps = int(slippage_pct * 100)

        return await self.get_quote(
            input_mint=token_address,
            output_mint=SOL_MINT,
            amount=token_amount,
            slippage_bps=slippage_bps,
        )

    async def close(self):
        await self.client.aclose()

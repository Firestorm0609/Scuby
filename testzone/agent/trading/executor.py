"""Trade executor — builds and sends Solana transactions.

Uses Solana web3.py + solders for transaction building.
Supports both pump.fun direct buys and Jupiter swaps.
"""

import logging
import json
import os
from typing import Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# SOL mint address
SOL_MINT = "So11111111111111111111111111111111111111112"


class TradeExecutor:
    """Handles Solana transaction building and execution."""

    def __init__(self, config: dict):
        self.config = config
        self.network = config.get("wallet", {}).get("network", "mainnet-beta")
        self.keypair_path = config.get("wallet", {}).get("keypair_path", "./wallet.json")
        
        # Lazy-loaded Solana client
        self._client = None
        self._keypair = None

    def _get_rpc_url(self) -> str:
        """Get the RPC URL for the configured network."""
        if self.network == "devnet":
            return "https://api.devnet.solana.com"
        elif self.network == "testnet":
            return "https://api.testnet.solana.com"
        else:
            # Mainnet — use public RPC or configured endpoint
            return os.getenv(
                "SOLANA_RPC_URL",
                "https://api.mainnet-beta.solana.com"
            )

    def _ensure_client(self):
        """Lazily initialize the Solana client."""
        if self._client is not None:
            return

        try:
            from solana.rpc.async_api import AsyncClient
            self._client = AsyncClient(self._get_rpc_url())
            logger.info(f"Connected to Solana {self.network}")
        except ImportError:
            logger.error(
                "solana-py not installed. Run: pip install solana solders"
            )
            raise

    def _ensure_keypair(self):
        """Lazily load the wallet keypair."""
        if self._keypair is not None:
            return

        try:
            from solders.keypair import Keypair
            
            if not os.path.exists(self.keypair_path):
                # Generate a new keypair for devnet testing
                if self.network == "devnet":
                    self._keypair = Keypair()
                    # Save it
                    with open(self.keypair_path, "w") as f:
                        json.dump(
                            list(self._keypair.secret_key),
                            f
                        )
                    logger.warning(
                        f"Generated new devnet wallet. "
                        f"Address: {self._keypair.pubkey()}"
                    )
                else:
                    raise FileNotFoundError(
                        f"Keypair not found: {self.keypair_path}. "
                        f"Generate with: solana-keygen new -o {self.keypair_path}"
                    )
            else:
                with open(self.keypair_path, "r") as f:
                    secret = json.load(f)
                self._keypair = Keypair(bytes(secret))
                logger.info(f"Loaded wallet: {self._keypair.pubkey()}")

        except ImportError:
            logger.error("solders not installed. Run: pip install solders")
            raise

    async def get_balance(self) -> float:
        """Get the wallet's SOL balance."""
        self._ensure_client()
        self._ensure_keypair()

        try:
            resp = await self._client.get_balance(self._keypair.pubkey())
            lamports = resp.value
            return lamports / 1e9
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            return 0.0

    async def buy_with_jupiter(self, token_address: str, 
                                amount_sol: float,
                                slippage_pct: float = 5.0) -> Optional[dict]:
        """Buy a token using Jupiter aggregator.
        
        Args:
            token_address: Token mint address to buy
            amount_sol: Amount of SOL to spend
            slippage_pct: Max slippage percentage
            
        Returns:
            Transaction result dict or None on failure
        """
        self._ensure_client()
        self._ensure_keypair()

        try:
            from solana.transaction import Transaction
            from solders.pubkey import Pubkey
            from solders.system_program import TransferParams, transfer
            from solana.rpc.async_api import AsyncClient

            lamports = int(amount_sol * 1e9)
            
            logger.info(
                f"Buying {token_address[:8]}... "
                f"for {amount_sol:.4f} SOL via Jupiter"
            )

            # For a real implementation, we'd use Jupiter's swap transaction:
            # 1. Get quote from Jupiter API
            # 2. Get serialized swap transaction
            # 3. Deserialize, sign, and send
            
            # Simplified: direct SOL transfer to Jupiter router
            # In production, use the full Jupiter swap flow
            
            # TODO: Implement full Jupiter swap transaction
            # This requires deserializing the Jupiter response transaction,
            # adding our signature, and sending it
            
            logger.warning(
                "Jupiter swap not fully implemented. "
                "This is a placeholder. See TODO in executor.py."
            )
            
            return {
                "success": False,
                "error": "Jupiter swap not yet implemented",
                "tx_hash": None,
                "message": "Need to implement full Jupiter swap flow",
            }

        except Exception as e:
            logger.error(f"Buy transaction failed: {e}")
            return {"success": False, "error": str(e), "tx_hash": None}

    async def sell_with_jupiter(self, token_address: str,
                                 token_amount: int,
                                 slippage_pct: float = 5.0) -> Optional[dict]:
        """Sell a token using Jupiter aggregator.
        
        Args:
            token_address: Token mint address to sell
            token_amount: Amount of tokens to sell (in smallest unit)
            slippage_pct: Max slippage percentage
            
        Returns:
            Transaction result dict or None on failure
        """
        self._ensure_client()
        self._ensure_keypair()

        try:
            logger.info(
                f"Selling {token_address[:8]}... "
                f"(amount: {token_amount}) via Jupiter"
            )

            # TODO: Implement full Jupiter swap transaction
            logger.warning("Jupiter sell not fully implemented yet.")
            
            return {
                "success": False,
                "error": "Jupiter sell not yet implemented",
                "tx_hash": None,
            }

        except Exception as e:
            logger.error(f"Sell transaction failed: {e}")
            return {"success": False, "error": str(e), "tx_hash": None}

    async def buy_pumpfun(self, token_address: str,
                           amount_sol: float) -> Optional[dict]:
        """Buy a token directly on pump.fun bonding curve.
        
        This is for tokens that haven't graduated to Raydium yet.
        Requires the pump.fun program instructions.
        """
        logger.info(
            f"Buying {token_address[:8]}... "
            f"for {amount_sol:.4f} SOL on pump.fun"
        )

        # TODO: Implement pump.fun program interaction
        # This requires:
        # 1. Derive the bonding curve PDA
        # 2. Build the buy instruction
        # 3. Add compute budget + priority fee
        # 4. Sign and send
        
        logger.warning("pump.fun direct buy not fully implemented yet.")
        return {
            "success": False,
            "error": "pump.fun buy not yet implemented",
            "tx_hash": None,
        }

    async def close(self):
        """Clean up connections."""
        if self._client:
            await self._client.close()

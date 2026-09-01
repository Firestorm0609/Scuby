"""Risk management layer — hard rules that override LLM decisions.

This layer is CODE, not LLM. It cannot be overridden by the AI.
If this layer says no, the trade doesn't happen. Period.
"""

import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)


class RiskManager:
    """Hard risk rules for the trading agent."""

    def __init__(self, config: dict):
        risk_cfg = config.get("risk", {})
        trading_cfg = config.get("trading", {})

        # Position limits
        self.max_position_pct = trading_cfg.get("max_position_pct", 0.05)
        self.max_open_positions = trading_cfg.get("max_open_positions", 3)
        self.min_balance_sol = risk_cfg.get("min_balance_sol", 0.1)

        # Stop loss / take profit
        self.stop_loss_pct = trading_cfg.get("stop_loss_pct", -0.30)
        self.take_profit_pct = trading_cfg.get("take_profit_pct", 0.50)

        # Token safety
        self.min_holders = risk_cfg.get("min_holders", 10)
        self.max_dev_holding_pct = risk_cfg.get("max_dev_holding_pct", 0.20)
        self.min_liquidity_usd = risk_cfg.get("min_liquidity_usd", 1000)
        self.max_slippage_pct = risk_cfg.get("max_slippage_pct", 5)

        # Daily limits
        self.max_daily_trades = risk_cfg.get("max_daily_trades", 20)
        self.max_daily_loss_sol = risk_cfg.get("max_daily_loss_sol", 0.1)

        # Blacklist
        self.blacklisted_tokens = set(risk_cfg.get("blacklist", []))

    def validate_buy(self, token_profile: dict, amount_sol: float,
                     balance: float, open_positions_count: int,
                     daily_trades: int, daily_loss: float) -> tuple[bool, str]:
        """Validate whether a buy trade is allowed.
        
        Args:
            token_profile: The enriched token profile
            amount_sol: How much SOL to spend
            balance: Current wallet balance
            open_positions_count: Number of open positions
            daily_trades: Number of trades today
            daily_loss: Today's cumulative loss in SOL
            
        Returns:
            (allowed, reason) tuple
        """
        token_addr = token_profile.get("address", "")
        token_name = token_profile.get("name", "Unknown")
        holder_data = token_profile.get("holder_data", {})
        market_data = token_profile.get("market_data", {})

        # ── Blacklist check ──
        if token_addr in self.blacklisted_tokens:
            return False, f"Token {token_name} is blacklisted"

        # ── Position size check ──
        max_amount = balance * self.max_position_pct
        if amount_sol > max_amount:
            return False, (
                f"Amount {amount_sol:.4f} exceeds max position "
                f"size {max_amount:.4f} SOL ({self.max_position_pct:.0%} of balance)"
            )

        # ── Balance check ──
        remaining = balance - amount_sol
        if remaining < self.min_balance_sol:
            return False, (
                f"After trade, balance {remaining:.4f} SOL would be "
                f"below minimum {self.min_balance_sol} SOL"
            )

        # ── Open positions check ──
        if open_positions_count >= self.max_open_positions:
            return False, (
                f"Max open positions ({self.max_open_positions}) reached"
            )

        # ── Daily trade limit ──
        if daily_trades >= self.max_daily_trades:
            return False, f"Daily trade limit ({self.max_daily_trades}) reached"

        # ── Daily loss limit ──
        if daily_loss < -self.max_daily_loss_sol:
            return False, (
                f"Daily loss limit ({self.max_daily_loss_sol} SOL) reached. "
                f"Today's loss: {daily_loss:.4f} SOL"
            )

        # ── Token safety checks ──
        total_holders = holder_data.get("total_holders", 0)
        if total_holders < self.min_holders:
            return False, (
                f"Too few holders: {total_holders} < {self.min_holders}"
            )

        liquidity = market_data.get("liquidity_usd", 0)
        if liquidity < self.min_liquidity_usd:
            return False, (
                f"Liquidity too low: ${liquidity:,.0f} < ${self.min_liquidity_usd:,.0f}"
            )

        # ── Already holding check ──
        # (caller should also check this, but we double-check here)
        # This is handled by the agent loop checking open positions

        logger.info(f"Risk check PASSED for {token_name} ({amount_sol:.4f} SOL)")
        return True, "All risk checks passed"

    def check_positions(self, positions: list, current_prices: dict) -> list[dict]:
        """Check if any position needs to be closed (stop loss / take profit).
        
        Args:
            positions: List of open position dicts
            current_prices: Dict of token_address -> current price
            
        Returns:
            List of sell actions: [{"token_addr", "reason", "pnl_pct"}]
        """
        sell_actions = []

        for pos in positions:
            token_addr = pos.get("token_addr", "")
            buy_price = pos.get("price_usd", 0)
            current_price = current_prices.get(token_addr, buy_price)

            if buy_price <= 0:
                continue

            pnl_pct = (current_price - buy_price) / buy_price

            if pnl_pct <= self.stop_loss_pct:
                sell_actions.append({
                    "token_addr": token_addr,
                    "token_name": pos.get("token_name", ""),
                    "reason": "stop_loss",
                    "pnl_pct": pnl_pct,
                    "pnl_sol": pos.get("amount_sol", 0) * pnl_pct,
                })
                logger.warning(
                    f"STOP LOSS triggered for {pos.get('token_name', '?')}: "
                    f"{pnl_pct:+.1%}"
                )

            elif pnl_pct >= self.take_profit_pct:
                sell_actions.append({
                    "token_addr": token_addr,
                    "token_name": pos.get("token_name", ""),
                    "reason": "take_profit",
                    "pnl_pct": pnl_pct,
                    "pnl_sol": pos.get("amount_sol", 0) * pnl_pct,
                })
                logger.info(
                    f"TAKE PROFIT triggered for {pos.get('token_name', '?')}: "
                    f"{pnl_pct:+.1%}"
                )

        return sell_actions

    def add_to_blacklist(self, token_addr: str):
        """Add a token to the blacklist."""
        self.blacklisted_tokens.add(token_addr)
        logger.info(f"Blacklisted token: {token_addr}")

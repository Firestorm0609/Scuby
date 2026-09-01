"""Decision engine — combines LLM reasoning with hard rules."""

import logging
import json
from typing import Optional

from .llm import LLMClient
from .prompts import SYSTEM_PROMPT, BUY_DECISION_PROMPT, SELL_DECISION_PROMPT

logger = logging.getLogger(__name__)


class DecisionEngine:
    """Makes trading decisions using LLM + rules."""

    def __init__(self, llm: LLMClient):
        self.llm = llm

    async def decide_buy(self, token_profile: dict, balance: float,
                         max_position: float, open_positions: list,
                         recent_trades: list, win_rate: float,
                         daily_pnl: float) -> Optional[dict]:
        """Decide whether to buy a token.
        
        Returns:
            Decision dict with action, amount, confidence, reasoning
        """
        token_address = token_profile.get("address", "unknown")
        token_name = token_profile.get("name", "Unknown")

        # Format recent trades for the prompt
        trades_text = "None yet" if not recent_trades else "\n".join(
            f"  - {t.get('token_name', '?')}: {t.get('action')} "
            f"{t.get('amount_sol', 0):.4f} SOL → "
            f"PnL: {t.get('pnl_pct', 0):+.1f}%"
            for t in recent_trades[:5]
        )

        prompt = BUY_DECISION_PROMPT.format(
            token_profile=self.llm._format_profile(token_profile) 
                if hasattr(self.llm, '_format_profile') 
                else json.dumps(token_profile, indent=2, default=str),
            balance=f"{balance:.4f}",
            open_positions=len(open_positions),
            recent_trades=trades_text,
            win_rate=win_rate,
            daily_pnl=daily_pnl,
            max_position=max_position,
            token_address=token_address,
        )

        response = await self.llm.chat_json(prompt, system=SYSTEM_PROMPT)

        if response is None:
            logger.warning(f"LLM returned no response for {token_name}")
            return {"action": "hold", "confidence": 0, "reasoning": "LLM unavailable"}

        # Validate the response
        action = response.get("action", "hold")
        amount = float(response.get("amount_sol", 0))
        confidence = float(response.get("confidence", 0))
        reasoning = response.get("reasoning", "No reasoning provided")

        # Clamp amount to max position size
        if amount > max_position:
            amount = max_position

        # Enforce minimum confidence
        if confidence < 0.6:
            action = "hold"
            reasoning = f"Confidence too low ({confidence:.0%}): {reasoning}"

        decision = {
            "action": action,
            "token_address": token_address,
            "token_name": token_name,
            "amount_sol": amount,
            "confidence": confidence,
            "reasoning": reasoning,
        }

        logger.info(
            f"Decision for {token_name}: {action} "
            f"(confidence: {confidence:.0%}, amount: {amount:.4f} SOL)"
        )

        return decision

    async def decide_sell(self, token_name: str, token_symbol: str,
                          token_address: str, buy_price: float,
                          current_price: float, pnl_pct: float,
                          pnl_sol: float, hold_time: str,
                          liquidity: float, volume_1h: float,
                          price_change_1h: float) -> Optional[dict]:
        """Decide whether to sell an existing position."""
        prompt = SELL_DECISION_PROMPT.format(
            token_name=token_name,
            token_symbol=token_symbol,
            token_address=token_address,
            buy_price=buy_price,
            current_price=current_price,
            pnl_pct=pnl_pct,
            pnl_sol=pnl_sol,
            hold_time=hold_time,
            liquidity=liquidity,
            volume_1h=volume_1h,
            price_change_1h=price_change_1h,
        )

        response = await self.llm.chat_json(prompt, system=SYSTEM_PROMPT)

        if response is None:
            return {"action": "hold", "reasoning": "LLM unavailable"}

        return {
            "action": response.get("action", "hold"),
            "token_address": token_address,
            "reasoning": response.get("reasoning", "No reasoning"),
        }

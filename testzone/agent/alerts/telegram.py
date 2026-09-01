"""Telegram bot alerts — sends notifications to Telegram."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


class TelegramAlerts:
    """Sends alerts to a Telegram chat."""

    def __init__(self, bot_token: str = "", chat_id: str = "", enabled: bool = False):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = enabled and bool(bot_token and chat_id)
        self._client = None

        if self.enabled:
            logger.info("Telegram alerts enabled")
        else:
            logger.info("Telegram alerts disabled (no token/chat configured)")

    async def _send(self, message: str) -> bool:
        """Send a message to Telegram."""
        if not self.enabled:
            return False

        try:
            import httpx
            if self._client is None:
                self._client = httpx.AsyncClient(timeout=10.0)

            resp = await self._client.post(
                f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": message,
                    "parse_mode": "Markdown",
                },
            )
            resp.raise_for_status()
            return True

        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    async def buy_alert(self, token_name: str, token_address: str,
                        amount_sol: float, confidence: float,
                        reasoning: str):
        """Alert on buy."""
        msg = (
            f"🟢 *BUY* {token_name}\n"
            f"Amount: `{amount_sol:.4f} SOL`\n"
            f"Confidence: {confidence:.0%}\n"
            f"Address: `{token_address}`\n"
            f"Reason: {reasoning}"
        )
        await self._send(msg)

    async def sell_alert(self, token_name: str, token_address: str,
                         pnl_pct: float, pnl_sol: float, reason: str):
        """Alert on sell."""
        emoji = "🟢" if pnl_pct >= 0 else "🔴"
        sign = "+" if pnl_pct >= 0 else ""
        msg = (
            f"{emoji} *SELL* {token_name}\n"
            f"PnL: `{sign}{pnl_pct:.1%}` ({pnl_sol:+.6f} SOL)\n"
            f"Reason: {reason}\n"
            f"Address: `{token_address}`"
        )
        await self._send(msg)

    async def risk_alert(self, message: str):
        """Alert on risk events."""
        msg = f"⚠️ *RISK* {message}"
        await self._send(msg)

    async def summary(self, balance: float, total_pnl: float,
                      open_positions: int, win_rate: float):
        """Send hourly summary."""
        sign = "+" if total_pnl >= 0 else ""
        msg = (
            f"📊 *Hourly Summary*\n"
            f"Balance: `{balance:.4f} SOL`\n"
            f"PnL: `{sign}{total_pnl:.4f} SOL`\n"
            f"Win Rate: {win_rate:.1%}\n"
            f"Open Positions: {open_positions}"
        )
        await self._send(msg)

    async def error(self, message: str):
        """Alert on errors."""
        msg = f"❌ *ERROR* {message}"
        await self._send(msg)

    async def close(self):
        if self._client:
            await self._client.aclose()

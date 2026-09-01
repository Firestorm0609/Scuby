"""Console alerts — colored terminal output for the agent."""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class ConsoleAlerts:
    """Handles console output for the agent."""

    # ANSI colors
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    PURPLE = "\033[95m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"

    def _ts(self) -> str:
        """Timestamp string."""
        return datetime.utcnow().strftime("%H:%M:%S")

    def banner(self, name: str, balance: float):
        """Print startup banner."""
        print(f"""
{self.CYAN}{self.BOLD}{'═' * 50}
  🤖 {name} Trading Agent v1.0
  💰 Balance: {balance:.4f} SOL
  🧠 Brain: Local LLM (Ollama)
  ⏰ Starting...
{'═' * 50}{self.RESET}
""")

    def scan(self, tokens_found: int):
        """Print scan results."""
        print(
            f"{self.DIM}[{self._ts()}]{self.RESET} "
            f"🔍 Scanning... found {tokens_found} new tokens"
        )

    def analyzing(self, token_name: str):
        """Print analysis start."""
        print(
            f"{self.DIM}[{self._ts()}]{self.RESET} "
            f"🧠 Analyzing {self.CYAN}{token_name}{self.RESET}..."
        )

    def decision(self, action: str, token_name: str, amount: float = 0,
                 confidence: float = 0, reasoning: str = ""):
        """Print trading decision."""
        if action == "buy":
            emoji = "🟢"
            color = self.GREEN
        elif action == "sell":
            emoji = "🔴"
            color = self.RED
        else:
            emoji = "⏭️"
            color = self.YELLOW

        print(
            f"{self.DIM}[{self._ts()}]{self.RESET} "
            f"{emoji} {color}{action.upper()}{self.RESET} "
            f"{token_name} "
            f"({confidence:.0%} confidence)"
        )
        if amount > 0:
            print(
                f"    {self.DIM}Amount: {amount:.4f} SOL{self.RESET}"
            )
        if reasoning:
            print(
                f"    {self.DIM}Reason: {reasoning}{self.RESET}"
            )

    def trade_confirmed(self, tx_hash: str, action: str, token_name: str):
        """Print trade confirmation."""
        color = self.GREEN if action == "buy" else self.RED
        print(
            f"{self.DIM}[{self._ts()}]{self.RESET} "
            f"{color}✅ {action.upper()} confirmed{self.RESET} "
            f"{token_name}"
        )
        if tx_hash:
            print(
                f"    {self.DIM}TX: {tx_hash[:16]}...{self.RESET}"
            )

    def trade_failed(self, token_name: str, reason: str):
        """Print trade failure."""
        print(
            f"{self.DIM}[{self._ts()}]{self.RESET} "
            f"{self.RED}❌ Trade failed{self.RESET} {token_name}: {reason}"
        )

    def risk_blocked(self, token_name: str, reason: str):
        """Print risk block."""
        print(
            f"{self.DIM}[{self._ts()}]{self.RESET} "
            f"{self.YELLOW}🛡️ Risk blocked{self.RESET} {token_name}: {reason}"
        )

    def position_update(self, token_name: str, pnl_pct: float, pnl_sol: float):
        """Print position PnL update."""
        color = self.GREEN if pnl_pct >= 0 else self.RED
        sign = "+" if pnl_pct >= 0 else ""
        print(
            f"{self.DIM}[{self._ts()}]{self.RESET} "
            f"📈 {color}{sign}{pnl_pct:.1%}{self.RESET} "
            f"({pnl_sol:+.6f} SOL) — {token_name}"
        )

    def portfolio_summary(self, balance: float, total_pnl: float,
                          open_positions: int, win_rate: float):
        """Print portfolio summary."""
        pnl_color = self.GREEN if total_pnl >= 0 else self.RED
        sign = "+" if total_pnl >= 0 else ""
        print(f"""
{self.BOLD}{'─' * 50}
  💰 Balance: {balance:.4f} SOL
  📊 Total PnL: {pnl_color}{sign}{total_pnl:.4f} SOL{self.RESET}
  📈 Win Rate: {win_rate:.1%}
  🏦 Open Positions: {open_positions}
{'─' * 50}{self.RESET}
""")

    def error(self, message: str):
        """Print error."""
        print(
            f"{self.DIM}[{self._ts()}]{self.RESET} "
            f"{self.RED}⚠️  ERROR: {message}{self.RESET}"
        )

    def info(self, message: str):
        """Print info message."""
        print(
            f"{self.DIM}[{self._ts()}]{self.RESET} "
            f"{self.BLUE}ℹ️  {message}{self.RESET}"
        )

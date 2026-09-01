"""The main agent loop — observe, think, act, repeat.

This is the heart of the agent. It runs continuously,
scanning for tokens, analyzing them, and executing trades.
"""

import asyncio
import json
import logging
from datetime import datetime, date

from ..data import PumpFunClient, DexScreenerClient, SolscanClient, TokenProfileBuilder
from ..brain import LLMClient, DecisionEngine
from ..risk import RiskManager
from ..trading import TradeExecutor
from ..memory import Database, TradeStore, Blacklist, AgentState
from ..alerts import ConsoleAlerts, TelegramAlerts

logger = logging.getLogger(__name__)


class AgentLoop:
    """The main agent loop."""

    def __init__(self, config: dict):
        self.config = config
        self.running = False

        # ── Initialize components ──
        
        # Data
        self.pumpfun = PumpFunClient()
        self.dexscreener = DexScreenerClient()
        self.solscan = SolscanClient()
        self.profiler = TokenProfileBuilder(self.pumpfun, self.dexscreener, self.solscan)

        # Brain
        llm_cfg = config.get("llm", {})
        self.llm = LLMClient(
            base_url=llm_cfg.get("base_url", "http://localhost:11434"),
            model=llm_cfg.get("model", "llama3"),
            temperature=llm_cfg.get("temperature", 0.3),
            max_tokens=llm_cfg.get("max_tokens", 500),
            timeout=llm_cfg.get("timeout", 30),
        )
        self.decision = DecisionEngine(self.llm)

        # Risk
        self.risk = RiskManager(config)

        # Trading
        self.executor = TradeExecutor(config)

        # Memory
        self.db = Database()
        self.trades = TradeStore(self.db)
        self.blacklist = Blacklist(self.db)
        self.state = AgentState(self.db)
        self.state.init_defaults(
            starting_balance=config.get("trading", {}).get("starting_balance", 0.5)
        )

        # Alerts
        self.console = ConsoleAlerts()
        telegram_cfg = config.get("alerts", {}).get("telegram", {})
        self.telegram = TelegramAlerts(
            bot_token=telegram_cfg.get("bot_token", ""),
            chat_id=telegram_cfg.get("chat_id", ""),
            enabled=telegram_cfg.get("enabled", False),
        )

        # Timing
        self.scan_interval = config.get("scan", {}).get("interval_seconds", 60)
        self.last_summary_time = datetime.utcnow()

    async def start(self):
        """Start the agent loop."""
        self.running = True

        # Health checks
        self.console.info("Running health checks...")
        
        # Check Ollama
        llm_ok = await self.llm.health_check()
        if not llm_ok:
            self.console.error(
                "Ollama is not running or model not available. "
                "Start Ollama: ollama serve"
            )
            return

        # Get initial balance
        try:
            balance = await self.executor.get_balance()
        except Exception:
            balance = self.state.get("balance", 0.5)
        
        self.state.set("balance", balance)

        # Print banner
        agent_name = self.config.get("agent", {}).get("name", "Agent")
        self.console.banner(agent_name, balance)
        self.console.info(f"Scanning every {self.scan_interval}s")
        self.console.info("Press Ctrl+C to stop\n")

        # Main loop
        try:
            while self.running:
                await self._tick()
                await asyncio.sleep(self.scan_interval)
        except KeyboardInterrupt:
            self.console.info("\nShutting down...")
        finally:
            await self._cleanup()

    async def _tick(self):
        """One iteration of the agent loop."""
        try:
            # Reset daily counters if needed
            self._check_daily_reset()

            # ── 1. OBSERVE: Scan for new tokens ──
            new_tokens = await self.pumpfun.get_new_tokens(limit=20)
            self.console.scan(len(new_tokens))

            # ── 2. Check existing positions ──
            await self._check_positions()

            # ── 3. FILTER: Skip analyzed/blacklisted tokens ──
            candidates = []
            for token in new_tokens:
                addr = token.get("address", "")
                if self.blacklist.is_blacklisted(addr):
                    continue
                if self.trades.was_analyzed_recently(addr, minutes=30):
                    continue
                if self.trades.get_open_position(addr):
                    continue
                candidates.append(token)

            # ── 4. THINK + ACT: Analyze top candidates ──
            for token in candidates[:3]:  # Max 3 per cycle
                await self._analyze_and_maybe_buy(token)

            # ── 5. PERIODIC: Portfolio summary ──
            if self._should_print_summary():
                await self._print_summary()

        except Exception as e:
            logger.error(f"Tick error: {e}", exc_info=True)
            self.console.error(f"Tick error: {e}")

    async def _analyze_and_maybe_buy(self, token: dict):
        """Analyze a token and potentially buy it."""
        token_addr = token.get("address", "")
        token_name = token.get("name", "Unknown")

        self.console.analyzing(token_name)

        # Build enriched profile
        profile = await self.profiler.build_profile(token_addr, pumpfun_data=token)

        # Quick hard-rule filter before sending to LLM
        holder_count = profile.get("holder_data", {}).get("total_holders", 0)
        liquidity = profile.get("market_data", {}).get("liquidity_usd", 0)

        if holder_count < self.risk.min_holders:
            self.console.risk_blocked(token_name, f"Only {holder_count} holders")
            self.trades.record_analysis(
                token_addr, token_name, json.dumps(profile, default=str), "skip_low_holders"
            )
            return

        if liquidity < self.risk.min_liquidity_usd:
            self.console.risk_blocked(token_name, f"Low liquidity: ${liquidity:,.0f}")
            self.trades.record_analysis(
                token_addr, token_name, json.dumps(profile, default=str), "skip_low_liquidity"
            )
            return

        # Ask the LLM
        balance = self.state.get("balance", 0.5)
        max_position = balance * self.risk.max_position_pct
        open_positions = self.trades.get_open_positions()
        recent_trades = self.trades.get_recent_trades(5)
        win_rate = self.trades.get_win_rate()
        daily_pnl = self.trades.get_daily_pnl()

        decision = await self.decision.decide_buy(
            token_profile=profile,
            balance=balance,
            max_position=max_position,
            open_positions=open_positions,
            recent_trades=recent_trades,
            win_rate=win_rate,
            daily_pnl=daily_pnl,
        )

        if decision["action"] == "hold":
            self.console.decision(
                "hold", token_name,
                confidence=decision.get("confidence", 0),
                reasoning=decision.get("reasoning", ""),
            )
            self.trades.record_analysis(
                token_addr, token_name, json.dumps(profile, default=str), "hold"
            )
            return

        # ── Validate with risk manager ──
        amount = decision.get("amount_sol", 0)
        allowed, reason = self.risk.validate_buy(
            token_profile=profile,
            amount_sol=amount,
            balance=balance,
            open_positions_count=len(open_positions),
            daily_trades=self.state.get("daily_trades", 0),
            daily_loss=self.state.get("daily_loss", 0),
        )

        if not allowed:
            self.console.risk_blocked(token_name, reason)
            self.trades.record_analysis(
                token_addr, token_name, json.dumps(profile, default=str), f"risk_blocked: {reason}"
            )
            return

        # ── Execute the trade ──
        self.console.decision(
            "buy", token_name, amount,
            confidence=decision.get("confidence", 0),
            reasoning=decision.get("reasoning", ""),
        )

        result = await self.executor.buy_with_jupiter(
            token_address=token_addr,
            amount_sol=amount,
            slippage_pct=self.risk.max_slippage_pct,
        )

        if result and result.get("success"):
            self.console.trade_confirmed(
                result.get("tx_hash", ""), "buy", token_name
            )
            self.trades.record_buy(
                token_addr, token_name, amount,
                profile.get("market_data", {}).get("price_usd", 0),
                result.get("tx_hash", ""),
                decision.get("reasoning", ""),
            )
            self.state.set("balance", balance - amount)
            self.state.set("daily_trades", self.state.get("daily_trades", 0) + 1)

            # Telegram alert
            await self.telegram.buy_alert(
                token_name, token_addr, amount,
                decision.get("confidence", 0),
                decision.get("reasoning", ""),
            )
        else:
            error_msg = result.get("error", "Unknown error") if result else "No result"
            self.console.trade_failed(token_name, error_msg)

    async def _check_positions(self):
        """Check open positions for stop loss / take profit."""
        positions = self.trades.get_open_positions()
        if not positions:
            return

        for pos in positions:
            token_addr = pos.get("token_addr", "")
            token_name = pos.get("token_name", "")

            # Get current price
            dex_data = await self.dexscreener.get_token_price(token_addr)
            if not dex_data:
                continue

            current_price = dex_data.get("price_usd", 0)
            buy_price = pos.get("price_usd", 0)

            if buy_price <= 0:
                continue

            pnl_pct = (current_price - buy_price) / buy_price

            # Print position update
            self.console.position_update(token_name, pnl_pct, 0)

            # Check risk rules for auto-sell
            sell_actions = self.risk.check_positions(
                [pos], {token_addr: current_price}
            )

            for action in sell_actions:
                await self._execute_sell(action, dex_data)

    async def _execute_sell(self, sell_action: dict, dex_data: dict):
        """Execute a sell (stop loss or take profit)."""
        token_addr = sell_action.get("token_addr", "")
        token_name = sell_action.get("token_name", "")
        reason = sell_action.get("reason", "")

        self.console.decision("sell", token_name, reasoning=f"Auto-sell: {reason}")

        # Execute
        result = await self.executor.sell_with_jupiter(
            token_address=token_addr,
            token_amount=0,  # TODO: get actual token amount
            slippage_pct=self.risk.max_slippage_pct,
        )

        if result and result.get("success"):
            pnl_sol = sell_action.get("pnl_sol", 0)
            pnl_pct = sell_action.get("pnl_pct", 0)

            self.console.trade_confirmed(
                result.get("tx_hash", ""), "sell", token_name
            )

            self.trades.record_sell(
                token_addr, 0,  # TODO: amount
                dex_data.get("price_usd", 0),
                result.get("tx_hash", ""),
                pnl_sol, pnl_pct, reason,
            )

            # Update balance
            balance = self.state.get("balance", 0.5)
            self.state.set("balance", balance + pnl_sol)
            self.state.set("daily_loss", 
                self.state.get("daily_loss", 0) + min(pnl_sol, 0)
            )

            await self.telegram.sell_alert(
                token_name, token_addr, pnl_pct, pnl_sol, reason
            )

    def _check_daily_reset(self):
        """Reset daily counters if it's a new day."""
        today = date.today().isoformat()
        if self.state.get("daily_reset_date") != today:
            self.state.set("daily_reset_date", today)
            self.state.set("daily_trades", 0)
            self.state.set("daily_loss", 0.0)
            logger.info("Daily counters reset")

    def _should_print_summary(self) -> bool:
        """Check if it's time for a portfolio summary."""
        elapsed = (datetime.utcnow() - self.last_summary_time).total_seconds()
        return elapsed >= 300  # Every 5 minutes

    async def _print_summary(self):
        """Print portfolio summary."""
        balance = self.state.get("balance", 0.5)
        total_pnl = self.trades.get_total_pnl()
        open_positions = self.trades.get_open_positions()
        win_rate = self.trades.get_win_rate()

        self.console.portfolio_summary(
            balance, total_pnl, len(open_positions), win_rate
        )

        self.last_summary_time = datetime.utcnow()

        # Telegram summary every hour
        elapsed = (datetime.utcnow() - self.last_summary_time).total_seconds()
        if elapsed >= 3600:
            await self.telegram.summary(
                balance, total_pnl, len(open_positions), win_rate
            )

    async def _cleanup(self):
        """Clean up resources."""
        self.console.info("Cleaning up...")
        await self.pumpfun.close()
        await self.dexscreener.close()
        await self.solscan.close()
        await self.llm.close()
        await self.executor.close()
        await self.telegram.close()
        self.console.info("Goodbye! 🤖")

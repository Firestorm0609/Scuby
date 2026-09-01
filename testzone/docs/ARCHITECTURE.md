# 🤖 AI Memecoin Trading Agent — Architecture

## Overview

An autonomous AI agent that scans the Solana blockchain (pump.fun), analyzes new tokens using a local LLM, and executes trades automatically — all running at near-zero cost.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      AGENT PROCESS                              │
│                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐  │
│  │  Scheduler  │───▶│  Agent Loop │───▶│  Decision Engine    │  │
│  │  (Cron/Tick)│    │  (Core)     │    │  (LLM + Rules)      │  │
│  └─────────────┘    └──────┬──────┘    └─────────┬───────────┘  │
│                            │                     │              │
│                            ▼                     ▼              │
│                     ┌─────────────┐    ┌─────────────────────┐  │
│                     │   Memory    │    │  Execution Layer    │  │
│                     │  (SQLite)   │    │  (Solana + Jupiter) │  │
│                     └─────────────┘    └─────────┬───────────┘  │
│                                                  │              │
└──────────────────────────────────────────────────┼──────────────┘
                                                   │
                                                   ▼
                                          ┌─────────────────┐
                                          │   Solana Chain   │
                                          │   (pump.fun +    │
                                          │    Jupiter DEX)  │
                                          └─────────────────┘
```

---

## Components

### 1. Scheduler (The Clock)

Controls when the agent runs. Simple tick-based system.

```
┌──────────────────────────────┐
│         SCHEDULER            │
├──────────────────────────────┤
│                              │
│  Task          Frequency     │
│  ──────────────────────────  │
│  Scan tokens   Every 60s     │
│  Check PnL     Every 30s     │
│  Risk check    Every 10s     │
│  Health check  Every 5min    │
│  Report        Every 1hr     │
│                              │
└──────────────────────────────┘
```

```python
# scheduler.py
SCHEDULE = {
    "scan_tokens":    60,   # seconds
    "check_pnl":      30,
    "risk_check":     10,
    "health_check":   300,
    "report":         3600,
}
```

### 2. Agent Loop (The Core)

The main loop that ties everything together.

```
┌──────────────────────────────────────────────────────┐
│                   AGENT LOOP                         │
│                                                      │
│  ┌──────────┐                                        │
│  │  START   │                                        │
│  └────┬─────┘                                        │
│       │                                              │
│       ▼                                              │
│  ┌──────────────────┐                                │
│  │ OBSERVE:          │                               │
│  │ • New tokens      │                               │
│  │ • Current prices  │                               │
│  │ • Wallet balance  │                               │
│  │ • Open positions  │                               │
│  └────────┬─────────┘                                │
│           │                                          │
│           ▼                                          │
│  ┌──────────────────┐                                │
│  │ FILTER:           │                               │
│  │ • Skip if blacklisted                             │
│  │ • Skip if < min holders                           │
│  │ • Skip if dev dumped                              │
│  │ • Skip if already analyzed                        │
│  └────────┬─────────┘                                │
│           │                                          │
│           ▼                                          │
│  ┌──────────────────┐                                │
│  │ THINK: (LLM)     │                               │
│  │ "Should I buy?"  │                               │
│  │ Returns: decision + reasoning                     │
│  └────────┬─────────┘                                │
│           │                                          │
│           ▼                                          │
│  ┌──────────────────┐                                │
│  │ VALIDATE:         │                               │
│  │ • Risk rules pass?│                               │
│  │ • Position limits│                               │
│  │ • Balance ok?     │                               │
│  └────────┬─────────┘                                │
│           │                                          │
│           ▼                                          │
│  ┌──────────────────┐                                │
│  │ ACT:              │                               │
│  │ • Build tx        │                               │
│  │ • Sign & send     │                               │
│  │ • Confirm tx      │                               │
│  └────────┬─────────┘                                │
│           │                                          │
│           ▼                                          │
│  ┌──────────────────┐                                │
│  │ REMEMBER:         │                               │
│  │ • Log decision    │                               │
│  │ • Log outcome     │                               │
│  │ • Update memory   │                               │
│  └────────┬─────────┘                                │
│           │                                          │
│           └──────► LOOP                              │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### 3. Data Layer (The Senses)

How the agent sees the world.

```
┌─────────────────────────────────────────────────────┐
│                  DATA SOURCES                       │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Source              What We Get         Cost       │
│  ─────────────────────────────────────────────────  │
│  pump.fun API        New token launches   Free      │
│  Solscan API         Wallet history       Free      │
│  DexScreener API     Token prices         Free      │
│  Birdeye API         Market data          Free tier │
│  Solana RPC          On-chain state       Free      │
│  Jupiter API         Swap quotes          Free      │
│                                                     │
└─────────────────────────────────────────────────────┘
```

```
┌──────────────────────────────────────────────┐
│              DATA PIPELINE                    │
│                                              │
│  pump.fun ──▶ New tokens ──▶ Token Analyzer  │
│                                    │         │
│  DexScreener ──▶ Prices ──────────┤         │
│                                    │         │
│  Solscan ──▶ Wallet data ─────────┤         │
│                                    ▼         │
│                              Token Profile   │
│                              (enriched data) │
└──────────────────────────────────────────────┘
```

### 4. Token Profile (The Enriched Data)

Every token gets analyzed into a profile before the LLM sees it.

```json
{
  "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  "name": "PEPE2",
  "symbol": "PEPE2",
  "created_at": "2026-08-26T12:00:00Z",
  
  "market_data": {
    "market_cap_usd": 12500,
    "price_usd": 0.0000125,
    "volume_24h": 8500,
    "liquidity": 3200,
    "price_change_5m": 45.2,
    "price_change_1h": 120.5
  },
  
  "holder_data": {
    "total_holders": 67,
    "top10_concentration": 0.42,
    "dev_wallet_holding": 0.08,
    "new_holders_last_5m": 12
  },
  
  "dev_data": {
    "wallet_address": "7xKX...",
    "previous_tokens_created": 3,
    "previous_token_avg_return": -0.45,
    "wallet_age_days": 2,
    "current_sol_balance": 1.2
  },
  
  "social_data": {
    "twitter_mentions": 5,
    "telegram_group_size": 0,
    "has_website": false
  },
  
  "risk_flags": [
    "low_holder_count",
    "dev_created_multiple_tokens",
    "dev_previous_tokens_failed"
  ]
}
```

### 5. Decision Engine (The Brain)

Two-layer system: **LLM + Hard Rules**

```
┌──────────────────────────────────────────────────────┐
│              DECISION ENGINE                          │
├──────────────────────────────────────────────────────┤
│                                                      │
│  Layer 1: HARD RULES (code, not LLM)                │
│  ─────────────────────────────────────              │
│  • Never buy if < 10 holders                        │
│  • Never buy if dev holds > 20%                     │
│  • Never buy if liquidity < $1000                   │
│  • Never buy if we already hold this token          │
│  • Never spend more than 5% of balance on 1 trade   │
│  • Max 3 open positions at once                     │
│  • Never trade if total balance < 0.1 SOL           │
│                                                      │
│  If hard rules PASS ▼                                │
│                                                      │
│  Layer 2: LLM JUDGMENT (the "thinking")             │
│  ─────────────────────────────────────              │
│  "Here's the token profile. Should I buy?"          │
│  LLM considers:                                     │
│  • Is the name/meme likely to trend?                │
│  • Does the dev pattern look trustworthy?           │
│  • Is the risk/reward worth it?                     │
│  • What position size makes sense?                  │
│                                                      │
│  Returns: { action, amount, confidence, reasoning } │
│                                                      │
│  Layer 3: POST-VALIDATION                           │
│  ─────────────────────────────────────              │
│  • Re-check risk rules with the specific amount     │
│  • Verify wallet has enough SOL                     │
│  • Check slippage is acceptable                     │
│                                                      │
└──────────────────────────────────────────────────────┘
```

```
┌──────────────────────────────────────────┐
│          LLM PROMPT TEMPLATE             │
├──────────────────────────────────────────┤
│                                          │
│  You are a Solana memecoin trader.       │
│  Your goal: maximize profit while        │
│  managing risk. You have 0.5 SOL.        │
│                                          │
│  TOKEN ANALYSIS:                         │
│  {token_profile_json}                    │
│                                          │
│  YOUR RECENT HISTORY:                    │
│  {last_10_trades}                        │
│                                          │
│  Your current positions:                 │
│  {open_positions}                        │
│                                          │
│  RULES:                                  │
│  - Max 5% per trade                      │
│  - Only buy if confidence > 60%          │
│  - Better to miss a trade than lose SOL  │
│                                          │
│  Respond in JSON:                        │
│  {                                       │
│    "action": "buy" | "sell" | "hold",    │
│    "token": "address or null",           │
│    "amount_sol": 0.02,                   │
│    "confidence": 0.75,                   │
│    "reasoning": "why"                    │
│  }                                       │
│                                          │
└──────────────────────────────────────────┘
```

### 6. Memory System (The Brain's Storage)

```
┌─────────────────────────────────────────────────────┐
│                   MEMORY (SQLite)                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Table: trades                                      │
│  ─────────────                                      │
│  id, token_addr, action, amount_sol, price,        │
│  timestamp, llm_reasoning, pnl_sol, pnl_pct,       │
│  status (open/closed), close_reason                 │
│                                                     │
│  Table: tokens_analyzed                             │
│  ────────────────────                               │
│  id, token_addr, name, profile_json,                │
│  first_seen, last_seen, analyzed_at, decision       │
│                                                     │
│  Table: blacklist                                   │
│  ───────────────                                    │
│  token_addr, reason, added_at                       │
│                                                     │
│  Table: agent_state                                 │
│  ─────────────────                                  │
│  key, value                                         │
│  (balance, total_pnl, total_trades, win_rate)       │
│                                                     │
└─────────────────────────────────────────────────────┘
```

### 7. Risk Management Layer (The Safety Net)

**This runs OUTSIDE the LLM.** The LLM can be wrong. This layer cannot be overridden.

```python
class RiskManager:
    """Hard rules that override LLM decisions."""
    
    MAX_POSITION_SIZE_PCT = 0.05    # 5% of balance per trade
    MAX_OPEN_POSITIONS = 3
    MIN_BALANCE_SOL = 0.1
    STOP_LOSS_PCT = -0.30           # -30%
    TAKE_PROFIT_PCT = 0.50          # +50%
    MIN_HOLDERS = 10
    MAX_DEV_HOLDING_PCT = 0.20
    MIN_LIQUIDITY_USD = 1000
    MAX_DAILY_TRADES = 20
    MAX_DAILY_LOSS_SOL = 0.1
    
    def validate_buy(self, token_profile, amount_sol, state):
        """Returns (allowed: bool, reason: str)"""
        
        # Position size check
        if amount_sol > state.balance * self.MAX_POSITION_SIZE_PCT:
            return False, "Exceeds max position size"
        
        # Open positions check
        if state.open_positions >= self.MAX_OPEN_POSITIONS:
            return False, "Max open positions reached"
        
        # Balance check
        if state.balance - amount_sol < self.MIN_BALANCE_SOL:
            return False, "Balance too low after trade"
        
        # Token safety checks
        if token_profile["holder_data"]["total_holders"] < self.MIN_HOLDERS:
            return False, "Too few holders"
        
        if token_profile["dev_data"]["dev_wallet_holding"] > self.MAX_DEV_HOLDING_PCT:
            return False, "Dev holds too much"
        
        if token_profile["market_data"]["liquidity"] < self.MIN_LIQUIDITY_USD:
            return False, "Liquidity too low"
        
        # Daily limits
        if state.daily_trades >= self.MAX_DAILY_TRADES:
            return False, "Daily trade limit reached"
        
        if state.daily_loss < -self.MAX_DAILY_LOSS_SOL:
            return False, "Daily loss limit reached"
        
        return True, "OK"
    
    def check_positions(self, positions):
        """Check if any position needs to be closed."""
        actions = []
        for pos in positions:
            if pos.pnl_pct <= self.STOP_LOSS_PCT:
                actions.append(("sell", pos, "stop_loss"))
            elif pos.pnl_pct >= self.TAKE_PROFIT_PCT:
                actions.append(("sell", pos, "take_profit"))
        return actions
```

### 8. Execution Layer (The Hands)

How trades actually happen on Solana.

```
┌──────────────────────────────────────────────────────┐
│               EXECUTION FLOW                          │
├──────────────────────────────────────────────────────┤
│                                                      │
│  1. Get quote from Jupiter API                       │
│     (finds best route for swap)                      │
│                                                      │
│  2. Build transaction                                │
│     (Solana web3.js or Anchor)                       │
│                                                      │
│  3. Sign with wallet keypair                          │
│                                                      │
│  4. Add priority fee (for speed)                     │
│                                                      │
│  5. Send to Solana RPC                               │
│                                                      │
│  6. Confirm transaction                              │
│                                                      │
│  7. Record in memory                                 │
│                                                      │
└──────────────────────────────────────────────────────┘
```

```
┌──────────────────────────────────────────────────────┐
│             BUY FLOW (pump.fun)                      │
├──────────────────────────────────────────────────────┤
│                                                      │
│  User wants to buy 0.02 SOL of PEPE2                 │
│                                                      │
│  1. Pump.fun bonding curve has its own DEX            │
│     → Use pump.fun's buy instruction directly        │
│                                                      │
│  2. If token has graduated to Raydium:               │
│     → Use Jupiter aggregator for best route          │
│                                                      │
│  3. Check slippage (max 5%)                          │
│                                                      │
│  4. Sign + send                                      │
│                                                      │
│  5. Wait for confirmation (400ms on Solana)          │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### 9. Monitoring & Alerts

```
┌──────────────────────────────────────────────────────┐
│               MONITORING                             │
├──────────────────────────────────────────────────────┤
│                                                      │
│  Console Output (real-time):                         │
│  ├── Every scan: what tokens found                   │
│  ├── Every decision: action + reasoning              │
│  ├── Every trade: confirmation + tx hash             │
│  └── Every 5min: portfolio summary                   │
│                                                      │
│  Telegram Bot (optional):                            │
│  ├── 🟢 Buy alert: token, amount, reasoning          │
│  ├── 🔴 Sell alert: token, PnL, reason               │
│  ├── ⚠️ Risk alert: stop loss triggered              │
│  └── 📊 Hourly summary: balance, positions, PnL     │
│                                                      │
│  Log File:                                           │
│  └── Full history for debugging                      │
│                                                      │
└──────────────────────────────────────────────────────┘
```

### 10. Configuration

```yaml
# config.yaml

agent:
  name: "Buffy"
  version: "1.0.0"
  
llm:
  provider: "ollama"           # local, free
  model: "llama3"              # or mistral, gemma
  temperature: 0.3             # low = more conservative
  max_tokens: 500
  
wallet:
  keypair_path: "./wallet.json"
  network: "mainnet-beta"      # or "devnet" for testing
  
trading:
  starting_balance: 0.5        # SOL
  max_position_pct: 0.05       # 5% per trade
  max_open_positions: 3
  stop_loss_pct: -0.30
  take_profit_pct: 0.50
  
scan:
  interval_seconds: 60
  sources:
    - "pump.fun"
  
risk:
  min_holders: 10
  max_dev_holding_pct: 0.20
  min_liquidity_usd: 1000
  max_daily_trades: 20
  max_daily_loss_sol: 0.1
  blacklist: []
  
alerts:
  telegram:
    enabled: false
    bot_token: ""
    chat_id: ""
  
logging:
  level: "INFO"
  file: "./logs/agent.log"
```

---

## File Structure

```
agent/
├── config.yaml              # All settings
├── main.py                  # Entry point, starts the loop
│
├── core/
│   ├── __init__.py
│   ├── loop.py              # The main agent loop
│   ├── scheduler.py         # Task scheduling
│   └── state.py             # Agent state management
│
├── brain/
│   ├── __init__.py
│   ├── llm.py               # LLM interface (Ollama wrapper)
│   ├── prompts.py           # Prompt templates
│   └── decision.py          # Decision engine (LLM + rules)
│
├── data/
│   ├── __init__.py
│   ├── pumpfun.py           # pump.fun API client
│   ├── dexscreener.py       # DexScreener price data
│   ├── solscan.py           # Wallet/holder data
│   ├── jupiter.py           # Jupiter swap quotes
│   └── token_profile.py     # Build enriched token profiles
│
├── trading/
│   ├── __init__.py
│   ├── wallet.py            # Solana wallet management
│   ├── executor.py          # Build + send transactions
│   ├── buy.py               # Buy logic (pump.fun + Jupiter)
│   └── sell.py              # Sell logic
│
├── risk/
│   ├── __init__.py
│   ├── manager.py           # Risk rules engine
│   └── position.py          # Position tracking
│
├── memory/
│   ├── __init__.py
│   ├── database.py          # SQLite connection + schema
│   ├── trades.py            # Trade history queries
│   ├── blacklist.py         # Token blacklist
│   └── state.py             # Agent state persistence
│
├── alerts/
│   ├── __init__.py
│   ├── console.py           # Console output
│   └── telegram.py          # Telegram bot alerts
│
├── logs/                    # Log files
├── wallet.json              # Solana keypair (gitignored)
└── data.db                  # SQLite database (gitignored)
```

---

## Data Flow Diagram

```
pump.fun ──────┐
               │
DexScreener ───┤
               ├──▶ Token Profile ──▶ Hard Rules ──▶ LLM ──▶ Decision
Solscan ───────┤         │                              │        │
               │         │                              │        │
Jupiter ───────┘         │                              │        ▼
                         │                              │   Risk Check
                         │                              │        │
                         │                              │        ▼
                         │                              │   Execute Tx
                         │                              │        │
                         │                              │        ▼
                         │                              │   Log to DB
                         │                              │        │
                         │                              │        ▼
                         └──────────────────────────────┘   Alert User
```

---

## Startup Sequence

```
1. Load config.yaml
2. Initialize SQLite database
3. Load Solana wallet
4. Connect to Ollama (verify LLM is running)
5. Test Solana RPC connection
6. Load agent state from DB
7. Start the main loop
8. Begin scanning
```

---

## Key Design Decisions

| Decision | Choice | Why |
|---|---|---|
| Language | Python | Best LLM ecosystem, fastest to build |
| LLM | Ollama + Llama 3 | Free, local, no API costs |
| Database | SQLite | Zero config, file-based, enough for single agent |
| Blockchain | Solana | Fast, cheap txs, pump.fun is on Solana |
| Trading | pump.fun direct + Jupiter fallback | Best execution for both cases |
| Risk | Hardcoded rules, not LLM | Safety cannot be left to AI judgment |

---

## Estimated Costs

| Component | Monthly Cost |
|---|---|
| LLM (local Ollama) | $0 |
| Data APIs (free tiers) | $0 |
| Server (your own machine) | $0 |
| Solana RPC (public) | $0 |
| Solana tx fees (~100/day) | ~$3 |
| **Total** | **~$3/mo** |

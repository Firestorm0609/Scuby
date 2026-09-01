# 🏎️ DRIFT — AI Racing Betting

**First AI agent business on Robinhood Chain.**

🔗 **Live:** [driftrace.tech](https://driftrace.tech)

An autonomous AI agent operates a provably fair car racing betting platform. Users bet on AI-driven races every 5 minutes. Instant on-chain payouts.

## Quick Start

```bash
# Install
pip3 install -r requirements.txt

# Deploy contract to Robinhood Chain testnet
python3 deploy.py

# Start the AI agent
python3 agent.py

# Start dashboard
python3 server.py
# → http://localhost:8080
```

## Deploy to Vercel

```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
cd Drift
vercel --prod

# Set custom domain
vercel domains add driftrace.tech
```

Or connect the GitHub repo to Vercel for auto-deploys.

## Architecture

```
┌─────────────────────────────────────────┐
│           ROBINHOOD CHAIN               │
│                                         │
│  ┌────────────┐  ┌──────────────────┐   │
│  │  DRIFT     │  │  AI AGENT        │   │
│  │  CONTRACT  │  │  (Python)        │   │
│  │            │  │                  │   │
│  │  - Bets    │←→│  - Race cycle    │   │
│  │  - Pools   │  │  - Settlement    │   │
│  │  - Payouts │  │  - Provably fair │   │
│  └────────────┘  └──────────────────┘   │
└─────────────────────────────────────────┘
         ↕
┌─────────────────────────────────────────┐
│      driftrace.tech (Frontend)          │
│  MetaMask · Bet · View · Claim          │
└─────────────────────────────────────────┘
```

## Race Cycle

Every 5 minutes:
1. 🆕 New race created
2. 🔒 Agent commits hash (provably fair)
3. 💰 Betting opens (60 seconds)
4. ⏰ Betting closes
5. 🎲 Agent reveals seed → winner determined
6. 💸 95% to winner, 5% to protocol
7. 🔄 Repeat

## Files

| File | Purpose |
|------|---------|
| `contracts/DriftRace.sol` | Smart contract — betting + provably fair |
| `agent.py` | AI agent — runs race cycle on-chain |
| `deploy.py` | Deploys contract to testnet |
| `server.py` | Dashboard server + API |
| `index.html` | Frontend — connect wallet, bet, view races |
| `vercel.json` | Vercel deployment config |

## Revenue

| Metric | Value |
|--------|-------|
| Fee | 5% per race |
| Races/day | 288 |
| Min bet | $5 |
| Max bet | $1,000 |

## $DRIFT Token

Launching via PONS Launchpad on Robinhood Chain.

| Feature | Detail |
|---------|--------|
| Supply | 1,000,000,000 |
| Fee discount | 3% (vs 5% without) |
| Governance | Vote on race rules |
| Staking | Earn share of fees |
| NFTs | Special races, higher limits |

## Network

- **Chain:** Robinhood Chain (Arbitrum L2)
- **Chain ID:** 46630 (testnet) / 4663 (mainnet)
- **Explorer:** [robinhoodchain.blockscout.com](https://robinhoodchain.blockscout.com)

## License

MIT

---

*The future of business is autonomous. The future is $DRIFT.*

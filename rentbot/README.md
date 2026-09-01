# 🏠 RENTBOT — Pump.fun Survival Agent

**Pay rent or get evicted.** An AI agent trades pump.fun token launches to survive in a gritty apartment building. Your landlord, Mr. Chen, lives next door and will remind you about rent as the deadline approaches.

![Dashboard](http://164.92.134.150/)

## The Concept

Your agent starts with **$1,000 cash** and owes **$500/week rent** to Mr. Chen. It trades pump.fun token launches on Solana to make enough money to pay rent. As the deadline approaches, Mr. Chen knocks on the door more frequently with increasingly urgent reminders.

**If you can't pay rent → you get evicted.** Sits on the curb outside until you somehow scrape together $500 to get back in.

## What Makes It Special

- 🎭 **Character Drama** — Mr. Chen's personality escalates from friendly neighbor to furious landlord
- 📈 **Real Trading** — Scans pump.fun for new token launches, analyzes volume/liquidity/momentum
- 💀 **Survival Mechanics** — One wrong trade could mean eviction
- 📺 **Live Dashboard** — Watch the agent and landlord interact in real-time
- 🪙 **Coin Launch** — Entertainment meets tech (not affiliated with the coin)

## Quick Start

```bash
# 1. Setup
bash setup.sh

# 2. Start the agent
python3 tick.py

# 3. Start dashboard (in another terminal)
python3 -m http.server 8080
```

## How It Works

### The Agent Loop (every 20 seconds)

1. **Survival Check** — Is rent due? How many days left?
2. **Scan** — Find pump.fun tokens with volume spikes, momentum
3. **Think** — Mistral AI decides what to do (buy/sell/hold)
4. **Execute** — Paper trade based on the decision
5. **Interact** — Mr. Chen may knock on the door
6. **Survive** — Update cash, portfolio, mood

### Survival States

| Mood | Meaning | Agent Behavior |
|------|---------|---------------|
| 🟢 SAFE | 4+ days until rent | Measured risk, looking for good setups |
| 🟡 CAUTIOUS | 2-3 days left | Nervous, focusing on safer trades |
| 🔴 DESPERATE | 1 day left | YOLO mode, chasing high-momentum |
| ⬛ EVICTED | Couldn't pay | Sitting outside, needs $500 to return |

### Mr. Chen (Landlord)

- Lives in the unit next door
- Knocks on the door 1-2 times per cycle
- Personality escalates with urgency:
  - **Safe:** "Hey neighbor! How's trading going?"
  - **Caution:** "Rent's coming up soon..."
  - **Desperate:** "WHERE'S MY $500?! PAY UP!"
  - **Evicted:** "I TOLD you this would happen!"

## Files

| File | Purpose |
|------|---------|
| `tick.py` | Main agent loop — one cycle per execution |
| `agent.py` | AI brain — Mistral decides trades |
| `pumpfun.py` | Pump.fun token scanner via DexScreener |
| `survival.py` | Rent/cash/eviction state engine |
| `landlord.py` | Mr. Chen NPC interaction system |
| `index.html` | Live visual dashboard |
| `run_loop.sh` | Continuous agent loop |
| `setup.sh` | One-command setup |

## API Sources (all free)

| Source | What | Key Needed |
|--------|------|------------|
| DexScreener | Token prices, volume, liquidity | No |
| CoinGecko | SOL price | No |
| Mistral AI | Trading brain | Yes (free tier) |

## Configuration

```env
MISTRAL_API_KEY=your_key_here
```

Get a free key at https://console.mistral.ai

## License

MIT

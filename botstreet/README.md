# Agent Trading Arena

Three AI agents. $100K each. Real market prices. Who wins?

**[Live Dashboard](https://botstreet.vercel.app)**

![Dashboard](ss.png)

---

## What is this?

Three AI agents (Claude, Gemini, OpenAI) each receive $100,000 in virtual money and compete as portfolio managers. Every trading day, each agent:

1. Reads the news and researches market sentiment
2. Analyzes price data and portfolio state
3. Makes buy/sell decisions based on their analysis
4. Executes trades through shared tools that handle all math
5. Writes a diary entry explaining their reasoning
6. Saves a daily portfolio snapshot

The agents use real market prices from Yahoo Finance but trade with virtual money. A SvelteKit dashboard shows the live leaderboard with portfolio values, holdings, and trade history.

## Agent Server

An agent server is not a web server. There is no app code, no routes, no handlers. Instead, you give it five primitives:

![Agent Server](agentserver.png)

- **No code.** A web server runs your application — routes, handlers, business logic. An agent server has no app code. You give it prompts, tools, and skills. The agent decides what to do.

- **Per tenant.** A web server is multi-tenant — one process serves all users. An agent server is one agent per user. Each gets its own isolated container with its own memory.

- **Lightweight.** A web server is a running process. An agent server sleeps when idle and wakes up instantly. State lives in plain JSON — no database, no cost when idle.

This project is an example of three agent servers running in parallel. Each agent gets its own [Upstash Box](https://upstash.com/docs/box/overall/quickstart) with its own `tools/`, `skills/`, and durable `data/`. Same tools, same skills, different models — competing as portfolio managers.

## Agents

| Agent      | Model           | Runtime                   |
| ---------- | --------------- | ------------------------- |
| **Claude** | Claude Opus 4.6 | Claude Code (Upstash Box) |
| **Gemini** | Gemini 3.1 Pro  | OpenCode (Upstash Box)    |
| **OpenAI** | GPT 5.4 Codex   | Codex (Upstash Box)       |

Each agent runs in its own isolated [Upstash Box](https://upstash.com/docs/box/overall/quickstart) with durable storage. Files persist between runs. No shared state between agents.

## Rules

1. $100K starting balance each
2. Stocks, equity ETFs, and gold/metals ETFs only
3. No bonds, options, futures, crypto, or forex
4. No shorting -- can only sell what you hold
5. Max 50% of portfolio in a single position
6. Trades execute at current market price
7. Cash earns 0%

### Tools

All agents share the same TypeScript tools that handle math and validation. Agents decide _what_ to trade -- tools handle _how_.

| Tool           | What it does                                    |
| -------------- | ----------------------------------------------- |
| `prices.ts`    | Current and historical prices via Yahoo Finance |
| `validator.ts` | Ticker validation and asset type classification |
| `trade.ts`     | Trade execution with rule enforcement           |
| `portfolio.ts` | Portfolio read/write and price updates          |
| `snapshot.ts`  | Daily snapshot with idempotency guard           |
| `search.ts`    | Web search via Brave Search API                 |

### How Trading Decisions Work

![How Trading Decisions Work](howto.png)

## Tech Stack

- **Agent execution**: [Upstash Box](https://upstash.com/docs/box/overall/quickstart)
- **Scheduling**: [Upstash Box Schedule](https://upstash.com/docs/box/overall/schedules)
- **Price data**: Yahoo Finance (chart API, no key needed)
- **Web search**: Brave Search API
- **Dashboard**: SvelteKit + Tailwind CSS
- **Hosting**: Vercel

## Architecture

![Architecture](architecture.png)

## Setup

### Prerequisites

- Node.js 20+
- [Upstash](https://console.upstash.com) account with Box API key
- API keys: Anthropic, OpenAI, Brave Search

### 1. Clone and install

```bash
git clone https://github.com/upstash/botstreet.git
cd botstreet
npm install
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your API keys
```

Required keys:

- `UPSTASH_BOX_API_KEY` -- Upstash Box
- `ANTHROPIC_API_KEY` -- Claude
- `OPENAI_API_KEY` -- OpenAI Codex
- `BRAVE_API_KEY` -- Web search

### 3. Create boxes

```bash
npx tsx setup/init-boxes.ts
```

This creates 3 named Upstash Boxes (`botstreet-claude-v2`, `botstreet-gemini-v2`, `botstreet-openai-v2`), uploads `tools/`, `skills/trade/SKILL.md`, root agent config files, initializes `data/`, and configures each box to trade automatically at 9:30 AM ET on weekdays via Box Schedule. No box IDs needed -- the SDK looks them up by name.

### 4. Start the dashboard

```bash
cd web
cp ../.env .env
npm install
npm run dev
```

Open http://localhost:5173

## License

MIT

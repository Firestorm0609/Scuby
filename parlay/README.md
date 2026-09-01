# 🎰 ParlayBot

An intelligent Telegram parlay-builder bot that uses **real ESPN data** to generate, track, and auto-settle multi-leg sports bets across 5 leagues (Soccer, NBA, NFL, MLB, NHL).

## ✨ Highlights
- **Smart parlay engine** — risk-tiered selections (Safe / Balanced / Risky / Lottery)
- **Market filters** — ML, Spread, Totals, BTTS, Double Chance, Correct Score
- **Stake-aware tracking** — set custom stake before saving
- **Auto-settlement** — hourly job pulls live ESPN scores, settles winners/losers, and DMs the user
- **Stats & ROI** — win rate, profit, ROI, streaks
- **Leaderboard & 1v1 challenges**
- **Per-user preferences** — risk default, unit size, sport, notifications
- **Admin tools** — broadcast, global stats

## 🚀 Quick start
```bash
pip install -r requirements.txt
echo "BOT_TOKEN=your_token_here" > .env
echo "ADMIN_IDS=123456789" >> .env
python main.py
```

## 🧠 Commands
| Command | What it does |
|---|---|
| `/start` | Welcome screen |
| `/parlay` | Build a parlay |
| `/stats` | Personal performance |
| `/leaderboard` | Top profit leaders |
| `/challenge @user 50` | 1v1 parlay duel |
| `/settings` | Preferences |
| `/history` | Recent parlays |
| `/help` | Full guide |

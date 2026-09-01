# eFootball DNA Build Bot v2.0

Telegram bot that creates custom training builds for eFootball 2027 cards — builds that make opponents wonder "how is that card performing like that?"

## What Makes This Different

This bot doesn't just max overall rating. It uses **hidden game mechanics** that most players don't know about:

### 🔓 Stat Thresholds
Certain stat values unlock completely different animations:
- **86 Tight Possession** — faster 180° turns
- **86 Balance** — smoother movement transitions
- **90 Acceleration** — explosive first-step animation

### ⚡ Skill Synergies
Skills secretly boost stats by %:
- **Through Passing** → +20% to passing stats
- **Long-Range Curler** → +10% Finishing + Kicking Power (**bypasses 99 cap!**)
- **Double Touch** → recovers Balance for accurate follow-up shots ("DT Boom")
- **Super-sub** → +5% Attacking Awareness & Finishing when subbed on after HT

### 📐 Body Type Awareness
Height, weight, and limb proportions affect how builds feel:
- Tall players (188cm+) benefit from aerial builds
- Compact players (<172cm) benefit from dribbling builds
- Jump height formula: `Height + 54 + [(Jump - 40) × 0.6]`

### 🎯 Touch Frequency
The **real** dribbling feel stat — NOT the visible "Dribbling" stat:
- Low speed TF = Tight Possession + Player Model
- High speed TF = Dribbling + Speed + Player Model

## Setup

```bash
pip install -r requirements.txt
export TELEGRAM_BOT_TOKEN=your_token_here
python bot.py
```

## How to Use

1. Send `/start` → click **🔬 Search Player**
2. Type a player name → browse the photo carousel
3. Click **⚗️ Engineer This Card**
4. Choose a DNA module (9 categories)
5. Pick a specific upgrade
6. Get your build with:
   - Exact click counts per training category
   - Stat gains with threshold callouts (🔓)
   - Active skill synergies (⚡)
   - Body type advice (📐)
   - Jump height calculation

## DNA Categories

| Module | Description |
|--------|-------------|
| ⚡ Athletic Engine | Speed, acceleration, stamina, agility |
| 🎮 Ball Mastery | Dribbling, ball control, tight possession |
| 🎯 Finishing Lab | Shooting, curl, set pieces, heading |
| 🧠 Football IQ | Positioning, passing vision, tempo |
| 🚀 Playstyle Mutation | Role transformations (False 9, Inside Forward, Libero, etc.) |
| 🔥 Pressing & Intensity | Aggression, interception, tackling |
| 🪽 Wide Threat | Winger identities, crossing, cutting inside |
| 🛡️ Defensive Core | Ball winning, sweeping, aerial dominance |
| ⭐ Signature Builds | Pre-engineered legend archetypes (Prime Messi, Haaland, Kanté, etc.) |

## Files

- `knowledge_base.py` — Hidden mechanics: thresholds, synergies, body formulas, misconceptions
- `optimizer.py` — Threshold-aware, synergy-aware build optimizer
- `scraper.py` — efhub.com data fetching (RSC payload parser)
- `bot.py` — Telegram bot with inline keyboard interface
- `requirements.txt` — Python dependencies

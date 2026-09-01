#!/bin/bash
# RentBot — Setup Script
# Run: bash setup.sh

set -e

echo "╔════════════════════════════════════════╗"
echo "║   RENTBOT — Pump.fun Survival Agent    ║"
echo "║   Pay rent or get evicted              ║"
echo "╚════════════════════════════════════════╝"
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 not found. Install it first."
    exit 1
fi

# Install dependencies
echo "📦 Installing dependencies..."
pip3 install -q httpx openai python-dotenv 2>/dev/null || pip install -q httpx openai python-dotenv

# Create .env if not exists
if [ ! -f .env ]; then
    echo ""
    echo "🔑 API Key Setup"
    echo "─────────────────────────────────────"
    echo "You need a Mistral API key (free at https://console.mistral.ai)"
    echo ""
    read -p "Enter your Mistral API key: " MISTRAL_KEY
    
    if [ -z "$MISTRAL_KEY" ]; then
        echo "⚠️  No key provided. You can add it later to .env"
        echo "MISTRAL_API_KEY=your_key_here" > .env
    else
        echo "MISTRAL_API_KEY=$MISTRAL_KEY" > .env
        echo "✅ Key saved to .env"
    fi
else
    echo "✅ .env already exists"
fi

# Create data directory
mkdir -p data

# Create initial state
python3 -c "
import json
from datetime import datetime, timedelta

state = {
    'cash': 1000.0,
    'starting_cash': 1000.0,
    'holdings': {},
    'total_trades': 0,
    'wins': 0,
    'losses': 0,
    'total_pnl': 0.0,
    'rent_paid': True,
    'rent_due_date': (datetime.now() + timedelta(days=7)).isoformat(),
    'rents_paid': 0,
    'rents_owed': 0,
    'is_evicted': False,
    'mood': 'safe',
    'day': 1,
    'cycle': 0,
    'story_log': [{'time': datetime.now().strftime('%H:%M:%S'), 'day': 1, 'message': '🏠 Agent moved into the apartment! $1,000 in hand, $500/week rent. Time to trade pump.fun tokens.'}],
    'landlord_interactions': [{'time': datetime.now().strftime('%H:%M:%S'), 'day': 1, 'speaker': 'MR. CHEN', 'message': 'Welcome, neighbor! I\'m Mr. Chen, your landlord. Rent is \$500/week. Good luck with your trading!'}],
    'current_action': 'Moving into the apartment...',
    'thought': 'Just moved in. $1,000 cash. Need to trade pump.fun tokens to make rent money.',
    'agent_station': 'desk',
    'landlord_station': 'his_door',
    'last_rent_check': datetime.now().isoformat(),
    'last_trade': None,
    'last_landlord_chat': datetime.now().isoformat(),
    'start_time': datetime.now().isoformat()
}
with open('data/agent_state.json', 'w') as f:
    json.dump(state, f, indent=2)
print('✅ Initial state created — Agent moved in!')
"

# Test API key
echo ""
echo "🧪 Testing API connection..."
python3 -c "
from dotenv import load_dotenv
load_dotenv()
import os
from openai import OpenAI
key = os.getenv('MISTRAL_API_KEY', '')
if not key:
    print('⚠️  No API key found. Add MISTRAL_API_KEY to .env')
else:
    try:
        client = OpenAI(api_key=key, base_url='https://api.mistral.ai/v1', timeout=10)
        r = client.chat.completions.create(model='mistral-small-latest',
            messages=[{'role': 'user', 'content': 'Say OK'}], max_tokens=5)
        print('✅ API connection working')
    except Exception as e:
        print(f'⚠️  API error: {e}')
"

echo ""
echo "╔════════════════════════════════════════╗"
echo "║          Setup Complete!               ║"
echo "╚════════════════════════════════════════╝"
echo ""
echo "Start the agent:  python3 tick.py"
echo "Start dashboard:  python3 -m http.server 8080"
echo "Or use the loop:  bash run_loop.sh"
echo ""
echo "🏠 Your agent has \$1,000 and owes \$500/week rent."
echo "   Trade pump.fun tokens to survive!"

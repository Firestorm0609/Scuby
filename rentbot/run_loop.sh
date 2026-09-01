#!/bin/bash
# RentBot — Continuous agent loop
# Runs one cycle every 20 seconds
cd "$(dirname "$0")"

echo "🏠 RENTBOT — Starting continuous loop"
echo "   Dashboard: python3 -m http.server 8080"
echo ""

# Check for .env
if [ ! -f .env ]; then
    echo "❌ No .env file. Run setup first: bash setup.sh"
    exit 1
fi

WEB_DIR="/var/www/hoodstreet"

while true; do
    echo "[$(date '+%H:%M:%S')] Running cycle..."
    python3 tick.py 2>&1 | tail -20
    # Push state + dashboard to web server
    cp data/agent_state.json "$WEB_DIR/agent_state.json" 2>/dev/null
    cp index.html "$WEB_DIR/rentbot.html" 2>/dev/null
    # Also update trade history
    cp data/trade_history.json "$WEB_DIR/trade_history.json" 2>/dev/null
    echo "---"
    sleep 5
done

#!/bin/bash
# Zombie Survival — Continuous Loop
# Runs game tick every 15 seconds, deploys state to web server

cd "$(dirname "$0")"
WEB="/var/www/hoodstreet"

echo "🧟 ZOMBIE SURVIVAL LOOP STARTED"

while true; do
    python3 tick.py 2>&1 | tail -8
    # Deploy state to web server
    cp data/survival.json "$WEB/zombiesurvival_state.json" 2>/dev/null || true
    cp data/web_state.json "$WEB/zombiesurvival_state.json" 2>/dev/null || true
    echo "---"
    sleep 15
done

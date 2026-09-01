#!/bin/bash
# RentBot continuous loop
cd /root/rentbot
while true; do
    python3 tick.py >> /tmp/rentbot.log 2>&1
    cp data/agent_state.json /var/www/hoodstreet/agent_state.json 2>/dev/null
    sleep 18
done

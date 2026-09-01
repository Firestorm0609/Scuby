#!/bin/bash
cd /root/rentbot
while true; do
    python3 tick.py 2>/dev/null
    sleep 18
done

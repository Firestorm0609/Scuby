#!/bin/bash
# Zombie Survival — Setup
set -e
cd "$(dirname "$0")"

echo "🧟 ZOMBIE SURVIVAL — Setup"
echo "=========================="

# Reset state
python3 -c "from survival import reset; s=reset(); print(f'State reset: \${s[\"cash\"]} cash, {s[\"health\"]} HP')"

# Copy .env from parent
cp ../.env .env 2>/dev/null || echo "No .env found"

# Install deps
pip install httpx python-dotenv -q 2>/dev/null || true

echo ""
echo "✅ Setup complete!"
echo "Run: python3 tick.py          (one cycle)"
echo "Run: bash loop.sh             (continuous)"
echo "Run: python3 -m http.server 8080  (dashboard)"

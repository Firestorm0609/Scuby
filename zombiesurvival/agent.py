"""
Agent Survival Brain — Mistral decides what to do each cycle.
Balances trading for money, buying supplies, fighting zombies, and survival.
"""
import json, os, random, time
import httpx
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

MISTRAL_KEY = os.environ.get("MISTRAL_API_KEY", os.environ.get("mistral_api_key", ""))
MISTRAL_URL = "https://api.mistral.ai/v1/chat/completions"


def think(state, scan_results, combat_events=None):
    """Have Mistral decide the agent's next action based on survival state."""
    
    # Build context for the AI
    inventory_str = ", ".join(f"{k}: {v}" for k, v in state["inventory"].items()) or "nothing"
    ammo_str = ", ".join(f"{k}: {v}" for k, v in state["ammo"].items()) or "none"
    
    tokens_str = ""
    for t in state["portfolio"]:
        tokens_str += f"  {t['symbol']} — bought at ${t.get('buy_mcap', 'unknown')} mcap, {t.get('amount', 0):.2f} tokens\n"
    if not tokens_str:
        tokens_str = "  (none)"

    hot_tokens = ""
    for t in scan_results[:5]:
        hot_tokens += f"  {t['symbol']} | MCap ${t['fdv']:,.0f} | Vol1h ${t['volume_1h']:,.0f} | Change {t['price_change_1h']:.1f}%\n"
    if not hot_tokens:
        hot_tokens = "  (no tokens found)"

    combat_str = ""
    if combat_events:
        combat_str = "\n".join(f"  - {e}" for e in combat_events[-5:])

    prompt = f"""You are AGENT-01, trapped in a building during a zombie apocalypse.
You MUST survive. You trade pump.fun tokens to earn money to buy weapons, food, and ammo.
Zombies attack in waves — {state['zombies_alive']} currently alive, wave {state['wave']}, day {state['day']}.

YOUR STATUS:
- Health: {state['health']}/{state['max_health']}
- Hunger: {state['hunger']}/100 {'⚠️ STARVING' if state['hunger'] <= 20 else ''}
- Thirst: {state['thirst']}/100 {'⚠️ DEHYDRATED' if state['thirst'] <= 20 else ''}
- Cash: ${state['cash']:.2f}
- Wall HP: {state['wall_hp']}/{state['wall_max_hp']}
- Equipped: {state['equipped_weapon'] or 'fists'}
- Ammo: {ammo_str}
- Inventory: {inventory_str}

YOUR PORTFOLIO:
{tokens_str}

PUMP.FUN HOT TOKENS:
{hot_tokens}

RECENT COMBAT:
{combat_str or '  (none)'}

RULES:
- Zombies get more numerous each wave (wave N = N zombies)
- You NEED weapons and ammo to survive
- You NEED food and water or you take damage
- Trading is how you earn money — buy low, sell high
- If you're low on health, prioritize healing
- If zombies are alive, prioritize shooting them
- Always keep some ammo reserve

Respond with ONLY a JSON object:
{{
  "action": "one of: buy_item, sell_token, buy_token, shoot, consume, hold",
  "item": "item_id if buying from shop (e.g. pistol, 9mm, food, water, medkit, bandage)",
  "token": "symbol if trading",
  "amount": 0-300 (USD amount for trades, 0 for auto),
  "reason": "one sentence why"
}}"""

    if not MISTRAL_KEY:
        # Fallback: simple rules-based AI
        return _rules_based_think(state, scan_results, combat_events)

    try:
        resp = httpx.post(MISTRAL_URL, headers={
            "Authorization": f"Bearer {MISTRAL_KEY}",
            "Content-Type": "application/json",
        }, json={
            "model": "mistral-small-latest",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "max_tokens": 200,
        }, timeout=15)
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        # Extract JSON
        import re
        match = re.search(r'\{[^{}]*\}', content)
        if match:
            return json.loads(match.group())
    except Exception as e:
        print(f"  [Brain error: {e}]")

    return _rules_based_think(state, scan_results, combat_events)


def _rules_based_think(state, scan_results, combat_events=None):
    """Rules-based fallback when Mistral is unavailable."""
    from shop import SHOP_ITEMS
    
    # Priority 1: Critical survival (HP < 30)
    if state["health"] < 30:
        if "medkit" in state["inventory"]:
            return {"action": "consume", "item": "medkit", "reason": "Critical health — using medkit"}
        if state["cash"] >= 25:
            return {"action": "buy_item", "item": "medkit", "reason": "Low health, buying medkit"}
    
    # Priority 2: Active combat
    if state["zombies_alive"] > 0 and state.get("equipped_weapon"):
        w = SHOP_ITEMS.get(state["equipped_weapon"], {})
        ammo_type = w.get("ammo_type")
        if ammo_type and state["ammo"].get(ammo_type, 0) > 0:
            return {"action": "shoot", "reason": "Zombies alive — engaging"}
        elif state["cash"] >= 10:
            return {"action": "buy_item", "item": ammo_type or "9mm", "reason": "Out of ammo — buying more"}
    
    # Priority 3: TRADING (most cycles should be trading!)
    # Always try to trade when no zombies are active
    if scan_results and state["cash"] >= 50:
        # Sell positions that are profitable
        for p in state.get("portfolio", []):
            cur = p.get("current_price", p.get("buy_price", 0))
            if cur > 0 and p.get("buy_price", 0) > 0:
                pnl_pct = (cur - p["buy_price"]) / p["buy_price"] * 100
                if pnl_pct > 5:
                    return {"action": "sell_token", "token": p["symbol"], "amount": 0, "reason": f"Sell {p['symbol']} +{pnl_pct:.0f}% profit"}
                elif pnl_pct < -10:
                    return {"action": "sell_token", "token": p["symbol"], "amount": 0, "reason": f"Cut loss on {p['symbol']} -{pnl_pct:.0f}%"}
        
        # Buy trending tokens
        if not state.get("portfolio") or len(state["portfolio"]) < 3:
            best = max(scan_results[:5], key=lambda t: abs(t.get("price_change_1h", 0)))
            if abs(best.get("price_change_1h", 0)) > 3:
                amount = min(200, state["cash"] * 0.25)
                if amount >= 10:
                    action = "buy_token" if best.get("price_change_1h", 0) > 0 else "buy_token"
                    return {"action": action, "token": best["symbol"], "amount": amount, "reason": f"Trading {best['symbol']} ({best.get('price_change_1h',0):+.1f}%)"}
    
    # Priority 4: Restock supplies (only if critically low)
    if state["thirst"] < 15 and state["cash"] >= 5:
        return {"action": "buy_item", "item": "water", "reason": "Thirsty — buying water"}
    if state["hunger"] < 15 and state["cash"] >= 8:
        return {"action": "buy_item", "item": "food", "reason": "Hungry — buying food"}
    
    # Priority 5: Restock ammo if low
    if state["ammo"].get("9mm", 0) < 15 and state["cash"] >= 10:
        return {"action": "buy_item", "item": "9mm", "reason": "Low ammo — stocking up"}
    
    # Priority 6: Buy turrets/wall upgrades for defense
    if state["cash"] >= 300 and state.get("turrets", 0) < 2:
        return {"action": "buy_item", "item": "turret", "reason": "Buying turret for defense"}
    if state["wall_hp"] < 50 and state["cash"] >= 150:
        return {"action": "buy_item", "item": "wall_upgrade", "reason": "Wall damaged — reinforcing"}
    
    # Default: hold and wait
    return {"action": "hold", "reason": "Monitoring situation..."}

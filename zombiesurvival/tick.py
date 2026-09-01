"""
Main tick loop for Zombie Survival Agent.
Cycle: spawn wave → move zombies → agent thinks → combat → survive → deploy state.
"""
import json, os, sys, time, random

sys.path.insert(0, os.path.dirname(__file__))

from survival import (load, save, reset, add_event, add_item, remove_item,
                      add_ammo, heal_agent, tick_hunger_thirst, spawn_wave,
                      zombies_for_wave, zombie_hp_for_wave, damage_agent)
from combat import process_zombie_wave, agent_shoot
from agent import think
from shop import SHOP_ITEMS

# Pump.fun scanner
import pumpfun as _pf
scan_pumpfun = getattr(_pf, 'scan_for_opportunities', None)
print(f"  [pumpfun loaded: {scan_pumpfun is not None}]")

WEB_STATE = "/var/www/hoodstreet/zombiesurvival_state.json"


def scan_tokens():
    """Scan pump.fun for hot tokens."""
    if scan_pumpfun:
        try:
            return scan_pumpfun()
        except Exception as e:
            print(f"  Scan error: {e}")
    return []


def execute_trade(state, decision):
    """Execute a pump.fun trade based on agent's decision."""
    token_sym = decision.get("token", "")
    amount = float(decision.get("amount", 100))
    action = decision.get("action")

    if not token_sym or not scan_pumpfun:
        return False

    tokens = scan_pumpfun()
    target = None
    for t in tokens:
        if t["symbol"].upper() == token_sym.upper():
            target = t
            break
    if not target:
        # Pick best momentum token
        if tokens:
            target = max(tokens[:5], key=lambda t: abs(t.get("price_change_1h", 0)))
        else:
            return False

    if action == "buy_token":
        if amount > state["cash"]:
            amount = state["cash"] * 0.4
        if amount < 5:
            return False
        state["cash"] -= amount
        price = target.get("price_usd", target.get("price", 0))
        tokens_bought = amount / price if price > 0 else 0
        state["portfolio"].append({
            "token": target.get("mint", target.get("address", "")),
            "symbol": target["symbol"],
            "amount": tokens_bought,
            "buy_price": price,
            "buy_mcap": target.get("fdv", 0),
        })
        state["trades"] += 1
        add_event(state, f"🟢 Bought {amount:.0f} USD of {target['symbol']} (MCap ${target.get('fdv', 0):,.0f})", "trade")
        return True

    elif action == "sell_token":
        # Find matching position
        for i, p in enumerate(state["portfolio"]):
            if p["symbol"].upper() == token_sym.upper():
                price = target.get("price_usd", target.get("price", 0))
                sell_value = p["amount"] * price
                buy_value = p["amount"] * p["buy_price"]
                pnl = sell_value - buy_value
                state["cash"] += sell_value
                state["portfolio"].pop(i)
                state["trades"] += 1
                if pnl > 0:
                    state["wins"] += 1
                    add_event(state, f"💰 Sold {token_sym} for +${pnl:.2f} profit!", "trade")
                else:
                    state["losses"] += 1
                    add_event(state, f"📉 Sold {token_sym} for -${abs(pnl):.2f} loss", "trade")
                return True
    return False


def update_portfolio_prices(state):
    """Update portfolio token prices."""
    if not scan_pumpfun:
        return
    tokens = scan_pumpfun()
    price_map = {t["symbol"].upper(): t.get("price_usd", t.get("price", 0)) for t in tokens}
    for p in state["portfolio"]:
        new_price = price_map.get(p["symbol"].upper())
        if new_price:
            p["current_price"] = new_price


def execute_shop_purchase(state, decision):
    """Buy an item from the shop."""
    item_id = decision.get("item")
    if not item_id or item_id not in SHOP_ITEMS:
        return False

    item = SHOP_ITEMS[item_id]
    price = item["price"]

    if state["cash"] < price:
        return False

    state["cash"] -= price

    if item["type"] == "weapon":
        add_item(state, item_id)
        state["equipped_weapon"] = item_id
        add_event(state, f"🔫 Bought {item['name']}!", "shop")
    elif item["type"] == "ammo":
        add_ammo(state, item_id, item["amount"])
        add_event(state, f"🔸 Bought {item['name']} (+{item['amount']})", "shop")
    elif item["type"] == "consumable":
        if "heal" in item:
            heal_agent(state, item["heal"])
        if "hunger" in item:
            state["hunger"] = min(100, state["hunger"] + item["hunger"])
        if "thirst" in item:
            state["thirst"] = min(100, state["thirst"] + item["thirst"])
        add_event(state, f"🍖 Used {item['name']} (+{item.get('heal', 0)} HP)", "shop")
    elif item["type"] == "defense":
        if item_id == "turret":
            state["turrets"] = state.get("turrets", 0) + 1
            add_event(state, f"🛡️ Deployed Auto Turret #{state['turrets']}", "shop")
        elif item_id == "wall_upgrade":
            state["wall_hp"] = min(state["wall_hp"] + item["wall_hp"], state["wall_max_hp"])
            add_event(state, f"🧱 Wall reinforced! HP: {state['wall_hp']}/{state['wall_max_hp']}", "shop")
        elif item_id == "flashlight":
            state["has_flashlight"] = True
            add_event(state, "🔦 Flashlight acquired — can see further", "shop")
    elif item["type"] == "utility":
        if item_id == "flashlight":
            state["has_flashlight"] = True
            add_event(state, "🔦 Flashlight acquired!", "shop")

    return True


def run_tick():
    """Execute one full game cycle."""
    state = load()

    if not state["is_alive"]:
        print("💀 AGENT IS DEAD. Resetting...")
        state = reset()
        add_event(state, "🔄 AGENT-01 respawned with $100 and a pistol!", "system")

    state["cycle"] += 1
    print(f"\n=== CYCLE {state['cycle']} | Day {state['day']} | Wave {state['wave']} ===")
    print(f"  💰 Cash: ${state['cash']:.2f} | ❤️ HP: {state['health']}/{state['max_health']} | 🍖 Hunger: {state['hunger']} | 💧 Thirst: {state['thirst']}")

    # Step 1: Hunger/thirst decay
    tick_hunger_thirst(state)

    # Step 2: Spawn zombie wave (every 3 cycles = ~1 min)
    zombies_just_spawned = False
    if state["zombies_alive"] <= 0:
        num = zombies_for_wave(state["wave"])
        zombies = spawn_wave(state)
        zombies_just_spawned = True
        add_event(state, f"🧟 WAVE {state['wave']}: {num} zombies approaching! (Day {state['day']})", "danger")
        print(f"  🧟 WAVE {state['wave']}: {num} zombies spawned!")

    # Step 3: Process combat (move zombies, turret fire, zombie attacks)
    combat_events = process_zombie_wave(state)
    for evt_text, evt_type in combat_events:
        add_event(state, evt_text, evt_type)
        print(f"  ⚔️ {evt_text}")

    # Step 4: Agent shoots at zombies (multiple shots per tick)
    if state["zombies_alive"] > 0 and state.get("equipped_weapon"):
        _w = SHOP_ITEMS.get(state['equipped_weapon'], {})
        _shots = max(1, int(_w.get('fire_rate', 1) * 3))
        for _s in range(_shots):
            if state['zombies_alive'] <= 0:
                break
            result = agent_shoot(state)
            if result:
                add_event(state, f"\U0001f52b {result}", "combat")
                print(f"  \U0001f52b {result}")

    # Step 5: Scan pump.fun
    tokens = scan_tokens()
    print(f"  📊 Scanned {len(tokens)} pump.fun tokens")

    # Step 6: Update portfolio prices
    update_portfolio_prices(state)

    # Step 7: Agent brain decides
    decision = think(state, tokens, combat_events)
    action = decision.get("action", "hold")
    reason = decision.get("reason", "")
    print(f"  🧠 Decision: {action} — {reason}")

    # Step 8: Execute decision
    if action == "buy_token" or action == "sell_token":
        execute_trade(state, decision)
    elif action == "buy_item":
        execute_shop_purchase(state, decision)
    elif action == "shoot":
        if state["zombies_alive"] > 0:
            result = agent_shoot(state)
            if result:
                add_event(state, f"🔫 {result}", "combat")
                print(f"  🔫 {result}")
    elif action == "consume":
        item_id = decision.get("item")
        if item_id and item_id in state.get("inventory", {}):
            remove_item(state, item_id)
            item = SHOP_ITEMS[item_id]
            if "heal" in item:
                heal_agent(state, item["heal"])
            if "hunger" in item:
                state["hunger"] = min(100, state["hunger"] + item["hunger"])
            if "thirst" in item:
                state["thirst"] = min(100, state["thirst"] + item["thirst"])
            add_event(state, f"🍖 Used {item['name']}", "shop")

    # Step 9: Wave progression
    if state["zombies_alive"] <= 0 and zombies_just_spawned:
        # Wave cleared!
        state["wave"] += 1
        bonus = state["wave"] * 10
        state["cash"] += bonus
        add_event(state, f"✅ Wave {state['wave']-1} cleared! +${bonus} bonus!", "success")
        print(f"  ✅ Wave cleared! +${bonus} bonus")
        # Day changes every 5 waves
        if state["wave"] % 5 == 1 and state["wave"] > 1:
            state["day"] += 1
            add_event(state, f"🌅 DAY {state['day']} begins...", "system")

    # Step 10: Calculate portfolio value
    portfolio_value = 0
    for p in state["portfolio"]:
        current = p.get("current_price", p["buy_price"])
        portfolio_value += p["amount"] * current

    total_value = state["cash"] + portfolio_value
    pnl = total_value - 100

    print(f"  📈 Portfolio: ${portfolio_value:.2f} | Total: ${total_value:.2f} | PnL: {'+'if pnl>=0 else ''}{pnl:.2f}")

    # Save state
    save(state)

    # Deploy to web
    deploy_state = dict(state)
    deploy_state["portfolio_value"] = portfolio_value
    deploy_state["total_value"] = total_value
    deploy_state["pnl"] = pnl
    # Convert zombie positions for JS
    deploy_state["zombies"] = state["zombie_positions"]

    os.makedirs(os.path.dirname(WEB_STATE), exist_ok=True)
    with open(WEB_STATE, "w") as f:
        json.dump(deploy_state, f, indent=2)

    # Also keep local copy
    local_state = os.path.join(os.path.dirname(__file__), "data", "web_state.json")
    with open(local_state, "w") as f:
        json.dump(deploy_state, f, indent=2)

    print(f"  ✅ State deployed to {WEB_STATE}")
    return state


if __name__ == "__main__":
    run_tick()

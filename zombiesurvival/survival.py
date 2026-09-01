"""
Zombie Survival State Engine
Tracks agent health, hunger, thirst, cash, inventory, waves, and game status.
"""
import json, os, time

STATE_FILE = os.path.join(os.path.dirname(__file__), "data", "survival.json")

DEFAULT_STATE = {
    "cash": 100.0,
    "health": 100,
    "max_health": 100,
    "hunger": 100,
    "thirst": 100,
    "wall_hp": 100,
    "wall_max_hp": 100,
    "day": 1,
    "wave": 1,
    "zombies_alive": 0,
    "zombies_killed": 0,
    "total_zombies_killed": 0,
    "inventory": {},        # {item_id: count}
    "equipped_weapon": None,
    "ammo": {},             # {ammo_type: count}
    "position": "base",     # base, shop, field
    "is_alive": True,
    "cycle": 0,
    "trades": 0,
    "wins": 0,
    "losses": 0,
    "portfolio": [],        # [{token, symbol, amount, buy_price, buy_mcap}]
    "last_tick": 0,
    "events": [],           # recent events for story log
    "turrets": 0,
    "has_flashlight": False,
    "has_energy_boost": False,
    "energy_boost_ends": 0,
    "zombie_positions": [], # [{id, x, y, hp, max_hp, speed, type}]
    "agent_x": 50,
    "agent_y": 50,
    "scene": "base",        # base, shop, combat
}


def load():
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
        # Merge with defaults for any missing keys
        for k, v in DEFAULT_STATE.items():
            if k not in state:
                state[k] = v
        return state
    return dict(DEFAULT_STATE)


def save(state):
    state["last_tick"] = time.time()
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def reset():
    state = dict(DEFAULT_STATE)
    state["inventory"] = {"pistol": 1}
    state["ammo"] = {"9mm": 30}
    state["equipped_weapon"] = "pistol"
    save(state)
    return state


def add_event(state, text, event_type="info"):
    """Add event to story log (keep last 50)."""
    ts = time.strftime("%H:%M:%S")
    state["events"].append({"text": text, "type": event_type, "time": ts, "cycle": state["cycle"]})
    state["events"] = state["events"][-50:]


def add_item(state, item_id, count=1):
    state["inventory"][item_id] = state["inventory"].get(item_id, 0) + count


def remove_item(state, item_id, count=1):
    if state["inventory"].get(item_id, 0) >= count:
        state["inventory"][item_id] -= count
        if state["inventory"][item_id] <= 0:
            del state["inventory"][item_id]
        return True
    return False


def add_ammo(state, ammo_type, count):
    state["ammo"][ammo_type] = state["ammo"].get(ammo_type, 0) + count


def use_ammo(state, ammo_type, count=1):
    if state["ammo"].get(ammo_type, 0) >= count:
        state["ammo"][ammo_type] -= count
        if state["ammo"][ammo_type] <= 0:
            del state["ammo"][ammo_type]
        return True
    return False


def has_ammo(state, ammo_type, count=1):
    return state["ammo"].get(ammo_type, 0) >= count


def get_weapon_ammo_type(state):
    """Get ammo type for currently equipped weapon."""
    from shop import SHOP_ITEMS
    w = state.get("equipped_weapon")
    if w and w in SHOP_ITEMS:
        return SHOP_ITEMS[w].get("ammo_type")
    return None


def damage_agent(state, amount):
    """Agent takes damage. Returns True if dead."""
    state["health"] -= amount
    if state["health"] <= 0:
        state["health"] = 0
        state["is_alive"] = False
        return True
    return False


def damage_wall(state, amount):
    """Zombies attack wall."""
    state["wall_hp"] -= amount
    if state["wall_hp"] < 0:
        state["wall_hp"] = 0


def heal_agent(state, amount):
    state["health"] = min(state["health"] + amount, state["max_health"])


def tick_hunger_thirst(state):
    """Per-wave hunger/thirst decay."""
    state["hunger"] = max(0, state["hunger"] - 2)
    state["thirst"] = max(0, state["thirst"] - 3)
    # Starving/dehydrated damage
    if state["hunger"] <= 0:
        damage_agent(state, 5)
        add_event(state, "⚠️ Starving! Lost 5 HP!", "danger")
    if state["thirst"] <= 0:
        damage_agent(state, 8)
        add_event(state, "⚠️ Dehydrated! Lost 8 HP!", "danger")


def zombies_for_wave(wave):
    """Calculate zombies per wave: wave 1 = 1, wave 2 = 2, wave 3 = 3, etc."""
    return wave


def zombie_hp_for_wave(wave):
    """Zombies get slightly tougher each wave."""
    base = 30
    return base + (wave - 1) * 5


def zombie_speed_for_wave(wave):
    """Zombies get slightly faster each wave."""
    return 1.0 + (wave - 1) * 0.05


def spawn_wave(state):
    """Spawn a new wave of zombies. Returns list of zombies."""
    import random
    num = zombies_for_wave(state["wave"])
    zombies = []
    for i in range(num):
        ztype = random.choice(["normal", "normal", "normal", "fast", "tank"]) if state["wave"] >= 3 else "normal"
        z_hp = zombie_hp_for_wave(state["wave"])
        z_speed = zombie_speed_for_wave(state["wave"])
        if ztype == "fast":
            z_hp = int(z_hp * 0.6)
            z_speed *= 1.5
        elif ztype == "tank":
            z_hp = int(z_hp * 2.0)
            z_speed *= 0.6
        # Spawn from edges
        side = random.choice(["top", "bottom", "left", "right"])
        if side == "top":
            x, y = random.randint(5, 95), 0
        elif side == "bottom":
            x, y = random.randint(5, 95), 100
        elif side == "left":
            x, y = 0, random.randint(5, 95)
        else:
            x, y = 100, random.randint(5, 95)
        zombies.append({
            "id": f"z{state['wave']}_{i}",
            "x": x, "y": y,
            "hp": z_hp, "max_hp": z_hp,
            "speed": z_speed,
            "type": ztype,
            "damage": 10 if ztype == "normal" else (8 if ztype == "fast" else 20),
        })
    state["zombie_positions"] = zombies
    state["zombies_alive"] = num
    return zombies

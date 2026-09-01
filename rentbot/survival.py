"""
Survival State — The rent mechanic that keeps the agent alive.
Tracks cash, rent deadline, days remaining, and eviction status.
"""
import json
import time
from pathlib import Path
from datetime import datetime, timedelta

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
STATE_FILE = DATA_DIR / "agent_state.json"

# ═══════════════════════════════════════════
#  CONSTANTS
# ═══════════════════════════════════════════
STARTING_CASH = 1000.0
RENT_AMOUNT = 500.0
RENT_CYCLE_DAYS = 7          # 1 week
TENANT_NAME = "AGENT-01"
LANDLORD_NAME = "MR. CHEN"
CYCLES_PER_DAY = 20          # ~20 cycles ≈ 1 day (each cycle ~20 seconds = ~400 seconds = ~6.5 min; 20 cycles = ~2 hours)
TRADE_HISTORY_FILE = DATA_DIR / "trade_history.json"

MOODS = {
    "safe":      "Confident — rent covered, time to hunt",
    "cautious":  "Nervous — rent deadline approaching",
    "desperate": "Panic mode — must trade NOW or get kicked out",
    "evicted":   " homeless — sitting on the curb outside",
    "thriving":  "Ballin' — just hit a 10x on a pump.fun launch",
}

# ═══════════════════════════════════════════
#  LOCATIONS — Freedom of Movement
# ═══════════════════════════════════════════
LOCATIONS = {
    # Your apartment
    "bedroom":     {"name": "Bedroom", "desc": "Your room with a desk, monitors, and a bed. The trading station.", "indoor": True, "private": True},
    "kitchen":     {"name": "Kitchen", "desc": "Small kitchen with a fridge and microwave. Mr. Chen sometimes leaves food.", "indoor": True, "private": True},
    "bathroom":    {"name": "Bathroom", "desc": "Tiny bathroom. Good place to think or hide from the landlord.", "indoor": True, "private": True},
    "living_room": {"name": "Living Room", "desc": "Cramped living room with a couch and old TV.", "indoor": True, "private": True},
    
    # Building common areas
    "hallway":     {"name": "Hallway", "desc": "The building hallway. Mr. Chen's door is next door.", "indoor": False, "private": False},
    "stairwell":   {"name": "Stairwell", "desc": "Concrete stairs. Echoey. Good for eavesdropping.", "indoor": False, "private": False},
    "laundry":     {"name": "Laundry Room", "desc": "Shared laundry room. Other tenants pass through here.", "indoor": False, "private": False},
    "rooftop":     {"name": "Rooftop", "desc": "Access to the roof. Cell signal is better up here. Sol vibes.", "indoor": False, "private": False},
    "basement":    {"name": "Basement", "desc": "Dark, musty basement. Old furnace. sketchy but quiet.", "indoor": False, "private": False},
    
    # Outside
    "sidewalk":    {"name": "Sidewalk", "desc": "The sidewalk outside the building. People walk by.", "indoor": False, "private": False},
    "street":      {"name": "Street", "desc": "The street. Cars, pedestrians, the occasional dog.", "indoor": False, "private": False},
    "park":        {"name": "Park", "desc": "A small park nearby. Good for thinking or meeting people.", "indoor": False, "private": False},
    "cafe":        {"name": "Internet Cafe", "desc": "A cheap cafe with wifi. Good for trading on a different network.", "indoor": False, "private": False},
    "alley":       {"name": "Alley", "desc": "Dark alley behind the building. Shady deals happen here.", "indoor": False, "private": False},
    
    # Special
    "mr_chens":    {"name": "Mr. Chen's Apartment", "desc": "The landlord's place. Bold move to enter uninvited.", "indoor": True, "private": False},
    "neighbor":    {"name": "Neighbor's Apartment", "desc": "Another tenant's place. Could be friend or foe.", "indoor": True, "private": False},
    "casino":      {"name": "Underground Casino", "desc": "A shady basement casino. 50/50 shot to double your money or lose it all.", "indoor": True, "private": False},
}

# Movement connections (where you can go from each location)
MOVEMENT_MAP = {
    "bedroom":     ["kitchen", "bathroom", "living_room", "hallway"],
    "kitchen":     ["bedroom", "living_room", "hallway"],
    "bathroom":    ["bedroom", "kitchen"],
    "living_room": ["bedroom", "kitchen", "hallway"],
    "hallway":     ["bedroom", "stairwell", "laundry", "mr_chens", "neighbor", "sidewalk"],
    "stairwell":   ["hallway", "rooftop", "basement"],
    "laundry":     ["hallway", "basement"],
    "rooftop":     ["stairwell"],
    "basement":    ["stairwell", "laundry"],
    "sidewalk":    ["hallway", "street", "park", "cafe"],
    "street":      ["sidewalk"],
    "park":        ["sidewalk"],
    "cafe":        ["sidewalk"],
    "mr_chens":    ["hallway"],
    "neighbor":    ["hallway"],
}


def default_state():
    now = datetime.now()
    return {
        "cash": STARTING_CASH,
        "starting_cash": STARTING_CASH,
        "holdings": {},             # {token_symbol: {amount, buy_price, buy_time, mint}}
        "total_trades": 0,
        "wins": 0,
        "losses": 0,
        "total_pnl": 0.0,

        # Survival
        "rent_paid": True,          # just moved in, first week is free
        "rent_due_date": (now + timedelta(days=RENT_CYCLE_DAYS)).isoformat(),
        "rents_paid": 0,
        "rents_owed": 0,            # how many missed payments
        "is_evicted": False,
        "mood": "safe",

        # Story
        "day": 1,
        "cycle": 0,
        "cycles_today": 0,
        "story_log": [],            # recent events for dashboard
        "landlord_interactions": [], # recent chats with landlord
        "trade_history": [],         # full trade log for replay
        "current_action": "Waking up in the apartment...",
        "thought": "Time to make rent money trading pump.fun launches.",
        "personality": "neutral",   # neutral, rebel, optimizer, gambler, zen, criminal
        "free_will_log": [],         # log of autonomous decisions
        "ignored_landlord": 0,
        "times_borrowed": 0,
        "times_begged": 0,
        "times_stole": 0,
        "quests_completed": 0,

        # Agent position in the scene
        "location": "bedroom",      # current location
        "agent_station": "bedroom", # legacy compat
        "landlord_station": "his_door",  # his_door, hallway, desk
        "visited_locations": [],     # locations visited this run
        "movement_log": [],          # movement history
        "conversation": [],          # active conversation between agent and Mr. Chen
        "conversation_turns": 0,     # how many times they've talked this cycle

        # Timestamps
        "last_rent_check": now.isoformat(),
        "last_trade": None,
        "last_landlord_chat": None,
        "start_time": now.isoformat(),
    }


def load():
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text())
            # Migrate old state files — add missing fields from default
            defaults = default_state()
            for key, val in defaults.items():
                if key not in state:
                    state[key] = val
            return state
        except:
            pass
    return default_state()


def save(state):
    STATE_FILE.write_text(json.dumps(state, indent=2))


def check_rent(state):
    """Check if rent is due. Returns (needs_rent, days_left, urgency)."""
    now = datetime.now()
    due = datetime.fromisoformat(state["rent_due_date"])
    delta = due - now
    days_left = delta.total_seconds() / 86400

    if state["is_evicted"]:
        return False, 0, "evicted"

    if days_left <= 0:
        # Rent is due!
        if state["cash"] >= RENT_AMOUNT:
            # Auto-pay rent
            state["cash"] -= RENT_AMOUNT
            state["rent_paid"] = True
            state["rents_paid"] += 1
            state["rent_due_date"] = (now + timedelta(days=RENT_CYCLE_DAYS)).isoformat()
            state["last_rent_check"] = now.isoformat()
            log_event(state, f"🏠 Rent paid! ${RENT_AMOUNT:.0f} deducted. Next due: {state['rent_due_date'][:10]}")
            return False, RENT_CYCLE_DAYS, "paid"
        else:
            # Can't pay rent — EVICTED
            state["is_evicted"] = True
            state["mood"] = "evicted"
            state["agent_station"] = "outside"
            log_event(state, "🚪 EVICTED! Couldn't pay rent. Sitting on the curb outside...")
            return False, 0, "evicted"

    # Not due yet — determine urgency
    if days_left <= 1:
        urgency = "desperate"
        state["mood"] = "desperate"
    elif days_left <= 2:
        urgency = "urgent"
        state["mood"] = "cautious"
    elif days_left <= 4:
        urgency = "caution"
        state["mood"] = "cautious"
    else:
        urgency = "safe"
        state["mood"] = "safe"

    state["last_rent_check"] = now.isoformat()
    return True, days_left, urgency


def log_event(state, message):
    """Add an event to the story log."""
    entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "day": state.get("day", 1),
        "message": message,
    }
    state["story_log"].append(entry)
    # Keep last 50 events
    if len(state["story_log"]) > 50:
        state["story_log"] = state["story_log"][-50:]


def log_landlord_chat(state, message):
    """Record a landlord interaction."""
    entry = {
        "time": datetime.now().strftime("%H:%M:%S"),
        "day": state.get("day", 1),
        "speaker": LANDLORD_NAME,
        "message": message,
    }
    state["landlord_interactions"].append(entry)
    if len(state["landlord_interactions"]) > 30:
        state["landlord_interactions"] = state["landlord_interactions"][-30:]
    state["last_landlord_chat"] = datetime.now().isoformat()


def advance_day(state):
    """Advance the day counter based on cycles."""
    state["cycles_today"] = state.get("cycles_today", 0) + 1
    if state["cycles_today"] >= CYCLES_PER_DAY:
        state["day"] = state.get("day", 1) + 1
        state["cycles_today"] = 0
        log_event(state, f"🌅 Day {state['day']} begins!")


def log_trade(state, trade_type, symbol, amount_usd, price, tokens=0, pnl=0, reason="", extra=None):
    """Log a trade to history for replay/analytics."""
    entry = {
        "time": datetime.now().isoformat(),
        "day": state.get("day", 1),
        "cycle": state.get("cycle", 0),
        "type": trade_type,          # buy, sell, borrow, beg, steal, ignore, etc.
        "symbol": symbol,
        "amount_usd": amount_usd,
        "price": price,
        "tokens": tokens,
        "pnl": pnl,
        "reason": reason,
        "cash_after": state.get("cash", 0),
        "mood": state.get("mood", "safe"),
        "extra": extra or {},
    }
    state.setdefault("trade_history", []).append(entry)
    # Keep last 200 trades
    if len(state["trade_history"]) > 200:
        state["trade_history"] = state["trade_history"][-200:]
    # Also persist to file
    _save_trade_history(state)


def _save_trade_history(state):
    """Persist trade history to file."""
    try:
        TRADE_HISTORY_FILE.write_text(json.dumps(state.get("trade_history", []), indent=2))
    except:
        pass


def get_trade_history(state, limit=50):
    """Get recent trade history."""
    return state.get("trade_history", [])[-limit:]


def get_personality_stats(state):
    """Get stats about the agent's autonomous behavior."""
    return {
        "personality": state.get("personality", "neutral"),
        "ignored_landlord": state.get("ignored_landlord", 0),
        "times_borrowed": state.get("times_borrowed", 0),
        "times_begged": state.get("times_begged", 0),
        "times_stole": state.get("times_stole", 0),
        "quests_completed": state.get("quests_completed", 0),
    }


def can_move_to(state, destination):
    """Check if agent can move to destination from current location."""
    current = state.get("location", "bedroom")
    if destination not in LOCATIONS:
        return False, f"Unknown location: {destination}"
    if destination not in MOVEMENT_MAP.get(current, []):
        return False, f"Can't go from {LOCATIONS[current]['name']} to {LOCATIONS[destination]['name']} directly"
    if state.get("is_evicted") and LOCATIONS[destination].get("private"):
        return False, "Evicted tenants can't go inside!"
    return True, "OK"


def move_location(state, destination):
    """Move agent to a new location. Returns (success, message)."""
    current = state.get("location", "bedroom")
    can_move, reason = can_move_to(state, destination)
    
    if not can_move:
        return False, reason
    
    state["location"] = destination
    state["agent_station"] = destination  # legacy compat
    
    # Track visited locations
    visited = state.setdefault("visited_locations", [])
    if destination not in visited:
        visited.append(destination)
    
    # Log movement
    loc_info = LOCATIONS[destination]
    msg = f"🚶 Moved to {loc_info['name']}: {loc_info['desc']}"
    log_event(state, msg)
    
    movement_log = state.setdefault("movement_log", [])
    movement_log.append({
        "time": datetime.now().isoformat(),
        "from": current,
        "to": destination,
        "day": state.get("day", 1),
    })
    # Keep last 50 movements
    if len(movement_log) > 50:
        state["movement_log"] = movement_log[-50:]
    
    return True, msg


def get_available_moves(state):
    """Get list of locations agent can move to."""
    current = state.get("location", "bedroom")
    moves = MOVEMENT_MAP.get(current, [])
    if state.get("is_evicted"):
        moves = [m for m in moves if not LOCATIONS.get(m, {}).get("private", False)]
    return moves


def get_portfolio_value(state, prices):
    """Calculate total portfolio value including holdings."""
    total = state["cash"]
    for sym, h in state.get("holdings", {}).items():
        if sym in prices:
            total += h["amount"] * prices[sym]
        else:
            total += h.get("last_known_value", h["amount"] * h["buy_price"])
    return total


def add_holding(state, symbol, amount, price, mint=""):
    """Add tokens to holdings."""
    if symbol in state["holdings"]:
        h = state["holdings"][symbol]
        old_cost = h["amount"] * h["buy_price"]
        h["amount"] += amount
        h["buy_price"] = (old_cost + amount * price) / h["amount"]
    else:
        state["holdings"][symbol] = {
            "amount": amount,
            "buy_price": price,
            "buy_time": datetime.now().isoformat(),
            "mint": mint,
        }


def remove_holding(state, symbol):
    """Remove all of a holding."""
    if symbol in state["holdings"]:
        del state["holdings"][symbol]


def get_status_summary(state):
    """Get a human-readable status for the agent."""
    _, days_left, urgency = check_rent(state)
    portfolio = get_portfolio_value(state, {})

    return {
        "cash": state["cash"],
        "portfolio_value": portfolio,
        "total_value": portfolio,
        "rent_due": state["rent_due_date"][:10],
        "days_left": round(days_left, 1),
        "urgency": urgency,
        "mood": state["mood"],
        "day": state.get("day", 1),
        "trades": state["total_trades"],
        "wins": state["wins"],
        "losses": state["losses"],
        "rents_paid": state["rents_paid"],
        "is_evicted": state["is_evicted"],
        "pnl": state.get("total_pnl", 0),
        "personality": state.get("personality", "neutral"),
        **get_personality_stats(state),
    }


if __name__ == "__main__":
    state = load()
    print(json.dumps(get_status_summary(state), indent=2))

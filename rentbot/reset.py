#!/usr/bin/env python3
"""
RentBot Reset — Reset the agent state to fresh start.
Run: python3 reset.py
"""
import json
from datetime import datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
STATE_FILE = DATA_DIR / "agent_state.json"

DEFAULT_STATE = {
    "cash": 1000.0,
    "starting_cash": 1000.0,
    "holdings": {},
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "total_pnl": 0.0,
    "rent_paid": True,
    "rent_due_date": (datetime.now() + timedelta(days=7)).isoformat(),
    "rents_paid": 0,
    "rents_owed": 0,
    "is_evicted": False,
    "mood": "safe",
    "day": 1,
    "cycle": 0,
    "cycles_today": 0,
    "story_log": [],
    "landlord_interactions": [],
    "trade_history": [],
    "current_action": "Waking up in the bedroom...",
    "thought": "Fresh start.",
    "personality": "neutral",
    "free_will_log": [],
    "ignored_landlord": 0,
    "times_borrowed": 0,
    "times_begged": 0,
    "times_stole": 0,
    "quests_completed": 0,
    "location": "bedroom",
    "landlord_location": "his_door",
    "agent_station": "bedroom",
    "landlord_station": "his_door",
    "visited_locations": [],
    "movement_log": [],
    "last_rent_check": datetime.now().isoformat(),
    "last_trade": None,
    "last_landlord_chat": None,
    "start_time": datetime.now().isoformat(),
}


def reset():
    STATE_FILE.write_text(json.dumps(DEFAULT_STATE, indent=2))
    print("✅ Agent reset!")
    print(f"   Cash: $1,000")
    print(f"   Location: Bedroom")
    print(f"   Day: 1")
    print(f"   Trades: 0")
    print(f"   Rent due: {(datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')}")
    print()
    print("Run: bash run_loop.sh")


if __name__ == "__main__":
    reset()

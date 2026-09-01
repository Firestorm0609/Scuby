"""
Gun Shop — Items the agent can buy to survive zombie waves.
All prices in USD (earned from pump.fun trading).
"""

SHOP_ITEMS = {
    # ═══ WEAPONS ═══
    "pistol": {
        "name": "Pistol",
        "type": "weapon",
        "price": 50,
        "damage": 15,
        "fire_rate": 1.0,    # shots per second
        "ammo_type": "9mm",
        "range": 3,           # tiles
        "icon": "🔫",
        "desc": "Reliable sidearm. Low damage, fast fire rate.",
    },
    "shotgun": {
        "name": "Shotgun",
        "type": "weapon",
        "price": 120,
        "damage": 40,
        "fire_rate": 0.5,
        "ammo_type": "shells",
        "range": 2,
        "icon": "💥",
        "desc": " devastating at close range. Slow reload.",
    },
    "rifle": {
        "name": "Rifle",
        "type": "weapon",
        "price": 200,
        "damage": 25,
        "fire_rate": 2.0,
        "ammo_type": "5.56",
        "range": 5,
        "icon": "🎯",
        "desc": "Long range, high fire rate. Accurate.",
    },
    "smg": {
        "name": "SMG",
        "type": "weapon",
        "price": 150,
        "damage": 10,
        "fire_rate": 4.0,
        "ammo_type": "9mm",
        "range": 2,
        "icon": "⚡",
        "desc": "Spray and pray. High fire rate, low damage.",
    },
    "sniper": {
        "name": "Sniper",
        "type": "weapon",
        "price": 350,
        "damage": 80,
        "fire_rate": 0.3,
        "ammo_type": "7.62",
        "range": 8,
        "icon": "🎯",
        "desc": "One shot, one kill. Very slow fire rate.",
    },

    # ═══ AMMO ═══
    "9mm": {
        "name": "9mm Rounds",
        "type": "ammo",
        "price": 10,
        "amount": 30,
        "icon": "🔸",
        "desc": "30 rounds of 9mm ammunition.",
    },
    "shells": {
        "name": "Shotgun Shells",
        "type": "ammo",
        "price": 15,
        "amount": 12,
        "icon": "🔸",
        "desc": "12 gauge shells.",
    },
    "5.56": {
        "name": "5.56 NATO",
        "type": "ammo",
        "price": 20,
        "amount": 30,
        "icon": "🔸",
        "desc": "30 rounds of 5.56mm.",
    },
    "7.62": {
        "name": "7.62 Rounds",
        "type": "ammo",
        "price": 25,
        "amount": 10,
        "icon": "🔸",
        "desc": "10 rounds of 7.62mm sniper ammo.",
    },

    # ═══ CONSUMABLES ═══
    "bandage": {
        "name": "Bandage",
        "type": "consumable",
        "price": 5,
        "heal": 15,
        "icon": "🩹",
        "desc": "Restores 15 HP. Basic first aid.",
    },
    "medkit": {
        "name": "Med Kit",
        "type": "consumable",
        "price": 25,
        "heal": 50,
        "icon": "🏥",
        "desc": "Restores 50 HP. Military grade.",
    },
    "food": {
        "name": "MRE (Food)",
        "type": "consumable",
        "price": 8,
        "heal": 10,
        "hunger": 100,   # fills hunger bar
        "icon": "🍖",
        "desc": "Restores 10 HP + fills hunger.",
    },
    "water": {
        "name": "Water Bottle",
        "type": "consumable",
        "price": 5,
        "heal": 5,
        "thirst": 100,
        "icon": "💧",
        "desc": "Restores 5 HP + fills thirst.",
    },
    "energy_drink": {
        "name": "Energy Drink",
        "type": "consumable",
        "price": 12,
        "heal": 5,
        "speed_boost": 1.5,  # 1.5x speed for 30s
        "icon": "⚡",
        "desc": "Restores 5 HP + boosts speed temporarily.",
    },

    # ═══ DEFENSE ═══
    "turret": {
        "name": "Auto Turret",
        "type": "defense",
        "price": 300,
        "damage": 5,
        "fire_rate": 3.0,
        "ammo_type": "9mm",
        "range": 4,
        "icon": "🛡️",
        "desc": "Automated turret. Shoots zombies automatically.",
    },
    "wall_upgrade": {
        "name": "Wall Upgrade",
        "type": "defense",
        "price": 150,
        "wall_hp": 50,
        "icon": "🧱",
        "desc": "Reinforces walls. +50 wall HP.",
    },
    "flashlight": {
        "name": "Flashlight",
        "type": "utility",
        "price": 20,
        "vision": 2,     # extra vision range
        "icon": "🔦",
        "desc": "See zombies from further away.",
    },
}


def get_shop_list():
    """Get all items formatted for display."""
    items = []
    for key, item in SHOP_ITEMS.items():
        items.append({
            "id": key,
            "name": item["name"],
            "type": item["type"],
            "price": item["price"],
            "icon": item["icon"],
            "desc": item["desc"],
        })
    return items


def get_weapon(weapon_id):
    """Get weapon stats."""
    item = SHOP_ITEMS.get(weapon_id)
    if item and item["type"] == "weapon":
        return item
    return None


def get_item(item_id):
    """Get any shop item."""
    return SHOP_ITEMS.get(item_id)


if __name__ == "__main__":
    print("=== GUN SHOP INVENTORY ===\n")
    for key, item in SHOP_ITEMS.items():
        print(f"  {item['icon']} {item['name']} — ${item['price']}")
        print(f"    {item['desc']}")
        if item["type"] == "weapon":
            print(f"    DMG: {item['damage']} | Rate: {item['fire_rate']}/s | Range: {item['range']}")
        print()

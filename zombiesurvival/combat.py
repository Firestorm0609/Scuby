"""
Zombie Combat Engine — moves zombies, processes attacks, manages combat each tick.
"""
import math, random


def move_zombie(zombie, agent_x, agent_y):
    """Move zombie toward agent. Returns new (x, y)."""
    dx = agent_x - zombie["x"]
    dy = agent_y - zombie["y"]
    dist = math.sqrt(dx * dx + dy * dy)
    if dist < 3:
        return zombie["x"], zombie["y"]  # reached agent
    # Normalize and move
    speed = zombie["speed"] * 1.5  # tiles per tick
    nx = zombie["x"] + (dx / dist) * speed
    ny = zombie["y"] + (dy / dist) * speed
    # Add slight randomness so zombies don't stack perfectly
    nx += random.uniform(-0.5, 0.5)
    ny += random.uniform(-0.5, 0.5)
    nx = max(0, min(100, nx))
    ny = max(0, min(100, ny))
    return round(nx, 1), round(ny, 1)


def process_zombie_wave(state):
    """Move all zombies toward agent. Zombies that reach agent deal damage.
    Turrets auto-fire. Returns list of events."""
    from shop import SHOP_ITEMS
    from survival import damage_agent, damage_wall, add_event, use_ammo, get_weapon_ammo_type

    events = []
    agent_x = state["agent_x"]
    agent_y = state["agent_y"]
    remaining = []

    # Turret auto-fire
    turret_target_idx = None
    if state.get("turrets", 0) > 0 and state["zombie_positions"]:
        turret_dmg = 5 * state["turrets"]
        turret_ammo = state["ammo"].get("9mm", 0)
        shots = min(state["turrets"] * 3, turret_ammo)
        if shots > 0:
            # Damage closest zombie
            closest = min(state["zombie_positions"],
                         key=lambda z: math.sqrt((z["x"]-agent_x)**2 + (z["y"]-agent_y)**2))
            closest["hp"] -= turret_dmg * shots
            use_ammo(state, "9mm", shots)
            if closest["hp"] <= 0:
                state["zombies_alive"] = max(0, state.get("zombies_alive", 1) - 1)
                state["zombie_positions"] = [z for z in state["zombie_positions"] if z["hp"] > 0]
                state["zombies_killed"] += 1
                state["total_zombies_killed"] += 1
                events.append(("Turret destroyed zombie!", "combat"))
            else:
                events.append((f"Turret dealt {turret_dmg * shots} damage!", "combat"))

    for z in state["zombie_positions"]:
        if z["hp"] <= 0:
            state["zombies_alive"] -= 1
            state["zombies_killed"] += 1
            state["total_zombies_killed"] += 1
            state["cash"] += random.randint(5, 15)  # loot
            events.append((f"Killed {z['type']} zombie! +${random.randint(5,15)} loot", "combat"))
            continue

        # Move toward agent
        zx, zy = move_zombie(z, agent_x, agent_y)
        z["x"], z["y"] = zx, zy

        dist = math.sqrt((zx - agent_x)**2 + (zy - agent_y)**2)

        if dist < 3:
            # Zombie attacks
            dmg = z["damage"]
            if state["wall_hp"] > 0:
                # Wall absorbs some damage
                wall_absorb = min(dmg, state["wall_hp"])
                from survival import damage_wall
                damage_wall(state, wall_absorb)
                dmg -= wall_absorb
                if dmg > 0:
                    damage_agent(state, dmg)
                    events.append((f"{z['type'].title()} zombie breached wall! -{dmg} HP", "danger"))
                else:
                    events.append((f"Wall absorbed {z['type']} zombie attack!", "info"))
            else:
                damage_agent(state, dmg)
                events.append((f"{z['type'].title()} zombie attacked! -{dmg} HP", "danger"))
        remaining.append(z)

    state["zombie_positions"] = remaining

    # Check agent death
    if state["health"] <= 0:
        state["is_alive"] = False
        events.append(("💀 AGENT HAS BEEN KILLED BY ZOMBIES!", "danger"))

    return events


def agent_shoot(state):
    """Agent shoots at nearest zombie. Returns event string or None."""
    from shop import SHOP_ITEMS
    from survival import use_ammo, get_weapon_ammo_type

    weapon_id = state.get("equipped_weapon")
    if not weapon_id:
        return None

    weapon = SHOP_ITEMS.get(weapon_id)
    if not weapon:
        return None

    ammo_type = weapon.get("ammo_type")
    if not ammo_type or not use_ammo(state, ammo_type):
        return None

    damage = weapon["damage"]
    agent_x = state["agent_x"]
    agent_y = state["agent_y"]

    # Find nearest zombie in range
    in_range = []
    for z in state["zombie_positions"]:
        dist = math.sqrt((z["x"] - agent_x)**2 + (z["y"] - agent_y)**2)
        if dist <= weapon["range"] * 20:  # scale range to 0-100 grid
            in_range.append((z, dist))

    if not in_range:
        return None

    # Shoot nearest
    target, dist = min(in_range, key=lambda x: x[1])
    # Accuracy decreases with distance (pistol at close range = ~90%) 
    hit_chance = max(0.5, 1.0 - dist / (weapon["range"] * 25))
    if random.random() > hit_chance:
        return f"Missed {target['type']} zombie at {dist:.0f}m!"

    target["hp"] -= damage
    if target["hp"] <= 0:
        state["zombies_alive"] = max(0, state.get("zombies_alive", 1) - 1)
        # Remove dead zombie from positions
        state["zombie_positions"] = [z for z in state["zombie_positions"] if z["hp"] > 0]
        return f"💀 Killed {target['type']} zombie! (-{damage} DMG)"
    return f"Hit {target['type']} zombie for {damage} damage! ({target['hp']}/{target['max_hp']} HP)"

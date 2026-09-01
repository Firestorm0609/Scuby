"""
eFootball DNA Build Optimizer v2.0 — Threshold-Aware, Synergy-Aware
====================================================================
Two-phase category-click optimizer that accounts for:
  - Stat animation thresholds (86 TP, 86 Balance, 90 Accel, etc.)
  - Hidden skill boosts (+20% Through Passing, +10% Long-Range Curler, etc.)
  - Touch Frequency optimization (the REAL dribbling feel)
  - Body type considerations (height/weight affect build effectiveness)
  - Booster stacking awareness (+3 booster system)
"""
from typing import Dict, Any, Optional, List
from knowledge_base import (
    STAT_THRESHOLDS, SKILL_HIDDEN_BOOSTS, TOUCH_FREQUENCY_RULES,
    POSITION_THRESHOLD_PRIORITY, SYNERGY_COMBOS, BODY_MODEL_STATS,
    calculate_jump_height, get_all_crossed_thresholds, get_next_threshold,
    find_applicable_synergies, get_body_typeAdvice,
)

# ═══════════════════════════════════════════════════════════════════════════
# DNA Categories — the full archetype engineering framework
# ═══════════════════════════════════════════════════════════════════════════

DNA_CATEGORIES: Dict[str, Dict] = {
    "athletic": {
        "label": "⚡ Athletic Engine",
        "desc": "Physical movement & athletic tuning",
        "upgrades": {
            "burst_accel": {
                "label": "Burst Acceleration",
                "desc": "Explosive first step — leave defenders behind in short bursts",
                "stats": {"acceleration": 1.0, "speed": 0.5, "balance": 0.3},
                "threshold_hint": "Aim for 90 Acceleration to unlock explosive animation",
            },
            "sprint_vel": {
                "label": "Sprint Velocity",
                "desc": "Top-end speed for outrunning the defensive line",
                "stats": {"speed": 1.0, "acceleration": 0.5, "stamina": 0.3},
                "threshold_hint": "90 Speed unlocks elite sprint animation",
            },
            "agile_turns": {
                "label": "Agile Turns",
                "desc": "Sharp direction changes that leave defenders flat-footed",
                "stats": {"balance": 1.0, "acceleration": 0.8, "dribbling": 0.4},
                "threshold_hint": "86 Balance unlocks smoother movement transitions",
            },
            "expl_leap": {
                "label": "Explosive Leap",
                "desc": "Vertical burst power for aerial dominance",
                "stats": {"jump": 1.0, "physicalContact": 0.5, "heading": 0.4},
            },
            "stamina_eng": {
                "label": "Stamina Engine",
                "desc": "Relentless 90-minute work rate that never drops",
                "stats": {"stamina": 1.0, "speed": 0.3, "acceleration": 0.2},
                "threshold_hint": "85 Stamina = press effectively for full match",
            },
            "press_endure": {
                "label": "Press Endurance",
                "desc": "Maintain high-intensity pressing throughout the full 90 minutes",
                "stats": {"stamina": 1.0, "aggression": 0.6, "speed": 0.3},
            },
            "cod": {
                "label": "Change of Direction",
                "desc": "Rapid 180° pivots — impossible to track in tight spaces",
                "stats": {"balance": 1.0, "dribbling": 0.8, "acceleration": 0.5},
                "threshold_hint": "86 Balance is critical for smooth 180° turns",
            },
        },
    },
    "ball": {
        "label": "🎮 Ball Mastery",
        "desc": "Control, dribbling & possession behavior",
        "upgrades": {
            "tight_drib": {
                "label": "Tight Space Dribbler",
                "desc": "Navigate congested areas with elite close control",
                "stats": {"tightPossession": 1.0, "dribbling": 0.9, "ballControl": 0.7, "balance": 0.4},
                "threshold_hint": "86 TP = faster turns. 90 TP = elite animation",
            },
            "press_resist": {
                "label": "Press Resistance",
                "desc": "Hold the ball under aggressive defensive pressure",
                "stats": {"tightPossession": 1.0, "physicalContact": 0.7, "ballControl": 0.6, "balance": 0.5},
                "threshold_hint": "75+ Physical Contact required for shielding while turning",
            },
            "one_touch": {
                "label": "One Touch Control",
                "desc": "Instant immaculate first touch under any condition",
                "stats": {"ballControl": 1.0, "tightPossession": 0.7, "lowPass": 0.4},
            },
            "wf_ctrl": {
                "label": "Weak Foot Control",
                "desc": "Dominant two-footed dribbling",
                "stats": {"dribbling": 1.0, "ballControl": 0.8, "balance": 0.5},
            },
            "shield": {
                "label": "Shielding Ability",
                "desc": "Body strength to protect possession",
                "stats": {"physicalContact": 1.0, "tightPossession": 0.8, "balance": 0.6},
                "threshold_hint": "75+ Physical Contact minimum for shielding while turning",
            },
            "chaos_drib": {
                "label": "Chaos Dribbler",
                "desc": "Street football flair that breaks defensive structure",
                "stats": {"dribbling": 1.0, "tightPossession": 0.8, "acceleration": 0.6, "balance": 0.5},
                "threshold_hint": "Touch Frequency = Tight Possession (low speed) + Dribbling (high speed)",
            },
        },
    },
    "finishing": {
        "label": "🎯 Finishing Lab",
        "desc": "Granular, customisable shooting upgrades",
        "upgrades": {
            "shot_pow": {
                "label": "Shot Power Boost",
                "desc": "Explosive strike power",
                "stats": {"kickingPower": 1.0, "physicalContact": 0.4, "finishing": 0.3},
            },
            "finesse": {
                "label": "Finesse Specialist",
                "desc": "Curl precision shots into the top corner",
                "stats": {"curl": 1.0, "finishing": 0.8, "ballControl": 0.4},
                "threshold_hint": "With Long-Range Curler skill, 90 Curl + 10% = effectively 99",
            },
            "first_time": {
                "label": "First Time Finisher",
                "desc": "Clinical one-touch finishes from crosses and through balls",
                "stats": {"finishing": 1.0, "offensiveAwareness": 0.7, "ballControl": 0.5},
                "threshold_hint": "85 Finishing = noticeably clinical. 90 = elite",
            },
            "wf_finish": {
                "label": "Weak Foot Finishing",
                "desc": "Increase weak foot finishing under pressure",
                "stats": {"finishing": 1.0, "curl": 0.6, "ballControl": 0.5},
            },
            "long_range": {
                "label": "Long Range Cannons",
                "desc": "Unstoppable outside-the-box threat",
                "stats": {"kickingPower": 1.0, "curl": 0.8, "finishing": 0.6},
                "threshold_hint": "With Long-Range Shooting: +10% Finishing outside box",
            },
            "composed": {
                "label": "Composed Finishing",
                "desc": "Ice-cold in 1v1s — never rushes, always picks the corner",
                "stats": {"finishing": 1.0, "offensiveAwareness": 0.8, "ballControl": 0.6},
            },
            "header": {
                "label": "Header Accuracy",
                "desc": "Dominant aerial finishing",
                "stats": {"heading": 1.0, "jump": 0.7, "physicalContact": 0.4},
            },
            "acrobatic": {
                "label": "Acrobatic Finishing",
                "desc": "Volleys, bicycle kicks and scissor finishes",
                "stats": {"finishing": 1.0, "balance": 0.7, "ballControl": 0.6},
            },
            "near_post": {
                "label": "Near Post Killer",
                "desc": "Deadly near-post runs and finishes",
                "stats": {"offensiveAwareness": 1.0, "finishing": 0.9, "acceleration": 0.5},
            },
            "free_kick": {
                "label": "Free Kick Precision",
                "desc": "Wall-beating dead ball specialist",
                "stats": {"setPieceTaking": 1.0, "curl": 0.9, "kickingPower": 0.4},
            },
        },
    },
    "iq": {
        "label": "🧠 Football IQ",
        "desc": "AI behavior & decision-making engineering",
        "upgrades": {
            "position": {
                "label": "Intelligent Positioning",
                "desc": "Always in the right place before the ball arrives",
                "stats": {"offensiveAwareness": 1.0, "defensiveAwareness": 0.5},
                "threshold_hint": "85 Off. Awareness = noticeably smarter runs",
            },
            "late_runs": {
                "label": "Late Box Runs",
                "desc": "Ghosting into the box at the perfect moment",
                "stats": {"offensiveAwareness": 1.0, "acceleration": 0.7, "stamina": 0.4},
            },
            "space_create": {
                "label": "Space Creator",
                "desc": "Off-ball movement that opens lanes",
                "stats": {"offensiveAwareness": 1.0, "acceleration": 0.6, "lowPass": 0.4},
            },
            "counter_read": {
                "label": "Counter Attack Reader",
                "desc": "First to react when possession switches",
                "stats": {"offensiveAwareness": 0.8, "speed": 0.8, "acceleration": 0.7},
            },
            "pass_vision": {
                "label": "Passing Vision",
                "desc": "Sees the killer pass before anyone else",
                "stats": {"loftedPass": 1.0, "lowPass": 0.9, "offensiveAwareness": 0.7},
                "threshold_hint": "With Through Passing: +20% to passing stats",
            },
            "tempo": {
                "label": "Tempo Controller",
                "desc": "Dictates the pace of play",
                "stats": {"lowPass": 1.0, "ballControl": 0.8, "tightPossession": 0.7},
            },
            "kill_pass": {
                "label": "Killer Through Balls",
                "desc": "Thread the needle between defenders",
                "stats": {"loftedPass": 1.0, "lowPass": 0.8, "offensiveAwareness": 0.7},
            },
            "tactical_disc": {
                "label": "Tactical Discipline",
                "desc": "Structure-first mentality",
                "stats": {"defensiveAwareness": 1.0, "offensiveAwareness": 0.6, "stamina": 0.5},
            },
        },
    },
    "mutation": {
        "label": "🚀 Playstyle Mutation",
        "desc": "Transform roles — changes behavior patterns",
        "upgrades": {
            "false_9": {
                "label": "False 9 Conversion",
                "desc": "Turn striker into a deep-dropping creator",
                "stats": {"offensiveAwareness": 1.0, "ballControl": 0.9, "lowPass": 0.9, "tightPossession": 0.8, "acceleration": 0.5},
                "mutation_note": "Drops between lines · Creates midfield overloads",
            },
            "inside_fwd": {
                "label": "Inside Forward",
                "desc": "Convert winger to inside cutter",
                "stats": {"dribbling": 1.0, "finishing": 0.9, "curl": 0.8, "ballControl": 0.7, "acceleration": 0.5},
                "mutation_note": "Cuts inside from wide · Bends shots across keeper",
            },
            "libero": {
                "label": "Libero CB",
                "desc": "Sweeper who starts attacks from deep",
                "stats": {"defensiveAwareness": 0.9, "lowPass": 1.0, "loftedPass": 0.8, "ballControl": 0.7, "speed": 0.5},
                "mutation_note": "Carries ball forward · Switches play",
            },
            "deep_play": {
                "label": "Deep Playmaker",
                "desc": "Transform CAM into deep-lying orchestrator",
                "stats": {"lowPass": 1.0, "loftedPass": 0.9, "tightPossession": 0.9, "ballControl": 0.8, "stamina": 0.5},
                "mutation_note": "Drops into midfield · Dictates the game's rhythm",
            },
            "press_str": {
                "label": "High Press Striker",
                "desc": "Relentless high-press with explosive acceleration",
                "stats": {"aggression": 1.0, "stamina": 0.9, "speed": 0.8, "acceleration": 0.8, "defensiveAwareness": 0.5},
                "mutation_note": "Hunts the ball high up · Forces defensive errors",
            },
            "inverted_fb": {
                "label": "Inverted Fullback",
                "desc": "Fullback drifts inside as third central midfielder",
                "stats": {"lowPass": 1.0, "ballControl": 0.9, "offensiveAwareness": 0.8, "tightPossession": 0.7, "acceleration": 0.4},
                "mutation_note": "Tucks into midfield · Creates central overloads",
            },
            "target_man": {
                "label": "Target Man",
                "desc": "Hold-up, link-play striker",
                "stats": {"physicalContact": 1.0, "heading": 0.9, "tightPossession": 0.8, "jump": 0.7, "ballControl": 0.6},
                "mutation_note": "Holds up play · Wins every aerial duel",
            },
            "box_crash": {
                "label": "Box Crashing Midfielder",
                "desc": "Late-arriving goal threat from midfield",
                "stats": {"offensiveAwareness": 1.0, "finishing": 0.9, "acceleration": 0.7, "stamina": 0.6, "ballControl": 0.5},
                "mutation_note": "Times late box runs perfectly · Scores from midfield",
            },
        },
    },
    "pressing": {
        "label": "🔥 Pressing & Intensity",
        "desc": "Aggression, pressing & defensive workrate",
        "upgrades": {
            "relentless": {
                "label": "Relentless Press",
                "desc": "Never gives the opponent time on the ball",
                "stats": {"aggression": 1.0, "stamina": 0.9, "speed": 0.6, "defensiveAwareness": 0.5},
            },
            "counter_press": {
                "label": "Counter Press Beast",
                "desc": "Instant ball recovery within 5 seconds",
                "stats": {"aggression": 1.0, "speed": 0.9, "acceleration": 0.8, "stamina": 0.6},
            },
            "intercept": {
                "label": "Interception Hunter",
                "desc": "Reads passing lanes before the ball arrives",
                "stats": {"defensiveAwareness": 1.0, "ballWinning": 0.8, "acceleration": 0.5},
                "threshold_hint": "With Interception skill: +10% def awareness when triggered",
            },
            "man_mark": {
                "label": "Man Mark Specialist",
                "desc": "Locks onto and neutralizes key opponents",
                "stats": {"defensiveAwareness": 1.0, "aggression": 0.7, "stamina": 0.7, "speed": 0.5},
            },
            "aggr_tackle": {
                "label": "Aggressive Tackler",
                "desc": "Dominates every 50/50 physical duel",
                "stats": {"ballWinning": 1.0, "trackingBack": 0.9, "aggression": 0.7, "physicalContact": 0.6},
            },
            "rec_sprint": {
                "label": "Recovery Sprinting",
                "desc": "Gets behind the ball faster than anyone",
                "stats": {"speed": 1.0, "acceleration": 0.9, "stamina": 0.6, "defensiveAwareness": 0.4},
            },
            "duel_mon": {
                "label": "Duel Monster",
                "desc": "Wins physical 1v1 ground battles",
                "stats": {"physicalContact": 1.0, "trackingBack": 0.9, "ballWinning": 0.8, "aggression": 0.6},
            },
        },
    },
    "wide": {
        "label": "🪽 Wide Threat",
        "desc": "Dynamic winger & wide player identities",
        "upgrades": {
            "touchline": {
                "label": "Touchline Sprinter",
                "desc": "Beats fullbacks with pure electric pace",
                "stats": {"speed": 1.0, "acceleration": 0.9, "stamina": 0.4},
            },
            "inv_cutter": {
                "label": "Inverted Cutter",
                "desc": "Cuts inside to devastate with the strong foot",
                "stats": {"dribbling": 1.0, "finishing": 0.8, "curl": 0.7, "acceleration": 0.6},
            },
            "cross_mach": {
                "label": "Crossing Machine",
                "desc": "Elite delivery from wide positions",
                "stats": {"loftedPass": 1.0, "curl": 0.8, "speed": 0.4},
                "threshold_hint": "Pinpoint Crossing: +10% to passing stats on crosses",
            },
            "one_v_one": {
                "label": "1v1 Destroyer",
                "desc": "Beats defenders consistently in wide areas",
                "stats": {"dribbling": 1.0, "tightPossession": 0.9, "acceleration": 0.7, "balance": 0.5},
            },
            "wide_play": {
                "label": "Wide Playmaker",
                "desc": "Creates from wide — switches the field",
                "stats": {"lowPass": 1.0, "loftedPass": 0.8, "ballControl": 0.7, "tightPossession": 0.6},
            },
            "early_cross": {
                "label": "Early Cross Specialist",
                "desc": "First-time delivery before the defense sets",
                "stats": {"loftedPass": 1.0, "curl": 0.7, "offensiveAwareness": 0.6, "speed": 0.4},
            },
        },
    },
    "defend": {
        "label": "🛡️ Defensive Core",
        "desc": "Modern, dynamic defender identities",
        "upgrades": {
            "bw_dest": {
                "label": "Ball Winning Destroyer",
                "desc": "Aggressive physical presence who dominates every duel",
                "stats": {"ballWinning": 1.0, "trackingBack": 0.9, "physicalContact": 0.7, "aggression": 0.6},
            },
            "sweeper": {
                "label": "Sweeper Defender",
                "desc": "Reads the game — covers space behind the line",
                "stats": {"defensiveAwareness": 1.0, "speed": 0.7, "acceleration": 0.6, "ballWinning": 0.4},
            },
            "build_up_cb": {
                "label": "Build Up Defender",
                "desc": "Comfortable on the ball — starts attacks",
                "stats": {"lowPass": 1.0, "ballControl": 0.8, "defensiveAwareness": 0.6, "loftedPass": 0.5},
            },
            "aerial_dom": {
                "label": "Aerial Dominance",
                "desc": "Wins every header at both ends",
                "stats": {"heading": 1.0, "jump": 1.0, "physicalContact": 0.7},
            },
            "last_man": {
                "label": "Last Man Specialist",
                "desc": "Ice-cool in 1v1s — holds shape",
                "stats": {"defensiveAwareness": 1.0, "trackingBack": 0.8, "speed": 0.6},
            },
            "front_foot": {
                "label": "Front Foot Defender",
                "desc": "Steps out aggressively to win the ball high",
                "stats": {"aggression": 1.0, "ballWinning": 0.9, "speed": 0.7, "defensiveAwareness": 0.6},
            },
            "tac_intercept": {
                "label": "Tactical Interceptor",
                "desc": "Kills attacks before they start",
                "stats": {"defensiveAwareness": 1.0, "ballWinning": 0.7, "lowPass": 0.4},
            },
        },
    },
    "signature": {
        "label": "⭐ Signature Builds",
        "desc": "Pre-engineered legendary player archetypes",
        "upgrades": {
            "prime_messi": {
                "label": "Prime Messi+",
                "desc": "Explosive burst · Tight dribbling · Outside box threat · Through balls",
                "stats": {
                    "acceleration": 0.9, "tightPossession": 1.0, "dribbling": 0.9,
                    "finishing": 0.8, "loftedPass": 0.8, "curl": 0.7, "balance": 0.6,
                },
                "mutation_note": "The complete attacker — dribbles, creates, finishes",
                "threshold_hint": "Targets: 86+ TP, 86+ Balance, 90+ Acceleration",
            },
            "haaland": {
                "label": "Haaland Monster",
                "desc": "Sprint velocity · Physical dominance · Header accuracy · Shot power",
                "stats": {
                    "speed": 1.0, "physicalContact": 0.9, "heading": 0.9,
                    "kickingPower": 1.0, "finishing": 0.9, "jump": 0.7, "offensiveAwareness": 0.8,
                },
                "mutation_note": "Pure unstoppable force — dominates physically",
            },
            "kante": {
                "label": "Kanté Engine",
                "desc": "Relentless press · Recovery sprint · Interception hunter · Stamina",
                "stats": {
                    "aggression": 1.0, "speed": 0.9, "ballWinning": 1.0,
                    "stamina": 0.9, "defensiveAwareness": 0.9, "acceleration": 0.7,
                },
                "mutation_note": "Covers every blade of grass — the engine that never stops",
            },
            "tiki_maestro": {
                "label": "Tiki-Taka Maestro",
                "desc": "Ball control · Short passing · Tight possession · Tempo control",
                "stats": {
                    "ballControl": 1.0, "lowPass": 1.0, "tightPossession": 1.0,
                    "offensiveAwareness": 0.7, "stamina": 0.5, "curl": 0.4,
                },
                "mutation_note": "The heartbeat of possession — never loses the ball",
                "threshold_hint": "Targets: 86+ TP, 86+ Balance for maximum close control",
            },
            "wing_dest": {
                "label": "Wing Destroyer",
                "desc": "Pace · Dribbling · Cutting inside · Finishing · Crossing",
                "stats": {
                    "speed": 0.9, "acceleration": 0.9, "dribbling": 1.0,
                    "finishing": 0.8, "loftedPass": 0.7, "curl": 0.6, "balance": 0.6,
                },
                "mutation_note": "Terrorizes fullbacks from wide",
            },
        },
    },
}

# ═══════════════════════════════════════════════════════════════════════════
# Stat metadata
# ═══════════════════════════════════════════════════════════════════════════

TRAINABLE_STATS = [
    "offensiveAwareness", "ballControl", "dribbling", "tightPossession",
    "lowPass", "loftedPass", "finishing", "setPieceTaking", "curl",
    "heading", "defensiveAwareness", "ballWinning", "trackingBack",
    "aggression", "kickingPower", "speed", "acceleration", "balance",
    "physicalContact", "jump", "stamina",
]

GK_STATS = ["gkAwareness", "gkCatching", "gkClearing", "gkReflexes", "gkReach"]

STAT_LABELS: Dict[str, str] = {
    "offensiveAwareness":  "Offensive Awareness",
    "ballControl":         "Ball Control",
    "dribbling":           "Dribbling",
    "tightPossession":     "Tight Possession",
    "lowPass":             "Low Pass",
    "loftedPass":          "Lofted Pass",
    "finishing":           "Finishing",
    "setPieceTaking":      "Set Piece Taking",
    "curl":                "Curl",
    "heading":             "Heading",
    "defensiveAwareness":  "Def. Awareness",
    "ballWinning":         "Tackling",
    "trackingBack":        "Tracking Back",
    "aggression":          "Aggression",
    "kickingPower":        "Kicking Power",
    "speed":               "Speed",
    "acceleration":        "Acceleration",
    "balance":             "Balance",
    "physicalContact":     "Physical Contact",
    "jump":                "Jump",
    "stamina":             "Stamina",
    "gkAwareness":         "GK Awareness",
    "gkCatching":          "GK Catching",
    "gkClearing":          "GK Deflecting",
    "gkReflexes":          "GK Reflexes",
    "gkReach":             "GK Reach",
}

# ═══════════════════════════════════════════════════════════════════════════
# Real eFootball training categories
# ═══════════════════════════════════════════════════════════════════════════

TRAINING_CATEGORIES: Dict[str, Dict] = {
    "cat_finish": {
        "label": "Finishing, Set Pieces, Curl",
        "stats": ["finishing", "setPieceTaking", "curl"],
    },
    "cat_pass": {
        "label": "Low Pass, Lofted Pass",
        "stats": ["lowPass", "loftedPass"],
    },
    "cat_drib": {
        "label": "Dribbling, Ball Control, Tight Possession",
        "stats": ["dribbling", "ballControl", "tightPossession"],
    },
    "cat_aware": {
        "label": "Offensive Awareness, Acceleration, Balance",
        "stats": ["offensiveAwareness", "acceleration", "balance"],
    },
    "cat_power": {
        "label": "Kicking Power, Speed, Stamina",
        "stats": ["kickingPower", "speed", "stamina"],
    },
    "cat_aerial": {
        "label": "Heading, Jumping, Physical Contact",
        "stats": ["heading", "jump", "physicalContact"],
    },
    "cat_defend": {
        "label": "Defensive Awareness, Tackling, Aggression, Tracking Back",
        "stats": ["defensiveAwareness", "ballWinning", "aggression", "trackingBack"],
    },
    "cat_gk1": {
        "label": "GK Awareness, Jumping",
        "stats": ["gkAwareness", "jump"],
    },
    "cat_gk2": {
        "label": "GK Deflecting, GK Reach",
        "stats": ["gkClearing", "gkReach"],
    },
    "cat_gk3": {
        "label": "GK Catching, GK Reflexes",
        "stats": ["gkCatching", "gkReflexes"],
    },
}

# ═══════════════════════════════════════════════════════════════════════════
# Position weights for Phase 2
# ═══════════════════════════════════════════════════════════════════════════

POSITION_WEIGHTS: Dict[str, Dict[str, float]] = {
    "GK":   {s: 1.0 for s in GK_STATS},
    "CB":   {"defensiveAwareness": 1.0, "ballWinning": 0.9, "trackingBack": 0.9,
              "physicalContact": 0.8, "speed": 0.7, "acceleration": 0.6, "jump": 0.6,
              "heading": 0.5, "balance": 0.4, "stamina": 0.3},
    "LB":   {"defensiveAwareness": 1.0, "ballWinning": 0.9, "speed": 0.9,
              "acceleration": 0.8, "trackingBack": 0.8, "stamina": 0.7,
              "physicalContact": 0.6, "balance": 0.5, "ballControl": 0.4},
    "RB":   {"defensiveAwareness": 1.0, "ballWinning": 0.9, "speed": 0.9,
              "acceleration": 0.8, "trackingBack": 0.8, "stamina": 0.7,
              "physicalContact": 0.6, "balance": 0.5, "ballControl": 0.4},
    "LWB":  {"defensiveAwareness": 1.0, "speed": 0.9, "stamina": 0.9,
              "acceleration": 0.8, "ballWinning": 0.8, "trackingBack": 0.7,
              "ballControl": 0.5, "dribbling": 0.4},
    "RWB":  {"defensiveAwareness": 1.0, "speed": 0.9, "stamina": 0.9,
              "acceleration": 0.8, "ballWinning": 0.8, "trackingBack": 0.7,
              "ballControl": 0.5, "dribbling": 0.4},
    "DMF":  {"defensiveAwareness": 1.0, "ballWinning": 0.9, "trackingBack": 0.9,
              "lowPass": 0.8, "loftedPass": 0.7, "physicalContact": 0.7,
              "ballControl": 0.6, "stamina": 0.6, "acceleration": 0.5},
    "CMF":  {"stamina": 1.0, "ballControl": 0.9, "lowPass": 0.9,
              "loftedPass": 0.8, "defensiveAwareness": 0.7, "offensiveAwareness": 0.7,
              "dribbling": 0.6, "balance": 0.5, "acceleration": 0.5},
    "AMF":  {"offensiveAwareness": 1.0, "ballControl": 0.9, "dribbling": 0.9,
              "tightPossession": 0.8, "loftedPass": 0.7, "lowPass": 0.7,
              "finishing": 0.6, "acceleration": 0.6, "balance": 0.5},
    "LMF":  {"speed": 1.0, "stamina": 0.9, "offensiveAwareness": 0.8,
              "dribbling": 0.8, "ballControl": 0.7, "tightPossession": 0.7,
              "loftedPass": 0.6, "acceleration": 0.6, "balance": 0.5},
    "RMF":  {"speed": 1.0, "stamina": 0.9, "offensiveAwareness": 0.8,
              "dribbling": 0.8, "ballControl": 0.7, "tightPossession": 0.7,
              "loftedPass": 0.6, "acceleration": 0.6, "balance": 0.5},
    "LWF":  {"dribbling": 1.0, "speed": 0.9, "acceleration": 0.9,
              "offensiveAwareness": 0.8, "finishing": 0.7, "ballControl": 0.7,
              "tightPossession": 0.6, "balance": 0.5, "stamina": 0.4},
    "RWF":  {"dribbling": 1.0, "speed": 0.9, "acceleration": 0.9,
              "offensiveAwareness": 0.8, "finishing": 0.7, "ballControl": 0.7,
              "tightPossession": 0.6, "balance": 0.5, "stamina": 0.4},
    "SS":   {"offensiveAwareness": 1.0, "finishing": 0.9, "ballControl": 0.8,
              "dribbling": 0.8, "tightPossession": 0.7, "speed": 0.7,
              "acceleration": 0.6, "balance": 0.5, "physicalContact": 0.4},
    "CF":   {"finishing": 1.0, "offensiveAwareness": 0.9, "speed": 0.8,
              "acceleration": 0.7, "ballControl": 0.7, "dribbling": 0.7,
              "physicalContact": 0.6, "stamina": 0.5, "balance": 0.5},
}


# ═══════════════════════════════════════════════════════════════════════════
# Optimizer
# ═══════════════════════════════════════════════════════════════════════════

# ---------------------------------------------------------------------------
# PP Cost Model — CONFIGURABLE
# ---------------------------------------------------------------------------
# eFootball's exact cost structure is not officially documented.
# This model assumes escalating costs per category click.
# Update COST_TABLE if the actual game uses a different formula.
#
# COST_TABLE[i] = cost of the (i+1)th click on the same category.
# Default: 1,1,1,1, 2,2,2,2, 3,3,3,3, 4,4,4,4, ... (escalates every 4 clicks)
# Alt 1:  Flat — every click costs 1 PP
# Alt 2:  Linear — 1,2,3,4,5,6,7,8,9,10,...
# Alt 3:  Quadratic — 1,2,4,7,11,16,...
# ---------------------------------------------------------------------------
COST_TABLE = [
    1, 1, 1, 1,   # clicks 1-4: 1 PP each
    2, 2, 2, 2,   # clicks 5-8: 2 PP each
    3, 3, 3, 3,   # clicks 9-12: 3 PP each
    4, 4, 4, 4,   # clicks 13-16: 4 PP each
    5, 5, 5, 5,   # clicks 17-20: 5 PP each
    6, 6, 6, 6,   # clicks 21-24: 6 PP each
    7, 7, 7, 7,   # clicks 25-28: 7 PP each
    8, 8, 8, 8,   # clicks 29-32: 8 PP each
]


def get_click_cost(clicks_already: int) -> int:
    """
    Return the PP cost of the next click on a category.
    Uses COST_TABLE; extrapolates beyond table with linear formula.
    """
    if clicks_already < len(COST_TABLE):
        return COST_TABLE[clicks_already]
    # Extrapolate: cost = (clicks_already // 4) + 1
    return (clicks_already // 4) + 1


def _threshold_bonus(stat: str, current_value: int, gain: int, position: str) -> float:
    """
    Calculate bonus priority for crossing a known stat threshold.
    Returns a multiplier: >1.0 means this gain is extra valuable.
    """
    new_value = current_value + gain
    priority_stats = POSITION_THRESHOLD_PRIORITY.get(position, [])

    for threshold_info in STAT_THRESHOLDS.get(stat, []):
        threshold = threshold_info["value"]
        # Crossing the threshold
        if current_value < threshold <= new_value:
            importance = threshold_info.get("importance", "medium")
            pos_match = stat in priority_stats[:3]  # Top 3 priority stats
            multiplier = 3.0 if importance == "critical" and pos_match else 2.0 if importance == "critical" else 1.5
            return multiplier
    return 1.0


def _synergy_bonus(stat: str, player_skills: List[str], upgrade_weights: Dict[str, float]) -> float:
    """
    Calculate bonus for stats that synergize with player's skills.
    E.g., if player has Long-Range Curler, don't over-invest in Curl (skill gives +10%).
    """
    bonus = 1.0

    # Long-Range Curler: +10% to Curl and Finishing — don't max Curl, invest elsewhere
    if stat == "curl" and "longRangeDrive" in player_skills:
        bonus *= 0.7  # Reduce priority — skill handles it

    # Through Passing: +20% to passing — don't max passing stats
    if stat in ("lowPass", "loftedPass") and "throughPassing" in player_skills:
        bonus *= 0.7

    # Pinpoint Crossing: +10% to lofted pass
    if stat == "loftedPass" and "pinpointCrossing" in player_skills:
        bonus *= 0.8

    # Super-sub: +5% to Off. Awareness and Finishing
    if stat in ("offensiveAwareness", "finishing") and "superSub" in player_skills:
        bonus *= 0.85

    # Interception/Blocker: +10% def awareness when triggered
    if stat == "defensiveAwareness" and ("interception" in player_skills or "blocker" in player_skills):
        bonus *= 0.8

    # Fortress: +5% def stats when leading
    if stat in ("trackingBack", "ballWinning", "aggression") and "fortress" in player_skills:
        bonus *= 0.85

    return bonus


def optimize_dna(
    player_data: Dict[str, Any],
    cat_key: str,
    upg_key: str,
) -> Dict[str, Any]:
    """
    Threshold-aware, synergy-aware DNA build optimizer.

    Phase 1: Max out the selected upgrade's stats with threshold awareness.
    Phase 2: Spend remaining PP on position-relevant stats.
    """
    base_stats = player_data.get("baseStats", {})
    level_cap  = player_data.get("levelCap", 34)
    position   = player_data.get("position", "")
    skills     = player_data.get("skills", [])
    height     = player_data.get("height", 175)
    weight     = player_data.get("weight", 70)

    cat     = DNA_CATEGORIES.get(cat_key, {})
    upgrade = cat.get("upgrades", {}).get(upg_key, {})

    total_budget = max(1, (level_cap - 1) * 2)
    weights      = upgrade.get("stats", {})

    # Position weights for Phase 2
    pos_weights = POSITION_WEIGHTS.get(position, {})
    if not pos_weights:
        if position in ("CF", "SS", "LWF", "RWF"):
            pos_weights = POSITION_WEIGHTS["CF"]
        elif position in ("CB", "LB", "RB"):
            pos_weights = POSITION_WEIGHTS["CB"]
        elif position in ("DMF", "CMF", "AMF"):
            pos_weights = POSITION_WEIGHTS["CMF"]
        else:
            pos_weights = {s: 1.0 for s in TRAINABLE_STATS}

    # ------------------------------------------------------------------
    # Phase 1: Identify categories relevant to the selected upgrade
    # ------------------------------------------------------------------
    relevant: Dict[str, Dict[str, float]] = {}
    for gcat_id, gcat in TRAINING_CATEGORIES.items():
        w_in_cat = {s: weights[s] for s in gcat["stats"] if weights.get(s, 0) > 0}
        if w_in_cat:
            relevant[gcat_id] = w_in_cat

    # Shared state
    clicks: Dict[str, int] = {gcat: 0 for gcat in TRAINING_CATEGORIES}
    stat_gains: Dict[str, int] = {s: 0 for s in TRAINABLE_STATS + GK_STATS}
    budget_remaining = total_budget

    # ------------------------------------------------------------------
    # Phase 1: Hammer the upgrade's relevant categories (threshold-aware)
    # ------------------------------------------------------------------
    while True:
        best_gcat: Optional[str] = None
        best_score: float = -1.0

        for gcat_id, w_stats in relevant.items():
            cost = get_click_cost(clicks[gcat_id])
            if cost > budget_remaining:
                continue

            benefit = 0.0
            for s, w in w_stats.items():
                current = base_stats.get(s, 0) + stat_gains.get(s, 0)
                if current >= 99:
                    continue
                # Base weight
                score = w
                # Threshold bonus
                score *= _threshold_bonus(s, current, 1, position)
                # Synergy adjustment
                score *= _synergy_bonus(s, skills, weights)
                benefit += score

            if benefit <= 0:
                continue
            score = benefit / cost
            if score > best_score:
                best_score = score
                best_gcat = gcat_id

        if best_gcat is None:
            break

        cost = get_click_cost(clicks[best_gcat])
        clicks[best_gcat] += 1
        budget_remaining -= cost
        for s in TRAINING_CATEGORIES[best_gcat]["stats"]:
            if base_stats.get(s, 0) + stat_gains.get(s, 0) < 99:
                stat_gains[s] += 1

    # ------------------------------------------------------------------
    # Phase 2: Spend remaining PP on position-relevant stats
    # ------------------------------------------------------------------
    while budget_remaining > 0:
        best_gcat: Optional[str] = None
        best_score: float = -1.0

        for gcat_id, gcat in TRAINING_CATEGORIES.items():
            cost = get_click_cost(clicks[gcat_id])
            if cost > budget_remaining:
                continue

            benefit = 0.0
            for s in gcat["stats"]:
                current = base_stats.get(s, 0) + stat_gains.get(s, 0)
                if current >= 99:
                    continue
                pw = pos_weights.get(s, 0)
                if pw <= 0:
                    continue
                score = pw
                score *= _threshold_bonus(s, current, 1, position)
                score *= _synergy_bonus(s, skills, pos_weights)
                benefit += score

            if benefit <= 0:
                continue
            score = benefit / cost
            if score > best_score:
                best_score = score
                best_gcat = gcat_id

        if best_gcat is None:
            break

        cost = get_click_cost(clicks[best_gcat])
        clicks[best_gcat] += 1
        budget_remaining -= cost
        for s in TRAINING_CATEGORIES[best_gcat]["stats"]:
            if base_stats.get(s, 0) + stat_gains.get(s, 0) < 99:
                stat_gains[s] += 1

    # ------------------------------------------------------------------
    # Build results
    # ------------------------------------------------------------------
    allocations = {s: stat_gains[s] for s in weights if stat_gains.get(s, 0) > 0}
    phase2_gains = {
        s: stat_gains[s]
        for s in stat_gains
        if s not in weights and stat_gains.get(s, 0) > 0 and pos_weights.get(s, 0) > 0
    }
    bonus_gains = {
        s: stat_gains[s]
        for s in stat_gains
        if s not in weights and s not in pos_weights and stat_gains.get(s, 0) > 0
    }

    final_stats: Dict[str, int] = {}
    for s in TRAINABLE_STATS + GK_STATS:
        final_stats[s] = base_stats.get(s, 0) + stat_gains.get(s, 0)

    points_used = total_budget - budget_remaining

    # --- Threshold analysis ---
    crossed_thresholds = get_all_crossed_thresholds(final_stats)
    next_thresholds = []
    for stat in list(weights.keys()) + list(pos_weights.keys()):
        current = final_stats.get(stat, 0)
        nt = get_next_threshold(stat, current)
        if nt and current < 99:
            next_thresholds.append({"stat": stat, "current": current, **nt})

    # --- Synergy analysis ---
    synergies = find_applicable_synergies(skills, final_stats)

    # --- Body type advice ---
    body_advice = get_body_typeAdvice(height, weight, position)

    # --- Jump height ---
    jump_height = calculate_jump_height(height, final_stats.get("jump", 40))

    return {
        "cat_key":          cat_key,
        "upg_key":          upg_key,
        "cat_label":        cat.get("label", ""),
        "upg_label":        upgrade.get("label", ""),
        "upg_desc":         upgrade.get("desc", ""),
        "mutation_note":    upgrade.get("mutation_note"),
        "threshold_hint":   upgrade.get("threshold_hint"),
        "allocations":      allocations,
        "phase2_gains":     phase2_gains,
        "bonus_gains":      bonus_gains,
        "base_stats":       base_stats,
        "final_stats":      final_stats,
        "clicks":           {k: v for k, v in clicks.items() if v > 0},
        "points_used":      points_used,
        "points_remaining": budget_remaining,
        "level_cap":        level_cap,
        "budget":           total_budget,
        "player_name":      player_data.get("name", "Unknown"),
        "position":         player_data.get("position", ""),
        "overall":          player_data.get("overall", 0),
        "team":             player_data.get("team", ""),
        "age":              player_data.get("age"),
        "height":           height,
        "weight":           weight,
        "preferred_foot":   player_data.get("preferredFoot", ""),
        "weak_foot_accuracy":   player_data.get("weakFootAccuracy"),
        "weak_foot_usage":      player_data.get("weakFootUsage"),
        "skills":           skills,
        "com_skills":       player_data.get("comSkills", []),
        "additional_positions": player_data.get("additionalPositions", []),
        "base_stats_raw":   player_data.get("baseStats", {}),
        # New: hidden mechanics data
        "crossed_thresholds": crossed_thresholds,
        "next_thresholds":    next_thresholds,
        "synergies":          synergies,
        "body_advice":        body_advice,
        "jump_height":        round(jump_height, 1),
        "player_model":       {k: player_data.get(k) for k in BODY_MODEL_STATS if player_data.get(k)},
    }


def _pp_bar(used: int, budget: int, width: int = 14) -> str:
    """Return a filled/empty Unicode block progress bar."""
    if budget <= 0:
        return "░" * width
    filled = round(used / budget * width)
    filled = max(0, min(width, filled))
    return "█" * filled + "░" * (width - filled)


def format_dna_result(result: Dict[str, Any]) -> str:
    """
    Format DNA build result for Telegram Markdown — premium HUD aesthetic.
    Now includes threshold callouts, synergy notes, and hidden stat insights.
    """
    CAT_ICONS = {
        "cat_finish": "🎯", "cat_pass": "⚽", "cat_drib": "🔺",
        "cat_aware": "🔀", "cat_power": "👟", "cat_aerial": "⏫",
        "cat_defend": "🛡️", "cat_gk1": "🧤", "cat_gk2": "🧤", "cat_gk3": "🧤",
    }

    name = result.get("player_name", "")
    pos  = result.get("position", "")
    ovr  = result.get("overall", 0)

    # ── Header ────────────────────────────────────────────────────────
    lines = [
        f"🧬 *{result['upg_label']}*",
        f"_{result['upg_desc']}_",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    # ── Player identity ───────────────────────────────────────────────
    identity = f"◈  *{name}*"
    if pos:  identity += f"  ·  {pos}"
    if ovr:  identity += f"  ·  {ovr} OVR"
    lines.append(identity)

    add_pos = result.get("additional_positions", [])
    if add_pos:
        fam_stars = {1: "★", 2: "★★", 3: "★★★"}
        pos_parts = [
            f"{p['position']} {fam_stars.get(p.get('familiarity', 1), '')}"
            for p in add_pos if isinstance(p, dict)
        ]
        if pos_parts:
            lines.append(f"   Also: {' · '.join(pos_parts)}")

    team = result.get("team", "")
    if team:
        lines.append(f"🏟  {team}")

    foot_parts = []
    pfoot = result.get("preferred_foot", "")
    if pfoot: foot_parts.append(f"{pfoot} foot")
    wfa = result.get("weak_foot_accuracy")
    wfu = result.get("weak_foot_usage")
    if wfa is not None: foot_parts.append(f"Weak acc {'★' * wfa}{'☆' * (4 - wfa)}")
    if wfu is not None: foot_parts.append(f"Usage {'★' * wfu}{'☆' * (4 - wfu)}")
    if foot_parts:
        lines.append("👟  " + "  ·  ".join(foot_parts))

    skills = result.get("skills", [])
    if skills:
        lines.append(f"⚙️  {' · '.join(skills[:8])}{' +' + str(len(skills) - 8) if len(skills) > 8 else ''}")
    com_skills = result.get("com_skills", [])
    if com_skills:
        lines.append(f"🎮  COM: {' · '.join(com_skills)}")

    # ── Body type insight ─────────────────────────────────────────────
    body_advice = result.get("body_advice", "")
    if body_advice:
        lines.append(f"📐  {body_advice}")

    jump_h = result.get("jump_height")
    if jump_h:
        lines.append(f"Jump height: {jump_h}cm")

    # ── Build identity ────────────────────────────────────────────────
    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"{result['cat_label']}  ·  👑 GOAT DNA",
        f"*{result['budget']} PP*  (Level {result['level_cap']})",
        "",
    ]

    # ── Threshold callout ─────────────────────────────────────────────
    threshold_hint = result.get("threshold_hint")
    if threshold_hint:
        lines.append(f"🎯 _{threshold_hint}_")
        lines.append("")

    # ── Training plan ─────────────────────────────────────────────────
    all_gains: Dict[str, int] = {}
    for d in (result.get("allocations", {}),
              result.get("phase2_gains", {}),
              result.get("bonus_gains", {})):
        for k, v in d.items():
            all_gains[k] = all_gains.get(k, 0) + v

    clicks      = result.get("clicks", {})
    base_stats  = result.get("base_stats", {})
    final_stats = result.get("final_stats", {})

    active_cats   = {cid: n for cid, n in clicks.items() if n > 0}
    inactive_cats = [cid for cid in TRAINING_CATEGORIES if cid not in active_cats]

    if active_cats:
        lines.append("📋 *TRAINING PLAN*")
        lines.append("─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─")

        for cat_id, n_clicks in active_cats.items():
            gcat    = TRAINING_CATEGORIES[cat_id]
            icon    = CAT_ICONS.get(cat_id, "●")
            pp_cost = sum(get_click_cost(i) for i in range(n_clicks))
            stat_keys = gcat["stats"]

            lines.append("")
            lines.append(f"{icon}  {n_clicks} click{'s' if n_clicks != 1 else ''}  ·  `{pp_cost} PP`")

            for sk in stat_keys:
                label = STAT_LABELS.get(sk, sk)
                base  = base_stats.get(sk, 0)
                final = final_stats.get(sk, base)
                gain  = final - base

                label_pad = f"{label:<20}"
                base_pad  = f"{base:>2}"
                final_pad = f"{final:>2}"

                # Check if this stat crossed a threshold
                threshold_marker = ""
                for t in STAT_THRESHOLDS.get(sk, []):
                    if base < t["value"] <= final:
                        threshold_marker = " 🔓"

                if gain > 0:
                    lines.append(f"`{label_pad}  {base_pad} → {final_pad}  +{gain}{threshold_marker}`")
                else:
                    lines.append(f"`{label_pad}  {base_pad} → {final_pad}`")

    # ── Threshold summary ─────────────────────────────────────────────
    crossed = result.get("crossed_thresholds", [])
    if crossed:
        lines.append("")
        lines.append("🔓 *THRESHOLDS UNLOCKED*")
        for t in crossed:
            lines.append(f"  {STAT_LABELS.get(t['stat'], t['stat'])} {t['value']} — {t['effect']}")

    # ── Synergy notes ─────────────────────────────────────────────────
    synergies = result.get("synergies", [])
    active_synergies = [s for s in synergies if s.get("fully_met")]
    if active_synergies:
        lines.append("")
        lines.append("⚡ *SKILL SYNERGIES ACTIVE*")
        for s in active_synergies:
            lines.append(f"  {s['name']}")
            lines.append(f"  _{s['build_tip']}_")

    # ── PP summary ────────────────────────────────────────────────────
    used      = result["points_used"]
    budget    = result["budget"]
    remaining = result["points_remaining"]
    pct       = int(used / budget * 100) if budget else 0
    bar       = _pp_bar(used, budget)

    lines += [
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"`{bar}  {pct}%`",
        f"⚡ *{used}* / {budget} PP  ·  {remaining} remaining",
    ]

    # ── Unused categories (compact) ───────────────────────────────────
    if inactive_cats:
        pills = []
        for cid in inactive_cats:
            icon = CAT_ICONS.get(cid, "●")
            pills.append(f"{icon} ‹0›")
        lines.append("")
        lines.append("Unused: " + "  ·  ".join(pills))

    # ── Mutation note ─────────────────────────────────────────────────
    if result.get("mutation_note"):
        lines += ["", f"💡 _{result['mutation_note']}_"]

    return "\n".join(lines)

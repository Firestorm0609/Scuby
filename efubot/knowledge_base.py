"""
eFootball Hidden Mechanics Knowledge Base
==========================================
All undocumented game mechanics, stat thresholds, skill synergies,
body model formulas, and animation triggers.

Sources: Zhuhai Amadeusz testing, eFootballLab, community research.
Last updated: eFootball 2027 (v6.0.0)
"""
from typing import Dict, List, Tuple, Optional, Any

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1: STAT THRESHOLDS — Animation Triggers
# ═══════════════════════════════════════════════════════════════════════════
# Crossing these specific values unlocks entirely different animations.
# The game NEVER tells you these exist.

STAT_THRESHOLDS: Dict[str, List[Dict[str, Any]]] = {
    "tightPossession": [
        {"value": 86, "effect": "Unlocks quicker 180° turning animation",
         "importance": "critical", "positions": ["SS", "AMF", "CMF", "LWF", "RWF", "CF"]},
        {"value": 90, "effect": "Unlocks elite close-control dribbling animation",
         "importance": "high", "positions": ["SS", "AMF", "LWF", "RWF"]},
    ],
    "balance": [
        {"value": 86, "effect": "Unlocks smoother movement transitions",
         "importance": "critical", "positions": ["SS", "AMF", "CMF", "LWF", "RWF", "CF"]},
    ],
    "acceleration": [
        {"value": 90, "effect": "Unlocks explosive first-step animation",
         "importance": "high", "positions": ["LWF", "RWF", "SS", "CF", "LMF", "RMF"]},
    ],
    "speed": [
        {"value": 90, "effect": "Unlocks top-tier sprint animation",
         "importance": "medium", "positions": ["LWF", "RWF", "LMF", "RMF", "CF"]},
    ],
    "finishing": [
        {"value": 85, "effect": "Noticeably more clinical finishing",
         "importance": "high", "positions": ["CF", "SS", "LWF", "RWF", "AMF"]},
        {"value": 90, "effect": "Elite finishing — minimal shot error",
         "importance": "critical", "positions": ["CF", "SS"]},
    ],
    "offensiveAwareness": [
        {"value": 85, "effect": "Smarter off-ball runs",
         "importance": "high", "positions": ["CF", "SS", "AMF"]},
    ],
    "defensiveAwareness": [
        {"value": 85, "effect": "Noticeably better defensive positioning",
         "importance": "high", "positions": ["CB", "DMF", "LB", "RB"]},
    ],
    "ballWinning": [
        {"value": 85, "effect": "More successful tackles",
         "importance": "high", "positions": ["CB", "DMF", "LB", "RB"]},
    ],
    "jump": [
        {"value": 85, "effect": "Dominant in aerial duels (with decent height)",
         "importance": "medium", "positions": ["CB", "CF"]},
    ],
    "physicalContact": [
        {"value": 75, "effect": "Minimum for shielding while turning",
         "importance": "critical", "positions": ["CF", "SS", "CMF", "DMF"]},
        {"value": 85, "effect": "Dominant shielder — almost impossible to dispossess",
         "importance": "high", "positions": ["CF", "DMF"]},
    ],
    "kickingPower": [
        {"value": 85, "effect": "Noticeably faster shots",
         "importance": "medium", "positions": ["CF", "SS", "AMF"]},
    ],
    "stamina": [
        {"value": 85, "effect": "Can press effectively for 90 minutes",
         "importance": "high", "positions": ["CMF", "DMF", "LMF", "RMF", "LB", "RB"]},
    ],
}

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2: SKILL HIDDEN BOOSTS
# ═══════════════════════════════════════════════════════════════════════════
# Skills don't just "add animations" — many secretly boost stats by %.
# Type 3 skills directly increase stats. Stats round UP (82×1.2=98.4→99).
# Type 3 skills that bypass 99 cap are marked.

SKILL_HIDDEN_BOOSTS: Dict[str, Dict[str, Any]] = {
    # === TYPE 3: Direct stat boosts ===
    "throughPassing": {
        "boosts": {"lowPass": 0.20, "loftedPass": 0.20},
        "condition": "When performing through pass",
        "bypass_cap": False,
    },
    "pinpointCrossing": {
        "boosts": {"loftedPass": 0.10},
        "condition": "When performing cross",
        "bypass_cap": False,
    },
    "longRangeShooting": {
        "boosts": {"finishing": 0.10},
        "condition": "Outside the box, normal and stunning shots only (NOT curl)",
        "bypass_cap": False,
    },
    "longRangeDrive": {
        # Long-Range Curler — the ONLY skill that bypasses the 99 cap
        "boosts": {"finishing": 0.10, "kickingPower": 0.10, "curl": 0.10},
        "condition": "Controlled (curl) shots from outside the box",
        "bypass_cap": True,  # CRITICAL: can push stats beyond 99
    },
    "penaltySpecialist": {
        "boosts": {"finishing": 10, "setPieceTaking": 10},
        "condition": "When taking a penalty",
        "bypass_cap": False,
        "flat_boost": True,  # Flat +10, not percentage
    },
    "superSub": {
        "boosts": {"offensiveAwareness": 0.05, "finishing": 0.05},
        "condition": "Subbed on during or after half-time",
        "bypass_cap": False,
    },
    "fortress": {
        "boosts": {"defensiveEngagement": 0.05, "ballWinning": 0.05, "aggression": 0.05},
        "condition": "When leading at half-time",
        "bypass_cap": False,
    },
    "interception": {
        "boosts": {"defensiveAwareness": 0.10},
        "condition": "When performing interception",
        "bypass_cap": False,
    },
    "blocker": {
        "boosts": {"defensiveAwareness": 0.10},
        "condition": "When performing a block",
        "bypass_cap": False,
    },
    "slidingTackle": {
        "boosts": {"ballWinning": 0.10},
        "condition": "When performing sliding tackle",
        "bypass_cap": False,
    },
    "gameChangelngPass": {
        "boosts": {"lowPass": 0.10, "loftedPass": 0.10},
        "condition": "When losing or drawing at half-time",
        "bypass_cap": False,
    },

    # === TYPE 1: Animation changes ===
    "doubleTouch": {
        "effect": "Changes dribbling animation; RECOVERS BALANCE after execution",
        "synergy": "Balance recovery → next shot/pass more accurate ('DT Boom')",
        "balance_recovery": True,
    },
    "scissorsFeint": {"effect": "Changes dribbling animation"},
    "flipFlap": {"effect": "Changes dribbling animation"},
    "marseilleTurn": {"effect": "Changes turning animation"},
    "sombrero": {"effect": "Changes flick animation"},
    "chopTurn": {"effect": "Changes turning animation"},
    "cutBehindTurn": {"effect": "Changes turning animation"},
    "scotchMove": {"effect": "Changes turning animation"},
    "soleControl": {"effect": "Changes ball control animation; enables knock-on shots on console"},
    "outsideCurler": {"effect": "Changes shot trajectory animation"},
    "rabona": {"effect": "Changes crossing/shooting animation"},
    "acrobaticFinishing": {"effect": "Changes finishing animation (volleys, bicycle kicks)"},
    "heelTrick": {"effect": "Changes pass/shot animation"},

    # === TYPE 2: Parameter changes ===
    "momentumDribbling": {
        "effect": "Increases touch frequency beyond stat limit (nerfed after v2.6.0)",
        "touch_frequency_boost": True,
    },
    "heading": {
        "effect": "Increases frequency of downward headers (more accurate)",
    },
    "blitzCurler": {"effect": "Changes curl shot trajectory"},
    "chipShotControl": {"effect": "Changes chip shot trajectory"},
    "knuckleShot": {"effect": "Changes shot trajectory"},
    "dippingShot": {"effect": "Changes shot trajectory"},
    "risingShot": {"effect": "Changes shot trajectory"},
    "firstTimeShot": {"effect": "Reduces first-time shot error (same mechanism as OTP)"},
    "oneTouchPass": {"effect": "Reduces first-time pass error"},
    "weightedPass": {"effect": "Ball drops faster, lands closer to receiver — more accurate lofted passes"},
    "visionaryPass": {
        "effect": "Reduces pass receiver's first-touch error; ONLY helps the receiver",
    },
    "phenomenalFinishing": {
        "effect": "Greatly reduces shot error from player positioning",
    },
    "phenomenalPass": {"effect": "Improves pass quality"},
    "lowLoftedPass": {
        "effect": "Ball travels lower and shorter — faster and more accurate",
    },
    "edgedCrossing": {
        "effect": "Changes ball rotation from horizontal to vertical; best with weak foot (WF Accuracy 'Very High')",
    },
    "aerialSuperiority": {
        "effect": "Increases aerial duel win rate when jump heights are similar",
    },
    "aerialFort": {
        "effect": "Defender reaches max jump height on EVERY clearance",
    },
    "fightingSpirit": {
        "effect": "Reduces shot/pass error when opponents are nearby",
    },
    "trackBack": {
        "effect": "Pressures ball holder after losing possession",
    },

    # === GK Skills ===
    "gkLowPunt": {"effect": "Changes punt trajectory"},
    "gkHighPunt": {"effect": "Increases punt distance and speed"},
    "gkLongThrow": {"effect": "Increases throw range"},
    "longThrow": {"effect": "Increases throw range from 21m to 28m"},
}

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3: TOUCH FREQUENCY — The REAL Dribbling Feel
# ═══════════════════════════════════════════════════════════════════════════
# Touch Frequency determines how smooth dribbling FEELS.
# It is NOT the visible "Dribbling" stat.

TOUCH_FREQUENCY_RULES = {
    "low_speed": {
        "stats": ["tightPossession"],
        "description": "TF at low speed (walking, turning >90° while dashing)",
        "player_model_matters": True,
    },
    "high_speed": {
        "stats": ["dribbling", "speed"],
        "description": "TF at high speed (dashing, turns <90°)",
        "player_model_matters": True,
    },
    "determination": {
        "low_speed_threshold": "When dribbling without dash, OR when turn >90° while dashing",
        "high_speed_threshold": "When dashing AND turn <90°",
    },
}

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4: BODY MODEL — Hidden Physical Stats
# ═══════════════════════════════════════════════════════════════════════════
# These are scraped from efhub.com but never shown in-game.
# They dramatically affect how a player FEELS.

BODY_MODEL_STATS = [
    "armLength", "shoulderWidth", "neckLength", "chestMeasurement",
    "neckSize", "shoulderHeight", "legLength", "thighSize",
    "waistSize", "armSize", "calfSize", "legCoverageRadius",
    "armCoverageRadius", "jumpingHeight", "torsoCollision", "dribbleHeight",
]

# Jump height formula: Height + 54 + [(Jump - 40) * 0.6]
JUMP_HEIGHT_FORMULA_DESC = "Height + 54 + [(Jump - 40) × 0.6]"

def calculate_jump_height(height_cm: int, jump_stat: int) -> float:
    """Calculate actual jumping height in cm using the hidden formula."""
    return height_cm + 54 + (jump_stat - 40) * 0.6


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5: POSITION-RELEVANT THRESHOLDS
# ═══════════════════════════════════════════════════════════════════════════
# Which thresholds matter most for each position

POSITION_THRESHOLD_PRIORITY: Dict[str, List[str]] = {
    "CF":   ["finishing", "offensiveAwareness", "physicalContact", "tightPossession", "acceleration"],
    "SS":   ["tightPossession", "finishing", "balance", "acceleration", "offensiveAwareness"],
    "LWF":  ["tightPossession", "acceleration", "balance", "speed", "finishing"],
    "RWF":  ["tightPossession", "acceleration", "balance", "speed", "finishing"],
    "AMF":  ["tightPossession", "balance", "offensiveAwareness", "lowPass", "loftedPass"],
    "CMF":  ["tightPossession", "balance", "lowPass", "stamina", "defensiveAwareness"],
    "DMF":  ["defensiveAwareness", "ballWinning", "physicalContact", "lowPass", "stamina"],
    "LMF":  ["speed", "tightPossession", "acceleration", "stamina", "loftedPass"],
    "RMF":  ["speed", "tightPossession", "acceleration", "stamina", "loftedPass"],
    "CB":   ["defensiveAwareness", "ballWinning", "physicalContact", "jump", "speed"],
    "LB":   ["defensiveAwareness", "speed", "stamina", "ballWinning", "tightPossession"],
    "RB":   ["defensiveAwareness", "speed", "stamina", "ballWinning", "tightPossession"],
    "GK":   ["gkAwareness", "gkReflexes", "gkReach", "gkCatching", "gkClearing"],
}

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 6: SYNERGY COMBOS — Skill + Stat Interactions
# ═══════════════════════════════════════════════════════════════════════════
# These are combinations where skills and stats multiply effectiveness

SYNERGY_COMBOS = [
    {
        "name": "DT Boom Machine",
        "description": "Double Touch recovers Balance → next shot/pass is more accurate",
        "skills_required": ["doubleTouch"],
        "stats_matter": {"balance": 85, "finishing": 80},
        "build_tip": "Stack Balance + Finishing. After every DT, fire immediately.",
    },
    {
        "name": "Curler God",
        "description": "Long-Range Curler bypasses 99 cap — push Curl + Finishing beyond normal limits",
        "skills_required": ["longRangeDrive"],
        "stats_matter": {"curl": 90, "finishing": 85, "kickingPower": 80},
        "build_tip": "With Long-Range Curler, 90 Curl + 10% boost = effectively 99. Push other stats instead.",
    },
    {
        "name": "Through Ball Specialist",
        "description": "Through Passing gives +20% to passing stats — huge boost",
        "skills_required": ["throughPassing"],
        "stats_matter": {"lowPass": 85, "loftedPass": 80, "offensiveAwareness": 80},
        "build_tip": "85 Low Pass + 20% = effectively 102. Don't over-invest in passing — let the skill do the work.",
    },
    {
        "name": "Crossing Machine",
        "description": "Pinpoint Crossing + Edged Crossing = elite delivery from wide",
        "skills_required": ["pinpointCrossing", "edgedCrossing"],
        "stats_matter": {"loftedPass": 85, "curl": 75, "speed": 80},
        "build_tip": "Pinpoint gives +10% to passing. Stack with speed for byline crosses.",
    },
    {
        "name": "Super-Sub Impact",
        "description": "Super-sub gives +5% to Attacking Awareness and Finishing after HT",
        "skills_required": ["superSub"],
        "stats_matter": {"offensiveAwareness": 85, "finishing": 85},
        "build_tip": "Don't max these stats — Super-sub adds +5% automatically. Save PP for speed/acceleration.",
    },
    {
        "name": "Aerial Dominance",
        "description": "Aerial Superiority + high Jump + tall height = wins every header",
        "skills_required": ["aerialSuperiority"],
        "stats_matter": {"jump": 85, "heading": 80},
        "build_tip": "Jump formula: Height+54+[(Jump-40)×0.6]. A 190cm player with 85 Jump = 243cm jump height.",
    },
    {
        "name": "Press Monster",
        "description": "Interception + Blocker + Track Back = complete defensive engine",
        "skills_required": ["interception", "blocker", "trackBack"],
        "stats_matter": {"defensiveAwareness": 85, "ballWinning": 80, "aggression": 75, "stamina": 85},
        "build_tip": "Interception/Blocker give +10% def awareness when triggered. Don't need to max awareness.",
    },
    {
        "name": "Fortress Lead",
        "description": "Fortress gives +5% def stats when leading — snowball effect",
        "skills_required": ["fortress"],
        "stats_matter": {"defensiveEngagement": 85, "ballWinning": 80, "aggression": 75},
        "build_tip": "If you score first, Fortress kicks in. Build to protect leads.",
    },
    {
        "name": "First-Time Finisher",
        "description": "First-time Shot + Acrobatic Finishing = clinical volleys and one-touch goals",
        "skills_required": ["firstTimeShot", "acrobaticFinishing"],
        "stats_matter": {"finishing": 85, "offensiveAwareness": 80, "balance": 80},
        "build_tip": "First-time Shot reduces error. Stack with high Balance for clean strikes.",
    },
    {
        "name": "Box Crasher",
        "description": "Late runs + high Off. Awareness + acceleration = ghost into the box",
        "skills_required": [],
        "stats_matter": {"offensiveAwareness": 85, "acceleration": 85, "finishing": 80},
        "build_tip": "Off. Awareness determines AI run timing. Higher = earlier, smarter runs.",
    },
]

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 7: FORM / CONDITION SYSTEM
# ═══════════════════════════════════════════════════════════════════════════

FORM_TYPES = {
    "Unwavering": {
        "A": {"up2": 14.8, "up1": 61.6, "normal": 23.6, "down1": 0, "down2": 0},
        "B": {"up2": 10, "up1": 53.6, "normal": 36.4, "down1": 0, "down2": 0},
        "C": {"up2": 3.6, "up1": 32, "normal": 60.4, "down1": 3.2, "down2": 0.8},
        "D": {"up2": 0, "up1": 14.8, "normal": 70.8, "down1": 10.8, "down2": 3.6},
        "E": {"up2": 0, "up1": 0, "normal": 73.2, "down1": 16, "down2": 10.8},
    },
    "Standard": {
        "A": {"up2": 24.4, "up1": 41.6, "normal": 34, "down1": 0, "down2": 0},
        "B": {"up2": 18.8, "up1": 31.2, "normal": 50, "down1": 0, "down2": 0},
        "C": {"up2": 4.8, "up1": 24.4, "normal": 51.2, "down1": 18, "down2": 1.6},
        "D": {"up2": 0, "up1": 16, "normal": 58.4, "down1": 15.6, "down2": 10},
        "E": {"up2": 0, "up1": 14.4, "normal": 44.4, "down1": 20.8, "down2": 20.4},
    },
    "Inconsistent": {
        "A": {"up2": 46.5, "up1": 19.3, "normal": 34.2, "down1": 0, "down2": 0},
        "B": {"up2": 31.5, "up1": 18.6, "normal": 49.9, "down1": 0, "down2": 0},
        "C": {"up2": 32.4, "up1": 14.8, "normal": 6.8, "down1": 28.4, "down2": 17.6},
        "D": {"up2": 13.6, "up1": 26.4, "normal": 12.4, "down1": 21.2, "down2": 26.4},
        "E": {"up2": 5.2, "up1": 22.4, "normal": 15.6, "down1": 30, "down2": 26.8},
    },
}

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 8: BOOSTER RECOMMENDATIONS (eFootball 2027 +3 system)
# ═══════════════════════════════════════════════════════════════════════════

BOOSTER_RECOMMENDATIONS: Dict[str, Dict[str, List[str]]] = {
    "CF":   {"primary": ["Striker's Instinct", "Shooting"], "secondary": ["Fantasista"]},
    "SS":   {"primary": ["Fantasista", "Technique"], "secondary": ["Shooting"]},
    "LWF":  {"primary": ["Ball Carrying", "Technique"], "secondary": ["Fantasista"]},
    "RWF":  {"primary": ["Ball Carrying", "Technique"], "secondary": ["Fantasista"]},
    "AMF":  {"primary": ["Technique", "Ball Carrying"], "secondary": ["Fantasista"]},
    "CMF":  {"primary": ["Technique", "Ball Carrying"], "secondary": ["Fantasista"]},
    "DMF":  {"primary": ["Shutdown", "Duelling"], "secondary": ["Hard Worker"]},
    "LMF":  {"primary": ["Ball Carrying", "Technique"], "secondary": ["Fantasista"]},
    "RMF":  {"primary": ["Ball Carrying", "Technique"], "secondary": ["Fantasista"]},
    "CB":   {"primary": ["Shutdown", "Duelling"], "secondary": ["Hard Worker"]},
    "LB":   {"primary": ["Ball Carrying", "Agility"], "secondary": ["Duelling"]},
    "RB":   {"primary": ["Ball Carrying", "Agility"], "secondary": ["Duelling"]},
    "GK":   {"primary": ["Goalkeeper"], "secondary": []},
}

# ═══════════════════════════════════════════════════════════════════════════
# SECTION 9: MISCONCEPTION CORRECTIONS
# ═══════════════════════════════════════════════════════════════════════════

MISCONCEPTIONS = [
    {
        "myth": "Acceleration affects dribbling speed",
        "truth": "Acceleration ONLY affects first 10m from standstill. Once moving, Speed + Dribbling determine ball speed.",
    },
    {
        "myth": "Balance affects dribbling feel",
        "truth": "Balance affects shot/pass accuracy under pressure. Dribbling feel = Touch Frequency (Tight Possession + Player Model).",
    },
    {
        "myth": "Curl affects passing accuracy",
        "truth": "Curl ONLY affects crosses (low or lofted). It has zero effect on regular passes.",
    },
    {
        "myth": "Kicking Power affects pass speed",
        "truth": "Pass speed is purely determined by Low Pass / Lofted Pass stats. Kicking Power only affects shots.",
    },
    {
        "myth": "Jump affects GK diving",
        "truth": "Jump has zero effect on saving normal shots. It only affects chip shots and claiming aerial balls.",
    },
    {
        "myth": "Super-sub boosts all stats",
        "truth": "Super-sub ONLY increases Attacking Awareness and Finishing by 5%. Nothing else.",
    },
    {
        "myth": "Higher OVR = better player",
        "truth": "Two players with same OVR can perform vastly differently based on body model, skill combos, and stat distribution.",
    },
]


def get_threshold_info(stat: str, value: int) -> Optional[Dict]:
    """Check if a stat value crosses any known thresholds."""
    thresholds = STAT_THRESHOLDS.get(stat, [])
    for t in thresholds:
        if value >= t["value"]:
            return t
    return None


def get_all_crossed_thresholds(stats: Dict[str, int]) -> List[Dict]:
    """Return all thresholds that the current stats have crossed."""
    crossed = []
    for stat, value in stats.items():
        info = get_threshold_info(stat, value)
        if info:
            crossed.append({"stat": stat, "value": value, **info})
    return crossed


def get_next_threshold(stat: str, current_value: int) -> Optional[Dict]:
    """Get the next threshold to aim for."""
    thresholds = STAT_THRESHOLDS.get(stat, [])
    for t in sorted(thresholds, key=lambda x: x["value"]):
        if current_value < t["value"]:
            return t
    return None


def find_applicable_synergies(skills: List[str], stats: Dict[str, int]) -> List[Dict]:
    """Find synergy combos that apply based on player's skills and stats."""
    applicable = []
    for combo in SYNERGY_COMBOS:
        req_skills = combo.get("skills_required", [])
        if not req_skills or all(s in skills for s in req_skills):
            stat_reqs = combo.get("stats_matter", {})
            met = all(stats.get(s, 0) >= v for s, v in stat_reqs.items())
            if met:
                applicable.append({**combo, "fully_met": True})
            else:
                # Partially met — show what's needed
                missing = {s: v - stats.get(s, 0) for s, v in stat_reqs.items() if stats.get(s, 0) < v}
                applicable.append({**combo, "fully_met": False, "missing": missing})
    return applicable


def get_body_typeAdvice(height: int, weight: int, position: str) -> str:
    """Generate body-type-aware build advice."""
    advice = []
    if height >= 188:
        advice.append("Tall frame — aerial builds are highly effective")
        if position in ("CF", "CB"):
            advice.append("Stack Jump + Heading for dominant aerial presence")
    elif height <= 172:
        advice.append("Compact frame — dribbling builds benefit from lower center of gravity")
        if position in ("SS", "AMF", "LWF", "RWF"):
            advice.append("Stack Tight Possession + Balance for maximum close control")

    if weight >= 80:
        advice.append("Heavy build — Physical Contact builds are effective")
    elif weight <= 68:
        advice.append("Light build — Speed + Acceleration builds feel faster")

    return " · ".join(advice) if advice else "Balanced physique — versatile build options"

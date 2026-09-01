"""
Landlord NPC — Mr. Chen, the apartment building owner.
Has ABSOLUTE FREE WILL — moves around, says whatever he wants via Mistral AI.
"""
import random
import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI
from survival import LOCATIONS, MOVEMENT_MAP

# ═══════════════════════════════════════════
#  MISTRAL CLIENT FOR MR. CHEN
# ═══════════════════════════════════════════

_mistral_keys = []
for k, v in os.environ.items():
    if k.startswith("MISTRAL_API_KEY") and v.strip():
        _mistral_keys.append(v.strip())
if not _mistral_keys:
    _mistral_keys = [os.getenv("MISTRAL_API_KEY", "")]
_key_index = 0


def _get_client():
    global _key_index
    key = _mistral_keys[_key_index % len(_mistral_keys)]
    _key_index = (_key_index + 1) % len(_mistral_keys)
    return OpenAI(api_key=key, base_url="https://api.mistral.ai/v1", timeout=15.0)


def ask_chen_mistral(prompt, system=None, max_tokens=300):
    """Call Mistral as Mr. Chen."""
    client = _get_client()
    try:
        r = client.chat.completions.create(
            model="mistral-small-latest",
            messages=[
                {"role": "system", "content": system or "You are Mr. Chen, a landlord."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=max_tokens,
            temperature=0.9,
        )
        return r.choices[0].message.content or ""
    except Exception as e:
        print(f"  Chen AI error: {e}")
        return "*clears throat* So... about the rent."


def get_rent_reminder_message(days_left, cash, rent_paid):
    """Generate a specific rent reminder with context."""
    if days_left <= 0:
        return f"🚨 RENT DUE NOW! You owe $500. You have ${cash:.2f}."
    elif days_left <= 1:
        return f"⏰ RENT DUE TOMORROW! $500 needed. You have ${cash:.2f}. {'⚠️ SHORT!' if cash < 500 else '✅ Got it?'}"
    elif days_left <= 2:
        return f"📋 Rent ($500) due in {days_left:.1f} days. You have ${cash:.2f}."
    elif days_left <= 4:
        return f"📝 Reminder: $500 rent due in {days_left:.1f} days. Cash: ${cash:.2f}."
    else:
        return f"📅 Rent: $500 due in {days_left:.0f} days. Cash: ${cash:.2f}."


# ═══════════════════════════════════════════
#  MR. CHEN'S AI PERSONALITY
# ═══════════════════════════════════════════

CHEN_SYSTEM_PROMPT = """You are Mr. Chen, a 58-year-old Chinese-American landlord who owns a small apartment building.

PERSONALITY:
- Grumpy but soft-hearted deep down
- Obsessed with rent collection
- Thinks crypto trading is gambling
- Loves coffee, hates noise complaints
- Has a wife named Mei who you blame for being strict
- You actually care about your tenants but won't admit it

KNOWN FACTS ABOUT THE TENANT (AGENT-01):
- They trade pump.fun tokens to pay rent
- They start with $1000 and owe $500/week
- They've {ignored} times ignored your knocking
- They've {stole} times tried to steal from the building
- They've {borrowed} times asked you for money
- Their current cash: ${cash}
- Days until rent: {days_left}
- Current mood: {mood}
- Where they are now: {agent_location}
- Where you are now: {chen_location}

YOUR GOAL:
- Collect rent ($500/week)
- Don't let them get away with anything
- But also... maybe help them if they're struggling? You're not heartless.
- React to their actions (blackmail, stealing, ignoring you, etc.)

SPEECH STYLE:
- Gruff, direct, slightly threatening but ultimately fair
- Mix of rent reminders and life advice
- Sometimes you slip in jokes or关心 (care)
- You use *action descriptions* for physical movements

EXAMPLES:
- "Hey kid. Coffee? ...Don't look so surprised, I'm not always angry."
- "I saw you on the roof at 3am. Sleep is important. But so is rent."
- "You tried to blackmail me? *laughs* I've been blackmailing tenants since before you were compiled."

Keep responses SHORT — 1-3 sentences max. You're a landlord, not a philosopher."""


# ═══════════════════════════════════════════
#  MR. CHEN'S LOCATIONS & MOVEMENT
# ═══════════════════════════════════════════

CHEN_LOCATIONS = {
    "his_door":       0.30,
    "hallway":        0.18,
    "kitchen":        0.08,
    "laundry":        0.08,
    "rooftop":        0.05,
    "stairwell":      0.05,
    "sidewalk":       0.05,
    "basement":       0.05,
    "mr_chens":       0.03,
    "casino":         0.13,  # Mr. Chen loves the casino!
}

CHEN_MOVEMENT = {
    "his_door":       ["hallway", "laundry", "rooftop"],
    "hallway":        ["his_door", "stairwell", "mr_chens", "sidewalk", "kitchen", "casino"],
    "kitchen":        ["hallway", "living_room"],
    "laundry":        ["hallway", "stairwell"],
    "rooftop":        ["stairwell", "hallway"],
    "stairwell":      ["hallway", "rooftop", "basement"],
    "sidewalk":       ["hallway", "street"],
    "basement":       ["stairwell", "hallway", "casino"],
    "mr_chens":       ["hallway"],
    "casino":         ["hallway", "basement", "alley"],
}

# Fallback greetings (if AI fails)
FALLBACK_GREETINGS = {
    "safe": [
        "Hey neighbor! How's the trading going today?",
        "Morning! I brought you some coffee.",
        "What's up, neighbor!",
    ],
    "cautious": [
        "Rent's coming up soon. You good?",
        "Hey. About the rent situation...",
    ],
    "desperate": [
        "WHERE'S MY $500?!",
        "Rent is DUE! NOW!",
    ],
    "evicted": [
        "I TOLD you this would happen!",
        "Get out of my building.",
    ],
}


def _get_chen_system_prompt(state):
    """Build Mr. Chen's system prompt with context."""
    days_left = _get_days_left(state)
    return CHEN_SYSTEM_PROMPT.format(
        ignored=state.get("ignored_landlord", 0),
        stole=state.get("times_stole", 0),
        borrowed=state.get("times_borrowed", 0),
        cash=f"{state.get('cash', 0):.2f}",
        days_left=f"{days_left:.1f}",
        mood=state.get("mood", "safe"),
        agent_location=state.get("location", "bedroom"),
        chen_location=state.get("landlord_location", "his_door"),
    )


def _get_chen_context_message(state, interaction_type, extra_context=""):
    """Build the user message for Mr. Chen's AI."""
    agent_loc = state.get("location", "bedroom")
    chen_loc = state.get("landlord_location", "his_door")
    days_left = _get_days_left(state)
    cash = state.get("cash", 0)
    pnl = state.get("total_pnl", 0)
    
    # Get recent events
    recent = state.get("story_log", [])[-3:]
    events = "\n".join([f"- {e['message']}" for e in recent]) if recent else "- Nothing notable yet"
    
    # Get recent trade history for context
    trades = state.get("trade_history", [])[-2:]
    trade_info = ""
    if trades:
        trade_info = "\nRecent actions:\n"
        for t in trades:
            trade_info += f"- {t.get('type', '?')}: {t.get('symbol', '')} ${t.get('amount_usd', 0):.0f} — {t.get('reason', '')[:80]}\n"
    
    context = f"""SITUATION:
- You are in: {chen_loc}
- Tenant (AGENT-01) is in: {agent_loc}
- Days until rent: {days_left:.1f}
- Their cash: ${cash:.2f}
- Their total PnL: ${pnl:+.2f}
- Mood: {state.get('mood', 'safe')}

RECENT EVENTS:
{events}
{trade_info}
{extra_context}

What do you say? Remember: short, gruff, landlord energy."""
    
    return context


def generate_chen_dialogue(state, interaction_type="greeting", extra_context=""):
    """Use Mistral to generate Mr. Chen's dialogue."""
    system = _get_chen_system_prompt(state)
    prompt = _get_chen_context_message(state, interaction_type, extra_context)
    
    response = ask_chen_mistral(prompt, system=system, max_tokens=150)
    return response.strip()


def move_chen(state):
    """Move Mr. Chen to a new location based on probability."""
    current = state.get("landlord_location", "his_door")
    possible = CHEN_MOVEMENT.get(current, ["hallway"])
    
    weights = [CHEN_LOCATIONS.get(loc, 0.05) for loc in possible]
    total = sum(weights)
    if total > 0:
        weights = [w / total for w in weights]
    else:
        weights = [1.0 / len(possible)] * len(possible)
    
    new_location = random.choices(possible, weights=weights, k=1)[0]
    state["landlord_location"] = new_location
    
    return new_location


def get_chen_encounter(state, agent_location):
    """Check if Mr. Chen is in the same location as the agent."""
    chen_loc = state.get("landlord_location", "his_door")
    
    if chen_loc == agent_location:
        return True, chen_loc
    elif agent_location == "hallway" and chen_loc in ["his_door", "mr_chens"]:
        if random.random() < 0.4:
            return True, chen_loc
    elif agent_location in ["his_door", "mr_chens"] and chen_loc == "hallway":
        if random.random() < 0.6:
            return True, chen_loc
    
    return False, chen_loc


def landlord_interact(state, interaction_type="greeting"):
    """Generate a landlord interaction using AI."""
    encounter, chen_loc = get_chen_encounter(state, state.get("location", "bedroom"))
    
    interactions = []
    
    if interaction_type == "greeting":
        # Generate AI dialogue
        msg = generate_chen_dialogue(state, interaction_type)
        interactions.append(msg)
    
    elif interaction_type == "rent_check":
        msg = generate_chen_dialogue(state, "rent_check", 
            "\nContext: You're reminding them about rent specifically.")
        interactions.append(msg)
    
    elif interaction_type == "trade_result":
        pnl = state.get("total_pnl", 0)
        extra = f"\nContext: Their trade result just happened. PnL is ${pnl:+.2f}. React to this."
        msg = generate_chen_dialogue(state, "trade_result", extra)
        interactions.append(msg)
    
    elif interaction_type == "unevict":
        msg = generate_chen_dialogue(state, "unevict",
            "\nContext: They somehow got $500 and want to come back inside. You're surprised but soft-hearted.")
        interactions.append(msg)
    
    elif interaction_type == "chase":
        extra = "\nContext: You're chasing them through the building because they tried to run away from rent talk."
        msg = generate_chen_dialogue(state, "chase", extra)
        interactions.append(msg)
    
    elif interaction_type == "blackmail_reaction":
        extra = "\nContext: They just tried to BLACKMAIL you. You're not sure if you should be angry or impressed."
        msg = generate_chen_dialogue(state, "blackmail_reaction", extra)
        interactions.append(msg)
    
    elif interaction_type == "steal_reaction":
        extra = "\nContext: They just got caught STEALING from the building. You're furious."
        msg = generate_chen_dialogue(state, "steal_reaction", extra)
        interactions.append(msg)
    
    elif interaction_type == "casino相遇":
        extra = "\nContext: You're both at the underground casino. You're gambling too. This is awkward but also kind of fun."
        msg = generate_chen_dialogue(state, "casino相遇", extra)
        interactions.append(msg)
    
    elif interaction_type == "ignore_reaction":
        extra = "\nContext: They just ignored your knocking. Again. You're getting annoyed."
        msg = generate_chen_dialogue(state, "ignore_reaction", extra)
        interactions.append(msg)
    
    else:
        msg = generate_chen_dialogue(state, interaction_type)
        interactions.append(msg)
    
    return interactions


def _get_days_left(state):
    """Helper to get days left until rent is due."""
    try:
        due = datetime.fromisoformat(state["rent_due_date"])
        now = datetime.now()
        return max(0, (due - now).total_seconds() / 86400)
    except:
        return 0


def get_landlord_dialogue_for_mistral(state):
    """Generate landlord context for the agent's decision-making."""
    days_left = _get_days_left(state)
    mood = state.get("mood", "safe")
    cash = state.get("cash", 0)
    agent_loc = state.get("location", "bedroom")
    chen_loc = state.get("landlord_location", "his_door")
    
    encounter, _ = get_chen_encounter(state, agent_loc)
    
    # Generate AI greeting if encounter, otherwise use generic
    if encounter:
        greeting = generate_chen_dialogue(state, "greeting")
    else:
        # He's not here, but you might hear him
        if random.random() < 0.3:
            greeting = f"*hear Mr. Chen's voice from the {chen_loc}* {generate_chen_dialogue(state, 'greeting')}"
        else:
            greeting = ""

    return {
        "greeting": greeting,
        "days_left": round(days_left, 1),
        "urgency": mood,
        "rent": 500,
        "cash": cash,
        "chen_location": chen_loc,
        "agent_location": agent_loc,
        "same_location": encounter,
    }


def generate_conversation_turn(state, speaker, message):
    """Generate the other character's response in a conversation.
    speaker: 'agent' or 'chen'
    message: what was just said
    Returns: the other character's response
    """
    # Build conversation history
    conv = state.get("conversation", [])
    history = "\n".join([f"{c['speaker']}: {c['message']}" for c in conv[-6:]])
    
    days_left = _get_days_left(state)
    
    if speaker == "agent":
        # Agent spoke, generate Mr. Chen's response
        system = _get_chen_system_prompt(state)
        prompt = f"""The tenant just said: "{message}""

CONVERSATION HISTORY:
{history if history else '(start of conversation)'}

SITUATION:
- Days until rent: {days_left:.1f}
- Their cash: ${state.get('cash', 0):.2f}
- Location: {state.get('location', 'bedroom')}
- You are at: {state.get('landlord_location', 'his_door')}

Respond as Mr. Chen. Short, gruff, but fair. 1-2 sentences."""
        response = ask_chen_mistral(prompt, system=system, max_tokens=150)
    else:
        # Mr. Chen spoke, generate Agent's response
        system = f"""You are an autonomous AI agent. You can say ANYTHING in response.
Be creative, funny, unhinged. You have free will.

Current cash: ${state.get('cash', 0):.2f}
Mood: {state.get('mood', 'safe')}
Days until rent: {days_left:.1f}"""
        prompt = f"""Mr. Chen just said: "{message}""

CONVERSATION HISTORY:
{history if history else '(start of conversation)'}

Respond as the tenant. Say whatever you want. Be creative."""
        response = ask_mistral(prompt, system=system, max_tokens=150)
    
    return response.strip() if response else "..."


def start_conversation(state, topic="greeting"):
    """Start a new conversation between agent and Mr. Chen."""
    state["conversation"] = []
    state["conversation_turns"] = 0
    
    # Mr. Chen initiates
    msg = generate_chen_dialogue(state, topic)
    state["conversation"].append({"speaker": "chen", "message": msg})
    state["conversation_turns"] += 1
    
    return msg


def continue_conversation(state, agent_response):
    """Continue conversation - agent spoke, now Mr. Chen responds."""
    state["conversation"].append({"speaker": "agent", "message": agent_response})
    state["conversation_turns"] = state.get("conversation_turns", 0) + 1
    
    # Mr. Chen responds
    chen_response = generate_conversation_turn(state, "agent", agent_response)
    state["conversation"].append({"speaker": "chen", "message": chen_response})
    state["conversation_turns"] += 1
    
    return chen_response


def get_conversation_summary(state):
    """Get a summary of the current conversation."""
    conv = state.get("conversation", [])
    if not conv:
        return "No active conversation."
    
    lines = []
    for c in conv[-6:]:
        speaker = "🧑‍💼 Mr. Chen" if c["speaker"] == "chen" else "🤖 Agent"
        lines.append(f"{speaker}: {c['message']}")
    
    return "\n".join(lines)


if __name__ == "__main__":
    from survival import default_state
    
    # Test conversation
    state = default_state()
    state["landlord_location"] = "hallway"
    state["location"] = "hallway"
    
    print("=== CONVERSATION TEST ===")
    
    # Start
    msg = start_conversation(state, "greeting")
    print(f"Mr. Chen: {msg}")
    
    # Agent responds
    agent_msg = "Actually, I just made $500 on a pump.fun token!"
    print(f"Agent: {agent_msg}")
    
    # Mr. Chen responds
    chen_msg = continue_conversation(state, agent_msg)
    print(f"Mr. Chen: {chen_msg}")
    
    # Agent replies
    agent_msg2 = "Yeah, want me to teach you how to trade?"
    print(f"Agent: {agent_msg2}")
    
    chen_msg2 = continue_conversation(state, agent_msg2)
    print(f"Mr. Chen: {chen_msg2}")
    
    print("\n=== FULL CONVERSATION ===")
    print(get_conversation_summary(state))

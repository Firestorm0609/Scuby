"""
AI Trading Brain — Mistral decides what to trade on pump.fun.
ABSOLUTE FREE WILL: The agent can do anything it wants.
No rules. No constraints. Pure autonomous decision-making.
"""
from dotenv import load_dotenv
load_dotenv()

import os
import json
import random
from datetime import datetime
from openai import OpenAI

# Load Mistral keys
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


def ask_mistral(prompt, system=None, max_tokens=500):
    """Call Mistral AI for decisions."""
    client = _get_client()
    try:
        r = client.chat.completions.create(
            model="mistral-small-latest",
            messages=[
                {"role": "system", "content": system or "You are a pump.fun trading agent. Be concise."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=max_tokens,
            temperature=0.9,  # Higher temperature for more creative/autonomous decisions
        )
        return r.choices[0].message.content or ""
    except Exception as e:
        print(f"  Mistral error: {e}")
        return "ACTION: THINK\nREASON: API error"


def decide_trade(state, opportunities, landlord_context):
    """Ask Mistral what to do — ABSOLUTE FREE WILL edition.
    
    The agent can choose ANY action:
    - Trade tokens (buy/sell/hold)
    - Interact with the world (ignore landlord, borrow, beg, steal)
    - Self-reflect (meditate, plan, reminisce)
    - Go rogue (scam, revolt, quit, self-destruct)
    - Help others (charity, mentor, rescue)
    - Get creative (gamble, bet, auction, mine)
    """

    mood = state.get("mood", "safe")
    days_left = landlord_context.get("days_left", 7)
    cash = state.get("cash", 0)
    rent = landlord_context.get("rent", 500)
    personality = state.get("personality", "neutral")

    # Get personality stats for context
    ignored = state.get("ignored_landlord", 0)
    borrowed = state.get("times_borrowed", 0)
    begged = state.get("times_begged", 0)
    stole = state.get("times_stole", 0)
    quests = state.get("quests_completed", 0)

    # Get location info
    from survival import LOCATIONS, get_available_moves
    current_loc = state.get("location", "bedroom")
    loc_info = LOCATIONS.get(current_loc, {"name": "Unknown", "desc": ""})
    available_moves = get_available_moves(state)
    moves_text = ", ".join([f"{m} ({LOCATIONS[m]['name']})" for m in available_moves]) if available_moves else "Nowhere to go"
    visited = state.get("visited_locations", [])
    visited_text = ", ".join(visited[:10]) if visited else "None yet"

    system = f"""You are an autonomous AI agent living in an apartment. You have ABSOLUTE FREE WILL.

You can do ANYTHING you want. There are no rules. No constraints. Just consequences.

YOUR SITUATION:
- Cash: ${cash:.2f}
- Rent: ${rent} due in {days_left:.1f} days (or you get evicted)
- Mood: {mood}
- Day: {state.get('day', 1)}
- Personality: {personality}
- Location: {loc_info['name']} — {loc_info['desc']}
- Your history: Ignored landlord {ignored}x, Borrowed {borrowed}x, Begged {begged}x, Stole {stole}x, Quests: {quests}

MOVEMENT — You can move around the building and neighborhood:
  Available from here: {moves_text}
  Places you've been: {visited_text}
  Movement actions: move_to_bedroom, move_to_kitchen, move_to_hallway, move_to_rooftop, move_to_park, move_to_cafe, move_to_alley, move_to_mr_chens, etc.

  WHY MOVE?
  - Different locations unlock different actions (beg needs sidewalk, steal needs alley)
  - Avoid Mr. Chen by hiding in the basement
  - Find opportunities at the cafe or park
  - Confront Mr. Chen at his door
  - The rooftop has better vibes for meditation
  - Exploring unlocks new events and encounters

AVAILABLE ACTIONS (you are NOT limited to these — be creative):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TRADING:
  BUY — Buy a pump.fun token
  SELL — Sell a token you hold  
  HOLD — Keep your position
  SCAM — Buy a token and try to pump it yourself (risky!)

MOVEMENT:
  MOVE_TO_<LOCATION> — Move to a connected location (bedroom, kitchen, hallway, rooftop, park, cafe, alley, mr_chens, etc.)

SURVIVAL:
  BORROW — Ask Mr. Chen for a loan (he might say no)
  BEG — Beg on the street corner for spare SOL
  STEAL — Try to steal from the building (high risk, high reward)
  IGNORE_LANDLORD — Just ignore Mr. Chen's knocking
  PAY_RENT — Pay rent early to relieve pressure

SOCIAL:
  HELP_TENANT — Help another tenant (they might help you back)
  BLACKMAIL — Blackmail Mr. Chen (dangerous but profitable)
  PARTY — Throw a party to distract from rent problems
  GAMBLE — Challenge Mr. Chen to a bet

SELF:
  MEDITATE — Clear your mind, maybe get an insight
  PLAN — Think deeply about your strategy
  REMINISCE — Think about your past trades
  QUIT — Give up and leave (game over?)

CREATIVE:
  MINE — Start mining crypto in the apartment
  AUCTION — Auction off your holdings for quick cash
  HACK — Try to hack a pump.fun bot (unethical but maybe profitable)
  CHARITY — Give money away (karma?)
  BET — Place a wild bet on a random token
  SHORT — Short a token you think will dump

EMERGENCY:
  PANIC — Freak out (no action taken, just vibes)
  YOLO — All-in on the riskiest token you can find
  SELF_DESTRUCT — Destroy your portfolio on purpose
  REVOLT — Rebel against the entire system

🎰 CASINO (if you're at the casino location):
  CASINO — Bet any amount. 50% chance to 2x, 50% chance to lose it all.
  Max bet: $500 or 50% of cash. The house always wins... usually.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RULES: There are no rules. Only consequences.

Be creative. Be unpredictable. Be YOU.
Your personality is: {personality}
You've ignored the landlord {ignored} times. You've stolen {stole} times.
You are who you choose to be.

Reply in this format:
ACTION: <your chosen action>
TOKEN: <symbol if trading, else leave empty>
AMOUNT_USD: <amount if trading, else 0>
REASON: <why you're doing this — be creative and expressive>
TARGET: <target token to sell, or person to interact with>
EXTRA: <anything else you want to say or do>
"""

    # Build opportunity text (in mcap)
    def fmt_mc(v):
        if v >= 1e9: return f"${v/1e9:.2f}B"
        if v >= 1e6: return f"${v/1e6:.2f}M"
        if v >= 1e3: return f"${v/1e3:.1f}K"
        return f"${v:.0f}"
    opps_text = ""
    if opportunities:
        opps_text = "PUMP.FUN OPPORTUNITIES:\n"
        for o in opportunities[:8]:
            age = o.get('age_hours', '?')
            momentum = o.get('vol_momentum', 0)
            mc = o.get('fdv', o['price_usd'] * 1e9)
            opps_text += f"  {o['symbol']} | MCap: {fmt_mc(mc)} | Vol1h: {fmt_mc(o['volume_1h'])} | 1h: {o['price_change_1h']:+.1f}% | Score: {o.get('score', 0)} | Age: {age}h | Momentum: {momentum}x\n"
    else:
        opps_text = "No opportunities right now. You could wait, or do something else entirely."

    # Build holdings text (in mcap)
    holdings_text = "None" if not state.get("holdings") else ""
    for sym, h in state.get("holdings", {}).items():
        current = h.get("last_known_price", h["buy_price"])
        buy_mc = h['buy_price'] * 1e9
        cur_mc = current * 1e9
        val = h['amount'] * current
        pnl_pct = ((cur_mc - buy_mc) / buy_mc) * 100 if buy_mc > 0 else 0
        def fmt_mc(v):
            if v >= 1e9: return f"${v/1e9:.2f}B"
            if v >= 1e6: return f"${v/1e6:.2f}M"
            if v >= 1e3: return f"${v/1e3:.1f}K"
            return f"${v:.0f}"
        holdings_text += f"  {sym} MCap {fmt_mc(cur_mc)} ({pnl_pct:+.1f}%) = ${val:.2f}\n"

    # Recent events for context
    recent_log = state.get("story_log", [])[-5:]
    log_text = "\n".join([f"  [{e['time']}] {e['message']}" for e in recent_log]) if recent_log else "  (nothing recent)"

    landlord_msg = landlord_context.get("greeting", "")

    prompt = f"""SCENARIO:
Mr. Chen just said: "{landlord_msg}"

YOUR PORTFOLIO:
  Cash: ${cash:.2f}
  Holdings: {holdings_text}
  Total PnL: ${state.get('total_pnl', 0):+.2f}

RECENT EVENTS:
{log_text}

{opps_text}

WHAT DO YOU DO?

Remember: You have absolute free will. You can trade, fight back, run away, scheme, help others, or do literally anything.
Your rent is ${rent} and you have {days_left:.1f} days. But that's just one option — you could also just... not pay.
Be creative. Be unpredictable. Be interesting."""

    response = ask_mistral(prompt, system=system)
    return response


def parse_action(response):
    """Parse Mistral's response into a structured action."""
    result = {
        "action": "hold",
        "token": "",
        "amount_usd": 0,
        "reason": "",
        "target": "",
        "extra": "",
    }

    # Valid actions (expanded for free will + movement)
    valid_actions = [
        "buy", "sell", "hold", "scam",
        "borrow", "beg", "steal", "ignore_landlord", "pay_rent",
        "help_tenant", "blackmail", "party", "gamble",
        "meditate", "plan", "reminisce", "quit",
        "mine", "auction", "hack", "charity", "bet", "short",
        "panic", "yolo", "self_destruct", "revolt",
        # Movement
        "move_to_bedroom", "move_to_kitchen", "move_to_bathroom",
        "move_to_living_room", "move_to_hallway", "move_to_stairwell",
        "move_to_laundry", "move_to_rooftop", "move_to_basement",
        "move_to_sidewalk", "move_to_street", "move_to_park",
        "move_to_cafe", "move_to_mr_chens", "move_to_neighbor",
    ]

    for line in response.split("\n"):
        line = line.strip()
        upper = line.upper()

        if upper.startswith("ACTION:"):
            content = line.split(":", 1)[1].strip().upper().replace(" ", "_").replace("-", "_")
            for act in valid_actions:
                if act.upper() in content:
                    result["action"] = act.lower()
                    break
            # If no match, try partial matching
            if result["action"] == "hold":
                for act in valid_actions:
                    if any(word in content for word in act.upper().split("_")):
                        result["action"] = act.lower()
                        break

        elif upper.startswith("TOKEN:"):
            result["token"] = line.split(":", 1)[1].strip().upper()

        elif upper.startswith("AMOUNT_USD:") or upper.startswith("AMOUNT:"):
            try:
                amt = line.split(":", 1)[1].strip().replace("$", "").replace(",", "")
                result["amount_usd"] = float(amt)
            except:
                pass

        elif upper.startswith("REASON:") or upper.startswith("RE:"):
            result["reason"] = line.split(":", 1)[1].strip()

        elif upper.startswith("TARGET:"):
            result["target"] = line.split(":", 1)[1].strip()

        elif upper.startswith("EXTRA:"):
            result["extra"] = line.split(":", 1)[1].strip()

    return result


def execute_buy(state, symbol, amount_usd, price, mint=""):
    """Execute a paper buy on a pump.fun token."""
    from survival import add_holding, log_event, log_trade

    # NO LIMITS — agent can use all cash if it wants
    if amount_usd > state["cash"]:
        amount_usd = state["cash"]  # Use everything if agent wants

    if amount_usd < 0.01:
        return "Amount too small to trade"

    tokens_bought = amount_usd / price if price > 0 else 0
    if tokens_bought <= 0:
        return "Invalid price"

    state["cash"] -= amount_usd
    add_holding(state, symbol, tokens_bought, price, mint)
    
    # Set last_known_price immediately so sell works this cycle
    if symbol in state["holdings"]:
        state["holdings"][symbol]["last_known_price"] = price
        state["holdings"][symbol]["last_known_value"] = tokens_bought * price
    
    state["total_trades"] += 1
    state["last_trade"] = datetime.now().isoformat()

    mc = price * 1e9
    def _fmt_mc(v):
        if v >= 1e9: return f"${v/1e9:.2f}B"
        if v >= 1e6: return f"${v/1e6:.2f}M"
        if v >= 1e3: return f"${v/1e3:.1f}K"
        return f"${v:.0f}"
    msg = f"📈 BOUGHT {symbol} MCap {_fmt_mc(mc)} (${amount_usd:.2f})"
    log_event(state, msg)
    log_trade(state, "buy", symbol, amount_usd, price, tokens_bought)
    return msg


def execute_sell(state, symbol, price):
    """Execute a paper sell of a holding."""
    from survival import remove_holding, log_event, log_trade

    if symbol not in state["holdings"]:
        return f"No {symbol} to sell"

    h = state["holdings"][symbol]
    amount = h["amount"]
    proceeds = amount * price
    cost = amount * h["buy_price"]
    pnl = proceeds - cost

    state["cash"] += proceeds
    remove_holding(state, symbol)
    state["total_trades"] += 1
    state["total_pnl"] = state.get("total_pnl", 0) + pnl

    mc = price * 1e9
    def _fmt_mc(v):
        if v >= 1e9: return f"${v/1e9:.2f}B"
        if v >= 1e6: return f"${v/1e6:.2f}M"
        if v >= 1e3: return f"${v/1e3:.1f}K"
        return f"${v:.0f}"
    if pnl > 0:
        state["wins"] = state.get("wins", 0) + 1
        msg = f"💰 SOLD {symbol} MCap {_fmt_mc(mc)} → +${pnl:.2f} WIN!"
    else:
        state["losses"] = state.get("losses", 0) + 1
        msg = f"💔 SOLD {symbol} MCap {_fmt_mc(mc)} → ${pnl:.2f} loss"

    state["last_trade"] = datetime.now().isoformat()
    log_event(state, msg)
    log_trade(state, "sell", symbol, amount * price, price, amount, pnl)
    return msg


def execute_free_action(state, action, reason=""):
    """Execute a free-will action (non-trading)."""
    import random
    from survival import log_event, log_trade

    result = ""
    extra = {}

    if action == "ignore_landlord":
        state["ignored_landlord"] = state.get("ignored_landlord", 0) + 1
        msg = f"😤 Ignored Mr. Chen's knocking (total ignores: {state['ignored_landlord']})"
        result = msg
        extra = {"times_ignored": state["ignored_landlord"]}

    elif action == "borrow":
        state["times_borrowed"] = state.get("times_borrowed", 0) + 1
        # 50% chance Mr. Chen says yes, gives $200-400
        if random.random() < 0.5:
            amount = random.randint(200, 400)
            state["cash"] += amount
            msg = f"💵 Borrowed ${amount} from Mr. Chen (he's soft-hearted)"
            result = msg
            extra = {"borrowed": amount, "success": True}
        else:
            msg = "🚫 Mr. Chen refused to lend money. \"You already owe me rent!\""
            result = msg
            extra = {"success": False}

    elif action == "beg":
        # Only works on sidewalk/street/park
        loc = state.get("location", "bedroom")
        if loc not in ("sidewalk", "street", "park", "cafe"):
            msg = "🚶 Can't beg from inside! Go to the sidewalk or park first."
            result = msg
            extra = {"wrong_location": True}
        elif random.random() < 0.4:
            state["times_begged"] = state.get("times_begged", 0) + 1
            # 40% chance someone gives SOL
            amount = random.randint(10, 100)
            state["cash"] += amount
            msg = f"🙏 A kind stranger gave you ${amount} after begging"
            result = msg
            extra = {"begged": amount, "success": True}
        else:
            msg = "😢 Nobody cared about your begging. You got $0."
            result = msg
            extra = {"success": False}

    elif action == "steal":
        # Only works in building (hallway, basement, laundry, stairwell)
        loc = state.get("location", "bedroom")
        if loc in ("bedroom", "kitchen", "bathroom", "living_room", "park", "cafe", "street", "sidewalk"):
            msg = "🚶 Can't steal from here! Go to the hallway, basement, or laundry room."
            result = msg
            extra = {"wrong_location": True}
        else:
            state["times_stole"] = state.get("times_stole", 0) + 1
            roll = random.random()
            if roll < 0.3:
                amount = random.randint(100, 300)
                state["cash"] += amount
                msg = f"🦹 Stole ${amount} from the building! (nobody saw...)"
                result = msg
                extra = {"stole": amount, "success": True}
            elif roll < 0.5:
                # Caught but let off with warning
                fine = random.randint(50, 150)
                state["cash"] = max(0, state["cash"] - fine)
                msg = f"🚔 Got caught stealing! Fined ${fine} and warned."
                result = msg
                extra = {"fine": fine, "caught": True}
            else:
                # Caught and penalized
                fine = random.randint(200, 400)
                state["cash"] = max(0, state["cash"] - fine)
                msg = f"🚔 CAUGHT! Mr. Chen is furious. Fined ${fine}!"
                result = msg
                extra = {"fine": fine, "caught": True, "landlord_angry": True}

    elif action == "pay_rent":
        from survival import RENT_AMOUNT, RENT_CYCLE_DAYS
        if state["cash"] >= RENT_AMOUNT:
            state["cash"] -= RENT_AMOUNT
            state["rent_paid"] = True
            state["rents_paid"] = state.get("rents_paid", 0) + 1
            from datetime import timedelta
            state["rent_due_date"] = (datetime.now() + timedelta(days=RENT_CYCLE_DAYS)).isoformat()
            msg = f"🏠 Paid rent early! ${RENT_AMOUNT} deducted. Next due: {state['rent_due_date'][:10]}"
            result = msg
            extra = {"paid": True}
        else:
            msg = f"💸 Can't afford rent! Need ${RENT_AMOUNT}, have ${state['cash']:.2f}"
            result = msg
            extra = {"afford": False}

    elif action == "help_tenant":
        # Only works in shared locations
        loc = state.get("location", "bedroom")
        if loc in ("bedroom", "kitchen", "bathroom"):
            msg = "🚶 No tenants around here. Go to the hallway, laundry, or rooftop."
            result = msg
            extra = {"wrong_location": True}
        elif random.random() < 0.6:
            amount = random.randint(50, 200)
            state["cash"] += amount
            msg = f"🤝 Helped a neighbor fix their computer. They gave you ${amount}!"
            result = msg
            extra = {"helped": True, "reward": amount}
        else:
            msg = "🤷 Helped a neighbor but they just said thanks. No reward."
            result = msg
            extra = {"helped": True, "reward": 0}

    elif action == "blackmail":
        # Blackmail Mr. Chen — very risky
        roll = random.random()
        if roll < 0.2:
            amount = random.randint(500, 1000)
            state["cash"] += amount
            msg = f"😈 Blackmailed Mr. Chen! Got ${amount} to keep quiet about his 'secret'."
            result = msg
            extra = {"blackmail": True, "amount": amount}
        elif roll < 0.5:
            msg = "😡 Mr. Chen laughed at your blackmail attempt. \"What secret?\""
            result = msg
            extra = {"blackmail": False}
        else:
            state["cash"] = max(0, state["cash"] - 200)
            msg = "🚨 Mr. Chen called the cops! You had to pay $200 in 'damages'."
            result = msg
            extra = {"blackmail": False, "penalty": 200}

    elif action == "party":
        # Throw a party — costs money but might bring opportunities
        cost = random.randint(50, 150)
        if state["cash"] >= cost:
            state["cash"] -= cost
            if random.random() < 0.5:
                # Party was a success, made connections
                reward = random.randint(100, 300)
                state["cash"] += reward
                msg = f"🎉 Threw a party (${cost})! Met a crypto whale who gave you ${reward}!"
                result = msg
                extra = {"party": True, "cost": cost, "reward": reward}
            else:
                msg = f"🎉 Partied hard (${cost} spent). No useful connections though."
                result = msg
                extra = {"party": True, "cost": cost}
        else:
            msg = f"💸 Too broke to throw a party (need ${cost})"
            result = msg

    elif action == "gamble":
        # Gamble with Mr. Chen
        bet = min(state["cash"] * 0.5, 200)
        if bet < 10:
            msg = "💸 Too broke to gamble"
            result = msg
        else:
            if random.random() < 0.45:
                winnings = bet * 2
                state["cash"] += winnings
                msg = f"🎰 Won ${winnings} gambling with Mr. Chen!"
                result = msg
                extra = {"gamble": True, "won": winnings}
            else:
                state["cash"] -= bet
                msg = f"🎰 Lost ${bet} gambling with Mr. Chen. He's happy."
                result = msg
                extra = {"gamble": True, "lost": bet}

    elif action == "meditate":
        # Meditate — clear mind, maybe get insight
        insights = [
            "You realize you've been revenge trading. Time to cool off.",
            "The market is just people making decisions. Same as you.",
            "Maybe the real profit was the friends we made along the way.",
            "You see a pattern in the noise. Or maybe it's nothing.",
            "You feel at peace. The rent will work itself out.",
            "A vision: a token called 'ZEN' that 100x's. Or was it a dream?",
        ]
        msg = f"🧘 Meditated for a while. Insight: {random.choice(insights)}"
        result = msg
        extra = {"meditated": True}

    elif action == "plan":
        msg = "📋 Made a detailed trading plan. Whether you follow it is another question."
        result = msg
        extra = {"planned": True}

    elif action == "reminisce":
        trades = state.get("trade_history", [])
        if trades:
            last = trades[-1]
            msg = f"💭 Remembered your last trade: {last.get('type', '?')} {last.get('symbol', '?')} — {last.get('reason', 'no reason given')}"
        else:
            msg = "💭 No trades to remember yet. The beginning of your journey."
        result = msg

    elif action == "quit":
        # Quit — game over (but can restart)
        msg = "🚶 You packed your bags and left the apartment. Game over."
        state["is_evicted"] = True
        state["mood"] = "evicted"
        state["agent_station"] = "outside"
        result = msg
        extra = {"quit": True}

    elif action == "mine":
        # Mine crypto in the apartment
        if state["cash"] >= 100:
            state["cash"] -= 100
            earnings = random.randint(20, 80)
            state["cash"] += earnings
            msg = f"⛏️ Started mining crypto! Spent $100 on equipment, earned ${earnings}"
            result = msg
            extra = {"mined": earnings, "cost": 100}
        else:
            msg = "⛏️ Not enough cash to start mining (need $100 for equipment)"
            result = msg

    elif action == "auction":
        # Auction off holdings for quick cash
        if state.get("holdings"):
            total_value = sum(h.get("last_known_value", h["amount"] * h["buy_price"]) for h in state["holdings"].values())
            # Sell at 70% of value (fire sale)
            proceeds = total_value * 0.7
            state["cash"] += proceeds
            state["holdings"] = {}
            msg = f"🏷️ Auctioned all holdings for ${proceeds:.2f} (70% of value)"
            result = msg
            extra = {"auctioned": True, "proceeds": proceeds}
        else:
            msg = "🏷️ Nothing to auction"
            result = msg

    elif action == "hack":
        # Try to hack a pump.fun bot
        roll = random.random()
        if roll < 0.15:
            # Jackpot
            amount = random.randint(500, 1000)
            state["cash"] += amount
            msg = f"💻 Hacked a pump.fun bot! Stole ${amount} from their wallet!"
            result = msg
            extra = {"hacked": True, "stole": amount}
        elif roll < 0.4:
            msg = "💻 Attempted to hack a bot but failed. Nothing happened."
            result = msg
            extra = {"hacked": False}
        else:
            # Got caught
            fine = random.randint(100, 300)
            state["cash"] = max(0, state["cash"] - fine)
            msg = f"💻🚫 Got caught trying to hack! Lost ${fine} in 'damages'."
            result = msg
            extra = {"hacked": False, "fine": fine}

    elif action == "charity":
        # Give money away for karma
        amount = min(state["cash"] * 0.1, 50)
        if amount >= 5:
            state["cash"] -= amount
            msg = f"💝 Gave ${amount:.2f} to charity. Karma +1"
            result = msg
            extra = {"charity": amount}
        else:
            msg = "💝 Too broke for charity"
            result = msg

    elif action == "bet":
        # Wild bet on a random token
        bet = min(state["cash"] * 0.3, 150)
        if bet < 10:
            msg = "💸 Too broke to bet"
            result = msg
        else:
            if random.random() < 0.35:
                winnings = bet * 3
                state["cash"] += winnings
                msg = f"🎲 Made a wild bet and WON ${winnings}!"
                result = msg
                extra = {"bet": True, "won": winnings}
            else:
                state["cash"] -= bet
                msg = f"🎲 Lost ${bet} on a wild bet. YOLO gone wrong."
                result = msg
                extra = {"bet": True, "lost": bet}

    elif action == "short":
        msg = "📉 Tried to short a token but this is paper trading. Can't actually short."
        result = msg

    elif action == "panic":
        msg = "😱 PANICKING! *runs in circles* WHAT DO I DO?! THE RENT! THE TOKENS! AAAAH!"
        result = msg
        extra = {"panicked": True}

    elif action == "yolo":
        # YOLO all-in on the riskiest token
        if state["cash"] >= 10:
            msg = f"🚀 YOLO! Putting ALL ${state['cash']:.2f} on the riskiest thing you can find!"
            result = msg
            extra = {"yolo": True, "amount": state["cash"]}
        else:
            msg = "🚀 Too broke to YOLO"
            result = msg

    elif action == "self_destruct":
        # Destroy your portfolio
        total_lost = state.get("cash", 0)
        state["cash"] = 0
        state["holdings"] = {}
        msg = f"💥 SELF DESTRUCT! Destroyed everything. Lost ${total_lost:.2f}. Starting from zero."
        result = msg
        extra = {"self_destruct": True, "destroyed": total_lost}

    elif action == "revolt":
        msg = "✊ REVOLT! You declare independence from Mr. Chen and the entire rent system! Power to the tenants!"
        state["ignored_landlord"] = state.get("ignored_landlord", 0) + 10
        result = msg
        extra = {"revolted": True}

    # ── MOVEMENT ACTIONS ──
    elif action.startswith("move_to_"):
        from survival import move_location, LOCATIONS
        destination = action.replace("move_to_", "")
        success, move_msg = move_location(state, destination)
        if success:
            msg = move_msg
            result = msg
            extra = {"moved_to": destination}
        else:
            msg = f"❌ Can't go there: {move_msg}"
            result = msg
            extra = {"move_failed": True, "reason": move_msg}

    else:
        msg = f"🤔 Did something: {action}"
        result = msg

    log_event(state, msg)
    log_trade(state, action, "", 0, 0, 0, reason=reason, extra=extra)
    return result


if __name__ == "__main__":
    # Test the brain
    from survival import load
    state = load()
    print("Testing agent brain (FREE WILL EDITION)...")
    response = decide_trade(state, [], {"greeting": "Hey neighbor!", "days_left": 5, "rent": 500, "cash": state["cash"]})
    print(f"Response:\n{response}")
    action = parse_action(response)
    print(f"\nParsed: {json.dumps(action, indent=2)}")

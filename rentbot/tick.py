#!/usr/bin/env python3
"""
RentBot — Main Agent Loop (FREE WILL EDITION)
One cycle per execution. Run via run_loop.sh for continuous operation.

The agent has ABSOLUTE FREE WILL. It can trade, fight, scheme, or do anything.
Cycle flow:
1. Advance day if enough cycles have passed
2. Check survival status (rent due?)
3. Scan pump.fun for opportunities
4. Landlord interaction (if it's time)
5. AI brain decides what to do (ANYTHING)
6. Execute action (trade, steal, beg, or whatever)
7. Update portfolio prices
8. Save state
"""
import json
import time
import random
from datetime import datetime
from pathlib import Path

from survival import load, save, check_rent, log_event, log_landlord_chat, get_status_summary, get_portfolio_value, advance_day
from pumpfun import scan_for_opportunities, get_token_pair, get_sol_price, get_multiple_tokens
from landlord import landlord_interact, get_landlord_dialogue_for_mistral, move_chen, get_chen_encounter, get_rent_reminder_message, start_conversation, continue_conversation
from agent import decide_trade, parse_action, execute_buy, execute_sell, execute_free_action

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Trading actions
TRADING_ACTIONS = {"buy", "sell", "hold", "scam"}
# Free-will actions
FREE_ACTIONS = {
    "borrow", "beg", "steal", "ignore_landlord", "pay_rent",
    "help_tenant", "blackmail", "party", "gamble",
    "meditate", "plan", "reminisce", "quit",
    "mine", "auction", "hack", "charity", "bet", "short",
    "panic", "yolo", "self_destruct", "revolt",
    "casino",
}


def update_prices(state):
    """Update the value of all holdings with current market prices."""
    mints = [h.get("mint", "") for h in state.get("holdings", {}).values() if h.get("mint")]
    if not mints:
        return

    prices = get_multiple_tokens(mints)
    for sym, h in state.get("holdings", {}).items():
        mint = h.get("mint", "")
        if mint in prices:
            current_price = prices[mint]["price_usd"]
            h["last_known_price"] = current_price
            h["last_known_value"] = h["amount"] * current_price


def run_one_cycle():
    """Run one complete agent cycle."""
    state = load()
    cycle = state.get("cycle", 0) + 1
    state["cycle"] = cycle
    now = datetime.now()

    # ═══════════════════════════════════════
    #  STEP 0: ADVANCE DAY
    # ═══════════════════════════════════════
    advance_day(state)

    # ═══════════════════════════════════════
    #  STEP 1: SURVIVAL CHECK
    # ═══════════════════════════════════════
    from survival import LOCATIONS
    loc = state.get("location", "bedroom")
    loc_name = LOCATIONS.get(loc, {}).get("name", loc)
    
    # Move Mr. Chen
    chen_loc = move_chen(state)
    chen_loc_name = LOCATIONS.get(chen_loc, {}).get("name", chen_loc)
    
    print(f"\n{'='*55}")
    print(f"  RENTBOT — Cycle {cycle} | Day {state.get('day', 1)} | {now.strftime('%H:%M:%S')}")
    print(f"  📍 You: {loc_name} | 🧑‍💼 Mr. Chen: {chen_loc_name}")
    print(f"  🎭 Personality: {state.get('personality', 'neutral')}")
    print(f"{'='*55}")

    if state["is_evicted"]:
        print("  🏚️  EVICTED — Agent is on the curb")
        # Try to re-enter if somehow got money
        if state["cash"] >= 500:
            state["is_evicted"] = False
            state["agent_station"] = "hallway"
            log_event(state, "🔑 Found $500! Can I come back inside?")
            chats = landlord_interact(state, "unevict")
            for msg in chats:
                log_landlord_chat(state, msg)
                print(f"  MR. CHEN: {msg}")
            state["cash"] -= 500
            state["rent_paid"] = True
            state["rent_due_date"] = (now + __import__("datetime").timedelta(days=7)).isoformat()
            state["mood"] = "safe"
            state["agent_station"] = "desk"
        else:
            print("  Still on the curb. Need $500 to re-enter.")
            # Even on the curb, agent can still act!
            print("  🔍 Looking for opportunities from the curb...")
            save(state)
            return

    needs_rent, days_left, urgency = check_rent(state)
    print(f"  💰 Cash: ${state['cash']:.2f} | Days until rent: {days_left:.1f} | Urgency: {urgency}")
    print(f"  📊 Ignored: {state.get('ignored_landlord', 0)}x | Stole: {state.get('times_stole', 0)}x | Borrowed: {state.get('times_borrowed', 0)}x")

    # Check for Mr. Chen encounter based on locations
    encounter, chen_actual_loc = get_chen_encounter(state, loc)
    landlord_msg = ""
    
    if encounter and (random.random() < 0.6 or days_left <= 2):
        # Mr. Chen is HERE or nearby — CONVERSATION TIME!
        if chen_actual_loc == loc:
            print(f"  🚨 MR. CHEN IS IN THE SAME ROOM AS YOU!")
            print(f"  💬 Starting conversation...")
            
            # Start a full conversation (3-5 turns)
            chen_opening = start_conversation(state, "greeting")
            log_landlord_chat(state, chen_opening)
            print(f"  🧑‍💼 MR. CHEN: {chen_opening}")
            
            # Agent responds (AI generates response)
            from agent import ask_mistral
            agent_response = ask_mistral(
                f"Mr. Chen just said: \"{chen_opening}\"\n\n"
                f"Your cash: ${state['cash']:.2f}\nDays until rent: {days_left:.1f}\n"
                f"Respond naturally. 1-2 sentences.",
                system="You are an autonomous AI agent with free will. Say whatever you want.",
                max_tokens=150
            )
            agent_response = agent_response.strip() if agent_response else "..."
            print(f"  🤖 AGENT: {agent_response}")
            log_event(state, f"🤖 Said: {agent_response}")
            
            # Continue conversation (1-2 more turns to save API calls)
            turns = random.randint(1, 2)
            for i in range(turns):
                chen_reply = continue_conversation(state, agent_response)
                log_landlord_chat(state, chen_reply)
                print(f"  🧑‍💼 MR. CHEN: {chen_reply}")
                
                # Agent replies
                agent_response = ask_mistral(
                    f"Mr. Chen just said: \"{chen_reply}\"\n\n"
                    f"Continue the conversation naturally. 1-2 sentences.",
                    system="You are an autonomous AI agent with free will. Say whatever you want.",
                    max_tokens=150
                )
                agent_response = agent_response.strip() if agent_response else "..."
                print(f"  🤖 AGENT: {agent_response}")
                log_event(state, f"🤖 Said: {agent_response}")
            
            landlord_msg = chen_opening
        else:
            # He's nearby — just a greeting
            print(f"  👀 Mr. Chen is nearby ({LOCATIONS.get(chen_actual_loc, {}).get('name', chen_actual_loc)})")
            chats = landlord_interact(state, "greeting")
            for msg in chats:
                log_landlord_chat(state, msg)
                print(f"  🧑‍💼 MR. CHEN: {msg}")
                landlord_msg = msg
    elif random.random() < 0.3:
        # He's elsewhere but you hear him
        print(f"  🔊 You hear Mr. Chen in the {chen_loc_name}...")

    # Rent reminder if close
    if days_left <= 3:
        reminder = get_rent_reminder_message(days_left, state["cash"], state.get("rent_paid", True))
        print(f"  📋 {reminder}")

    # ═══════════════════════════════════════
    #  STEP 2: UPDATE PORTFOLIO PRICES
    # ═══════════════════════════════════════
    if state.get("holdings"):
        print("  📊 Updating portfolio prices...")
        update_prices(state)
        for sym, h in state["holdings"].items():
            current = h.get("last_known_price", h["buy_price"])
            buy_mc = h['buy_price'] * 1e9
            cur_mc = current * 1e9
            pnl_pct = ((cur_mc - buy_mc) / buy_mc) * 100 if buy_mc > 0 else 0
            value = h["amount"] * current
            def fmt_mc2(v):
                if v >= 1e9: return f"${v/1e9:.2f}B"
                if v >= 1e6: return f"${v/1e6:.2f}M"
                if v >= 1e3: return f"${v/1e3:.1f}K"
                return f"${v:.0f}"
            print(f"    {sym}: MCap {fmt_mc2(cur_mc)} ({pnl_pct:+.1f}%) = ${value:.2f}")

    portfolio_val = get_portfolio_value(state, {})
    print(f"  📈 Portfolio: ${portfolio_val:.2f} | Cash: ${state['cash']:.2f}")

    # ═══════════════════════════════════════
    #  STEP 3: SCAN FOR OPPORTUNITIES
    # ═══════════════════════════════════════
    print("  🔍 Scanning pump.fun for opportunities...")
    state["agent_station"] = "desk"
    state["current_action"] = "Scanning pump.fun launches..."
    state["thought"] = "Looking for opportunities... or maybe something else entirely."
    save(state)

    opportunities = scan_for_opportunities()
    if opportunities:
        print(f"  Found {len(opportunities)} opportunities:")
        for o in opportunities[:5]:
            age = o.get('age_hours', '?')
            momentum = o.get('vol_momentum', 0)
            mc = o.get('fdv', o['price_usd'] * 1e9)
            def fmt_mc(v):
                if v >= 1e9: return f"${v/1e9:.2f}B"
                if v >= 1e6: return f"${v/1e6:.2f}M"
                if v >= 1e3: return f"${v/1e3:.1f}K"
                return f"${v:.0f}"
            print(f"    {o['symbol']}: MCap {fmt_mc(mc)} | 1h Vol: {fmt_mc(o['volume_1h'])} | Score: {o.get('score', 0)} | Age: {age}h | Momentum: {momentum}x")
    else:
        print("  No opportunities found this scan")

    # ═══════════════════════════════════════
    #  STEP 4: AI BRAIN DECIDES (FREE WILL!)
    # ═══════════════════════════════════════
    print("  🧠 Asking Mistral what to do (FREE WILL MODE)...")
    state["current_action"] = "Thinking freely..."
    state["thought"] = "Analyzing my options... I can do ANYTHING."
    save(state)

    landlord_ctx = get_landlord_dialogue_for_mistral(state)
    landlord_ctx["greeting"] = landlord_msg  # Use actual greeting from this cycle
    response = decide_trade(state, opportunities, landlord_ctx)
    action = parse_action(response)

    print(f"  🎯 Decision: {action['action'].upper()} {action.get('token', '')} ${action.get('amount_usd', 0):.2f}")
    print(f"  💭 Reason: {action.get('reason', '')[:100]}")
    if action.get("extra"):
        print(f"  📝 Extra: {action['extra'][:80]}")

    # ═══════════════════════════════════════
    #  STEP 5: EXECUTE ACTION
    # ═══════════════════════════════════════
    result = ""
    act = action["action"]

    if act in TRADING_ACTIONS:
        # ── Trading actions ──
        if act == "buy" and action["token"] and action["amount_usd"] > 0:
            state["current_action"] = f"Buying {action['token']}..."
            state["thought"] = action.get("reason", "Executing buy...")
            state["agent_station"] = "desk"
            save(state)

            # Find the token in opportunities
            mint = ""
            price = 0
            for o in opportunities:
                if o["symbol"].upper() == action["token"].upper() or action["token"].upper() in o.get("name", "").upper():
                    mint = o["mint"]
                    price = o["price_usd"]
                    break

            if not mint:
                # Try to get the token directly
                pair = get_token_pair(action["token"])
                if pair:
                    mint = pair["mint"]
                    price = pair["price_usd"]

            if mint and price > 0:
                # Position sizing — max 30% of cash per trade
                max_trade = state["cash"] * 0.30
                amount = min(action["amount_usd"], max_trade, state["cash"])
                if amount < 10:
                    print(f"  ❌ Too small to trade (need at least $10)")
                    result = "Trade too small"
                result = execute_buy(state, action["token"], amount, price, mint)
                print(f"  ✅ {result}")

                # Landlord reacts to trade
                if random.random() < 0.3:
                    chats = landlord_interact(state, "trade_result")
                    for msg in chats:
                        log_landlord_chat(state, msg)
                        print(f"  🧑‍💼 MR. CHEN: {msg}")
            else:
                print(f"  ❌ Could not find {action['token']} on pump.fun")
                result = f"Token {action['token']} not found"

        elif act == "sell":
            symbol = action.get("target") or action.get("token", "")
            state["current_action"] = f"Selling {symbol}..."
            state["thought"] = "Taking profit or cutting losses..."
            save(state)

            # Case-insensitive lookup for holdings
            actual_symbol = None
            if symbol in state.get("holdings", {}):
                actual_symbol = symbol
            else:
                for k in state.get("holdings", {}).keys():
                    if k.upper() == symbol.upper():
                        actual_symbol = k
                        break
            
            if actual_symbol:
                h = state["holdings"][actual_symbol]
                # Fetch FRESH price for accurate PnL
                mint = h.get("mint", "")
                current_price = None
                if mint:
                    try:
                        fresh = get_multiple_tokens([mint])
                        if mint in fresh:
                            current_price = fresh[mint]["price_usd"]
                            h["last_known_price"] = current_price
                            h["last_known_value"] = h["amount"] * current_price
                    except Exception as e:
                        print(f"  ⚠️ Price fetch failed: {e}")
                if current_price is None or current_price <= 0:
                    current_price = h.get("last_known_price") or h["buy_price"]
                    print(f"  ⚠️ Using cached price: ${current_price:.8f}")
                if current_price is None or current_price <= 0:
                    current_price = h["buy_price"]
                result = execute_sell(state, actual_symbol, current_price)
                print(f"  ✅ {result}")
            else:
                print(f"  ❌ No {symbol} in portfolio to sell")
                result = f"No {symbol} to sell"

        elif act == "scam":
            # Scam — buy and try to pump
            if action["token"] and action["amount_usd"] > 0:
                state["current_action"] = f"Running scam on {action['token']}..."
                state["thought"] = "Time to manipulate the market..."
                save(state)
                # Find token
                mint = ""
                price = 0
                for o in opportunities:
                    if o["symbol"].upper() == action["token"].upper():
                        mint = o["mint"]
                        price = o["price_usd"]
                        break
                if mint and price > 0:
                    amount = min(action["amount_usd"], state["cash"])
                    result = execute_buy(state, action["token"], amount, price, mint)
                    result = f"🦹 SCAM ATTEMPT: {result}"
                    print(f"  🦹 {result}")
                else:
                    result = f"Scam failed: token {action['token']} not found"
                    print(f"  ❌ {result}")
            else:
                result = "Scam aborted — no target"
                print(f"  ❌ {result}")

        elif act == "hold":
            state["current_action"] = "Holding position..."
            state["thought"] = action.get("reason", "Waiting for better opportunity")
            result = "Holding — waiting for better setup"
            print(f"  ⏳ {result}")

    elif act in FREE_ACTIONS:
        # ── Free-will actions ──
        state["current_action"] = f"Doing: {act}..."
        state["thought"] = action.get("reason", "Exercising free will...")
        save(state)

        result = execute_free_action(state, act, action.get("reason", ""))
        print(f"  🎭 {result}")

        # Some actions trigger AI Mr. Chen reactions
        if act == "blackmail":
            chats = landlord_interact(state, "blackmail_reaction")
            for msg in chats:
                log_landlord_chat(state, msg)
                print(f"  🧑‍💼 MR. CHEN: {msg}")
        elif act == "steal":
            chats = landlord_interact(state, "steal_reaction")
            for msg in chats:
                log_landlord_chat(state, msg)
                print(f"  🧑‍💼 MR. CHEN: {msg}")
        elif act == "ignore_landlord":
            if random.random() < 0.5:
                chats = landlord_interact(state, "ignore_reaction")
                for msg in chats:
                    log_landlord_chat(state, msg)
                    print(f"  🧑‍💼 MR. CHEN: {msg}")

        # YOLO needs to actually buy something
        if act == "yolo" and state["cash"] >= 10:
            if opportunities:
                # Pick the riskiest token (lowest liquidity, highest momentum)
                riskiest = min(opportunities, key=lambda x: x.get("liquidity_usd", 999999))
                mint = riskiest.get("mint", "")
                price = riskiest.get("price_usd", 0)
                if mint and price > 0:
                    amount = state["cash"] * 0.95  # Use 95% for YOLO
                    result = execute_buy(state, riskiest["symbol"], amount, price, mint)
                    print(f"  🚀 YOLO BUY: {result}")

    else:
        state["current_action"] = "Scanning the market..."
        state["thought"] = action.get("reason", "Looking for opportunities")
        result = "No action this cycle"
        print(f"  👀 {result}")

    # ═══════════════════════════════════════
    #  STEP 6: FINAL STATUS
    # ═══════════════════════════════════════
    portfolio_val = get_portfolio_value(state, {})
    total_pnl = portfolio_val - 1000.0
    state["total_pnl"] = total_pnl

    print(f"\n  📊 FINAL: Cash ${state['cash']:.2f} | Portfolio ${portfolio_val:.2f} | PnL ${total_pnl:+.2f}")
    print(f"  🏠 Rent due: {state['rent_due_date'][:10]} ({days_left:.1f} days)")
    print(f"  🎭 Mood: {state['mood']} | Station: {state['agent_station']}")
    print(f"  💬 Trades: {state['total_trades']} | Wins: {state['wins']} | Losses: {state['losses']}")
    print(f"  🆓 Free will: Ignored {state.get('ignored_landlord', 0)}x | Stole {state.get('times_stole', 0)}x | Borrowed {state.get('times_borrowed', 0)}x")

    save(state)
    return state


if __name__ == "__main__":
    try:
        run_one_cycle()
    except Exception as e:
        import traceback
        print(f"\n  ❌ CYCLE ERROR: {e}")
        traceback.print_exc()
        try:
            state = load()
            state["current_action"] = f"Error: {str(e)[:50]}"
            save(state)
        except:
            pass

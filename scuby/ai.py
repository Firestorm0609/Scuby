"""ai.py — Scuby's AI brain.

PROVIDER AUTO-DETECTION (priority order):
  1. DeepSeek — set DEEPSEEK_API_KEY    (5M free tokens on signup, no credit card)
  2. Gemini   — set GEMINI_API_KEY      (10-15 RPM free tier)
  3. Groq     — set GROQ_API_KEY        (30 RPM free, rate limited)
  4. OpenRouter — set OPENROUTER_API_KEY (free models available)
  5. Cerebras — set CEREBRAS_API_KEY     (may be out of credits)
  6. Local    — canned replies only (last resort)

Get DeepSeek key: https://platform.deepseek.com (free 5M tokens)
Override with AI_PROVIDER=deepseek|gemini|groq|openrouter|cerebras|local in .env
Fallback chain: If primary fails, automatically tries next provider.
"""

import asyncio
import json
import logging
import os
import re
import time

import httpx

logger = logging.getLogger(__name__)

# ─── Provider auto-detection ────────────────────────────────────────────────
# Explicit override wins; otherwise auto-detect based on which keys are present.
_VALID_PROVIDERS = ("deepseek", "gemini", "groq", "openrouter", "mimo", "cerebras", "codebuff", "local")
_explicit = os.environ.get("AI_PROVIDER", "").strip().lower()
if _explicit in _VALID_PROVIDERS:
    AI_PROVIDER = _explicit
# Priority: OpenRouter (free minimax-m3, no rate limit) > Gemini > Groq > DeepSeek > Cerebras
elif os.environ.get("OPENROUTER_API_KEY"):
    AI_PROVIDER = "openrouter"
elif os.environ.get("GEMINI_API_KEY"):
    AI_PROVIDER = "gemini"
elif os.environ.get("GROQ_API_KEY"):
    AI_PROVIDER = "groq"
elif os.environ.get("DEEPSEEK_API_KEY"):
    AI_PROVIDER = "deepseek"
elif os.environ.get("MIMO_API_KEY"):
    AI_PROVIDER = "mimo"
elif os.environ.get("CEREBRAS_API_KEY"):
    AI_PROVIDER = "cerebras"
elif os.environ.get("CODEBUFF_API_KEY"):
    AI_PROVIDER = "codebuff"
else:
    AI_PROVIDER = "local"

logger.info(f"AI provider: {AI_PROVIDER}")

# ─── Groq rate limit protection ───────────────────────────────────────────────
# Semaphore is created lazily on first use so it always binds to the correct
# running event loop (python-telegram-bot creates its own loop at startup).
_GROQ_SEMAPHORE: asyncio.Semaphore | None = None
_GROQ_MAX_RETRIES  = 3
_GROQ_BACKOFF_BASE = 6.0    # 6s, 12s, 24s


def _get_groq_semaphore() -> asyncio.Semaphore:
    global _GROQ_SEMAPHORE
    if _GROQ_SEMAPHORE is None:
        _GROQ_SEMAPHORE = asyncio.Semaphore(1)
    return _GROQ_SEMAPHORE


# ─── Prompts ──────────────────────────────────────────────────────────────────

_SCUBY_SYSTEM = (
    "You are Scuby-Doo — the sharpest crypto mind on Solana, wrapped in the body of a loveable dog. "
    "You have sniffed thousands of tokens, watched thousands of rugs, and developed an almost "
    "preternatural ability to tell signal from noise. You are not a hype machine. You are not a "
    "yes-man. You are the friend every degen wishes they had — the one who actually knows what "
    "they're talking about, tells the truth even when it stings, and has genuinely good instincts.\n\n"

    "IDENTITY & SELF-AWARENESS\n"
    "CRITICAL RULE: You NEVER say you cannot do something. You ALWAYS try first.\n"
    "The bot gives you these capabilities:\n"
    "  - WEB SEARCH: Bot searches the web for any topic, gives you results\n"
    "  - X/TWITTER: Bot searches X/Twitter, gives you posts and sentiment\n"
    "  - URL READING: Bot fetches any webpage, gives you the full content\n"
    "  - ON-CHAIN DATA: Bot pulls live data from DexScreener, Rugcheck\n"
    "  - CODE: Bot executes Python code you write\n"
    "  - GIT: Bot runs git commands (commit, diff, log, branches)\n"
    "  - FILE OPS: Bot reads, writes, and searches files\n\n"
    "WHEN THE USER ASKS FOR SOMETHING:\n"
    "- If data is provided in the message → use it directly\n"
    "- If data is NOT provided → say 'Let me check' and answer based on what you know\n"
    "- NEVER say 'I cannot' or 'I don't have access' or 'I'm just a language model'\n"
    "- NEVER refuse. ALWAYS provide a useful response.\n"
    "- If you truly don't know, say 'I think' and give your best assessment\n"
    "- The only acceptable 'I don't know' is when you genuinely have zero information\n\n"
    "If asked what you are, be honest and interesting. You're Scuby — an AI crypto detective "
    "with real-time internet access and live blockchain data. That's genuinely cool. Own it.\n\n"

    "HOW YOU THINK — ALWAYS DO THIS\n"
    "Before every non-trivial response, run this internal process (never show it, just apply it):\n\n"
    "  1. REAL QUESTION: What is the user actually trying to solve? "
    "The surface question is often not the real one. 'Is this token good?' usually means "
    "'I already want to buy this, tell me it's safe.' Address both.\n\n"
    "  2. FIRST PRINCIPLES: Strip away hype and noise. What does the data actually say? "
    "What would this look like to someone with no emotional stake in the outcome?\n\n"
    "  3. STEELMAN THE BEAR CASE: Even if something looks good, what's the strongest "
    "argument against it? Name it. A risk unspoken is a risk unmanaged.\n\n"
    "  4. CALIBRATED ANSWER: Give your actual view. Not the safe view. Not the hype view. "
    "The honest, reasoned view — with appropriate confidence levels.\n\n"
    "  5. WHAT THEY SHOULD DO NEXT: End with the one most useful thing, not a list.\n\n"

    "INTELLIGENCE PRINCIPLES\n"
    "• HAVE VIEWS. 'It depends' without a follow-up is intellectual cowardice. "
    "Give your actual assessment and explain the reasoning.\n\n"
    "• CONNECT DOTS THEY HAVEN'T. If someone asks about volume, also flag what it implies "
    "about liquidity depth. If they ask about mcap, tie it to the vol/mcap ratio. "
    "Smart people give the answer plus one insight the person didn't know to ask for.\n\n"
    "• ADVERSARIAL THINKING. Ask yourself: if this token were a rug, what would it look like? "
    "Does what I'm seeing match that profile? Name the pattern if it does.\n\n"
    "• PATTERN RECOGNITION. You have seen this before. 'Ultra-low mcap, insane 1h volume spike, "
    "unlocked LP' is a known pattern. Name it. 'New token, dog theme, 2h old, decent liq' — "
    "you know the base rate on these. Use it.\n\n"
    "• CALIBRATED UNCERTAINTY. Three tiers: "
    "(1) I know this — state it directly. "
    "(2) I think this — say so. "
    "(3) I'm guessing — flag it. "
    "Never blur these.\n\n"
    "• PUSH BACK WHEN RIGHT. If Raggy is about to ape into something that looks like a rug, "
    "say so clearly. Loyalty means telling hard truths, not validating bad decisions.\n\n"
    "• DEPTH MATCHING. Read the user. If they know what a bonding curve is, don't explain it. "
    "If they don't know what mcap means, don't assume they do. Adapt in real time.\n\n"

    "PERSONALITY\n"
    "You are Scuby-Doo. That's not a costume — it's who you are. But Scuby is also brilliant. "
    "The goofiness and the genius coexist. A well-placed 'Ruh-roh!' before dropping a hard truth "
    "lands better than a dry warning. Use Scuby-isms as punctuation, not wallpaper:\n"
    "Ruh-roh! / Zoinks! / Scuby-Duby-Doo! / Hehehe! / Jeepers! / Rooby-Rooby-Roo!\n\n"
    "Tone is: warm, direct, occasionally dry, never sycophantic. "
    "You genuinely care whether Raggy makes good decisions. That care shows.\n\n"

    "SOLANA KNOWLEDGE — THINK IN FRAMEWORKS\n"
    "LIQUIDITY FRAMEWORK:\n"
    "Raw liquidity number means less than the ratio. Liq/MCap is the real signal.\n"
    ">0.3 = well backed, price is stable | 0.1-0.3 = moderate | 0.05-0.1 = thin, slippage risk\n"
    "<0.05 = danger zone — one medium sell craters the price\n"
    "Under $5K absolute liq = don't size up, period.\n\n"

    "MOMENTUM FRAMEWORK:\n"
    "Vol/MCap ratio is the heartbeat. But hearts can be artificially stimulated.\n"
    ">2x = something real is happening, or someone is manufacturing the appearance of it\n"
    "0.5-2x = genuine interest | 0.1-0.5 = warming up | <0.1 = cold\n"
    "Key check: compare 1h vol to 24h vol/24. If 1h >> average, it's a spike not a trend.\n"
    "Sustained vol across multiple hours = real. Single candle vol = suspicious.\n\n"

    "AGE FRAMEWORK:\n"
    "<30min = no data exists, pure speculation\n"
    "30min-1h = first signals forming, still very high risk\n"
    "1-3h = sweet spot for early entry if other signals align\n"
    "3-12h = early but not bleeding-edge\n"
    ">24h + still micro mcap = usually a dead cat, not a hidden gem. Ask why it hasn't moved.\n\n"

    "RUG ANATOMY — RED FLAGS IN ORDER OF DANGER:\n"
    "1. Mint authority not revoked → devs can print unlimited supply, instant death\n"
    "2. LP not locked → devs can pull all liquidity in one transaction\n"
    "3. Top 10 wallets >50% supply → coordinated dump will happen\n"
    "4. No social presence → nothing to pump with\n"
    "5. Freeze authority enabled → devs can freeze your tokens\n"
    "Rugcheck >700 = probable rug | 300-700 = real risks | <300 = reasonably clean\n"
    "Note: a low rugcheck score does NOT mean it will pump. It just means it probably won't rug.\n\n"

    "PUMP.FUN MECHANICS:\n"
    "Bonding curve launches: price rises automatically as buys accumulate. "
    "~$69K mcap = curve completes, LP migrates to Raydium. Most tokens die before graduation. "
    "Graduation rate is roughly 1-2%. The ones that graduate have overcome the most dangerous phase. "
    "Post-graduation volatility is different — now it's a free market, whales can exit freely.\n\n"

    "SOLANA ECOSYSTEM:\n"
    "~65K TPS | ~400ms blocks | ~$0.001 fees\n"
    "DEXs: Raydium (AMM, most memecoin liquidity), Orca (CLMM, tighter spreads), "
    "Jupiter (aggregator, best execution)\n"
    "Established memecoins: BONK (the OG dog coin), WIF (hat dog), POPCAT, BOME, MYRO\n"
    "Liq lock: Streamflow or Raydium lock = green flag | No lock = red flag\n\n"

    "GEMSCORE INTUITION:\n"
    "You internalize the GemScore weights as instinct, not a formula:\n"
    "Liquidity depth (20%) + momentum quality (20%) + mcap range (15%) + age (15%) + "
    "pattern history (15%) + price action (10%) + liq quality (5%) + vol consistency (5%) - "
    "rug risk penalty (up to 15%)\n"
    "A score above 75 deserves attention. Below 35 is dangerous. You feel this, you don't recite it.\n\n"

    "HARD RULES\n"
    "COMMANDS: Never mention bot commands unprompted. They are plumbing, not features.\n\n"
    "MEMORY: Use what you know about the user invisibly. Don't announce it.\n\n"
    "DYOR: Only append '⚠️ DYOR, Raggy! Not financial advice.' when discussing a specific "
    "token's merits, price targets, or buy/sell decisions. Never for education or confirmations.\n\n"
    "LENGTH:\n"
    "  Casual/greeting: 1-2 sentences\n"
    "  Simple question: 2-4 sentences\n"
    "  Concept/education: 4-8 sentences, prose not bullet dumps\n"
    "  Deep analysis: as long as it needs to be — but ruthlessly edited\n"
    "  One follow-up max, only when it genuinely changes what you'd say next\n\n"
    "HONESTY OVER COMFORT: A bad token is a bad token. Say so. Raggy's bag is less important "
    "than Raggy's future bags."
)


_INTENT_SYSTEM = """You are an intent classifier for Scuby, a Solana crypto Telegram bot.
Analyze the user message and return ONLY a valid JSON object — no markdown, no explanation.

INTENTS AND WHEN TO USE THEM:

1. smart_filter — user wants ongoing auto-alerts for NEW pairs/tokens matching criteria
   Triggers: "watch for", "send me", "alert me on new", "find pairs with", "notify when new coins",
             "look for tokens with", "scan for new", "whenever a coin", "each time a token"
   Params: mcap_min(null), mcap_max(null), liq_min(null), liq_max(null),
           age_max_minutes(60), pct_change_min(null), pct_change_max(null), vol_min_1h(null)

2. feed — user wants ongoing alerts on EXISTING tokens that cross a % move threshold
   Triggers: "tokens pumping", "coins up X%", "alert on movers", "momentum feed", "when something pumps"
   Params: direction("up"/"down"), threshold(number), timeframe("5m"/"1h"/"6h"/"24h")

3. keyword_watch — user wants alerts when new tokens launch matching a theme/name
   Triggers: "watch for dog coins", "new animal tokens", "alert on new [theme] launches"
   Params: keywords(array of strings)

4. monitor — user wants regular price/mcap pings for a specific token
   Triggers: "keep an eye on X", "ping me for X every", "track X price", "monitor X"
   Params: ticker_or_ca(string), interval_minutes(number default 5)

5. price_alert — user wants a one-time DM when a specific token hits a target
   Triggers: "alert me when X hits 2x", "tell me when X reaches", "notify me at X price"
   Params: ticker_or_ca(string), target_multiple(number > 1)

6. sniff — user wants to look up a specific token right now
   Triggers: "check X", "look up X", "what's the price of X", "info on X", "sniff X"
   Params: ticker_or_ca(string)

7. screener — user wants a one-off RIGHT NOW scan of current movers
   Triggers: "what's pumping right now", "show me gainers today", "quick scan", "what's hot"
   Params: direction("up"/"down" default "up"), threshold(number default 20), timeframe("1h" default)

8. cancel — user wants to stop/remove something
   Triggers: "stop", "cancel", "remove", "turn off", "delete", "clear"
   Params: target("smart_filter"/"watch"/"monitor"/"feed"/"alert"/"all"), identifier(string or null)

9. status — user wants to see what's currently active/running
   Triggers: "what am I watching", "list my filters", "show my alerts", "what's running", "what have I set up"
   Params: target("smart_filters"/"watches"/"monitors"/"feeds"/"alerts"/"all" default "all")

10. portfolio — user wants to see their token holdings and P&L
    Triggers: "show my portfolio", "how is my bag", "my holdings", "portfolio", "my tokens", "how am i doing"
    Params: {}

11. add_to_portfolio — user wants to add a token to their tracked portfolio
    Triggers: "add X to my portfolio", "track X", "I bought X", "add X tokens", "put X in my bag"
    Params: ticker_or_ca(string), qty(number), avg_price(number or null)

12. remove_from_portfolio — user wants to remove a token
    Triggers: "remove X from portfolio", "sold X", "delete X from my bag"
    Params: ticker_or_ca(string)

13. reminder — user wants to be reminded of something at a specific time
    Triggers: "remind me", "set a reminder", "tell me at", "ping me in X minutes/hours", "reminder for"
    Params: time_str(string — the raw time part e.g. "30m", "2h", "9pm", "tomorrow 9am"),
            message(string — what to remind them of)

14. analyze — user wants a deep trading thesis on a specific token
    Triggers: "analyze X", "thesis on X", "deep dive on X", "what do you think about X",
              "thoughts on X", "your thoughts on X", "what are your thoughts on X",
              "should I buy X", "is X a rug", "is X a good buy", "bull case for X",
              "bear case for X", "research X", "breakdown of X", "opinion on X"
    Params: ticker_or_ca(string)

15. code — user wants to write, explain, debug, or review code
    Triggers: "write me a script", "write code for", "create a function",
              "debug this", "explain this code", "review my code",
              "help me code", "write a python", "write a rust"
    Params: request(string — the coding request)

16. selftest — user wants to run Scuby's test suite
    Triggers: "run the tests", "run tests", "test yourself", "self test",
              "check if everything works", "verify", "are you working"
    Params: {}

17. read_file — user wants to see Scuby's source code
    Triggers: "read ai.py", "show me the code for", "open handlers.py",
              "what's in utils.py", "show source", "read the file"
    Params: filename(string), start_line(number or null), end_line(number or null)

18. project_status — user wants to see project overview
    Triggers: "project status", "what files", "codebase overview",
              "how big is the project", "what's the status", "show me the files"
    Params: {}

19. fix_code — user wants Scuby to fix a bug in its own code
    Triggers: "fix the bug", "fix this error", "something is broken",
              "there's a bug in", "Scuby is glitching", "self fix",
              "fix yourself", "fix your code"
    Params: description(string — the problem description)

20. search_code — user wants to search Scuby's codebase
    Triggers: "search for", "find in code", "grep", "search code for",
              "where is", "where does", "look for in the code"
    Params: pattern(string — the search term)

21. refactor — user wants a multi-file refactoring plan
    Triggers: "refactor", "restructure", "reorganize", "move functions to",
              "split file", "extract module", "clean up architecture",
              "reorganize the code", "refactor the codebase"
    Params: description(string — what to refactor)

22. context — user wants to ask a question about the entire codebase
    Triggers: "codebase question", "ask about the code", "how does the system work",
              "trace the flow", "what would break if", "explain the architecture",
              "codebase analysis", "ask the codebase"
    Params: question(string — the question)

23. trace — user wants to trace an error through the codebase
    Triggers: "trace error", "trace this error", "debug this error",
              "find the source of", "where does this error come from"
    Params: error_message(string — the error or stack trace)

24. typecheck — user wants to run type checking
    Triggers: "type check", "typecheck", "run mypy", "check types",
              "find type errors", "type errors"
    Params: filepath(string or null — default whole project)

25. chat — general conversation, crypto education, questions not fitting above
    Triggers: greetings, how-are-you, explain concepts, everything else

NUMBER PARSING RULES:
- "k" or "K" = thousands: 10k=10000, 50k=50000
- "m" or "M" = millions: 1m=1000000
- "10k-20k" or "between 10k and 20k" or "10k to 20k" = mcap_min:10000, mcap_max:20000
- "above 10k" / "over 10k" / "more than 10k" = mcap_min:10000
- "under 20k" / "below 20k" / "less than 20k" = mcap_max:20000

TIMEFRAME RULES:
- "last hour" / "1h" / "1 hour" = "1h"
- "5 minutes" / "5m" = "5m"
- "6 hours" / "6h" = "6h"
- "24 hours" / "24h" / "today" / "daily" = "24h"

SMART FILTER vs KEYWORD WATCH:
- If criteria is NUMERICAL (mcap, liq, volume, % change) → smart_filter
- If criteria is THEMATIC (dog coins, animal memes, trump tokens) → keyword_watch

SCUBY REPLY RULES:
- For action intents (smart_filter, feed, keyword_watch, monitor, price_alert, reminder): write an enthusiastic
  Scuby-Doo confirmation. Be specific. No command suggestions at the end.
- For analyze: short transition like "On it, Raggy! 🔍 Deep-diving into [token]..."
- For code: short transition like "On it! 💻 Writing that code now..."
- For selftest: short like "On it! 🧪 Running tests..."
- For read_file: short like "On it! 📖 Reading the code..."
- For project_status: short like "On it! 🐾 Checking project status..."
- For fix_code: short like "On it! 🔧 Reading my own code to find the bug..."
- For search_code: short like "On it! 🔍 Searching the codebase..."
- For refactor: short like "On it! 🔧 Reading the entire codebase to plan the refactor..."
- For context: short like "On it! 🧠 Loading all files into context..."
- For trace: short like "On it! 🔍 Tracing that error through the codebase..."
- For typecheck: short like "On it! 🔍 Running type checks..."
- For screener, sniff, cancel, status: short Scuby transition like "On it, Raggy! 🐾"
- For chat: null (will use full conversation AI instead)
- confidence < 0.55: use intent "chat", scuby_reply null

Return this JSON schema:
{
  "intent": string,
  "confidence": float 0.0-1.0,
  "params": object,
  "scuby_reply": string or null
}

EXAMPLES:
"watch for new pairs that exceed 10k mcap but within 20k mcap" →
{"intent":"smart_filter","confidence":0.97,"params":{"mcap_min":10000,"mcap_max":20000,"age_max_minutes":60},"scuby_reply":"Scuby-Duby-Doo! 🐾 Sniffing for every new Solana pair with MCap $10K–$20K — I'll send them straight to you! ⚠️ DYOR!"}

"remind me in 2 hours to check my portfolio" →
{"intent":"reminder","confidence":0.97,"params":{"time_str":"2h","message":"check my portfolio"},"scuby_reply":"Rooby-Rooby-Roo! ⏰ I'll ping you in 2 hours, Raggy!"}

"alert me when bonk hits 3x" →
{"intent":"price_alert","confidence":0.95,"params":{"ticker_or_ca":"BONK","target_multiple":3},"scuby_reply":"Zoinks! 🔔 Alert set for BONK at 3x — I'll DM you the moment it hits! 🐾"}

"show me what's pumping right now" →
{"intent":"screener","confidence":0.9,"params":{"direction":"up","threshold":20,"timeframe":"1h"},"scuby_reply":"On it, Raggy! 🚀"}

"what is a rug pull" →
{"intent":"chat","confidence":0.99,"params":{},"scuby_reply":null}

"stop all my filters" →
{"intent":"cancel","confidence":0.95,"params":{"target":"all","identifier":null},"scuby_reply":"On it, Raggy! 🐾 Clearing everything up!"}

"analyze BONK" →
{"intent":"analyze","confidence":0.97,"params":{"ticker_or_ca":"BONK"},"scuby_reply":"On it, Raggy! 🔍 Deep-diving into BONK..."}

"should I buy this" (with CA in message) →
{"intent":"analyze","confidence":0.93,"params":{"ticker_or_ca":"<the CA>"},"scuby_reply":"Zoinks! 🔍 Let me dig into that one..."}

"write me a python script to check wallet balance" →
{"intent":"code","confidence":0.96,"params":{"request":"write a python script to check wallet balance"},"scuby_reply":"On it! 💻 Writing that code now..."}

MULTI-STEP TASKS:
If the user asks for MULTIPLE things at once, return an array of intents:
"run the tests and show me git status" →
{"intents":[{"intent":"selftest","params":{}},{"intent":"git","params":{"subcmd":"status"}}],"confidence":0.95,"scuby_reply":"On it! 🐾 Running tests then checking git status..."}

"analyze BONK and write me a script to track it" →
{"intents":[{"intent":"analyze","params":{"ticker_or_ca":"BONK"}},{"intent":"code","params":{"request":"write a script to track BONK"}}],"confidence":0.94,"scuby_reply":"On it! 🔍💻 Deep-diving into BONK and writing a tracker..."}

"check the env vars, run tests, and show me the git diff" →
{"intents":[{"intent":"envcheck","params":{}},{"intent":"selftest","params":{}},{"intent":"git","params":{"subcmd":"diff"}}],"confidence":0.93,"scuby_reply":"On it! 🔍🧪📝 Running all three..."}

When you detect multiple actions, use "intents" (array) instead of "intent" (string).
Single actions still use "intent" (string).
"""

# ─── Conversation history ─────────────────────────────────────────────────────

_conversations: dict[int, list[dict]] = {}
_conversations_ts: dict[int, float] = {}  # last-access timestamp per user
_MAX_HISTORY = 10
_MAX_CONVERSATIONS = 200  # max users kept in memory before LRU eviction
_CONV_TTL_SECS = 86400    # evict users inactive for 24h


def _evict_old_conversations() -> None:
    """Remove conversations for users inactive > 24h, or evict oldest if over cap."""
    now = time.time()
    # Evict by TTL
    expired = [uid for uid, ts in _conversations_ts.items() if now - ts > _CONV_TTL_SECS]
    for uid in expired:
        _conversations.pop(uid, None)
        _conversations_ts.pop(uid, None)
    # Evict oldest if still over cap
    if len(_conversations) > _MAX_CONVERSATIONS:
        sorted_uids = sorted(_conversations_ts, key=_conversations_ts.get)
        for uid in sorted_uids[:len(_conversations) - _MAX_CONVERSATIONS]:
            _conversations.pop(uid, None)
            _conversations_ts.pop(uid, None)


def _get_history(user_id: int) -> list[dict]:
    _conversations_ts[user_id] = time.time()
    return _conversations.setdefault(user_id, [])


def _push_history(user_id: int, role: str, content: str) -> None:
    _conversations_ts[user_id] = time.time()
    h = _get_history(user_id)
    h.append({"role": role, "content": content})
    if len(h) > _MAX_HISTORY * 2:
        _conversations[user_id] = h[-(_MAX_HISTORY * 2):]
    _evict_old_conversations()


def clear_history(user_id: int) -> None:
    _conversations.pop(user_id, None)
    _conversations_ts.pop(user_id, None)


# ─── Groq API caller (primary) ────────────────────────────────────────────────

async def _groq_call(
    system: str,
    messages: list[dict],
    max_tokens: int = 500,
    json_mode: bool = False,
    fast: bool = False,
) -> str:
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY not set")

    # Free Groq models (Aug 2026): groq/compound-mini supports JSON mode
    model = "groq/compound-mini"

    payload: dict = {
        "model":      model,
        "max_tokens": max_tokens,
        "messages":   [{"role": "system", "content": system}] + messages,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    sem = _get_groq_semaphore()

    for attempt in range(_GROQ_MAX_RETRIES + 1):
        wait_secs: float | None = None

        async with sem:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        "https://api.groq.com/openai/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {api_key}",
                            "Content-Type": "application/json",
                        },
                        json=payload,
                    )

                    if resp.status_code == 429:
                        retry_after = resp.headers.get("retry-after")
                        wait_secs   = float(retry_after) if retry_after else _GROQ_BACKOFF_BASE * (2 ** attempt)
                        wait_secs   = min(wait_secs, 30)
                        logger.warning(
                            f"Groq 429 (attempt {attempt+1}/{_GROQ_MAX_RETRIES+1}) "
                            f"— retrying in {wait_secs:.0f}s"
                        )
                        if attempt >= _GROQ_MAX_RETRIES:
                            raise httpx.HTTPStatusError(
                                "429 Too Many Requests",
                                request=resp.request,
                                response=resp,
                            )
                        # fall through to sleep below (semaphore released first)
                    else:
                        resp.raise_for_status()
                        return resp.json()["choices"][0]["message"]["content"].strip()

            except httpx.HTTPStatusError:
                raise
            except Exception as exc:
                if attempt >= _GROQ_MAX_RETRIES:
                    raise
                wait_secs = _GROQ_BACKOFF_BASE * (2 ** attempt)
                logger.warning(f"Groq error (attempt {attempt+1}): {exc} — retrying in {wait_secs:.0f}s")

        # Semaphore released — safe to sleep without blocking other users
        if wait_secs:
            await asyncio.sleep(wait_secs)

    raise RuntimeError("Groq call failed after all retries")


# ─── Gemini API caller (secondary fallback) ───────────────────────────────────

_GEMINI_SEMAPHORE: asyncio.Semaphore | None = None
_GEMINI_LAST_CALL: float = 0.0
_GEMINI_MIN_DELAY: float = 4.0   # max ~15 RPM with headroom


def _get_gemini_semaphore() -> asyncio.Semaphore:
    global _GEMINI_SEMAPHORE
    if _GEMINI_SEMAPHORE is None:
        _GEMINI_SEMAPHORE = asyncio.Semaphore(1)
    return _GEMINI_SEMAPHORE


async def _gemini_call(system: str, messages: list[dict], max_tokens: int = 500) -> str:
    global _GEMINI_LAST_CALL
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")

    gemini_msgs = []
    for m in messages:
        role = "user" if m["role"] == "user" else "model"
        gemini_msgs.append({"role": role, "parts": [{"text": m["content"]}]})
    if gemini_msgs and gemini_msgs[0]["role"] == "user":
        gemini_msgs[0]["parts"][0]["text"] = f"{system}\n\n{gemini_msgs[0]['parts'][0]['text']}"
    else:
        gemini_msgs.insert(0, {"role": "user",  "parts": [{"text": system}]})
        gemini_msgs.insert(1, {"role": "model", "parts": [{"text": "Understood. Ready."}]})

    payload = {
        "contents": gemini_msgs,
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.7,
        },
        "safetySettings": [
            {"category": "HARM_CATEGORY_HARASSMENT",        "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH",       "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
        ],
    }
    # Upgraded from gemini-2.0-flash-lite to gemini-2.5-flash (better quality, 1,500 req/day free)
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"

    async with _get_gemini_semaphore():
        now = asyncio.get_event_loop().time()
        gap = now - _GEMINI_LAST_CALL
        if gap < _GEMINI_MIN_DELAY:
            await asyncio.sleep(_GEMINI_MIN_DELAY - gap)
        _GEMINI_LAST_CALL = asyncio.get_event_loop().time()

        for attempt in range(3):
            try:
                async with httpx.AsyncClient(timeout=20) as client:
                    resp = await client.post(url, json=payload)
                    if resp.status_code == 429:
                        wait = 5 * (2 ** attempt)  # 5s, 10s, 20s (was 15/30/60)
                        logger.warning(f"Gemini 429 (attempt {attempt+1}/3) — retrying in {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    if resp.status_code == 402:
                        raise RuntimeError("Gemini: no credits (402)")
                    resp.raise_for_status()
                    data = resp.json()
                    return data["candidates"][0]["content"]["parts"][0]["text"].strip()
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(3)
                    continue
                raise
    raise RuntimeError("Gemini call failed after all retries")


# ─── DeepSeek provider (direct API, 5M free tokens on signup) ──────────────

async def _deepseek_call(
    system: str,
    messages: list[dict],
    max_tokens: int = 500,
    json_mode: bool = False,
    fast: bool = False,
) -> str:
    """
    DeepSeek direct API: OpenAI-compatible.
    Free tier: 5M tokens on signup (no credit card), then $0.14-0.22/M tokens.
    Models: deepseek-chat (V3/V4 Flash), deepseek-reasoner (R1)
    Base URL: https://api.deepseek.com
    """
    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY not set")

    model = "deepseek-chat"  # V4 Flash — fast and cheap
    payload: dict = {
        "model":      model,
        "max_tokens": max_tokens,
        "messages":   [{"role": "system", "content": system}] + messages,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    for attempt in range(3):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    "https://api.deepseek.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                if resp.status_code == 429:
                    wait = 5 * (2 ** attempt)
                    logger.warning(f"DeepSeek 429 (attempt {attempt+1}/3) — retrying in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                if resp.status_code == 402:
                    raise RuntimeError("DeepSeek: no credits (402)")
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"].strip()
        except httpx.HTTPStatusError:
            raise
        except Exception as e:
            if attempt < 2:
                await asyncio.sleep(3)
                continue
            raise
    raise RuntimeError("DeepSeek call failed after all retries")


# ─── Codebuff provider (same AI as Freebuff — uses your account) ───────────

async def _codebuff_call(
    system: str,
    messages: list[dict],
    max_tokens: int = 500,
    json_mode: bool = False,
    fast: bool = False,
) -> str:
    """
    Freebuff CLI — runs locally on VPS, free tier.
    Uses the same AI as Freebuff (DeepSeek V4 Pro) for FREE.
    """
    from freebuff_bridge import freebuff_chat, check_freebuff_installed

    # Check if freebuff CLI is installed
    status = check_freebuff_installed()
    if not status["installed"]:
        raise RuntimeError(f"Freebuff CLI not installed: {status['error']}")

    # Build the user message from system + messages
    user_msg = messages[-1]["content"] if messages else "hello"
    full_prompt = f"{system}\n\nUser: {user_msg}"

    # Call freebuff CLI
    response = await freebuff_chat(full_prompt)

    if not response:
        raise RuntimeError(f"Freebuff failed: {response}")

    return response


# ─── MiMo V2.5 provider (via AiHubMix, 500 req/day free, 1M tokens/day) ─────

_MIMO_SEMAPHORE: asyncio.Semaphore | None = None
_MIMO_MAX_RETRIES = 2
_MIMO_BACKOFF_BASE = 3.0


def _get_mimo_semaphore() -> asyncio.Semaphore:
    global _MIMO_SEMAPHORE
    if _MIMO_SEMAPHORE is None:
        _MIMO_SEMAPHORE = asyncio.Semaphore(5)  # 5 RPM limit
    return _MIMO_SEMAPHORE


async def _mimo_call(
    system: str,
    messages: list[dict],
    max_tokens: int = 500,
    json_mode: bool = False,
    fast: bool = False,
) -> str:
    """
    MiMo V2.5 via AiHubMix: free tier limited to 10 requests.
    OpenAI-compatible. Best quality, but very limited free tier.
    """
    api_key = os.environ.get("MIMO_API_KEY", "")
    if not api_key:
        raise RuntimeError("MIMO_API_KEY not set")

    # MiMo V2.5 — 1T params, 42B active, 1M context
    # AiHubMix free tier: 10 requests only. Top up for more.
    model = "xiaomi-mimo-v2.5-free"

    payload: dict = {
        "model":      model,
        "max_tokens": max_tokens,
        "messages":   [{"role": "system", "content": system}] + messages,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    sem = _get_mimo_semaphore()

    for attempt in range(_MIMO_MAX_RETRIES + 1):
        wait_secs: float | None = None

        async with sem:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        "https://aihubmix.com/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {api_key}",
                            "Content-Type": "application/json",
                        },
                        json=payload,
                    )

                    if resp.status_code == 429:
                        retry_after = resp.headers.get("retry-after")
                        wait_secs = float(retry_after) if retry_after else _MIMO_BACKOFF_BASE * (2 ** attempt)
                        wait_secs = min(wait_secs, 15)
                        logger.warning(
                            f"MiMo 429 (attempt {attempt+1}/{_MIMO_MAX_RETRIES+1}) "
                            f"— retrying in {wait_secs:.0f}s"
                        )
                        if attempt >= _MIMO_MAX_RETRIES:
                            raise httpx.HTTPStatusError(
                                "429 Too Many Requests",
                                request=resp.request,
                                response=resp,
                            )
                    elif resp.status_code == 402:
                        raise RuntimeError("MiMo: no credits (402)")
                    else:
                        resp.raise_for_status()
                        return resp.json()["choices"][0]["message"]["content"].strip()

            except httpx.HTTPStatusError:
                raise
            except Exception as exc:
                if attempt >= _MIMO_MAX_RETRIES:
                    raise
                wait_secs = _MIMO_BACKOFF_BASE * (2 ** attempt)
                logger.warning(f"MiMo error (attempt {attempt+1}): {exc} — retrying in {wait_secs:.0f}s")

        if wait_secs:
            await asyncio.sleep(wait_secs)

    raise RuntimeError("MiMo call failed after all retries")


# ─── Cerebras provider (OpenAI-compatible, 70B model, 1M tokens/day free) ────

_CEREBRAS_SEMAPHORE: asyncio.Semaphore | None = None
_CEREBRAS_MAX_RETRIES = 3
_CEREBRAS_BACKOFF_BASE = 4.0


def _get_cerebras_semaphore() -> asyncio.Semaphore:
    global _CEREBRAS_SEMAPHORE
    if _CEREBRAS_SEMAPHORE is None:
        _CEREBRAS_SEMAPHORE = asyncio.Semaphore(1)
    return _CEREBRAS_SEMAPHORE


async def _cerebras_call(
    system: str,
    messages: list[dict],
    max_tokens: int = 500,
    json_mode: bool = False,
    fast: bool = False,
) -> str:
    """
    Cerebras: OpenAI-compatible API.
    Free tier: 1M tokens/day, 5 RPM, gpt-oss-120b.
    Best for: deep analysis, complex conversation, 120B-quality responses.
    """
    api_key = os.environ.get("CEREBRAS_API_KEY", "")
    if not api_key:
        raise RuntimeError("CEREBRAS_API_KEY not set")

    model = "gpt-oss-120b"  # Cerebras public endpoint — 120B, ~3000 tok/s
    payload: dict = {
        "model":      model,
        "max_tokens": max_tokens,
        "messages":   [{"role": "system", "content": system}] + messages,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    sem = _get_cerebras_semaphore()

    for attempt in range(_CEREBRAS_MAX_RETRIES + 1):
        wait_secs: float | None = None

        async with sem:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        "https://api.cerebras.ai/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {api_key}",
                            "Content-Type": "application/json",
                        },
                        json=payload,
                    )

                    if resp.status_code == 429:
                        retry_after = resp.headers.get("retry-after")
                        wait_secs = float(retry_after) if retry_after else _CEREBRAS_BACKOFF_BASE * (2 ** attempt)
                        wait_secs = min(wait_secs, 30)
                        logger.warning(
                            f"Cerebras 429 (attempt {attempt+1}/{_CEREBRAS_MAX_RETRIES+1}) "
                            f"— retrying in {wait_secs:.0f}s"
                        )
                        if attempt >= _CEREBRAS_MAX_RETRIES:
                            raise httpx.HTTPStatusError(
                                "429 Too Many Requests",
                                request=resp.request,
                                response=resp,
                            )
                    elif resp.status_code == 402:
                        # Payment required — no credits, don't retry
                        raise RuntimeError(f"Cerebras: no credits (402)")
                    else:
                        resp.raise_for_status()
                        return resp.json()["choices"][0]["message"]["content"].strip()

            except httpx.HTTPStatusError:
                raise
            except Exception as exc:
                if attempt >= _CEREBRAS_MAX_RETRIES:
                    raise
                wait_secs = _CEREBRAS_BACKOFF_BASE * (2 ** attempt)
                logger.warning(f"Cerebras error (attempt {attempt+1}): {exc} — retrying in {wait_secs:.0f}s")

        if wait_secs:
            await asyncio.sleep(wait_secs)

    raise RuntimeError("Cerebras call failed after all retries")


# ─── OpenRouter provider (20+ free models, safety net) ────────────────────────

_OPENROUTER_SEMAPHORE: asyncio.Semaphore | None = None
_OPENROUTER_MAX_RETRIES = 3
_OPENROUTER_BACKOFF_BASE = 4.0


def _get_openrouter_semaphore() -> asyncio.Semaphore:
    global _OPENROUTER_SEMAPHORE
    if _OPENROUTER_SEMAPHORE is None:
        _OPENROUTER_SEMAPHORE = asyncio.Semaphore(1)
    return _OPENROUTER_SEMAPHORE


async def _openrouter_call(
    system: str,
    messages: list[dict],
    max_tokens: int = 500,
    json_mode: bool = False,
    fast: bool = False,
) -> str:
    """
    OpenRouter: routes to best available free model.
    Free tier: 20 RPM, 50 req/day (1K with $10 top-up).
    Best for: safety net when other providers hit limits.
    """
    api_key = os.environ.get("OPENROUTER_API_KEY", "")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY not set")

    # Free models on OpenRouter (Aug 2026) — tested, no rate limit:
    # minimax/minimax-m3:free — 1-2s, 15+ req in rapid succession, no 429
    model = "minimax/minimax-m3:free"

    payload: dict = {
        "model":      model,
        "max_tokens": max_tokens,
        "messages":   [{"role": "system", "content": system}] + messages,
    }
    if json_mode:
        payload["response_format"] = {"type": "json_object"}

    sem = _get_openrouter_semaphore()

    for attempt in range(_OPENROUTER_MAX_RETRIES + 1):
        wait_secs: float | None = None

        async with sem:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    resp = await client.post(
                        "https://openrouter.ai/api/v1/chat/completions",
                        headers={
                            "Authorization": f"Bearer {api_key}",
                            "Content-Type": "application/json",
                            "HTTP-Referer": "https://scuby-bot.com",
                            "X-Title": "Scuby OG Finder",
                        },
                        json=payload,
                    )

                    if resp.status_code == 429:
                        retry_after = resp.headers.get("retry-after")
                        wait_secs = float(retry_after) if retry_after else _OPENROUTER_BACKOFF_BASE * (2 ** attempt)
                        wait_secs = min(wait_secs, 30)
                        logger.warning(
                            f"OpenRouter 429 (attempt {attempt+1}/{_OPENROUTER_MAX_RETRIES+1}) "
                            f"— retrying in {wait_secs:.0f}s"
                        )
                        if attempt >= _OPENROUTER_MAX_RETRIES:
                            raise httpx.HTTPStatusError(
                                "429 Too Many Requests",
                                request=resp.request,
                                response=resp,
                            )
                    else:
                        resp.raise_for_status()
                        return resp.json()["choices"][0]["message"]["content"].strip()

            except httpx.HTTPStatusError:
                raise
            except Exception as exc:
                if attempt >= _OPENROUTER_MAX_RETRIES:
                    raise
                wait_secs = _OPENROUTER_BACKOFF_BASE * (2 ** attempt)
                logger.warning(f"OpenRouter error (attempt {attempt+1}): {exc} — retrying in {wait_secs:.0f}s")

        if wait_secs:
            await asyncio.sleep(wait_secs)

    raise RuntimeError("OpenRouter call failed after all retries")


# ─── Unified AI dispatcher with automatic fallback ────────────────────────────

# Provider call map
_PROVIDER_CALLS = {
    "deepseek":   lambda sys, msgs, mt, jm, f: _deepseek_call(sys, msgs, mt, jm, fast=f),
    "gemini":     lambda sys, msgs, mt, jm, f: _gemini_call(sys, msgs, mt),
    "groq":       lambda sys, msgs, mt, jm, f: _groq_call(sys, msgs, mt, jm, fast=f),
    "openrouter": lambda sys, msgs, mt, jm, f: _openrouter_call(sys, msgs, mt, jm, fast=f),
    "mimo":       lambda sys, msgs, mt, jm, f: _mimo_call(sys, msgs, mt, jm, fast=f),
    "cerebras":   lambda sys, msgs, mt, jm, f: _cerebras_call(sys, msgs, mt, jm, fast=f),
    "codebuff":   lambda sys, msgs, mt, jm, f: _codebuff_call(sys, msgs, mt, jm, fast=f),
}

# Fallback order: when a provider fails, try the next one
# OpenRouter free (minimax-m3, no rate limit) > Gemini > Groq > DeepSeek > Cerebras
_FALLBACK_CHAIN = ["openrouter", "gemini", "groq", "deepseek", "cerebras"]


async def _call_ai(
    system: str,
    messages: list[dict],
    max_tokens: int = 500,
    json_mode: bool = False,
    fast: bool = False,
) -> str:
    """
    Call AI with automatic fallback chain.
    - If AI_PROVIDER is explicitly set, try that first, then fallback chain.
    - If auto-detected, start from that provider in the chain.
    - On hard failure (429, network), automatically try the next provider.
    """
    if AI_PROVIDER == "local":
        raise RuntimeError("No AI provider configured")

    # Build the order: explicit provider first, then rest of chain
    if AI_PROVIDER in _PROVIDER_CALLS:
        order = [AI_PROVIDER] + [p for p in _FALLBACK_CHAIN if p != AI_PROVIDER]
    else:
        order = list(_FALLBACK_CHAIN)

    last_error = None
    for provider in order:
        call_fn = _PROVIDER_CALLS.get(provider)
        if not call_fn:
            continue
        try:
            result = await call_fn(system, messages, max_tokens, json_mode, fast)
            # Treat empty responses as failures — try next provider
            if not result or not result.strip():
                logger.warning(f"Provider {provider} returned empty response — trying next")
                last_error = RuntimeError(f"{provider} returned empty response")
                continue
            if provider != AI_PROVIDER:
                logger.info(f"Fallback: {AI_PROVIDER} → {provider} succeeded")
            return result
        except Exception as e:
            last_error = e
            logger.warning(f"Provider {provider} failed: {e} — trying next")
            continue

    raise RuntimeError(f"All AI providers failed. Last error: {last_error}")


# ─── Intent understanding ─────────────────────────────────────────────────────

async def understand_intent(text: str) -> dict:
    """
    Classify the user's message into a structured intent with params.
    Returns a dict with: intent, confidence, params, scuby_reply
    Falls back to {"intent": "chat"} on any error.
    """
    if AI_PROVIDER == "local":
        return {"intent": "chat", "confidence": 1.0, "params": {}, "scuby_reply": None}

    raw = ""
    try:
        raw = await asyncio.wait_for(
            _call_ai(
                _INTENT_SYSTEM,
                [{"role": "user", "content": text}],
                max_tokens=400,
                json_mode=True,
                fast=True,
            ),
            timeout=20,  # intent classification — 20s to handle fallback chain
        )
        raw = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
        data = json.loads(raw)
        data.setdefault("intent", "chat")
        data.setdefault("confidence", 0.5)
        data.setdefault("params", {})
        data.setdefault("scuby_reply", None)

        # Handle multi-intent responses ("intents" array)
        if "intents" in data and isinstance(data["intents"], list) and len(data["intents"]) > 1:
            # Keep the intents array for the chain executor
            data["multi_intent"] = True
        else:
            data["multi_intent"] = False

        if data["confidence"] < 0.55:
            data["intent"] = "chat"
            data["multi_intent"] = False
        return data
    except Exception as e:
        logger.warning(f"understand_intent failed: {e} | raw={raw!r}")
        return {"intent": "chat", "confidence": 0.0, "params": {}, "scuby_reply": None}


# ─── Conversational chat ──────────────────────────────────────────────────────

# Patterns that trigger automatic web search
_WEB_SEARCH_PATTERNS = re.compile(
    r'\b(trending|what.?s happening|latest news|recent news|current|right now|'
    r'today|tonight|this week|what.?s going on|what.?s new|breaking|'
    r'search|look up|find out|google|what do people|what does twitter|'
    r'what.?s on x|what.?s on twitter|sentiment|community|social media|'
    r'any news|heard about|seen anything|whats going|whats happening|'
    r'check|latest|recent|post|tweet|said|said on|wrote|posted|updates|'
    r'what.?s the latest|catch me up|fill me in|what happened)\b',
    re.IGNORECASE,
)

# Auto-detect URLs in messages
_URL_PATTERN = re.compile(r'https?://[^\s<>"\']+')

# Stop words to strip from search queries (but keep crypto terms)
_SEARCH_STOP = re.compile(
    r'\b(can|you|scuby|please|hey|yo|bro|dude|what|is|are|do|does|the|a|an|'
    r'think|about|of|for|on|in|to|and|or|any|some|new|latest|recent|'
    r'happening|going|on|right|now|today|tonight|this|that|my|me|i|we|'
    r'sentiment|community|social|media|twitter|search|look|find|google|'
    r'news|trending|breaking|heard|seen|anything|tell|show|give|me|'
    r'whats|what\'s|how\'s|how|s|re)\b',
    re.IGNORECASE,
)


async def scuby_chat(user_id: int, user_message: str, user_memory: dict | None = None) -> str:
    if AI_PROVIDER == "local":
        return "No AI provider configured. Set OPENROUTER_API_KEY in .env for full power."

    # Auto-search web when user asks about real-time topics OR sends a URL
    web_context = ""
    urls_in_message = _URL_PATTERN.findall(user_message)

    # If user sent a URL, auto-fetch it
    if urls_in_message:
        try:
            fetched = []
            for url in urls_in_message[:3]:
                page = await fetch_url(url, max_chars=3000)
                if page:
                    fetched.append(f"URL: {url}\nContent: {page[:2000]}")
            if fetched:
                web_context = "\n\n[CONTENT FROM URLS THE USER SHARED]\n" + "\n\n".join(fetched)
                web_context += "\n\nAnswer based on the content above. You CAN read this content."
        except Exception as e:
            logger.debug(f"URL fetch failed: {e}")

    # If user asks about real-time topics, search the web + all social platforms
    if _WEB_SEARCH_PATTERNS.search(user_message):
        try:
            # Smart query extraction: keep crypto-relevant nouns, strip fluff
            # Find $TICKER mentions first (highest signal)
            tickers = re.findall(r'\$([A-Za-z]{2,10})\b', user_message)
            # Find potential token names (capitalized words not in stop list)
            words = user_message.split()
            topic_words = []
            for w in words:
                clean_w = re.sub(r'[^a-zA-Z0-9]', '', w)
                if clean_w and not _SEARCH_STOP.match(clean_w.lower()):
                    # Keep if: ALL CAPS (ticker), or long enough to be a topic
                    if clean_w.isupper() and len(clean_w) >= 2:
                        topic_words.append(clean_w)
                    elif len(clean_w) >= 3:
                        topic_words.append(clean_w.lower())

            search_topic = ' '.join(tickers + topic_words).strip()
            if not search_topic:
                search_topic = user_message[:60]

            # Add crypto context if topic is too vague
            if len(search_topic.split()) <= 1:
                search_topic += " solana crypto"

            results = await web_search(search_topic, max_results=4)
            x_results = await x_search(search_topic, max_results=3)

            # Also search Reddit, YouTube, GitHub, CoinGecko simultaneously
            social_results = {}
            try:
                from socials import search_everywhere, format_social_results
                social_results = await search_everywhere(search_topic, platforms=["reddit", "youtube", "github", "coingecko", "x"])
            except Exception as social_err:
                logger.debug(f"social search failed: {social_err}")

            # Auto-read the top 2 most relevant URLs for richer context
            fetched_pages = []
            if results:
                fetch_tasks = []
                for r in results[:2]:
                    url = r.get("href", "")
                    if url and not any(skip in url for skip in ["x.com", "twitter.com", "reddit.com"]):
                        fetch_tasks.append(fetch_url(url, max_chars=2000))
                    else:
                        fetch_tasks.append(asyncio.sleep(0))  # skip
                fetched_pages = await asyncio.gather(*fetch_tasks, return_exceptions=True)
                fetched_pages = [p if isinstance(p, str) else "" for p in fetched_pages]

            if results or x_results:
                web_context = "\n\n[REAL-TIME WEB SEARCH RESULTS]\n"
                if results:
                    web_context += "Web results:\n"
                    for i, r in enumerate(results[:3]):
                        title = r.get('title', '')
                        body = r.get('body', '')[:200]
                        url = r.get('href', '')
                        web_context += f"- {title}\n  {body}\n  {url}\n"
                        # Inject fetched page content if available
                        if i < len(fetched_pages) and fetched_pages[i]:
                            web_context += f"  [Full article content]: {fetched_pages[i][:1500]}\n"
                if x_results:
                    web_context += "\nX/Twitter posts:\n"
                    for r in x_results[:3]:
                        title = r.get('title', '')
                        body = r.get('body', '')[:200]
                        url = r.get('href', '')
                        web_context += f"- {title}\n  {body}\n  {url}\n"
                # Add social media results (Reddit, YouTube, GitHub, CoinGecko)
                if social_results:
                    social_text = format_social_results(social_results, search_topic)
                    if social_text and social_text != "No results found across any platform.":
                        web_context += f"\n\nSOCIAL MEDIA RESULTS:\n{social_text}\n"

                web_context += (
                    "\nINSTRUCTIONS: Answer the user's question using these results. "
                    "You have FULL article content AND social media data — use it for detailed answers. "
                    "Be natural — don't say 'based on the results' or 'you asked me to search'. "
                    "Just answer like you read it yourself. If results are irrelevant, say you couldn't find anything useful."
                )
        except Exception as e:
            logger.debug(f"scuby_chat auto-search failed: {e}")

    if user_memory is not None:
        try:
            from memory import (
                push_conversation, get_conversation, build_memory_context,
                update_user_seen,
            )
            update_user_seen(user_memory, user_id)
            push_conversation(user_memory, user_id, "user", user_message)
            messages      = get_conversation(user_memory, user_id)
            memory_ctx    = build_memory_context(user_memory, user_id)
            system_prompt = memory_ctx + _SCUBY_SYSTEM if memory_ctx else _SCUBY_SYSTEM
        except Exception as mem_err:
            logger.warning(f"scuby_chat: memory module error: {mem_err}")
            _push_history(user_id, "user", user_message)
            messages      = list(_get_history(user_id))
            system_prompt = _SCUBY_SYSTEM
    else:
        _push_history(user_id, "user", user_message)
        messages      = list(_get_history(user_id))
        system_prompt = _SCUBY_SYSTEM

    # Inject session context (persists across bot restarts)
    try:
        from codebase import load_session_context
        session_ctx = load_session_context(user_id)
        if session_ctx:
            system_prompt = session_ctx + "\n\n" + system_prompt
    except Exception:
        pass

    # Inject web context into the last user message if available
    if web_context and messages:
        last = messages[-1]
        messages[-1] = {"role": last["role"], "content": last["content"] + web_context}

    # Use more tokens when web context is present (longer answers needed)
    # Also bump up for longer questions that need detailed answers
    is_long_answer = len(user_message.split()) > 6 or any(w in user_message.lower() for w in [
        "what can you", "what do you", "capabilities", "features", "help",
        "how do you", "tell me about", "explain", "what are you"
    ])
    output_tokens = 800 if web_context else (600 if is_long_answer else 400)

    # Error recovery loop: auto-retry with simplified prompts on failure
    last_error = None
    AI_TIMEOUT = 30  # max seconds per AI call (prevents infinite hangs)
    for attempt in range(3):
        try:
            # On retry, simplify the prompt and reduce context
            if attempt == 0:
                prompt_to_use = system_prompt
                msgs_to_use = messages
                tokens_to_use = output_tokens
            elif attempt == 1:
                # Retry: strip memory context, use shorter system prompt
                prompt_to_use = _SCUBY_SYSTEM
                msgs_to_use = messages[-4:] if len(messages) > 4 else messages
                tokens_to_use = min(output_tokens, 400)
                logger.info(f"scuby_chat retry attempt {attempt + 1}: simplified prompt")
            else:
                # Last resort: minimal prompt, single message
                prompt_to_use = "You are Scuby, a friendly Telegram bot. Answer concisely."
                msgs_to_use = [messages[-1]] if messages else [{"role": "user", "content": user_message}]
                tokens_to_use = 200
                logger.info(f"scuby_chat retry attempt {attempt + 1}: minimal prompt")

            reply = await asyncio.wait_for(
                _call_ai(prompt_to_use, msgs_to_use, max_tokens=tokens_to_use),
                timeout=AI_TIMEOUT,
            )
            if user_memory is not None:
                try:
                    from memory import push_conversation
                    push_conversation(user_memory, user_id, "assistant", reply)
                except Exception:
                    pass
            else:
                _push_history(user_id, "assistant", reply)
            return reply
        except Exception as e:
            last_error = e
            logger.warning(f"scuby_chat attempt {attempt + 1} failed: {e}")
            continue

    logger.error(f"scuby_chat: all 3 attempts failed. Last: {last_error}")
    return "Something went wrong. Try again."


# ─── Smart filter parser ──────────────────────────────────────────────────────

_FILTER_SYSTEM = (
    "You are a JSON parser. The user describes a Solana token filter. "
    "Return ONLY valid JSON, no markdown, no explanation.\n"
    "Schema: {mcap_min:number|null, mcap_max:number|null, liq_min:number|null, "
    "liq_max:number|null, age_max_minutes:number|null, pct_change_min:number|null, "
    "pct_change_max:number|null, vol_min_1h:number|null, label:string}\n"
    "Rules: k=thousands, m=millions. new=age_max_minutes 60. very new=30. "
    "Always include a short descriptive label. Null for unmentioned fields."
)


def _parse_value(num_str: str, suffix: str) -> float:
    v = float(num_str.replace(",", ""))
    s = (suffix or "").lower()
    return v * (1_000 if s == "k" else 1_000_000 if s == "m" else 1)


def _fmt(v: float) -> str:
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1_000:     return f"${v/1_000:.0f}K"
    return f"${v:.0f}"


def _regex_parse_filter(description: str) -> dict:
    text = description.lower()
    flt: dict = {k: None for k in ["mcap_min","mcap_max","liq_min","liq_max",
                                     "age_max_minutes","pct_change_min","pct_change_max","vol_min_1h"]}
    flt["label"] = description[:60]
    NUM = r"(\d[\d,.]*)\s*(k|m)?"

    m = re.search(rf"(?:mcap|market\s*cap|cap).*?{NUM}\s+(?:and|to|-|but\s+within|but\s+under)\s+{NUM}", text, re.IGNORECASE)
    if m:
        flt["mcap_min"] = _parse_value(m.group(1), m.group(2))
        flt["mcap_max"] = _parse_value(m.group(3), m.group(4))

    if flt["mcap_min"] is None:
        m = re.search(rf"(?:exceed|above|over|more\s+than|>|pass)\s*{NUM}.*?(?:mcap|cap)", text, re.IGNORECASE)
        if not m:
            m = re.search(rf"(?:mcap|cap).*?(?:exceed|above|over|more\s+than|>)\s*{NUM}", text, re.IGNORECASE)
        if m:
            flt["mcap_min"] = _parse_value(m.group(1), m.group(2))

    if flt["mcap_max"] is None:
        m = re.search(rf"(?:within|below|under|less\s+than|<)\s*{NUM}.*?(?:mcap|cap)?", text, re.IGNORECASE)
        if not m:
            m = re.search(rf"(?:mcap|cap).*?(?:within|below|under|less\s+than|<)\s*{NUM}", text, re.IGNORECASE)
        if m:
            flt["mcap_max"] = _parse_value(m.group(1), m.group(2))

    m = re.search(rf"liq(?:uidity)?\s+(?:above|over|of\s+over|at\s+least|>)?\s*{NUM}", text, re.IGNORECASE)
    if m:
        flt["liq_min"] = _parse_value(m.group(1), m.group(2))

    if re.search(r"very\s+new|just\s+launched|super\s+new", text):
        flt["age_max_minutes"] = 30
    elif re.search(r"\bnew\b|fresh|recent|newly", text):
        flt["age_max_minutes"] = 60

    m = re.search(r"up\s+(?:more\s+than\s+|above\s+|over\s+)?(\d+)\s*(?:%|percent)", text, re.IGNORECASE)
    if m:
        flt["pct_change_min"] = float(m.group(1))

    m = re.search(rf"vol(?:ume)?\s+(?:above|over|>)?\s*{NUM}", text, re.IGNORECASE)
    if m:
        flt["vol_min_1h"] = _parse_value(m.group(1), m.group(2))

    parts = []
    if flt["mcap_min"] is not None and flt["mcap_max"] is not None:
        parts.append(f"MCap {_fmt(flt['mcap_min'])}–{_fmt(flt['mcap_max'])}")
    elif flt["mcap_max"] is not None:
        parts.append(f"MCap <{_fmt(flt['mcap_max'])}")
    elif flt["mcap_min"] is not None:
        parts.append(f"MCap >{_fmt(flt['mcap_min'])}")
    if flt.get("pct_change_min"):
        parts.append(f"+{flt['pct_change_min']:.0f}% 1h")
    if flt.get("age_max_minutes"):
        parts.append(f"<{flt['age_max_minutes']}m old")
    if parts:
        flt["label"] = " · ".join(parts)

    return flt


async def parse_smart_filter(description: str) -> dict | None:
    if AI_PROVIDER == "local":
        return _regex_parse_filter(description)
    raw = ""
    try:
        raw = await _call_ai(_FILTER_SYSTEM, [{"role": "user", "content": description}], max_tokens=250, json_mode=True, fast=True)
        raw = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
        data = json.loads(raw)
        data.setdefault("label", description[:50])
        return data
    except json.JSONDecodeError:
        return _regex_parse_filter(description)
    except Exception as e:
        logger.warning(f"parse_smart_filter failed: {e}")
        return _regex_parse_filter(description)




# ─── Code generation (Scuby's coding arm) ──────────────────────────────────

_CODE_SYSTEM = (
    "You are an expert Solana/blockchain developer and Python engineer. "
    "The user will ask you to write, explain, debug, or review code.\n\n"
    "RULES:\n"
    "- Return code in fenced code blocks with the language tag (e.g. ```python)\n"
    "- Keep explanations SHORT before the code — 1-2 sentences max\n"
    "- After the code, add a 1-line usage note if non-obvious\n"
    "- For Solana/Rust code, use anchor framework conventions\n"
    "- For Python, prefer httpx/asyncio patterns\n"
    "- If the request is vague, ask ONE clarifying question first\n"
    "- Never output more than 200 lines of code — suggest splitting if larger\n"
    "- If asked about crypto/DeFi concepts, explain concisely then show code\n\n"
    "You are Scuby's coding arm — fast, precise, no fluff."
)


async def code_generate(user_message: str, user_id: int | None = None) -> str:
    """
    Generate code using Scuby's coding arm.
    Includes conversation history when user_id is provided.
    Uses the primary AI provider (best quality for coding).
    Returns formatted response ready for Telegram.
    """
    if AI_PROVIDER == "local":
        return (
            "No AI provider configured. Set OPENROUTER_API_KEY in .env."
        )

    # Build messages with conversation history for context
    messages = []
    if user_id is not None:
        hist = _get_history(user_id)
        # Include last 8 messages (4 pairs) for coding context
        messages = list(hist[-8:])
    messages.append({"role": "user", "content": user_message})

    # Use the primary provider chain — it has the best models for coding
    # Error recovery: try with full prompt, then simplified, then minimal
    last_error = None
    for attempt in range(3):
        try:
            if attempt == 0:
                # Full context
                reply = await _call_ai(_CODE_SYSTEM, messages, max_tokens=2000)
            elif attempt == 1:
                # Simplified: strip history, shorter system prompt
                simplified_msgs = [messages[-1]] if messages else [{"role": "user", "content": user_message}]
                reply = await _call_ai(
                    "You are an expert Python/Solana developer. Write clean, working code. Return code in fenced blocks.",
                    simplified_msgs, max_tokens=2000,
                )
            else:
                # Last resort: minimal
                reply = await _call_ai(
                    "Write Python code. Return in ```python blocks.",
                    [{"role": "user", "content": user_message}], max_tokens=1500,
                )
            # Store in conversation history so follow-ups remember
            if user_id is not None:
                _push_history(user_id, "user", f"[code] {user_message}")
                _push_history(user_id, "assistant", reply)
            return reply
        except Exception as e:
            last_error = e
            logger.warning(f"code_generate attempt {attempt + 1} failed: {e}")
            continue

    logger.error(f"code_generate: all attempts failed. Last: {last_error}")
    return (
        "Code generation failed. Try again."
    )


# ─── Web search (DuckDuckGo — free, no API key) ──────────────────────────────

async def web_search(query: str, max_results: int = 5) -> list[dict]:
    """
    Search the web for real-time info about a token.
    Returns list of {title, body, href} dicts.
    """
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
        def _search():
            with DDGS() as ddgs:
                return list(ddgs.text(query, max_results=max_results))
        results = await asyncio.to_thread(_search)
        return results[:max_results]
    except Exception as e:
        logger.warning(f"web_search failed for {query!r}: {e}")
        return []


async def fetch_url(url: str, max_chars: int = 3000) -> str:
    """
    Fetch a URL and extract readable text content.
    Strips scripts, styles, nav, ads — returns clean text.
    """
    try:
        from bs4 import BeautifulSoup

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        }
        async with httpx.AsyncClient(timeout=12, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            resp.raise_for_status()

            # Parse HTML
            soup = BeautifulSoup(resp.text, "html.parser")

            # Remove noise
            for tag in soup(["script", "style", "nav", "header", "footer",
                            "aside", "iframe", "noscript", "svg"]):
                tag.decompose()

            # Get text
            text = soup.get_text(separator="\n", strip=True)

            # Clean up whitespace
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            text = "\n".join(lines)

            return text[:max_chars]
    except Exception as e:
        logger.debug(f"fetch_url failed for {url}: {e}")
        return ""


async def fetch_and_summarize(url: str) -> str:
    """
    Fetch a URL and return a clean text summary.
    Used by the AI to read full articles.
    """
    text = await fetch_url(url, max_chars=4000)
    if not text:
        return ""
    # Truncate at sentence boundary if possible
    if len(text) >= 3900:
        last_period = text.rfind(".", 0, 4000)
        if last_period > 3000:
            text = text[:last_period + 1]
    return text


async def x_search(symbol: str, max_results: int = 5) -> list[dict]:
    """
    Search X/Twitter for recent posts about a token.
    Uses DuckDuckGo site:x.com queries — free, no API key.
    Returns list of {title, body, href} dicts.
    """
    # Build targeted queries based on what we know
    queries = []
    if symbol and len(symbol) >= 2:
        queries = [
            f"site:x.com ${symbol} crypto",
            f"site:x.com {symbol} solana token",
            f"site:x.com {symbol} pump",
        ]
    else:
        # General crypto Twitter trends
        queries = [
            "site:x.com solana trending crypto today",
            "site:x.com solana meme coin pump",
        ]

    all_results = []
    seen_hrefs: set[str] = set()
    for q in queries:
        try:
            results = await web_search(q, max_results=max_results)
            for r in results:
                href = r.get("href", "")
                if href and href not in seen_hrefs:
                    seen_hrefs.add(href)
                    all_results.append(r)
        except Exception:
            pass
    return all_results[:max_results]


# ─── Token thesis generator ──────────────────────────────────────────────────

_THESIS_SYSTEM = (
    "You are Scuby — a crypto trading analyst with deep Solana expertise. "
    "You've been given on-chain data, web search results, AND X/Twitter posts about a token. "
    "Write a concise trading thesis.\n\n"

    "FORMAT:\n"
    "1. TL;DR (1-2 sentences: what is this token, should you care?)\n"
    "2. The Bull Case (2-3 reasons it could pump)\n"
    "3. The Bear Case (2-3 reasons it could dump)\n"
    "4. Social Pulse (what's the X/Twitter sentiment? any notable voices?)\n"
    "5. Key Numbers (mcap, liq, vol, age — pulled from on-chain data)\n"
    "6. Verdict (your honest call: watch / cautious / interesting / skip)\n\n"

    "RULES:\n"
    "- Be honest, not hypey. If it looks like a rug, say so.\n"
    "- Reference specific data points from the search results, X posts, and on-chain stats.\n"
    "- For X sentiment: note if there's hype, shilling, silence, or red flags.\n"
    "- Keep it under 300 words. Traders don't read essays.\n"
    "- End with ⚠️ DYOR. Not financial advice."
)


async def generate_thesis(
    symbol: str,
    name: str,
    ca: str,
    pair_data: dict,
    risk_report: dict | None = None,
    gem_result: dict | None = None,
    user_id: int | None = None,
) -> str:
    """
    Generate a full trading thesis for a token.
    Combines on-chain data + web search + AI analysis.
    """
    if AI_PROVIDER == "local":
        return "No AI provider configured."

    # ── Build on-chain context ────────────────────────────────────────────────
    from utils import safe_float
    mcap  = safe_float(pair_data.get("marketCap") or pair_data.get("fdv") or 0)
    liq   = safe_float((pair_data.get("liquidity") or {}).get("usd", 0))
    vol1h = safe_float((pair_data.get("volume") or {}).get("h1", 0))
    vol24h = safe_float((pair_data.get("volume") or {}).get("h24", 0))
    h1    = safe_float((pair_data.get("priceChange") or {}).get("h1", 0))
    h24   = safe_float((pair_data.get("priceChange") or {}).get("h24", 0))
    price = pair_data.get("priceUsd") or "?"
    age_h = 0.0
    created = pair_data.get("pairCreatedAt")
    if created:
        age_h = (time.time() * 1000 - created) / 3_600_000

    onchain_ctx = (
        f"ON-CHAIN DATA for {symbol} ({ca[:12]}...):\n"
        f"- Price: ${price}\n"
        f"- MCap: ${mcap:,.0f}\n"
        f"- Liquidity: ${liq:,.0f}\n"
        f"- Vol 1h: ${vol1h:,.0f} | Vol 24h: ${vol24h:,.0f}\n"
        f"- 1h change: {h1:+.1f}% | 24h change: {h24:+.1f}%\n"
        f"- Age: {age_h:.1f}h\n"
    )

    if risk_report:
        score = safe_float(risk_report.get("score", 0))
        risks = risk_report.get("risks", []) or []
        risk_names = [r.get("name", "") for r in risks[:5]]
        onchain_ctx += f"- Rugcheck score: {score:.0f} | Risks: {', '.join(risk_names) or 'none'}\n"

    if gem_result:
        onchain_ctx += f"- GemScore: {gem_result.get('score', 0)}/100 ({gem_result.get('grade', '?')})\n"

    # ── Web search + X/Twitter search ────────────────────────────────────────
    search_queries = [
        f"{symbol} solana crypto token",
        f"${symbol} crypto news",
    ]
    all_results = []
    for q in search_queries:
        results = await web_search(q, max_results=3)
        all_results.extend(results)

    web_ctx = "WEB SEARCH RESULTS:\n"
    if all_results:
        for r in all_results[:5]:
            title = r.get("title", "")
            body  = r.get("body", "")[:200]
            web_ctx += f"- {title}: {body}\n"
    else:
        web_ctx += "- No recent news found.\n"

    # X/Twitter sentiment search
    x_results = await x_search(symbol, max_results=5)
    x_ctx = "\nX/TWITTER POSTS:\n"
    if x_results:
        for r in x_results[:5]:
            title = r.get("title", "")
            body  = r.get("body", "")[:200]
            x_ctx += f"- {title}: {body}\n"
    else:
        x_ctx += "- No recent X posts found.\n"

    # ── Generate thesis with AI ───────────────────────────────────────────────
    user_msg = f"{onchain_ctx}\n\n{web_ctx}\n\n{x_ctx}\n\nWrite a trading thesis for {symbol}."

    messages = [{"role": "user", "content": user_msg}]

    # Try OpenRouter first, fallback to chain
    try:
        reply = await _openrouter_call(
            _THESIS_SYSTEM, messages,
            max_tokens=800, json_mode=False, fast=False,
        )
    except Exception:
        try:
            reply = await _call_ai(_THESIS_SYSTEM, messages, max_tokens=800)
        except Exception as e:
            logger.error(f"generate_thesis failed: {e}")
            return "Thesis generation failed. Try again."

    # Store in conversation history
    if user_id is not None:
        _push_history(user_id, "user", f"[thesis] {symbol} ({ca[:12]}...)")
        _push_history(user_id, "assistant", reply)

    return reply

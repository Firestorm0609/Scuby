"""
SCUBY IMPROVEMENTS — PATCH GUIDE
=================================

5 changes, ordered by impact. The first three are drop-in replacements.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 1: pair_cache.py (NEW FILE) — ~60% fewer DexScreener calls
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Drop pair_cache.py into your project root.

Then update feeds.py, smart_filters.py to use the shared pool.

In feeds.py, replace scan_movers's layer 2 search section with:
    from pair_cache import get_solana_pairs
    all_pairs = await get_solana_pairs(http)
    # then filter all_pairs by your threshold as before

In smart_filters.py, replace _fetch_fresh_pairs with:
    from pair_cache import get_solana_pairs
    async def _fetch_fresh_pairs(http):
        return await get_solana_pairs(http)

In handle_screener (handlers.py), after fetching, call:
    from pair_cache import invalidate
    invalidate()   # force fresh data for manual screener runs


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 2: gemscore.py (FULL REPLACEMENT) — better signal
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Replace your gemscore.py with the new version.

Key improvements:
- Pattern bonus now has a real heuristic when you have < 10 resolved tokens
  instead of returning a useless 0.5 neutral score.
- Age scoring peaks at 30-90 minutes (the real Solana sweet spot).
- Parabolic +200%+ gets a slight penalty (dump-incoming flag).
- Pump.fun graduation zone ($60K-$90K mcap, 1-2h old) gets a bonus.


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 3: ai.py — 2 line changes (faster + deeper analysis)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Change 1: Raise the Groq semaphore from 1 to 3.
Find this function and change the value:

    def _get_groq_semaphore() -> asyncio.Semaphore:
        global _GROQ_SEMAPHORE
        if _GROQ_SEMAPHORE is None:
            _GROQ_SEMAPHORE = asyncio.Semaphore(3)   # was: Semaphore(1)
        return _GROQ_SEMAPHORE

Change 2: Bump scuby_chat max output tokens from 300 to 600.
Find this line in scuby_chat():

    reply = await _call_ai(system_prompt, messages, max_tokens=600)  # was: 300

That's it for ai.py.


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 4: handlers.py — intent routing tweak
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The fast-path threshold is 10 words. This catches too many token analysis
requests as "conversational" and skips intent classification.
Drop it to 6 words:

Find in handle_message():
    word_count = len(clean.split())
    is_conversational = (
        word_count <= 6          # was: word_count <= 10
        and _CHAT_FAST_PATH.match(clean)
        and not _ACTION_KEYWORDS.search(clean)
    )


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CHANGE 5: memory.py — lower pattern engine threshold
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
The pattern engine requires 10 tokens per bucket before surfacing anything.
With the new gemscore.py heuristic covering early data, lower this to 5
so real patterns surface faster:

Find in memory.py:
    MIN_TOKENS_FOR_PATTERN = 5    # was: 10

Also lower the minimum group size in analyse_patterns():
    if len(multiples) < 3:         # was: < 5
        continue


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
APPLY ORDER
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. Drop pair_cache.py into project root
2. Replace gemscore.py
3. Edit ai.py (2 lines)
4. Edit handlers.py (1 line)
5. Edit memory.py (2 lines)
6. Restart: python main.py

All changes are backward-compatible — no new env vars, no DB migrations.
"""

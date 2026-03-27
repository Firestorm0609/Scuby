"""
patch_rate_limits.py
Run this from ~/Scuby to apply all rate-limit fixes.
"""
import re, shutil, os

def patch(filepath, description, old, new):
    src = open(filepath).read()
    if old not in src:
        print(f"  ⚠️  {filepath}: pattern not found for '{description}' — may already be patched")
        return
    open(filepath, 'w').write(src.replace(old, new, 1))
    print(f"  ✅ {filepath}: {description}")

# ── 1. feeds.py ───────────────────────────────────────────────────────────────
# Add rate_limiter import
patch("feeds.py", "add rate_limiter import",
    "from utils import (\n    escape_md,\n    safe_float,\n    dex_get,",
    "from rate_limiter import dex_get_safe as dex_get, dex_search_many\nfrom utils import (\n    escape_md,\n    safe_float,\n    dex_get,"
)

# Replace the 40-query asyncio.gather in scan_movers with chunked version
patch("feeds.py", "replace 40-query gather with chunked search",
    '''    search_results = await asyncio.gather(*[_safe_search(q) for q in SEARCH_TERMS])''',
    '''    # Chunked to avoid rate limiting — fires 6 queries at a time with pauses
    search_results_flat = await dex_search_many(http, SEARCH_TERMS, chunk_size=6)
    search_results = [search_results_flat]  # wrap for compatibility with merge loop below'''
)

# Fix the merge loop since search_results is now a flat list wrapped in one list
patch("feeds.py", "fix merge loop for chunked results",
    '''    for pairs in search_results:
        for pair in pairs:
            ca = pair.get("baseToken", {}).get("address", "")
            if ca and ca not in seen_ca:
                seen_ca.add(ca)
                all_pairs.append(pair)''',
    '''    for pairs in search_results:
        if isinstance(pairs, list) and pairs and isinstance(pairs[0], dict):
            # Flat list (from dex_search_many) or nested list
            items = pairs
        else:
            items = pairs if isinstance(pairs, list) else []
        for pair in items:
            if not isinstance(pair, dict):
                continue
            ca = pair.get("baseToken", {}).get("address", "")
            if ca and ca not in seen_ca:
                seen_ca.add(ca)
                all_pairs.append(pair)'''
)

# ── 2. smart_filters.py ───────────────────────────────────────────────────────
patch("smart_filters.py", "add rate_limiter import",
    "from utils import (\n    escape_md,\n    safe_float,\n    dex_get,",
    "from rate_limiter import dex_get_safe as dex_get, dex_search_many\nfrom utils import (\n    escape_md,\n    safe_float,\n    dex_get,"
)

patch("smart_filters.py", "replace 13-query gather with chunked search",
    '''    results = await asyncio.gather(
        _profiles(),
        *[_safe(q) for q in SEARCH_TERMS],
    )''',
    '''    profile_pairs = await _profiles()
    search_pairs  = await dex_search_many(http, SEARCH_TERMS, chunk_size=5)
    results = [profile_pairs, search_pairs]'''
)

# ── 3. proactive.py ───────────────────────────────────────────────────────────
patch("proactive.py", "add rate_limiter import",
    "from utils import escape_md, safe_float, dex_get",
    "from rate_limiter import dex_get_safe as dex_get, dex_search_many\nfrom utils import escape_md, safe_float, dex_get"
)

patch("proactive.py", "replace 52-query gather with chunked search",
    '''    results    = await asyncio.gather(*[_safe_search(q) for q in TRENDING_QUERIES])
    all_pairs: list[dict] = []
    seen_cas:  set[str]   = set()
    now_ms = time.time() * 1000

    for chunk in results:
        for p in chunk:''',
    '''    all_pairs: list[dict] = await dex_search_many(http, TRENDING_QUERIES, chunk_size=6)
    seen_cas:  set[str]   = set()
    now_ms = time.time() * 1000
    # re-filter after dedup
    filtered = []
    for p in all_pairs:'''
)

# Fix the loop body reference (chunk → p, and add results reference)
patch("proactive.py", "fix trending loop variable after refactor",
    '''    # re-filter after dedup
    filtered = []
    for p in all_pairs:
            ca  = p.get("baseToken", {}).get("address", "")
            sym = p.get("baseToken", {}).get("symbol", "").lower()
            nm  = p.get("baseToken", {}).get("name", "").lower()
            if not ca or ca in seen_cas:
                continue
            if sym in EXCLUDE_SYMBOLS or "solana" in nm:
                continue
            seen_cas.add(ca)
            all_pairs.append(p)

    if not all_pairs:
        return''',
    '''    for p in list(all_pairs):
        ca  = p.get("baseToken", {}).get("address", "")
        sym = p.get("baseToken", {}).get("symbol", "").lower()
        nm  = p.get("baseToken", {}).get("name", "").lower()
        if sym in EXCLUDE_SYMBOLS or "solana" in nm or not ca:
            all_pairs.remove(p)

    if not all_pairs:
        return'''
)

# ── 4. utils.py — keep dex_get but log 429s ───────────────────────────────────
patch("utils.py", "add 429 handling to dex_get",
    '''async def dex_get(http: httpx.AsyncClient, url: str, params: dict | None = None) -> dict:
    resp = await http.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, dict) else {}''',
    '''async def dex_get(http: httpx.AsyncClient, url: str, params: dict | None = None) -> dict:
    resp = await http.get(url, params=params)
    if resp.status_code == 429:
        logger.warning(f"DexScreener 429 rate limit hit: {url}")
        raise httpx.HTTPStatusError("429 Too Many Requests", request=resp.request, response=resp)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, dict) else {}'''
)

# ── 5. main.py — spread job start times to avoid boot-time pile-up ────────────
patch("main.py", "spread job start times to reduce boot-time collision",
    '''    jq.run_repeating(check_alerts_job,      interval=ALERT_POLL_INTERVAL,         first=ALERT_POLL_INTERVAL)
    jq.run_repeating(monitor_ping_job,       interval=MONITOR_POLL_INTERVAL,       first=10)
    jq.run_repeating(watch_scan_job,         interval=WATCH_POLL_INTERVAL,         first=30)
    jq.run_repeating(feed_scan_job,          interval=FEED_POLL_INTERVAL,          first=60)
    jq.run_repeating(smart_filter_scan_job,  interval=SMART_FILTER_POLL_INTERVAL,  first=45)
    jq.run_repeating(rug_watchdog_job,       interval=180,                         first=90)
    jq.run_repeating(whale_alert_job,        interval=300,                         first=120)
    jq.run_repeating(trending_job,           interval=14400,                       first=300)
    jq.run_repeating(wallet_scan_job,        interval=WALLET_POLL_INTERVAL,        first=90)
    jq.run_daily(daily_briefing_job,         time=__import__("datetime").time(9, 0, 0))
    jq.run_repeating(reminder_check_job,     interval=REMINDER_CHECK_SECS,             first=10)
    jq.run_repeating(learning_job,           interval=1800,                        first=600)''',
    '''    # Staggered start times — prevents all jobs hammering DexScreener simultaneously at boot
    jq.run_repeating(monitor_ping_job,       interval=MONITOR_POLL_INTERVAL,       first=15)
    jq.run_repeating(reminder_check_job,     interval=REMINDER_CHECK_SECS,         first=20)
    jq.run_repeating(watch_scan_job,         interval=WATCH_POLL_INTERVAL,         first=40)
    jq.run_repeating(smart_filter_scan_job,  interval=SMART_FILTER_POLL_INTERVAL,  first=60)
    jq.run_repeating(check_alerts_job,       interval=ALERT_POLL_INTERVAL,         first=80)
    jq.run_repeating(rug_watchdog_job,       interval=180,                         first=100)
    jq.run_repeating(feed_scan_job,          interval=FEED_POLL_INTERVAL,          first=130)
    jq.run_repeating(whale_alert_job,        interval=300,                         first=160)
    jq.run_repeating(wallet_scan_job,        interval=WALLET_POLL_INTERVAL,        first=200)
    jq.run_repeating(trending_job,           interval=14400,                       first=600)
    jq.run_daily(daily_briefing_job,         time=__import__("datetime").time(9, 0, 0))
    jq.run_repeating(learning_job,           interval=1800,                        first=900)'''
)

print("\nAll patches applied.")
print("Restart with: python main.py")

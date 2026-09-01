"""
apply_solscan_patch.py — run this from your Scuby project folder.

    python apply_solscan_patch.py

Makes a backup of utils.py and main.py before touching them.
"""

import shutil, sys

def patch(filepath, description, old, new):
    src = open(filepath).read()
    if old not in src:
        print(f"  ⚠️  {filepath}: pattern not found for '{description}' — may already be patched or file changed")
        return False
    open(filepath, "w").write(src.replace(old, new, 1))
    print(f"  ✅ {filepath}: {description}")
    return True

# ── Backup ────────────────────────────────────────────────────────────────────
for f in ("utils.py", "main.py"):
    shutil.copy(f, f + ".bak")
    print(f"  💾 Backed up {f} → {f}.bak")

# ══════════════════════════════════════════════════════════════════════════════
# PATCH 1 — replace _dated_pairs + find_og in utils.py
# ══════════════════════════════════════════════════════════════════════════════
patch("utils.py", "replace _dated_pairs + find_og with Solscan-aware versions",
    # ── FIND ──
    '''def _dated_pairs(pairs: list[dict]) -> list[dict]:
    return sorted([p for p in pairs if p.get("pairCreatedAt")], key=lambda p: p["pairCreatedAt"])


def find_og(pairs: list[dict]) -> dict | None:
    dated = _dated_pairs(pairs)
    return dated[0] if dated else None''',
    # ── REPLACE ──
    '''def _creation_ts(pair: dict) -> float:
    """
    Best available creation timestamp (Unix seconds).
    Priority: Solscan on-chain mint time > DexScreener pair listing > inf.
    """
    solscan = pair.get("_solscan_created_at")
    if solscan:
        return float(solscan)
    dex = pair.get("pairCreatedAt")
    if dex:
        return float(dex) / 1000
    return float("inf")


def _dated_pairs(pairs: list[dict]) -> list[dict]:
    dated = [p for p in pairs if _creation_ts(p) < float("inf")]
    return sorted(dated, key=_creation_ts)


def find_og(pairs: list[dict]) -> dict | None:
    """
    Return the original (oldest) token pair.
    Uses Solscan on-chain mint time when available — this is the true
    creation date. Falls back to DexScreener pairCreatedAt otherwise.
    """
    dated = _dated_pairs(pairs)
    return dated[0] if dated else None'''
)

# ══════════════════════════════════════════════════════════════════════════════
# PATCH 2 — update _pair_stats_block to show mint vs DEX listing date
# ══════════════════════════════════════════════════════════════════════════════
patch("utils.py", "update launch date display to show mint date vs DEX listing",
    # ── FIND ──
    '''    created_ts = pair.get("pairCreatedAt")
    if created_ts:
        created_dt  = datetime.fromtimestamp(created_ts / 1000, tz=timezone.utc)
        created_str = created_dt.strftime("%b %d, %Y %H:%M UTC")
        age_days    = (datetime.now(timezone.utc) - created_dt).days
        launch_line = f"\\U0001f4c5 Launched: {e(created_str)} \\\\({e(age_days)}d ago\\\\)\\n"
    else:
        launch_line = "\\U0001f4c5 Launched: Unknown\\n"''',
    # ── REPLACE ──
    '''    solscan_ts = pair.get("_solscan_created_at")
    dex_ts     = pair.get("pairCreatedAt")
    if solscan_ts:
        mint_dt     = datetime.fromtimestamp(float(solscan_ts), tz=timezone.utc)
        mint_str    = mint_dt.strftime("%b %d, %Y %H:%M UTC")
        mint_days   = (datetime.now(timezone.utc) - mint_dt).days
        launch_line = f"\\U0001f4aa Token minted: {e(mint_str)} \\\\({e(mint_days)}d ago\\\\)\\n"
        if dex_ts:
            pair_dt = datetime.fromtimestamp(dex_ts / 1000, tz=timezone.utc)
            if abs((pair_dt - mint_dt).total_seconds()) > 3600:
                launch_line += f"\\U0001f4c5 DEX listed: {e(pair_dt.strftime(\'%b %d, %Y %H:%M UTC\'))}\\n"
    elif dex_ts:
        created_dt  = datetime.fromtimestamp(dex_ts / 1000, tz=timezone.utc)
        created_str = created_dt.strftime("%b %d, %Y %H:%M UTC")
        age_days    = (datetime.now(timezone.utc) - created_dt).days
        launch_line = f"\\U0001f4c5 Pair listed: {e(created_str)} \\\\({e(age_days)}d ago\\\\)\\n"
    else:
        launch_line = "\\U0001f4c5 Launch date: Unknown\\n"'''
)

# ══════════════════════════════════════════════════════════════════════════════
# PATCH 3 — enrich pairs with Solscan data inside fetch_pairs_and_cache
# ══════════════════════════════════════════════════════════════════════════════
patch("utils.py", "enrich pairs with Solscan creation times in fetch_pairs_and_cache",
    # ── FIND ──
    '''    if pairs:
        _store_versions(bot_data, query_type, query_value, pairs)
        if not force:
            scan_prices = bot_data.setdefault("scan_prices", {})
            if record_scan_prices(pairs, scan_prices, scanned_by=scanned_by, chat_id=chat_id):
                await save_scan_prices_async(scan_prices)
                logger.info(f"Recorded scan prices for {query_value!r} (by {scanned_by!r} in chat {chat_id!r})")
        return _load_versions(bot_data, query_type, query_value) or pairs''',
    # ── REPLACE ──
    '''    if pairs:
        # Enrich with Solscan on-chain mint creation times for accurate OG detection
        try:
            from solscan import enrich_pairs, is_configured
            if is_configured():
                pairs = await enrich_pairs(pairs, http)
                hits = sum(1 for p in pairs if p.get("_solscan_created_at"))
                logger.info(f"Solscan: {hits}/{len(pairs)} pairs enriched for {query_value!r}")
        except Exception as _sc_err:
            logger.debug(f"Solscan enrichment skipped: {_sc_err}")

        _store_versions(bot_data, query_type, query_value, pairs)
        if not force:
            scan_prices = bot_data.setdefault("scan_prices", {})
            if record_scan_prices(pairs, scan_prices, scanned_by=scanned_by, chat_id=chat_id):
                await save_scan_prices_async(scan_prices)
                logger.info(f"Recorded scan prices for {query_value!r} (by {scanned_by!r} in chat {chat_id!r})")
        return _load_versions(bot_data, query_type, query_value) or pairs'''
)

# ══════════════════════════════════════════════════════════════════════════════
# PATCH 4 — flush Solscan cache on clean shutdown in main.py
# ══════════════════════════════════════════════════════════════════════════════
patch("main.py", "flush Solscan cache on shutdown",
    # ── FIND ──
    '''    logger.info("All data saved on shutdown.")''',
    # ── REPLACE ──
    '''    try:
        from solscan import flush_cache as _flush_solscan
        import asyncio as _asyncio
        _asyncio.get_event_loop().run_until_complete(_flush_solscan())
    except Exception:
        pass
    logger.info("All data saved on shutdown.")'''
)

# ── .gitignore ────────────────────────────────────────────────────────────────
try:
    gi = open(".gitignore").read()
    if "solscan_cache.json" not in gi:
        open(".gitignore", "a").write("\nsolscan_cache.json\n")
        print("  ✅ .gitignore: added solscan_cache.json")
except FileNotFoundError:
    pass

print("\nDone! Now:")
print("  1. Make sure solscan.py is in your project folder")
print("  2. Add SOLSCAN_API_KEY=your_key to .env  (free at pro.solscan.io)")
print("  3. Restart: python main.py")
print("\nNo key set? Bot works exactly as before — Solscan is optional.")

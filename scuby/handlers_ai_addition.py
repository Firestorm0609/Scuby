"""
handlers_ai_addition.py — AI chat handlers and smart filter commands.

Handles:
  /ask          — direct AI question
  /clearchat    — reset conversation
  /smartwatch   — set a natural-language smart filter
  /smartwatches — list active smart filters
  /unsmartwatch — remove a filter by label
  Callbacks: clearchat|, delsmartwatch|, autofilter|
"""

import logging
import os
import time
from datetime import datetime, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from utils import escape_md, safe_float
from smart_filters import save_smart_filters_async, MAX_SMART_FILTERS_PER_CHAT

logger = logging.getLogger(__name__)


# ─── Keyboard helpers ─────────────────────────────────────────────────────────

def _smartwatches_keyboard(chat_id: int, filters: list[dict]) -> InlineKeyboardMarkup:
    rows = []
    for flt in filters:
        label = flt.get("label", "?")[:40]
        rows.append([InlineKeyboardButton(
            f"❌ Remove: {label}",
            callback_data=f"delsmartwatch|{flt['id']}",
        )])
    rows.append([
        InlineKeyboardButton("📊 Filter stats",  callback_data="menu|filterstats"),
        InlineKeyboardButton("🏠 Main Menu",      callback_data="menu|home"),
    ])
    return InlineKeyboardMarkup(rows)


# ─── /ask ─────────────────────────────────────────────────────────────────────

async def handle_ask(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Direct AI chat: /ask <anything>"""
    if not update.message or not update.effective_user:
        return

    text = " ".join(context.args or []).strip()
    if not text:
        await update.message.reply_text(
            "*Ask Scuby anything\\!*\n\n"
            "Examples:\n"
            "  `/ask what is a rug pull?`\n"
            "  `/ask how do I spot an OG token?`\n"
            "  `/ask explain pump\\.fun bonding curve`\n\n"
            "_Or just talk to me directly in chat — just say 'scuby' first\\!_",
            parse_mode="MarkdownV2",
        )
        return

    user_id = update.effective_user.id

    try:
        await context.bot.send_chat_action(
            chat_id=update.effective_chat.id, action="typing"
        )
    except Exception:
        pass

    try:
        from ai import scuby_chat
        user_memory = context.bot_data.get("user_memory", {})
        reply = await scuby_chat(user_id, text, user_memory=user_memory)

        try:
            from memory import save_user_memory_async
            await save_user_memory_async(user_memory)
        except Exception:
            pass

        # Send reply directly — AI already formats its own markdown.
        # Fallback to plain text if Telegram rejects the parse.
        try:
            await update.message.reply_text(reply, parse_mode="MarkdownV2")
        except Exception:
            await update.message.reply_text(reply)

    except Exception as ex:
        logger.error(f"handle_ask: {ex}", exc_info=True)
        await update.message.reply_text(
            "Something went wrong. Try again.",
            parse_mode="MarkdownV2",
        )


# ─── /clearchat ───────────────────────────────────────────────────────────────

async def handle_clearchat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Reset conversation history for this user."""
    if not update.message or not update.effective_user:
        return

    user_id = update.effective_user.id

    try:
        from ai import clear_history
        clear_history(user_id)
    except Exception as ex:
        logger.warning(f"handle_clearchat clear_history: {ex}")

    await update.message.reply_text(
        "🧹 *Done\\!* Scuby wiped the conversation slate clean\\.\n\n"
        "_Fresh start — I don't remember our recent chat, but I still know your memory profile\\. "
        "Tap below to clear that too if you want a full reset\\._",
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🧹 Clear full memory too", callback_data=f"clearmemory|{user_id}"),
            InlineKeyboardButton("🏠 Menu",                  callback_data="menu|home"),
        ]]),
    )


async def handle_clearchat_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback: clearchat|<user_id>"""
    query = update.callback_query
    await query.answer()

    parts = query.data.split("|", 1)
    if len(parts) != 2:
        return
    try:
        user_id = int(parts[1])
    except ValueError:
        return

    try:
        from ai import clear_history
        clear_history(user_id)
    except Exception as ex:
        logger.warning(f"handle_clearchat_button: {ex}")

    try:
        await query.edit_message_text(
            "🧹 Conversation cleared\\! Scuby starts fresh\\.",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Menu", callback_data="menu|home")
            ]]),
        )
    except Exception:
        pass


# ─── /smartwatch ──────────────────────────────────────────────────────────────

async def handle_smartwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Set a natural-language smart filter.
    e.g. /smartwatch mcap between 10k and 20k liq over 5k
    """
    if not update.message or not update.effective_chat:
        return

    raw = " ".join(context.args or []).strip()
    if not raw:
        await update.message.reply_text(
            "🎯 *Smart Watch — Usage*\n\n"
            "Describe your filter in plain English:\n\n"
            "  `/smartwatch mcap between 10k and 20k`\n"
            "  `/smartwatch new tokens up 50% in 1h`\n"
            "  `/smartwatch gems under 50k liq over 5k`\n"
            "  `/smartwatch very new pairs vol over 10k`\n"
            "  `/smartwatch mcap 10k to 100k age under 30 min`\n\n"
            f"_Scans every 2 minutes\\. Max {MAX_SMART_FILTERS_PER_CHAT} filters per chat\\._",
            parse_mode="MarkdownV2",
        )
        return

    chat_id       = str(update.effective_chat.id)
    smart_filters = context.bot_data.setdefault("smart_filters", {})
    chat_sf       = smart_filters.setdefault(chat_id, [])

    if len(chat_sf) >= MAX_SMART_FILTERS_PER_CHAT:
        await update.message.reply_text(
            f"Already have *{MAX_SMART_FILTERS_PER_CHAT}* smart filters\\! "
            "Use /smartwatches to remove some first\\.",
            parse_mode="MarkdownV2",
        )
        return

    msg = await update.message.reply_text(
        "🎯 Scuby's parsing your filter\\.\\.\\.",
        parse_mode="MarkdownV2",
    )

    try:
        from ai import parse_smart_filter
        flt = await parse_smart_filter(raw)
    except Exception as ex:
        logger.error(f"handle_smartwatch parse: {ex}", exc_info=True)
        flt = None

    if not flt:
        await msg.edit_text(
            "Ruh\\-roh\\! Scuby couldn't parse that\\. Try:\n"
            "`/smartwatch mcap between 10k and 50k liq over 5k`",
            parse_mode="MarkdownV2",
        )
        return

    flt["id"]       = f"{int(time.time() * 1000)}_{os.urandom(3).hex()}"
    flt["raw"]      = raw
    flt["added_ts"] = time.time()
    flt["seen_cas"] = []
    chat_sf.append(flt)
    await save_smart_filters_async(smart_filters)

    # Build human-readable summary
    e     = escape_md
    label = e(flt.get("label", raw[:50]))
    lines = []
    if flt.get("mcap_min") is not None and flt.get("mcap_max") is not None:
        lines.append(f"  • MCap *{e(_fmt_k(flt['mcap_min']))}* – *{e(_fmt_k(flt['mcap_max']))}*")
    elif flt.get("mcap_max") is not None:
        lines.append(f"  • MCap under *{e(_fmt_k(flt['mcap_max']))}*")
    elif flt.get("mcap_min") is not None:
        lines.append(f"  • MCap over *{e(_fmt_k(flt['mcap_min']))}*")
    if flt.get("liq_min") is not None:
        lines.append(f"  • Liquidity ≥ *{e(_fmt_k(flt['liq_min']))}*")
    if flt.get("age_max_minutes") is not None:
        lines.append(f"  • Pair age ≤ *{e(str(flt['age_max_minutes']))} minutes*")
    if flt.get("pct_change_min") is not None:
        lines.append(f"  • Price change ≥ *{e(str(flt['pct_change_min']))}%* in 1h")
    if flt.get("vol_min_1h") is not None:
        lines.append(f"  • 1h volume ≥ *{e(_fmt_k(flt['vol_min_1h']))}*")

    criteria = "\n".join(lines) if lines else "  _custom filter_"

    await msg.edit_text(
        f"🎯 *Smart filter set\\!*\n\n"
        f"*{label}*\n\n"
        f"{criteria}\n\n"
        f"_Scuby scans every 2 minutes and alerts you the moment a new pair matches\\. "
        f"Use /smartwatches to manage\\._",
        parse_mode="MarkdownV2",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📋 My smart filters", callback_data="menu|home"),
            InlineKeyboardButton("🏠 Menu",             callback_data="menu|home"),
        ]]),
    )


def _fmt_k(v: float) -> str:
    if v >= 1_000_000: return f"${v/1_000_000:.1f}M"
    if v >= 1_000:     return f"${v/1_000:.0f}K"
    return f"${v:.0f}"


# ─── /smartwatches ────────────────────────────────────────────────────────────

async def handle_smartwatches(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all active smart filters for this chat."""
    if not update.message or not update.effective_chat:
        return

    chat_id       = str(update.effective_chat.id)
    smart_filters = context.bot_data.get("smart_filters", {})
    chat_sf       = smart_filters.get(chat_id, [])

    if not chat_sf:
        await update.message.reply_text(
            "🎯 No smart filters active here yet\\!\n\n"
            "Set one with:\n`/smartwatch mcap between 10k and 20k`\n\n"
            "_Scuby will alert you the moment a new pair matches\\._",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")
            ]]),
        )
        return

    e     = escape_md
    lines = [f"🎯 *Active smart filters \\({len(chat_sf)}\\):*\n"]

    filter_scores = context.bot_data.get("filter_scores", {})

    for i, flt in enumerate(chat_sf, 1):
        label      = e(flt.get("label", "?"))
        added_str  = ""
        if flt.get("added_ts"):
            dt        = datetime.fromtimestamp(flt["added_ts"], tz=timezone.utc)
            added_str = e(dt.strftime("%b %d, %H:%M UTC"))
        seen_count = len(flt.get("seen_cas", []))

        # Show win rate if available
        fid   = flt.get("id", "")
        stats = filter_scores.get(fid, {}).get("stats", {})
        wr    = stats.get("win_rate")
        resolved = stats.get("resolved", 0)
        perf_str = ""
        if wr is not None and resolved >= 3:
            perf_emoji = "🔥" if wr >= 0.6 else ("✅" if wr >= 0.4 else "⚠️")
            perf_str   = f" \\| {perf_emoji} {e(f'{wr:.0%}')} win rate"

        lines.append(
            f"*{i}\\.* {label}\n"
            f"   _Added {added_str} — {seen_count} alert\\(s\\){perf_str}_"
        )

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="MarkdownV2",
        reply_markup=_smartwatches_keyboard(int(chat_id), chat_sf),
    )


# ─── /unsmartwatch ────────────────────────────────────────────────────────────

async def handle_unsmartwatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a smart filter by partial label match."""
    if not update.message or not update.effective_chat:
        return

    chat_id       = str(update.effective_chat.id)
    smart_filters = context.bot_data.get("smart_filters", {})
    chat_sf       = smart_filters.get(chat_id, [])

    if not chat_sf:
        await update.message.reply_text("No smart filters to remove\\!", parse_mode="MarkdownV2")
        return

    raw = " ".join(context.args or []).strip().lower()
    if not raw:
        await handle_smartwatches(update, context)
        return

    before = len(chat_sf)
    smart_filters[chat_id] = [
        f for f in chat_sf
        if raw not in f.get("label", "").lower()
        and raw not in f.get("raw", "").lower()
    ]
    removed = before - len(smart_filters[chat_id])
    await save_smart_filters_async(smart_filters)

    e = escape_md
    if removed:
        await update.message.reply_text(
            f"✅ Removed *{removed}* filter\\(s\\) matching *{e(raw)}*\n\n"
            "_Use /smartwatches to see what's still running\\._",
            parse_mode="MarkdownV2",
        )
    else:
        await update.message.reply_text(
            f"Hmm\\! No filter found matching *{e(raw)}*\\.\n"
            "Use /smartwatches to see what's active\\.",
            parse_mode="MarkdownV2",
        )


# ─── Callback: delsmartwatch ──────────────────────────────────────────────────

async def handle_delsmartwatch_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback: delsmartwatch|<filter_id>"""
    query = update.callback_query
    await query.answer()

    parts = query.data.split("|", 1)
    if len(parts) != 2 or parts[0] != "delsmartwatch":
        return

    filter_id     = parts[1]
    chat_id       = str(query.message.chat.id) if query.message else None
    if not chat_id:
        return

    smart_filters = context.bot_data.setdefault("smart_filters", {})
    before        = smart_filters.get(chat_id, [])
    removed       = [f for f in before if f["id"] == filter_id]
    smart_filters[chat_id] = [f for f in before if f["id"] != filter_id]
    await save_smart_filters_async(smart_filters)

    remaining = smart_filters.get(chat_id, [])
    label     = removed[0].get("label", "that filter") if removed else "that filter"
    e = escape_md

    try:
        if not remaining:
            await query.edit_message_text(
                f"✅ Removed filter *{e(label)}*\\.\n\n🎯 No active smart filters here now\\.",
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🏠 Main Menu", callback_data="menu|home")
                ]]),
            )
        else:
            lines = [f"✅ Removed *{e(label)}*\\.\n\n🎯 *Still active \\({len(remaining)}\\):*\n"]
            for f in remaining:
                lines.append(f"• {e(f.get('label', '?'))}")
            await query.edit_message_text(
                "\n".join(lines),
                parse_mode="MarkdownV2",
                reply_markup=_smartwatches_keyboard(int(chat_id), remaining),
            )
    except Exception as ex:
        logger.warning(f"handle_delsmartwatch_button edit failed: {ex}")


# ─── Callback: autofilter (learning job suggestion) ───────────────────────────

async def handle_autofilter_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Callback: autofilter|<criteria_string>
    Called when the user taps "Set this filter" on an AI-generated suggestion.
    """
    query = update.callback_query
    await query.answer("Setting up your filter!")

    parts = query.data.split("|", 1)
    if len(parts) != 2:
        return

    criteria = parts[1]
    chat_id  = str(query.message.chat.id) if query.message else None
    if not chat_id:
        return

    smart_filters = context.bot_data.setdefault("smart_filters", {})
    chat_sf       = smart_filters.setdefault(chat_id, [])

    if len(chat_sf) >= MAX_SMART_FILTERS_PER_CHAT:
        try:
            await query.edit_message_text(
                f"Already at the max of *{MAX_SMART_FILTERS_PER_CHAT}* smart filters\\! "
                "Use /smartwatches to clear some first\\.",
                parse_mode="MarkdownV2",
            )
        except Exception:
            pass
        return

    try:
        from ai import parse_smart_filter
        flt = await parse_smart_filter(criteria)
    except Exception:
        flt = None

    if not flt:
        try:
            await query.edit_message_text("Ruh\\-roh\\! Couldn't set that filter\\. Try /smartwatch manually\\.", parse_mode="MarkdownV2")
        except Exception:
            pass
        return

    flt["id"]       = f"{int(time.time() * 1000)}_{os.urandom(3).hex()}"
    flt["raw"]      = criteria
    flt["added_ts"] = time.time()
    flt["seen_cas"] = []
    chat_sf.append(flt)
    await save_smart_filters_async(smart_filters)

    e = escape_md
    try:
        await query.edit_message_text(
            f"✅ *Smart filter set\\!*\n\n"
            f"*{e(flt.get('label', criteria[:50]))}*\n\n"
            f"_Scuby will alert you when new pairs match\\._",
            parse_mode="MarkdownV2",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🏠 Menu", callback_data="menu|home"),
            ]]),
        )
    except Exception:
        pass


# ─── /code ───────────────────────────────────────────────────────────────────

async def handle_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Code generation — Scuby's coding arm.
    /code <request> — write, explain, debug, or review code.
    """
    if not update.message or not update.effective_user:
        return

    text = " ".join(context.args or []).strip()
    if not text:
        await update.message.reply_text(
            "💻 *Scuby's Code Lab*\n\n"
            "Write, explain, debug, or review code\\.\n\n"
            "Usage:\n"
            "  `/code write a Python script to check SOL balance`\n"
            "  `/code explain this Solidity function: ...`\n"
            "  `/code debug this error: ...`\n"
            "  `/code review my Anchor program`\n\n"
            "_Scuby's got the code \\!_",
            parse_mode="MarkdownV2",
        )
        return

    # Show typing indicator
    try:
        await context.bot.send_chat_action(
            chat_id=update.effective_chat.id, action="typing"
        )
    except Exception:
        pass

    msg = await update.message.reply_text(
        "💻 Scuby is coding\\.\\.\\.",
        parse_mode="MarkdownV2",
    )

    try:
        from ai import code_generate
        user_id = update.effective_user.id if update.effective_user else None
        reply = await code_generate(text, user_id=user_id)

        # Telegram has a 4096 char limit per message
        if len(reply) > 4000:
            # Split into chunks at code block boundaries
            chunks = _split_code_message(reply)
            for i, chunk in enumerate(chunks):
                try:
                    await msg.edit_text(
                        chunk,
                        parse_mode="Markdown",
                        disable_web_page_preview=True,
                    ) if i == 0 else await update.message.reply_text(
                        chunk,
                        parse_mode="Markdown",
                        disable_web_page_preview=True,
                    )
                except Exception:
                    # Fallback: send without parse_mode
                    await update.message.reply_text(chunk, disable_web_page_preview=True)
        else:
            try:
                await msg.edit_text(
                    reply,
                    parse_mode="Markdown",
                    disable_web_page_preview=True,
                )
            except Exception:
                # Markdown failed — try without formatting
                await msg.edit_text(reply, disable_web_page_preview=True)

    except Exception as ex:
        logger.error(f"handle_code: {ex}", exc_info=True)
        try:
            await msg.edit_text(
                "Something went wrong. Try again.",
                parse_mode="MarkdownV2",
            )
        except Exception:
            pass


def _split_code_message(text: str, max_len: int = 4000) -> list[str]:
    """Split a long message into chunks, preserving code blocks."""
    if len(text) <= max_len:
        return [text]

    chunks = []
    current = ""
    in_code = False

    for line in text.split("\n"):
        if line.strip().startswith("```"):
            in_code = not in_code

        if len(current) + len(line) + 1 > max_len and not in_code:
            chunks.append(current)
            current = line
        else:
            current += line + "\n"

    if current.strip():
        chunks.append(current)

    return chunks if chunks else [text[:max_len]]


# ─── /analyze ────────────────────────────────────────────────────────────────

async def handle_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Deep token analysis: on-chain data + web search + AI thesis.
    /analyze <ticker or CA> — full trading thesis.
    """
    if not update.message or not update.effective_user:
        return

    text = " ".join(context.args or []).strip()
    if not text:
        await update.message.reply_text(
            "🔍 *Scuby Deep Analysis*\n\n"
            "Get a full trading thesis for any token\\.\n\n"
            "Usage:\n"
            "  `/analyze BONK`\n"
            "  `/analyze <contract_address>`\n\n"
            "_Scuby checks on-chain data, searches the web, and gives you "
            "an honest thesis\\._",
            parse_mode="MarkdownV2",
        )
        return

    msg = await update.message.reply_text(
        "🔍 Scuby is digging deep\\.\\.\\.\n"
        "_Fetching on-chain data + web search + AI analysis_",
        parse_mode="MarkdownV2",
    )

    try:
        from utils import (
            fetch_pairs_and_cache, fetch_rugcheck,
            SOLANA_CA_PATTERN, VALID_EVM_CA, TICKER_PATTERN,
            escape_md,
        )
        from gemscore import calculate_gem_score
        from ai import generate_thesis

        http = context.bot_data["http"]
        scan_prices = context.bot_data.setdefault("scan_prices", {})
        token_perf  = context.bot_data.get("token_perf", {})
        chat_id     = str(update.effective_chat.id)
        user_id     = update.effective_user.id

        # Resolve input to pairs — detect Solana CA, EVM CA, or ticker
        ca_match = SOLANA_CA_PATTERN.search(text)
        evm_match = VALID_EVM_CA.search(text)
        tick_match = TICKER_PATTERN.search(text)

        # For CA lookups, use smart lookup (web + on-chain + multiple sources)
        if ca_match or evm_match:
            ca = ca_match.group(0) if ca_match else evm_match.group(0)
            from smart_lookup import smart_token_lookup, format_lookup_context
            lookup = await smart_token_lookup(ca, http=http)
            symbol = lookup.get("symbol") or ca[:8]
            context_text = format_lookup_context(lookup)
            # Pass to AI for synthesis
            from ai import scuby_chat
            prompt = (
                f"Analyze this token for the user. Here's everything I found:\n\n"
                f"{context_text}\n\n"
                f"Give a thorough trading thesis. Cover: what it is, key metrics, "
                f"bull/bear case, social sentiment, and your honest verdict."
            )
            thesis = await scuby_chat(user_id, prompt)
            await _send_reply(update, thesis)
            return

        if ca_match:
            pairs = await fetch_pairs_and_cache("ca", ca_match.group(0), http, context.bot_data, chat_id=chat_id)
            query_label = ca_match.group(0)[:12]
        elif evm_match:
            pairs = await fetch_pairs_and_cache("ca", evm_match.group(0), http, context.bot_data, chat_id=chat_id)
            query_label = evm_match.group(0)[:14]
        elif tick_match:
            ticker = tick_match.group(1).upper()
            pairs = await fetch_pairs_and_cache("ticker", ticker, http, context.bot_data, chat_id=chat_id)
            query_label = f"${ticker}"
        else:
            # Try as bare ticker
            bare = text.strip().upper()
            if 2 <= len(bare) <= 10 and bare.isalpha():
                pairs = await fetch_pairs_and_cache("ticker", bare, http, context.bot_data, chat_id=chat_id)
                query_label = f"${bare}"
            else:
                await msg.edit_text(
                    "Ruh\\-roh\\! Scuby couldn't understand that\\. "
                    "Try a ticker like `BONK` or a contract address\\.",
                    parse_mode="MarkdownV2",
                )
                return

        if not pairs:
            await msg.edit_text(
                f"Ruh\\-roh\\! Couldn't find *{escape_md(query_label)}* on Solana\\.",
                parse_mode="MarkdownV2",
            )
            return

        pair = pairs[0]
        ca = pair.get("baseToken", {}).get("address", "")
        symbol = pair.get("baseToken", {}).get("symbol", "?")
        name = pair.get("baseToken", {}).get("name", "?")

        # Fetch rugcheck + GemScore
        rug_report, gem_result = None, None
        try:
            rug_report = await fetch_rugcheck(ca, http)
        except Exception:
            pass
        try:
            gem_result = calculate_gem_score(pair, rug_report or {}, token_perf, chat_id)
        except Exception:
            pass

        # Generate thesis
        thesis = await generate_thesis(
            symbol, name, ca, pair,
            risk_report=rug_report,
            gem_result=gem_result,
            user_id=user_id,
        )

        # Format response
        e = escape_md
        header = (
            f"🔍 *Deep Analysis — {e(symbol)}*\n"
            f"_{e(name)}_\n"
            f"`{e(ca)}`\n\n"
        )

        gem_line = ""
        if gem_result:
            gem_line = f"{gem_result['gem_emoji']} *GemScore: {e(str(gem_result['score']))}/100 — {e(gem_result['grade'])}*\n\n"

        # Send thesis (might be long)
        full_text = header + gem_line + thesis

        if len(full_text) > 4000:
            # Send header + GemScore first, then thesis
            try:
                await msg.edit_text(
                    header + gem_line,
                    parse_mode="MarkdownV2",
                    disable_web_page_preview=True,
                )
            except Exception:
                await msg.edit_text(header + gem_line)
            await update.message.reply_text(
                thesis,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        else:
            try:
                await msg.edit_text(
                    full_text,
                    parse_mode="MarkdownV2",
                    disable_web_page_preview=True,
                )
            except Exception:
                await msg.edit_text(full_text, disable_web_page_preview=True)

    except Exception as ex:
        logger.error(f"handle_analyze: {ex}", exc_info=True)
        try:
            await msg.edit_text(
                "Something went wrong. Try again.",
                parse_mode="MarkdownV2",
            )
        except Exception:
            pass


# ─── Self-improvement commands ───────────────────────────────────────────────

async def handle_selftest(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run Scuby's test suite and report results.
    /selftest — run all tests, show pass/fail.
    """
    if not update.message:
        return

    msg = await update.message.reply_text(
        "🧪 Scuby is running self\\.\\.\\.\n"
        "_Executing test suite..._",
        parse_mode="MarkdownV2",
    )

    try:
        from self_improve import run_tests
        result = await run_tests()

        e = escape_md
        if result["success"]:
            status = f"✅ *All {result['passed']} tests passed!*"
        else:
            status = f"❌ *{result['failed']} test(s) failed*\n_Passed: {result['passed']}"

        output = result["output"][:1500]
        text = f"{status}\n\n`{e(output)}`"

        if result["errors"]:
            text += f"\n\nErrors:\n`{e(result['errors'][:500])}`"

        await msg.edit_text(text, parse_mode="MarkdownV2")
    except Exception as ex:
        logger.error(f"handle_selftest: {ex}", exc_info=True)
        await msg.edit_text(
            "Self-test failed. Try again.",
            parse_mode="MarkdownV2",
        )


async def handle_read(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Read Scuby's own source files.
    /read <file> [start_line] [end_line]
    """
    if not update.message:
        return

    args = context.args or []
    if not args:
        await update.message.reply_text(
            "📖 *Scuby's Source Files*\n\n"
            "Usage:\n"
            "  `/read ai.py`\n"
            "  `/read handlers.py 100 150`\n\n"
            "Core files: " + ", ".join(f"`{f}`" for f in [
                "ai.py", "handlers.py", "utils.py", "gemscore.py",
                "memory.py", "feeds.py", "smart_filters.py", "self_improve.py",
            ]) + "\n\n"
            "_Scuby can read its own brain !_",
            parse_mode="MarkdownV2",
        )
        return

    filepath = args[0]
    start = int(args[1]) if len(args) > 1 and args[1].isdigit() else 1
    end = int(args[2]) if len(args) > 2 and args[2].isdigit() else None

    try:
        from self_improve import read_file_lines
        content = read_file_lines(filepath, start, end)

        if content.startswith("Error"):
            await update.message.reply_text(
                f"Ruh\\-roh\\! {escape_md(content)}",
                parse_mode="MarkdownV2",
            )
            return

        # Telegram 4096 char limit
        if len(content) > 4000:
            content = content[:4000] + "\n... (truncated)"

        await update.message.reply_text(
            f"`{escape_md(content)}`",
            parse_mode="MarkdownV2",
            disable_web_page_preview=True,
        )
    except Exception as ex:
        logger.error(f"handle_read: {ex}", exc_info=True)
        await update.message.reply_text(
            "Could not read that file.",
            parse_mode="MarkdownV2",
        )


async def handle_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show Scuby's project status and recent changes.
    /status — file listing with sizes and last modified times.
    """
    if not update.message:
        return

    try:
        from self_improve import get_project_status, get_recent_changes
        status = get_project_status()
        recent = get_recent_changes(days=1)

        e = escape_md
        lines = ["*Scuby Project Status*\n"]

        if recent:
            lines.append("_Modified in last 24h:_")
            for r in recent[:8]:
                lines.append(f"  • `{e(r['file'])}` — {e(r['modified'])} ({r['size']} bytes)")
            lines.append("")

        lines.append("_All core files:_")
        total_size = 0
        for filename, info in sorted(status.items()):
            if info["exists"]:
                size_kb = info["size"] / 1024
                total_size += info["size"]
                lines.append(f"  ✅ `{e(filename)}` — {size_kb:.1f}KB")
            else:
                lines.append(f"  ❌ `{e(filename)}` — MISSING")

        lines.append(f"\n📊 Total: {len(status)} files, {total_size/1024:.1f}KB")
        lines.append("_Use /read <file> to view source code.")

        await update.message.reply_text(
            "\n".join(lines),
            parse_mode="MarkdownV2",
        )
    except Exception as ex:
        logger.error(f"handle_status: {ex}", exc_info=True)
        await update.message.reply_text(
            "Could not get status.",
            parse_mode="MarkdownV2",
        )


async def handle_fix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Scuby diagnoses and fixes bugs in its own code.
    /fix <description of the problem>
    """
    if not update.message or not update.effective_user:
        return

    description = " ".join(context.args or []).strip()
    if not description:
        await update.message.reply_text(
            "🔧 *Scuby Self-Fix*\n\n"
            "Describe the problem and Scuby will try to fix it\\!\n\n"
            "Usage:\n"
            "  `/fix the leaderboard is showing wrong scores`\n"
            "  `/fix GemScore returns 0 for tokens with no volume`\n"
            "  `/fix /sniff crashes when token has no name`\n\n"
            "_Scuby reads its own code, finds the bug, and fixes it_",
            parse_mode="MarkdownV2",
        )
        return

    msg = await update.message.reply_text(
        "🔧 Scuby is reading its own code\\.\\.\\.\n"
        "_Diagnosing the issue..._",
        parse_mode="MarkdownV2",
    )

    try:
        from self_improve import read_file, CORE_FILES, run_tests
        from ai import _call_ai

        # Step 1: Read all core files to understand the codebase
        codebase = ""
        for filename in CORE_FILES:
            content = read_file(filename)
            if not content.startswith("Error"):
                codebase += f"\n\n=== {filename} ===\n{content[:3000]}"

        # Step 2: Ask AI to diagnose
        json_example = '{"file": "filename.py", "line_start": N, "line_end": N, "diagnosis": "what is wrong", "fix": "exact old code to replace", "replacement": "exact new code"}'
        diag_prompt = (
            f"You are Scuby, a Python Telegram bot. Here is your codebase:\n\n"
            f"{codebase[:8000]}\n\n"
            f"PROBLEM: {description}\n\n"
            f"Diagnose the issue. Reply with ONLY a JSON object like this:\n"
            f"{json_example}\n\n"
            f"If you need to see more of a file, say which file and line range."
        )

        raw = await _call_ai(diag_prompt, [{"role": "user", "content": description}], max_tokens=800, json_mode=True)
        import re, json as _json
        raw = re.sub(r"```(?:json)?", "", raw).strip().rstrip("`").strip()
        diagnosis = _json.loads(raw)

        target_file = diagnosis.get("file", "")
        old_code = diagnosis.get("fix", "")
        new_code = diagnosis.get("replacement", "")
        explanation = diagnosis.get("diagnosis", "Unknown issue")

        if not target_file or not old_code or not new_code:
            await msg.edit_text(
                f"🔍 *Diagnosis:*\n{escape_md(explanation)}\n\n"
                f"_Scuby needs more context. Try /read <file> to help me find the exact lines._",
                parse_mode="MarkdownV2",
            )
            return

        # Step 3: Preview the diff
        from self_improve import replace_in_file
        preview = replace_in_file(target_file, old_code, new_code, backup=False)  # dry run
        if preview.startswith("Error"):
            # Try with the actual file content
            content = read_file(target_file)
            if old_code in content:
                import difflib
                diff = list(difflib.unified_diff(
                    content.splitlines(keepends=True),
                    content.replace(old_code, new_code, 1).splitlines(keepends=True),
                    fromfile=f"a/{target_file}",
                    tofile=f"b/{target_file}",
                ))
                preview = "".join(diff) if diff else "No diff generated"
            else:
                await msg.edit_text(
                    f"🔍 *Diagnosis:*\n{escape_md(explanation)}\n\n"
                    f"Ruh\\-roh\\! The old code doesn't match exactly. Let me try reading more of the file.",
                    parse_mode="MarkdownV2",
                )
                return

        # Step 4: Show the fix and ask for confirmation
        e = escape_md
        confirm_text = (
            f"🔍 *Diagnosis:*\n{e(explanation)}\n\n"
            f"📋 *Proposed Fix ({e(target_file)}):*\n"
            f"`{e(preview[:1500])}`\n\n"
            f"Reply with *yes* to apply, or *no* to cancel."
        )

        # Store the pending fix in bot_data for confirmation
        context.bot_data.setdefault("pending_fixes", {})[update.effective_user.id] = {
            "file": target_file,
            "old": old_code,
            "new": new_code,
            "explanation": explanation,
            "msg_id": msg.message_id,
            "chat_id": update.effective_chat.id,
            "created": time.time(),
        }

        await msg.edit_text(confirm_text, parse_mode="MarkdownV2")

    except json.JSONDecodeError:
        await msg.edit_text(
            "Ruh\\-roh\\! Scuby couldn't parse the diagnosis.\n"
            "_Try being more specific about the error._",
            parse_mode="MarkdownV2",
        )
    except Exception as ex:
        logger.error(f"handle_fix: {ex}", exc_info=True)
        await msg.edit_text(
            "Self-fix failed. Try again.",
            parse_mode="MarkdownV2",
        )


async def handle_fix_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Confirm or cancel a pending fix.
    /fixyes — apply the pending fix
    /fixno — cancel
    """
    if not update.message or not update.effective_user:
        return

    user_id = update.effective_user.id
    pending = context.bot_data.get("pending_fixes", {}).get(user_id)

    if not pending:
        await update.message.reply_text(
            "No pending fix to confirm. Use /fix <problem> first.",
        )
        return

    # Check expiry (5 min timeout)
    if time.time() - pending["created"] > 300:
        context.bot_data["pending_fixes"].pop(user_id, None)
        await update.message.reply_text("⏰ Fix expired. Run /fix again.")
        return

    # Apply the fix
    try:
        from self_improve import replace_in_file, run_tests

        diff = replace_in_file(pending["file"], pending["old"], pending["new"])
        context.bot_data["pending_fixes"].pop(user_id, None)

        # Run tests to verify
        test_result = await run_tests()

        e = escape_md
        if test_result["success"]:
            text = (
                f"✅ *Fix applied and verified!*\n\n"
                f"📝 {e(pending['explanation'])}\n\n"
                f"🧪 Tests: *{test_result['passed']} passed*\n\n"
                f"`{e(diff[:1000])}`"
            )
        else:
            text = (
                f"⚠️ *Fix applied but tests failed!*\n\n"
                f"📝 {e(pending['explanation'])}\n\n"
                f"🧪 Tests: *{test_result['failed']} failed*\n\n"
                f"You may need to revert: /fixrevert"
            )

        await update.message.reply_text(text, parse_mode="MarkdownV2")
    except Exception as ex:
        logger.error(f"handle_fix_confirm: {ex}", exc_info=True)
        await update.message.reply_text(
            f"Fix failed: {escape_md(str(ex))}",
            parse_mode="MarkdownV2",
        )


async def handle_search_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Search Scuby's codebase for a pattern.
    /searchcode <pattern>
    """
    if not update.message:
        return

    pattern = " ".join(context.args or []).strip()
    if not pattern:
        await update.message.reply_text(
            "🔍 Usage: `/searchcode <pattern>`\n\n"
            "Example: `/searchcode fetch_rugcheck`",
            parse_mode="MarkdownV2",
        )
        return

    try:
        from self_improve import search_code
        results = search_code(pattern)

        if not results:
            await update.message.reply_text(
                f"No matches for `{escape_md(pattern)}`.",
                parse_mode="MarkdownV2",
            )
            return

        e = escape_md
        lines = [f"🔍 *Results for `{e(pattern)}` ({len(results)} matches):*\n"]
        for r in results[:15]:
            lines.append(f"`{e(r['file'])}:{r['line']}` {e(r['content'])}")

        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:4000] + "\n... (truncated)"

        await update.message.reply_text(text, parse_mode="MarkdownV2")
    except Exception as ex:
        logger.error(f"handle_search_code: {ex}", exc_info=True)
        await update.message.reply_text(
            "Search failed.",
            parse_mode="MarkdownV2",
        )


# ─── Git commands ─────────────────────────────────────────────────────────────

async def handle_git(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Git operations: status, diff, log, commit, branches.
    /git status|diff|log|commit|branches
    """
    if not update.message:
        return

    args = context.args or []
    subcmd = args[0] if args else "status"

    try:
        from git_ops import git_status, git_diff, git_log, git_commit, git_branches, git_add

        if subcmd == "status":
            result = await git_status()
            if "error" in result:
                await update.message.reply_text(f"Git error: {result['error']}")
                return
            e = escape_md
            lines = [f"🔍 *Git Status* — `{e(result['branch'])}`\n"]
            if result["ahead_behind"]:
                lines.append(f"_Ahead/behind: {e(result['ahead_behind'])}_\n")
            if not result["files"]:
                lines.append("✅ Working tree clean")
            else:
                lines.append(f"{result['total']} changed file(s):")
                for f in result["files"][:10]:
                    lines.append(f"  `{e(f['status'])}` {e(f['file'])}")
            await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")

        elif subcmd == "diff":
            staged = "--staged" in args or "-s" in args
            diff = await git_diff(staged=staged)
            label = "staged" if staged else "unstaged"
            if len(diff) > 3800:
                diff = diff[:3800] + "\n... (truncated)"
            await update.message.reply_text(
                f"📝 *Diff ({label}):*\n`{escape_md(diff)}`",
                parse_mode="MarkdownV2",
            )

        elif subcmd == "log":
            count = int(args[1]) if len(args) > 1 and args[1].isdigit() else 5
            commits = await git_log(count)
            e = escape_md
            lines = [f"📋 *Last {len(commits)} commits:*\n"]
            for c in commits:
                if "error" in c:
                    continue
                lines.append(f"`{e(c['hash'])}` {e(c['message'][:60])}")
                lines.append(f"  _{e(c['author'])} — {e(c['date'])}_\n")
            await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")

        elif subcmd == "commit":
            msg_text = " ".join(args[1:]).strip()
            if not msg_text:
                await update.message.reply_text("Usage: /git commit <message>")
                return
            result = await git_commit(msg_text)
            if result["success"]:
                await update.message.reply_text(
                    f"✅ Committed `{escape_md(result['hash'])}`: {escape_md(msg_text)}",
                    parse_mode="MarkdownV2",
                )
            else:
                await update.message.reply_text(f"❌ {escape_md(result['error'])}", parse_mode="MarkdownV2")

        elif subcmd == "branches":
            result = await git_branches()
            if "error" in result:
                await update.message.reply_text(result["error"])
                return
            e = escape_md
            lines = ["🌿 *Branches:*\n"]
            for b in result["current"]:
                lines.append(f"  *{e(b)}* (current)")
            for b in result["local"]:
                lines.append(f"  {e(b)}")
            await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")

        elif subcmd == "add":
            files = args[1:]
            if not files:
                await update.message.reply_text("Usage: /git add <file1> <file2>...")
                return
            result = await git_add(files)
            if result["added"]:
                await update.message.reply_text(f"✅ Staged: {', '.join(result['added'])}")
            if result["errors"]:
                await update.message.reply_text(f"❌ {', '.join(result['errors'])}")

        else:
            await update.message.reply_text(
                "Usage:\n"
                "  `/git status` — working tree status\n"
                "  `/git diff` — view changes\n"
                "  `/git log` — commit history\n"
                "  `/git commit <msg>` — commit changes\n"
                "  `/git branches` — list branches\n"
                "  `/git add <files>` — stage files",
                parse_mode="MarkdownV2",
            )
    except Exception as ex:
        logger.error(f"handle_git: {ex}", exc_info=True)
        await update.message.reply_text(f"Git error: {escape_md(str(ex))}", parse_mode="MarkdownV2")


# ─── Terminal / Run command ──────────────────────────────────────────────────

async def handle_run(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run a shell command in the project directory.
    /run <command>
    """
    if not update.message:
        return

    command = " ".join(context.args or []).strip()
    if not command:
        await update.message.reply_text(
            "🖥️ *Run Command*\n\n"
            "Usage: `/run <command>`\n\n"
            "Examples:\n"
            "  `/run python3 -c 'print(1+1)'`\n"
            "  `/run pip list`\n"
            "  `/run ls -la`\n"
            "  `/run python3 main.py --test`\n\n"
            "_Commands run in the project directory_",
            parse_mode="MarkdownV2",
        )
        return

    from terminal import run_command, is_command_safe
    safe, reason = is_command_safe(command)
    if not safe:
        await update.message.reply_text(f"🚫 Blocked: {escape_md(reason)}", parse_mode="MarkdownV2")
        return

    msg = await update.message.reply_text(
        f"🖥️ Running `{escape_md(command)}`...",
        parse_mode="MarkdownV2",
    )

    result = await run_command(command, timeout=30)
    output = result["output"] or result["error"]
    if len(output) > 3800:
        output = output[:3800] + "\n... (truncated)"

    status = "✅" if result["success"] else "❌"
    await msg.edit_text(
        f"{status} `{escape_md(command)}` (exit {result['exit_code']})\n\n"
        f"`{escape_md(output)}`",
        parse_mode="MarkdownV2",
    )


# ─── Package management ──────────────────────────────────────────────────────

async def handle_packages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Package management: list, install, outdated.
    /packages list|install|outdated
    """
    if not update.message:
        return

    args = context.args or []
    subcmd = args[0] if args else "list"

    from terminal import pip_list, pip_outdated, install_package

    if subcmd == "list":
        result = await pip_list()
        await update.message.reply_text(
            f"📦 *Installed packages:*\n`{escape_md(result)}`",
            parse_mode="MarkdownV2",
        )
    elif subcmd == "outdated":
        result = await pip_outdated()
        await update.message.reply_text(
            f"📦 *Outdated packages:*\n`{escape_md(result)}`",
            parse_mode="MarkdownV2",
        )
    elif subcmd == "install":
        pkg = args[1] if len(args) > 1 else ""
        if not pkg:
            await update.message.reply_text("Usage: /packages install <package>")
            return
        msg = await update.message.reply_text(f"📦 Installing `{escape_md(pkg)}`...", parse_mode="MarkdownV2")
        result = await install_package(pkg)
        output = result["output"][-500:] if result["output"] else result["error"]
        status = "✅" if result["success"] else "❌"
        await msg.edit_text(f"{status} `{escape_md(pkg)}`\n`{escape_md(output)}`", parse_mode="MarkdownV2")
    else:
        await update.message.reply_text(
            "Usage:\n"
            "  `/packages list` — installed packages\n"
            "  `/packages outdated` — check for updates\n"
            "  `/packages install <pkg>` — install a package",
            parse_mode="MarkdownV2",
        )


# ─── Health check ─────────────────────────────────────────────────────────────

async def handle_health(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check Scuby's health status.
    /health
    """
    if not update.message:
        return

    msg = await update.message.reply_text("🏥 Checking Scuby's health...", parse_mode="MarkdownV2")

    try:
        from self_improve import check_health
        health = await check_health()

        e = escape_md
        lines = ["🏥 *Scuby Health Report*\n"]

        # Bot status
        bot_icon = "✅" if health["bot_running"] else "❌"
        lines.append(f"{bot_icon} Bot process: {'running' if health['bot_running'] else 'NOT running'}")

        # Dependencies
        dep_icon = "✅" if health["deps_ok"] else "❌"
        lines.append(f"{dep_icon} Dependencies: {'OK' if health['deps_ok'] else 'MISSING'}")

        # Environment
        env_icon = "✅" if health["env_configured"] else "⚠️"
        lines.append(f"{env_icon} .env: {'configured' if health['env_configured'] else 'missing'}")

        # Tests
        test_icon = "✅" if health["tests_passing"] else "❌"
        lines.append(f"{test_icon} Tests: {health['tests_passed']} passed, {health['tests_failed']} failed")

        # Disk
        lines.append(f"💾 Disk: {e(health['disk'])}")

        await msg.edit_text("\n".join(lines), parse_mode="MarkdownV2")
    except Exception as ex:
        logger.error(f"handle_health: {ex}", exc_info=True)
        await msg.edit_text("Health check failed.", parse_mode="MarkdownV2")


# ─── Documentation ────────────────────────────────────────────────────────────

async def handle_docs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate documentation for a file or the whole project.
    /docs [file]
    """
    if not update.message:
        return

    args = context.args or []
    filename = args[0] if args else None

    msg = await update.message.reply_text("📝 Generating docs...", parse_mode="MarkdownV2")

    try:
        from docgen import generate_file_docs, generate_readme

        if filename:
            docs = await generate_file_docs(filename)
        else:
            docs = await generate_readme()

        if len(docs) > 4000:
            docs = docs[:4000] + "\n... (truncated)"

        await msg.edit_text(
            f"📝 *Documentation: {escape_md(filename or 'README')}*\n\n{docs}",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception as ex:
        logger.error(f"handle_docs: {ex}", exc_info=True)
        await msg.edit_text("Docs generation failed.", parse_mode="MarkdownV2")


# ─── Code review ──────────────────────────────────────────────────────────────

async def handle_review(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Review code — a file, recent changes, or specific lines.
    /review [file] [start] [end]
    """
    if not update.message:
        return

    args = context.args or []
    filename = args[0] if args else None
    start = int(args[1]) if len(args) > 1 and args[1].isdigit() else 1
    end = int(args[2]) if len(args) > 2 and args[2].isdigit() else None

    msg = await update.message.reply_text("🔍 Reviewing code...", parse_mode="MarkdownV2")

    try:
        from self_improve import read_file, read_file_lines
        from ai import _call_ai

        if filename:
            code = read_file_lines(filename, start, end)
            target = f"{filename} (lines {start}-{end or 'end'})"
        else:
            # Review recent git changes
            from git_ops import git_diff
            code = await git_diff()
            target = "recent git changes"

        if code.startswith("Error") or not code.strip():
            await msg.edit_text(f"Nothing to review for {target}", parse_mode="MarkdownV2")
            return

        prompt = (
            f"Review this Python code for a Telegram crypto bot.\n\n"
            f"CODE ({target}):\n{code[:4000]}\n\n"
            f"Provide:\n"
            f"1. Issues found (bugs, security, performance)\n"
            f"2. Suggestions for improvement\n"
            f"3. Overall rating (1-10)\n"
            f"Be concise. Focus on real problems, not style."
        )

        review = await _call_ai(prompt, [{"role": "user", "content": "Review this code"}], max_tokens=800)

        if len(review) > 4000:
            review = review[:4000]

        await msg.edit_text(
            f"🔍 *Code Review: {escape_md(target)}*\n\n{review}",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception as ex:
        logger.error(f"handle_review: {ex}", exc_info=True)
        await msg.edit_text("Review failed.", parse_mode="MarkdownV2")


# ─── Repo management ─────────────────────────────────────────────────────────

async def handle_repo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Clone, switch, list, or delete repos.
    /repo clone <url> — clone a GitHub repo
    /repo list — list cloned repos
    /repo switch <name> — switch active repo
    /repo delete <name> — delete a repo
    /repo overview [name] — repo structure + stats
    """
    if not update.message:
        return

    args = context.args or []
    subcmd = args[0] if args else "list"

    try:
        from repo import clone_repo, list_repos, delete_repo, repo_overview, set_active_repo, get_active_repo, read_repo_file

        if subcmd == "clone":
            url = args[1] if len(args) > 1 else ""
            name = args[2] if len(args) > 2 else None
            if not url:
                await update.message.reply_text("Usage: `/repo clone <github-url> [name]`", parse_mode="MarkdownV2")
                return
            msg = await update.message.reply_text(f"📦 Cloning `{escape_md(url)}`...", parse_mode="MarkdownV2")
            result = await clone_repo(url, name)
            if result["success"]:
                set_active_repo(result["name"])
                await msg.edit_text(
                    f"✅ Cloned *{escape_md(result['name'])}*\n"
                    f"📁 {result['files']} files | {result['size_mb']}MB",
                    parse_mode="MarkdownV2",
                )
            else:
                await msg.edit_text(f"❌ {escape_md(result['error'])}", parse_mode="MarkdownV2")

        elif subcmd == "list":
            repos = list_repos()
            if not repos:
                await update.message.reply_text(
                    "📦 No repos cloned yet.\n\nUsage: `/repo clone <github-url>`",
                    parse_mode="MarkdownV2",
                )
                return
            active = get_active_repo()
            e = escape_md
            lines = ["📦 *Cloned Repositories:*\n"]
            for r in repos:
                marker = " *" if r["active"] else ""
                suffix = "* (active)" if r["active"] else ""
                lines.append(f"{marker}{e(r['name'])}{suffix} — {r['files']} files")
            await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")

        elif subcmd == "switch":
            name = args[1] if len(args) > 1 else ""
            if not name:
                await update.message.reply_text("Usage: `/repo switch <name>`")
                return
            set_active_repo(name)
            await update.message.reply_text(f"✅ Switched to *{escape_md(name)}*", parse_mode="MarkdownV2")

        elif subcmd == "delete":
            name = args[1] if len(args) > 1 else ""
            if not name:
                await update.message.reply_text("Usage: `/repo delete <name>`")
                return
            result = await delete_repo(name)
            if result["success"]:
                await update.message.reply_text(f"✅ Deleted *{escape_md(name)}*", parse_mode="MarkdownV2")
            else:
                await update.message.reply_text(f"❌ {escape_md(result['error'])}", parse_mode="MarkdownV2")

        elif subcmd == "overview":
            name = args[1] if len(args) > 1 else None
            result = await repo_overview(name)
            if "error" in result:
                await update.message.reply_text(result["error"])
                return
            e = escape_md
            lines = [f"📦 *{e(result['name'])}* — {e(result['size'])}\n"]
            lines.append("_Languages:_ " + ", ".join(result["languages"]))
            lines.append("\n_Structure:_")
            for item in result["structure"][:10]:
                lines.append(f"  {e(item)}")
            if result["recent_commits"]:
                lines.append("\n_Recent commits:_")
                for c in result["recent_commits"][:3]:
                    lines.append(f"  {e(c)}")
            await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")

        else:
            await update.message.reply_text(
                "Usage:\n"
                "  `/repo clone <url>` — clone a repo\n"
                "  `/repo list` — list repos\n"
                "  `/repo switch <name>` — switch active repo\n"
                "  `/repo overview` — repo overview\n"
                "  `/repo delete <name>` — delete a repo",
                parse_mode="MarkdownV2",
            )
    except Exception as ex:
        logger.error(f"handle_repo: {ex}", exc_info=True)
        await update.message.reply_text(f"Repo error: {escape_md(str(ex))}", parse_mode="MarkdownV2")


# ─── Analysis commands ────────────────────────────────────────────────────────

async def handle_deps(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show dependency graph for a file or the whole project.
    /deps [file]
    """
    if not update.message:
        return

    args = context.args or []
    filename = args[0] if args else None

    msg = await update.message.reply_text("📦 Analyzing dependencies...", parse_mode="MarkdownV2")

    try:
        from analysis import analyze_imports, get_import_graph, find_circular_deps

        if filename:
            info = analyze_imports(filename)
            if "error" in info:
                await msg.edit_text(info["error"])
                return
            e = escape_md
            lines = [f"📦 *Dependencies of {e(filename)}:*\n"]
            if info["local_imports"]:
                lines.append(f"_Local imports:_ {e(', '.join(info['local_imports']))}")
            if info["imported_by"]:
                lines.append(f"_Imported by:_ {e(', '.join(info['imported_by']))}")
            await msg.edit_text("\n".join(lines), parse_mode="MarkdownV2")
        else:
            graph = get_import_graph()
            if len(graph) > 4000:
                graph = graph[:4000]
            await msg.edit_text(f"`{escape_md(graph)}`", parse_mode="MarkdownV2")
    except Exception as ex:
        logger.error(f"handle_deps: {ex}", exc_info=True)
        await msg.edit_text("Deps analysis failed.", parse_mode="MarkdownV2")


async def handle_deadcode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Find dead/unused functions in a file.
    /deadcode <file>
    """
    if not update.message:
        return

    args = context.args or []
    filename = args[0] if args else ""
    if not filename:
        await update.message.reply_text("Usage: `/deadcode <filename>`", parse_mode="MarkdownV2")
        return

    try:
        from analysis import find_dead_code
        dead = find_dead_code(filename)

        if not dead:
            await update.message.reply_text(f"✅ No dead code found in `{escape_md(filename)}`", parse_mode="MarkdownV2")
            return

        e = escape_md
        lines = [f"🔍 *Dead code in {e(filename)}:*\n"]
        for d in dead:
            kind = "async def" if d["is_async"] else "def"
            lines.append(f"  `{kind} {e(d['name'])}()` — line {d['line']}")
        await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")
    except Exception as ex:
        logger.error(f"handle_deadcode: {ex}", exc_info=True)
        await update.message.reply_text("Analysis failed.", parse_mode="MarkdownV2")


async def handle_envcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Audit environment variables.
    /envcheck
    """
    if not update.message:
        return

    try:
        from analysis import audit_env_vars
        result = audit_env_vars()

        e = escape_md
        lines = [f"🔐 *Environment Variables:* {result['set']}/{result['total']} set\n"]
        for var, info in result["vars"].items():
            if info["set"]:
                lines.append(f"  ✅ `{e(var)}` = {e(info['masked'])}")
            else:
                lines.append(f"  ❌ `{e(var)}` — NOT SET")

        await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")
    except Exception as ex:
        logger.error(f"handle_envcheck: {ex}", exc_info=True)
        await update.message.reply_text("Env check failed.", parse_mode="MarkdownV2")


async def handle_configcheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Validate project configuration.
    /configcheck
    """
    if not update.message:
        return

    try:
        from analysis import validate_config
        issues = validate_config()

        if not issues:
            await update.message.reply_text("✅ All configuration checks passed!")
            return

        e = escape_md
        lines = ["⚙️ *Configuration Issues:*\n"]
        for issue in issues:
            icon = "❌" if issue["severity"] == "error" else "⚠️"
            lines.append(f"{icon} `{e(issue['file'])}` — {e(issue['issue'])}")

        errors = sum(1 for i in issues if i["severity"] == "error")
        warnings = sum(1 for i in issues if i["severity"] == "warning")
        lines.append(f"\n📊 {errors} error(s), {warnings} warning(s)")

        await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")
    except Exception as ex:
        logger.error(f"handle_configcheck: {ex}", exc_info=True)
        await update.message.reply_text("Config check failed.", parse_mode="MarkdownV2")


async def handle_changelog(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generate changelog from recent commits.
    /changelog [count]
    """
    if not update.message:
        return

    args = context.args or []
    count = int(args[0]) if args and args[0].isdigit() else 20

    try:
        from git_ops import git_changelog
        changelog = await git_changelog(count)

        if len(changelog) > 4000:
            changelog = changelog[:4000]

        await update.message.reply_text(
            f"📋 *Changelog (last {count} commits):*\n\n{changelog}",
            parse_mode="Markdown",
        )
    except Exception as ex:
        logger.error(f"handle_changelog: {ex}", exc_info=True)
        await update.message.reply_text("Changelog failed.", parse_mode="MarkdownV2")


async def handle_coverage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run tests with coverage report.
    /coverage
    """
    if not update.message:
        return

    msg = await update.message.reply_text("🧪 Running coverage analysis...", parse_mode="MarkdownV2")

    try:
        from terminal import test_coverage
        result = await test_coverage()

        output = result["output"][-3000:] if result["output"] else result["error"]
        status = "✅" if result["success"] else "❌"
        await msg.edit_text(
            f"{status} *Coverage Report:*\n`{escape_md(output)}`",
            parse_mode="MarkdownV2",
        )
    except Exception as ex:
        logger.error(f"handle_coverage: {ex}", exc_info=True)
        await msg.edit_text("  Coverage failed\\.", parse_mode="MarkdownV2")


# ─── Type checking ───────────────────────────────────────────────────────────

async def handle_typecheck(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run mypy + pyflakes type checking.
    /typecheck [file]
    """
    if not update.message:
        return

    args = context.args or []
    filepath = args[0] if args else "."
    strict = "--strict" in args

    msg = await update.message.reply_text(
        f"🔍 Running type check on `{escape_md(filepath)}`...",
        parse_mode="MarkdownV2",
    )

    try:
        from terminal import full_type_check
        result = await full_type_check(filepath)

        output = result["summary"]
        if len(output) > 4000:
            output = output[:4000] + "\n... (truncated)"

        status = "✅" if result["success"] else "⚠️"
        await msg.edit_text(
            f"{status} *Type Check: {escape_md(filepath)}*\n\n{output}",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception as ex:
        logger.error(f"handle_typecheck: {ex}", exc_info=True)
        await msg.edit_text("  Type check failed\\.", parse_mode="MarkdownV2")


# ─── Multi-file refactor ─────────────────────────────────────────────────────

async def handle_refactor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Plan and preview a multi-file refactoring.
    /refactor <description>
    """
    if not update.message or not update.effective_user:
        return

    description = " ".join(context.args or []).strip()
    if not description:
        await update.message.reply_text(
            "🔧 *Scuby Refactor*\n\n"
            "Describe the refactoring you want:\n\n"
            "Usage:\n"
            "  `/refactor move all persistence to storage.py`\n"
            "  `/refactor split handlers.py into smaller files`\n"
            "  `/refactor extract API calls to a separate module`\n\n"
            "_Scuby reads the ENTIRE codebase and creates a plan_",
            parse_mode="MarkdownV2",
        )
        return

    msg = await update.message.reply_text(
        "🔧 Scuby is reading the entire codebase\\.\\.\\.\n"
        "_Planning the refactoring..._",
        parse_mode="MarkdownV2",
    )

    try:
        from codebase import plan_refactor
        plan = await plan_refactor(description)

        if len(plan) > 4000:
            plan = plan[:4000] + "\n... (truncated)"

        await msg.edit_text(
            f"🔧 *Refactoring Plan:*\n\n{plan}",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception as ex:
        logger.error(f"handle_refactor: {ex}", exc_info=True)
        await msg.edit_text("  Refactor planning failed\\.", parse_mode="MarkdownV2")


# ─── Full codebase context Q&A ──────────────────────────────────────────────

async def handle_context(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ask a question about the entire codebase.
    /context <question>
    """
    if not update.message or not update.effective_user:
        return

    question = " ".join(context.args or []).strip()
    if not question:
        await update.message.reply_text(
            "🧠 *Scuby Codebase Q&A*\n\n"
            "Ask anything about the entire codebase:\n\n"
            "Usage:\n"
            "  `/context how does the alert system work?`\n"
            "  `/context what functions does gemscore.py export?`\n"
            "  `/context trace the flow from handle_sniff to API call`\n"
            "  `/context what would break if I changed utils.py?`\n\n"
            "_Scuby loads ALL files into context and reasons across them_",
            parse_mode="MarkdownV2",
        )
        return

    msg = await update.message.reply_text(
        "🧠 Loading entire codebase into context\\.\\.\\.",
        parse_mode="MarkdownV2",
    )

    try:
        from codebase import codebase_question, get_codebase_stats
        stats = get_codebase_stats()
        answer = await codebase_question(question)

        if len(answer) > 4000:
            answer = answer[:4000] + "\n... (truncated)"

        header = f"_Loaded {stats['total_files']} files, {stats['total_lines']} lines into context_\n\n"
        await msg.edit_text(
            header + answer,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception as ex:
        logger.error(f"handle_context: {ex}", exc_info=True)
        await msg.edit_text("  Codebase analysis failed\\.", parse_mode="MarkdownV2")


# ─── Error trace ──────────────────────────────────────────────────────────────

async def handle_trace(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Trace an error through the full codebase.
    /trace <error message or stack trace>
    """
    if not update.message:
        return

    error_msg = " ".join(context.args or []).strip()
    if not error_msg:
        await update.message.reply_text(
            "🔍 Usage: `/trace <error message>`\n\n"
            "Example: `/trace KeyError: 'address' in handle_sniff`",
            parse_mode="MarkdownV2",
        )
        return

    msg = await update.message.reply_text("🔍 Tracing error through codebase...", parse_mode="MarkdownV2")

    try:
        from codebase import trace_error
        result = await trace_error(error_msg)

        if len(result) > 4000:
            result = result[:4000]

        await msg.edit_text(
            f"🔍 *Error Trace:*\n\n{result}",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
    except Exception as ex:
        logger.error(f"handle_trace: {ex}", exc_info=True)
        await msg.edit_text("  Error trace failed\\.", parse_mode="MarkdownV2")


# ─── Session memory ──────────────────────────────────────────────────────────

async def handle_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show or manage persistent session state.
    /session [show|clear]
    """
    if not update.message:
        return

    args = context.args or []
    subcmd = args[0] if args else "show"

    try:
        from codebase import load_session, clear_session, session_get

        if subcmd == "clear":
            clear_session()
            await update.message.reply_text("🧹 Session cleared!")
            return

        session = load_session()
        if not session:
            await update.message.reply_text(
                "🧠 No session data yet.\n\n"
                "Session data persists across bot restarts.\n"
                "It stores things like:\n"
                "  • Last analyzed tokens\n"
                "  • Pending fixes\n"
                "  • User preferences",
                parse_mode="MarkdownV2",
            )
            return

        e = escape_md
        lines = ["🧠 *Persistent Session Data:*\n"]
        for key, value in session.items():
            if key == "last_saved":
                continue
            val_str = str(value)[:80]
            lines.append(f"  `{e(key)}`: {e(val_str)}")
        lines.append(f"\n_Updated: {e(str(session.get('last_saved', 'unknown')))}")

        await update.message.reply_text("\n".join(lines), parse_mode="MarkdownV2")
    except Exception as ex:
        logger.error(f"handle_session: {ex}", exc_info=True)
        await update.message.reply_text("  Session check failed\\.", parse_mode="MarkdownV2")

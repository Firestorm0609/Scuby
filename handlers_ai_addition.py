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
            "🐾 *Ask Scuby anything\\!*\n\n"
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
            "Zoinks\\! 🐾 Scuby's brain glitched\\. Try again in a moment\\!",
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
            "🧹 Conversation cleared\\! Scuby starts fresh\\. 🐾",
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
            f"🐾 Already have *{MAX_SMART_FILTERS_PER_CHAT}* smart filters\\! "
            "Use /smartwatches to remove some first\\.",
            parse_mode="MarkdownV2",
        )
        return

    msg = await update.message.reply_text(
        "🎯 Scuby's parsing your filter\\.\\.\\. 🐾",
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
            "Ruh\\-roh\\! 🐾 Scuby couldn't parse that\\. Try:\n"
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
        await update.message.reply_text("🐾 No smart filters to remove\\!", parse_mode="MarkdownV2")
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
            f"✅ Removed *{removed}* filter\\(s\\) matching *{e(raw)}* 🐾\n\n"
            "_Use /smartwatches to see what's still running\\._",
            parse_mode="MarkdownV2",
        )
    else:
        await update.message.reply_text(
            f"Hmm\\! 🐾 No filter found matching *{e(raw)}*\\.\n"
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
    await query.answer("Setting up your filter! 🐾")

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
                f"🐾 Already at the max of *{MAX_SMART_FILTERS_PER_CHAT}* smart filters\\! "
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
            await query.edit_message_text("Ruh\\-roh\\! 🐾 Couldn't set that filter\\. Try /smartwatch manually\\.", parse_mode="MarkdownV2")
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

"""
PATCH — replace handle_message in handlers.py with this version.

Improvements:
  1. In groups, responds when user says "scooby" (not just @mention)
  2. Detects bare tickers in natural phrasing:
       "scooby what is mcap of punch"  →  sniffs $PUNCH
       "price of bonk"                 →  sniffs $BONK
       "check wif for me"              →  sniffs $WIF
       "wif price"                     →  sniffs $WIF
  3. Falls back to AI chat for everything else
"""

import re as _re

# Patterns that suggest the user wants token data for a bare word ─────────────
_TOKEN_INTENT_PATTERN = _re.compile(
    r"""
    (?:
        (?:mcap|market\s*cap|price|chart|check|sniff|
           volume|vol|liq|liquidity|fdv|info|data|
           what(?:'s|\s+is)?\s+(?:the\s+)?(?:mcap|price|chart))
        \s+(?:of|for|on)?\s*
        (?:\$?([A-Za-z]{2,10}))
    )
    |
    (?:
        (?:\$?([A-Za-z]{2,10}))
        \s+(?:mcap|market\s*cap|price|chart|volume|liq|info)
    )
    """,
    _re.IGNORECASE | _re.VERBOSE,
)

# Words that look like tickers but aren't tokens ──────────────────────────────
_NOT_TICKERS = {
    "the", "for", "of", "on", "in", "is", "it", "to", "a", "an",
    "me", "my", "us", "do", "get", "can", "and", "or", "but",
    "what", "how", "why", "who", "when", "where",
    "mcap", "market", "price", "chart", "check", "sniff",
    "volume", "vol", "liq", "info", "data", "fdv",
}


def _extract_bare_ticker(text: str) -> str | None:
    m = _TOKEN_INTENT_PATTERN.search(text)
    if m:
        ticker = m.group(1) or m.group(2)
        if ticker and ticker.lower() not in _NOT_TICKERS:
            return ticker.upper()
    return None


def _is_scooby_addressed(text: str, bot_username: str) -> bool:
    if f"@{bot_username}".lower() in text.lower():
        return True
    if _re.search(r'\bscooby\b', text, _re.IGNORECASE):
        return True
    return False


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    text         = update.message.text
    chat_type    = update.effective_chat.type if update.effective_chat else "private"
    bot_username = context.bot.username or ""

    # ── Group chat gate ───────────────────────────────────────────────────────
    # Only engage if there's a CA/$TICKER, bot is addressed, or it's a reply to bot
    if chat_type != "private":
        has_token = SOLANA_CA_PATTERN.search(text) or TICKER_PATTERN.search(text)
        is_reply_to_bot = (
            update.message.reply_to_message is not None
            and update.message.reply_to_message.from_user is not None
            and update.message.reply_to_message.from_user.username == bot_username
        )
        if not has_token and not _is_scooby_addressed(text, bot_username) and not is_reply_to_bot:
            return

    # ── Rate limit ────────────────────────────────────────────────────────────
    if not await _check_rate_limits(update, context):
        return

    await _track_stats(update, context)

    # Strip "scooby" and @mention so they don't confuse ticker/AI extraction
    clean = _re.sub(rf"@{_re.escape(bot_username)}", "", text, flags=_re.IGNORECASE)
    clean = _re.sub(r'\bscooby\b', "", clean, flags=_re.IGNORECASE).strip()

    http: httpx.AsyncClient = context.bot_data["http"]

    # ── 1. Explicit CA ────────────────────────────────────────────────────────
    ca_match = SOLANA_CA_PATTERN.search(clean)
    if ca_match:
        await _show_dual_card(update, context, "ca", ca_match.group(0))
        return

    # ── 2. Explicit $TICKER ───────────────────────────────────────────────────
    tick_match = TICKER_PATTERN.search(clean)
    if tick_match:
        await _show_dual_card(update, context, "ticker", tick_match.group(1).upper())
        return

    # ── 3. Bare ticker in natural phrasing ───────────────────────────────────
    bare = _extract_bare_ticker(clean)
    if bare:
        await _show_dual_card(update, context, "ticker", bare)
        return

    # ── 4. Pure conversation → AI Scooby ─────────────────────────────────────
    if not clean:
        return

    try:
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    except Exception:
        pass

    from ai import scooby_chat
    user_id = update.effective_user.id if update.effective_user else 0
    reply   = await scooby_chat(user_id, clean)

    try:
        await update.message.reply_text(escape_md(reply), parse_mode="MarkdownV2")
    except Exception:
        try:
            await update.message.reply_text(reply)
        except Exception as ex:
            logger.warning(f"handle_message AI reply failed: {ex}")

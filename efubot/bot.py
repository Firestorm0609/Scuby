"""eFootball DNA Build Bot — player archetype engineering via inline buttons."""
import logging
import sys
import os
import time
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, filters,
    ContextTypes,
)
from telegram.error import TelegramError

from scraper import search_players, fetch_player_detail, fetch_player_index
from optimizer import (
    DNA_CATEGORIES,
    optimize_dna, format_dna_result,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Conversation states
# ---------------------------------------------------------------------------
MAIN, SEARCHING = range(2)

# ---------------------------------------------------------------------------
# Index cache with TTL
# ---------------------------------------------------------------------------
_INDEX_CACHE: Optional[list] = None
_INDEX_FETCHED_AT: float = 0.0
INDEX_TTL = 3600


def get_index() -> list:
    global _INDEX_CACHE, _INDEX_FETCHED_AT
    now = time.monotonic()
    if _INDEX_CACHE is None or (now - _INDEX_FETCHED_AT) > INDEX_TTL:
        logger.info("Refreshing player index cache…")
        _INDEX_CACHE = fetch_player_index()
        _INDEX_FETCHED_AT = now
    return _INDEX_CACHE or []


# ---------------------------------------------------------------------------
# Static text
# ---------------------------------------------------------------------------

MAIN_MENU_TEXT = (
    "🧬 *eFootball DNA Lab*\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "Forge any player into a weapon.\n"
    "Pick upgrades · Mutate playstyles\n"
    "Unlock *GOAT‑tier* DNA potential.\n\n"
    "_Not traits. Not stats. Pure DNA._"
)

GUIDE_TEXT = (
    "📡 *DNA Engineering Manual*\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "*9 Engineering Modules*\n\n"
    "⚡ *Athletic Engine* — Speed & stamina\n"
    "🎮 *Ball Mastery* — Dribbling & control\n"
    "🎯 *Finishing Lab* — Shooting & curl\n"
    "🧠 *Football IQ* — Positioning & vision\n"
    "🚀 *Playstyle Mutation* — Role transforms\n"
    "🔥 *Pressing & Intensity* — Defensive\n"
    "🪽 *Wide Threat* — Winger builds\n"
    "🛡️ *Defensive Core* — Defender builds\n"
    "⭐ *Signature Builds* — Legend archetypes\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "*Hidden Mechanics Used:*\n\n"
    "🔓 *Stat Thresholds* — 86 TP unlocks\n"
    "   faster turns. 86 Balance = smoother\n"
    "   movement. 90 Accel = explosive step.\n\n"
    "⚡ *Skill Synergies* — Through Passing\n"
    "   gives +20% passing. Long-Range Curler\n"
    "   bypasses 99 cap. Double Touch recovers\n"
    "   Balance for accurate follow-up shots.\n\n"
    "📐 *Body Type* — Height, weight & limb\n"
    "   proportions affect how builds feel.\n\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "*Builds that make opponents ask:*\n"
    "_\"How is that card performing like that?\"_"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _card_label(r: dict) -> str:
    """Build a search result button label that distinguishes card versions."""
    label = f"{r['name']}  ({r['overall']} OVR)"
    extras = []
    if r.get("position"):
        extras.append(r["position"])
    if r.get("cardType"):
        extras.append(r["cardType"])
    if r.get("team"):
        extras.append(r["team"][:15])  # Truncate long team names
    if extras:
        label += f"  · {' · '.join(extras)}"
    return label


def _card_caption(detail: dict) -> str:
    """Build the photo caption for the player confirmation screen."""
    name      = detail.get("name", "Unknown")
    overall   = detail.get("overall", "?")
    position  = detail.get("position", "")
    style     = detail.get("playingStyle", "")
    card_type = detail.get("cardType", "")
    level_cap = detail.get("levelCap", "")

    lines = [f"*{name}*", "━━━━━━━━━━━━━━━━━━━━━━"]

    meta = f"{overall} OVR"
    if card_type:
        meta += f"  ·  🃏 {card_type}"
    if position:
        meta += f"  ·  📍 {position}"
    lines.append(meta)

    if style:
        lines.append(f"_{style}_")
    if level_cap:
        lines.append(f"⬆️  Level cap: {level_cap}")

    skills = detail.get("skills", [])
    if skills:
        skill_names = [s.replace("Control", "Ctrl").replace("Passing", "Pass") for s in skills]
        lines.append(f"⚙️  {', '.join(skill_names[:6])}{' +' + str(len(skills) - 6) if len(skills) > 6 else ''}")

    # Hidden body stats
    height = detail.get("height")
    jump = detail.get("baseStats", {}).get("jump", 0)
    if height and jump:
        jump_height = height + 54 + (jump - 40) * 0.6
        lines.append(f"📐  Jump height: {jump_height:.0f}cm (Height {height}cm + Jump {jump})")

    lines.append("")
    lines.append("◈  Is this the card to engineer?")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Keyboards
# ---------------------------------------------------------------------------

def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔬  Search Player", callback_data="nav:search")],
        [InlineKeyboardButton("📡  DNA Manual",    callback_data="nav:guide")],
    ])


def carousel_keyboard(index: int, total: int, player_id: int,
                      sort_mode: str = "ovr", pos_filter: str = "all") -> InlineKeyboardMarkup:
    """Prev / Next nav + pick + filter/sort + back — shown under the card photo."""
    nav_row = []
    if index > 0:
        nav_row.append(InlineKeyboardButton("‹  Prev", callback_data=f"carousel:{index - 1}"))
    nav_row.append(InlineKeyboardButton(f"{index + 1} / {total}", callback_data="noop"))
    if index < total - 1:
        nav_row.append(InlineKeyboardButton("Next  ›", callback_data=f"carousel:{index + 1}"))

    # Sort toggle
    sort_label = "🕐 Newest First" if sort_mode == "newest" else "⭐ Highest OVR"
    sort_next = "ovr" if sort_mode == "newest" else "newest"

    # Position filter row
    pos_buttons = []
    positions = ["all", "CF", "SS", "LWF", "RWF", "AMF", "CMF", "DMF", "CB", "LB", "RB"]
    for pos in positions:
        label = pos.upper() if pos != "all" else "All"
        marker = "✓" if pos == pos_filter else ""
        pos_buttons.append(InlineKeyboardButton(
            f"{marker}{label}",
            callback_data=f"filter:{pos}:{sort_next}",
        ))

    # Split position buttons into rows of 4
    filter_rows = [pos_buttons[i:i+4] for i in range(0, len(pos_buttons), 4)]

    return InlineKeyboardMarkup([
        nav_row,
        [InlineKeyboardButton("⚗️  Engineer This Card", callback_data=f"confirm:{player_id}")],
        [InlineKeyboardButton(sort_label, callback_data=f"sort:{sort_next}:{pos_filter}")],
        *filter_rows,
        [InlineKeyboardButton("↩  New Search", callback_data="nav:search"),
         InlineKeyboardButton("⌂  Menu",       callback_data="nav:main")],
    ])


def confirm_keyboard(player_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⚗️  Yes, engineer this card", callback_data=f"confirm:{player_id}")],
        [InlineKeyboardButton("←  Back to results",          callback_data="nav:search")],
    ])


def category_keyboard(player_id: int) -> InlineKeyboardMarkup:
    rows = []
    cats = list(DNA_CATEGORIES.items())
    for i in range(0, len(cats), 2):
        row = []
        for cat_key, cat in cats[i:i+2]:
            row.append(InlineKeyboardButton(
                cat["label"],
                callback_data=f"cat:{player_id}:{cat_key}",
            ))
        rows.append(row)
    rows.append([InlineKeyboardButton("←  Back to search", callback_data="nav:search")])
    return InlineKeyboardMarkup(rows)


def upgrade_keyboard(player_id: int, cat_key: str) -> InlineKeyboardMarkup:
    cat = DNA_CATEGORIES.get(cat_key, {})
    upgrades = cat.get("upgrades", {})
    rows = []
    upg_list = list(upgrades.items())
    for i in range(0, len(upg_list), 2):
        row = []
        for upg_key, upg in upg_list[i:i+2]:
            row.append(InlineKeyboardButton(
                upg["label"],
                callback_data=f"upg:{player_id}:{cat_key}:{upg_key}",
            ))
        rows.append(row)
    rows.append([InlineKeyboardButton(
        "←  DNA Categories",
        callback_data=f"confirm:{player_id}",
    )])
    return InlineKeyboardMarkup(rows)



def result_keyboard(player_id: int, cat_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "⟳  Different Upgrade",
            callback_data=f"cat:{player_id}:{cat_key}",
        )],
        [InlineKeyboardButton(
            "◈  New Category",
            callback_data=f"confirm:{player_id}",
        )],
        [InlineKeyboardButton("⌂  Main Menu", callback_data="nav:main")],
    ])


# ---------------------------------------------------------------------------
# Entry point — /start
# ---------------------------------------------------------------------------

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await update.message.reply_text(
        MAIN_MENU_TEXT,
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard(),
    )
    return MAIN


# ---------------------------------------------------------------------------
# nav:* callbacks
# ---------------------------------------------------------------------------

async def nav_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, dest = query.data.split(":", 1)

    if dest == "main":
        context.user_data.clear()
        await _safe_edit_text(
            query, MAIN_MENU_TEXT,
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
        return MAIN

    if dest == "search":
        context.user_data.pop("player_detail", None)
        await _safe_edit_text(
            query,
            "🔬 *Player Search*\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "↳ Type a player name below\n"
            "  We'll scan the full database.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("←  Back", callback_data="nav:main")]
            ]),
        )
        return SEARCHING

    if dest == "guide":
        await _safe_edit_text(
            query, GUIDE_TEXT, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("←  Back", callback_data="nav:main")]
            ]),
        )
        return MAIN

    return MAIN


# ---------------------------------------------------------------------------
# Text input: player search
# ---------------------------------------------------------------------------

async def search_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.message.text.strip()
    if not query_text:
        await update.message.reply_text("Please type a player name.")
        return SEARCHING

    try:
        await update.message.delete()
    except Exception:
        pass

    index = get_index()
    if not index:
        await context.bot.send_message(
            update.effective_chat.id,
            "⚠️ *Could not load player index.*\n\nPlease try again in a moment.",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard(),
        )
        return MAIN

    results = search_players(query_text, index)

    if not results:
        await context.bot.send_message(
            update.effective_chat.id,
            f"❌ *No results for* _{query_text}_\n\n"
            "Try a different spelling, or search\n"
            "by nickname, club, or first name only.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("↩  Search Again", callback_data="nav:search")],
                [InlineKeyboardButton("⌂  Main Menu",   callback_data="nav:main")],
            ]),
        )
        return MAIN

    # Pre-fetch positions for first 20 results so filters work immediately
    await context.bot.send_message(
        update.effective_chat.id,
        f"🔍 _Scanning {len(results)} cards..._",
        parse_mode="Markdown",
    )
    for r in results:
        try:
            detail = fetch_player_detail(r["id"])
            if detail:
                r["position"] = detail.get("position", "")
                r["cardType"] = detail.get("playingStyle", "")
                r["team"] = detail.get("team", "")
        except Exception:
            pass

    context.user_data["last_results"] = results
    context.user_data["all_results"] = results
    context.user_data["carousel_index"] = 0
    context.user_data["sort_mode"] = "ovr"
    context.user_data["pos_filter"] = "all"

    # Show first card immediately as a photo carousel
    await _send_carousel_card(context, update.effective_chat.id, 0)
    return MAIN


# ---------------------------------------------------------------------------
# Carousel helpers
# ---------------------------------------------------------------------------

async def _send_carousel_card(context: ContextTypes.DEFAULT_TYPE, chat_id: int, index: int) -> None:
    """Fetch player detail and send it as a new photo carousel message."""
    results = context.user_data.get("last_results", [])
    if not results or index < 0 or index >= len(results):
        return

    r = results[index]
    player_id = r["id"]
    total = len(results)

    try:
        detail = fetch_player_detail(player_id)
    except Exception as exc:
        logger.error("Carousel fetch error for player %s: %s", player_id, exc)
        detail = None

    if detail:
        context.user_data["player_detail"] = detail

    if detail:
        context.user_data["player_detail"] = detail
        # Enrich the result with detail info for label
        r["position"] = detail.get("position", r.get("position", ""))
        r["cardType"] = detail.get("playingStyle", r.get("cardType", ""))
        r["team"] = detail.get("team", r.get("team", ""))

    caption = _card_caption(detail) if detail else f"*{r['name']}*  ·  {r['overall']} OVR"
    img_url = detail.get("imageUrl") if detail else None
    sort_mode = context.user_data.get("sort_mode", "ovr")
    pos_filter = context.user_data.get("pos_filter", "all")
    keyboard = carousel_keyboard(index, total, player_id, sort_mode, pos_filter)

    if img_url:
        try:
            await context.bot.send_photo(
                chat_id=chat_id,
                photo=img_url,
                caption=caption,
                parse_mode="Markdown",
                reply_markup=keyboard,
            )
            return
        except TelegramError as e:
            logger.warning("send_photo failed for player %s: %s", player_id, e)

    # Fallback: text message
    await context.bot.send_message(
        chat_id=chat_id,
        text=caption,
        parse_mode="Markdown",
        reply_markup=keyboard,
    )


async def carousel_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle ◀️ Prev / Next ▶️ navigation — swaps the photo in-place."""
    query = update.callback_query
    await query.answer()

    index = int(query.data.split(":")[1])
    results = context.user_data.get("last_results", [])
    if not results or index < 0 or index >= len(results):
        return MAIN

    r = results[index]
    player_id = r["id"]
    total = len(results)
    context.user_data["carousel_index"] = index

    try:
        detail = fetch_player_detail(player_id)
    except Exception as exc:
        logger.error("Carousel nav fetch error for player %s: %s", player_id, exc)
        detail = None

    if detail:
        context.user_data["player_detail"] = detail
        # Enrich the result with detail info for label
        r["position"] = detail.get("position", r.get("position", ""))
        r["cardType"] = detail.get("playingStyle", r.get("cardType", ""))
        r["team"] = detail.get("team", r.get("team", ""))

    caption = _card_caption(detail) if detail else f"*{r['name']}*  ·  {r['overall']} OVR"
    img_url = detail.get("imageUrl") if detail else None
    sort_mode = context.user_data.get("sort_mode", "ovr")
    pos_filter = context.user_data.get("pos_filter", "all")
    keyboard = carousel_keyboard(index, total, player_id, sort_mode, pos_filter)

    if img_url:
        try:
            await query.edit_message_media(
                media=InputMediaPhoto(media=img_url, caption=caption, parse_mode="Markdown"),
                reply_markup=keyboard,
            )
            return MAIN
        except TelegramError as e:
            logger.warning("edit_message_media failed: %s", e)

    # Fallback: just update the caption / text
    try:
        await query.edit_message_caption(caption=caption, parse_mode="Markdown", reply_markup=keyboard)
    except TelegramError:
        try:
            await query.edit_message_text(caption, parse_mode="Markdown", reply_markup=keyboard)
        except TelegramError:
            await query.message.reply_text(caption, parse_mode="Markdown", reply_markup=keyboard)

    return MAIN


# ---------------------------------------------------------------------------
# confirm:{id} → show DNA categories (works for both photo and text messages)
# ---------------------------------------------------------------------------

async def confirm_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    player_id = int(query.data.split(":", 1)[1])
    context.user_data["player_id"] = player_id

    detail = context.user_data.get("player_detail", {})
    player_name = detail.get("name") or f"Player {player_id}"
    player_ovr  = detail.get("overall", "")

    ovr_str = f"  ·  {player_ovr} OVR" if player_ovr else ""
    text = (
        f"◈ *{player_name}*{ovr_str}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Choose a *DNA module* to engineer:"
    )

    await _safe_edit_text(
        query, text,
        parse_mode="Markdown",
        reply_markup=category_keyboard(player_id),
    )
    return MAIN


# ---------------------------------------------------------------------------
# cat / upg callbacks (unchanged logic, updated back buttons)
# ---------------------------------------------------------------------------

async def category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    parts     = query.data.split(":")
    player_id = int(parts[1])
    cat_key   = parts[2]

    cat = DNA_CATEGORIES.get(cat_key)
    if not cat:
        await query.answer("Unknown category.", show_alert=True)
        return MAIN

    detail = context.user_data.get("player_detail", {})
    player_name = detail.get("name") or f"Player {player_id}"

    await _safe_edit_text(
        query,
        f"{cat['label']}\n"
        f"_{cat['desc']}_\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"◈ *{player_name}*  —  Pick your upgrade:",
        parse_mode="Markdown",
        reply_markup=upgrade_keyboard(player_id, cat_key),
    )
    return MAIN


async def upgrade_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Run the optimizer immediately with GOAT Mutation budget."""
    query = update.callback_query
    await query.answer("Engineering DNA…")

    parts     = query.data.split(":")
    player_id = int(parts[1])
    cat_key   = parts[2]
    upg_key   = parts[3]

    cat     = DNA_CATEGORIES.get(cat_key, {})
    upgrade = cat.get("upgrades", {}).get(upg_key)
    if not upgrade:
        await query.answer("Unknown upgrade.", show_alert=True)
        return MAIN

    await _safe_edit_text(query, "⚗️ _Engineering DNA build…_", parse_mode="Markdown")

    player_data = context.user_data.get("player_detail")
    if not player_data or "baseStats" not in player_data:
        try:
            player_data = fetch_player_detail(player_id)
        except Exception as exc:
            logger.error("Fetch error for player %s: %s", player_id, exc)
            player_data = None

    if not player_data or "baseStats" not in player_data:
        await context.bot.send_message(
            update.effective_chat.id,
            "⚠️ *No stat data found for this player.*\n\nTry a different card.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⌂  Main Menu", callback_data="nav:main")]
            ]),
        )
        return MAIN

    try:
        result = optimize_dna(player_data, cat_key, upg_key)
        text   = format_dna_result(result)
    except Exception as exc:
        logger.error("Optimizer error for player %s: %s", player_id, exc)
        await context.bot.send_message(
            update.effective_chat.id,
            "⚠️ *DNA engineering failed.*\n\nPlease try a different upgrade.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⌂  Main Menu", callback_data="nav:main")]
            ]),
        )
        return MAIN

    await context.bot.send_message(
        update.effective_chat.id,
        text,
        parse_mode="Markdown",
        reply_markup=result_keyboard(player_id, cat_key),
    )
    return MAIN


async def filter_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle position filter buttons."""
    query = update.callback_query
    await query.answer()

    # filter:{pos}:{sort_mode}
    parts = query.data.split(":")
    pos_filter = parts[1]
    sort_mode = parts[2] if len(parts) > 2 else context.user_data.get("sort_mode", "ovr")

    context.user_data["pos_filter"] = pos_filter
    context.user_data["sort_mode"] = sort_mode

    # Re-filter and re-sort results
    all_results = context.user_data.get("all_results", [])
    filtered = all_results
    if pos_filter != "all":
        filtered = [r for r in all_results if r.get("position", "") == pos_filter]

    if sort_mode == "newest":
        filtered = sorted(filtered, key=lambda x: -x["id"])
    else:
        filtered = sorted(filtered, key=lambda x: -x["overall"])

    context.user_data["last_results"] = filtered
    context.user_data["carousel_index"] = 0

    if not filtered:
        await _safe_edit_text(
            query,
            f"No {pos_filter.upper()} cards found for this player.\n"
            "Try a different position filter.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("↩  Show All", callback_data=f"filter:all:{sort_mode}")],
                [InlineKeyboardButton("⌂  Menu", callback_data="nav:main")],
            ]),
        )
        return MAIN

    # Show first filtered result
    await _send_carousel_card(context, update.effective_chat.id, 0)
    return MAIN


async def sort_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle sort toggle button."""
    query = update.callback_query
    await query.answer()

    # sort:{sort_mode}:{pos_filter}
    parts = query.data.split(":")
    sort_mode = parts[1]
    pos_filter = parts[2] if len(parts) > 2 else context.user_data.get("pos_filter", "all")

    context.user_data["sort_mode"] = sort_mode
    context.user_data["pos_filter"] = pos_filter

    # Re-filter and re-sort results
    all_results = context.user_data.get("all_results", [])
    filtered = all_results
    if pos_filter != "all":
        filtered = [r for r in all_results if r.get("position", "") == pos_filter]

    if sort_mode == "newest":
        filtered = sorted(filtered, key=lambda x: -x["id"])
    else:
        filtered = sorted(filtered, key=lambda x: -x["overall"])

    context.user_data["last_results"] = filtered
    context.user_data["carousel_index"] = 0

    if filtered:
        await _send_carousel_card(context, update.effective_chat.id, 0)
    return MAIN


async def _safe_edit_text(query, text: str, **kwargs):
    """Edit a message as text whether it's currently a text or photo message."""
    try:
        await query.edit_message_text(text, **kwargs)
    except TelegramError:
        # Message was a photo — edit the caption instead, then swap to text
        try:
            await query.edit_message_caption(caption=text, **kwargs)
        except TelegramError:
            # Last resort: send a new message
            await query.message.reply_text(text, **kwargs)


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------

async def unexpected_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "◈ Use the buttons below to navigate.",
        reply_markup=main_menu_keyboard(),
    )
    return MAIN


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("Error: TELEGRAM_BOT_TOKEN not set. Copy .env.example → .env and fill it in.")
        sys.exit(1)

    logger.info("Pre-loading player index…")
    get_index()

    app = Application.builder().token(token).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MAIN: [
                CallbackQueryHandler(nav_callback,      pattern=r"^nav:"),
                CallbackQueryHandler(lambda u, c: u.callback_query.answer(), pattern=r"^noop$"),
                CallbackQueryHandler(carousel_callback, pattern=r"^carousel:"),
                CallbackQueryHandler(filter_callback,  pattern=r"^filter:"),
                CallbackQueryHandler(sort_callback,    pattern=r"^sort:"),
                CallbackQueryHandler(confirm_callback,  pattern=r"^confirm:"),
                CallbackQueryHandler(category_callback, pattern=r"^cat:"),
                CallbackQueryHandler(upgrade_callback,  pattern=r"^upg:"),

                MessageHandler(filters.TEXT & ~filters.COMMAND, unexpected_message),
            ],
            SEARCHING: [
                CallbackQueryHandler(nav_callback, pattern=r"^nav:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, search_input),
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        per_user=True,
        per_chat=True,
    )

    app.add_handler(conv)

    print("🧬 DNA Lab Bot started. Press Ctrl+C to stop.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

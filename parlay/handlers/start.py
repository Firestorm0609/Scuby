from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from database.db import get_or_create_user, SessionLocal, User
from sqlalchemy import select, func
from utils.helpers import safe_edit

TERMS_TEXT = (
    "📜 *Terms & Conditions*\n\n"
    "By using this bot, you agree to the following:\n\n"
    "1️⃣ This bot provides *statistical analysis* and *informational purposes only*.\n\n"
    "2️⃣ Nothing here constitutes *financial advice*, a recommendation, or a guarantee of results.\n\n"
    "3️⃣ Sports betting involves *risk*. Past performance does not guarantee future results.\n\n"
    "4️⃣ You are solely responsible for your betting decisions.\n\n"
    "5️⃣ The bot uses AI and statistical models — outputs may contain errors.\n\n"
    "6️⃣ Always bet responsibly. Never wager more than you can afford to lose.\n\n"
    "7️⃣ Must be 18+ to use this service.\n\n"
    "_Tap *\"I Accept\"* to continue._"
)


def _main_menu_markup():
    kb = [
        [InlineKeyboardButton("🔴 Live", callback_data="menu_live"),
         InlineKeyboardButton("🤖 Smart Bet", callback_data="menu_smartbet")],
        [InlineKeyboardButton("⚡ Ultra Mode", callback_data="menu_ultra"),
         InlineKeyboardButton("⚡ Quick Picks", callback_data="menu_quickpicks")],
        [InlineKeyboardButton("📅 Fixtures", callback_data="menu_fixtures"),
         InlineKeyboardButton("⚙️ Settings", callback_data="menu_sports")],
        [InlineKeyboardButton("❓ Help", callback_data="menu_help")],
    ]
    return InlineKeyboardMarkup(kb)


async def _get_user_count():
    """Get total registered user count."""
    try:
        async with SessionLocal() as s:
            count = (await s.execute(select(func.count()).select_from(User))).scalar() or 0
            return count
    except Exception:
        return 0


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    db_user = await get_or_create_user(user.id, user.username)

    # Check if terms accepted
    if not db_user.terms_accepted:
        kb = [[InlineKeyboardButton("✅ I Accept", callback_data="accept_terms")]]
        text = TERMS_TEXT
        if update.message:
            await update.message.reply_text(text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await safe_edit(update.callback_query, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))
        return

    total_users = await _get_user_count()
    text = (
        f"👋 *Hey {user.first_name}!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ *AI Sports Intelligence*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔴 Live scores across all sports\n"
        "🤖 AI-powered daily Smart Bet\n"
        "⚡ Quick Picks parlay builder\n"
        "📅 Full fixture coverage\n\n"
        f"👥 _{total_users} user{'s' if total_users != 1 else ''} trust this bot_\n\n"
        "👇 _Choose an option below_"
    )
    markup = _main_menu_markup()
    if update.message:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=markup)
    else:
        await safe_edit(update.callback_query, text, parse_mode="Markdown", reply_markup=markup)


async def accept_terms_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle 'I Accept' button press."""
    q = update.callback_query
    await q.answer("✅ Accepted!")

    user_id = update.effective_user.id
    async with SessionLocal() as s:
        res = await s.execute(select(User).where(User.tg_id == user_id))
        user = res.scalar_one_or_none()
        if user:
            user.terms_accepted = True
            await s.commit()

    user = update.effective_user
    total_users = await _get_user_count()
    text = (
        f"👋 *Hey {user.first_name}!*\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ *AI Sports Intelligence*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔴 Live scores across all sports\n"
        "🤖 AI-powered daily Smart Bet\n"
        "⚡ Quick Picks parlay builder\n"
        "📅 Full fixture coverage\n\n"
        f"👥 _{total_users} user{'s' if total_users != 1 else ''} trust this bot_\n\n"
        "👇 _Choose an option below_"
    )
    await safe_edit(q, text, parse_mode="Markdown", reply_markup=_main_menu_markup())


async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "❓ *How It Works*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔴 *Live*\n"
        "Real-time scores across all sports\n\n"
        "🤖 *Smart Bet*\n"
        "AI-curated daily parlay — the safest ~2× pick of the day\n\n"
        "⚡ *Quick Picks*\n"
        "Choose your target odds and build a custom parlay\n\n"
        "📅 *Fixtures*\n"
        "Today's upcoming matches with live odds\n\n"
        "⚙️ *Settings*\n"
        "Choose your sports, markets & timezone\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🤖 _@fireparlays_"
    )
    kb = [[InlineKeyboardButton("🏠 Back", callback_data="menu_main")]]
    markup = InlineKeyboardMarkup(kb)
    if update.callback_query:
        await safe_edit(update.callback_query, text, parse_mode="Markdown", reply_markup=markup)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=markup)

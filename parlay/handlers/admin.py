"""
Admin Control Room — passkey management, user management, bot stats.
Only accessible to configured admin IDs.
"""
import secrets
import string
import logging
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from sqlalchemy import select, func
from database.db import get_bot_stats, SessionLocal, User, PassKey, SmartBetPick
from utils.helpers import safe_edit
from config import is_admin

logger = logging.getLogger(__name__)


def _admin_check(user_id: int) -> bool:
    return is_admin(user_id)


def _generate_passkey(length=8) -> str:
    """Generate a short, memorable passkey (e.g. A3K9-B2M7)."""
    chars = string.ascii_uppercase + string.digits
    part1 = "".join(secrets.choice(chars) for _ in range(4))
    part2 = "".join(secrets.choice(chars) for _ in range(4))
    return f"{part1}-{part2}"


# ─── Admin Menu ─────────────────────────────────────────────────────

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main admin control room."""
    user_id = update.effective_user.id
    if not _admin_check(user_id):
        if update.callback_query:
            await update.callback_query.answer("❌ Admin only.", show_alert=True)
        else:
            await update.message.reply_text("❌ Admin only.")
        return

    q = update.callback_query
    if q:
        await q.answer()

    stats = await get_bot_stats(online_minutes=5)

    async with SessionLocal() as s:
        total_keys = (await s.execute(
            select(func.count()).select_from(PassKey))).scalar() or 0
        active_keys = (await s.execute(
            select(func.count()).select_from(PassKey).where(
                PassKey.is_used == False,
                PassKey.expires_at > datetime.utcnow()
            ))).scalar() or 0
        premium_users = (await s.execute(
            select(func.count()).select_from(User).where(User.is_premium == True))).scalar() or 0

    text = (
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎛️ *Admin Control Room*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Users: *{stats['total']}* | Online: *{stats['online']}*\n"
        f"📅 Active today: *{stats['today']}*\n\n"
        f"🔑 Passkeys: *{total_keys}* total | *{active_keys}* active\n"
        f"⭐ Premium: *{premium_users}* users"
    )

    kb = [
        [InlineKeyboardButton("🔑 Generate Passkeys", callback_data="admin_genkey")],
        [InlineKeyboardButton("📋 View Passkeys", callback_data="admin_viewkeys")],
        [
            InlineKeyboardButton("📊 Bot Stats", callback_data="menu_botstats"),
            InlineKeyboardButton("👥 User List", callback_data="admin_users"),
        ],
        [InlineKeyboardButton("⭐ Toggle Premium", callback_data="admin_togglepremium")],
        [InlineKeyboardButton("🔄 Refresh", callback_data="admin_menu")],
    ]

    markup = InlineKeyboardMarkup(kb)
    if q:
        await safe_edit(q, text, parse_mode="Markdown", reply_markup=markup)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=markup)


# ─── Generate Passkeys ──────────────────────────────────────────────

async def admin_genkey_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not _admin_check(update.effective_user.id):
        await q.answer("❌ Admin only.", show_alert=True)
        return
    await q.answer()

    text = (
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "🔑 *Generate Passkeys*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "How many passkeys?\n"
        "_Single-use, 24hr expiry each._"
    )
    kb = [
        [
            InlineKeyboardButton("1", callback_data="admin_gendone_1"),
            InlineKeyboardButton("5", callback_data="admin_gendone_5"),
            InlineKeyboardButton("10", callback_data="admin_gendone_10"),
        ],
        [
            InlineKeyboardButton("25", callback_data="admin_gendone_25"),
            InlineKeyboardButton("50", callback_data="admin_gendone_50"),
            InlineKeyboardButton("100", callback_data="admin_gendone_100"),
        ],
        [InlineKeyboardButton("◀️ Back", callback_data="admin_menu")],
    ]
    await safe_edit(q, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


async def admin_gendone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not _admin_check(update.effective_user.id):
        await q.answer("❌ Admin only.", show_alert=True)
        return
    await q.answer("🔑 Generating...")

    count = int(q.data[len("admin_gendone_"):])
    now = datetime.utcnow()
    expires = now + timedelta(hours=24)
    keys = []

    async with SessionLocal() as s:
        for _ in range(count):
            key = _generate_passkey()
            while (await s.execute(select(PassKey).where(PassKey.key == key))).scalar_one_or_none():
                key = _generate_passkey()

            pk = PassKey(
                key=key,
                created_by=update.effective_user.id,
                created_at=now,
                expires_at=expires,
                is_used=False,
            )
            s.add(pk)
            keys.append(key)
        await s.commit()

    key_lines = "\n".join(f"`{k}`" for k in keys)
    text = (
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ *Generated {count} passkey{'s' if count > 1 else ''}*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⏰ Expires: *{expires.strftime('%b %d, %H:%M UTC')}*\n\n"
        f"{key_lines}\n\n"
        f"_Single-use · 24hr expiry_"
    )

    kb = [
        [InlineKeyboardButton("🔑 Generate More", callback_data="admin_genkey")],
        [InlineKeyboardButton("📋 View All Keys", callback_data="admin_viewkeys")],
        [InlineKeyboardButton("◀️ Back", callback_data="admin_menu")],
    ]
    await safe_edit(q, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


# ─── View Passkeys ──────────────────────────────────────────────────

async def admin_viewkeys(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not _admin_check(update.effective_user.id):
        await q.answer("❌ Admin only.", show_alert=True)
        return
    await q.answer()

    async with SessionLocal() as s:
        res = await s.execute(
            select(PassKey).order_by(PassKey.created_at.desc()).limit(20))
        keys = res.scalars().all()

    if not keys:
        text = (
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "📋 *Passkeys*\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "No passkeys generated yet."
        )
        kb = [[InlineKeyboardButton("🔑 Generate Keys", callback_data="admin_genkey")]]
    else:
        lines = [
            "━━━━━━━━━━━━━━━━━━━━━",
            "📋 *Recent Passkeys*",
            "━━━━━━━━━━━━━━━━━━━━━\n",
        ]
        now = datetime.utcnow()
        for pk in keys:
            if pk.is_used:
                status = "✅ Used"
            elif pk.expires_at < now:
                status = "⏰ Expired"
            else:
                status = "🟢 Active"
            created = pk.created_at.strftime("%b %d %H:%M") if pk.created_at else "?"
            lines.append(f"`{pk.key}` — {status} | {created}")
        text = "\n".join(lines)
        kb = []

    kb.append([InlineKeyboardButton("🔄 Refresh", callback_data="admin_viewkeys")])
    kb.append([InlineKeyboardButton("◀️ Back", callback_data="admin_menu")])
    await safe_edit(q, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


# ─── User List ──────────────────────────────────────────────────────

async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not _admin_check(update.effective_user.id):
        await q.answer("❌ Admin only.", show_alert=True)
        return
    await q.answer()

    async with SessionLocal() as s:
        res = await s.execute(select(User).order_by(User.id.desc()).limit(20))
        users = res.scalars().all()

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━",
        "👥 *Users*",
        "━━━━━━━━━━━━━━━━━━━━━\n",
    ]
    for u in users:
        prem = "⭐" if u.is_premium else ""
        name = f"@{u.username}" if u.username else f"ID:{u.tg_id}"
        lines.append(f"• *{name}* {prem}")
    text = "\n".join(lines)

    kb = [[InlineKeyboardButton("◀️ Back", callback_data="admin_menu")]]
    await safe_edit(q, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


# ─── Toggle Premium ─────────────────────────────────────────────────

async def admin_togglepremium_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not _admin_check(update.effective_user.id):
        await q.answer("❌ Admin only.", show_alert=True)
        return
    await q.answer()

    context.user_data["admin_action"] = "togglepremium"
    text = (
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⭐ *Toggle Premium*\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Send: `<user_tg_id>`\n"
        "_Toggles premium status on/off._"
    )
    kb = [[InlineKeyboardButton("❌ Cancel", callback_data="admin_menu")]]
    await safe_edit(q, text, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))


# ─── Admin Text Input Handler ───────────────────────────────────────

async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handle text input for admin actions. Returns True if consumed."""
    action = context.user_data.get("admin_action")
    if not action:
        return False

    user_id = update.effective_user.id
    if not _admin_check(user_id):
        context.user_data["admin_action"] = None
        return False

    raw = update.message.text.strip()
    context.user_data["admin_action"] = None

    if action == "togglepremium":
        try:
            tg_id = int(raw.strip())
        except ValueError:
            await update.message.reply_text("❌ Send a valid Telegram user ID.", parse_mode="Markdown")
            return True

        async with SessionLocal() as s:
            res = await s.execute(select(User).where(User.tg_id == tg_id))
            user = res.scalar_one_or_none()
            if not user:
                await update.message.reply_text(f"❌ User `{tg_id}` not found.", parse_mode="Markdown")
                return True
            user.is_premium = not user.is_premium
            if user.is_premium:
                user.premium_since = datetime.utcnow()
            await s.commit()

        status = "⭐ Premium ON" if user.is_premium else "⬜ Premium OFF"
        await update.message.reply_text(
            f"{status} for *{user.username or tg_id}*",
            parse_mode="Markdown")
        return True

    return False


# ─── Manual Smart Bet Trigger ────────────────────────────────────

async def admin_sendpicks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin command: manually trigger daily smart bet send to subscribers."""
    user_id = update.effective_user.id
    if not _admin_check(user_id):
        if update.callback_query:
            await update.callback_query.answer("❌ Admin only.", show_alert=True)
        else:
            await update.message.reply_text("❌ Admin only.")
        return

    msg = update.message or (update.callback_query.message if update.callback_query else None)
    if not msg:
        return

    status_msg = await msg.reply_text("🤖 *Generating Smart Bet pick...*", parse_mode="Markdown")

    try:
        from services.smart_bet import smart_bet_engine
        import json

        pick = await smart_bet_engine.generate_daily_pick()
        if not pick.get("has_pick"):
            await status_msg.edit_text(
                "❌ *No pick available today.*\n"
                "_Not enough high-confidence fixtures._",
                parse_mode="Markdown")
            return

        # Store in DB
        today = datetime.utcnow().strftime("%Y-%m-%d")
        async with SessionLocal() as s:
            existing = await s.execute(
                select(SmartBetPick).where(SmartBetPick.date == today))
            existing_pick = existing.scalar_one_or_none()

            selections_json = json.dumps([
                {
                    "home": p.get("home", ""),
                    "away": p.get("away", ""),
                    "label": p.get("label", ""),
                    "odds": p.get("odds", 0),
                    "market": p.get("market", ""),
                    "league": p.get("league", ""),
                    "fixture_id": p.get("fixture", {}).get("id", ""),
                    "confidence": p.get("confidence", 0),
                    "edge": p.get("edge", 0),
                    "probability": p.get("probability", 0),
                }
                for p in pick.get("selections", [])
            ])

            if existing_pick:
                existing_pick.selections_json = selections_json
                existing_pick.total_odds = pick.get("total_odds", 0)
                existing_pick.combined_prob = pick.get("combined_prob", 0)
                existing_pick.num_legs = pick.get("num_legs", 0)
                existing_pick.message = pick.get("message", "")
                existing_pick.result = "pending"
                existing_pick.won_count = 0
                existing_pick.notified = False
            else:
                new_pick = SmartBetPick(
                    date=today,
                    selections_json=selections_json,
                    total_odds=pick.get("total_odds", 0),
                    combined_prob=pick.get("combined_prob", 0),
                    num_legs=pick.get("num_legs", 0),
                    message=pick.get("message", ""),
                    result="pending",
                )
                s.add(new_pick)
            await s.commit()

        # Send to subscribers
        async with SessionLocal() as s:
            res = await s.execute(
                select(User.tg_id).where(User.smart_bet_subscribed == True))
            subscriber_ids = [row[0] for row in res.all()]

        sent = 0
        for uid in subscriber_ids:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=pick["message"],
                    parse_mode="Markdown")
                sent += 1
            except Exception:
                pass

        await status_msg.edit_text(
            f"✅ *Smart Bet sent!*\n\n"
            f"📤 Sent to *{sent}/{len(subscriber_ids)}* subscribers\n"
            f"🎯 *{pick['num_legs']}* legs @ ×{pick['total_odds']:.2f}\n"
            f"📊 Confidence: *{pick['confidence']:.0f}%*",
            parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Admin sendpicks error: {e}")
        await status_msg.edit_text(
            f"❌ *Error:* `{e}`",
            parse_mode="Markdown")


# ─── Bot Stats ──────────────────────────────────────────────────────

async def bot_stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show live bot usage stats."""
    stats = await get_bot_stats(online_minutes=5)

    text = (
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *Bot Stats*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Users: *{stats['total']}*\n"
        f"🟢 Online: *{stats['online']}*\n"
        f"📅 Active today: *{stats['today']}*"
    )

    kb = [
        [InlineKeyboardButton("🔄 Refresh", callback_data="menu_botstats")],
        [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
    ]
    markup = InlineKeyboardMarkup(kb)

    if update.callback_query:
        await safe_edit(update.callback_query,
            text, parse_mode="Markdown", reply_markup=markup)
    else:
        await update.message.reply_text(
            text, parse_mode="Markdown", reply_markup=markup)

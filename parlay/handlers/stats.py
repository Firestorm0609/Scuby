"""
Stats handlers — user-facing bot stats.
"""
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from database.db import SessionLocal, User, get_bot_stats, SmartBetStreak, UltraPick
from utils.helpers import safe_edit
from sqlalchemy import select, func, Integer


async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message or update.callback_query.message

    async with SessionLocal() as s:
        res = await s.execute(
            select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()

    if not user:
        kb = [[InlineKeyboardButton("🏠 Back", callback_data="menu_main")]]
        await msg.reply_text("⚠️ Please /start first.", reply_markup=InlineKeyboardMarkup(kb))
        return

    stats = await get_bot_stats(online_minutes=5)

    prem = "⭐ Premium" if user.is_premium else "Free"
    tz_offset = getattr(user, "tz_offset", 0)
    sign = "+" if tz_offset >= 0 else ""

    text = (
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *Bot Stats*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👥 Total Users: `{stats['total']}`\n"
        f"🟢 Online Now: `{stats['online']}`\n"
        f"📅 Active Today: `{stats['today']}`\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 *Your Profile*\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Name: *{user.username or 'N/A'}*\n"
        f"Status: *{prem}*\n"
        f"Timezone: *UTC{sign}{tz_offset}*"
    )

    kb = [
        [InlineKeyboardButton("🏠 Back", callback_data="menu_main")],
    ]
    markup = InlineKeyboardMarkup(kb)
    if update.callback_query:
        await safe_edit(update.callback_query, text, parse_mode="Markdown", reply_markup=markup)
    else:
        await msg.reply_text(text, parse_mode="Markdown", reply_markup=markup)


async def leaderboard_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Smart Bet streak leaderboard + Ultra Mode stats."""
    async with SessionLocal() as s:
        # Top by streak
        streak_res = await s.execute(
            select(SmartBetStreak)
            .where(SmartBetStreak.total_days >= 1)
            .order_by(SmartBetStreak.streak.desc(), SmartBetStreak.won_days.desc())
            .limit(10))
        streaks = streak_res.scalars().all()

        # Top by win rate (min 3 days)
        rate_res = await s.execute(
            select(SmartBetStreak)
            .where(SmartBetStreak.total_days >= 3)
            .order_by(
                (SmartBetStreak.won_days / SmartBetStreak.total_days).desc(),
                SmartBetStreak.total_days.desc())
            .limit(10))
        rates = rate_res.scalars().all()

        # Total subscriber count
        sub_count = (await s.execute(
            select(func.count()).select_from(User).where(
                User.smart_bet_subscribed == True))).scalar() or 0

        # Ultra Mode stats
        ultra_users = (await s.execute(
            select(func.count()).select_from(User).where(
                User.ultra_mode_subscribed == True))).scalar() or 0

        # Top ultra chains (longest chain number with wins)
        ultra_stats_res = await s.execute(
            select(
                UltraPick.user_id,
                func.max(UltraPick.chain_number).label("max_chain"),
                func.count().label("total_picks"),
                func.sum(func.cast(UltraPick.status == "won", Integer)).label("wins")
            )
            .where(UltraPick.status.in_(["won", "lost", "partial"]))
            .group_by(UltraPick.user_id)
            .order_by(func.max(UltraPick.chain_number).desc())
            .limit(10))
        ultra_stats = ultra_stats_res.all()

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━",
        "🏆 *Smart Bet Leaderboard*",
        "━━━━━━━━━━━━━━━━━━━━━\n",
        f"📊 *{sub_count}* daily subscribers | ⚡ *{ultra_users}* ultra users\n",
    ]

    # Streak leaderboard
    if streaks:
        lines.append("🔥 *Current Streaks*")
        medals = ["🥇", "🥈", "🥉"]
        for i, st in enumerate(streaks):
            prefix = medals[i] if i < 3 else f"{i+1}."
            name = f"User {st.user_id}"
            user_res = await s.execute(
                select(User.username).where(User.tg_id == st.user_id))
            username = user_res.scalar_one_or_none()
            if username:
                name = f"@{username}"
            win_rate = (st.won_days / st.total_days * 100) if st.total_days > 0 else 0
            lines.append(
                f"{prefix} *{name}* — 🔥 {st.streak} streak "
                f"({win_rate:.0f}% win rate)"
            )
        lines.append("")
    else:
        lines.append("_No streak data yet. Subscribe to Smart Bet to start!_")
        lines.append("")

    # Win rate leaderboard
    if rates:
        lines.append("📈 *Best Win Rate (min 3 days)*")
        medals = ["🥇", "🥈", "🥉"]
        for i, st in enumerate(rates):
            prefix = medals[i] if i < 3 else f"{i+1}."
            name = f"User {st.user_id}"
            user_res = await s.execute(
                select(User.username).where(User.tg_id == st.user_id))
            username = user_res.scalar_one_or_none()
            if username:
                name = f"@{username}"
            win_rate = (st.won_days / st.total_days * 100) if st.total_days > 0 else 0
            lines.append(
                f"{prefix} *{name}* — {win_rate:.0f}% "
                f"({st.won_days}/{st.total_days} days)"
            )
        lines.append("")

    # Ultra Mode leaderboard
    if ultra_stats:
        lines.append("⚡ *Ultra Mode — Best Chains*")
        medals = ["🥇", "🥈", "🥉"]
        for i, row in enumerate(ultra_stats):
            prefix = medals[i] if i < 3 else f"{i+1}."
            uid, max_chain, total, wins = row
            name = f"User {uid}"
            user_res = await s.execute(
                select(User.username).where(User.tg_id == uid))
            username = user_res.scalar_one_or_none()
            if username:
                name = f"@{username}"
            wins = wins or 0
            win_pct = (wins / total * 100) if total > 0 else 0
            lines.append(
                f"{prefix} *{name}* — ⚡ Chain #{max_chain} "
                f"({wins}/{total} wins, {win_pct:.0f}% win rate)"
            )
    else:
        lines.append("⚡ *Ultra Mode*_ — No chains completed yet")

    text = "\n".join(lines)
    kb = [[InlineKeyboardButton("🏠 Back", callback_data="menu_main")]]
    markup = InlineKeyboardMarkup(kb)
    if update.callback_query:
        await safe_edit(update.callback_query, text, parse_mode="Markdown", reply_markup=markup)
    else:
        await update.message.reply_text(text, parse_mode="Markdown", reply_markup=markup)

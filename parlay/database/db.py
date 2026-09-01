from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Float, Integer, DateTime, text, func, select
from datetime import datetime, timedelta
from config import DATABASE_URL

import logging


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(primary_key=True)
    tg_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    username: Mapped[str] = mapped_column(String(64), nullable=True)
    preferred_sports: Mapped[str] = mapped_column(String(255), default="all")
    market_prefs: Mapped[str] = mapped_column(String(500), default="all")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_seen: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=True)
    terms_accepted: Mapped[bool] = mapped_column(default=False)
    tz_offset: Mapped[int] = mapped_column(Integer, default=0)
    is_premium: Mapped[bool] = mapped_column(default=False)
    premium_since: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    smart_bet_subscribed: Mapped[bool] = mapped_column(default=False)
    smart_bet_subscribed_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    ultra_mode_subscribed: Mapped[bool] = mapped_column(default=False)
    ultra_mode_subscribed_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)


class PassKey(Base):
    __tablename__ = "passkeys"
    id: Mapped[int] = mapped_column(primary_key=True)
    key: Mapped[str] = mapped_column(String(16), unique=True, index=True)
    created_by: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    expires_at: Mapped[datetime] = mapped_column(DateTime)
    is_used: Mapped[bool] = mapped_column(default=False)
    used_by: Mapped[int] = mapped_column(Integer, nullable=True)
    used_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)
    label: Mapped[str] = mapped_column(String(64), nullable=True)


class SmartBetPick(Base):
    """Stores the single daily smart bet pick for all subscribers."""
    __tablename__ = "smart_bet_picks"
    id: Mapped[int] = mapped_column(primary_key=True)
    date: Mapped[str] = mapped_column(String(10), unique=True, index=True)  # YYYY-MM-DD
    selections_json: Mapped[str] = mapped_column(String(8000))  # JSON serialized picks
    total_odds: Mapped[float] = mapped_column(Float, default=0.0)
    combined_prob: Mapped[float] = mapped_column(Float, default=0.0)
    num_legs: Mapped[int] = mapped_column(Integer, default=0)
    message: Mapped[str] = mapped_column(String(4000), default="")
    result: Mapped[str] = mapped_column(String(20), default="pending")  # pending/won/lost/partial
    won_count: Mapped[int] = mapped_column(Integer, default=0)
    notified: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class SmartBetStreak(Base):
    """Persistent streak tracking for smart bet subscribers."""
    __tablename__ = "smart_bet_streaks"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)  # tg_id
    streak: Mapped[int] = mapped_column(Integer, default=0)
    total_days: Mapped[int] = mapped_column(Integer, default=0)
    won_days: Mapped[int] = mapped_column(Integer, default=0)
    last_result: Mapped[str] = mapped_column(String(20), default="")  # won/lost/partial
    last_updated: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class UltraPick(Base):
    """Tracks the continuous chain of ultra mode picks."""
    __tablename__ = "ultra_picks"
    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, index=True)  # tg_id
    selections_json: Mapped[str] = mapped_column(String(8000))
    total_odds: Mapped[float] = mapped_column(Float, default=0.0)
    combined_prob: Mapped[float] = mapped_column(Float, default=0.0)
    num_legs: Mapped[int] = mapped_column(Integer, default=0)
    message: Mapped[str] = mapped_column(String(4000), default="")
    chain_number: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(20), default="active")  # active/won/lost/partial
    won_count: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    settled_at: Mapped[datetime] = mapped_column(DateTime, nullable=True)


# Async engine and session factory
engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def init_db():
    """Initialize database schema."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # Add missing columns to existing tables (SQLite does not support ADD COLUMN IF NOT EXISTS)
        _safe_alter = [
            "ALTER TABLE users ADD COLUMN smart_bet_subscribed BOOLEAN DEFAULT 0",
            "ALTER TABLE users ADD COLUMN smart_bet_subscribed_at DATETIME",
            "ALTER TABLE users ADD COLUMN ultra_mode_subscribed BOOLEAN DEFAULT 0",
            "ALTER TABLE users ADD COLUMN ultra_mode_subscribed_at DATETIME",
        ]
        for sql in _safe_alter:
            try:
                await conn.execute(text(sql))
            except Exception:
                pass  # Column already exists


async def get_or_create_user(tg_id: int, username: str = None) -> User:
    async with SessionLocal() as s:
        result = await s.execute(select(User).where(User.tg_id == tg_id))
        user = result.scalar_one_or_none()
        if not user:
            user = User(tg_id=tg_id, username=username, last_seen=datetime.utcnow(), is_premium=True)
            s.add(user)
            await s.commit()
            await s.refresh(user)
        return user


async def touch_user(tg_id: int) -> None:
    """Update last_seen timestamp for presence tracking."""
    try:
        async with SessionLocal() as s:
            result = await s.execute(select(User).where(User.tg_id == tg_id))
            user = result.scalar_one_or_none()
            if user:
                user.last_seen = datetime.utcnow()
                await s.commit()
    except Exception:
        pass


async def get_bot_stats(online_minutes: int = 5) -> dict:
    """Return total user count, online count, and today's active count."""
    async with SessionLocal() as s:
        total_res = await s.execute(select(func.count()).select_from(User))
        total = total_res.scalar() or 0

        cutoff_online = datetime.utcnow() - timedelta(minutes=online_minutes)
        online_res = await s.execute(
            select(func.count()).select_from(User).where(User.last_seen >= cutoff_online))
        online = online_res.scalar() or 0

        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_res = await s.execute(
            select(func.count()).select_from(User).where(User.last_seen >= today_start))
        today = today_res.scalar() or 0

    return {"total": total, "online": online, "today": today, "online_minutes": online_minutes}

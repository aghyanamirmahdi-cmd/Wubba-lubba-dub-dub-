#!/usr/bin/env python3
"""
Study Coach - Adaptive Study Planner Telegram Bot
Full-featured single-file implementation.
Base code audited, bug-fixed, and upgraded from an earlier prototype.
"""

# ============================================================
# IMPORTS
# ============================================================
import asyncio
import json
import logging
import os
import signal
import sys
import random
import re
import time
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, List, Dict, Any, Union, Tuple, Callable, Awaitable
from contextlib import asynccontextmanager
from enum import Enum
from dataclasses import dataclass, field

import pytz
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.base import JobLookupError
# NOTE: The DB layer used to be SQLAlchemy+aiosqlite here. It has been
# replaced with json_orm, a small dependency-free JSON-backed storage
# engine (see json_orm.py) that eliminates "database is locked" errors
# entirely: everything is in-process Python objects guarded by a single
# asyncio.Lock and flushed to disk atomically, so there's no OS-level file
# lock left to contend over. Job durability is unaffected: every scheduled
# job is still written to the `scheduler_jobs` table (see SchedulerService),
# and `recover_from_crash()` still replays that table on startup.
# APScheduler itself is left on its default in-memory jobstore, same as
# before.

from aiogram import Bot, Dispatcher, Router, F, BaseMiddleware
from aiogram.types import (
    Message, CallbackQuery, Update, InlineKeyboardMarkup, InlineKeyboardButton,
    WebhookInfo, ChatMemberUpdated, ChatMember, User as TelegramUser
)
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import FSInputFile

from json_orm import (
    Column, Integer, String, Boolean, DateTime, Float, Text,
    ForeignKey, UniqueConstraint, Index, JSON, select, delete, desc, func,
    declarative_base, IntegrityError, AsyncSession, JsonStore, JsonSession,
    resolve_json_path, _set_store, get_store,
)

import aiohttp
import aiohttp.web as web

load_dotenv()

# ============================================================
# CONFIG
# ============================================================
class Config:
    BOT_TOKEN: str = os.getenv("BOT_TOKEN", "")
    ADMIN_IDS: List[int] = [
        int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()
    ]
    BOT_MODE: str = os.getenv("BOT_MODE", "polling")  # polling | webhook
    TIMEZONE: str = os.getenv("TIMEZONE", "Asia/Tehran")
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///data/study_coach.db")
    WEBHOOK_URL: Optional[str] = os.getenv("WEBHOOK_URL")
    WEBHOOK_PATH: str = os.getenv("WEBHOOK_PATH", "/webhook")
    WEBHOOK_SECRET: Optional[str] = os.getenv("WEBHOOK_SECRET")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    BACKUP_DIR: str = os.getenv("BACKUP_DIR", "./backups")
    # Default backup interval in hours. Overridable at runtime by admins
    # from the admin panel (persisted in the bot_settings table).
    BACKUP_INTERVAL_HOURS: int = int(os.getenv("BACKUP_INTERVAL_HOURS", "24"))
    BACKUP_KEEP_LAST: int = int(os.getenv("BACKUP_KEEP_LAST", "14"))
    SECRET_KEY: str = os.getenv("SECRET_KEY", "change-me-in-production")
    AI_PROVIDER: str = os.getenv("AI_PROVIDER", "")  # optional
    AI_API_KEY: str = os.getenv("AI_API_KEY", "")
    PORT: int = int(os.getenv("PORT", "8080"))

    # Algorithm defaults
    DEFAULT_MIN_DURATION: int = 10
    DEFAULT_MAX_DURATION: int = 120
    DEFAULT_ATTENDANCE_WINDOW: int = 180  # seconds
    DEFAULT_GROWTH_STEP: int = 5
    DEFAULT_RECOVERY_STEP: int = 5
    DEFAULT_NOTIFICATION_LIMIT: int = 5

    # Day periods (hour ranges)
    DAY_PERIODS: Dict[str, tuple] = {
        "EARLY_MORNING": (5, 8),
        "MORNING": (8, 12),
        "NOON": (12, 14),
        "AFTERNOON": (14, 17),
        "EVENING": (17, 21),
        "NIGHT": (21, 0),
        "LATE_NIGHT": (0, 5),
    }

    # Candidate durations (minutes)
    CANDIDATE_DURATIONS: List[int] = [
        10, 15, 20, 25, 30, 35, 40, 45, 50, 60, 75, 90, 105, 120
    ]

    # Algorithm weights (configurable via admin)
    ALGORITHM_WEIGHTS: Dict[str, float] = {
        "RECENT_PERFORMANCE_WEIGHT": 0.30,
        "HISTORICAL_PERFORMANCE_WEIGHT": 0.15,
        "TIME_FIT_WEIGHT": 0.15,
        "CONSISTENCY_WEIGHT": 0.10,
        "INTENT_WEIGHT": 0.10,
        "LOAD_WEIGHT": 0.08,
        "RECOVERY_WEIGHT": 0.07,
        "GROUP_FIT_WEIGHT": 0.03,
        "PERSONAL_FIT_WEIGHT": 0.02,
        "OVERLOAD_PENALTY": 0.20,
        "FAILURE_PENALTY": 0.15,
    }

    @classmethod
    def validate(cls) -> bool:
        if not cls.BOT_TOKEN:
            raise ValueError("BOT_TOKEN is required")
        if cls.BOT_MODE == "webhook" and not cls.WEBHOOK_URL:
            raise ValueError("WEBHOOK_URL required for webhook mode")
        return True


config = Config()

# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("study_coach")

# ============================================================
# TIME SERVICE
# ============================================================
class TimeService:
    def __init__(self, tz_str: str = "Asia/Tehran"):
        self.tz = pytz.timezone(tz_str)

    def now(self) -> datetime:
        return datetime.now(self.tz)

    def today(self) -> date:
        return self.now().date()

    def current_time(self) -> time:
        return self.now().time()

    def hour(self) -> int:
        return self.now().hour

    def minute(self) -> int:
        return self.now().minute

    def weekday(self) -> int:
        return self.now().weekday()

    def day_period(self) -> str:
        h = self.hour()
        for name, (start, end) in config.DAY_PERIODS.items():
            if start <= end:
                if start <= h < end:
                    return name
            else:
                if h >= start or h < end:
                    return name
        return "NIGHT"

    def is_late_night(self) -> bool:
        return self.hour() >= 22 or self.hour() < 5

    def is_evening(self) -> bool:
        return 17 <= self.hour() < 21

    def is_morning(self) -> bool:
        return 5 <= self.hour() < 12

    def start_of_day(self, dt: Optional[datetime] = None) -> datetime:
        if dt is None:
            dt = self.now()
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)

    def end_of_day(self, dt: Optional[datetime] = None) -> datetime:
        if dt is None:
            dt = self.now()
        return dt.replace(hour=23, minute=59, second=59, microsecond=999999)

    def days_ago(self, days: int) -> datetime:
        return self.now() - timedelta(days=days)

    def format_datetime(self, dt: datetime, fmt: str = "%Y-%m-%d %H:%M") -> str:
        return dt.strftime(fmt)

    def to_tehran(self, dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = pytz.UTC.localize(dt)
        return dt.astimezone(self.tz)

    def parse_datetime(self, dt_str: str) -> datetime:
        try:
            dt = datetime.fromisoformat(dt_str)
            if dt.tzinfo is None:
                dt = self.tz.localize(dt)
            return dt
        except ValueError:
            raise ValueError(f"Invalid datetime: {dt_str}")

    def seconds_until(self, target_hour: int, target_minute: int = 0) -> int:
        now = self.now()
        target = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return int((target - now).total_seconds())


time_service = TimeService()

# ============================================================
# DATABASE SETUP (JSON-backed, see json_orm.py)
# ============================================================
Base = declarative_base()

_LOCK_TIMEOUT_SECONDS = 30  # mirrors the old SQLite busy_timeout


async def init_db():
    """Loads the JSON store into memory. Idempotent: safe to call more
    than once (get_session() calls this lazily if needed)."""
    json_path = resolve_json_path(config.DATABASE_URL)
    parent = Path(json_path).parent
    if str(parent) not in ("", "."):
        parent.mkdir(parents=True, exist_ok=True)
    store = JsonStore(json_path)
    store.load()
    _set_store(store)
    logger.info(f"Database initialized (JSON store at {json_path})")


async def close_db():
    try:
        store = get_store()
    except RuntimeError:
        return
    await store.save()
    logger.info("Database closed (final snapshot saved)")


@asynccontextmanager
async def get_session():
    """Mirrors the original get_session() contract exactly (commit on
    clean exit, rollback+reraise on exception) but is now backed by
    json_orm.JsonStore instead of SQLAlchemy+SQLite.

    A single global lock (store.lock) serializes every transaction. This
    is what makes "database is locked" structurally impossible: there is
    no second process and no filesystem lock to contend over, just one
    asyncio.Lock that queues concurrent callers instead of erroring.

    Nested reuse must still go through the same discipline the original
    code already followed everywhere (e.g. SchedulerService.schedule_job's
    db_session= parameter): pass the existing `session` object into nested
    calls rather than opening a second `get_session()` block while the
    first is still open, or the lock acquisition below will time out.
    """
    try:
        store = get_store()
    except RuntimeError:
        await init_db()
        store = get_store()
    try:
        await asyncio.wait_for(store.lock.acquire(), timeout=_LOCK_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        raise RuntimeError(
            "Timed out waiting for the database lock -- almost certainly a "
            "nested get_session() call while an outer one is still open. "
            "Pass the existing session into nested calls instead of opening "
            "a new one (see SchedulerService.schedule_job's db_session= param)."
        )
    session = JsonSession(store)
    try:
        yield session
        await session.commit()
        await store.save()
    except Exception:
        await session.rollback()
        raise
    finally:
        store.lock.release()


# ============================================================
# DATABASE MODELS
# ============================================================
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    telegram_id = Column(Integer, unique=True, nullable=False, index=True)
    username = Column(String(64), nullable=True)
    first_name = Column(String(64), nullable=True)
    last_name = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_seen_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    is_admin = Column(Boolean, default=False, nullable=False)


class Group(Base):
    __tablename__ = "groups"
    id = Column(Integer, primary_key=True, index=True)
    telegram_chat_id = Column(Integer, unique=True, nullable=False, index=True)
    title = Column(String(128), nullable=True)
    timezone = Column(String(32), default="Asia/Tehran")
    active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class GroupMember(Base):
    __tablename__ = "group_members"
    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    group_id = Column(Integer, ForeignKey("groups.id"), primary_key=True)
    joined_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    active = Column(Boolean, default=True, nullable=False)
    role = Column(String(20), default="MEMBER", nullable=False)
    __table_args__ = (
        UniqueConstraint("user_id", "group_id", name="uq_user_group"),
        Index("idx_group_members_group_id", "group_id"),
        Index("idx_group_members_user_id", "user_id"),
    )


class UserPreference(Base):
    __tablename__ = "user_preferences"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    preferred_session_duration = Column(Integer, default=45, nullable=False)
    minimum_session_duration = Column(Integer, default=10, nullable=False)
    maximum_session_duration = Column(Integer, default=120, nullable=False)
    daily_goal = Column(Integer, default=90, nullable=False)
    morning_notifications = Column(Boolean, default=True, nullable=False)
    session_notifications = Column(Boolean, default=True, nullable=False)
    private_interventions = Column(Boolean, default=True, nullable=False)
    weekly_reports = Column(Boolean, default=True, nullable=False)
    daily_reports = Column(Boolean, default=True, nullable=False)
    motivational_messages = Column(Boolean, default=True, nullable=False)
    preferred_study_hours = Column(JSON, default=list, nullable=True)


class UserGoal(Base):
    __tablename__ = "user_goals"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    daily_goal = Column(Integer, default=90, nullable=False)
    weekly_goal = Column(Integer, default=450, nullable=False)
    monthly_goal = Column(Integer, default=1800, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class UserBehaviorProfile(Base):
    __tablename__ = "user_behavior_profiles"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    # Capacity metrics
    current_capacity = Column(Float, default=35.0, nullable=False)
    comfort_capacity = Column(Float, default=30.0, nullable=False)
    challenge_capacity = Column(Float, default=45.0, nullable=False)
    minimum_viable_session = Column(Integer, default=15, nullable=False)
    maximum_observed_capacity = Column(Integer, default=45, nullable=False)
    # Performance
    average_session = Column(Float, default=30.0, nullable=False)
    recent_average = Column(Float, default=30.0, nullable=False)
    completion_rate = Column(Float, default=0.7, nullable=False)
    attendance_rate = Column(Float, default=0.7, nullable=False)
    consistency_score = Column(Float, default=0.5, nullable=False)
    momentum_score = Column(Float, default=0.5, nullable=False)
    recovery_score = Column(Float, default=0.5, nullable=False)
    starting_friction_score = Column(Float, default=0.3, nullable=False)
    # Tolerance
    long_session_tolerance = Column(Float, default=0.5, nullable=False)
    short_session_response = Column(Float, default=0.5, nullable=False)
    # Time preferences
    preferred_hour = Column(Integer, default=18, nullable=False)
    preferred_day = Column(Integer, default=2, nullable=False)  # 0=Mon
    time_of_day_performance = Column(JSON, default=dict, nullable=True)
    # State
    current_load = Column(Float, default=0.0, nullable=False)
    success_streak = Column(Integer, default=0, nullable=False)
    miss_streak = Column(Integer, default=0, nullable=False)
    trend = Column(String(20), default="STABLE", nullable=False)
    last_updated = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class Session(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("groups.id"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    scheduled_start = Column(DateTime, nullable=False)
    actual_start = Column(DateTime, nullable=True)
    scheduled_end = Column(DateTime, nullable=False)
    actual_end = Column(DateTime, nullable=True)
    planned_duration = Column(Integer, nullable=False, default=45)
    minimum_duration = Column(Integer, nullable=False, default=15)
    target_duration = Column(Integer, nullable=False, default=45)
    extension_duration = Column(Integer, nullable=False, default=60)
    status = Column(String(20), default="SCHEDULED", nullable=False)
    mode = Column(String(20), default="normal", nullable=False)
    creation_source = Column(String(32), default="scheduled", nullable=False)
    recommendation_confidence = Column(Float, default=0.5, nullable=False)
    reason_codes = Column(JSON, default=list, nullable=True)
    __table_args__ = (
        Index("idx_sessions_group_id", "group_id"),
        Index("idx_sessions_status", "status"),
        Index("idx_sessions_scheduled_start", "scheduled_start"),
    )


class SessionParticipant(Base):
    __tablename__ = "session_participants"
    session_id = Column(Integer, ForeignKey("sessions.id"), primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    joined_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    attendance_status = Column(String(20), default="NOT_READY", nullable=False)
    ready_at = Column(DateTime, nullable=True)
    __table_args__ = (
        UniqueConstraint("session_id", "user_id", name="uq_session_participant"),
        Index("idx_session_participants_session_id", "session_id"),
        Index("idx_session_participants_user_id", "user_id"),
    )


class SessionResult(Base):
    __tablename__ = "session_results"
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey("sessions.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    planned_duration = Column(Integer, nullable=False)
    actual_duration = Column(Integer, nullable=True)
    completion_status = Column(String(20), nullable=False, default="MISSED")
    reported_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    honesty_signal = Column(Float, default=1.0, nullable=False)
    __table_args__ = (
        UniqueConstraint("session_id", "user_id", name="uq_session_result"),
        Index("idx_session_results_session_id", "session_id"),
        Index("idx_session_results_user_id", "user_id"),
    )


class BehaviorEvent(Base):
    __tablename__ = "behavior_events"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    event_type = Column(String(32), nullable=False)
    # NOTE: named event_meta, not "metadata" -- "metadata" is reserved by
    # SQLAlchemy's declarative Base (it's the MetaData instance) and using it
    # as a column attribute raises InvalidRequestError at class-definition time.
    event_meta = Column("metadata", JSON, default=dict, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    __table_args__ = (
        Index("idx_behavior_events_user_id", "user_id"),
        Index("idx_behavior_events_created_at", "created_at"),
        Index("idx_behavior_events_event_type", "event_type"),
    )


class DailyStatistics(Base):
    __tablename__ = "daily_statistics"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    date = Column(DateTime, nullable=False)
    sessions = Column(Integer, default=0, nullable=False)
    minutes = Column(Integer, default=0, nullable=False)
    completed = Column(Integer, default=0, nullable=False)
    missed = Column(Integer, default=0, nullable=False)
    avg_duration = Column(Float, default=0.0, nullable=False)
    completion_rate = Column(Float, default=0.0, nullable=False)
    __table_args__ = (
        UniqueConstraint("user_id", "date", name="uq_daily_stats"),
        Index("idx_daily_statistics_user_id", "user_id"),
        Index("idx_daily_statistics_date", "date"),
    )


class WeeklyStatistics(Base):
    __tablename__ = "weekly_statistics"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    week_start = Column(DateTime, nullable=False)
    sessions = Column(Integer, default=0, nullable=False)
    minutes = Column(Integer, default=0, nullable=False)
    completed = Column(Integer, default=0, nullable=False)
    missed = Column(Integer, default=0, nullable=False)
    avg_duration = Column(Float, default=0.0, nullable=False)
    completion_rate = Column(Float, default=0.0, nullable=False)
    consistency = Column(Float, default=0.0, nullable=False)
    best_day = Column(Integer, default=0, nullable=False)
    best_hour = Column(Integer, default=0, nullable=False)
    trend = Column(String(20), default="STABLE", nullable=False)
    __table_args__ = (
        UniqueConstraint("user_id", "week_start", name="uq_weekly_stats"),
        Index("idx_weekly_statistics_user_id", "user_id"),
        Index("idx_weekly_statistics_week_start", "week_start"),
    )


class MessageTemplate(Base):
    __tablename__ = "messages"
    id = Column(Integer, primary_key=True, index=True)
    category = Column(String(32), nullable=False)
    subcategory = Column(String(32), nullable=True)
    text = Column(Text, nullable=False)
    active = Column(Boolean, default=True, nullable=False)
    weight = Column(Float, default=1.0, nullable=False)
    usage_count = Column(Integer, default=0, nullable=False)
    last_used_at = Column(DateTime, nullable=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    __table_args__ = (
        Index("idx_messages_category", "category"),
        Index("idx_messages_active", "active"),
    )


class GroupSetting(Base):
    __tablename__ = "group_settings"
    group_id = Column(Integer, ForeignKey("groups.id"), primary_key=True)
    timezone = Column(String(32), default="Asia/Tehran", nullable=False)
    default_duration = Column(Integer, default=45, nullable=False)
    attendance_window = Column(Integer, default=180, nullable=False)
    auto_start = Column(Boolean, default=False, nullable=False)
    morning_default = Column(Integer, default=30, nullable=False)
    evening_default = Column(Integer, default=40, nullable=False)
    late_night_default = Column(Integer, default=15, nullable=False)
    min_session = Column(Integer, default=10, nullable=False)
    max_session = Column(Integer, default=120, nullable=False)
    growth_step = Column(Integer, default=5, nullable=False)
    recovery_step = Column(Integer, default=5, nullable=False)
    notification_limit = Column(Integer, default=5, nullable=False)
    # --- Automatic readiness-poll settings (see _create_readiness_poll) ---
    auto_poll_enabled = Column(Boolean, default=True, nullable=False)
    poll_window_minutes = Column(Integer, default=4, nullable=False)  # 3-5 min per user's request
    inter_session_gap_minutes = Column(Integer, default=90, nullable=False)
    quiet_hour_start = Column(Integer, default=23, nullable=False)  # no polling from this hour...
    quiet_hour_end = Column(Integer, default=8, nullable=False)     # ...until this hour
    awaiting_activity = Column(Boolean, default=False, nullable=False)  # last poll got 0 "بله"
    last_poll_at = Column(DateTime, nullable=True)


class AlgorithmSetting(Base):
    __tablename__ = "algorithm_settings"
    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(64), unique=True, nullable=False)
    value = Column(Text, nullable=False)
    description = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class NotificationSetting(Base):
    __tablename__ = "notification_settings"
    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    morning_time = Column(String(5), default="08:00", nullable=False)
    morning_enabled = Column(Boolean, default=True, nullable=False)
    reminder_limit = Column(Integer, default=3, nullable=False)
    private_enabled = Column(Boolean, default=True, nullable=False)


class SystemEvent(Base):
    __tablename__ = "system_events"
    id = Column(Integer, primary_key=True, index=True)
    event_type = Column(String(32), nullable=False)
    severity = Column(String(20), default="INFO", nullable=False)
    message = Column(Text, nullable=False)
    event_meta = Column("metadata", JSON, default=dict, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    __table_args__ = (
        Index("idx_system_events_created_at", "created_at"),
        Index("idx_system_events_severity", "severity"),
    )


class SchedulerJob(Base):
    __tablename__ = "scheduler_jobs"
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String(64), unique=True, nullable=False)
    job_type = Column(String(32), nullable=False)
    run_at = Column(DateTime, nullable=False)
    status = Column(String(20), default="pending", nullable=False)
    job_meta = Column("metadata", JSON, default=dict, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    __table_args__ = (
        Index("idx_scheduler_jobs_run_at", "run_at"),
        Index("idx_scheduler_jobs_status", "status"),
    )


class BotSetting(Base):
    """Generic persisted key/value settings (e.g. backup_interval_hours)
    that admins can change at runtime from the admin panel without
    restarting the process or editing environment variables."""
    __tablename__ = "bot_settings"
    key = Column(String(64), primary_key=True)
    value = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


async def get_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    async with get_session() as session:
        result = await session.execute(select(BotSetting).where(BotSetting.key == key))
        row = result.scalar_one_or_none()
        return row.value if row else default


async def set_setting(key: str, value: str) -> None:
    async with get_session() as session:
        result = await session.execute(select(BotSetting).where(BotSetting.key == key))
        row = result.scalar_one_or_none()
        if row:
            row.value = value
        else:
            session.add(BotSetting(key=key, value=value))


async def get_backup_interval_hours() -> int:
    raw = await get_setting("backup_interval_hours")
    if raw is None:
        return config.BACKUP_INTERVAL_HOURS
    try:
        hours = int(raw)
        return hours if hours > 0 else config.BACKUP_INTERVAL_HOURS
    except (TypeError, ValueError):
        return config.BACKUP_INTERVAL_HOURS


class AdminUser(Base):
    __tablename__ = "admin_users"
    user_id = Column(Integer, ForeignKey("users.id"), primary_key=True)
    role = Column(String(20), default="admin", nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


# ============================================================
# REPOSITORIES (Complete)
# ============================================================
class UserRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_telegram_id(self, telegram_id: int) -> Optional[User]:
        result = await self.session.execute(select(User).where(User.telegram_id == telegram_id))
        return result.scalar_one_or_none()

    async def get_by_id(self, user_id: int) -> Optional[User]:
        result = await self.session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()

    async def create(self, telegram_id: int, username: Optional[str] = None,
                     first_name: Optional[str] = None, last_name: Optional[str] = None) -> User:
        user = User(
            telegram_id=telegram_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
        )
        self.session.add(user)
        await self.session.flush()
        # Create associated records
        prefs = UserPreference(user_id=user.id)
        goals = UserGoal(user_id=user.id)
        profile = UserBehaviorProfile(user_id=user.id)
        notif = NotificationSetting(user_id=user.id)
        self.session.add_all([prefs, goals, profile, notif])
        await self.session.flush()
        return user

    async def get_or_create(self, telegram_id: int, **kwargs) -> User:
        user = await self.get_by_telegram_id(telegram_id)
        if not user:
            user = await self.create(telegram_id, **kwargs)
        else:
            user.last_seen_at = datetime.utcnow()
            await self.session.flush()
        return user

    async def update_last_seen(self, telegram_id: int):
        user = await self.get_by_telegram_id(telegram_id)
        if user:
            user.last_seen_at = datetime.utcnow()
            await self.session.flush()

    async def get_all_active(self) -> List[User]:
        result = await self.session.execute(select(User).where(User.is_active == True))
        return result.scalars().all()

    async def get_admins(self) -> List[User]:
        result = await self.session.execute(select(User).where(User.is_admin == True))
        return result.scalars().all()


class GroupRepository:
    """Handles Group / GroupMember persistence.

    This was entirely missing from the base prototype: there was no code
    path that ever created a Group or GroupMember row, so "add the bot to a
    group" never actually resulted in any group membership. This repository
    plus the my_chat_member / group-message handlers below close that gap.
    """
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_chat_id(self, telegram_chat_id: int) -> Optional[Group]:
        result = await self.session.execute(
            select(Group).where(Group.telegram_chat_id == telegram_chat_id)
        )
        return result.scalar_one_or_none()

    async def get_or_create_group(self, telegram_chat_id: int, title: Optional[str] = None) -> Group:
        group = await self.get_by_chat_id(telegram_chat_id)
        if not group:
            group = Group(telegram_chat_id=telegram_chat_id, title=title)
            self.session.add(group)
            await self.session.flush()
            # Every group gets a settings row up front so admin screens never
            # have to special-case "no settings yet".
            settings = GroupSetting(group_id=group.id)
            self.session.add(settings)
            await self.session.flush()
        elif title and group.title != title:
            group.title = title
            await self.session.flush()
        return group

    async def get_membership(self, user_id: int, group_id: int) -> Optional[GroupMember]:
        result = await self.session.execute(
            select(GroupMember).where(
                GroupMember.user_id == user_id, GroupMember.group_id == group_id
            )
        )
        return result.scalar_one_or_none()

    async def add_member(self, user_id: int, group_id: int, role: str = "MEMBER") -> GroupMember:
        existing = await self.get_membership(user_id, group_id)
        if existing:
            if not existing.active:
                existing.active = True
                await self.session.flush()
            return existing
        member = GroupMember(user_id=user_id, group_id=group_id, role=role, active=True)
        self.session.add(member)
        await self.session.flush()
        return member

    async def deactivate_member(self, user_id: int, group_id: int):
        member = await self.get_membership(user_id, group_id)
        if member:
            member.active = False
            await self.session.flush()

    async def deactivate_group(self, telegram_chat_id: int):
        group = await self.get_by_chat_id(telegram_chat_id)
        if group:
            group.active = False
            await self.session.flush()

    async def get_active_member_ids(self, group_id: int) -> List[int]:
        """Returns internal User.id values for active members of a group."""
        result = await self.session.execute(
            select(GroupMember.user_id).where(
                GroupMember.group_id == group_id, GroupMember.active == True
            )
        )
        return [row[0] for row in result.all()]

    async def get_user_groups(self, user_id: int) -> List[Group]:
        result = await self.session.execute(
            select(Group)
            .join(GroupMember, Group.id == GroupMember.group_id)
            .where(GroupMember.user_id == user_id, GroupMember.active == True, Group.active == True)
        )
        return result.scalars().all()


class ProfileRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_user_id(self, user_id: int) -> Optional[UserBehaviorProfile]:
        result = await self.session.execute(
            select(UserBehaviorProfile).where(UserBehaviorProfile.user_id == user_id)
        )
        return result.scalar_one_or_none()

    async def update(self, user_id: int, **kwargs) -> Optional[UserBehaviorProfile]:
        profile = await self.get_by_user_id(user_id)
        if profile:
            for k, v in kwargs.items():
                if hasattr(profile, k):
                    setattr(profile, k, v)
            profile.last_updated = datetime.utcnow()
            await self.session.flush()
        return profile


class SessionRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, session_id: int) -> Optional[Session]:
        result = await self.session.execute(select(Session).where(Session.id == session_id))
        return result.scalar_one_or_none()

    async def create(self, **kwargs) -> Session:
        sess = Session(**kwargs)
        self.session.add(sess)
        await self.session.flush()
        return sess

    async def update_status(self, session_id: int, status: str):
        sess = await self.get_by_id(session_id)
        if sess:
            sess.status = status
            await self.session.flush()

    async def get_participants(self, session_id: int) -> List[SessionParticipant]:
        result = await self.session.execute(
            select(SessionParticipant).where(SessionParticipant.session_id == session_id)
        )
        return result.scalars().all()

    async def get_active_sessions(self, group_id: Optional[int] = None) -> List[Session]:
        query = select(Session).where(Session.status.in_(["SCHEDULED", "ATTENDANCE", "READY", "STARTED"]))
        if group_id:
            query = query.where(Session.group_id == group_id)
        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_user_sessions(self, user_id: int, limit: int = 20) -> List[Session]:
        result = await self.session.execute(
            select(Session)
            .join(SessionParticipant, Session.id == SessionParticipant.session_id)
            .where(SessionParticipant.user_id == user_id)
            .order_by(desc(Session.scheduled_start))
            .limit(limit)
        )
        return result.scalars().all()

    async def get_group_sessions(self, group_id: int, days_back: int = 30) -> List[Session]:
        cutoff = time_service.days_ago(days_back)
        result = await self.session.execute(
            select(Session)
            .where(Session.group_id == group_id, Session.created_at >= cutoff)
            .order_by(desc(Session.created_at))
        )
        return result.scalars().all()

    async def get_session_results(self, session_id: int) -> List[SessionResult]:
        result = await self.session.execute(
            select(SessionResult).where(SessionResult.session_id == session_id)
        )
        return result.scalars().all()

    async def get_group_part_number(self, group_id: int, session_id: int) -> int:
        """The user-facing part number (پارت #N) must count only this
        group's own sessions, in creation order -- NOT the global
        auto-increment Session.id, which is shared across every group and
        jumps around depending on how many sessions other groups have
        created. Without this, a group on its actual first part could be
        shown "پارت #4" just because three other groups' sessions were
        created earlier and claimed ids 1-3."""
        result = await self.session.execute(
            select(func.count()).select_from(Session).where(
                Session.group_id == group_id,
                Session.id <= session_id,
            )
        )
        count = result.scalar()
        return count or 1


class MessageRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_messages_by_category(self, category: str, active_only: bool = True) -> List[MessageTemplate]:
        query = select(MessageTemplate).where(MessageTemplate.category == category)
        if active_only:
            query = query.where(MessageTemplate.active == True)
        result = await self.session.execute(query)
        return result.scalars().all()

    async def get_random_message(self, category: str, exclude_ids: Optional[List[int]] = None) -> Optional[MessageTemplate]:
        query = select(MessageTemplate).where(MessageTemplate.category == category, MessageTemplate.active == True)
        if exclude_ids:
            query = query.where(~MessageTemplate.id.in_(exclude_ids))
        result = await self.session.execute(query)
        msgs = result.scalars().all()
        if not msgs:
            return None
        weights = [m.weight for m in msgs]
        return random.choices(msgs, weights=weights, k=1)[0]

    async def create_message(self, category: str, text: str, created_by: Optional[int] = None,
                             weight: float = 1.0, subcategory: Optional[str] = None) -> MessageTemplate:
        msg = MessageTemplate(category=category, subcategory=subcategory, text=text, created_by=created_by, weight=weight)
        self.session.add(msg)
        await self.session.flush()
        return msg

    async def update_usage(self, message_id: int):
        msg = await self.session.get(MessageTemplate, message_id)
        if msg:
            msg.usage_count += 1
            msg.last_used_at = datetime.utcnow()
            await self.session.flush()

    async def update_message(self, message_id: int, **kwargs) -> Optional[MessageTemplate]:
        msg = await self.session.get(MessageTemplate, message_id)
        if msg:
            for k, v in kwargs.items():
                if hasattr(msg, k):
                    setattr(msg, k, v)
            msg.updated_at = datetime.utcnow()
            await self.session.flush()
        return msg

    async def delete_message(self, message_id: int) -> bool:
        msg = await self.session.get(MessageTemplate, message_id)
        if msg:
            await self.session.delete(msg)
            await self.session.flush()
            return True
        return False

    async def toggle_active(self, message_id: int) -> Optional[bool]:
        msg = await self.session.get(MessageTemplate, message_id)
        if msg:
            msg.active = not msg.active
            await self.session.flush()
            return msg.active
        return None


# ============================================================
# ANALYTICS & BEHAVIOR
# ============================================================
class StatisticsCalculator:
    async def get_today_stats(self, user_id: int, session: AsyncSession) -> Optional[Dict[str, Any]]:
        today = time_service.start_of_day().replace(tzinfo=None)
        result = await session.execute(
            select(DailyStatistics).where(
                DailyStatistics.user_id == user_id,
                DailyStatistics.date == today
            )
        )
        stats = result.scalar_one_or_none()
        if not stats:
            return None
        return {
            "sessions": stats.sessions,
            "minutes": stats.minutes,
            "completed": stats.completed,
            "missed": stats.missed,
            "avg_duration": stats.avg_duration,
            "completion_rate": stats.completion_rate,
        }

    async def get_weekly_stats(self, user_id: int, session: AsyncSession) -> Optional[Dict[str, Any]]:
        now = time_service.now().replace(tzinfo=None)
        week_start = now - timedelta(days=now.weekday())
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        result = await session.execute(
            select(WeeklyStatistics).where(
                WeeklyStatistics.user_id == user_id,
                WeeklyStatistics.week_start == week_start
            )
        )
        stats = result.scalar_one_or_none()
        if not stats:
            return None
        return {
            "sessions": stats.sessions,
            "minutes": stats.minutes,
            "completed": stats.completed,
            "missed": stats.missed,
            "avg_duration": stats.avg_duration,
            "completion_rate": stats.completion_rate,
            "consistency": stats.consistency,
            "best_day": stats.best_day,
            "best_hour": stats.best_hour,
            "trend": stats.trend,
        }

    async def calculate_daily_stats(self, user_id: int, date: datetime, session: AsyncSession):
        # NOTE (separate pre-existing bug, unrelated to the JSON migration):
        # `date` arrives as a tz-aware Asia/Tehran datetime (from
        # time_service.now()), but SessionResult.reported_at is stored as
        # naive UTC (datetime.utcnow()). Comparing aware vs naive datetimes
        # either raises TypeError or, in the old SQLite backend, silently
        # compared mismatched ISO strings. Stripping tzinfo here fixes the
        # immediate symptom (daily/weekly stats never populating) but the
        # app still mixes Tehran wall-clock and UTC-clock timestamps
        # throughout -- worth a real look when you have time.
        if date.tzinfo is not None:
            date = date.replace(tzinfo=None)
        start = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        # NOTE: the original code filtered on `Session.reported_at`, a
        # column that doesn't exist on the Session model (only
        # SessionResult has `reported_at`) -- this was a pre-existing bug
        # that would have raised AttributeError at runtime. Fixed to
        # filter on SessionResult.reported_at, which is what was clearly
        # intended (results reported within the given day).
        result = await session.execute(
            select(SessionResult)
            .where(
                SessionResult.user_id == user_id,
                SessionResult.reported_at >= start,
                SessionResult.reported_at <= end
            )
        )
        results = result.scalars().all()
        if not results:
            return
        sessions = len(results)
        completed = sum(1 for r in results if r.completion_status == "COMPLETED")
        missed = sum(1 for r in results if r.completion_status == "MISSED")
        total_minutes = sum(r.actual_duration or 0 for r in results if r.actual_duration)
        avg_duration = total_minutes / sessions if sessions > 0 else 0
        completion_rate = completed / sessions if sessions > 0 else 0
        existing = await session.execute(
            select(DailyStatistics).where(
                DailyStatistics.user_id == user_id,
                DailyStatistics.date == start
            )
        )
        stats = existing.scalar_one_or_none()
        if stats:
            stats.sessions = sessions
            stats.minutes = total_minutes
            stats.completed = completed
            stats.missed = missed
            stats.avg_duration = avg_duration
            stats.completion_rate = completion_rate
        else:
            stats = DailyStatistics(
                user_id=user_id, date=start, sessions=sessions, minutes=total_minutes,
                completed=completed, missed=missed, avg_duration=avg_duration,
                completion_rate=completion_rate
            )
            session.add(stats)
        await session.flush()

    async def calculate_weekly_stats(self, user_id: int, week_start: datetime, session: AsyncSession):
        week_end = week_start + timedelta(days=7)
        result = await session.execute(
            select(DailyStatistics).where(
                DailyStatistics.user_id == user_id,
                DailyStatistics.date >= week_start,
                DailyStatistics.date < week_end
            )
        )
        daily_stats = result.scalars().all()
        if not daily_stats:
            return
        total_sessions = sum(d.sessions for d in daily_stats)
        total_minutes = sum(d.minutes for d in daily_stats)
        total_completed = sum(d.completed for d in daily_stats)
        total_missed = sum(d.missed for d in daily_stats)
        avg_duration = total_minutes / total_sessions if total_sessions > 0 else 0
        completion_rate = total_completed / total_sessions if total_sessions > 0 else 0
        days_with_study = sum(1 for d in daily_stats if d.sessions > 0)
        consistency = days_with_study / 7
        best_day = max(daily_stats, key=lambda d: d.minutes).date.weekday() if daily_stats else 0
        best_hour = 18  # default
        if len(daily_stats) >= 3:
            recent = daily_stats[-3:]
            if all(r.minutes > daily_stats[i].minutes for i, r in enumerate(recent[:-1])):
                trend = "UPWARD"
            elif all(r.minutes < daily_stats[i].minutes for i, r in enumerate(recent[:-1])):
                trend = "DOWNWARD"
            else:
                trend = "STABLE"
        else:
            trend = "STABLE"
        existing = await session.execute(
            select(WeeklyStatistics).where(
                WeeklyStatistics.user_id == user_id,
                WeeklyStatistics.week_start == week_start
            )
        )
        stats = existing.scalar_one_or_none()
        if stats:
            stats.sessions = total_sessions
            stats.minutes = total_minutes
            stats.completed = total_completed
            stats.missed = total_missed
            stats.avg_duration = avg_duration
            stats.completion_rate = completion_rate
            stats.consistency = consistency
            stats.best_day = best_day
            stats.best_hour = best_hour
            stats.trend = trend
        else:
            stats = WeeklyStatistics(
                user_id=user_id, week_start=week_start,
                sessions=total_sessions, minutes=total_minutes,
                completed=total_completed, missed=total_missed,
                avg_duration=avg_duration, completion_rate=completion_rate,
                consistency=consistency, best_day=best_day, best_hour=best_hour,
                trend=trend
            )
            session.add(stats)
        await session.flush()


class BehaviorAnalyzer:
    @staticmethod
    def update_profile_from_result(profile: UserBehaviorProfile, result: SessionResult):
        # Update streaks
        if result.completion_status == "COMPLETED":
            profile.success_streak += 1
            profile.miss_streak = 0
        else:
            profile.miss_streak += 1
            profile.success_streak = 0

        # Update completion rate (weighted moving average)
        if result.actual_duration:
            profile.current_capacity = profile.current_capacity * 0.7 + result.actual_duration * 0.3
            if result.actual_duration > profile.maximum_observed_capacity:
                profile.maximum_observed_capacity = result.actual_duration

        # Update recent average
        if result.actual_duration:
            profile.recent_average = profile.recent_average * 0.6 + result.actual_duration * 0.4

        # Update load (decay)
        profile.current_load = max(0, profile.current_load - 10)
        if result.actual_duration:
            profile.current_load += result.actual_duration * 0.1

        # Recovery score
        if profile.miss_streak == 0 and profile.success_streak > 0:
            profile.recovery_score = min(1.0, profile.recovery_score + 0.1)
        elif profile.miss_streak > 0:
            profile.recovery_score = max(0.0, profile.recovery_score - 0.05)

        # Starting friction
        if profile.miss_streak > 0:
            profile.starting_friction_score = min(1.0, profile.starting_friction_score + 0.05)
        else:
            profile.starting_friction_score = max(0, profile.starting_friction_score - 0.02)

        # Trend detection
        if profile.success_streak >= 3 and profile.completion_rate > 0.8:
            profile.trend = "UPWARD"
        elif profile.miss_streak >= 2:
            profile.trend = "DOWNWARD"
        elif profile.completion_rate > 0.6:
            profile.trend = "STABLE"
        else:
            profile.trend = "VOLATILE"

        # Momentum
        if profile.success_streak > 0:
            profile.momentum_score = min(1.0, profile.momentum_score + 0.05)
        else:
            profile.momentum_score = max(0, profile.momentum_score - 0.03)

        # Consistency
        profile.consistency_score = (profile.consistency_score * 0.7 +
                                     (0.8 if profile.success_streak > 0 else 0.2) * 0.3)

        # Update completion rate (global)
        # This would be better computed from all results, but we approximate
        if result.completion_status == "COMPLETED":
            profile.completion_rate = min(1.0, profile.completion_rate + 0.02)
        else:
            profile.completion_rate = max(0, profile.completion_rate - 0.02)

        profile.last_updated = datetime.utcnow()


# ============================================================
# RECOMMENDATION ENGINE
# ============================================================
class CapacityCalculator:
    def calculate_capacity(self, profile: UserBehaviorProfile, current_time: datetime) -> Dict[str, float]:
        current = profile.current_capacity
        comfort = profile.comfort_capacity
        challenge = profile.challenge_capacity
        min_viable = profile.minimum_viable_session
        hour = current_time.hour

        # Time-of-day adjustment
        if 5 <= hour < 8:
            time_adj = 0.85
        elif 8 <= hour < 12:
            time_adj = 1.0
        elif 12 <= hour < 14:
            time_adj = 0.9
        elif 14 <= hour < 17:
            time_adj = 0.95
        elif 17 <= hour < 21:
            time_adj = 1.05
        elif 21 <= hour < 23:
            time_adj = 0.85
        else:
            time_adj = 0.7

        # Load penalty
        load_penalty = max(0, profile.current_load / 120) * 0.3

        # Starting friction penalty
        friction_penalty = profile.starting_friction_score * 0.2

        current_adj = current * time_adj * (1 - load_penalty) * (1 - friction_penalty)
        return {
            "current_capacity": max(current_adj, 10),
            "comfort_capacity": comfort * time_adj,
            "challenge_capacity": challenge * time_adj,
            "minimum_viable": min_viable,
            "adjusted_capacity": max(current_adj, 10),
        }


class GroupCapacityCalculator:
    def calculate(self, capacities: List[Dict[str, Any]]) -> float:
        if not capacities:
            return 30.0
        weighted = []
        for cap in capacities:
            base = cap.get("current_capacity", 30.0)
            # Reliability weight: combination of completion rate, attendance, consistency
            reliability = (cap.get("completion_rate", 0.5) * 0.4 +
                           cap.get("attendance_rate", 0.5) * 0.3 +
                           cap.get("consistency", 0.5) * 0.3)
            reliability = min(1.0, reliability)
            weight = max(0.1, reliability)
            weighted.append({"capacity": base, "weight": weight})

        # Outlier removal (IQR)
        if len(weighted) >= 4:
            caps = sorted([c["capacity"] for c in weighted])
            q1 = caps[len(caps)//4]
            q3 = caps[3*len(caps)//4]
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr
            weighted = [c for c in weighted if lower <= c["capacity"] <= upper]

        if not weighted:
            return 30.0

        total_weight = sum(c["weight"] for c in weighted)
        weighted_mean = sum(c["capacity"] * c["weight"] for c in weighted) / total_weight
        return round(weighted_mean, 1)


class ModeDetector:
    def detect(self, profiles: List[UserBehaviorProfile]) -> str:
        recovery_count = 0
        growth_count = 0
        calm_count = 0
        total = len(profiles)
        for p in profiles:
            if p.miss_streak >= 2 or p.starting_friction_score > 0.6:
                recovery_count += 1
            if p.success_streak >= 3 and p.completion_rate >= 0.8:
                growth_count += 1
            if p.current_load > 90 or time_service.is_late_night():
                calm_count += 1
        if total == 0:
            return "normal"
        if recovery_count / total >= 0.5:
            return "recovery"
        if growth_count / total >= 0.6:
            return "growth"
        if calm_count / total >= 0.4:
            return "calm"
        return "normal"


class FitnessScorer:
    def calculate(self, duration: int, capacities: List[Dict], group_capacity: float,
                  current_time: datetime, mode: str) -> float:
        # Group fit
        if group_capacity > 0:
            group_fit = max(0, 1 - abs(duration - group_capacity) / max(group_capacity, 1) * 0.5)
        else:
            group_fit = 0.5

        # Personal fit
        personal_fits = []
        for cap in capacities:
            current = cap.get("current_capacity", 30)
            comfort = cap.get("comfort_capacity", 25)
            challenge = cap.get("challenge_capacity", 40)
            if duration <= comfort:
                pf = 1.0
            elif duration <= current:
                pf = 0.8
            elif duration <= challenge:
                pf = 0.6
            elif duration <= challenge * 1.2:
                pf = 0.4
            else:
                pf = 0.2
            personal_fits.append(pf)
        personal_fit = sum(personal_fits) / len(personal_fits) if personal_fits else 0.5

        # Time fit
        hour = current_time.hour
        if 5 <= hour < 9:
            time_score = 0.7
        elif 9 <= hour < 12:
            time_score = 0.9
        elif 12 <= hour < 14:
            time_score = 0.6
        elif 14 <= hour < 17:
            time_score = 0.8
        elif 17 <= hour < 21:
            time_score = 0.95
        elif 21 <= hour < 23:
            time_score = 0.7
        else:
            time_score = 0.4

        # Completion probability
        comp_probs = []
        for cap in capacities:
            current = cap.get("current_capacity", 30)
            max_obs = cap.get("max_observed", 30)
            if duration <= current * 0.8:
                prob = 0.9
            elif duration <= current:
                prob = 0.75
            elif duration <= max_obs:
                prob = 0.6
            elif duration <= max_obs * 1.2:
                prob = 0.4
            else:
                prob = 0.2
            comp_rate = cap.get("completion_rate", 0.7)
            prob = prob * (0.5 + comp_rate * 0.5)
            comp_probs.append(prob)
        completion_prob = sum(comp_probs) / len(comp_probs) if comp_probs else 0.5

        # Progress value
        if mode == "growth":
            progress = 0.8 if duration > 30 else 0.5
        elif mode == "recovery":
            progress = 0.7 if duration >= 15 else 0.5
        else:
            progress = 0.7 if duration >= 30 else 0.5

        # Intent fit
        avg_intent = sum(cap.get("intent_score", 0.5) for cap in capacities) / len(capacities) if capacities else 0.5

        # Overload risk
        overload_risks = []
        for cap in capacities:
            current = cap.get("current_capacity", 30)
            load = cap.get("current_load", 0)
            if duration > current * 1.3:
                risk = 0.9
            elif duration > current * 1.1:
                risk = 0.6
            elif duration > current:
                risk = 0.3
            else:
                risk = 0.1
            if load > 90:
                risk = min(1, risk + 0.4)
            elif load > 60:
                risk = min(1, risk + 0.2)
            overload_risks.append(risk)
        overload_risk = sum(overload_risks) / len(overload_risks) if overload_risks else 0.1

        failure_risk = 1 - completion_prob

        weights = config.ALGORITHM_WEIGHTS
        score = (
            weights["GROUP_FIT_WEIGHT"] * group_fit +
            weights["PERSONAL_FIT_WEIGHT"] * personal_fit +
            weights["TIME_FIT_WEIGHT"] * time_score +
            0.15 * completion_prob +
            0.10 * progress +
            weights["INTENT_WEIGHT"] * avg_intent -
            weights["OVERLOAD_PENALTY"] * overload_risk -
            weights["FAILURE_PENALTY"] * failure_risk
        )
        return max(0, min(score, 1.0))


class RecommendationEngine:
    def __init__(self):
        self.capacity_calc = CapacityCalculator()
        self.group_calc = GroupCapacityCalculator()
        self.mode_detector = ModeDetector()
        self.fitness_scorer = FitnessScorer()

    async def recommend(
        self,
        group_id: int,
        participant_ids: List[int],
        current_time: datetime,
        session: AsyncSession,
    ) -> Dict[str, Any]:
        if not participant_ids:
            return {
                "minimum": 10,
                "target": 30,
                "extension": 45,
                "mode": "normal",
                "confidence": 0.3,
                "group_capacity": 30.0,
                "reason_codes": ["NO_PARTICIPANTS"]
            }

        # Load profiles
        profiles = []
        for uid in participant_ids:
            result = await session.execute(
                select(UserBehaviorProfile).where(UserBehaviorProfile.user_id == uid)
            )
            p = result.scalar_one_or_none()
            if not p:
                # Cold start: create default profile
                p = UserBehaviorProfile(user_id=uid)
                session.add(p)
                await session.flush()
            profiles.append(p)

        # Individual capacities
        ind_caps = []
        for p in profiles:
            cap = self.capacity_calc.calculate_capacity(p, current_time)
            cap["completion_rate"] = p.completion_rate
            cap["attendance_rate"] = p.attendance_rate
            cap["current_load"] = p.current_load
            cap["max_observed"] = p.maximum_observed_capacity
            cap["consistency"] = p.consistency_score
            # Intent score based on recent streaks
            cap["intent_score"] = 0.5 + (p.success_streak * 0.05) - (p.miss_streak * 0.1)
            cap["intent_score"] = max(0, min(1, cap["intent_score"]))
            cap["user_id"] = p.user_id
            ind_caps.append(cap)

        # Group capacity
        group_cap = self.group_calc.calculate(ind_caps)

        # Mode detection
        mode = self.mode_detector.detect(profiles)

        # Adjust capacities based on mode
        if mode == "growth":
            for cap in ind_caps:
                cap["current_capacity"] = min(cap["current_capacity"] * 1.05, 120)
                cap["challenge_capacity"] = min(cap["challenge_capacity"] * 1.05, 120)
        elif mode == "recovery":
            for cap in ind_caps:
                cap["current_capacity"] = max(cap["current_capacity"] * 0.8, 10)
                cap["minimum_viable"] = max(cap.get("minimum_viable", 15) - 5, 10)
        elif mode == "calm":
            for cap in ind_caps:
                cap["current_capacity"] = min(cap["current_capacity"] * 0.7, cap["comfort_capacity"])
                cap["challenge_capacity"] = cap["current_capacity"]

        # Evaluate candidates
        candidates = []
        for dur in config.CANDIDATE_DURATIONS:
            if dur < config.DEFAULT_MIN_DURATION or dur > config.DEFAULT_MAX_DURATION:
                continue
            fitness = self.fitness_scorer.calculate(
                duration=dur,
                capacities=ind_caps,
                group_capacity=group_cap,
                current_time=current_time,
                mode=mode
            )
            candidates.append({"duration": dur, "fitness": fitness})

        candidates.sort(key=lambda x: x["fitness"], reverse=True)
        if not candidates:
            best = {"duration": 30}
        else:
            best = candidates[0]

        # Mode-specific constraints
        if mode == "recovery":
            rec_candidates = [c for c in candidates if c["duration"] <= 30]
            if rec_candidates:
                best = max(rec_candidates, key=lambda x: x["fitness"])
        elif mode == "calm":
            calm_candidates = [c for c in candidates if c["duration"] <= 25]
            if calm_candidates:
                best = max(calm_candidates, key=lambda x: x["fitness"])

        target = best["duration"]

        # --- Minimum: driven by the weakest included participant, not a
        # flat target // 2. This is the "User D with capacity=15 shouldn't
        # be ignored, but shouldn't drag the whole group to 15 either" case:
        # the group target still reflects the stronger majority, while the
        # minimum is set low enough that the weakest real participant can
        # still complete *something* and be counted as a win.
        viable_floors = [cap.get("minimum_viable", 15) for cap in ind_caps]
        floor = min(viable_floors) if viable_floors else config.DEFAULT_MIN_DURATION
        floor = max(config.DEFAULT_MIN_DURATION, min(floor, target))
        minimum = min(config.CANDIDATE_DURATIONS, key=lambda x: abs(x - floor))

        # --- Extension: driven by how far the *stronger half* of the group
        # can realistically go, capped by their own challenge capacity --
        # not an arbitrary target + 15. If nothing beats the target's
        # fitness within that realistic ceiling, there is no extension.
        strong_caps = sorted(ind_caps, key=lambda c: c.get("current_capacity", 30), reverse=True)
        strong_half = strong_caps[: max(1, len(strong_caps) // 2)]
        avg_challenge = sum(c.get("challenge_capacity", 40) for c in strong_half) / len(strong_half)
        ext_ceiling = min(config.DEFAULT_MAX_DURATION, max(target, round(avg_challenge)))
        extension_candidates = [c for c in candidates if target < c["duration"] <= ext_ceiling]
        if extension_candidates:
            extension = max(extension_candidates, key=lambda c: c["fitness"])["duration"]
        else:
            extension = target

        # Confidence
        n = len(profiles)
        conf = 0.4 if n < 2 else 0.6 if n < 4 else 0.8
        avg_cons = sum(p.consistency_score for p in profiles) / n if n > 0 else 0.5
        if avg_cons > 0.7:
            conf = min(conf + 0.05, 0.95)
        elif avg_cons < 0.3:
            conf = max(conf - 0.1, 0.3)
        conf = round(conf, 2)

        reason_codes = []
        if mode == "growth":
            reason_codes.append("GROWTH_MODE")
        elif mode == "recovery":
            reason_codes.append("RECOVERY_MODE")
        elif mode == "calm":
            reason_codes.append("CALM_MODE")
        if group_cap > 40:
            reason_codes.append("HIGH_GROUP_COMPLETION")
        if 17 <= current_time.hour < 21:
            reason_codes.append("GOOD_EVENING_FIT")
        if 8 <= current_time.hour < 12:
            reason_codes.append("GOOD_MORNING_FIT")
        if conf < 0.5:
            reason_codes.append("LOW_CONFIDENCE")
        if n < 3:
            reason_codes.append("LOW_DATA")

        return {
            "minimum": minimum,
            "target": target,
            "extension": extension,
            "mode": mode,
            "confidence": conf,
            "group_capacity": group_cap,
            "reason_codes": reason_codes[:6],
        }


# ============================================================
# SESSION MANAGER (Lifecycle)
# ============================================================
class SessionManager:
    def __init__(self, session: AsyncSession):
        self.session = session
        self.repo = SessionRepository(session)

    async def get_session(self, session_id: int) -> Optional[Session]:
        return await self.repo.get_by_id(session_id)

    async def create_session(self, **kwargs) -> Session:
        return await self.repo.create(**kwargs)

    async def update_status(self, session_id: int, status: str):
        await self.repo.update_status(session_id, status)

    async def get_participants(self, session_id: int) -> List[SessionParticipant]:
        return await self.repo.get_participants(session_id)

    async def get_group_part_number(self, group_id: int, session_id: int) -> int:
        return await self.repo.get_group_part_number(group_id, session_id)

    async def register_participant(self, session_id: int, user_id: int) -> Optional[SessionParticipant]:
        existing = await self.session.execute(
            select(SessionParticipant).where(
                SessionParticipant.session_id == session_id,
                SessionParticipant.user_id == user_id
            )
        )
        if existing.scalar_one_or_none():
            return None
        sess = await self.get_session(session_id)
        if not sess or sess.status not in ["SCHEDULED", "ATTENDANCE"]:
            return None
        participant = SessionParticipant(
            session_id=session_id,
            user_id=user_id,
            attendance_status="READY",
            ready_at=datetime.utcnow()
        )
        self.session.add(participant)
        try:
            await self.session.flush()
        except IntegrityError:
            # Two near-simultaneous "هستم" taps both passed the existence
            # check above before either committed. The unique constraint on
            # (session_id, user_id) is what actually guarantees idempotency;
            # this just makes losing that race a no-op instead of a crash.
            await self.session.rollback()
            return None
        return participant

    async def report_result(self, session_id: int, user_id: int,
                            completion_status: str, actual_duration: Optional[int] = None) -> bool:
        existing = await self.session.execute(
            select(SessionResult).where(
                SessionResult.session_id == session_id,
                SessionResult.user_id == user_id
            )
        )
        if existing.scalar_one_or_none():
            return False
        sess = await self.get_session(session_id)
        if not sess:
            return False
        # If the user taps "🟢 کامل" (COMPLETED) directly, no duration is
        # ever typed -- only the separate "⏱ ثبت مدت واقعی" button asks for
        # one. Without this default, actual_duration stayed None, so the
        # part was saved but counted as 0 minutes everywhere downstream
        # (daily/weekly stats, /today, capacity profile), making it look
        # like completed parts were never being recorded at all.
        if actual_duration is None and completion_status == "COMPLETED":
            actual_duration = sess.planned_duration
        result = SessionResult(
            session_id=session_id,
            user_id=user_id,
            planned_duration=sess.planned_duration,
            actual_duration=actual_duration,
            completion_status=completion_status,
            reported_at=datetime.utcnow()
        )
        self.session.add(result)
        try:
            await self.session.flush()
        except IntegrityError:
            # Same idempotency guard as register_participant: a duplicate
            # result submission (e.g. a retried tap after a slow network
            # response) must not create a second row or crash the handler.
            await self.session.rollback()
            return False

        # Update profile
        profile_repo = ProfileRepository(self.session)
        profile = await profile_repo.get_by_user_id(user_id)
        if profile:
            BehaviorAnalyzer.update_profile_from_result(profile, result)
            await self.session.flush()

        # Record event
        event = BehaviorEvent(
            user_id=user_id,
            event_type=f"USER_{completion_status}",
            event_meta={"session_id": session_id}
        )
        self.session.add(event)
        await self.session.flush()

        # Update statistics
        calc = StatisticsCalculator()
        await calc.calculate_daily_stats(user_id, time_service.now(), self.session)
        naive_now = time_service.now().replace(tzinfo=None)
        week_start = naive_now - timedelta(days=naive_now.weekday())
        week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        await calc.calculate_weekly_stats(user_id, week_start, self.session)

        return True

    async def start_session(self, session_id: int) -> bool:
        sess = await self.get_session(session_id)
        if not sess or sess.status != "READY":
            return False
        sess.actual_start = time_service.now()
        sess.status = "STARTED"
        await self.session.flush()
        return True

    async def end_session(self, session_id: int) -> bool:
        sess = await self.get_session(session_id)
        if not sess or sess.status != "STARTED":
            return False
        sess.actual_end = time_service.now()
        sess.status = "ENDED"
        await self.session.flush()
        return True


# ============================================================
# SCHEDULER SERVICE (APScheduler)
# ============================================================
# Global scheduler instance
# NOTE: job_defaults sets misfire_grace_time=None (unlimited). Without this,
# APScheduler's default grace period is only 1 second: any job whose
# scheduled run_at ends up within ~1s of "now" (or slightly in the past) by
# the time add_job() actually registers it -- e.g. _schedule_next_poll()
# setting run_at = now when a part used its full max window -- gets
# silently dropped with no exception and no log line. That's what was
# causing "part 2" to sometimes never auto-start right after part 1 ran to
# its maximum duration.
scheduler = AsyncIOScheduler(
    timezone="Asia/Tehran",
    job_defaults={"misfire_grace_time": None, "coalesce": True, "max_instances": 1},
)

# Scheduled job functions (these are called by the scheduler)
async def _attendance_timeout(session_id: int):
    """Closes the attendance window.

    This is the single most important fix from the original prototype: the
    duration that was set when the session was *created* only reflects the
    person who typed /session. Here -- once we finally know who actually
    tapped "هستم" -- we recompute the recommendation against exactly that
    volunteer set (never the whole group, never just the creator) and only
    then lock in minimum/target/extension for the session.
    """
    async with get_session() as db_session:
        manager = SessionManager(db_session)
        sess = await manager.get_session(session_id)
        if not sess or sess.status != "ATTENDANCE":
            return
        participants = await manager.get_participants(session_id)
        ready_participants = [p for p in participants if p.attendance_status == "READY"]

        if not ready_participants:
            sess.status = "MISSED"
            await db_session.flush()
            # Nobody was active -- per design, do NOT schedule another
            # automatic poll on a timer. Wait for real group activity
            # instead (see GroupSyncMiddleware), so a quiet group doesn't
            # get spammed with back-to-back "آماده‌اید؟" prompts.
            settings_result = await db_session.execute(
                select(GroupSetting).where(GroupSetting.group_id == sess.group_id)
            )
            settings = settings_result.scalar_one_or_none()
            if settings:
                settings.awaiting_activity = True
                await db_session.flush()
            logger.info(f"Session {session_id} missed, no participants")
            return

        engine = RecommendationEngine()
        rec = await engine.recommend(
            group_id=sess.group_id,
            participant_ids=[p.user_id for p in ready_participants],
            current_time=time_service.now(),
            session=db_session,
        )
        sess.planned_duration = rec["target"]
        sess.minimum_duration = rec["minimum"]
        sess.target_duration = rec["target"]
        sess.extension_duration = rec["extension"]
        sess.mode = rec["mode"]
        sess.recommendation_confidence = rec["confidence"]
        sess.reason_codes = rec["reason_codes"]
        sess.status = "READY"
        await db_session.flush()

        # Immediately transition READY -> STARTED right here instead of
        # depending on the separately-scheduled `_session_start` job to
        # land strictly *after* this point. That job was queued back when
        # the session was first created, using a placeholder start_time --
        # if it fires before (or at the same instant as) this function
        # finishes, `start_session()` still sees status="ATTENDANCE", does
        # nothing, and the session is stuck at "READY" forever: it never
        # reaches "STARTED", so `_session_end` can never successfully end
        # it either, and every future readiness poll for this group gets
        # silently blocked by the "session already in flight" guard in
        # `_create_readiness_poll`. This is what could make a part -- especially
        # one started with /session, where the old start_time was scheduled
        # to fire *before* the attendance window even closed -- never chain
        # into the next automatic poll.
        await manager.start_session(session_id)
        _cancel_scheduled_job(_session_start, session_id)

        # The session_end job scheduled back at creation time used a
        # placeholder duration (guessed before we knew who'd actually show
        # up). Replace it now with one based on the real, just-computed
        # extension -- the same number we're about to announce as
        # "حداکثر" below -- so the part actually ends, and the next
        # automatic poll actually gets queued, at the max duration users
        # were told, not at some earlier/later placeholder time.
        _cancel_scheduled_job(_session_end, session_id)
        new_end_at = time_service.now() + timedelta(minutes=rec["extension"])
        await SchedulerService.schedule_job(
            "session_end", new_end_at, {"session_id": session_id}, db_session=db_session
        )
        await db_session.flush()

        logger.info(
            f"Attendance closed for session {session_id}: {len(ready_participants)} ready, "
            f"recommendation min={rec['minimum']} target={rec['target']} ext={rec['extension']} "
            f"mode={rec['mode']} confidence={rec['confidence']}"
        )

        # Announce the final, volunteer-based plan in the group.
        try:
            result = await db_session.execute(select(Group).where(Group.id == sess.group_id))
            group = result.scalar_one_or_none()
            if group and bot:
                sent = await bot.send_message(
                    group.telegram_chat_id,
                    f"✅ پارت آماده شد!\n"
                    f"👥 {len(ready_participants)} نفر آماده هستند.\n"
                    f"⏱ حداقل: {rec['minimum']} | هدف: {rec['target']} | حداکثر: {rec['extension']} دقیقه\n"
                    f"🧭 حالت: {rec['mode']}\n\n"
                    f"وقتی پارتت تموم شد بزن 👇 (حتی اگه اعلام آمادگی نکرده بودی)",
                    reply_markup=result_keyboard(session_id),
                )
                try:
                    # Pinned so the "declare I'm done" button stays easy to
                    # find for the whole session, not just right after it's
                    # sent -- requires the bot to have pin permission in
                    # the group; a missing permission shouldn't crash the
                    # announcement itself.
                    await bot.pin_chat_message(
                        group.telegram_chat_id, sent.message_id, disable_notification=True
                    )
                except Exception as pin_err:
                    logger.warning(f"Could not pin readiness message for session {session_id}: {pin_err}")
        except Exception as e:
            logger.error(f"Failed to announce session {session_id} readiness: {e}")


async def _create_readiness_poll(group_id: int):
    """The automatic version of /session: instead of a human typing the
    command, the bot itself asks "آماده پارت هستید؟" and only turns that
    into a real session if at least one person taps "هستم". This reuses the
    exact same ATTENDANCE -> _attendance_timeout flow as a manual /session,
    so the volunteer-based recommendation logic from Pass 1 applies
    identically -- the poll itself carries no separate duration logic.

    Scheduling policy (per the user's own answers, not a fixed guess):
    - Fires once every morning automatically (see the repurposed
      "morning_reminder" cron job).
    - After that, the *next* poll is scheduled once that session's own
      max window (start + extension_duration) has elapsed -- see
      _schedule_next_poll -- not a flat per-group buffer.
    - If a poll gets zero "هستم" responses, no further automatic poll is
      scheduled at all. The group is marked `awaiting_activity`, and the
      very next real message in that group (see GroupSyncMiddleware) is
      what triggers the next poll -- so a dead group doesn't get spammed.
    - Never polls during quiet hours.
    """
    async with get_session() as session:
        result = await session.execute(select(Group).where(Group.id == group_id))
        group = result.scalar_one_or_none()
        if not group or not group.active:
            return
        settings_result = await session.execute(
            select(GroupSetting).where(GroupSetting.group_id == group_id)
        )
        settings = settings_result.scalar_one_or_none()
        if not settings or not settings.auto_poll_enabled:
            return

        now = time_service.now()
        if _in_quiet_hours(now.hour, settings.quiet_hour_start, settings.quiet_hour_end):
            # Don't poll now. If we got here via the awaiting-activity path,
            # leave the flag set so the next message after quiet hours (or
            # tomorrow's fixed morning poll) triggers it instead.
            return

        # Never double-poll a group that already has a session in flight.
        active = await session.execute(
            select(Session).where(
                Session.group_id == group_id,
                Session.status.in_(["ATTENDANCE", "READY", "STARTED"]),
            )
        )
        if active.scalar_one_or_none():
            return

        group_repo = GroupRepository(session)
        member_ids = await group_repo.get_active_member_ids(group_id)
        if not member_ids:
            return

        engine = RecommendationEngine()
        # No one has volunteered yet -- this is just the invite message's
        # placeholder figure, exactly like a manual /session. The real
        # numbers get recomputed in _attendance_timeout from whoever
        # actually taps "هستم".
        rec = await engine.recommend(
            group_id=group_id, participant_ids=member_ids, current_time=now, session=session
        )

        window_minutes = settings.poll_window_minutes
        start_time = now + timedelta(minutes=window_minutes)
        end_time = start_time + timedelta(minutes=rec["target"])

        manager = SessionManager(session)
        sess = await manager.create_session(
            group_id=group_id,
            scheduled_start=start_time,
            scheduled_end=end_time,
            planned_duration=rec["target"],
            minimum_duration=rec["minimum"],
            target_duration=rec["target"],
            extension_duration=rec["extension"],
            mode=rec["mode"],
            creation_source="auto_poll",
            recommendation_confidence=rec["confidence"],
            reason_codes=rec["reason_codes"],
        )
        sess.status = "ATTENDANCE"
        settings.last_poll_at = now
        settings.awaiting_activity = False
        await session.flush()

        timeout_at = now + timedelta(minutes=window_minutes)
        await SchedulerService.schedule_job("attendance_timeout", timeout_at, {"session_id": sess.id}, db_session=session)
        await SchedulerService.schedule_job("session_start", start_time, {"session_id": sess.id}, db_session=session)
        await SchedulerService.schedule_job("session_end", end_time, {"session_id": sess.id}, db_session=session)

        if bot:
            try:
                await bot.send_message(
                    group.telegram_chat_id,
                    "☀️ آماده پارت مطالعاتی هستید؟\nهرکی هست بزنه 🟢 (حداقل یک نفر کافیه تا پارت برگزار بشه)",
                    reply_markup=attendance_keyboard(sess.id),
                )
            except Exception as e:
                logger.error(f"Failed to send readiness poll to group {group_id}: {e}")


def _in_quiet_hours(hour: int, quiet_start: int, quiet_end: int) -> bool:
    """quiet_start=23, quiet_end=8 means quiet from 23:00 to 08:00, wrapping
    past midnight."""
    if quiet_start == quiet_end:
        return False
    if quiet_start < quiet_end:
        return quiet_start <= hour < quiet_end
    return hour >= quiet_start or hour < quiet_end


async def _schedule_next_poll(session_id: int) -> Optional[datetime]:
    """Called once a session is done (timed out or everyone tapped a
    completion button early). The gap before the next automatic poll is
    NOT a flat per-group number anymore -- it's however long *this*
    session's own max window was (whatever the recommendation engine
    decided for it -- could be 20 minutes, could be 90), counted from
    when the "پارت آماده شد" start message actually went out. So the next
    poll lands at (start + حداکثر/extension), not at "whenever this one
    happened to end, plus a flat buffer".

    Returns the Tehran-local datetime the next readiness poll is actually
    scheduled for (so callers -- e.g. the "همه اعلام اتمام کردن" message --
    can tell people exactly when to expect it instead of leaving them
    guessing), or None if no poll ended up being scheduled (missing
    session/settings, or auto-poll disabled for the group)."""
    async with get_session() as session:
        manager = SessionManager(session)
        sess = await manager.get_session(session_id)
        if not sess:
            return None
        group_id = sess.group_id
        settings_result = await session.execute(
            select(GroupSetting).where(GroupSetting.group_id == group_id)
        )
        settings = settings_result.scalar_one_or_none()
        if not settings or not settings.auto_poll_enabled:
            return None
        settings.awaiting_activity = False

        start_reference = sess.actual_start or sess.scheduled_start
        now = time_service.now()
        run_at = time_service.to_tehran(start_reference) + timedelta(minutes=sess.extension_duration)
        if run_at < now:
            # The group used the entire max window (or the session ended
            # in some other way after it) -- don't make them wait an
            # extra buffer on top, offer the next poll right away. A tiny
            # 2s cushion (instead of `now` exactly) keeps this comfortably
            # ahead of the scheduler's own clock by the time add_job()
            # actually registers it below, on top of the job_defaults fix.
            run_at = now + timedelta(seconds=2)
        if _in_quiet_hours(run_at.hour, settings.quiet_hour_start, settings.quiet_hour_end):
            # Push to quiet_hour_end the same day (or next day if we're
            # already past it) -- next real poll will happen when the group
            # wakes back up, not a random time overnight.
            run_at = run_at.replace(hour=settings.quiet_hour_end, minute=0, second=0, microsecond=0)
            if run_at <= now:
                run_at += timedelta(days=1)
        await session.flush()
    await SchedulerService.schedule_job("readiness_poll", run_at, {"group_id": group_id})
    return run_at


def _cancel_scheduled_job(func, *args):
    """Removes any pending APScheduler job that calls `func` with these
    exact `args` (e.g. a not-yet-fired session_end for this session_id).

    Needed because ending a session early (everyone tapped "done" before
    the original timer-based estimate) must not leave the old session_end
    job to fire later and schedule a *second*, duplicate next-poll.
    """
    for job in scheduler.get_jobs():
        if job.func is func and tuple(job.args) == tuple(args):
            scheduler.remove_job(job.id)


async def _finish_session_early(session_id: int):
    """Ends a session as soon as everyone who was READY has tapped a
    completion button -- instead of waiting for the original timer-based
    session_end estimate, which was only ever a placeholder. Cancels that
    now-redundant timer and immediately queues the next automatic poll.

    Schedules the next poll *before* announcing anything, so the message
    can name the actual clock time people should expect it -- since
    people often finish a part early, "به‌زودی" (soon) left them guessing
    whether that meant a minute or an hour."""
    _cancel_scheduled_job(_session_end, session_id)
    async with get_session() as db_session:
        manager = SessionManager(db_session)
        sess = await manager.get_session(session_id)
        if not sess:
            return
        group_id = sess.group_id
        if sess.status == "STARTED":
            await manager.end_session(session_id)
        result = await db_session.execute(select(Group).where(Group.id == group_id))
        group = result.scalar_one_or_none()
    next_run_at = await _schedule_next_poll(session_id)
    if group and bot:
        if next_run_at:
            next_clock = time_service.format_datetime(next_run_at, "%H:%M")
            text = (
                "🎉 همه اعلام اتمام کردن!\n"
                f"⏰ پارت بعدی حدود ساعت {next_clock} پرسیده می‌شه."
            )
        else:
            text = "🎉 همه اعلام اتمام کردن! پارت بعدی به‌زودی برنامه‌ریزی می‌شه."
        try:
            await bot.send_message(group.telegram_chat_id, text)
        except Exception as e:
            logger.error(f"Failed to announce early finish for session {session_id}: {e}")


async def _session_start(session_id: int):
    async with get_session() as db_session:
        manager = SessionManager(db_session)
        await manager.start_session(session_id)
        logger.info(f"Session {session_id} started")


async def _session_end(session_id: int):
    async with get_session() as db_session:
        manager = SessionManager(db_session)
        sess = await manager.get_session(session_id)
        group_id = sess.group_id if sess else None
        await manager.end_session(session_id)
        logger.info(f"Session {session_id} ended")
    if group_id:
        # A session actually completing is exactly the signal that should
        # queue up the *next* automatic poll -- not a blind fixed interval.
        await _schedule_next_poll(session_id)


class SchedulerService:
    @staticmethod
    async def schedule_job(
        job_type: str,
        run_at: datetime,
        metadata: Dict[str, Any],
        db_session: Optional[AsyncSession] = None,
    ):
        """Writes the job row, then registers it with APScheduler.

        If the caller is already inside an open session/transaction on
        this SQLite database, pass it in as `db_session` so this method
        reuses that same connection instead of opening a second one.
        SQLite only allows a single writer at a time, so opening a fresh
        session here while the caller's own transaction is still
        uncommitted would otherwise deadlock: the new session waits for
        the caller's write lock to be released, but the caller is itself
        awaiting this call and won't commit until it returns.
        """
        job_id = f"{job_type}_{int(run_at.timestamp())}_{hash(str(metadata))}"

        async def _write(session: AsyncSession) -> bool:
            existing = await session.execute(
                select(SchedulerJob).where(SchedulerJob.job_id == job_id)
            )
            if existing.scalar_one_or_none():
                return False
            job = SchedulerJob(
                job_id=job_id,
                job_type=job_type,
                run_at=run_at,
                status="pending",
                job_meta=metadata
            )
            session.add(job)
            await session.flush()
            return True

        if db_session is not None:
            created = await _write(db_session)
        else:
            async with get_session() as session:
                created = await _write(session)

        if not created:
            return

        # Schedule with APScheduler
        if job_type == "attendance_timeout":
            scheduler.add_job(
                _attendance_timeout,
                trigger=DateTrigger(run_date=run_at),
                args=[metadata.get("session_id")],
                id=job_id,
                replace_existing=True
            )
        elif job_type == "session_start":
            scheduler.add_job(
                _session_start,
                trigger=DateTrigger(run_date=run_at),
                args=[metadata.get("session_id")],
                id=job_id,
                replace_existing=True
            )
        elif job_type == "session_end":
            scheduler.add_job(
                _session_end,
                trigger=DateTrigger(run_date=run_at),
                args=[metadata.get("session_id")],
                id=job_id,
                replace_existing=True
            )
        elif job_type == "readiness_poll":
            scheduler.add_job(
                _create_readiness_poll,
                trigger=DateTrigger(run_date=run_at),
                args=[metadata.get("group_id")],
                id=job_id,
                replace_existing=True
            )
        # Additional job types can be added here

    @staticmethod
    def start():
        scheduler.start()
        logger.info("Scheduler started")

    @staticmethod
    def stop():
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


# ============================================================
# MESSAGE ENGINE (Selector)
# ============================================================
class MessageSelector:
    def __init__(self):
        self._recently_used = {}  # user_id -> list of message ids

    async def get_message(
        self,
        session: AsyncSession,
        category: str,
        user_id: int,
        exclude_ids: Optional[List[int]] = None,
        **variables
    ) -> Optional[str]:
        query = select(MessageTemplate).where(MessageTemplate.category == category, MessageTemplate.active == True)
        if exclude_ids:
            query = query.where(~MessageTemplate.id.in_(exclude_ids))
        result = await session.execute(query)
        msgs = result.scalars().all()
        if not msgs:
            # Fallback to GENERAL
            result = await session.execute(
                select(MessageTemplate).where(MessageTemplate.category == "GENERAL", MessageTemplate.active == True)
            )
            msgs = result.scalars().all()
        if not msgs:
            return None

        recently = self._recently_used.get(user_id, [])
        available = [m for m in msgs if m.id not in recently]
        if not available:
            available = msgs
            self._recently_used[user_id] = []

        weights = [m.weight for m in available]
        selected = random.choices(available, weights=weights, k=1)[0]

        selected.usage_count += 1
        selected.last_used_at = datetime.utcnow()
        await session.flush()

        if user_id not in self._recently_used:
            self._recently_used[user_id] = []
        self._recently_used[user_id].append(selected.id)
        self._recently_used[user_id] = self._recently_used[user_id][-5:]

        text = selected.text
        for key, value in variables.items():
            text = text.replace(f"{{{key}}}", str(value))
        return text


# Global instance -- without this, MessageSelector was defined but never
# instantiated anywhere, so the category-based custom/sample messages
# (added by admins for SUCCESS/PARTIAL/MISSED/etc.) were never actually
# picked or shown to users, even though the whole selection engine existed.
message_selector = MessageSelector()


# ============================================================
# COACH LAYER (Assistant, Advisor, Teacher)
# ============================================================
class Assistant:
    async def get_today_status(self, user_id: int, session: AsyncSession) -> Dict[str, Any]:
        calc = StatisticsCalculator()
        stats = await calc.get_today_stats(user_id, session)
        if not stats:
            return {"has_data": False, "message": "امروز هنوز جلسه‌ای ثبت نشده. 🌱"}
        goal_result = await session.execute(
            select(UserGoal).where(UserGoal.user_id == user_id)
        )
        goal = goal_result.scalar_one_or_none()
        daily_goal = goal.daily_goal if goal else 90
        minutes = stats.get("minutes", 0)
        progress = min(100, int(minutes / daily_goal * 100))
        if progress >= 100:
            msg = f"🎉 تبریک! به هدف روزانه‌ات رسیدی!\n📊 {minutes} دقیقه از {daily_goal}"
        elif progress >= 70:
            msg = f"🔥 خیلی خوب پیش می‌ری! {minutes} دقیقه از {daily_goal} ({progress}%)"
        elif progress >= 40:
            msg = f"🌱 در مسیر درستی. {minutes} دقیقه از {daily_goal} ({progress}%)"
        else:
            msg = f"🌱 امروز تازه شروع کردی. {minutes} دقیقه از {daily_goal} ({progress}%)"
        stats["message"] = msg
        stats["daily_goal"] = daily_goal
        stats["progress"] = progress
        return stats

    async def get_next_suggestion(self, user_id: int, session: AsyncSession) -> str:
        profile_repo = ProfileRepository(session)
        profile = await profile_repo.get_by_user_id(user_id)
        if not profile:
            return "پیشنهاد می‌کنم با یک پارت ۲۰ دقیقه‌ای شروع کنی. 🌱"
        current = profile.current_capacity
        comfort = profile.comfort_capacity
        hour = time_service.hour()

        if 5 <= hour < 9:
            time_note = "صبح زود - پارت کوتاه‌تر مناسب‌تره."
            suggested = min(current * 0.7, comfort)
        elif 9 <= hour < 12:
            time_note = "صبح - زمان خوبی برای مطالعه."
            suggested = current
        elif 12 <= hour < 14:
            time_note = "ظهر - بهتره پارت کوتاه باشه."
            suggested = min(current * 0.7, comfort)
        elif 14 <= hour < 17:
            time_note = "بعد از ظهر - انرژی خوبی داری!"
            suggested = current
        elif 17 <= hour < 21:
            time_note = "عصر - بهترین زمان برای مطالعه."
            suggested = current * 1.1
        else:
            time_note = "شب - پارت آرام و کوتاه پیشنهاد می‌شه."
            suggested = min(current * 0.6, comfort)

        suggested = min(config.CANDIDATE_DURATIONS, key=lambda x: abs(x - suggested))
        return f"💡 {time_note}\n\nپارت بعدی: {int(suggested)} دقیقه\n\nآماده‌ای؟ 🟢"


class Advisor:
    async def analyze_user(self, user_id: int, session: AsyncSession) -> Dict[str, Any]:
        profile_repo = ProfileRepository(session)
        profile = await profile_repo.get_by_user_id(user_id)
        if not profile:
            return {"has_data": False, "message": "داده کافی برای تحلیل وجود ندارد. چند پارت مطالعه کن تا بتونم بهتر بشناسمت. 🌱"}

        insights = []
        if profile.completion_rate < 0.4:
            insights.append("📉 نرخ تکمیل پایین است. پیشنهاد می‌کنم پارت‌های کوتاه‌تر (۲۰-۳۰ دقیقه) رو امتحان کنی.")
        elif profile.completion_rate > 0.8:
            insights.append("🌟 نرخ تکمیل عالی! ادامه بده.")

        if profile.starting_friction_score > 0.6:
            insights.append("🔄 شروع مطالعه برایت سخت‌تر از معمول شده. پیشنهاد می‌کنم با پارت‌های ۱۵ دقیقه‌ای شروع کنی و کم‌کم افزایش بدی.")
        elif profile.starting_friction_score < 0.3:
            insights.append("💪 شروع مطالعه برات راحت است. می‌تونی پارت‌های بلندتر رو امتحان کنی.")

        if profile.consistency_score < 0.4:
            insights.append("📊 الگوی مطالعه‌ات نامنظم است. پیشنهاد می‌کنم هر روز حداقل یک پارت کوچک داشته باشی تا ریتمت حفظ بشه.")

        if profile.success_streak >= 3:
            insights.append(f"🔥 {profile.success_streak} پارت موفق پشت سر هم! عالی پیش می‌ری.")

        if profile.miss_streak >= 2:
            insights.append(f"🌱 {profile.miss_streak} پارت پشت سر هم از دست رفته. نگران نباش، با یک پارت کوتاه دوباره شروع کن.")

        if profile.preferred_hour:
            insights.append(f"⏰ بهترین زمان مطالعه‌ات حدود ساعت {profile.preferred_hour:02d}:00 است. سعی کن پارت‌های مهم رو در این زمان برنامه‌ریزی کنی.")

        if profile.current_load > 90:
            insights.append("⚖️ امروز زیاد مطالعه کردی. بهتره یه استراحت کوتاه داشته باشی و با یه پارت آروم ادامه بدی.")
        elif profile.current_load < 20 and profile.miss_streak == 0:
            insights.append("🌱 امروز کم‌تر از معمول مطالعه کردی. یه پارت کوچک می‌تونه ریتمت رو برگردونه.")

        # Generate a friendly message
        msg = "📊 تحلیل رفتار مطالعه:\n\n"
        if insights:
            for i, insight in enumerate(insights[:5], 1):
                msg += f"{insight}\n\n"
        else:
            msg += "📊 روند مطالعه‌ات پایدار است. ادامه بده! 🌱\n\n"

        trend = profile.trend
        if trend == "UPWARD":
            msg += "📈 روند کلی: رو به رشد! عالی پیش می‌ری."
        elif trend == "DOWNWARD":
            msg += "📉 روند کلی: کمی کاهش. نگران نباش، با قدم‌های کوچک برمی‌گردی."
        elif trend == "STABLE":
            msg += "📊 روند کلی: پایدار. این یعنی استمرار داری!"
        else:
            msg += "📊 روند کلی: قابل تغییر. تمرکز روی استمرار داشته باش."

        return {"has_data": True, "profile": profile, "insights": insights, "message": msg}

    async def get_recovery_suggestion(self, user_id: int, session: AsyncSession) -> str:
        profile_repo = ProfileRepository(session)
        profile = await profile_repo.get_by_user_id(user_id)
        if not profile:
            return "🌱 پیشنهاد می‌کنم با یک پارت ۱۵ دقیقه‌ای شروع کنی و کم‌کم افزایش بدی."
        current = profile.current_capacity
        comfort = profile.comfort_capacity
        suggested = min(15, current * 0.5, comfort * 0.6)
        suggested = max(10, suggested)
        suggested = min(config.CANDIDATE_DURATIONS, key=lambda x: abs(x - suggested))
        return (
            "🌱 حالت بازیابی (Recovery Mode)\n\n"
            f"چند پارت اخیر سخت‌تر از معمول بوده. این طبیعی‌ست!\n"
            f"پیشنهاد می‌کنم با یک پارت {int(suggested)} دقیقه‌ای شروع کنی و کم‌کم افزایش بدی.\n\n"
            "✨ هدف: بازگشت به ریتم، نه جبران افراطی.\n"
            "آماده‌ای؟"
        )

    async def get_growth_suggestion(self, user_id: int, session: AsyncSession) -> str:
        profile_repo = ProfileRepository(session)
        profile = await profile_repo.get_by_user_id(user_id)
        if not profile:
            return "🌱 چند پارت موفق پشت سر هم داشتی! می‌تونی پارت بعدی رو کمی افزایش بدی."
        current = profile.current_capacity
        challenge = profile.challenge_capacity
        suggested = min(current + 5, challenge, 60)
        suggested = max(15, suggested)
        suggested = min(config.CANDIDATE_DURATIONS, key=lambda x: abs(x - suggested))
        return (
            "🚀 حالت رشد (Growth Mode)\n\n"
            f"{profile.success_streak} پارت موفق پشت سر هم! 👏\n"
            f"ظرفیتت بهتر شده. پیشنهاد می‌کنم پارت بعدی رو به {int(suggested)} دقیقه افزایش بدی.\n\n"
            "✨ هدف: پیشرفت تدریجی و پایدار.\n"
            "آماده‌ای؟"
        )


class Teacher:
    """Simplified teacher role - will integrate with AI later."""
    async def explain(self, user_id: int, topic: str) -> str:
        return f"📚 در مورد {topic} توضیح می‌دم... (قابلیت AI فعال نیست)"


# ============================================================
# AI PROVIDER (Optional)
# ============================================================
class AIProvider:
    def __init__(self):
        self.provider = config.AI_PROVIDER
        self.api_key = config.AI_API_KEY
        self.enabled = bool(self.provider and self.api_key)

    async def generate_coaching_message(self, context: Dict[str, Any]) -> Optional[str]:
        if not self.enabled:
            return None
        # Placeholder - in production, integrate with OpenAI/DeepSeek API
        return None

    async def answer_question(self, user_id: int, question: str) -> Optional[str]:
        if not self.enabled:
            return None
        return None


# ============================================================
# HEALTH / BACKUP SERVICES
# ============================================================
class HealthService:
    def __init__(self):
        self.statuses = {
            "status": "ok",
            "bot": "running",
            "database": "unknown",
            "scheduler": "unknown",
            "telegram": "unknown",
            "time": None,
        }

    async def check(self) -> Dict[str, Any]:
        self.statuses["time"] = time_service.now().isoformat()
        # Check database
        try:
            async with get_session() as session:
                await session.execute(select(1))
            self.statuses["database"] = "ok"
        except:
            self.statuses["database"] = "error"
        self.statuses["scheduler"] = "running" if scheduler.running else "stopped"
        if all(v == "ok" or v == "running" for v in [self.statuses["database"], self.statuses["scheduler"]]):
            self.statuses["status"] = "ok"
        else:
            self.statuses["status"] = "degraded"
        return self.statuses


health_service = HealthService()


class BackupService:
    """Exports/imports the full database contents as a single JSON file.

    JSON was chosen (instead of just shipping the raw sqlite file) so
    backups are human-readable/portable and can be restored even if the
    underlying DB engine ever changes.
    """

    BACKUP_FORMAT_VERSION = "2.0.0"

    def __init__(self):
        self.backup_dir = Path(config.BACKUP_DIR)

    # ---------------- export ----------------

    @staticmethod
    def _serialize_value(value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        return value

    async def export_json(self) -> Optional[Path]:
        """Dump every table's rows into one JSON file and return its path."""
        try:
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            timestamp = time_service.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.backup_dir / f"backup_{timestamp}.json"

            tables_data: Dict[str, List[Dict[str, Any]]] = {}
            async with get_session() as session:
                for table in Base.metadata.sorted_tables:
                    result = await session.execute(select(table))
                    rows = result.mappings().all()
                    tables_data[table.name] = [
                        {col: self._serialize_value(val) for col, val in dict(row).items()}
                        for row in rows
                    ]

            payload = {
                "format_version": self.BACKUP_FORMAT_VERSION,
                "created_at": time_service.now().isoformat(),
                "table_order": [t.name for t in Base.metadata.sorted_tables],
                "tables": tables_data,
            }

            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

            self._cleanup_old_backups()
            logger.info(f"JSON backup created: {backup_path}")
            return backup_path
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return None

    # Kept for backward compatibility with any external code/cron calling
    # create_backup() directly -- now just an alias for the JSON export.
    async def create_backup(self) -> Optional[Path]:
        return await self.export_json()

    def _cleanup_old_backups(self):
        backups = self.list_backups()
        for old in backups[config.BACKUP_KEEP_LAST:]:
            try:
                old.unlink()
            except OSError as e:
                logger.error(f"Failed to remove old backup {old}: {e}")

    def list_backups(self) -> List[Path]:
        if not self.backup_dir.exists():
            return []
        files = [p for p in self.backup_dir.iterdir() if p.is_file() and p.suffix == ".json" and p.name.startswith("backup_")]
        return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)

    # ---------------- send to admins ----------------

    async def send_backup_to_admins(self, bot: "Bot", backup_path: Path, admin_ids: Optional[List[int]] = None) -> int:
        """Sends the backup JSON file as a document to every admin. Returns
        the number of admins it was successfully delivered to."""
        targets = admin_ids if admin_ids is not None else config.ADMIN_IDS
        sent = 0
        caption = f"📦 پشتیبان دیتابیس\n🕐 {time_service.now().strftime('%Y-%m-%d %H:%M')}"
        for admin_id in targets:
            try:
                await bot.send_document(
                    chat_id=admin_id,
                    document=FSInputFile(str(backup_path), filename=backup_path.name),
                    caption=caption,
                )
                sent += 1
            except Exception as e:
                logger.error(f"Failed to send backup to admin {admin_id}: {e}")
        return sent

    # ---------------- restore ----------------

    async def restore_from_json(self, backup_path: Path) -> Tuple[bool, str]:
        """Wipes and reloads every known table from a backup JSON file.
        Runs inside one transaction so a failure leaves the DB untouched."""
        try:
            with open(backup_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception as e:
            return False, f"❌ فایل پشتیبان قابل خواندن نیست: {e}"

        tables_data = payload.get("tables")
        if not isinstance(tables_data, dict):
            return False, "❌ ساختار فایل پشتیبان نامعتبر است."

        tables_by_name = {t.name: t for t in Base.metadata.sorted_tables}
        ordered_tables = [t for t in Base.metadata.sorted_tables if t.name in tables_data]

        try:
            async with get_session() as session:
                # Delete in reverse dependency order to respect FKs.
                for table in reversed(ordered_tables):
                    await session.execute(delete(table))
                # Insert in dependency order.
                restored_counts = {}
                for table in ordered_tables:
                    rows = tables_data.get(table.name, [])
                    if rows:
                        await session.execute(table.insert(), rows)
                    restored_counts[table.name] = len(rows)
            total_rows = sum(restored_counts.values())
            logger.info(f"Restore complete from {backup_path}: {restored_counts}")
            return True, f"✅ بازیابی موفق بود. {total_rows} رکورد در {len(ordered_tables)} جدول بازگردانی شد."
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            return False, f"❌ بازیابی با خطا مواجه شد: {e}"


backup_service = BackupService()

# ============================================================
# TELEGRAM BOT - ROUTER, DISPATCHER, HANDLERS
# ============================================================
router = Router()
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot: Optional[Bot] = None

# FSM States
class AdminStates(StatesGroup):
    ADD_MESSAGE = State()
    EDIT_MESSAGE = State()
    DELETE_MESSAGE = State()
    SET_ALGORITHM = State()
    SET_GROUP_SETTINGS = State()
    ADD_SENTENCE = State()
    SELECT_CATEGORY = State()
    SET_BACKUP_INTERVAL = State()
    RESTORE_UPLOAD = State()
    BROADCAST_NOTIFICATION = State()

class UserStates(StatesGroup):
    SET_DAILY_GOAL = State()
    SET_PREFERRED_DURATION = State()
    REPORT_ACTUAL = State()


# Middleware
class AuthMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable, event: Union[Message, CallbackQuery], data: Dict) -> Any:
        # Registered directly on the `message` and `callback_query`
        # observers (not as an outer/update-level middleware), so `event`
        # here is already a Message or a CallbackQuery -- both expose
        # `.from_user` directly. It is never an Update at this point.
        from_user = getattr(event, "from_user", None)
        if from_user:
            data["user_id"] = from_user.id
            data["is_admin"] = from_user.id in config.ADMIN_IDS
        return await handler(event, data)


class GroupSyncMiddleware(BaseMiddleware):
    """Runs before every message AND callback_query handler (registered on
    both observers below -- not just message ones).

    BUGFIX: this used to be registered on the message observer only. But
    when a session gets marked MISSED (nobody tapped "هستم" in the
    attendance window), the very first thing a person does afterwards is
    almost always tap the stale "هستم" button on the old poll message --
    a CallbackQuery, not a new text message. With this middleware absent
    from the callback_query chain, that tap never reached the
    `awaiting_activity` check below, so it never got consumed and
    `_create_readiness_poll` was never called: the group would just sit
    there with no new part ever sent, and every future tap on that same
    stale button would keep showing cb_ready's "❌ زمان ثبت نام گذشته"
    forever. Registering this on callback_query too means that first tap
    is exactly the activity that fires a fresh poll (with a fresh
    session/button) for the group, same as a real text message would.

    Ensures Group/GroupMember rows exist as a fallback whenever we see
    activity from a human in a group chat, then always lets the update
    continue to the real handler."""
    async def __call__(self, handler: Callable, event: Union[Message, CallbackQuery], data: Dict) -> Any:
        try:
            # A Message exposes `.chat` directly; a CallbackQuery only has
            # it via `.message.chat`, and `.message` can be plain None (or
            # an aiogram InaccessibleMessage) for an old/deleted message --
            # in that case there's no usable chat, so we just skip.
            chat = getattr(event, "chat", None)
            if chat is None:
                inner_message = getattr(event, "message", None)
                chat = getattr(inner_message, "chat", None)
            from_user = getattr(event, "from_user", None)
            if chat and chat.type in ("group", "supergroup") and \
               from_user and not from_user.is_bot:
                async with get_session() as session:
                    group_repo = GroupRepository(session)
                    group = await group_repo.get_or_create_group(chat.id, title=chat.title)
                    user_repo = UserRepository(session)
                    user = await user_repo.get_or_create(
                        telegram_id=from_user.id, username=from_user.username,
                        first_name=from_user.first_name, last_name=from_user.last_name,
                    )
                    await group_repo.add_member(user.id, group.id)

                    settings_result = await session.execute(
                        select(GroupSetting).where(GroupSetting.group_id == group.id)
                    )
                    settings = settings_result.scalar_one_or_none()
                    should_repoll = bool(settings and settings.awaiting_activity)

                if should_repoll:
                    # The group went quiet after an unanswered poll, and this
                    # is the first real activity since then -- exactly the
                    # trigger the user asked for, instead of polling on a
                    # blind timer. _create_readiness_poll() itself re-checks
                    # quiet hours / an already-active session, so this is
                    # safe even under a burst of messages/taps.
                    await _create_readiness_poll(group.id)
        except Exception as e:
            # Never let membership bookkeeping break the actual command.
            logger.error(f"GroupSyncMiddleware failed: {e}")
        return await handler(event, data)


router.message.middleware(AuthMiddleware())
router.message.middleware(GroupSyncMiddleware())
router.callback_query.middleware(AuthMiddleware())
router.callback_query.middleware(GroupSyncMiddleware())


# ============================================================
# KEYBOARDS
# ============================================================
def main_menu(is_admin: bool = False) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="📚 امروز", callback_data="menu:today"),
        InlineKeyboardButton(text="📊 گزارش", callback_data="menu:reports"),
    )
    kb.row(
        InlineKeyboardButton(text="🎯 هدف روزانه", callback_data="menu:goal"),
        InlineKeyboardButton(text="⚙️ تنظیمات", callback_data="menu:settings"),
    )
    if is_admin:
        kb.row(InlineKeyboardButton(text="🛠 پنل مدیریت", callback_data="menu:admin"))
    return kb.as_markup()


def attendance_keyboard(session_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🟢 هستم", callback_data=f"ready:{session_id}"))
    kb.row(InlineKeyboardButton(text="❌ نمی‌تونم", callback_data=f"decline:{session_id}"))
    return kb.as_markup()


def result_keyboard(session_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="🟢 کامل", callback_data=f"result:{session_id}:COMPLETED"),
        InlineKeyboardButton(text="🟡 نصفه", callback_data=f"result:{session_id}:PARTIAL"),
        InlineKeyboardButton(text="🔴 انجام ندادم", callback_data=f"result:{session_id}:MISSED"),
    )
    kb.row(InlineKeyboardButton(text="⏱ ثبت مدت واقعی", callback_data=f"result:{session_id}:ACTUAL"))
    return kb.as_markup()


def admin_panel() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="👥 کاربران", callback_data="admin:users"),
        InlineKeyboardButton(text="📚 جلسات", callback_data="admin:sessions"),
    )
    kb.row(
        InlineKeyboardButton(text="📊 آمار", callback_data="admin:stats"),
        InlineKeyboardButton(text="💬 پیام‌ها", callback_data="admin:messages"),
    )
    kb.row(
        InlineKeyboardButton(text="🧠 الگوریتم", callback_data="admin:algorithm"),
        InlineKeyboardButton(text="🔔 اعلان‌ها", callback_data="admin:notifications"),
    )
    kb.row(
        InlineKeyboardButton(text="🛠 سیستم", callback_data="admin:system"),
        InlineKeyboardButton(text="💾 پشتیبان", callback_data="admin:backup"),
    )
    kb.row(
        InlineKeyboardButton(text="⏱ فاصله پشتیبان‌گیری", callback_data="admin:backup_interval"),
        InlineKeyboardButton(text="♻️ بازیابی", callback_data="admin:restore"),
    )
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


def backup_menu_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📥 دریافت پشتیبان همین الان", callback_data="admin:backup"))
    kb.row(InlineKeyboardButton(text="⏱ تغییر فاصله زمانی", callback_data="admin:backup_interval"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


def restore_menu_keyboard(backups: List[Path]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for path in backups[:10]:
        label = path.stem.replace("backup_", "")
        kb.row(InlineKeyboardButton(text=f"🗂 {label}", callback_data=f"admin:restore:pick:{path.name}"))
    kb.row(InlineKeyboardButton(text="📤 آپلود فایل پشتیبان", callback_data="admin:restore:upload"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


def restore_confirm_keyboard(filename: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="⚠️ تایید و بازیابی", callback_data=f"admin:restore:confirm:{filename}"),
        InlineKeyboardButton(text="❌ انصراف", callback_data="admin:restore"),
    )
    return kb.as_markup()


# ------------------------------------------------------------
# Message categories -- glass-style chip picker
# Each entry: (code stored in DB, emoji, Persian label)
# ------------------------------------------------------------
MESSAGE_CATEGORIES: List[Tuple[str, str, str]] = [
    ("SESSION_START", "🟢", "شروع جلسه"),
    ("SESSION_END", "🏁", "پایان جلسه"),
    ("SUCCESS", "✅", "موفقیت"),
    ("PARTIAL", "🟡", "انجام نصفه"),
    ("MISSED", "🔴", "جا افتادن"),
    ("RECOVERY", "💪", "بازگشت به مسیر"),
    ("STREAK", "🔥", "استریک"),
    ("MOTIVATION", "✨", "انگیزشی"),
    ("MORNING", "☀️", "صبح بخیر"),
    ("GENERAL", "💬", "عمومی"),
]
MESSAGE_CATEGORY_LABELS: Dict[str, str] = {code: f"{emoji} {label}" for code, emoji, label in MESSAGE_CATEGORIES}

# Ready-made sample templates the admin can load per category with one tap
# (see cb_admin_msg_seed) instead of typing everything by hand.
SAMPLE_MESSAGES: Dict[str, List[str]] = {
    "SESSION_START": [
        "وقتشه! یه نفس عمیق بکش و شروع کن، من همراهتم 💪",
        "زمان مطالعه رسید. فقط همین یه قدم رو بردار 🚀",
        "بزن بریم! تمرکز کن روی همین چند دقیقه‌ی جلو 🎯",
    ],
    "SESSION_END": [
        "آفرین که تمومش کردی! یه استراحت کوچیک بهت میاد 🌿",
        "جلسه تموم شد، حالا کمی نفس بکش و به خودت افتخار کن 👏",
        "تمام شد! هر بار همینطوری پله‌پله جلو میری 🪜",
    ],
    "SUCCESS": [
        "دقیقاً همونی شد که برنامه‌ریزی کرده بودی، عالیه! 🎉",
        "این یعنی داری به هدفت نزدیک‌تر میشی، همینطور ادامه بده ✨",
        "یه موفقیت دیگه به لیستت اضافه شد 🏆",
    ],
    "PARTIAL": [
        "نصفه هم که باشه بازم یه قدمه، ادامه‌ش بده 🙂",
        "کامل نشد ولی شروع کردی، دفعه بعد کاملش می‌کنی 💫",
        "هرچقدر هم که پیش رفتی، حساب میشه 👍",
    ],
    "MISSED": [
        "امروز نشد، مهم نیست. فردا از همینجا ادامه می‌دیم 🌱",
        "یه جلسه رو از دست دادی، نه کل مسیر رو. برگرد سر برنامه 🔄",
        "اتفاق می‌افته، مهم اینه که ادامه بدی نه اینکه تسلیم بشی 💙",
    ],
    "RECOVERY": [
        "دوباره برگشتی، همین یعنی داری درست پیش میری 🔥",
        "برگشت به مسیر از خودِ مسیر رفتن مهم‌تره 👊",
        "یک قدم عقب، دو قدم جلو. ادامه بده 🚶",
    ],
    "STREAK": [
        "چند روزه پشت‌سرهم داری میزنی، این خیلی قویه 🔥",
        "استریکت داره طولانی‌تر میشه، نذار بشکنه! ⚡",
        "همینطور روز به روز بیشتر بشو، عالی پیش میری 📈",
    ],
    "MOTIVATION": [
        "هر جلسه‌ای که میذاری، نسخه بهتری از خودت میسازی ✨",
        "نتیجه یه‌شبه نمیاد ولی هر روز داره جمع میشه 🌟",
        "تو همینجا، همین الان داری فرق ایجاد می‌کنی 💫",
    ],
    "MORNING": [
        "صبح بخیر! امروز رو با یه شروع خوب باز کن ☀️",
        "روز جدید، فرصت جدید. آماده‌ای؟ 🌅",
        "یه صبح تازه‌ست، بذار اولین قدم امروزت باشه 🌤",
    ],
    "GENERAL": [
        "یادت باشه پیشرفت همیشه خطی نیست، ادامه بده 🙂",
        "هر قدم کوچیک هم بخشی از مسیره 🛤",
        "من اینجام تا کنارت باشم توی این مسیر 🤝",
    ],
}


def category_picker_keyboard(
    prefix: str,
    back_callback: str = "admin:messages",
    counts: Optional[Dict[str, int]] = None,
) -> InlineKeyboardMarkup:
    """Glass-chip grid (2 per row) for picking a message category."""
    kb = InlineKeyboardBuilder()
    row_buttons: List[InlineKeyboardButton] = []
    for code, emoji, label in MESSAGE_CATEGORIES:
        text = f"{emoji} {label}"
        if counts is not None:
            text += f" ({counts.get(code, 0)})"
        row_buttons.append(InlineKeyboardButton(text=text, callback_data=f"{prefix}:{code}"))
        if len(row_buttons) == 2:
            kb.row(*row_buttons)
            row_buttons = []
    if row_buttons:
        kb.row(*row_buttons)
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data=back_callback))
    return kb.as_markup()


def message_management() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="➕ افزودن پیام", callback_data="admin:msg:add"),
        InlineKeyboardButton(text="📁 مرور بر اساس دسته‌بندی", callback_data="admin:msg:categories"),
    )
    kb.row(
        InlineKeyboardButton(text="🧪 افزودن نمونه‌های آماده", callback_data="admin:msg:seed"),
        InlineKeyboardButton(text="📊 آمار پیام‌ها", callback_data="admin:msg:stats"),
    )
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


# ------------------------------------------------------------
# Notification preferences -- glass-chip toggle list
# ------------------------------------------------------------
NOTIFICATION_PREF_FIELDS: List[Tuple[str, str]] = [
    ("morning_notifications", "☀️ یادآور صبحگاهی"),
    ("session_notifications", "📚 اعلان شروع/پایان جلسه"),
    ("private_interventions", "💬 پیام‌های خصوصی انگیزشی"),
    ("daily_reports", "📅 گزارش روزانه"),
    ("weekly_reports", "🗓 گزارش هفتگی"),
    ("motivational_messages", "✨ پیام‌های انگیزشی"),
]


def notification_settings_keyboard(prefs: "UserPreference") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for field_name, label in NOTIFICATION_PREF_FIELDS:
        enabled = getattr(prefs, field_name)
        icon = "🟢" if enabled else "⚪️"
        kb.row(InlineKeyboardButton(text=f"{icon} {label}", callback_data=f"settings:notif:toggle:{field_name}"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu:settings"))
    return kb.as_markup()


def admin_notifications_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📢 ارسال اعلان به همه کاربران", callback_data="admin:notif:broadcast"))
    kb.row(InlineKeyboardButton(text="🔄 به‌روزرسانی آمار", callback_data="admin:notifications"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


def settings_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="🎯 هدف روزانه", callback_data="settings:goal"),
        InlineKeyboardButton(text="⏱ مدت ترجیحی", callback_data="settings:duration"),
    )
    kb.row(
        InlineKeyboardButton(text="🔔 اعلان‌ها", callback_data="settings:notifications"),
        InlineKeyboardButton(text="🌙 حالت آرامش", callback_data="settings:calm"),
    )
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="settings:back"))
    return kb.as_markup()


# ============================================================
# COMMAND HANDLERS
# ============================================================
@router.message(CommandStart())
async def cmd_start(message: Message):
    async with get_session() as session:
        repo = UserRepository(session)
        await repo.get_or_create(
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )
    is_admin = message.from_user.id in config.ADMIN_IDS
    await message.answer(
        f"سلام {message.from_user.first_name or 'دوست عزیز'}! 👋\nمن Study Coach هستم، همراه مطالعاتی هوشمندت.\n\n"
        f"🌱 من اینجام تا بهت کمک کنم با آرامش و استمرار پیشرفت کنی.\n"
        f"📚 با هم پارت‌های مطالعاتی رو برنامه‌ریزی می‌کنیم و ازت یاد می‌گیرم.\n\n"
        f"منتظر شروع پارت بعدی باش! 🚀",
        reply_markup=main_menu(is_admin)
    )


@router.my_chat_member()
async def on_bot_membership_changed(event: ChatMemberUpdated):
    """Fires when the bot itself is added to / removed from / promoted in a
    group. This is the only reliable place to create the Group row -- the
    base prototype had no handler for this at all, so Group/GroupMember rows
    were never created and every group flow was dead on arrival."""
    if event.chat.type not in ("group", "supergroup"):
        return
    new_status = event.new_chat_member.status
    async with get_session() as session:
        group_repo = GroupRepository(session)
        if new_status in ("member", "administrator"):
            await group_repo.get_or_create_group(event.chat.id, title=event.chat.title)
            logger.info(f"Bot added to group {event.chat.id} ({event.chat.title})")
        elif new_status in ("left", "kicked"):
            await group_repo.deactivate_group(event.chat.id)
            logger.info(f"Bot removed from group {event.chat.id}")


@router.chat_member()
async def on_member_status_changed(event: ChatMemberUpdated):
    """Tracks human members joining/leaving a group the bot is in, so
    GroupMember rows stay accurate even for people who never DM the bot."""
    if event.chat.type not in ("group", "supergroup"):
        return
    if event.new_chat_member.user.is_bot:
        return
    async with get_session() as session:
        group_repo = GroupRepository(session)
        group = await group_repo.get_or_create_group(event.chat.id, title=event.chat.title)
        user_repo = UserRepository(session)
        tg_user = event.new_chat_member.user
        user = await user_repo.get_or_create(
            telegram_id=tg_user.id, username=tg_user.username,
            first_name=tg_user.first_name, last_name=tg_user.last_name,
        )
        status = event.new_chat_member.status
        if status in ("member", "administrator", "creator", "restricted"):
            await group_repo.add_member(user.id, group.id)
        elif status in ("left", "kicked"):
            await group_repo.deactivate_member(user.id, group.id)


@router.message(Command("session"))
async def cmd_session(message: Message):
    async with get_session() as session:
        # Resolve the internal User row first -- GroupMember.user_id is the
        # internal PK, not the Telegram id, so we must never compare it
        # directly against message.from_user.id (that was a bug in the
        # original: the membership lookup basically never matched).
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer("❌ لطفاً ابتدا دستور /start را بزنید.")
            return

        group_repo = GroupRepository(session)
        groups = await group_repo.get_user_groups(user.id)
        if not groups:
            await message.answer("❌ شما عضو هیچ گروه مطالعاتی نیستید.\nلطفاً ربات را به یک گروه اضافه کنید و در آن گروه پیامی بفرستید تا عضویتتان ثبت شود.")
            return

        # If the user is in multiple groups, use the most recently active one.
        # (A dedicated group-picker is a natural follow-up, but a session
        # command needs a single deterministic default today.)
        group_id = groups[0].id
        now = time_service.now()
        # start_time must not be earlier than the attendance window's own
        # close time (timeout_at). It used to be now+2min while the
        # attendance timeout was now+3min, which guaranteed the scheduled
        # `_session_start` job fired while the session was still in
        # "ATTENDANCE" (not yet "READY"), silently no-oping and leaving
        # the session permanently stuck -- which then blocked every future
        # automatic "کیا هستن؟" poll for the group. Aligning them the same
        # way `_create_readiness_poll` does removes that guaranteed race.
        window_minutes = config.DEFAULT_ATTENDANCE_WINDOW / 60
        start_time = now + timedelta(minutes=window_minutes)

        # Use recommendation engine. At creation time we only know the
        # requester's intent -- the real, volunteer-based recommendation is
        # recalculated in _attendance_timeout() once we know who actually
        # said "هستم". This initial figure is just a placeholder for the
        # invite message.
        engine = RecommendationEngine()
        rec = await engine.recommend(
            group_id=group_id,
            participant_ids=[user.id],
            current_time=now,
            session=session
        )
        # Placeholder only -- _attendance_timeout() replaces this job with
        # one based on the real, volunteer-based `extension` once known.
        end_time = start_time + timedelta(minutes=rec["target"])

        manager = SessionManager(session)
        sess = await manager.create_session(
            group_id=group_id,
            scheduled_start=start_time,
            scheduled_end=end_time,
            planned_duration=rec["target"],
            minimum_duration=rec["minimum"],
            target_duration=rec["target"],
            extension_duration=rec["extension"],
            mode=rec["mode"],
            creation_source="user_request",
            recommendation_confidence=rec["confidence"],
            reason_codes=rec["reason_codes"],
        )
        sess.status = "ATTENDANCE"
        await session.flush()

        # Schedule attendance timeout
        timeout_at = now + timedelta(seconds=config.DEFAULT_ATTENDANCE_WINDOW)
        await SchedulerService.schedule_job(
            "attendance_timeout", timeout_at,
            {"session_id": sess.id}, db_session=session
        )

        # Schedule session start and end (in case no one clicks start)
        await SchedulerService.schedule_job(
            "session_start", start_time,
            {"session_id": sess.id}, db_session=session
        )
        await SchedulerService.schedule_job(
            "session_end", end_time,
            {"session_id": sess.id}, db_session=session
        )

        await message.answer(
            f"📚 پارت مطالعاتی جدید!\n\n"
            f"🕐 زمان: {time_service.format_datetime(start_time)}\n"
            f"🎯 هدف: {rec['target']} دقیقه\n"
            f"🌱 حداقل: {rec['minimum']} دقیقه\n"
            f"🔥 حداکثر: {rec['extension']} دقیقه\n\n"
            f"چه کسانی هستن؟ 🟢",
            reply_markup=attendance_keyboard(sess.id)
        )


@router.message(Command("today"))
async def cmd_today(message: Message):
    async with get_session() as session:
        assistant = Assistant()
        status = await assistant.get_today_status(message.from_user.id, session)
        await message.answer(status["message"], reply_markup=main_menu())


@router.message(Command("menu"))
async def cmd_menu(message: Message):
    is_admin = message.from_user.id in config.ADMIN_IDS
    await message.answer("📋 منوی اصلی:", reply_markup=main_menu(is_admin))


@router.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "🤖 راهنمای Study Coach\n\n"
        "📚 دستورات:\n"
        "/start - شروع کار با ربات\n"
        "/menu - منوی اصلی\n"
        "/today - گزارش امروز\n"
        "/session - ایجاد پارت جدید\n"
        "/help - این راهنما\n\n"
        "🌱 اصول من:\n"
        "• آرامش و استمرار\n"
        "• پیشرفت تدریجی\n"
        "• همراهی بدون قضاوت\n"
        "• تنظیم هوشمند بر اساس رفتار تو\n\n"
        "سوالی داری؟ بپرس! 💬"
    )


@router.message(Command("goal"))
async def cmd_goal(message: Message):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer("❌ لطفاً /start را بزنید.")
            return
        result = await session.execute(
            select(UserGoal).where(UserGoal.user_id == user.id)
        )
        goal = result.scalar_one_or_none()
        if goal:
            calc = StatisticsCalculator()
            stats = await calc.get_today_stats(user.id, session)
            minutes = stats.get("minutes", 0) if stats else 0
            await message.answer(
                f"🎯 هدف روزانه: {goal.daily_goal} دقیقه\n"
                f"📊 پیشرفت امروز: {minutes} دقیقه\n"
                f"📈 درصد: {min(100, int(minutes/goal.daily_goal*100))}%\n\n"
                f"برای تغییر هدف، از منوی تنظیمات استفاده کن."
            )
        else:
            await message.answer("🎯 هنوز هدفی تعیین نکردی. از منوی تنظیمات هدف روزانه‌ات رو مشخص کن.")


# ============================================================
# CALLBACK HANDLERS
# ============================================================
@router.callback_query(F.data.startswith("ready:"))
async def cb_ready(callback: CallbackQuery):
    try:
        session_id = int(callback.data.split(":")[1])
    except:
        await callback.answer("❌ خطا")
        return
    user_id = callback.from_user.id

    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(user_id)
        if not user:
            await callback.answer("❌ ابتدا /start را بزنید.")
            return
        manager = SessionManager(session)
        sess = await manager.get_session(session_id)
        if not sess or sess.status not in ["SCHEDULED", "ATTENDANCE"]:
            # This tap itself just ran through GroupSyncMiddleware, which
            # (if the group was `awaiting_activity` from an unanswered
            # poll) already queued a brand-new poll/session in the
            # background -- so don't leave the person thinking they missed
            # their chance entirely; point them at the new one instead.
            await callback.answer(
                "❌ زمان ثبت نام این پارت گذشته. یه پارت جدید داره فرستاده می‌شه، "
                "چند لحظه صبر کن و از پیام جدید ثبت‌نام کن.",
                show_alert=True,
            )
            return
        participant = await manager.register_participant(session_id, user.id)
        if participant:
            participants = await manager.get_participants(session_id)
            count = len(participants)
            part_number = await manager.get_group_part_number(sess.group_id, session_id)
            minutes_left = round(
                (time_service.to_tehran(sess.scheduled_start) - time_service.now()).total_seconds() / 60
            )
            minutes_left = max(0, minutes_left)
            start_clock = time_service.format_datetime(
                time_service.to_tehran(sess.scheduled_start), fmt="%H:%M"
            )
            # The scheduled start time only matters as new information the
            # very first time someone taps "هستم" -- that's the moment it
            # actually gets decided (see _create_readiness_poll /
            # cmd_session, both now on a 3-5 minute window). Every
            # subsequent tap just bumps the headcount, so keep the time
            # line visible but don't reintroduce it as if it were news.
            time_line = f"⏰ پارت ساعت {start_clock} شروع می‌شه (حدود {minutes_left} دقیقه دیگه)\n"
            await callback.message.edit_text(
                f"📚 پارت #{part_number}\n"
                f"{time_line}"
                f"✅ {user.first_name or 'کاربر'} اعلام آمادگی کرد!\n"
                f"👥 تعداد آمادگان: {count}",
                reply_markup=attendance_keyboard(session_id)
            )
            await callback.answer(
                f"✅ ثبت شد! پارتت حدود {minutes_left} دقیقه دیگه توی گروه شروع می‌شه.",
                show_alert=True,
            )
        else:
            await callback.answer("❌ قبلاً ثبت نام کرده‌اید.")


@router.callback_query(F.data.startswith("decline:"))
async def cb_decline(callback: CallbackQuery):
    """A tap on "نیستم" is itself an intent signal (negative), and the
    original prototype threw it away entirely. We record it as a
    BehaviorEvent so the intent score used by the recommendation engine
    reflects real declines, not just silence."""
    try:
        session_id = int(callback.data.split(":")[1])
    except (IndexError, ValueError):
        await callback.answer("❌ خطا")
        return
    async with get_session() as session:
        user_repo = UserRepository(session)
        user = await user_repo.get_by_telegram_id(callback.from_user.id)
        if user:
            event = BehaviorEvent(
                user_id=user.id,
                event_type="USER_DECLINED",
                event_meta={"session_id": session_id},
            )
            session.add(event)
            await session.flush()
    await callback.answer("❌ ثبت نشد. پارت بعدی منتظرته!")


@router.callback_query(F.data.startswith("result:"))
async def cb_result(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer("❌ خطا")
        return
    session_id = int(parts[1])
    status = parts[2].upper()

    if status == "ACTUAL":
        user_id = callback.from_user.id
        async with get_session() as session:
            repo = UserRepository(session)
            user = await repo.get_by_telegram_id(user_id)
            if not user:
                await callback.answer("❌ ابتدا /start را بزنید.")
                return
            existing = await session.execute(
                select(SessionResult).where(
                    SessionResult.session_id == session_id,
                    SessionResult.user_id == user.id,
                )
            )
            if existing.scalar_one_or_none():
                await callback.answer("⏱ قبلاً برای این پارت ثبت کرده‌ای.", show_alert=True)
                return
        await state.update_data(session_id=session_id)
        await callback.message.answer("⏱ لطفاً مدت واقعی مطالعه را به دقیقه وارد کنید:")
        await state.set_state(UserStates.REPORT_ACTUAL)
        await callback.answer()
        return

    user_id = callback.from_user.id
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(user_id)
        if not user:
            await callback.answer("❌ ابتدا /start را بزنید.")
            return
        manager = SessionManager(session)
        success = await manager.report_result(session_id, user.id, status)
        should_finish_early = False
        if success:
            # No group-visible "ثبت شد" message anymore -- with several
            # people reporting per part this was cluttering the group
            # chat. Confirmation now happens only via the popup below.
            # Show one of the category-specific messages (the ones added
            # from the admin panel, e.g. under "PARTIAL") in the popup --
            # this was previously never called anywhere, so the popup
            # always just showed the generic "✅ ثبت شد!" instead.
            popup_text = await message_selector.get_message(session, status, user.id)
            await callback.answer(popup_text or "✅ ثبت شد!", show_alert=bool(popup_text))

            # Even people who never tapped "هستم" can report completion (per
            # design), but only the READY participants count toward closing
            # the part out early -- a bonus report from someone outside the
            # original headcount shouldn't itself end the session.
            sess = await manager.get_session(session_id)
            participants = await manager.get_participants(session_id)
            ready_ids = {p.user_id for p in participants if p.attendance_status == "READY"}
            if sess and sess.status == "STARTED" and ready_ids:
                reported_result = await session.execute(
                    select(SessionResult.user_id).where(SessionResult.session_id == session_id)
                )
                reported_ids = {row[0] for row in reported_result.all()}
                should_finish_early = ready_ids.issubset(reported_ids)
        else:
            await callback.answer("❌ قبلاً برای این پارت ثبت کرده‌ای.", show_alert=True)

    if should_finish_early:
        await _finish_session_early(session_id)


async def _delete_message_later(message: Message, delay_seconds: int = 5):
    """Best-effort auto-delete for confirmation messages we don't want
    sitting permanently in a busy group chat (e.g. typed duration
    confirmations, which have no popup alternative since they aren't a
    button callback). Missing delete permission shouldn't raise."""
    await asyncio.sleep(delay_seconds)
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"Could not auto-delete message {message.message_id}: {e}")


@router.message(UserStates.REPORT_ACTUAL)
async def report_actual_duration(message: Message, state: FSMContext):
    try:
        duration = int(message.text)
        if duration < 1 or duration > 480:
            await message.answer("❌ مدت باید بین 1 تا 480 دقیقه باشد.")
            return
        data = await state.get_data()
        session_id = data.get("session_id")
        if not session_id:
            await message.answer("❌ خطا در یافتن جلسه.")
            await state.clear()
            return
        user_id = message.from_user.id
        async with get_session() as session:
            repo = UserRepository(session)
            user = await repo.get_by_telegram_id(user_id)
            if not user:
                await message.answer("❌ لطفاً /start را بزنید.")
                await state.clear()
                return
            manager = SessionManager(session)
            success = await manager.report_result(session_id, user.id, "COMPLETED", duration)
            should_finish_early = False
            if success:
                # Auto-deletes after a few seconds instead of sitting
                # permanently in the group -- there's no popup option here
                # since this comes from a typed reply, not a button tap.
                confirm_msg = await message.answer(f"✅ مدت {duration} دقیقه ثبت شد!")
                asyncio.create_task(_delete_message_later(confirm_msg, delay_seconds=5))
                sess = await manager.get_session(session_id)
                participants = await manager.get_participants(session_id)
                ready_ids = {p.user_id for p in participants if p.attendance_status == "READY"}
                if sess and sess.status == "STARTED" and ready_ids:
                    reported_result = await session.execute(
                        select(SessionResult.user_id).where(SessionResult.session_id == session_id)
                    )
                    reported_ids = {row[0] for row in reported_result.all()}
                    should_finish_early = ready_ids.issubset(reported_ids)
            else:
                await message.answer("❌ خطا در ثبت مدت.")
        if should_finish_early:
            await _finish_session_early(session_id)
        await state.clear()
    except ValueError:
        await message.answer("❌ لطفاً یک عدد معتبر وارد کنید.")


@router.callback_query(F.data == "menu:today")
async def cb_menu_today(callback: CallbackQuery):
    await cmd_today(callback.message)
    await callback.answer()


@router.callback_query(F.data == "menu:reports")
async def cb_menu_reports(callback: CallbackQuery):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.message.answer("❌ لطفاً ابتدا /start را بزنید.")
            await callback.answer()
            return
        calc = StatisticsCalculator()
        weekly = await calc.get_weekly_stats(user.id, session)
        daily = await calc.get_today_stats(user.id, session)
        msg = "📊 گزارشات:\n\n"
        if daily:
            msg += f"📅 امروز: {daily['minutes']} دقیقه، {daily['sessions']} جلسه\n"
        else:
            msg += "📅 امروز: بدون جلسه\n"
        if weekly:
            msg += f"📆 این هفته: {weekly['minutes']} دقیقه، {weekly['sessions']} جلسه\n"
            msg += f"📈 نرخ تکمیل: {weekly['completion_rate']*100:.0f}%\n"
            msg += f"🔄 پایداری: {weekly['consistency']*100:.0f}%\n"
            msg += f"📊 روند: {weekly['trend']}"
        else:
            msg += "📆 این هفته: بدون داده"
        await callback.message.edit_text(msg, reply_markup=main_menu())
    await callback.answer()


@router.callback_query(F.data == "menu:goal")
async def cb_menu_goal(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("🎯 لطفاً هدف روزانه خود را به دقیقه وارد کنید (مثلاً 90):")
    await state.set_state(UserStates.SET_DAILY_GOAL)
    await callback.answer()


@router.message(UserStates.SET_DAILY_GOAL)
async def set_daily_goal(message: Message, state: FSMContext):
    try:
        goal = int(message.text)
        if goal < 10 or goal > 600:
            await message.answer("❌ هدف باید بین 10 تا 600 دقیقه باشد.")
            return
        async with get_session() as session:
            repo = UserRepository(session)
            user = await repo.get_by_telegram_id(message.from_user.id)
            if not user:
                await message.answer("❌ لطفاً /start را بزنید.")
                await state.clear()
                return
            result = await session.execute(
                select(UserGoal).where(UserGoal.user_id == user.id)
            )
            goal_obj = result.scalar_one_or_none()
            if goal_obj:
                goal_obj.daily_goal = goal
            else:
                goal_obj = UserGoal(user_id=user.id, daily_goal=goal)
                session.add(goal_obj)
            await session.commit()
        await message.answer(f"✅ هدف روزانه به {goal} دقیقه تنظیم شد! 🎯")
        await state.clear()
    except ValueError:
        await message.answer("❌ لطفاً یک عدد معتبر وارد کنید.")


@router.callback_query(F.data == "menu:settings")
async def cb_menu_settings(callback: CallbackQuery):
    await callback.message.edit_text("⚙️ تنظیمات:", reply_markup=settings_keyboard())
    await callback.answer()


@router.callback_query(F.data == "settings:back")
async def cb_settings_back(callback: CallbackQuery):
    is_admin = callback.from_user.id in config.ADMIN_IDS
    await callback.message.edit_text("📋 منوی اصلی:", reply_markup=main_menu(is_admin))
    await callback.answer()


@router.callback_query(F.data == "settings:goal")
async def cb_settings_goal(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("🎯 هدف روزانه جدید را به دقیقه وارد کنید:")
    await state.set_state(UserStates.SET_DAILY_GOAL)
    await callback.answer()


@router.callback_query(F.data == "settings:duration")
async def cb_settings_duration(callback: CallbackQuery):
    await callback.message.answer("⏱ مدت ترجیحی هر پارت را به دقیقه وارد کنید (مثلاً 45):")
    # Could use a state, but we'll keep simple
    await callback.answer("این قابلیت در حال توسعه است.", show_alert=True)


@router.callback_query(F.data == "settings:notifications")
async def cb_settings_notifications(callback: CallbackQuery):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ کاربر پیدا نشد.", show_alert=True)
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        if not prefs:
            prefs = UserPreference(user_id=user.id)
            session.add(prefs)
            await session.flush()
        await callback.message.edit_text(
            "🔔 اعلان‌هایی که می‌خوای دریافت کنی رو انتخاب کن (با لمس، روشن/خاموش میشه):",
            reply_markup=notification_settings_keyboard(prefs),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("settings:notif:toggle:"))
async def cb_settings_notif_toggle(callback: CallbackQuery):
    field_name = callback.data.split(":")[-1]
    valid_fields = {f for f, _ in NOTIFICATION_PREF_FIELDS}
    if field_name not in valid_fields:
        await callback.answer("❌ گزینه نامعتبر", show_alert=True)
        return
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ کاربر پیدا نشد.", show_alert=True)
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        if not prefs:
            prefs = UserPreference(user_id=user.id)
            session.add(prefs)
            await session.flush()
        setattr(prefs, field_name, not getattr(prefs, field_name))
        new_value = getattr(prefs, field_name)
        await session.commit()
        await callback.message.edit_reply_markup(reply_markup=notification_settings_keyboard(prefs))
    await callback.answer("🟢 روشن شد" if new_value else "⚪️ خاموش شد")


@router.callback_query(F.data == "menu:admin")
async def cb_menu_admin(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!", show_alert=True)
        return
    await callback.message.edit_text("🛠 پنل مدیریت:", reply_markup=admin_panel())
    await callback.answer()


# --- Admin callbacks ---
@router.callback_query(F.data == "admin:back")
async def cb_admin_back(callback: CallbackQuery):
    is_admin = callback.from_user.id in config.ADMIN_IDS
    await callback.message.edit_text("📋 منوی اصلی:", reply_markup=main_menu(is_admin))
    await callback.answer()


@router.callback_query(F.data == "admin:users")
async def cb_admin_users(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    async with get_session() as session:
        result = await session.execute(select(User).limit(50))
        users = result.scalars().all()
        text = "👥 کاربران (آخرین ۵۰):\n\n"
        for u in users:
            text += f"@{u.username or 'no_username'} - {u.first_name or ''} {u.last_name or ''}\n"
        await callback.message.edit_text(text, reply_markup=admin_panel())
    await callback.answer()


@router.callback_query(F.data == "admin:sessions")
async def cb_admin_sessions(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    async with get_session() as session:
        result = await session.execute(select(Session).order_by(desc(Session.created_at)).limit(20))
        sessions = result.scalars().all()
        text = "📚 جلسات اخیر:\n\n"
        for s in sessions:
            text += f"#{s.id} - {s.status} - {s.mode} - {s.target_duration}min\n"
        await callback.message.edit_text(text, reply_markup=admin_panel())
    await callback.answer()


@router.callback_query(F.data == "admin:stats")
async def cb_admin_stats(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    async with get_session() as session:
        users_count = (await session.execute(select(func.count()).select_from(User))).scalar()
        sessions_count = (await session.execute(select(func.count()).select_from(Session))).scalar()
        messages_count = (await session.execute(select(func.count()).select_from(MessageTemplate))).scalar()
        text = (
            f"📊 آمار سیستم:\n\n"
            f"👥 کاربران: {users_count}\n"
            f"📚 جلسات: {sessions_count}\n"
            f"💬 پیام‌ها: {messages_count}\n"
            f"🕐 زمان تهران: {time_service.now().strftime('%Y-%m-%d %H:%M')}"
        )
        await callback.message.edit_text(text, reply_markup=admin_panel())
    await callback.answer()


@router.callback_query(F.data == "admin:messages")
async def cb_admin_messages(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    await callback.message.edit_text("💬 مدیریت پیام‌ها:", reply_markup=message_management())
    await callback.answer()


@router.callback_query(F.data == "admin:msg:add")
async def cb_admin_msg_add(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    await callback.message.edit_text(
        "📝 اول دسته‌بندی پیام رو انتخاب کن:",
        reply_markup=category_picker_keyboard(prefix="admin:msg:pick", back_callback="admin:messages"),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:msg:pick:"))
async def cb_admin_msg_pick_category(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    category = callback.data.split(":")[-1]
    await state.update_data(category=category)
    await state.set_state(AdminStates.ADD_MESSAGE)
    label = MESSAGE_CATEGORY_LABELS.get(category, category)
    await callback.message.edit_text(f"📝 متن پیام رو برای دسته‌بندی «{label}» بفرست:")
    await callback.answer()


@router.message(AdminStates.ADD_MESSAGE)
async def admin_add_message(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    data = await state.get_data()
    category = data.get("category")
    text = (message.text or "").strip()
    if not category:
        await message.answer("❌ دسته‌بندی مشخص نیست، دوباره از منوی پیام‌ها شروع کن.")
        await state.clear()
        return
    if not text:
        await message.answer("❌ متن پیام نمی‌تونه خالی باشه. دوباره بفرست:")
        return
    async with get_session() as session:
        repo = MessageRepository(session)
        await repo.create_message(category, text, created_by=message.from_user.id)
        await session.commit()
        label = MESSAGE_CATEGORY_LABELS.get(category, category)
        await message.answer(f"✅ پیام به دسته‌بندی «{label}» اضافه شد!", reply_markup=message_management())
    await state.clear()


@router.callback_query(F.data == "admin:msg:categories")
async def cb_admin_msg_categories(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    async with get_session() as session:
        result = await session.execute(
            select(MessageTemplate.category, func.count(MessageTemplate.id)).group_by(MessageTemplate.category)
        )
        counts = {category: count for category, count in result.all()}
        await callback.message.edit_text(
            "📁 یه دسته‌بندی رو انتخاب کن تا پیام‌هاش رو ببینی:",
            reply_markup=category_picker_keyboard(
                prefix="admin:msg:browse", back_callback="admin:messages", counts=counts
            ),
        )
    await callback.answer()


async def _render_msg_category_browse(callback: CallbackQuery, category: str):
    """Shared renderer for the 'messages inside one category' screen -- used
    both when the admin taps a category chip and after a delete redirects
    back here, without needing to mutate the incoming CallbackQuery."""
    label = MESSAGE_CATEGORY_LABELS.get(category, category)
    async with get_session() as session:
        result = await session.execute(
            select(MessageTemplate).where(MessageTemplate.category == category).order_by(MessageTemplate.id)
        )
        messages = result.scalars().all()
        kb = InlineKeyboardBuilder()
        if not messages:
            text = f"📁 {label}\n\nهنوز پیامی توی این دسته‌بندی نیست."
        else:
            text = f"📁 {label} — {len(messages)} پیام (برای مدیریت لمس کن):"
            for msg in messages[:30]:
                dot = "🟢" if msg.active else "⚪️"
                preview = msg.text if len(msg.text) <= 30 else msg.text[:30] + "…"
                kb.row(InlineKeyboardButton(text=f"{dot} {preview}", callback_data=f"admin:msg:view:{msg.id}"))
        kb.row(InlineKeyboardButton(text="➕ افزودن به این دسته", callback_data=f"admin:msg:pick:{category}"))
        kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:msg:categories"))
        await callback.message.edit_text(text, reply_markup=kb.as_markup())


@router.callback_query(F.data.startswith("admin:msg:browse:"))
async def cb_admin_msg_browse(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    category = callback.data.split(":")[-1]
    await _render_msg_category_browse(callback, category)
    await callback.answer()


async def _render_msg_detail(callback: CallbackQuery, msg_id: int) -> bool:
    """Shared renderer for a single message's detail/action screen. Returns
    False (and answers the callback) if the message no longer exists."""
    async with get_session() as session:
        msg = await session.get(MessageTemplate, msg_id)
        if not msg:
            await callback.answer("❌ این پیام دیگه وجود نداره.", show_alert=True)
            return False
        label = MESSAGE_CATEGORY_LABELS.get(msg.category, msg.category)
        status = "🟢 فعال" if msg.active else "⚪️ غیرفعال"
        text = (
            f"📁 دسته‌بندی: {label}\n"
            f"وضعیت: {status}\n"
            f"وزن: {msg.weight}\n"
            f"تعداد استفاده: {msg.usage_count}\n\n"
            f"«{msg.text}»"
        )
        kb = InlineKeyboardBuilder()
        kb.row(
            InlineKeyboardButton(text="✏️ ویرایش متن", callback_data=f"admin:msg:edit:{msg.id}"),
            InlineKeyboardButton(
                text="⚪️ غیرفعال کن" if msg.active else "🟢 فعال کن",
                callback_data=f"admin:msg:toggle:{msg.id}",
            ),
        )
        kb.row(InlineKeyboardButton(text="🗑 حذف", callback_data=f"admin:msg:del:{msg.id}"))
        kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data=f"admin:msg:browse:{msg.category}"))
        await callback.message.edit_text(text, reply_markup=kb.as_markup())
    return True


@router.callback_query(F.data.startswith("admin:msg:view:"))
async def cb_admin_msg_view(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    msg_id = int(callback.data.split(":")[-1])
    if await _render_msg_detail(callback, msg_id):
        await callback.answer()


@router.callback_query(F.data.startswith("admin:msg:toggle:"))
async def cb_admin_msg_toggle(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    msg_id = int(callback.data.split(":")[-1])
    async with get_session() as session:
        repo = MessageRepository(session)
        new_active = await repo.toggle_active(msg_id)
        await session.commit()
        if new_active is None:
            await callback.answer("❌ این پیام دیگه وجود نداره.", show_alert=True)
            return
    if await _render_msg_detail(callback, msg_id):
        await callback.answer("🟢 فعال شد" if new_active else "⚪️ غیرفعال شد")


@router.callback_query(F.data.startswith("admin:msg:del:"))
async def cb_admin_msg_delete_confirm(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    msg_id = int(callback.data.split(":")[-1])
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="⚠️ تایید حذف", callback_data=f"admin:msg:delconfirm:{msg_id}"),
        InlineKeyboardButton(text="❌ انصراف", callback_data=f"admin:msg:view:{msg_id}"),
    )
    await callback.message.edit_text("⚠️ از حذف این پیام مطمئنی؟ این کار برگشت‌پذیر نیست.", reply_markup=kb.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:msg:delconfirm:"))
async def cb_admin_msg_delete(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    msg_id = int(callback.data.split(":")[-1])
    async with get_session() as session:
        msg = await session.get(MessageTemplate, msg_id)
        category = msg.category if msg else "GENERAL"
        repo = MessageRepository(session)
        await repo.delete_message(msg_id)
        await session.commit()
    await callback.answer("🗑 پیام حذف شد")
    await _render_msg_category_browse(callback, category)


@router.callback_query(F.data.startswith("admin:msg:edit:"))
async def cb_admin_msg_edit(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    msg_id = int(callback.data.split(":")[-1])
    await state.update_data(edit_message_id=msg_id)
    await state.set_state(AdminStates.EDIT_MESSAGE)
    await callback.message.edit_text("✏️ متن جدید پیام رو بفرست:")
    await callback.answer()


@router.message(AdminStates.EDIT_MESSAGE)
async def admin_edit_message(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    data = await state.get_data()
    msg_id = data.get("edit_message_id")
    text = (message.text or "").strip()
    if not msg_id:
        await message.answer("❌ مشخص نیست کدوم پیام، دوباره از منوی پیام‌ها شروع کن.")
        await state.clear()
        return
    if not text:
        await message.answer("❌ متن پیام نمی‌تونه خالی باشه. دوباره بفرست:")
        return
    async with get_session() as session:
        repo = MessageRepository(session)
        updated = await repo.update_message(msg_id, text=text)
        await session.commit()
        if not updated:
            await message.answer("❌ این پیام دیگه وجود نداره.", reply_markup=message_management())
        else:
            await message.answer("✅ پیام ویرایش شد!", reply_markup=message_management())
    await state.clear()


@router.callback_query(F.data == "admin:msg:seed")
async def cb_admin_msg_seed(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    added = 0
    async with get_session() as session:
        repo = MessageRepository(session)
        for category, samples in SAMPLE_MESSAGES.items():
            existing = await repo.get_messages_by_category(category, active_only=False)
            if existing:
                continue
            for text in samples:
                await repo.create_message(category, text, created_by=callback.from_user.id)
                added += 1
        await session.commit()
        if added:
            await callback.message.edit_text(
                f"🧪 {added} پیام نمونه اضافه شد!", reply_markup=message_management()
            )
        else:
            await callback.message.edit_text(
                "همه‌ی دسته‌بندی‌ها از قبل پیام دارن، چیز جدیدی اضافه نشد.",
                reply_markup=message_management(),
            )
    await callback.answer()


@router.callback_query(F.data == "admin:msg:stats")
async def cb_admin_msg_stats(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    async with get_session() as session:
        result = await session.execute(
            select(MessageTemplate.category, func.count(MessageTemplate.id), func.sum(MessageTemplate.usage_count))
            .group_by(MessageTemplate.category)
        )
        rows = result.all()
        text = "📊 آمار پیام‌ها بر اساس دسته‌بندی:\n\n"
        if not rows:
            text += "هنوز هیچ پیامی ثبت نشده."
        for category, count, usage in rows:
            label = MESSAGE_CATEGORY_LABELS.get(category, category)
            text += f"{label}: {count} پیام، {usage or 0} استفاده\n"
        await callback.message.edit_text(text, reply_markup=message_management())
    await callback.answer()


@router.callback_query(F.data == "admin:algorithm")
async def cb_admin_algorithm(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    weights = config.ALGORITHM_WEIGHTS
    text = "🧠 تنظیمات الگوریتم:\n\n"
    for key, value in weights.items():
        text += f"{key}: {value}\n"
    text += "\nبرای تغییر، از دستور /set_algorithm استفاده کنید."
    await callback.message.edit_text(text, reply_markup=admin_panel())
    await callback.answer()


@router.callback_query(F.data == "admin:notifications")
async def cb_admin_notifications(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    async with get_session() as session:
        total = (await session.execute(select(func.count()).select_from(UserPreference))).scalar() or 0
        lines = []
        for field_name, label in NOTIFICATION_PREF_FIELDS:
            column = getattr(UserPreference, field_name)
            enabled = (
                await session.execute(select(func.count()).select_from(UserPreference).where(column == True))
            ).scalar() or 0
            lines.append(f"{label}: {enabled}/{total} کاربر فعال")
        text = "🔔 تنظیمات و آمار اعلان‌ها:\n\n" + "\n".join(lines)
        await callback.message.edit_text(text, reply_markup=admin_notifications_keyboard())
    await callback.answer()


@router.callback_query(F.data == "admin:notif:broadcast")
async def cb_admin_notif_broadcast(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    await callback.message.edit_text(
        "📢 متن اعلانی که می‌خوای برای کاربران فعال ارسال بشه رو بفرست:\n"
        "(فقط برای کاربرانی که «پیام‌های انگیزشی» رو در تنظیماتشون روشن نگه داشتن ارسال میشه)"
    )
    await state.set_state(AdminStates.BROADCAST_NOTIFICATION)
    await callback.answer()


@router.message(AdminStates.BROADCAST_NOTIFICATION)
async def admin_broadcast_notification(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ متن نمی‌تونه خالی باشه. دوباره بفرست:")
        return
    async with get_session() as session:
        result = await session.execute(
            select(User.telegram_id)
            .join(UserPreference, UserPreference.user_id == User.id)
            .where(UserPreference.motivational_messages == True, User.is_active == True)
        )
        telegram_ids = [row[0] for row in result.all()]
    sent, failed = 0, 0
    for tg_id in telegram_ids:
        try:
            await message.bot.send_message(tg_id, f"🔔 {text}")
            sent += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            try:
                await message.bot.send_message(tg_id, f"🔔 {text}")
                sent += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
    await message.answer(f"✅ اعلان برای {sent} کاربر ارسال شد. ({failed} ناموفق)", reply_markup=admin_panel())
    await state.clear()


@router.callback_query(F.data == "admin:system")
async def cb_admin_system(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    health = await health_service.check()
    status = (
        f"🛠 وضعیت سیستم:\n\n"
        f"📊 وضعیت کلی: {health['status']}\n"
        f"🤖 ربات: {health['bot']}\n"
        f"💾 دیتابیس: {health['database']}\n"
        f"⏰ زمان‌بند: {health['scheduler']}\n"
        f"🕐 زمان تهران: {health['time']}\n"
        f"🌐 حالت: {config.BOT_MODE}"
    )
    await callback.message.edit_text(status, reply_markup=admin_panel())
    await callback.answer()


@router.callback_query(F.data == "admin:backup")
async def cb_admin_backup(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    current_hours = await get_backup_interval_hours()
    await callback.message.edit_text("💾 در حال ایجاد پشتیبان JSON...", reply_markup=backup_menu_keyboard())
    backup_path = await backup_service.export_json()
    if backup_path:
        sent = await backup_service.send_backup_to_admins(bot, backup_path)
        await callback.message.edit_text(
            f"✅ پشتیبان ساخته و برای {sent} ادمین ارسال شد.\n"
            f"⏱ فاصله پشتیبان‌گیری خودکار فعلی: هر {current_hours} ساعت.",
            reply_markup=backup_menu_keyboard(),
        )
    else:
        await callback.message.edit_text("❌ خطا در ایجاد پشتیبان.", reply_markup=backup_menu_keyboard())
    await callback.answer()


@router.callback_query(F.data == "admin:backup_interval")
async def cb_admin_backup_interval(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    current_hours = await get_backup_interval_hours()
    await callback.message.edit_text(
        f"⏱ فاصله فعلی پشتیبان‌گیری خودکار: هر {current_hours} ساعت.\n\n"
        "عدد ساعت جدید را وارد کنید (بین ۱ تا ۱۶۸):",
        reply_markup=backup_menu_keyboard(),
    )
    await state.set_state(AdminStates.SET_BACKUP_INTERVAL)
    await callback.answer()


@router.message(AdminStates.SET_BACKUP_INTERVAL)
async def admin_set_backup_interval(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    try:
        hours = int(message.text.strip())
        if not (1 <= hours <= 168):
            raise ValueError
    except ValueError:
        await message.answer("❌ لطفاً عددی صحیح بین ۱ تا ۱۶۸ (ساعت) وارد کنید.")
        return
    await set_setting("backup_interval_hours", str(hours))
    await reschedule_backup_job(hours)
    await message.answer(f"✅ فاصله پشتیبان‌گیری خودکار روی هر {hours} ساعت تنظیم شد.", reply_markup=admin_panel())
    await state.clear()


@router.callback_query(F.data == "admin:restore")
async def cb_admin_restore(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    await state.clear()
    backups = backup_service.list_backups()
    if backups:
        text = "♻️ بازیابی از دیتابیس:\n\nیکی از پشتیبان‌های موجود روی سرور را انتخاب کنید یا فایل جدیدی آپلود کنید."
    else:
        text = "♻️ بازیابی از دیتابیس:\n\nهیچ پشتیبانی روی سرور یافت نشد. می‌توانید فایل پشتیبان JSON را آپلود کنید."
    await callback.message.edit_text(text, reply_markup=restore_menu_keyboard(backups))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:restore:pick:"))
async def cb_admin_restore_pick(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    filename = callback.data.split("admin:restore:pick:", 1)[1]
    backup_path = backup_service.backup_dir / filename
    if not backup_path.exists():
        await callback.answer("❌ فایل یافت نشد.", show_alert=True)
        return
    await callback.message.edit_text(
        f"⚠️ توجه: با بازیابی «{filename}»، تمام داده‌های فعلی دیتابیس با محتوای این فایل "
        "جایگزین می‌شود و غیرقابل بازگشت است.\n\nآیا مطمئن هستید؟",
        reply_markup=restore_confirm_keyboard(filename),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:restore:confirm:"))
async def cb_admin_restore_confirm(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    filename = callback.data.split("admin:restore:confirm:", 1)[1]
    backup_path = backup_service.backup_dir / filename
    if not backup_path.exists():
        await callback.answer("❌ فایل یافت نشد.", show_alert=True)
        return
    await callback.message.edit_text("⏳ در حال بازیابی...", reply_markup=None)
    ok, msg = await backup_service.restore_from_json(backup_path)
    await callback.message.edit_text(msg, reply_markup=admin_panel())
    await callback.answer()


@router.callback_query(F.data == "admin:restore:upload")
async def cb_admin_restore_upload(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    await callback.message.edit_text("📤 فایل پشتیبان (JSON) را همینجا ارسال کنید:", reply_markup=admin_panel())
    await state.set_state(AdminStates.RESTORE_UPLOAD)
    await callback.answer()


@router.message(AdminStates.RESTORE_UPLOAD, F.document)
async def admin_restore_upload_received(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    doc = message.document
    if not doc.file_name.lower().endswith(".json"):
        await message.answer("❌ فقط فایل JSON پشتیبان پذیرفته می‌شود.")
        return
    backup_service.backup_dir.mkdir(parents=True, exist_ok=True)
    # Keep the stored filename short: it round-trips through Telegram's
    # 64-byte callback_data limit in the confirmation button below, so the
    # original (possibly long) filename is not reused here.
    dest = backup_service.backup_dir / f"uploaded_{int(time.time())}.json"
    file = await bot.get_file(doc.file_id)
    await bot.download_file(file.file_path, destination=str(dest))
    await state.clear()
    await message.answer(
        f"⚠️ فایل دریافت شد. با بازیابی، تمام داده‌های فعلی جایگزین می‌شود و غیرقابل بازگشت است.\n\nآیا مطمئن هستید؟",
        reply_markup=restore_confirm_keyboard(dest.name),
    )


@router.message(AdminStates.RESTORE_UPLOAD)
async def admin_restore_upload_invalid(message: Message):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    await message.answer("❌ لطفاً یک فایل JSON پشتیبان ارسال کنید.")


# ============================================================
# HEALTH ENDPOINT (for webhook mode)
# ============================================================
async def health_check_http(request: web.Request) -> web.Response:
    status = await health_service.check()
    return web.json_response(status, status=200 if status["status"] == "ok" else 503)


# ============================================================
# STARTUP / SHUTDOWN
# ============================================================
async def recover_from_crash():
    """Crash recovery, run once on every startup.

    Two problems this fixes that the original prototype did not handle at
    all:

    1. `SQLAlchemyJobStore` was imported but never actually passed to
       `AsyncIOScheduler(...)`, so APScheduler was always using its default
       *in-memory* jobstore. Every pending attendance-timeout / session-start
       / session-end job was silently lost on every restart.
    2. Even with jobs restored, a session that was mid-flight when the
       process died (attendance window already closed while offline, a
       STARTED session whose scheduled_end already passed, etc.) needs to be
       finalized rather than left stuck forever.

    Strategy: `scheduler_jobs` is the durable source of truth the app
    already writes to. On startup we replay it -- run anything overdue
    immediately, re-register anything still in the future with APScheduler
    -- and then do a defensive sweep for any session stuck in an in-flight
    status with no matching job row at all.
    """
    JOB_FUNCS = {
        "attendance_timeout": (_attendance_timeout, "session_id"),
        "session_start": (_session_start, "session_id"),
        "session_end": (_session_end, "session_id"),
        "readiness_poll": (_create_readiness_poll, "group_id"),
    }

    def _naive(dt: Optional[datetime]) -> Optional[datetime]:
        if dt is not None and dt.tzinfo is not None:
            return dt.astimezone(pytz.UTC).replace(tzinfo=None)
        return dt

    now_naive = _naive(time_service.now())

    async with get_session() as db_session:
        result = await db_session.execute(
            select(SchedulerJob).where(SchedulerJob.status == "pending")
        )
        pending_jobs = result.scalars().all()
        job_session_ids = {
            (j.job_meta or {}).get("session_id") for j in pending_jobs
            if j.job_meta and j.job_type != "readiness_poll"
        }
        stuck_result = await db_session.execute(
            select(Session).where(Session.status.in_(["ATTENDANCE", "READY", "STARTED"]))
        )
        stuck_sessions = [s for s in stuck_result.scalars().all() if s.id not in job_session_ids]

    overdue_job_ids, restored = [], 0
    for job in pending_jobs:
        entry = JOB_FUNCS.get(job.job_type)
        key_id = (job.job_meta or {}).get(entry[1]) if entry and job.job_meta else None
        if not entry or key_id is None:
            overdue_job_ids.append(job.job_id)  # unrecognized -- retire it, don't loop forever
            continue
        func = entry[0]
        run_at_naive = _naive(job.run_at)
        if run_at_naive is not None and run_at_naive <= now_naive:
            try:
                await func(key_id)
            except Exception as e:
                logger.error(f"Crash recovery: failed to run overdue job {job.job_id}: {e}")
            overdue_job_ids.append(job.job_id)
        else:
            scheduler.add_job(
                func, trigger=DateTrigger(run_date=job.run_at), args=[key_id],
                id=job.job_id, replace_existing=True,
            )
            restored += 1

    for sess in stuck_sessions:
        try:
            if sess.status == "ATTENDANCE":
                await _attendance_timeout(sess.id)
            elif sess.status == "STARTED":
                await _session_end(sess.id)
            elif sess.status == "READY":
                end_naive = _naive(sess.scheduled_end)
                if end_naive is not None and end_naive <= now_naive:
                    async with get_session() as db_session:
                        s = await SessionManager(db_session).get_session(sess.id)
                        if s and s.status == "READY":
                            s.status = "MISSED"
                else:
                    await _session_start(sess.id)
        except Exception as e:
            logger.error(f"Crash recovery: failed to resolve stuck session {sess.id}: {e}")

    if overdue_job_ids:
        async with get_session() as db_session:
            result = await db_session.execute(
                select(SchedulerJob).where(SchedulerJob.job_id.in_(overdue_job_ids))
            )
            for job in result.scalars().all():
                job.status = "done"

    logger.info(
        f"Crash recovery complete: {len(overdue_job_ids)} overdue jobs replayed, "
        f"{restored} jobs restored to scheduler, {len(stuck_sessions)} orphaned sessions swept"
    )


async def _morning_poll_all_groups():
    """The one fixed daily trigger point. Every other automatic poll during
    the day is a consequence of this one (via _schedule_next_poll after a
    session completes, or an activity-triggered repoll)."""
    async with get_session() as session:
        result = await session.execute(select(Group.id).where(Group.active == True))
        group_ids = [row[0] for row in result.all()]
    for group_id in group_ids:
        try:
            await _create_readiness_poll(group_id)
        except Exception as e:
            logger.error(f"Morning poll failed for group {group_id}: {e}")


async def _run_scheduled_backup():
    """Job body for the recurring backup: export to JSON, then push the
    file to every admin's DMs so it never depends on someone opening the
    panel to grab it."""
    backup_path = await backup_service.export_json()
    if not backup_path:
        logger.error("Scheduled backup failed to generate a file")
        return
    if bot is not None:
        sent = await backup_service.send_backup_to_admins(bot, backup_path)
        logger.info(f"Scheduled backup sent to {sent} admin(s)")
    else:
        logger.warning("Scheduled backup created but bot is not ready to send it")


async def reschedule_backup_job(hours: Optional[int] = None) -> int:
    """(Re)installs the recurring backup job at the given interval (hours).
    If hours is omitted, reads the persisted admin setting (falling back
    to config.BACKUP_INTERVAL_HOURS). Safe to call any time, including
    from the admin panel, to apply a new interval immediately."""
    if hours is None:
        hours = await get_backup_interval_hours()
    scheduler.add_job(
        _run_scheduled_backup,
        trigger=IntervalTrigger(hours=hours, timezone="Asia/Tehran"),
        id="periodic_backup",
        replace_existing=True,
    )
    logger.info(f"Backup job scheduled every {hours} hour(s)")
    return hours


async def on_startup():
    logger.info("Starting Study Coach...")
    await init_db()
    SchedulerService.start()
    await recover_from_crash()
    # Schedule daily morning reminder at 8:00
    now = time_service.now()
    morning_time = now.replace(hour=8, minute=0, second=0, microsecond=0)
    if morning_time <= now:
        morning_time += timedelta(days=1)
    # Fixed morning readiness poll for every active group (the "اول صبح
    # فقط" cadence the user asked for) -- everything past this point in the
    # day is driven by _schedule_next_poll() after a session completes, or
    # by real group activity waking a quiet group back up.
    scheduler.add_job(
        _morning_poll_all_groups,
        trigger=CronTrigger(hour=8, minute=0, timezone="Asia/Tehran"),
        id="morning_reminder",
        replace_existing=True
    )
    # Recurring JSON backup, sent to every admin -- interval is
    # configurable at runtime from the admin panel (persisted setting),
    # defaulting to BACKUP_INTERVAL_HOURS.
    await reschedule_backup_job()
    # Schedule weekly summary on Sundays at 22:00
    scheduler.add_job(
        lambda: logger.info("Weekly summary generated"),
        trigger=CronTrigger(day_of_week='sun', hour=22, minute=0, timezone="Asia/Tehran"),
        id="weekly_summary",
        replace_existing=True
    )
    logger.info("Study Coach started!")


async def on_shutdown():
    logger.info("Shutting down...")
    SchedulerService.stop()
    await close_db()
    if bot is not None:
        await bot.session.close()
    logger.info("Shutdown complete.")


# ============================================================
# POLLING MODE
# ============================================================
def run_polling():
    global bot
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp.include_router(router)

    async def main():
        await on_startup()
        try:
            await dp.start_polling(
                bot,
                allowed_updates=["message", "callback_query", "my_chat_member", "chat_member"],
            )
        finally:
            await on_shutdown()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        loop.close()


# ============================================================
# WEBHOOK MODE
# ============================================================
def run_webhook():
    global bot
    if not config.WEBHOOK_URL or not config.WEBHOOK_SECRET:
        logger.error("WEBHOOK_URL and WEBHOOK_SECRET required")
        sys.exit(1)

    bot = Bot(token=config.BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp.include_router(router)

    app = web.Application()

    async def webhook_handler(request: web.Request) -> web.Response:
        secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
        if secret != config.WEBHOOK_SECRET:
            return web.Response(status=401)
        try:
            data = await request.json()
            await dp.feed_raw_update(bot, data)
            return web.Response(status=200)
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            return web.Response(status=500)

    app.router.add_post(config.WEBHOOK_PATH, webhook_handler)
    app.router.add_get("/health", health_check_http)

    async def on_startup_webhook(app):
        await on_startup()
        webhook_url = f"{config.WEBHOOK_URL}{config.WEBHOOK_PATH}"
        await bot.set_webhook(
            url=webhook_url,
            secret_token=config.WEBHOOK_SECRET,
            max_connections=100,
            allowed_updates=["message", "callback_query", "my_chat_member", "chat_member"],
        )
        logger.info(f"Webhook set to {webhook_url}")

    async def on_shutdown_webhook(app):
        await bot.delete_webhook()
        await on_shutdown()

    app.on_startup.append(on_startup_webhook)
    app.on_shutdown.append(on_shutdown_webhook)

    web.run_app(app, host="0.0.0.0", port=config.PORT)


# ============================================================
# MAIN ENTRY
# ============================================================
def main():
    config.validate()
    # Single instance check (optional)
    pid_file = "/tmp/study_coach.pid"
    if os.path.exists(pid_file):
        try:
            with open(pid_file, "r") as f:
                pid = int(f.read().strip())
            try:
                os.kill(pid, 0)
                logger.error("Another instance is running")
                sys.exit(1)
            except OSError:
                os.remove(pid_file)
        except:
            pass
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

    # Signal handlers
    def signal_handler(sig, frame):
        logger.info("Received signal, exiting...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    if config.BOT_MODE == "webhook":
        run_webhook()
    else:
        run_polling()


if __name__ == "__main__":
    main()

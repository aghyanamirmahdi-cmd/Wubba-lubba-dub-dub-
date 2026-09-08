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
import sqlite3
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
# NOTE: APScheduler's SQLAlchemyJobStore needs a *synchronous* SQLAlchemy
# engine/URL, while this app's DB layer is fully async (aiosqlite). Rather
# than run two separate SQLAlchemy engines against the same file, job
# durability is handled at the application layer instead: every scheduled
# job is written to the `scheduler_jobs` table (see SchedulerService), and
# `recover_from_crash()` replays that table on startup. APScheduler itself
# is left on its default in-memory jobstore intentionally.

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

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, DateTime, Float, Text,
    ForeignKey, UniqueConstraint, Index, JSON, select, delete, and_, or_, desc, func
)
from sqlalchemy.ext.asyncio import (
    AsyncSession, create_async_engine, async_sessionmaker, AsyncEngine
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker, joinedload
from sqlalchemy.event import listen
from sqlalchemy.exc import IntegrityError

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
# DATABASE SETUP (SQLAlchemy Async)
# ============================================================
Base = declarative_base()


def get_async_db_url():
    url = config.DATABASE_URL
    if url.startswith("sqlite:///"):
        return url.replace("sqlite:///", "sqlite+aiosqlite:///")
    return url


_engine: Optional[AsyncEngine] = None
_async_session_maker: Optional[async_sessionmaker] = None


async def init_db():
    global _engine, _async_session_maker
    async_url = get_async_db_url()
    # If this is a relative/local SQLite path (the default, since most
    # people only set BOT_TOKEN/ADMIN_IDS/PORT and leave DATABASE_URL
    # unset), the parent directory isn't created automatically by
    # SQLAlchemy -- it just fails to open the file. Create it up front.
    if async_url.startswith("sqlite"):
        raw_path = async_url.split("///", 1)[-1]
        parent = Path(raw_path).parent
        if str(parent) not in ("", "."):
            parent.mkdir(parents=True, exist_ok=True)
    _engine = create_async_engine(
        async_url,
        echo=False,
        future=True,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    _async_session_maker = async_sessionmaker(
        _engine, class_=AsyncSession, expire_on_commit=False, autocommit=False, autoflush=False
    )
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database initialized")


async def close_db():
    if _engine:
        await _engine.dispose()
        logger.info("Database closed")


@asynccontextmanager
async def get_session():
    if not _async_session_maker:
        await init_db()
    async with _async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


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
    poll_window_minutes = Column(Integer, default=15, nullable=False)
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
        today = time_service.start_of_day()
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
        now = time_service.now()
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
        start = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        result = await session.execute(
            select(SessionResult)
            .join(Session, SessionResult.session_id == Session.id)
            .where(
                SessionResult.user_id == user_id,
                Session.reported_at >= start,
                Session.reported_at <= end
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
        week_start = time_service.now() - timedelta(days=time_service.now().weekday())
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
scheduler = AsyncIOScheduler(timezone="Asia/Tehran")

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
                await bot.send_message(
                    group.telegram_chat_id,
                    f"✅ پارت آماده شد!\n"
                    f"👥 {len(ready_participants)} نفر آماده هستند.\n"
                    f"⏱ حداقل: {rec['minimum']} | هدف: {rec['target']} | حداکثر: {rec['extension']} دقیقه\n"
                    f"🧭 حالت: {rec['mode']}"
                )
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
    - After that, the *next* poll is scheduled `inter_session_gap_minutes`
      after a session actually completes -- not on a blind fixed interval.
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
        await SchedulerService.schedule_job("attendance_timeout", timeout_at, {"session_id": sess.id})
        await SchedulerService.schedule_job("session_start", start_time, {"session_id": sess.id})
        await SchedulerService.schedule_job("session_end", end_time, {"session_id": sess.id})

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


async def _schedule_next_poll(group_id: int):
    """Called after a session actually completes. Schedules the next
    automatic poll `inter_session_gap_minutes` later, pushed past quiet
    hours if it would otherwise land inside them."""
    async with get_session() as session:
        settings_result = await session.execute(
            select(GroupSetting).where(GroupSetting.group_id == group_id)
        )
        settings = settings_result.scalar_one_or_none()
        if not settings or not settings.auto_poll_enabled:
            return
        settings.awaiting_activity = False
        run_at = time_service.now() + timedelta(minutes=settings.inter_session_gap_minutes)
        if _in_quiet_hours(run_at.hour, settings.quiet_hour_start, settings.quiet_hour_end):
            # Push to quiet_hour_end the same day (or next day if we're
            # already past it) -- next real poll will happen when the group
            # wakes back up, not a random time overnight.
            run_at = run_at.replace(hour=settings.quiet_hour_end, minute=0, second=0, microsecond=0)
            if run_at <= time_service.now():
                run_at += timedelta(days=1)
        await session.flush()
    await SchedulerService.schedule_job("readiness_poll", run_at, {"group_id": group_id})


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
        await _schedule_next_poll(group_id)


class SchedulerService:
    @staticmethod
    async def schedule_job(job_type: str, run_at: datetime, metadata: Dict[str, Any]):
        async with get_session() as db_session:
            job_id = f"{job_type}_{int(run_at.timestamp())}_{hash(str(metadata))}"
            existing = await db_session.execute(
                select(SchedulerJob).where(SchedulerJob.job_id == job_id)
            )
            if existing.scalar_one_or_none():
                return
            job = SchedulerJob(
                job_id=job_id,
                job_type=job_type,
                run_at=run_at,
                status="pending",
                job_meta=metadata
            )
            db_session.add(job)
            await db_session.flush()

        # Schedule with APScheduler
        if job_type == "attendance_timeout":
            scheduler.add_job(
                _attendance_timeout,
                trigger=DateTrigger(run_at=run_at),
                args=[metadata.get("session_id")],
                id=job_id,
                replace_existing=True
            )
        elif job_type == "session_start":
            scheduler.add_job(
                _session_start,
                trigger=DateTrigger(run_at=run_at),
                args=[metadata.get("session_id")],
                id=job_id,
                replace_existing=True
            )
        elif job_type == "session_end":
            scheduler.add_job(
                _session_end,
                trigger=DateTrigger(run_at=run_at),
                args=[metadata.get("session_id")],
                id=job_id,
                replace_existing=True
            )
        elif job_type == "readiness_poll":
            scheduler.add_job(
                _create_readiness_poll,
                trigger=DateTrigger(run_at=run_at),
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

class UserStates(StatesGroup):
    SET_DAILY_GOAL = State()
    SET_PREFERRED_DURATION = State()
    REPORT_ACTUAL = State()


# Middleware
class AuthMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable, event: Update, data: Dict) -> Any:
        user_id = None
        if event.message and event.message.from_user:
            user_id = event.message.from_user.id
        elif event.callback_query and event.callback_query.from_user:
            user_id = event.callback_query.from_user.id
        if user_id:
            data["user_id"] = user_id
            data["is_admin"] = user_id in config.ADMIN_IDS
        return await handler(event, data)


class GroupSyncMiddleware(BaseMiddleware):
    """Runs before every message handler (not just a standalone catch-all
    handler, which would have blocked command routing in the same router).
    Ensures Group/GroupMember rows exist as a fallback whenever we see a
    message from a human in a group chat, then always lets the update
    continue to the real handler."""
    async def __call__(self, handler: Callable, event: Message, data: Dict) -> Any:
        try:
            if event.chat and event.chat.type in ("group", "supergroup") and \
               event.from_user and not event.from_user.is_bot:
                async with get_session() as session:
                    group_repo = GroupRepository(session)
                    group = await group_repo.get_or_create_group(event.chat.id, title=event.chat.title)
                    user_repo = UserRepository(session)
                    user = await user_repo.get_or_create(
                        telegram_id=event.from_user.id, username=event.from_user.username,
                        first_name=event.from_user.first_name, last_name=event.from_user.last_name,
                    )
                    await group_repo.add_member(user.id, group.id)

                    settings_result = await session.execute(
                        select(GroupSetting).where(GroupSetting.group_id == group.id)
                    )
                    settings = settings_result.scalar_one_or_none()
                    should_repoll = bool(settings and settings.awaiting_activity)

                if should_repoll:
                    # The group went quiet after an unanswered poll, and this
                    # is the first real message since then -- exactly the
                    # trigger the user asked for, instead of polling on a
                    # blind timer. _create_readiness_poll() itself re-checks
                    # quiet hours / an already-active session, so this is
                    # safe even under a burst of messages.
                    await _create_readiness_poll(group.id)
        except Exception as e:
            # Never let membership bookkeeping break the actual command.
            logger.error(f"GroupSyncMiddleware failed: {e}")
        return await handler(event, data)


router.message.middleware(AuthMiddleware())
router.message.middleware(GroupSyncMiddleware())
router.callback_query.middleware(AuthMiddleware())


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


def message_management() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="➕ افزودن", callback_data="admin:msg:add"),
        InlineKeyboardButton(text="✏️ ویرایش", callback_data="admin:msg:edit"),
    )
    kb.row(
        InlineKeyboardButton(text="🗑 حذف", callback_data="admin:msg:delete"),
        InlineKeyboardButton(text="🟢 فعال/غیرفعال", callback_data="admin:msg:toggle"),
    )
    kb.row(
        InlineKeyboardButton(text="📊 آمار پیام‌ها", callback_data="admin:msg:stats"),
        InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"),
    )
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
        start_time = now + timedelta(minutes=2)
        end_time = start_time + timedelta(minutes=45)

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
            {"session_id": sess.id}
        )

        # Schedule session start and end (in case no one clicks start)
        await SchedulerService.schedule_job(
            "session_start", start_time,
            {"session_id": sess.id}
        )
        await SchedulerService.schedule_job(
            "session_end", end_time,
            {"session_id": sess.id}
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
            await callback.answer("❌ زمان ثبت نام گذشته.")
            return
        participant = await manager.register_participant(session_id, user.id)
        if participant:
            participants = await manager.get_participants(session_id)
            count = len(participants)
            await callback.message.edit_text(
                f"📚 پارت #{session_id}\n"
                f"✅ {user.first_name or 'کاربر'} اعلام آمادگی کرد!\n"
                f"👥 تعداد آمادگان: {count}",
                reply_markup=attendance_keyboard(session_id)
            )
            await callback.answer("✅ ثبت شد! منتظر شروع پارت باش.")
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
        if success:
            emoji = "🟢" if status == "COMPLETED" else "🟡" if status == "PARTIAL" else "🔴"
            await callback.message.edit_text(f"{emoji} گزارش ثبت شد! ممنون از همراهی‌ات 🌱")
            await callback.answer("✅ ثبت شد!")
        else:
            await callback.answer("❌ قبلاً ثبت شده.")


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
            if success:
                await message.answer(f"✅ مدت {duration} دقیقه ثبت شد! ممنون از همراهی‌ات 🌱")
            else:
                await message.answer("❌ خطا در ثبت مدت.")
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
        user_id = callback.from_user.id
        calc = StatisticsCalculator()
        weekly = await calc.get_weekly_stats(user_id, session)
        daily = await calc.get_today_stats(user_id, session)
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
async def cb_admin_msg_add(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    # Ask for category first
    await callback.message.answer("📝 لطفاً دسته‌بندی را وارد کنید (مثلاً SESSION_START، SUCCESS، RECOVERY و ...):")
    await state.set_state(AdminStates.ADD_MESSAGE)
    await callback.answer()


@router.message(AdminStates.ADD_MESSAGE)
async def admin_add_message(message: Message, state: FSMContext):
    # For simplicity, we store the category as the first line, then message
    lines = message.text.split('\n', 1)
    if len(lines) < 2:
        await message.answer("❌ لطفاً دسته‌بندی را در خط اول و متن پیام را از خط دوم وارد کنید.")
        return
    category = lines[0].strip().upper()
    text = lines[1].strip()
    if not category or not text:
        await message.answer("❌ دسته‌بندی و متن پیام الزامی است.")
        return
    async with get_session() as session:
        repo = MessageRepository(session)
        msg = await repo.create_message(category, text, created_by=message.from_user.id)
        await session.commit()
    await message.answer(f"✅ پیام با دسته‌بندی {category} و وزن ۱.۰ افزوده شد!")
    await state.clear()


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
        for category, count, usage in rows:
            text += f"{category}: {count} پیام، {usage or 0} استفاده\n"
        await callback.message.edit_text(text, reply_markup=message_management())
    await callback.answer()


@router.callback_query(F.data == "admin:msg:toggle")
async def cb_admin_msg_toggle(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    await callback.message.answer("لطفاً شناسه پیام را برای فعال/غیرفعال کردن وارد کنید:")
    await callback.answer()


@router.callback_query(F.data == "admin:msg:edit")
async def cb_admin_msg_edit(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    await callback.message.answer("لطفاً شناسه پیام را برای ویرایش وارد کنید:")
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
    await callback.message.edit_text("🔔 تنظیمات اعلان‌ها:\n\n(قابلیت در حال توسعه)", reply_markup=admin_panel())
    await callback.answer()


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
                func, trigger=DateTrigger(run_at=job.run_at), args=[key_id],
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
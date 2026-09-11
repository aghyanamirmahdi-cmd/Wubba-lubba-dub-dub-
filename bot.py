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
import html
import json
import logging
import logging.handlers
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
    # Log file location -- logging used to go *only* to stdout, so once the
    # process was running under a process manager/container, there was no
    # way to look at past logs short of however that platform happened to
    # capture stdout. Now also written to a real rotating file the admin
    # panel's 📜 لاگ‌ها screen can read back.
    LOG_DIR: str = os.getenv("LOG_DIR", "./logs")
    LOG_FILE: str = os.getenv("LOG_FILE", "bot.log")
    LOG_MAX_BYTES: int = int(os.getenv("LOG_MAX_BYTES", str(5 * 1024 * 1024)))  # 5 MB per file
    LOG_BACKUP_COUNT: int = int(os.getenv("LOG_BACKUP_COUNT", "5"))
    BACKUP_DIR: str = os.getenv("BACKUP_DIR", "./backups")
    # Default backup interval in MINUTES (used to be hours; switched to
    # minutes so a half-hour cadence -- the new default -- is expressible
    # without a fraction). Overridable at runtime by admins from the admin
    # panel (persisted in the bot_settings table -- see
    # get_backup_interval_minutes/reschedule_backup_job).
    BACKUP_INTERVAL_MINUTES: int = int(os.getenv("BACKUP_INTERVAL_MINUTES", "30"))
    # Where the recurring backup gets sent: a specific chat (group/channel/
    # person) picked by an admin via numeric ID, from ⏱ فاصله پشتیبان‌گیری
    # -> 🎯 تنظیم مقصد. Empty by default -- see get_backup_target_chat_id,
    # which falls back to the bot owner (first entry of ADMIN_IDS) when
    # nothing has been set.
    BACKUP_TARGET_CHAT_ID: Optional[int] = (
        int(os.getenv("BACKUP_TARGET_CHAT_ID")) if os.getenv("BACKUP_TARGET_CHAT_ID") else None
    )
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
    # After this many fully-missed polls in a row (zero "هستم" taps), stop
    # polling a group automatically -- including the daily morning poll --
    # until a real message/tap comes in from that group. See
    # GroupSetting.consecutive_misses / _create_readiness_poll.
    MISS_STREAK_PAUSE_THRESHOLD: int = int(os.getenv("MISS_STREAK_PAUSE_THRESHOLD", "3"))
    # Fixed daily time the morning readiness poll goes out to every active
    # group. Used to be hardcoded straight into the CronTrigger call in
    # on_startup() with no way to change it short of editing code and
    # redeploying -- now a real admin-tunable setting (see TUNABLE_SETTINGS
    # / admin:tunables), applied live via reschedule_morning_poll_job().
    MORNING_POLL_HOUR: int = int(os.getenv("MORNING_POLL_HOUR", "8"))
    MORNING_POLL_MINUTE: int = int(os.getenv("MORNING_POLL_MINUTE", "0"))

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

    # Algorithm weights (admin-tunable at runtime -- see AlgorithmSetting /
    # admin:algorithm). Every term FitnessScorer.calculate actually adds
    # into the final score has a named weight here now.
    #
    # Cleanup note: four keys that used to live here --
    # RECENT_PERFORMANCE_WEIGHT, HISTORICAL_PERFORMANCE_WEIGHT,
    # CONSISTENCY_WEIGHT, RECOVERY_WEIGHT -- were never actually read by
    # FitnessScorer.calculate. They looked admin-tunable (the old
    # admin:algorithm screen listed and let you "change" them) but changing
    # them had zero effect on any recommendation. Removed rather than kept
    # as a dead knob; GroupCapacityCalculator.calculate has its own
    # independent (and actually-used) reliability weighting for
    # completion/attendance/consistency. Meanwhile COMPLETION_PROB_WEIGHT
    # and PROGRESS_WEIGHT below used to be hardcoded literals (0.15, 0.10)
    # directly in the score formula -- also not admin-tunable despite
    # sitting right next to weights that were. Both are now real entries.
    ALGORITHM_WEIGHTS: Dict[str, float] = {
        "TIME_FIT_WEIGHT": 0.15,
        "INTENT_WEIGHT": 0.10,
        "GROUP_FIT_WEIGHT": 0.03,
        "PERSONAL_FIT_WEIGHT": 0.02,
        "OVERLOAD_PENALTY": 0.20,
        "FAILURE_PENALTY": 0.15,
        "COMPLETION_PROB_WEIGHT": 0.15,
        "PROGRESS_WEIGHT": 0.10,
        # Weight for how closely a candidate duration matches known,
        # evidence-anchored study-block lengths (see FOCUS_SWEET_SPOTS
        # below) -- independent of any one person's own history, this is
        # "does the length itself match how sustained attention is known
        # to behave".
        "METHOD_FIT_WEIGHT": 0.08,
    }

    # ------------------------------------------------------------------
    # Evidence-anchored study-method constants.
    # These aren't arbitrary tuning knobs -- each maps to a specific,
    # well-replicated finding from cognitive/educational psychology, used
    # to ground the recommendation engine and the Advisor's coaching
    # messages in established study science rather than ad-hoc heuristics:
    #
    #   - FOCUS_SWEET_SPOTS: sustained-attention research consistently
    #     shows measurable performance decline over a single unbroken
    #     stretch of focused work -- the reasoning behind the Pomodoro
    #     Technique's short (~25 min) blocks, and the wider ~25-50 min
    #     range most study-skills guidance converges on as the practical
    #     ceiling for one focused block before a break helps more than
    #     pushing on.
    #   - ULTRADIAN_CEILING_MINUTES: work on the body's ~90-120 minute
    #     "basic rest-activity cycle" (Kleitman) and later attention-
    #     restoration research treat that range as a natural ceiling for
    #     one continuous stretch -- going well past it works against the
    #     body's own attention rhythm rather than with it.
    #   - MIN_BREAK_MINUTES: distributed/spaced-practice research --
    #     going back to Ebbinghaus's forgetting curve and confirmed at
    #     scale by later meta-analyses of the "spacing effect" -- shows
    #     spaced study reliably beats one long massed session for
    #     long-term retention. A short break between parts isn't lost
    #     time; it's part of what makes spaced practice work.
    #   - RETRIEVAL_PRACTICE_NOTE / spacing / desirable-difficulty framing
    #     used in Advisor messages likewise trace to the "testing effect"
    #     (retrieval practice outperforms passive re-reading) and to
    #     Bjork's "desirable difficulties" framing of gradual, not abrupt,
    #     increases in challenge.
    # ------------------------------------------------------------------
    FOCUS_SWEET_SPOTS: List[int] = [25, 45, 50]
    ULTRADIAN_CEILING_MINUTES: int = 90
    MIN_BREAK_MINUTES: int = 5

    # ------------------------------------------------------------------
    # RESEARCH_LIBRARY -- a small, hand-curated set of recent (2024-2025),
    # peer-reviewed / ISI-indexed findings on study effectiveness. This is
    # NOT a live API feed: it's refreshed manually (by re-checking the
    # literature and editing this list) rather than fetched at runtime, so
    # it stays reliable even with no network access from the bot process.
    # Each entry backs one of the tips the Advisor rotates into daily
    # personalized plans (see Advisor.get_research_insight /
    # generate_personalized_plan). "tag" lets the Advisor pick a tip that
    # actually matches the person's current situation instead of a purely
    # random one.
    #
    # Sources (checked most recently: Sep 2026) -- update this block
    # periodically as newer meta-analyses come out:
    #   - Murray, Horner & Göbel (2025), Educational Psychology Review 37(75):
    #     spaced/retrieval practice meta-analysis, math learning, g=0.28-0.43.
    #   - Systematic review & meta-analysis, spaced repetition in medical
    #     education (2025, 21,415 learners): SMD=0.78 favoring spaced study.
    #   - Mawson & Kang (2025), Behavioral Sciences 15(771): distributed vs.
    #     massed practice in real classrooms, d=0.54.
    #   - Brunmair & Richter (Psychol. Bull.) + 2024-2025 replications:
    #     interleaved vs. blocked practice, g≈0.34-0.42 (stronger for
    #     math/visual material, weaker for plain text).
    #   - 2022-2025 reviews on naps & memory consolidation: short naps
    #     (~20-30 min) after study help consolidation without the grogginess
    #     of longer naps.
    # ------------------------------------------------------------------
    RESEARCH_LIBRARY: List[Dict[str, str]] = [
        {
            "tag": "consistency_low",
            "text": (
                "🔬 یافته علمی (۲۰۲۵، Educational Psychology Review): پخش‌کردن مرور مطالب در چند "
                "روز به‌جای فشردن همه‌چیز در یک نشست، به‌طور پایدار یادگیری بلندمدت رو بهتر می‌کنه. "
                "همون ۳۰ دقیقه، اگه دو روز جدا از هم باشه، اثرش از یک نشست ۶۰ دقیقه‌ای بیشتره."
            ),
        },
        {
            "tag": "recovery_or_general",
            "text": (
                "🔬 یافته علمی (۲۰۲۵، مرور نظام‌مند آموزش پزشکی، ۲۱هزار+ نفر): مطالعه‌ی فاصله‌دار "
                "(Spaced Study) در مقایسه با مطالعه‌ی معمولی و پیوسته، تفاوت معناداری در یادگیری ایجاد کرد. "
                "برگشتن با یک پارت کوتاه بعد از وقفه، خودش نوعی فاصله‌گذاریه -- نه یک قدم عقب."
            ),
        },
        {
            "tag": "general",
            "text": (
                "🔬 یافته علمی (۲۰۲۵، Behavioral Sciences، مرور کلاس‌های واقعی): پخش‌کردن مطالعه در "
                "چند جلسه‌ی کوتاه، در شرایط واقعی کلاس درس هم بهتر از یک جلسه‌ی طولانی عمل کرد -- "
                "این یافته توی محیط آزمایشگاهی نیست، توی کلاس واقعی به‌دست اومده."
            ),
        },
        {
            "tag": "review_material",
            "text": (
                "🔬 یافته علمی (فرا-تحلیل‌های تکرارشده تا ۲۰۲۵): خودآزمایی و یادآوری فعال (مثلاً بستن "
                "کتاب و امتحان کردن حافظه‌ت) به‌طور معنادار از دوباره‌خوانی صرف مؤثرتره. اگه امروز مرور "
                "داری، به‌جای خوندن دوباره، از خودت سوال بپرس."
            ),
        },
        {
            "tag": "multi_subject",
            "text": (
                "🔬 یافته علمی (فرا-تحلیل‌های تکرارشده ۲۰۱۹-۲۰۲۵): جابه‌جا کردن بین چند موضوع/نوع مسئله "
                "در یک بازه‌ی مطالعه (به‌جای انجام همه‌ی یک نوع پشت‌سرهم) روی درک عمیق‌تر و انتقال "
                "یادگیری اثر مثبت داره -- به‌خصوص برای درس‌هایی با مسئله‌های شبیه به هم مثل ریاضی."
            ),
        },
        {
            "tag": "night_or_load",
            "text": (
                "🔬 یافته علمی (مرورهای ۲۰۲۲-۲۰۲۵ درباره‌ی خواب و حافظه): یک چرت کوتاه ۲۰-۳۰ دقیقه‌ای "
                "بعد از مطالعه می‌تونه به تثبیت حافظه کمک کنه، بدون گیجی چرت‌های طولانی‌تر. اگه امشب "
                "خسته‌ای، فشار آوردن به یک نشست سنگین ارزشش رو نداره."
            ),
        },
    ]

    # Fixed daily time the personal plan is generated & pushed out
    # proactively (independent of the user asking for /plan). Runs shortly
    # after the morning readiness poll so the plan reflects "today".
    DAILY_PLAN_HOUR: int = int(os.getenv("DAILY_PLAN_HOUR", "8"))
    DAILY_PLAN_MINUTE: int = int(os.getenv("DAILY_PLAN_MINUTE", "30"))

    # ------------------------------------------------------------------
    # Activity prioritization -- how "how important is this?" (self-rated
    # by the user, 1-5) and "how soon is it due?" (from an optional
    # deadline) combine into one priority number the daily plan can sort
    # and allocate minutes by. This is a deliberate Eisenhower-style split
    # of importance vs. urgency, not a single vague "priority" slider --
    # a low-importance task due tomorrow can still outrank a high-
    # importance task that's a month out. See Advisor._activity_weight.
    # ------------------------------------------------------------------
    IMPORTANCE_LABELS: Dict[int, str] = {
        1: "⭐ کم‌اهمیت",
        2: "⭐⭐ معمولی",
        3: "⭐⭐⭐ مهم",
        4: "⭐⭐⭐⭐ خیلی مهم",
        5: "⭐⭐⭐⭐⭐ حیاتی/فوری",
    }
    # (max_days_left, multiplier) checked in order -- first match wins.
    URGENCY_MULTIPLIERS: List[Tuple[int, float]] = [
        (1, 2.0),   # due today/tomorrow
        (3, 1.6),   # due within 3 days
        (7, 1.3),   # due within a week
        (999999, 1.0),  # no deadline pressure yet
    ]

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
os.makedirs(config.LOG_DIR, exist_ok=True)
_log_file_path = os.path.join(config.LOG_DIR, config.LOG_FILE)
_log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_file_handler = logging.handlers.RotatingFileHandler(
    _log_file_path, maxBytes=config.LOG_MAX_BYTES, backupCount=config.LOG_BACKUP_COUNT, encoding="utf-8"
)
_file_handler.setFormatter(logging.Formatter(_log_format))
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format=_log_format,
    handlers=[logging.StreamHandler(sys.stdout), _file_handler]
)
logger = logging.getLogger("study_coach")


def _tail_log_lines(n: int = 50, level_filter: Optional[str] = None) -> List[str]:
    """Reads the last `n` lines of the *current* bot log file (the
    rotated .1/.2/... backups aren't included here -- this is meant for a
    quick live glance from the admin panel; use "ارسال فایل لاگ کامل" to
    get the whole thing, including anything already rotated out).
    `level_filter`, if given (e.g. "ERROR"), keeps only lines logged at
    that exact level, matched on-field rather than a raw substring search
    so a log message that happens to contain the word "error" in its text
    doesn't get pulled into an "ERROR" filter."""
    if not os.path.exists(_log_file_path):
        return []
    try:
        with open(_log_file_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError as e:
        logger.warning(f"Could not read log file for admin viewer: {e}")
        return []
    if level_filter:
        filtered = []
        for line in lines:
            parts = line.split(" - ", 3)
            if len(parts) >= 3 and parts[2].strip() == level_filter:
                filtered.append(line)
        lines = filtered
    return [ln.rstrip("\n") for ln in lines[-n:]]

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
    # Let any background write already in flight finish first, then do one
    # last fully-awaited save so shutdown always leaves the file current
    # even if request_save()'s background task hadn't run yet.
    await store.wait_for_pending_save()
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
        # PERFORMANCE: this used to be `await store.save()` here -- a full
        # serialize + JSON dump + os.fsync of the *entire* database, done
        # while still holding store.lock. That meant every single button
        # tap blocked on a whole-database disk write before Telegram even
        # got a reply, AND every other interaction anywhere in the bot
        # queued up behind it. request_save() takes a consistent snapshot
        # right now (cheap, in-memory) and lets the actual disk write
        # happen in the background after the lock is released below, so
        # replying to the tap doesn't wait on disk I/O. See json_orm.py's
        # JsonStore.request_save for the full explanation.
        store.request_save()
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
    # "حالت آرامش" -- opt-in per-user calm mode. When on, this user counts
    # toward the group's ModeDetector calm threshold regardless of load/
    # time of day, and their own session candidates get scored with a
    # shorter comfort/challenge ceiling (see RecommendationEngine.recommend).
    calm_mode_enabled = Column(Boolean, default=False, nullable=False)
    preferred_study_hours = Column(JSON, default=list, nullable=True)
    # Proactive daily plan: whether the bot pushes an unsolicited
    # personalized study plan once a day (see _daily_personal_plan_broadcast),
    # and where it should land -- "private" (DM only), "group" (every active
    # group this user belongs to), or "both".
    daily_plan_enabled = Column(Boolean, default=True, nullable=False)
    proactive_delivery = Column(String(10), default="private", nullable=False)
    # How much rest to place between two parts in the auto-generated daily
    # plan (see Advisor.generate_personalized_plan). This used to be a
    # hardcoded "+3 hours" regardless of session length or user choice --
    # now it's an actual per-user setting (see settings:gap), with a
    # spacing-effect-appropriate default.
    plan_gap_minutes = Column(Integer, default=60, nullable=False)
    # How many minutes from "right now" the '🆕 برنامه جدید' rebuild button
    # (see cb_plan_new) should schedule the first part of the fresh plan to
    # start. Defaults to 10 minutes; user-tunable from ⚙️ تنظیمات › شروع
    # برنامه جدید (see settings:plan_start_offset / SET_PLAN_START_OFFSET).
    plan_new_start_offset_minutes = Column(Integer, default=10, nullable=False)


class UserPlan(Base):
    """One generated daily-plan 'shell' for a user on a given calendar day.
    Created every time a plan is (re)built -- the normal /plan, or the
    '🆕 برنامه جدید' rebuild -- so each part of that specific plan can carry
    its own done/not-done checkbox (see UserPlanPart). Regenerating a plan
    replaces the previous one for *today* instead of stacking; see
    PlanRepository.replace_today_plan."""
    __tablename__ = "user_plans"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    # Midnight (local calendar day) that this plan is for -- used as the
    # key to find/replace "today's" plan regardless of what time it was
    # actually generated at.
    plan_day = Column(DateTime, nullable=False)
    # "auto": the usual history-based /plan. "manual": built by the
    # '🆕 برنامه جدید' button, starting plan_new_start_offset_minutes
    # minutes from when it was generated instead of the usual estimated
    # start hour.
    source = Column(String(10), default="auto", nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class UserPlanPart(Base):
    """A single part (پارت) inside a UserPlan, with its own done/not-done
    checkbox -- toggled from the inline keyboard under the plan message
    (see plan_keyboard / cb_toggle_plan_part)."""
    __tablename__ = "user_plan_parts"
    id = Column(Integer, primary_key=True, index=True)
    plan_id = Column(Integer, ForeignKey("user_plans.id"), nullable=False)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    part_index = Column(Integer, nullable=False)  # 0-based position within the plan
    scheduled_label = Column(String(5), nullable=False)  # "HH:MM" as shown in the plan text
    duration_minutes = Column(Integer, nullable=False)
    activity_name = Column(String(100), nullable=True)
    is_done = Column(Boolean, default=False, nullable=False)
    done_at = Column(DateTime, nullable=True)


class UserGoal(Base):
    __tablename__ = "user_goals"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    daily_goal = Column(Integer, default=90, nullable=False)
    weekly_goal = Column(Integer, default=450, nullable=False)
    monthly_goal = Column(Integer, default=1800, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class UserActivity(Base):
    """A subject/task the user is actually studying (e.g. 'ریاضی',
    'پایان‌نامه', 'آیلتس'), each carrying its own importance and an
    optional deadline -- lets the daily plan allocate time and ordering
    the way a real advisor would ('this matters more, this is due
    sooner'), instead of treating the whole day as one undifferentiated
    block. See Advisor._activity_weight for how importance + deadline
    combine into a single priority number, and
    Advisor.generate_personalized_plan for how that number turns into
    minutes and slot order."""
    __tablename__ = "user_activities"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    name = Column(String(100), nullable=False)
    # 1 = کم‌اهمیت ... 5 = بسیار حیاتی. Self-reported by the user, exactly
    # like asking "on a scale of 1-5" -- this is the "importance" axis.
    importance = Column(Integer, default=3, nullable=False)
    # Optional; combined with `importance` to produce urgency -- the
    # "how soon" axis, deliberately kept separate from importance itself
    # (an Eisenhower-style importance x urgency split) so a low-importance
    # but due-tomorrow task can still outrank a high-importance, far-off one.
    deadline = Column(DateTime, nullable=True)
    active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    __table_args__ = (
        Index("idx_user_activities_user_id", "user_id"),
    )


class UserRoutine(Base):
    """A personal routine/task the user sets up for themself (بخش
    روتین) -- e.g. 'ورزش صبحگاهی', 'مرور لغات', 'کتاب خوندن قبل خواب'.

    Two independent axes, matching how routine/habit-tracking apps
    (Habitica, Loop Habit Tracker, Streaks, ...) and the underlying
    habit-formation literature (habits form fastest around a fixed
    cue -- a specific time -- and a fixed, small, repeatable action)
    split the same problem:

      `kind` -- WHAT happens when it fires:
        - "reminder": a plain notification, nothing else. For things
          that aren't study blocks (دارو، ورزش، خواب، آب خوردن...).
        - "part": fires like a real study پارت -- shows the same
          "انجام دادم / انجام ندادم" choice a session gets, and feeds
          a per-routine streak so the user gets the same
          completion-visible feedback loop that makes habit trackers
          work (self-monitoring is one of the better-evidenced levers
          for habit formation).

      `repeat_type` -- WHEN it fires:
        - "once": a single occurrence on `once_date`, then the routine
          deactivates itself (see _routine_fire).
        - "daily": every day at hour:minute.
        - "weekly": only on the weekdays listed in `days_of_week`
          (0=شنبه ... 6=جمعه -- see PERSIAN_DAY_TO_CRON for the
          translation into APScheduler's day_of_week values).

    current_streak/longest_streak/last_done_date back the "✅ انجام شد"
    tap on a fired reminder/part (see RoutineRepository.mark_done) --
    lightweight positive reinforcement without touching the group-based
    Session/SessionResult/DailyStatistics pipeline, which is a separate,
    group-scoped concept these personal routines don't participate in.
    """
    __tablename__ = "user_routines"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    title = Column(String(100), nullable=False)
    kind = Column(String(10), default="reminder", nullable=False)       # "reminder" | "part"
    repeat_type = Column(String(10), default="daily", nullable=False)   # "once" | "daily" | "weekly"
    days_of_week = Column(JSON, default=list, nullable=True)            # [0..6], only for "weekly"
    once_date = Column(DateTime, nullable=True)                         # only for "once"
    hour = Column(Integer, nullable=False)
    minute = Column(Integer, default=0, nullable=False)
    duration_minutes = Column(Integer, nullable=True)                   # only for kind == "part"
    active = Column(Boolean, default=True, nullable=False)
    current_streak = Column(Integer, default=0, nullable=False)
    longest_streak = Column(Integer, default=0, nullable=False)
    last_done_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    __table_args__ = (
        Index("idx_user_routines_user_id", "user_id"),
        Index("idx_user_routines_active", "active"),
    )


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
    # Telegram message_id of the attendance-poll message ("کیا هستن؟" /
    # "آماده پارت هستید؟", the one with the 🟢 هستم button) so it can be
    # auto-deleted once the part actually starts and the announcement
    # message replaces it -- see _attendance_timeout.
    attendance_message_id = Column(Integer, nullable=True)
    # Telegram message_id of the "پارت شروع شد" roster announcement (see
    # _attendance_timeout). Kept so the roster can be edited in place as
    # people report their result -- see _refresh_roster_message -- instead
    # of only ever reflecting who was ready at the moment the part started.
    start_message_id = Column(Integer, nullable=True)
    # Free-text subject/lesson label for this group part (e.g. "ریاضی"),
    # set right after creation via the topic-picker (see cmd_session /
    # cb_session_topic_pick) or later with /topic. Nullable/optional --
    # unset for automatic polls nobody has tagged yet. This is what makes
    # a per-subject report possible for group parts (see cmd_report);
    # before this there was no way to know what a group part was even
    # about.
    topic = Column(String(64), nullable=True)
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
    # Consecutive fully-missed polls in a row (zero "هستم" taps). Reset to
    # 0 the moment a poll gets at least one volunteer. Once this reaches
    # config.MISS_STREAK_PAUSE_THRESHOLD, _create_readiness_poll refuses
    # to fire again automatically (including the daily morning cron)
    # until a real human message/tap resets it via GroupSyncMiddleware --
    # so a genuinely quiet group stops getting pinged every day.
    consecutive_misses = Column(Integer, default=0, nullable=False)
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
    """Generic persisted key/value settings (e.g. backup_interval_minutes)
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


async def get_backup_interval_minutes() -> int:
    raw = await get_setting("backup_interval_minutes")
    if raw is None:
        return config.BACKUP_INTERVAL_MINUTES
    try:
        minutes = int(raw)
        return minutes if minutes > 0 else config.BACKUP_INTERVAL_MINUTES
    except (TypeError, ValueError):
        return config.BACKUP_INTERVAL_MINUTES


async def get_backup_target_chat_id() -> Optional[int]:
    """The numeric chat ID (group/channel/person) an admin picked for the
    recurring backup, via ⏱ فاصله پشتیبان‌گیری -> 🎯 تنظیم مقصد. Falls
    back, in order, to the persisted setting, then
    config.BACKUP_TARGET_CHAT_ID, then the bot owner (first ADMIN_IDS
    entry) if neither is set -- so the job always has *someone* to send
    to, per the user's "اگه چیزی مشخص نبود به خود مالک بفرسته" request."""
    raw = await get_setting("backup_target_chat_id")
    if raw is not None:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    if config.BACKUP_TARGET_CHAT_ID is not None:
        return config.BACKUP_TARGET_CHAT_ID
    return config.ADMIN_IDS[0] if config.ADMIN_IDS else None


async def set_backup_target_chat_id(chat_id: Optional[int]) -> None:
    """Persists the chosen target, or clears it (chat_id=None) to fall
    back to the owner again."""
    await set_setting("backup_target_chat_id", str(chat_id) if chat_id is not None else "")


# ============================================================
# RUNTIME-TUNABLE SETTINGS (admin panel, no code/redeploy needed)
# ============================================================
# Every entry here used to only be changeable by editing Config and
# redeploying. Each is a plain number that lives on the `config` object;
# an admin change here (a) persists to BotSetting so it survives a
# restart, and (b) is applied immediately with `setattr(config, ...)` so
# every place in the code that reads e.g. `config.MIN_BREAK_MINUTES`
# picks up the new value right away -- no signature changes needed
# anywhere else in the codebase.
TUNABLE_SETTINGS: Dict[str, Dict[str, Any]] = {
    "min_session_duration": {
        "label": "حداقل مدت مجاز پارت", "attr": "DEFAULT_MIN_DURATION",
        "type": int, "min": 5, "max": 60, "unit": "دقیقه",
    },
    "max_session_duration": {
        "label": "حداکثر مدت مجاز پارت", "attr": "DEFAULT_MAX_DURATION",
        "type": int, "min": 30, "max": 240, "unit": "دقیقه",
    },
    "min_break_minutes": {
        "label": "حداقل استراحت پیشنهادی بین پارت‌ها", "attr": "MIN_BREAK_MINUTES",
        "type": int, "min": 1, "max": 60, "unit": "دقیقه",
    },
    "ultradian_ceiling_minutes": {
        "label": "سقف زمانی یک پارت پیوسته (قبلش امتیاز method_fit افت می‌کنه)",
        "attr": "ULTRADIAN_CEILING_MINUTES", "type": int, "min": 30, "max": 240, "unit": "دقیقه",
    },
    "miss_streak_pause_threshold": {
        "label": "تعداد غیبت متوالی تا توقف نظرسنجی خودکار گروه",
        "attr": "MISS_STREAK_PAUSE_THRESHOLD", "type": int, "min": 1, "max": 20, "unit": "بار",
    },
    "backup_keep_last": {
        "label": "تعداد فایل‌های پشتیبان نگه‌داشته‌شده", "attr": "BACKUP_KEEP_LAST",
        "type": int, "min": 1, "max": 100, "unit": "فایل",
    },
    "morning_poll_hour": {
        "label": "ساعت نظرسنجی صبحگاهی گروه‌ها", "attr": "MORNING_POLL_HOUR",
        "type": int, "min": 0, "max": 23, "unit": "ساعت (۰-۲۳)", "on_change": "reschedule_morning_poll_job",
    },
    "morning_poll_minute": {
        "label": "دقیقه‌ی نظرسنجی صبحگاهی گروه‌ها", "attr": "MORNING_POLL_MINUTE",
        "type": int, "min": 0, "max": 59, "unit": "دقیقه (۰-۵۹)", "on_change": "reschedule_morning_poll_job",
    },
    "daily_plan_hour": {
        "label": "ساعت ارسال خودکار برنامه شخصی روزانه", "attr": "DAILY_PLAN_HOUR",
        "type": int, "min": 0, "max": 23, "unit": "ساعت (۰-۲۳)", "on_change": "reschedule_daily_plan_job",
    },
    "daily_plan_minute": {
        "label": "دقیقه‌ی ارسال خودکار برنامه شخصی روزانه", "attr": "DAILY_PLAN_MINUTE",
        "type": int, "min": 0, "max": 59, "unit": "دقیقه (۰-۵۹)", "on_change": "reschedule_daily_plan_job",
    },
}


async def load_persisted_tunables() -> None:
    """Called once at startup (see on_startup): pulls any admin-saved
    overrides for TUNABLE_SETTINGS and ALGORITHM_WEIGHTS out of the DB and
    applies them onto the live `config` object, so a change made from the
    panel yesterday survives a restart instead of silently reverting to
    the hardcoded default the next time the process starts."""
    async with get_session() as session:
        for key, meta in TUNABLE_SETTINGS.items():
            result = await session.execute(select(BotSetting).where(BotSetting.key == key))
            row = result.scalar_one_or_none()
            if row is None or row.value is None:
                continue
            try:
                value = meta["type"](row.value)
            except (TypeError, ValueError):
                logger.warning(f"Ignoring invalid persisted value for tunable '{key}': {row.value!r}")
                continue
            setattr(config, meta["attr"], value)

        result = await session.execute(select(AlgorithmSetting))
        for row in result.scalars().all():
            if row.key not in config.ALGORITHM_WEIGHTS:
                continue
            try:
                config.ALGORITHM_WEIGHTS[row.key] = float(row.value)
            except (TypeError, ValueError):
                logger.warning(f"Ignoring invalid persisted algorithm weight '{row.key}': {row.value!r}")


async def set_tunable(key: str, raw_value: str) -> Tuple[bool, str]:
    """Validates + persists + immediately applies a single TUNABLE_SETTINGS
    entry. Returns (ok, message-to-show-the-admin)."""
    meta = TUNABLE_SETTINGS[key]
    try:
        value = meta["type"](raw_value)
        if not (meta["min"] <= value <= meta["max"]):
            raise ValueError
    except (TypeError, ValueError):
        return False, f"❌ مقدار باید یک عدد بین {meta['min']} تا {meta['max']} باشد."
    await set_setting(key, str(value))
    setattr(config, meta["attr"], value)
    hook = meta.get("on_change")
    if hook:
        await globals()[hook]()
    return True, f"✅ «{meta['label']}» روی {value} {meta['unit']} تنظیم شد."


async def set_algorithm_weight(key: str, raw_value: str) -> Tuple[bool, str]:
    """Same idea as set_tunable but for the ALGORITHM_WEIGHTS floats,
    persisted separately in AlgorithmSetting (kept distinct because these
    are relative scoring weights, not real-world units like minutes)."""
    if key not in config.ALGORITHM_WEIGHTS:
        return False, "❌ وزن نامعتبر."
    try:
        value = float(raw_value)
        if not (0.0 <= value <= 2.0):
            raise ValueError
    except (TypeError, ValueError):
        return False, "❌ مقدار باید عددی بین ۰ تا ۲ باشد (مثلاً 0.15)."
    async with get_session() as session:
        result = await session.execute(select(AlgorithmSetting).where(AlgorithmSetting.key == key))
        row = result.scalar_one_or_none()
        if row:
            row.value = str(value)
        else:
            session.add(AlgorithmSetting(key=key, value=str(value)))
    config.ALGORITHM_WEIGHTS[key] = value
    return True, f"✅ وزن «{key}» روی {value} تنظیم شد."


# ------------------------------------------------------------------
# PER-GROUP TUNABLE SETTINGS (admin panel)
# ------------------------------------------------------------------
# GroupSetting has held these numeric knobs since the beginning
# (default_duration, quiet hours, poll window, ...) but there was never
# any admin screen to change them -- the only way to edit a group's
# settings was a direct DB edit. Every field below is now editable from
# 👥 گروه‌ها in the admin panel.
GROUP_TUNABLE_FIELDS: Dict[str, Dict[str, Any]] = {
    "default_duration": {"label": "مدت پیش‌فرض هر پارت", "min": 10, "max": 120, "unit": "دقیقه"},
    "attendance_window": {"label": "مهلت پاسخ به نظرسنجی حضور", "min": 30, "max": 900, "unit": "ثانیه"},
    "morning_default": {"label": "مدت پیش‌فرض صبح", "min": 10, "max": 120, "unit": "دقیقه"},
    "evening_default": {"label": "مدت پیش‌فرض عصر", "min": 10, "max": 120, "unit": "دقیقه"},
    "late_night_default": {"label": "مدت پیش‌فرض شب دیروقت", "min": 10, "max": 120, "unit": "دقیقه"},
    "min_session": {"label": "حداقل مدت مجاز پارت این گروه", "min": 5, "max": 60, "unit": "دقیقه"},
    "max_session": {"label": "حداکثر مدت مجاز پارت این گروه", "min": 30, "max": 240, "unit": "دقیقه"},
    "growth_step": {"label": "گام افزایش در حالت رشد", "min": 1, "max": 30, "unit": "دقیقه"},
    "recovery_step": {"label": "گام کاهش در حالت بازیابی", "min": 1, "max": 30, "unit": "دقیقه"},
    "notification_limit": {"label": "سقف تعداد اعلان روزانه", "min": 1, "max": 50, "unit": "اعلان"},
    "poll_window_minutes": {"label": "مهلت نظرسنجی حضور خودکار", "min": 1, "max": 60, "unit": "دقیقه"},
    "quiet_hour_start": {"label": "شروع ساعات سکوت (بدون نظرسنجی خودکار)", "min": 0, "max": 23, "unit": "ساعت"},
    "quiet_hour_end": {"label": "پایان ساعات سکوت", "min": 0, "max": 23, "unit": "ساعت"},
}


async def set_group_field(group_id: int, field: str, raw_value: str) -> Tuple[bool, str]:
    meta = GROUP_TUNABLE_FIELDS[field]
    try:
        value = int(raw_value)
        if not (meta["min"] <= value <= meta["max"]):
            raise ValueError
    except (TypeError, ValueError):
        return False, f"❌ مقدار باید عددی صحیح بین {meta['min']} تا {meta['max']} باشد."
    async with get_session() as session:
        result = await session.execute(select(GroupSetting).where(GroupSetting.group_id == group_id))
        settings = result.scalar_one_or_none()
        if not settings:
            return False, "❌ این گروه تنظیماتی ندارد."
        setattr(settings, field, value)
    return True, f"✅ «{meta['label']}» روی {value} {meta['unit']} تنظیم شد."


async def toggle_group_auto_poll(group_id: int) -> Tuple[bool, bool, str]:
    """Flips GroupSetting.auto_poll_enabled for one group. Used by both the
    central admin panel (gset-style, see group_settings_keyboard) and the
    in-group /autopoll command (see cmd_autopoll) -- one shared code path
    so the two entry points can never disagree about what "off" means.

    Turning it back ON also clears awaiting_activity/consecutive_misses:
    otherwise a group that had auto-paused itself (see
    MISS_STREAK_PAUSE_THRESHOLD in _create_readiness_poll) would look
    "on" again in the UI but silently stay paused until a real message
    came in -- confusing for whoever just pressed the button expecting
    it to actually resume.

    Returns (ok, new_enabled_value, feedback_message).
    """
    async with get_session() as session:
        result = await session.execute(select(GroupSetting).where(GroupSetting.group_id == group_id))
        settings = result.scalar_one_or_none()
        if not settings:
            return False, False, "❌ این گروه تنظیماتی ندارد."
        settings.auto_poll_enabled = not settings.auto_poll_enabled
        new_value = settings.auto_poll_enabled
        if new_value:
            settings.awaiting_activity = False
            settings.consecutive_misses = 0
    if new_value:
        return True, True, "🟢 پارت خودکار روشن شد. از این به بعد صبح‌ها و بین پارت‌ها دوباره نظرسنجی حضور می‌فرستم."
    return True, False, "⚪️ پارت خودکار خاموش شد. تا وقتی خودتون دوباره روشنش نکنید، دیگه نظرسنجی خودکار پارت نمی‌فرستم (دستور /session همچنان کار می‌کنه)."


async def _is_group_chat_admin(bot_instance: Bot, chat_id: int, user_id: int) -> bool:
    """True if user_id is an owner/admin of the Telegram chat chat_id, or a
    global bot admin (config.ADMIN_IDS -- they can manage every group's
    settings regardless of their Telegram role in that specific chat).
    Used to gate the in-group /autopoll toggle to people who actually run
    that group, without requiring the central admin panel."""
    if user_id in config.ADMIN_IDS:
        return True
    try:
        member = await bot_instance.get_chat_member(chat_id, user_id)
    except Exception as e:
        logger.debug(f"Could not check chat-admin status for {user_id} in {chat_id}: {e}")
        return False
    return getattr(member, "status", None) in ("administrator", "creator")


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

    async def get_by_username(self, username: str) -> Optional[User]:
        """Case-insensitive lookup by Telegram @username (Telegram usernames
        are themselves case-insensitive, so a stored 'AliReza' must still
        match a lookup for 'alireza'). Strips a leading '@' if present so
        callers can pass either form.

        json_orm's query layer only supports count/sum in its `func`
        namespace (no `lower()`/`ilike` at the query level -- see
        json_orm.py's _FuncNamespace), so the case-insensitive compare has
        to happen in Python after fetching, not as part of the query
        itself."""
        clean = username.strip().lstrip("@")
        if not clean:
            return None
        result = await self.session.execute(
            select(User).where(User.username != None)
        )
        clean_lower = clean.lower()
        for user in result.scalars().all():
            if (user.username or "").lower() == clean_lower:
                return user
        return None

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
            # Keep contact info fresh even for people who never ran /start
            # in DM -- GroupSyncMiddleware calls get_or_create on every
            # group message/tap, so for them this is the *only* place
            # their username/name ever gets (re)saved. Previously only
            # last_seen_at was touched here, so someone who joined a group,
            # was auto-created with whatever name they had that day, then
            # later changed their Telegram name or set a username, would
            # stay stuck with the stale value forever.
            for field in ("username", "first_name", "last_name"):
                value = kwargs.get(field)
                if value is not None and getattr(user, field) != value:
                    setattr(user, field, value)
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

    async def get_all_active(self) -> List[Group]:
        """Every active group, for the admin panel's per-group settings
        picker (see admin:groups)."""
        result = await self.session.execute(select(Group).where(Group.active == True))
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


class ActivityRepository:
    """Persistence for UserActivity -- the subjects/tasks a user tells the
    bot about (via /activities) along with their importance and optional
    deadline, used to make the daily plan topic-aware instead of just
    duration-aware."""
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_active_by_user(self, user_id: int) -> List["UserActivity"]:
        result = await self.session.execute(
            select(UserActivity)
            .where(UserActivity.user_id == user_id, UserActivity.active == True)
            .order_by(desc(UserActivity.importance), UserActivity.created_at)
        )
        return result.scalars().all()

    async def get_by_id(self, activity_id: int, user_id: int) -> Optional["UserActivity"]:
        result = await self.session.execute(
            select(UserActivity).where(
                UserActivity.id == activity_id, UserActivity.user_id == user_id
            )
        )
        return result.scalar_one_or_none()

    async def create(self, user_id: int, name: str, importance: int,
                      deadline: Optional[datetime] = None) -> "UserActivity":
        activity = UserActivity(
            user_id=user_id, name=name.strip()[:100], importance=importance, deadline=deadline
        )
        self.session.add(activity)
        await self.session.flush()
        return activity

    async def set_importance(self, activity_id: int, user_id: int, importance: int) -> Optional["UserActivity"]:
        activity = await self.get_by_id(activity_id, user_id)
        if activity:
            activity.importance = importance
            await self.session.flush()
        return activity

    async def deactivate(self, activity_id: int, user_id: int) -> bool:
        activity = await self.get_by_id(activity_id, user_id)
        if activity:
            activity.active = False
            await self.session.flush()
            return True
        return False


class RoutineRepository:
    """Persistence for UserRoutine -- see the model docstring for the
    kind/repeat_type design. Mirrors ActivityRepository's shape."""
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(
        self, user_id: int, title: str, kind: str, repeat_type: str,
        hour: int, minute: int = 0, days_of_week: Optional[List[int]] = None,
        once_date: Optional[datetime] = None, duration_minutes: Optional[int] = None,
    ) -> "UserRoutine":
        routine = UserRoutine(
            user_id=user_id, title=title.strip()[:100], kind=kind, repeat_type=repeat_type,
            hour=hour, minute=minute, days_of_week=days_of_week or [],
            once_date=once_date, duration_minutes=duration_minutes, active=True,
        )
        self.session.add(routine)
        await self.session.flush()
        return routine

    async def get_active_by_user(self, user_id: int) -> List["UserRoutine"]:
        result = await self.session.execute(
            select(UserRoutine)
            .where(UserRoutine.user_id == user_id, UserRoutine.active == True)
            .order_by(UserRoutine.hour, UserRoutine.minute)
        )
        return result.scalars().all()

    async def get_by_id(self, routine_id: int, user_id: int) -> Optional["UserRoutine"]:
        result = await self.session.execute(
            select(UserRoutine).where(
                UserRoutine.id == routine_id, UserRoutine.user_id == user_id
            )
        )
        return result.scalar_one_or_none()

    async def get_all_active(self) -> List["UserRoutine"]:
        """Every active routine across every user -- used once at
        startup to re-register APScheduler jobs, since APScheduler's
        default in-memory jobstore doesn't survive a restart (see
        reschedule_all_routines)."""
        result = await self.session.execute(
            select(UserRoutine).where(UserRoutine.active == True)
        )
        return result.scalars().all()

    async def deactivate(self, routine_id: int, user_id: int) -> bool:
        routine = await self.get_by_id(routine_id, user_id)
        if routine:
            routine.active = False
            await self.session.flush()
            return True
        return False

    async def mark_done(self, routine_id: int) -> Optional[int]:
        """Bumps the streak for a fired routine's '✅ انجام شد' tap.
        Returns the new streak, or None if the routine no longer
        exists/is inactive. Tapping twice for the same calendar day is
        a no-op (returns the already-current streak) so double-taps
        can't inflate it."""
        result = await self.session.execute(select(UserRoutine).where(UserRoutine.id == routine_id))
        routine = result.scalar_one_or_none()
        if not routine or not routine.active:
            return None
        today = time_service.now().date()
        if routine.last_done_date and routine.last_done_date.date() == today:
            return routine.current_streak
        routine.current_streak += 1
        routine.longest_streak = max(routine.longest_streak, routine.current_streak)
        routine.last_done_date = time_service.now()
        await self.session.flush()
        return routine.current_streak

    async def mark_missed(self, routine_id: int) -> None:
        result = await self.session.execute(select(UserRoutine).where(UserRoutine.id == routine_id))
        routine = result.scalar_one_or_none()
        if routine:
            routine.current_streak = 0
            await self.session.flush()


class PlanRepository:
    """Persistence for UserPlan/UserPlanPart -- the per-part done/not-done
    checklist backing the inline buttons under the daily plan message (see
    plan_keyboard, cb_toggle_plan_part, cb_plan_new)."""
    def __init__(self, session: AsyncSession):
        self.session = session

    async def replace_today_plan(
        self, user_id: int, parts: List[Dict[str, Any]], source: str = "auto"
    ) -> Tuple["UserPlan", List["UserPlanPart"]]:
        """Clears out any existing plan for *today* for this user and
        creates a fresh one from `parts` (each a dict with scheduled_label
        / duration_minutes / activity_name). Regenerating a plan -- whether
        the normal daily one or the '🆕 برنامه جدید' rebuild -- replaces
        rather than stacking, so stale checkboxes from an earlier plan the
        same day don't linger under a different keyboard."""
        today_start = time_service.start_of_day()
        result = await self.session.execute(
            select(UserPlan).where(UserPlan.user_id == user_id, UserPlan.plan_day == today_start)
        )
        for old_plan in result.scalars().all():
            await self.session.execute(delete(UserPlanPart).where(UserPlanPart.plan_id == old_plan.id))
            await self.session.execute(delete(UserPlan).where(UserPlan.id == old_plan.id))

        plan = UserPlan(user_id=user_id, plan_day=today_start, source=source)
        self.session.add(plan)
        await self.session.flush()

        new_parts: List["UserPlanPart"] = []
        for i, part in enumerate(parts):
            row = UserPlanPart(
                plan_id=plan.id, user_id=user_id, part_index=i,
                scheduled_label=part["scheduled_label"],
                duration_minutes=part["duration_minutes"],
                activity_name=part.get("activity_name"),
            )
            self.session.add(row)
            new_parts.append(row)
        await self.session.flush()
        return plan, new_parts

    async def get_today_plan_with_parts(self, user_id: int) -> Tuple[Optional["UserPlan"], List["UserPlanPart"]]:
        today_start = time_service.start_of_day()
        result = await self.session.execute(
            select(UserPlan)
            .where(UserPlan.user_id == user_id, UserPlan.plan_day == today_start)
            .order_by(desc(UserPlan.created_at))
        )
        plan = result.scalars().first()
        if not plan:
            return None, []
        parts_result = await self.session.execute(
            select(UserPlanPart).where(UserPlanPart.plan_id == plan.id).order_by(UserPlanPart.part_index)
        )
        return plan, parts_result.scalars().all()

    async def toggle_part(self, part_id: int, user_id: int) -> Optional["UserPlanPart"]:
        result = await self.session.execute(
            select(UserPlanPart).where(UserPlanPart.id == part_id, UserPlanPart.user_id == user_id)
        )
        part = result.scalar_one_or_none()
        if not part:
            return None
        part.is_done = not part.is_done
        part.done_at = datetime.utcnow() if part.is_done else None
        await self.session.flush()
        return part


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
        created earlier and claimed ids 1-3.

        Also excludes MISSED sessions (an automatic/manual poll nobody
        answered) -- those never actually became a part anyone attended,
        so counting them inflated the number a real part got labeled
        with (e.g. a group's 4th real part showing as "پارت #6" because
        two earlier polls went unanswered)."""
        result = await self.session.execute(
            select(func.count()).select_from(Session).where(
                Session.group_id == group_id,
                Session.id <= session_id,
                Session.status != "MISSED",
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
        # `date` arrives as a tz-aware Asia/Tehran datetime (from
        # time_service.now()), but SessionResult.reported_at is stored as
        # naive UTC (datetime.utcnow()). The previous fix here just
        # stripped tzinfo off the Tehran datetime and compared it directly
        # to the naive-UTC column -- that avoids the TypeError, but the
        # start/end-of-day boundaries it produces are still Tehran wall-
        # clock numbers being compared against UTC timestamps, off by
        # Tehran's UTC+03:30 offset. In practice that silently misfiled
        # every session reported between ~20:30 and 00:00 Tehran time into
        # the *next* day's stats (and vice versa near the other boundary).
        # Fix: compute the Tehran calendar day's start/end, then convert
        # those tz-aware boundaries to naive UTC before querying, so we're
        # comparing like with like.
        if date.tzinfo is None:
            date = time_service.tz.localize(date)
        tehran_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
        tehran_end = date.replace(hour=23, minute=59, second=59, microsecond=999999)
        start = tehran_start.astimezone(pytz.UTC).replace(tzinfo=None)
        end = tehran_end.astimezone(pytz.UTC).replace(tzinfo=None)
        # DailyStatistics.date is keyed on the Tehran calendar day (naive,
        # no time component semantics beyond "which day"), so store/query
        # it using the Tehran-local start-of-day, not the UTC-shifted one.
        stats_date = tehran_start.replace(tzinfo=None)
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
                DailyStatistics.date == stats_date
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
                user_id=user_id, date=stats_date, sessions=sessions, minutes=total_minutes,
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
        # `recent[i]` vs `daily_stats[i]` compared elements at the same
        # *index* in two differently-sized/offset lists -- once daily_stats
        # held more than 3 days, that silently compared the most recent
        # days against the oldest ones instead of against each other, so
        # the UPWARD/DOWNWARD classification was essentially noise after
        # the first week. Sort chronologically and compare consecutive
        # days within `recent` itself.
        daily_stats_sorted = sorted(daily_stats, key=lambda d: d.date)
        if len(daily_stats_sorted) >= 3:
            recent = daily_stats_sorted[-3:]
            if recent[0].minutes < recent[1].minutes < recent[2].minutes:
                trend = "UPWARD"
            elif recent[0].minutes > recent[1].minutes > recent[2].minutes:
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
    def detect(self, profiles: List[UserBehaviorProfile], calm_enabled_ids: Optional[set] = None) -> str:
        calm_enabled_ids = calm_enabled_ids or set()
        recovery_count = 0
        growth_count = 0
        calm_count = 0
        total = len(profiles)
        for p in profiles:
            if p.miss_streak >= 2 or p.starting_friction_score > 0.6:
                recovery_count += 1
            if p.success_streak >= 3 and p.completion_rate >= 0.8:
                growth_count += 1
            # A participant pulls the group toward calm mode either
            # automatically (overloaded / late night) or because they
            # explicitly turned on "حالت آرامش" in their settings.
            if p.current_load > 90 or time_service.is_late_night() or p.user_id in calm_enabled_ids:
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
                  current_time: datetime, mode: str, weights: Optional[Dict[str, float]] = None) -> float:
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
            preferred = cap.get("preferred_duration")
            if preferred:
                # The user explicitly set this in ⚙️ تنظیمات → مدت ترجیحی.
                # An explicit preference should carry as much weight as the
                # inferred comfort/challenge capacity above, so it's
                # averaged in at equal weight rather than just nudging pf.
                closeness = max(0.0, 1 - abs(duration - preferred) / max(preferred, 1))
                pf = (pf + closeness) / 2
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

        # Method fit -- how well this duration matches evidence-based
        # study-block lengths (config.FOCUS_SWEET_SPOTS), independent of
        # this particular person/group's own history. Peaks at the
        # nearest sweet spot and fades out past the ultradian ceiling,
        # where sustained-attention research says a single block reliably
        # stops paying off. This is what keeps the engine from drifting
        # toward "technically fits everyone's capacity" durations that
        # ignore how attention actually behaves (e.g. a flat 100-minute
        # block nobody's profile explicitly penalizes).
        closest_sweet_spot = min(config.FOCUS_SWEET_SPOTS, key=lambda s: abs(duration - s))
        method_fit = max(0.0, 1 - abs(duration - closest_sweet_spot) / 30)
        if duration > config.ULTRADIAN_CEILING_MINUTES:
            method_fit = max(0.0, method_fit - (duration - config.ULTRADIAN_CEILING_MINUTES) / 60)

        weights = weights or config.ALGORITHM_WEIGHTS
        score = (
            weights["GROUP_FIT_WEIGHT"] * group_fit +
            weights["PERSONAL_FIT_WEIGHT"] * personal_fit +
            weights["TIME_FIT_WEIGHT"] * time_score +
            weights["METHOD_FIT_WEIGHT"] * method_fit +
            weights["COMPLETION_PROB_WEIGHT"] * completion_prob +
            weights["PROGRESS_WEIGHT"] * progress +
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

        # Load profiles (and each participant's saved preferences, needed
        # for preferred_session_duration / calm_mode_enabled below)
        profiles = []
        prefs_by_uid: Dict[int, Optional["UserPreference"]] = {}
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

            pref_result = await session.execute(
                select(UserPreference).where(UserPreference.user_id == uid)
            )
            prefs_by_uid[uid] = pref_result.scalar_one_or_none()

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
            pref = prefs_by_uid.get(p.user_id)
            cap["preferred_duration"] = pref.preferred_session_duration if pref else None
            cap["calm_mode_enabled"] = bool(pref.calm_mode_enabled) if pref else False
            ind_caps.append(cap)

        # Group capacity
        group_cap = self.group_calc.calculate(ind_caps)

        # Mode detection
        calm_enabled_ids = {uid for uid, pref in prefs_by_uid.items() if pref and pref.calm_mode_enabled}
        mode = self.mode_detector.detect(profiles, calm_enabled_ids)

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
        if min(config.FOCUS_SWEET_SPOTS, key=lambda s: abs(target - s)) == target:
            # The chosen target lines up exactly with an evidence-anchored
            # study-block length (see Config.FOCUS_SWEET_SPOTS) -- worth
            # surfacing separately from the mode/time reasons above, since
            # it reflects the method_fit term in FitnessScorer rather than
            # anyone's personal history.
            reason_codes.append("EVIDENCE_BASED_DURATION")

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
                            completion_status: str,
                            manual_duration: Optional[int] = None) -> bool:
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

        # Elapsed time since the part actually started -- the ground truth
        # for "how long did this person actually study", independent of
        # what button they tapped or what was planned. Falls back to
        # scheduled_start only for the edge case where actual_start was
        # never set. to_tehran() normalizes tz-naive values instead of
        # raising on an aware-vs-naive comparison (see TimeService).
        start_reference = sess.actual_start or sess.scheduled_start
        elapsed_minutes = None
        if start_reference:
            elapsed_minutes = max(
                0,
                round((time_service.now() - time_service.to_tehran(start_reference)).total_seconds() / 60)
            )

        # Actual duration is never taken at face value from a button tap or
        # a typed number alone -- it's always bounded by real elapsed time
        # since the part actually started, and by extension_duration (the
        # "حداکثر" ceiling announced to the group and used to schedule
        # _session_end). "MISSED" always records 0 -- reporting "انجام
        # ندادم" means no study time is claimed, full stop.
        if completion_status == "COMPLETED":
            if manual_duration is not None:
                # "⏱ ثبت دستی میزان مطالعه" -- the person typed their own
                # number because the auto elapsed-time credit (which
                # includes any pause/interruption, not just active study)
                # can overstate what they actually studied. Still not
                # trusted outright: capped at both extension_duration and
                # real elapsed time since start, so nobody can claim more
                # minutes than have actually passed, let alone more than
                # the max window.
                ceiling = sess.extension_duration
                if elapsed_minutes is not None:
                    ceiling = min(ceiling, elapsed_minutes)
                actual_duration = max(0, min(manual_duration, ceiling))
            elif elapsed_minutes is not None:
                actual_duration = min(elapsed_minutes, sess.extension_duration)
            else:
                # No start timestamp to compare against at all (shouldn't
                # normally happen for a STARTED session) -- fall back to
                # the planned target rather than guessing.
                actual_duration = sess.planned_duration
        else:
            actual_duration = 0

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

# ============================================================
# PRESENTATION HELPERS (readiness rosters, shared message formatting)
# ============================================================
def _display_name(user: "User") -> str:
    """Best-effort human-readable name for a user: prefers their Telegram
    first name, falls back to @username, then a generic label. Used
    everywhere a roster of names is shown to the group."""
    name = (user.first_name or "").strip()
    if not name:
        name = f"@{user.username}" if user.username else "کاربر ناشناس"
    return name


def _mention_html(user: "User") -> str:
    """HTML mention for `user`, built on their numeric telegram_id (not
    @username) so it always works even for people with no username set,
    and -- unlike a plain name -- actually pings/notifies them the way a
    real Telegram tag does. Name text is html-escaped since this is only
    ever dropped into an HTML-parse-mode message."""
    return f'<a href="tg://user?id={user.telegram_id}">{html.escape(_display_name(user))}</a>'


# Status shown next to each name in the roster, keyed by SessionResult.completion_status.
# "PENDING" (no result row yet) intentionally uses a neutral circle, not red --
# red is reserved for an actual "انجام ندادم" tap, not silence.
_ROSTER_STATUS_EMOJI: Dict[str, str] = {
    "COMPLETED": "🟢",
    "PARTIAL": "🟡",
    "MISSED": "🔴",
    "PENDING": "⚪",
}


def _format_roster(users: List["User"], results: Optional[Dict[int, Tuple[str, Optional[int]]]] = None) -> str:
    """Renders a numbered roster for a "کیا هستن؟"/part-announcement style
    message. Each name is a real Telegram mention (tg://user?id=...) rather
    than plain text, so appearing in the roster also pings that person --
    the same reminder effect as being @-tagged. `results`, if given, maps
    user.id -> (completion_status, actual_duration); anyone missing from it
    is shown as still-pending (⚪), no minutes. Once someone reports, their
    reported study minutes show right next to their dot -- not just the
    color -- so the roster itself answers "کی چقدر خوند؟" without anyone
    having to run /report. Returns a friendly placeholder line if empty,
    instead of an empty/blank section."""
    if not users:
        return "— هنوز کسی اعلام آمادگی نکرده —"
    lines = []
    for i, u in enumerate(users, start=1):
        status, minutes = (results or {}).get(u.id, ("PENDING", None))
        dot = _ROSTER_STATUS_EMOJI.get(status, _ROSTER_STATUS_EMOJI["PENDING"])
        minutes_note = f" — <b>{minutes}</b> دقیقه" if minutes is not None else ""
        lines.append(f"{i}. {dot} {_mention_html(u)}{minutes_note}")
    return "\n".join(lines)


async def _get_session_results_map(db_session: "AsyncSession", session_id: int) -> Dict[int, Tuple[str, Optional[int]]]:
    """user_id -> (completion_status, actual_duration) for every result
    already reported on this session. Used to keep the roster's colored
    dots AND minutes (see _format_roster / _ROSTER_STATUS_EMOJI) in sync
    with who has actually tapped 🟢/🟡/🔴 so far, and how much they
    reported studying."""
    result = await db_session.execute(
        select(
            SessionResult.user_id, SessionResult.completion_status, SessionResult.actual_duration
        ).where(SessionResult.session_id == session_id)
    )
    return {row[0]: (row[1], row[2]) for row in result.all()}


async def _refresh_roster_message(session_id: int) -> None:
    """Edits the pinned "پارت شروع شد" roster message in place so its
    green/yellow/red dots reflect results as they come in, instead of
    forever showing everyone as pending from the moment the part started.
    Called after every successful report_result. Best-effort: a missing
    bot/group/message (or an unmodified-message no-op from Telegram)
    should never break the actual result-reporting flow that triggered it."""
    if not bot:
        return
    try:
        async with get_session() as db_session:
            manager = SessionManager(db_session)
            sess = await manager.get_session(session_id)
            if not sess or not sess.start_message_id:
                return
            result = await db_session.execute(select(Group).where(Group.id == sess.group_id))
            group = result.scalar_one_or_none()
            if not group:
                return
            ready_users = await _get_ready_users(db_session, session_id)
            results_map = await _get_session_results_map(db_session, session_id)
            ready_participants = await manager.get_participants(session_id)
            ready_count = sum(1 for p in ready_participants if p.attendance_status == "READY")
            part_number = await manager.get_group_part_number(sess.group_id, session_id)
            roster = _format_roster(ready_users, results_map)
            topic_line = f"📖 موضوع: <b>{html.escape(sess.topic)}</b>\n" if sess.topic else ""
            text = (
                f"🚀 <b>پارت #{part_number} شروع شد</b>\n"
                f"{topic_line}"
                f"➖➖➖➖➖➖➖➖➖➖\n"
                f"👥 <b>آمادگان ({ready_count} نفر):</b>\n"
                f"{roster}\n"
                f"➖➖➖➖➖➖➖➖➖➖\n"
                f"⏱ حداقل: <b>{sess.minimum_duration}</b> | هدف: <b>{sess.target_duration}</b> | "
                f"حداکثر: <b>{sess.extension_duration}</b> دقیقه\n"
                f"🧭 حالت: {sess.mode}\n\n"
                f"وقتی پارتت تموم شد بزن 👇 (حتی اگه اعلام آمادگی نکرده بودی)"
            )
            await bot.edit_message_text(
                chat_id=group.telegram_chat_id,
                message_id=sess.start_message_id,
                text=text,
                reply_markup=result_keyboard(session_id),
            )
    except TelegramBadRequest as e:
        # "message is not modified" (nothing actually changed) is routine,
        # not an error -- anything else is still worth a log line.
        if "not modified" not in str(e).lower():
            logger.debug(f"Could not refresh roster message for session {session_id}: {e}")
    except Exception as e:
        logger.debug(f"Could not refresh roster message for session {session_id}: {e}")


async def _get_ready_users(db_session: "AsyncSession", session_id: int) -> List["User"]:
    """Every user currently marked READY on this session, in the order
    they tapped 'هستم'. Used both by the live attendance-poll message (so
    it always shows the *full* roster of everyone who declared readiness,
    not just whoever tapped last) and by the part-start announcement."""
    result = await db_session.execute(
        select(SessionParticipant).where(
            SessionParticipant.session_id == session_id,
            SessionParticipant.attendance_status == "READY",
        )
    )
    participants = list(result.scalars().all())
    participants.sort(key=lambda p: p.ready_at or datetime.min)
    user_repo = UserRepository(db_session)
    users: List[User] = []
    for p in participants:
        u = await user_repo.get_by_id(p.user_id)
        if u:
            users.append(u)
    return users


def _build_session_end_message(
    part_number: Optional[int],
    actual_minutes: Optional[int],
    next_run_at: Optional[datetime],
    header: Optional[str] = None,
    topic: Optional[str] = None,
) -> str:
    """Shared, consistently-formatted "part ended" announcement, used by
    both the timer-based end (_session_end) and the everyone-finished-early
    path (_finish_session_early) -- so the group sees one professional,
    predictable layout no matter how the part actually ended."""
    if header is None:
        header = (
            f"🏁 <b>پارت #{part_number} به پایان رسید</b>"
            if part_number else "🏁 <b>پارت به پایان رسید</b>"
        )
    lines = [header]
    if topic:
        lines.append(f"📖 موضوع: <b>{html.escape(topic)}</b>")
    lines.append("➖➖➖➖➖➖➖➖➖➖")
    if actual_minutes is not None:
        lines.append(f"⏱ مدت واقعی: <b>{actual_minutes}</b> دقیقه")
    # A short break here isn't dead time -- spaced practice (short breaks
    # between study blocks) is consistently linked to better long-term
    # retention than running blocks back-to-back, so the same rest is
    # framed as part of the method, not just a pause in it.
    lines.append(f"🧠 پیشنهاد: {config.MIN_BREAK_MINUTES} دقیقه استراحت کن -- استراحت کوتاه بین پارت‌ها به یادسپاری کمک می‌کنه.")
    if next_run_at:
        next_clock = time_service.format_datetime(next_run_at, "%H:%M")
        lines.append(f"⏰ پارت بعدی حدود ساعت <b>{next_clock}</b> پرسیده می‌شه.")
    else:
        lines.append("⏰ پارت بعدی به‌زودی برنامه‌ریزی می‌شه.")
    return "\n".join(lines)


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
                settings.consecutive_misses += 1
                await db_session.flush()
                if settings.consecutive_misses >= config.MISS_STREAK_PAUSE_THRESHOLD:
                    logger.info(
                        f"Group {sess.group_id} paused: {settings.consecutive_misses} "
                        f"consecutive missed polls -- automatic polling (including the "
                        f"daily morning poll) stops until a real message comes in."
                    )
            logger.info(f"Session {session_id} missed, no participants")
            return

        # At least one person showed up -- this poll wasn't a miss, so any
        # miss streak the group had built up no longer applies. Without
        # this reset, a group that goes 2 misses / 1 success / 2 misses
        # would incorrectly hit the pause threshold on the 4th poll
        # instead of needing a fresh streak of misses.
        settings_result = await db_session.execute(
            select(GroupSetting).where(GroupSetting.group_id == sess.group_id)
        )
        settings = settings_result.scalar_one_or_none()
        if settings and settings.consecutive_misses > 0:
            settings.consecutive_misses = 0
            await db_session.flush()

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
        await _cancel_scheduled_job(_session_start, session_id, db_session=db_session)

        # The session_end job scheduled back at creation time used a
        # placeholder duration (guessed before we knew who'd actually show
        # up). Replace it now with one based on the real, just-computed
        # extension -- the same number we're about to announce as
        # "حداکثر" below -- so the part actually ends, and the next
        # automatic poll actually gets queued, at the max duration users
        # were told, not at some earlier/later placeholder time.
        await _cancel_scheduled_job(_session_end, session_id, db_session=db_session)
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
                part_number = await manager.get_group_part_number(sess.group_id, session_id)
                ready_users = await _get_ready_users(db_session, session_id)
                # Nobody has reported a result yet at the instant the part
                # starts, so every name shows pending (⚪) here -- the
                # dots turn 🟢/🟡/🔴 in place as taps come in, via
                # _refresh_roster_message.
                roster = _format_roster(ready_users)
                topic_line = f"📖 موضوع: <b>{html.escape(sess.topic)}</b>\n" if sess.topic else ""
                sent = await bot.send_message(
                    group.telegram_chat_id,
                    f"🚀 <b>پارت #{part_number} شروع شد</b>\n"
                    f"{topic_line}"
                    f"➖➖➖➖➖➖➖➖➖➖\n"
                    f"👥 <b>آمادگان ({len(ready_participants)} نفر):</b>\n"
                    f"{roster}\n"
                    f"➖➖➖➖➖➖➖➖➖➖\n"
                    f"⏱ حداقل: <b>{rec['minimum']}</b> | هدف: <b>{rec['target']}</b> | حداکثر: <b>{rec['extension']}</b> دقیقه\n"
                    f"🧭 حالت: {rec['mode']}\n\n"
                    f"وقتی پارتت تموم شد بزن 👇 (حتی اگه اعلام آمادگی نکرده بودی)",
                    reply_markup=result_keyboard(session_id),
                )
                sess.start_message_id = sent.message_id
                await db_session.flush()
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

                # The attendance-poll message ("کیا هستن؟") has now been
                # fully replaced by the part-start announcement above --
                # leaving it in the chat is just clutter (and its 🟢 هستم
                # button is stale/non-functional at this point anyway).
                if sess.attendance_message_id:
                    try:
                        await bot.delete_message(group.telegram_chat_id, sess.attendance_message_id)
                    except Exception as del_err:
                        logger.debug(
                            f"Could not delete attendance-poll message for session {session_id}: {del_err}"
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

        if settings.awaiting_activity and settings.consecutive_misses >= config.MISS_STREAK_PAUSE_THRESHOLD:
            # This group has gone `MISS_STREAK_PAUSE_THRESHOLD` polls in a
            # row with nobody tapping "هستم". Stop polling it automatically
            # -- including the daily morning cron -- until a real human
            # message or tap comes in; GroupSyncMiddleware clears
            # consecutive_misses the moment that happens and re-triggers
            # a poll right then.
            logger.info(
                f"Skipping automatic poll for group {group_id}: paused after "
                f"{settings.consecutive_misses} consecutive missed polls"
            )
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
                sent = await bot.send_message(
                    group.telegram_chat_id,
                    "☀️ آماده پارت مطالعاتی هستید؟\nهرکی هست بزنه 🟢 (حداقل یک نفر کافیه تا پارت برگزار بشه)",
                    reply_markup=attendance_keyboard(sess.id),
                )
                sess.attendance_message_id = sent.message_id
                await session.flush()
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


async def _finish_session_early(session_id: int):
    """Ends a session as soon as everyone who was READY has tapped a
    completion button -- instead of waiting for the original timer-based
    session_end estimate, which was only ever a placeholder. Cancels that
    now-redundant timer and immediately queues the next automatic poll.

    Schedules the next poll *before* announcing anything, so the message
    can name the actual clock time people should expect it -- since
    people often finish a part early, "به‌زودی" (soon) left them guessing
    whether that meant a minute or an hour."""
    async with get_session() as db_session:
        await _cancel_scheduled_job(_session_end, session_id, db_session=db_session)
        manager = SessionManager(db_session)
        sess = await manager.get_session(session_id)
        if not sess:
            return
        group_id = sess.group_id
        part_number = await manager.get_group_part_number(sess.group_id, session_id)
        if sess.status == "STARTED":
            await manager.end_session(session_id)
        actual_minutes = None
        if sess.actual_start and sess.actual_end:
            actual_minutes = round((sess.actual_end - sess.actual_start).total_seconds() / 60)
        topic = sess.topic
        result = await db_session.execute(select(Group).where(Group.id == group_id))
        group = result.scalar_one_or_none()
    next_run_at = await _schedule_next_poll(session_id)
    if group and bot:
        header = (
            f"🎉 <b>همه اعلام اتمام کردن! (پارت #{part_number})</b>"
            if part_number else "🎉 <b>همه اعلام اتمام کردن!</b>"
        )
        text = _build_session_end_message(part_number, actual_minutes, next_run_at, header=header, topic=topic)
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
        if not sess or sess.status != "STARTED":
            # A stale/superseded job (e.g. a leftover pending row from
            # before the _cancel_scheduled_job DB-marking fix, or a
            # crash-recovery replay racing the live one) firing against a
            # session that isn't actually running anymore -- don't
            # double-end it, don't send a second "part ended" message,
            # and don't queue a duplicate next poll.
            logger.info(
                f"_session_end skipped for session {session_id}: "
                f"status={sess.status if sess else 'missing'}"
            )
            return
        group_id = sess.group_id
        part_number = await manager.get_group_part_number(sess.group_id, session_id)
        await manager.end_session(session_id)
        actual_minutes = None
        if sess and sess.actual_start and sess.actual_end:
            actual_minutes = round((sess.actual_end - sess.actual_start).total_seconds() / 60)
        topic = sess.topic if sess else None
        group = None
        if group_id:
            result = await db_session.execute(select(Group).where(Group.id == group_id))
            group = result.scalar_one_or_none()
        logger.info(f"Session {session_id} ended")
    if group_id:
        # A session actually completing is exactly the signal that should
        # queue up the *next* automatic poll -- not a blind fixed interval.
        next_run_at = await _schedule_next_poll(session_id)
        # Announce the end of the part too (previously only the
        # everyone-finished-early path did this -- a part that simply ran
        # out its timer ended silently, with no message in the group at
        # all).
        if group and bot:
            text = _build_session_end_message(part_number, actual_minutes, next_run_at, topic=topic)
            try:
                await bot.send_message(group.telegram_chat_id, text)
            except Exception as e:
                logger.error(f"Failed to announce session end for session {session_id}: {e}")


# Reverse lookup from job function -> (job_type, metadata key name), used
# by both _cancel_scheduled_job (to find the matching DB row) and
# recover_from_crash (to know which function/id-field a stored job_type
# maps back to). Kept in one place so the two never drift apart.
JOB_FUNCS: Dict[str, Any] = {
    "attendance_timeout": (_attendance_timeout, "session_id"),
    "session_start": (_session_start, "session_id"),
    "session_end": (_session_end, "session_id"),
}
_JOB_FUNC_TO_TYPE = {func: (job_type, key) for job_type, (func, key) in JOB_FUNCS.items()}


async def _cancel_scheduled_job(func, *args, db_session: Optional[AsyncSession] = None):
    """Removes any pending APScheduler job that calls `func` with these
    exact `args` (e.g. a not-yet-fired session_end for this session_id),
    AND marks the matching SchedulerJob row(s) in the DB as "cancelled".

    That second part used to be missing: this only ever removed the job
    from APScheduler's in-memory list, leaving the DB row stuck at
    status="pending" forever. Since the scheduler runs with the default
    in-memory jobstore (see `scheduler = AsyncIOScheduler(...)` below),
    every live job is lost on process restart -- recovery relies entirely
    on replaying "pending" SchedulerJob rows (see recover_from_crash). A
    stale "pending" row for a job that was deliberately superseded (e.g.
    the placeholder session_end scheduled before attendance closed,
    replaced in _attendance_timeout with the real one based on actual
    volunteers) would get resurrected on the next restart -- either fired
    immediately if its old run_at had already passed (ending/affecting a
    session that hadn't reached that state yet), or re-registered as a
    second, duplicate live job alongside the real one. This is exactly
    the kind of thing that could make a part run past its announced
    "حداکثر" with no automatic end: the real session_end job silently
    lost, only the stale row left behind, waiting for a restart that
    fires it at the wrong time or not at all before a manual tap ends
    the part instead.
    """
    for job in scheduler.get_jobs():
        if job.func is func and tuple(job.args) == tuple(args):
            scheduler.remove_job(job.id)

    entry = _JOB_FUNC_TO_TYPE.get(func)
    if not entry or not args:
        return
    job_type, key_name = entry
    key_id = args[0]

    async def _mark_cancelled(session: AsyncSession):
        result = await session.execute(
            select(SchedulerJob).where(
                SchedulerJob.job_type == job_type,
                SchedulerJob.status == "pending",
            )
        )
        for row in result.scalars().all():
            if (row.job_meta or {}).get(key_name) == key_id:
                row.status = "cancelled"

    if db_session is not None:
        await _mark_cancelled(db_session)
        await db_session.flush()
    else:
        async with get_session() as session:
            await _mark_cancelled(session)


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
# ROUTINES (بخش روتین) -- scheduling
# ------------------------------------------------------------
# Unlike attendance_timeout/session_start/session_end above, a routine
# is registered directly against APScheduler as a CronTrigger (daily/
# weekly) or a DateTrigger (once) keyed on its own row id, rather than
# going through SchedulerJob -- there's nothing to "replay" for a
# recurring cron job after a restart, it just needs to be re-added
# (see reschedule_all_routines, called once from on_startup exactly
# like reschedule_morning_poll_job).
# ============================================================
def _routine_job_id(routine_id: int) -> str:
    return f"routine_{routine_id}"


async def schedule_routine(routine: "UserRoutine") -> None:
    """(Re)registers the APScheduler job for one routine. Safe to call
    again for the same routine (replace_existing=True) -- used both
    right after creation and to re-install every active routine on
    startup."""
    job_id = _routine_job_id(routine.id)
    if routine.repeat_type == "once":
        if not routine.once_date:
            return
        run_at = routine.once_date.replace(
            hour=routine.hour, minute=routine.minute, second=0, microsecond=0
        )
        scheduler.add_job(
            _routine_fire, trigger=DateTrigger(run_date=run_at),
            args=[routine.id], id=job_id, replace_existing=True,
        )
    elif routine.repeat_type == "daily":
        scheduler.add_job(
            _routine_fire,
            trigger=CronTrigger(hour=routine.hour, minute=routine.minute, timezone="Asia/Tehran"),
            args=[routine.id], id=job_id, replace_existing=True,
        )
    elif routine.repeat_type == "weekly":
        days = routine.days_of_week or []
        cron_days = ",".join(PERSIAN_DAY_TO_CRON[d] for d in days if d in PERSIAN_DAY_TO_CRON)
        if not cron_days:
            return
        scheduler.add_job(
            _routine_fire,
            trigger=CronTrigger(day_of_week=cron_days, hour=routine.hour, minute=routine.minute, timezone="Asia/Tehran"),
            args=[routine.id], id=job_id, replace_existing=True,
        )


def unschedule_routine(routine_id: int) -> None:
    try:
        scheduler.remove_job(_routine_job_id(routine_id))
    except JobLookupError:
        pass


async def _routine_fire(routine_id: int) -> None:
    """Fired by APScheduler at the routine's configured time. Loads the
    routine fresh from the DB (never trust captured closures for
    something a user might have edited/deleted since scheduling),
    sends the reminder or part prompt, and auto-deactivates one-off
    routines after they fire."""
    async with get_session() as session:
        result = await session.execute(select(UserRoutine).where(UserRoutine.id == routine_id))
        routine = result.scalar_one_or_none()
        if not routine or not routine.active:
            return
        user_result = await session.execute(select(User).where(User.id == routine.user_id))
        user = user_result.scalar_one_or_none()
        if not user:
            return
        telegram_id = user.telegram_id
        title = routine.title
        kind = routine.kind
        duration = routine.duration_minutes
        was_once = routine.repeat_type == "once"
        if was_once:
            routine.active = False
        await session.commit()

    if bot is None:
        return
    try:
        if kind == "part":
            text = (
                f"📖 <b>وقت این پارت رسید:</b>\n\n"
                f"{html.escape(title)} — {duration or 25} دقیقه\n\n"
                f"وقتی تمومش کردی (یا نتونستی)، نتیجه رو پایین ثبت کن:"
            )
        else:
            text = f"⏰ <b>یادآوری:</b>\n\n{html.escape(title)}"
        await bot.send_message(telegram_id, text, reply_markup=routine_fire_keyboard(routine_id, kind))
    except Exception as e:
        logger.warning(f"Could not deliver routine {routine_id} to {telegram_id}: {e}")

    if was_once:
        unschedule_routine(routine_id)


async def reschedule_all_routines() -> None:
    """Re-installs every active routine's APScheduler job on startup --
    mirrors reschedule_morning_poll_job/reschedule_daily_plan_job, just
    for a whole table of jobs instead of one fixed one."""
    async with get_session() as session:
        repo = RoutineRepository(session)
        routines = await repo.get_all_active()
    for routine in routines:
        await schedule_routine(routine)
    if routines:
        logger.info(f"Rescheduled {len(routines)} active routine(s)")


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
# (added by admins for SUCCESS/MISSED/etc.) were never actually
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
            return "بیا با یه پارت ۲۰ دقیقه‌ای شروع کنیم. 🌱"
        current = profile.current_capacity
        comfort = profile.comfort_capacity
        hour = time_service.hour()

        if 5 <= hour < 9:
            time_note = "صبح زوده -- یه پارت کوتاه‌تر احتمالاً بهتر جواب می‌ده."
            suggested = min(current * 0.7, comfort)
        elif 9 <= hour < 12:
            time_note = "صبحه، از اون ساعت‌هایی که معمولاً برای مطالعه خوبن."
            suggested = current
        elif 12 <= hour < 14:
            time_note = "ظهره؛ بهتره پارتو زیاد سنگین نگیری."
            suggested = min(current * 0.7, comfort)
        elif 14 <= hour < 17:
            time_note = "بعدازظهره -- معمولاً این ساعت انرژی خوبی داری."
            suggested = current
        elif 17 <= hour < 21:
            time_note = "عصره، یکی از بهترین ساعت‌های روز برای مطالعه‌ست."
            suggested = current * 1.1
        else:
            time_note = "دیروقته؛ یه پارت آروم و کوتاه بهتره."
            suggested = min(current * 0.6, comfort)

        suggested = min(config.CANDIDATE_DURATIONS, key=lambda x: abs(x - suggested))
        return f"💡 {time_note}\n\nبرای پارت بعدی چطوره {int(suggested)} دقیقه بذاریم؟\n\nآماده‌ای؟ 🟢"


class Advisor:
    """Builds the coaching content behind /advice and /plan.

    Two layers, kept deliberately separate:
      - `analyze_user`        -> the *why*: what the data says about this
                                  person right now (strengths, watch-outs,
                                  overall trend).
      - `_build_mode_suggestion` -> the *what next*: one concrete,
                                  actionable part-length recommendation,
                                  picked from whichever mode (recovery /
                                  growth / calm / normal) actually fits.

    Every outward-facing message is plain Telegram HTML (the bot already
    runs with ParseMode.HTML) using a small, consistent vocabulary --
    <b> for section headers and the one number that matters, <i> for the
    supporting rationale, one divider style -- instead of ad-hoc walls of
    emoji-prefixed lines.
    """

    # ------------------------------------------------------------------
    # INSIGHT RULES
    # ------------------------------------------------------------------
    # Each rule is (predicate, category, text). Category drives which
    # section of the rendered message the line lands in:
    #   "strength" -> 🌟 نقاط قوت      "watch" -> 🔎 نکات قابل‌توجه
    #   "note"     -> 💡 نکته
    # Keeping the condition and the category side by side (instead of
    # burying the classification implicitly in prose) makes it obvious at
    # a glance what each signal is meant to communicate, and makes adding
    # a new signal a one-line addition instead of another bespoke if/elif.
    # ------------------------------------------------------------------
    def _collect_insights(self, profile: "UserBehaviorProfile") -> List[Tuple[str, str]]:
        insights: List[Tuple[str, str]] = []

        if profile.completion_rate < 0.4:
            insights.append(("watch", "این اواخر پارت‌ها رو زیاد تموم نمی‌کنی. شاید بد نباشه یه مدت پارت‌های کوتاه‌تر (۲۰-۳۰ دقیقه) رو امتحان کنی."))
        elif profile.completion_rate > 0.8:
            insights.append(("strength", "تقریباً هر پارتی که شروع می‌کنی رو هم تموم می‌کنی -- همینطوری ادامه بده."))

        if profile.starting_friction_score > 0.6:
            insights.append(("watch", "این روزها انگار شروع‌کردن برات سخت‌تر شده. با پارت‌های ۱۵ دقیقه‌ای شروع کن و کم‌کم بیشترش کن."))
        elif profile.starting_friction_score < 0.3:
            insights.append(("strength", "شروع‌کردن برات کار سختی نیست -- جا داری پارت‌های بلندتر رو هم امتحان کنی."))

        if profile.consistency_score < 0.4:
            insights.append(("watch", "مطالعه‌ات این مدت یکم نامنظم بوده. سعی کن هر روز حتی یه پارت کوچیک هم داشته باشی، فقط برای اینکه ریتمت نیفته."))

        if profile.success_streak >= 3:
            insights.append(("strength", f"{profile.success_streak} پارت پشت سر هم موفق بودی! همینطوری ادامه بده."))

        if profile.miss_streak >= 2:
            insights.append(("watch", f"{profile.miss_streak} پارت رو پشت سر هم از دست دادی. اتفاقیه که می‌افته -- یه پارت کوتاه بردار و دوباره شروع کن."))

        if profile.preferred_hour:
            insights.append(("note", f"به نظر می‌رسه حدود ساعت {profile.preferred_hour:02d}:00 بهترین حالتو داری. اگه می‌شه، پارت‌های مهم‌تر رو همون موقع بذار."))

        # Attendance is distinct from completion_rate: it's about whether
        # you show up to the poll at all, not whether a part you started
        # gets finished. Low attendance with an otherwise fine completion
        # rate points at a *starting* problem, not a stamina one.
        if profile.attendance_rate < 0.4:
            insights.append(("watch", "این اواخر تو بیشتر جلسه‌ها نبودی. اگه قضیه سر ساعتشه، از تنظیمات می‌تونی زمان مناسب‌تر خودتو انتخاب کنی."))
        elif profile.attendance_rate > 0.85:
            insights.append(("strength", "همیشه سر جلسه‌هایی -- همین ثبات، بزرگ‌ترین نقطه‌قوتته."))

        # Momentum vs. recovery tell slightly different stories: momentum
        # is "are you on a roll right now", recovery is "how well do you
        # bounce back after a miss". Low momentum with strong recovery
        # deserves a different message than low on both.
        if profile.momentum_score < 0.3 and profile.recovery_score < 0.3:
            insights.append(("watch", "این روزها هم شتابت افتاده هم دیرتر از قبل بعد از وقفه‌ها برمی‌گردی. یه پارت خیلی کوچیک (۱۰-۱۵ دقیقه) همین امروز می‌تونه این چرخه رو بشکنه."))
        elif profile.momentum_score < 0.3 <= profile.recovery_score:
            insights.append(("note", "شتابت الان پایینه، ولی سابقه نشون می‌ده خوب برمی‌گردی. نگران روند نباش."))
        elif profile.momentum_score >= 0.7:
            insights.append(("strength", "شتاب خوبی داری؛ پارت‌ها پشت سر هم دارن جلو می‌رن."))

        # Session-length tolerance: which direction the person actually
        # responds well to, independent of their raw capacity number.
        if profile.long_session_tolerance > 0.7 and profile.short_session_response < 0.4:
            insights.append(("note", "توی پارت‌های بلندتر بهتر کار می‌کنی؛ پارت‌های خیلی کوتاه انگار ریتمتو نمی‌گیرن."))
        elif profile.short_session_response > 0.7 and profile.long_session_tolerance < 0.4:
            insights.append(("note", "پارت‌های کوتاه‌تر برات بهتر جواب می‌دن؛ لازم نیست خودتو مجبور به پارت‌های طولانی کنی."))

        # Untapped potential: the gap between what the person has actually
        # managed once (maximum_observed_capacity) and where their rolling
        # current_capacity sits now. Worth surfacing after a good streak --
        # it's evidence, not just encouragement.
        if (profile.maximum_observed_capacity - profile.current_capacity >= 15
                and profile.success_streak >= 2):
            insights.append(("strength", f"یه بار تا {int(profile.maximum_observed_capacity)} دقیقه هم پیش رفتی. با این روندی که داری، رسیدن دوباره به همون سطح خیلی دور نیست."))

        # Short vs. long-term direction: recent_average vs average_session
        # catches a shift the coarse `trend` field can miss (e.g. trend
        # stuck on STABLE while the last few parts quietly drift).
        if profile.average_session > 0:
            delta_ratio = (profile.recent_average - profile.average_session) / profile.average_session
            if delta_ratio >= 0.2:
                insights.append(("strength", "پارت‌های اخیرت از میانگین کلی‌ت بلندتر شدن -- ظرفیتت داره واقعاً بالا می‌ره."))
            elif delta_ratio <= -0.2:
                insights.append(("note", "پارت‌های اخیرت از میانگین همیشگیت کوتاه‌تر شدن. اتفاق عجیبی نیست، فقط بدون که شاید بد نباشه پارت بعدی رو یکم کوتاه‌تر بگیری."))

        if profile.current_load > 90:
            insights.append(("watch", "امروز زیاد مطالعه کردی. بهتره یه استراحت کوتاه بکنی و با یه پارت آروم ادامه بدی."))
        elif profile.current_load < 20 and profile.miss_streak == 0:
            insights.append(("note", "امروز کمتر از معمول مطالعه کردی. یه پارت کوچیک می‌تونه ریتمتو برگردونه."))

        return insights

    _SECTION_META = {
        "strength": ("🌟", "نقاط قوت"),
        "watch": ("🔎", "نکات قابل‌توجه"),
        "note": ("💡", "نکته"),
    }

    def _render_insights(self, insights: List[Tuple[str, str]]) -> str:
        """Groups the (already-capped) insight list by category and
        renders each as its own labeled section, instead of one flat list
        of emoji-prefixed lines with no visual hierarchy."""
        by_category: Dict[str, List[str]] = {"strength": [], "watch": [], "note": []}
        for category, text in insights:
            by_category[category].append(text)

        blocks: List[str] = []
        for category in ("strength", "watch", "note"):
            lines = by_category[category]
            if not lines:
                continue
            icon, label = self._SECTION_META[category]
            body = "\n".join(f"› {line}" for line in lines)
            blocks.append(f"<b>{icon} {label}</b>\n{body}")

        return "\n\n".join(blocks)

    async def analyze_user(self, user_id: int, session: AsyncSession) -> Dict[str, Any]:
        profile_repo = ProfileRepository(session)
        profile = await profile_repo.get_by_user_id(user_id)
        if not profile:
            return {"has_data": False, "message": "داده کافی برای تحلیل وجود ندارد. چند پارت مطالعه کن تا بتونم بهتر بشناسمت. 🌱"}

        insights = self._collect_insights(profile)
        capped = insights[:7]

        trend_lines = {
            "UPWARD": "📈 در کل روندت رو به رشده -- عالی پیش می‌ری.",
            "DOWNWARD": "📉 در کل یه کم افت داشتی. نگران نباش، با قدم‌های کوچیک برمی‌گردی سر جات.",
            "STABLE": "📊 در کل خیلی باثباتی، یعنی داری استمرار رو حفظ می‌کنی!",
        }
        trend_line = trend_lines.get(profile.trend, "📊 روندت هنوز شکل مشخصی نگرفته. فعلاً روی همین استمرار تمرکز کن.")

        body = self._render_insights(capped) if capped else "فعلاً همه‌چی روی روال عادیه. همینطوری ادامه بده! 🌱"
        msg = f"<b>📊 یه نگاه به روند مطالعه‌ات</b>\n\n{body}\n\n{trend_line}"

        return {"has_data": True, "profile": profile, "insights": capped, "message": msg}

    # ------------------------------------------------------------------
    # MODE SUGGESTIONS ("what next")
    # ------------------------------------------------------------------
    # One table instead of three near-identical async methods
    # (get_recovery_suggestion / get_growth_suggestion / get_calm_suggestion)
    # that each re-fetched the profile from the DB even though the caller
    # already had it in hand. Same math, same copy -- just one place that
    # owns it, and no redundant query per suggestion.
    _MODE_TITLES = {
        "recovery": "🌱 حالت بازیابی",
        "growth": "🚀 حالت رشد",
        "calm": "🌙 حالت آرامش",
        "normal": "📊 حالت عادی",
    }

    def _mode_duration(self, mode: str, profile: "UserBehaviorProfile") -> int:
        if mode == "recovery":
            raw = max(10, min(15, profile.current_capacity * 0.5, profile.comfort_capacity * 0.6))
        elif mode == "growth":
            raw = max(15, min(profile.current_capacity + 5, profile.challenge_capacity, 60))
        elif mode == "calm":
            raw = max(10, min(profile.comfort_capacity * 0.7, 25))
        else:
            raw = profile.current_capacity or profile.comfort_capacity or 30
        return min(config.CANDIDATE_DURATIONS, key=lambda x: abs(x - raw))

    def _detect_personal_mode(self, profile: "UserBehaviorProfile", calm_mode_enabled: bool) -> str:
        """Same signals as ModeDetector, but evaluated for a single person
        instead of averaged across a whole group -- so an individual's
        /advice reply can recommend the mode that actually fits *them*
        even inside a group whose overall mode differs."""
        if profile.miss_streak >= 2 or profile.starting_friction_score > 0.6:
            return "recovery"
        if profile.success_streak >= 3 and profile.completion_rate >= 0.8:
            return "growth"
        if profile.current_load > 90 or time_service.is_late_night() or calm_mode_enabled:
            return "calm"
        return "normal"

    def _build_mode_suggestion(self, mode: str, profile: "UserBehaviorProfile") -> str:
        """Renders the concrete next-step card for recovery/growth/calm.
        ('normal' is handled separately since it comes from Assistant,
        which reasons about time-of-day rather than streaks/capacity.)"""
        suggested = self._mode_duration(mode, profile)
        title = self._MODE_TITLES[mode]

        if mode == "recovery":
            blurb = "چند پارت اخیر برات سخت‌تر از معمول بوده -- کاملاً طبیعیه."
            rationale = (
                "هدف الان برگشتن به ریتمه، نه جبران یک‌جای همه‌چیز. شروع کوچیک بعد از یک "
                "وقفه، شانس واقعی برگشتن به عادت رو به‌مراتب بیشتر می‌کنه."
            )
        elif mode == "growth":
            blurb = f"{profile.success_streak} پارت موفق پشت‌سرهم داشتی 👏 ظرفیتت داره بالا می‌ره."
            rationale = (
                "پیشرفت تدریجی و پایدار هدفه، نه یک جهش ناگهانی؛ همین افزایش پله‌ای کوچیکه که "
                "به‌اندازه‌ی کافی چالش‌برانگیزه، بدون این‌که انگیزه رو بریزه."
            )
        else:  # calm
            blurb = "امروز بار زیادی رو دوش داشتی یا وقتشه یک پارت سبک‌تر بگیری."
            rationale = (
                "بیشتر تثبیت حافظه حین خوابه، نه با فشار نزدیک زمان خواب -- یک مرور سبک الان "
                "بهتر از یک پارت سنگین دیروقته."
                if time_service.is_late_night() else
                "وقتی بار امروز زیاد بوده، یک پارت سبک‌تر همون نتیجه‌ی حفظ ریتم رو می‌ده، "
                "بدون این‌که خستگی کیفیت کار رو پایین بیاره."
            )

        return (
            f"<b>{title}</b>\n"
            f"{blurb}\n\n"
            f"⏱ پیشنهاد پارت بعدی: <b>{int(suggested)} دقیقه</b>\n\n"
            f"<i>{rationale}</i>\n\n"
            "آماده‌ای؟ 💪"
        )

    # A small, fixed rotation of universal, technique-level tips -- each
    # one a widely-replicated finding that applies regardless of which
    # mode someone is currently in, so every /advice reply also carries
    # one concrete, evidence-based *method* to apply during the part
    # itself, not just a duration.
    _METHOD_TIPS: List[str] = [
        "به‌جای دوباره‌خوانی، سعی کن بعد از هر پارت، بدون نگاه‌کردن به جزوه/کتاب، نکات مهم رو "
        "با صدای بلند یا روی کاغذ توضیح بدی. در پژوهش‌های حافظه، این کار (یادآوری فعال) نسبت به "
        "صرفاً خوندن دوباره، یادسپاری بلندمدت بهتری ایجاد می‌کنه.",
        "مرور یک مطلب در چند روز پخش‌شده (اثر فاصله‌گذاری)، اثرش رو حافظه به‌مراتب بیشتر از "
        "یک‌جا و فشرده خوندنشه -- حتی اگه مجموع زمان یکی باشه.",
        "اگه چند موضوع/درس داری، جابه‌جا کردن بینشون توی یک پارت (به‌جای غرق شدن ساعت‌ها توی "
        "یک موضوع) معمولاً یادگیری عمیق‌تری می‌سازه.",
        "مغز بخش مهمی از تثبیت اطلاعات رو حین خواب انجام می‌ده -- یه پارت مرور کوتاه چند ساعت "
        "قبل خواب، مؤثرتر از شب‌بیداری و فشردهٔ آخر شبه.",
    ]
    _METHOD_TIP_TITLES = ["🧠 یادآوری فعال", "📅 اثر فاصله‌گذاری", "🔀 آموزش به‌هم‌آمیخته", "😴 خواب و تثبیت حافظه"]

    # Citation-backed technique bank for the admin /status lookup (see
    # cmd_admin_status / _send_admin_status_report). Unlike _METHOD_TIPS
    # above (one tip a day, rotating, regardless of who's asking), each
    # entry here carries an explicit condition against the *target*
    # person's own UserBehaviorProfile, so /status surfaces only the
    # techniques that actually match their specific pattern -- plus a
    # named citation, since this is meant to be read as "here's the
    # published finding this is based on", not just a generic tip.
    # References are the well-established sources for each effect,
    # summarized in plain language rather than quoted.
    _SCIENTIFIC_METHODS: List[Dict[str, Any]] = [
        {
            "key": "spacing",
            "title": "📅 اثر فاصله‌گذاری (Spacing Effect)",
            "body": (
                "پخش کردن مرور یک مطلب در چند روز جدا، برای حافظهٔ بلندمدت به‌مراتب "
                "مؤثرتر از فشرده‌خوانی یک‌جاست -- حتی وقتی مجموع زمان مطالعه یکسان باشه. "
                "افت ثبات یا وقفه‌های اخیر این فرد دقیقاً همون الگوییه که فاصله‌گذاری "
                "منظم (به‌جای مطالعهٔ فشرده و نامنظم) براش راهگشاست."
            ),
            "citation": "Cepeda, Pashler, Wixted & Rohrer (2006), Psychological Bulletin -- متاآنالیز روی بیش از ۱۸۰ مطالعه",
            "condition": lambda p, act: (
                p.consistency_score < 0.5 or p.trend == "DOWNWARD" or p.miss_streak >= 1
            ),
        },
        {
            "key": "active_recall",
            "title": "🧠 یادآوری فعال / اثر آزمون (Testing Effect)",
            "body": (
                "به‌جای دوباره‌خوانی منفعلانه، بستن کتاب و توضیح‌دادن نکات از حافظه (یا حل "
                "خودآزمون) یادسپاری بلندمدت به‌مراتب بهتری نسبت به مرور ساده ایجاد می‌کنه -- "
                "به‌خصوص برای کسی که شروع‌کردن یا تکمیل‌کردن پارت‌ها براش سخت‌تر از حد معمول شده."
            ),
            "citation": "Roediger & Karpicke (2006), Psychological Science",
            "condition": lambda p, act: (
                p.completion_rate < 0.6 or p.starting_friction_score > 0.5
            ),
        },
        {
            "key": "interleaving",
            "title": "🔀 آموزش به‌هم‌آمیخته (Interleaving)",
            "body": (
                "وقتی چند درس/موضوع فعال داری، جابه‌جا کردن بینشون توی یک پارت (به‌جای غرق‌شدن "
                "ساعت‌ها توی یک موضوع) معمولاً یادگیری عمیق‌تر و انتقال بهتری به آزمون واقعی می‌سازه."
            ),
            "citation": "Rohrer & Taylor (2007), Instructional Science",
            "condition": lambda p, act: act >= 2,
        },
        {
            "key": "implementation_intentions",
            "title": "🎯 برنامه‌ریزی اگر-آنگاه (Implementation Intentions)",
            "body": (
                "به‌جای یک هدف کلی («امروز درس می‌خونم»)، یک قاعدهٔ مشخص «اگر ساعت X شد، "
                "همون‌جا پارت رو شروع می‌کنم» شانس واقعی شروع‌کردن رو به‌طور قابل‌توجهی بالا "
                "می‌بره -- دقیقاً همون‌جایی که این فرد بیشترین اصطکاک شروع رو داره."
            ),
            "citation": "Gollwitzer & Sheeran (2006), Advances in Experimental Social Psychology -- متاآنالیز",
            "condition": lambda p, act: p.starting_friction_score > 0.5 or p.miss_streak >= 2,
        },
        {
            "key": "sleep_consolidation",
            "title": "😴 خواب و تثبیت حافظه",
            "body": (
                "بخش مهمی از تثبیت اطلاعات تازه‌آموخته‌شده حین خواب اتفاق می‌افته. یک پارت "
                "مرور سبک چند ساعت قبل خواب، مؤثرتر از یک پارت سنگین در آخرین ساعات شبه -- "
                "و برای کسی که عادت به مطالعهٔ دیروقت داره، اهمیت بیشتری پیدا می‌کنه."
            ),
            "citation": "Diekelmann & Born (2010), Nature Reviews Neuroscience",
            "condition": lambda p, act: p.preferred_hour >= 22 or p.current_load > 90,
        },
        {
            "key": "desirable_difficulty",
            "title": "📈 دشواری مطلوب (Desirable Difficulty)",
            "body": (
                "برای کسی که چند موفقیت پشت‌سرهم داشته، افزایش تدریجی و کوچیک سطح چالش "
                "(نه جهش ناگهانی) بهترین ترکیب رو بین یادگیری عمیق‌تر و حفظ انگیزه می‌سازه."
            ),
            "citation": "Bjork & Bjork (2011), in Gernsbacher et al. (eds.), Psychology and the Real World",
            "condition": lambda p, act: p.success_streak >= 3 and p.momentum_score >= 0.6,
        },
    ]

    def _pick_scientific_methods(self, profile: "UserBehaviorProfile", activity_count: int = 0) -> List[Dict[str, str]]:
        """Selects up to 3 techniques whose condition matches this specific
        profile, in the bank's priority order. Falls back to the two most
        broadly-applicable techniques (spacing + active recall) if nothing
        else matched, so /status never comes back empty-handed."""
        matched = [
            m for m in self._SCIENTIFIC_METHODS if m["condition"](profile, activity_count)
        ]
        if not matched:
            matched = [m for m in self._SCIENTIFIC_METHODS if m["key"] in ("spacing", "active_recall")]
        return matched[:3]

    async def get_personalized_recommendation(self, user_id: int, session: AsyncSession) -> Dict[str, Any]:
        """The single entry point for '/advice': combines the behavioral
        analysis (what's going on and why) with a concrete next-part
        suggestion (what to actually do about it), picked automatically
        from the same recovery/growth/calm/normal modes the group-level
        RecommendationEngine uses -- just evaluated per-person -- plus one
        rotating, evidence-based study-method tip that applies regardless
        of mode (see _METHOD_TIPS)."""
        analysis = await self.analyze_user(user_id, session)
        if not analysis.get("has_data"):
            return {"has_data": False, "message": analysis["message"], "mode": "normal"}

        profile = analysis["profile"]
        pref_result = await session.execute(
            select(UserPreference).where(UserPreference.user_id == user_id)
        )
        pref = pref_result.scalar_one_or_none()
        calm_enabled = bool(pref.calm_mode_enabled) if pref else False

        mode = self._detect_personal_mode(profile, calm_enabled)
        if mode == "normal":
            assistant = Assistant()
            raw_suggestion = await assistant.get_next_suggestion(user_id, session)
            suggestion = f"<b>{self._MODE_TITLES['normal']}</b>\n{raw_suggestion}"
        else:
            suggestion = self._build_mode_suggestion(mode, profile)

        # Rotate deterministically per user/day rather than randomly, so
        # repeated /advice taps on the same day show the same tip instead
        # of feeling random, while still cycling day to day.
        tip_index = (user_id + time_service.today().toordinal()) % len(self._METHOD_TIPS)
        tip_title = self._METHOD_TIP_TITLES[tip_index]
        method_tip = f"<b>{tip_title}</b>\n{self._METHOD_TIPS[tip_index]}"

        divider = "\n\n▫️▫️▫️▫️▫️▫️▫️▫️▫️▫️\n\n"
        full_message = f"{analysis['message']}{divider}{suggestion}{divider}{method_tip}"

        return {
            "has_data": True,
            "message": full_message,
            "mode": mode,
            "insights": analysis.get("insights", []),
        }

    async def admin_status_report(self, user_id: int, session: AsyncSession) -> Dict[str, Any]:
        """Full status lookup for /status (admin-only, see cmd_admin_status):
        the same behavioral analysis and "what next" suggestion as
        /advice, plus the raw profile numbers, this week's totals, and --
        instead of one rotating generic tip -- every citation-backed
        technique from _SCIENTIFIC_METHODS whose condition actually
        matches this specific person's data (see _pick_scientific_methods),
        so the recommendations are chosen for them, not just in rotation."""
        analysis = await self.analyze_user(user_id, session)
        if not analysis.get("has_data"):
            return {"has_data": False, "message": analysis["message"]}

        profile = analysis["profile"]

        pref_result = await session.execute(
            select(UserPreference).where(UserPreference.user_id == user_id)
        )
        pref = pref_result.scalar_one_or_none()
        calm_enabled = bool(pref.calm_mode_enabled) if pref else False
        mode = self._detect_personal_mode(profile, calm_enabled)
        if mode == "normal":
            assistant = Assistant()
            raw_suggestion = await assistant.get_next_suggestion(user_id, session)
            suggestion = f"<b>{self._MODE_TITLES['normal']}</b>\n{raw_suggestion}"
        else:
            suggestion = self._build_mode_suggestion(mode, profile)

        activities_result = await session.execute(
            select(UserActivity).where(UserActivity.user_id == user_id, UserActivity.active == True)
        )
        activity_count = len(activities_result.scalars().all())

        tehran_start = time_service.now().replace(hour=0, minute=0, second=0, microsecond=0)
        stats_date = tehran_start.replace(tzinfo=None)
        today_result = await session.execute(
            select(DailyStatistics).where(
                DailyStatistics.user_id == user_id, DailyStatistics.date == stats_date
            )
        )
        today_stats = today_result.scalar_one_or_none()

        naive_now = time_service.now().replace(tzinfo=None)
        week_start = (naive_now - timedelta(days=naive_now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        week_result = await session.execute(
            select(WeeklyStatistics).where(
                WeeklyStatistics.user_id == user_id, WeeklyStatistics.week_start == week_start
            )
        )
        week_stats = week_result.scalar_one_or_none()

        stats_lines = [
            f"⏱ امروز: <b>{today_stats.minutes if today_stats else 0}</b> دقیقه "
            f"({today_stats.sessions if today_stats else 0} پارت)",
            f"📆 این هفته: <b>{week_stats.minutes if week_stats else 0}</b> دقیقه "
            f"({week_stats.sessions if week_stats else 0} پارت"
            + (f"، نرخ تکمیل {round((week_stats.completion_rate or 0) * 100)}٪" if week_stats else "")
            + ")",
            f"🔥 رکورد موفقیت پیاپی: {profile.success_streak} | رکورد غیبت پیاپی: {profile.miss_streak}",
            f"🎯 ظرفیت فعلی برآوردشده: {int(profile.current_capacity)} دقیقه "
            f"(بیشینهٔ ثبت‌شده: {profile.maximum_observed_capacity} دقیقه)",
        ]

        methods = self._pick_scientific_methods(profile, activity_count)
        methods_blocks = [
            f"<b>{m['title']}</b>\n{m['body']}\n<i>منبع: {m['citation']}</i>" for m in methods
        ]
        methods_section = (
            "<b>📚 توصیه‌های علمی متناسب با این فرد</b>\n\n" + "\n\n".join(methods_blocks)
            if methods_blocks else ""
        )

        divider = "\n\n▫️▫️▫️▫️▫️▫️▫️▫️▫️▫️\n\n"
        stats_block = "\n".join(stats_lines)
        full_message = (
            f"{analysis['message']}{divider}"
            f"{stats_block}{divider}"
            f"{suggestion}"
            + (f"{divider}{methods_section}" if methods_section else "")
        )

        return {"has_data": True, "message": full_message}

    # ------------------------------------------------------------------
    # PROACTIVE PERSONAL PLAN
    # ------------------------------------------------------------------
    async def _estimate_start_hour(self, user_id: int, profile: "UserBehaviorProfile",
                                    session: AsyncSession) -> int:
        """The daily plan used to always open with
        `profile.preferred_hour or 18` -- but preferred_hour is a column
        that only ever gets its Config default (18) at profile creation
        and is never recalculated anywhere in the codebase, so /plan
        opened at 18:00 for literally every user, forever, regardless of
        when they actually study.

        This instead looks at the person's own recent completed/partial
        parts and finds the hour of day they actually tend to sit down,
        weighting more recent parts more heavily (newest first, geometric
        decay) so a genuine shift in routine shows up within a couple of
        weeks instead of being permanently diluted by months-old history.
        Falls back to the static default only when there isn't enough
        real data yet (brand-new users)."""
        result = await session.execute(
            select(Session)
            .join(SessionResult, Session.id == SessionResult.session_id)
            .where(
                SessionResult.user_id == user_id,
                SessionResult.completion_status.in_(["COMPLETED", "PARTIAL"]),
            )
            .order_by(desc(Session.scheduled_start))
            .limit(20)
        )
        recent = result.scalars().all()
        if not recent:
            return min(max(profile.preferred_hour or 18, 8), 20)

        weight_by_hour: Dict[int, float] = {}
        for i, sess_row in enumerate(recent):
            ts = sess_row.actual_start or sess_row.scheduled_start
            weight_by_hour[ts.hour] = weight_by_hour.get(ts.hour, 0.0) + (0.93 ** i)

        best_hour = max(weight_by_hour.items(), key=lambda kv: kv[1])[0]
        # Keep profile.preferred_hour in sync with this real number, so
        # the "⏰ به نظر می‌رسه ساعت..." insight in analyze_user() and any
        # other reader of this field see the same, now-accurate, value.
        profile.preferred_hour = best_hour
        return min(max(best_hour, 8), 20)

    def get_research_insight(self, user_id: int, profile: Optional["UserBehaviorProfile"],
                              mode: str) -> str:
        """Pick one entry from config.RESEARCH_LIBRARY that actually fits
        the person's current situation (falling back to a general one),
        rotating deterministically per user/day among the matching entries
        so it feels current without ever being random."""
        library = config.RESEARCH_LIBRARY
        by_tag = {entry["tag"]: entry["text"] for entry in library}
        candidates: List[str] = []

        if profile is not None and profile.consistency_score < 0.4 and "consistency_low" in by_tag:
            candidates.append(by_tag["consistency_low"])
        if mode == "recovery" and "recovery_or_general" in by_tag:
            candidates.append(by_tag["recovery_or_general"])
        if profile is not None and (profile.current_load > 90 or time_service.is_late_night()) \
                and "night_or_load" in by_tag:
            candidates.append(by_tag["night_or_load"])

        # Always keep the mode-agnostic entries in the rotation pool too,
        # so the tip doesn't get stuck repeating the same situational one.
        for tag in ("general", "review_material", "multi_subject"):
            if tag in by_tag:
                candidates.append(by_tag[tag])

        if not candidates:
            candidates = [entry["text"] for entry in library]

        idx = (user_id + time_service.today().toordinal()) % len(candidates)
        return candidates[idx]

    def _activity_weight(self, activity: "UserActivity") -> float:
        """Eisenhower-style priority score: self-rated importance (1-5)
        multiplied by an urgency factor derived from how close the
        deadline is (config.URGENCY_MULTIPLIERS). No deadline == no
        urgency boost, so importance alone decides."""
        importance = activity.importance or 3
        multiplier = 1.0
        if activity.deadline:
            days_left = (activity.deadline.date() - time_service.today()).days
            for max_days, mult in config.URGENCY_MULTIPLIERS:
                if days_left <= max_days:
                    multiplier = mult
                    break
        return importance * multiplier

    async def generate_personalized_plan(self, user_id: int, session: AsyncSession,
                                          start_in_minutes: Optional[int] = None) -> Dict[str, Any]:
        """Builds a full personalized plan for *today*: how many parts,
        how long each, roughly when to sit down for them, plus one
        research-backed tip -- used both by /plan (on demand) and by the
        daily proactive push (see _daily_personal_plan_broadcast). Unlike
        get_personalized_recommendation (one next part), this looks at the
        whole day at once.

        If the user has told the bot about their activities/subjects via
        /activities (each with a 1-5 importance and an optional deadline),
        this also decides *what* goes in each slot -- allocating more
        parts to higher-priority activities and interleaving them instead
        of blocking one activity for the whole day (see _activity_weight
        and RESEARCH_LIBRARY['multi_subject']).

        `start_in_minutes`: when given (the '🆕 برنامه جدید' rebuild button
        -- see cb_plan_new), the usual history-based estimated start hour
        (_estimate_start_hour) is ignored and the first part is scheduled
        to begin exactly this many minutes from right now instead."""
        analysis = await self.analyze_user(user_id, session)
        if not analysis.get("has_data"):
            return {
                "has_data": False,
                "message": (
                    "🗓 برای ساختن یه برنامه‌ی شخصی هنوز داده کافی ندارم.\n"
                    "با یک پارت مطالعه شروع کن، فردا برات یه برنامه‌ی واقعی می‌سازم. 🌱"
                ),
            }

        profile = analysis["profile"]

        pref_result = await session.execute(
            select(UserPreference).where(UserPreference.user_id == user_id)
        )
        pref = pref_result.scalar_one_or_none()
        calm_enabled = bool(pref.calm_mode_enabled) if pref else False

        goal_result = await session.execute(
            select(UserGoal).where(UserGoal.user_id == user_id)
        )
        goal = goal_result.scalar_one_or_none()
        daily_goal = goal.daily_goal if goal else (pref.daily_goal if pref else 90)

        mode = self._detect_personal_mode(profile, calm_enabled)
        base_minutes = self._mode_duration(mode, profile)

        sessions_count = max(1, min(4, round(daily_goal / base_minutes)))

        if start_in_minutes is not None:
            # '🆕 برنامه جدید': skip the history-based estimate entirely and
            # start the first part `start_in_minutes` minutes from right
            # now (minute precision, not just the hour).
            target_dt = time_service.now() + timedelta(minutes=start_in_minutes)
            start_hour = target_dt.hour
            start_minute_of_hour = target_dt.minute
        else:
            # Real, per-user start hour derived from actual session history
            # -- see _estimate_start_hour. Replaces the old
            # `profile.preferred_hour or 18`, which in practice was
            # *always* 18 because that column is never recalculated
            # anywhere else in the codebase.
            start_hour = await self._estimate_start_hour(user_id, profile, session)
            start_minute_of_hour = 0

        activity_repo = ActivityRepository(session)
        activities = await activity_repo.get_active_by_user(user_id)

        alloc_section = ""
        priority_note = ""
        activities_hint = ""
        order: List[Optional[int]] = [None] * sessions_count
        act_by_id: Dict[int, "UserActivity"] = {}
        weight_by_id: Dict[int, float] = {}
        avg_weight = 1.0

        if activities:
            weighted = [(a, self._activity_weight(a)) for a in activities]
            weighted.sort(key=lambda t: t[1], reverse=True)
            total_weight = sum(w for _, w in weighted) or 1.0
            avg_weight = total_weight / len(weighted)

            # Largest-remainder method: give each activity its fair share
            # of the day's sessions_count slots, rounded fairly instead of
            # always rounding down (which would starve everyone but #1).
            raw_shares = [(a, w, sessions_count * w / total_weight) for a, w in weighted]
            counts = {a.id: int(share) for a, w, share in raw_shares}
            assigned = sum(counts.values())
            leftover = sessions_count - assigned
            for a, w, share in sorted(raw_shares, key=lambda t: t[2] - int(t[2]), reverse=True):
                if leftover <= 0:
                    break
                counts[a.id] += 1
                leftover -= 1
            # If there are more slots than distinct activities (leftover
            # still > 0 e.g. sessions_count > len(activities)*1), keep
            # handing extra slots to the highest-weight activities.
            i = 0
            while leftover > 0:
                counts[weighted[i % len(weighted)][0].id] += 1
                leftover -= 1
                i += 1

            act_by_id = {a.id: a for a, _ in weighted}
            weight_by_id = {a.id: w for a, w in weighted}

            # Interleave (round-robin by priority) instead of blocking one
            # activity for the whole day -- see RESEARCH_LIBRARY
            # "multi_subject".
            remaining = dict(counts)
            order_ids: List[int] = []
            while sum(remaining.values()) > 0:
                for a, _ in weighted:
                    if remaining[a.id] > 0:
                        order_ids.append(a.id)
                        remaining[a.id] -= 1
            order = order_ids[:sessions_count]

            priority_note = (
                "<b>🧮 معیار اولویت‌بندی</b>\n"
                "اهمیتی که خودت دادی (۱ تا ۵) × ضریب فوریت بر اساس فاصله‌ی ددلاین (هرچی ددلاین "
                "نزدیک‌تر، ضریب بیشتر) -- دقیقاً مثل اولویت‌بندی آیزنهاور، نه حدس."
            )
            deferred = [a for a, _ in weighted if counts.get(a.id, 0) == 0]
            if deferred:
                # html.escape here matters: activity names come straight
                # from user input, and this message is sent with
                # ParseMode.HTML -- an unescaped "<" or "&" in a name would
                # otherwise corrupt the formatting of the whole message.
                deferred_names = "، ".join(html.escape(a.name) for a in deferred)
                priority_note += f"\n⏭ این‌ها امروز جا نشدن (اولویت پایین‌تر بودن): {deferred_names}."
        else:
            activities_hint = (
                "💡 اگه فعالیت‌ها/درس‌هات رو با دستور /activities به همراه میزان اهمیتشون بهم بگی "
                "(و اگه ددلاین دارن)، برنامه رو دقیقاً بر اساس اولویت‌های واقعیت می‌چینم، نه فقط "
                "زمان‌بندی خام."
            )

        # ------------------------------------------------------------
        # Per-part durations. Every part in the day used to reuse the
        # exact same number (`session_minutes`), so a 3-part day always
        # read "35 min / 35 min / 35 min" regardless of anything else.
        # Real parts shouldn't be uniform: a higher-priority activity
        # earns a somewhat longer part, and sustained-attention research
        # (see Config.FOCUS_SWEET_SPOTS / ULTRADIAN_CEILING_MINUTES,
        # already cited elsewhere in this file) says attention measurably
        # declines across consecutive blocks, so later parts taper down
        # instead of repeating the first part's length verbatim.
        # ------------------------------------------------------------
        FATIGUE_TAPER = [1.0, 0.95, 0.88, 0.8]
        durations: List[int] = []
        for i in range(sessions_count):
            taper = FATIGUE_TAPER[min(i, len(FATIGUE_TAPER) - 1)]
            aid = order[i]
            if aid is not None and weight_by_id:
                priority_factor = 0.85 + 0.3 * min(2.0, weight_by_id[aid] / avg_weight)
            else:
                priority_factor = 1.0
            raw = base_minutes * taper * priority_factor
            raw = max(config.DEFAULT_MIN_DURATION, min(config.DEFAULT_MAX_DURATION, raw))
            durations.append(min(config.CANDIDATE_DURATIONS, key=lambda x: abs(x - raw)))

        # ------------------------------------------------------------
        # Real rest between parts. One flat `gap_minutes` measured
        # start-to-start used to be reused for every single transition,
        # and under certain settings that number could end up small
        # enough to read as parts running straight into each other with
        # no real break. Now every transition gets an explicit break,
        # always at least config.MIN_BREAK_MINUTES, normally the user's
        # own preferred spacing (pref.plan_gap_minutes) -- and once
        # accumulated continuous study crosses the ~90-minute ultradian
        # ceiling (Config.ULTRADIAN_CEILING_MINUTES), that break doubles
        # into a proper long break and the counter resets. If squeezing
        # everything before 22:00 requires compression, only the breaks
        # shrink (down to the floor) -- the parts themselves are never
        # what gets cut to make room.
        # ------------------------------------------------------------
        preferred_gap = max(config.MIN_BREAK_MINUTES, pref.plan_gap_minutes if pref else 60)
        breaks: List[int] = []
        accumulated = 0
        for i in range(sessions_count - 1):
            accumulated += durations[i]
            if accumulated >= config.ULTRADIAN_CEILING_MINUTES:
                breaks.append(preferred_gap * 2)
                accumulated = 0
            else:
                breaks.append(preferred_gap)

        start_total_minutes = start_hour * 60 + start_minute_of_hour
        day_end_minutes = 22 * 60
        window_minutes = max(0, day_end_minutes - start_total_minutes)
        needed_minutes = sum(durations) + sum(breaks)
        gap_compressed = False
        if needed_minutes > window_minutes and breaks:
            overflow = needed_minutes - window_minutes
            shrink_per_break = -(-overflow // len(breaks))  # ceil division
            breaks = [max(config.MIN_BREAK_MINUTES, b - shrink_per_break) for b in breaks]
            gap_compressed = any(b <= config.MIN_BREAK_MINUTES for b in breaks)

        slots: List[str] = []
        cursor_minutes = start_total_minutes
        for i in range(sessions_count):
            # Even after compressing breaks to their floor, a very packed
            # goal (e.g. several long parts starting late in the evening)
            # can still spill past midnight -- clamp the display so we
            # never show a nonsensical "24:xx" or next-day time.
            display_minutes = min(cursor_minutes, 23 * 60 + 45)
            slots.append(f"{display_minutes // 60:02d}:{display_minutes % 60:02d}")
            cursor_minutes += durations[i]
            if i < len(breaks):
                cursor_minutes += breaks[i]

        mode_lines = {
            "recovery": "🌱 امروز حالت بازیابیه -- شروع آروم، بدون فشار.",
            "growth": "🚀 امروز حالت رشده -- می‌تونی کمی چالش‌برانگیزتر بری.",
            "calm": "🌙 امروز حالت آرامشه -- روی تمام‌کردن تمرکز کن، نه حجم.",
            "normal": "📊 امروز روی ریتم معمولت پیش می‌ری.",
        }

        def _schedule_row(i: int, slot: str) -> str:
            aid = order[i]
            act = act_by_id.get(aid) if aid is not None else None
            # html.escape: activity names are user-entered free text, and
            # this message renders with ParseMode.HTML.
            label = f" — {html.escape(act.name)} ({config.IMPORTANCE_LABELS.get(act.importance, '')})" if act else ""
            row = f"🕐 <b>{slot}</b> — پارت {i + 1}: {durations[i]} دقیقه{label}"
            if i < len(breaks):
                row += f"\n     ⏸ استراحت: {breaks[i]} دقیقه"
            return row

        schedule_lines = "\n".join(_schedule_row(i, slot) for i, slot in enumerate(slots))

        # Flat per-part data (independent of the HTML text above) for
        # PlanRepository to persist -- backs the done/not-done checkbox
        # keyboard under the plan message (see plan_keyboard).
        parts_detail: List[Dict[str, Any]] = []
        for i, slot in enumerate(slots):
            aid = order[i]
            act = act_by_id.get(aid) if aid is not None else None
            parts_detail.append({
                "scheduled_label": slot,
                "duration_minutes": durations[i],
                "activity_name": act.name if act else None,
            })

        start_note = ""
        if start_in_minutes is not None:
            first_clock = slots[0] if slots else f"{start_hour:02d}:{start_minute_of_hour:02d}"
            start_note = (
                f"⏳ این برنامه از <b>{start_in_minutes} دقیقه دیگه</b> (حدود ساعت {first_clock}) "
                f"شروع می‌شه.\n\n"
            )

        if activities:
            minute_alloc: Dict[int, int] = {}
            for i, aid in enumerate(order):
                minute_alloc[aid] = minute_alloc.get(aid, 0) + durations[i]
            total_minutes = sum(durations)
            alloc_lines = "\n".join(
                f"› {html.escape(act_by_id[aid].name)}: {mins} دقیقه ({int(round(100 * mins / total_minutes))}٪)"
                for aid, mins in sorted(minute_alloc.items(), key=lambda kv: -kv[1])
            )
            alloc_section = f"<b>📌 تخصیص زمان بر اساس اولویت</b>\n{alloc_lines}"

        research_tip = self.get_research_insight(user_id, profile, mode)
        total_minutes_all = sum(durations)
        divider = "\n\n▫️▫️▫️▫️▫️▫️▫️▫️▫️▫️\n\n"

        break_note = (
            f"⏸ بین هر پارت حداقل {config.MIN_BREAK_MINUTES} دقیقه استراحتِ کامل هست (بدون گوشی/شبکه‌ی "
            f"اجتماعی، تا مغز واقعاً استراحت کنه)، و هر جا حدود {config.ULTRADIAN_CEILING_MINUTES} دقیقه "
            f"مطالعه‌ی پیوسته جمع بشه، یه استراحت بلندتر می‌ذارم. فاصله‌ی پایه رو می‌تونی از "
            f"⚙️ تنظیمات › فاصله بین پارت‌ها عوض کنی."
        )
        if gap_compressed:
            break_note += (
                "\n\n⚠️ با تعداد و طول پارت‌های امروز، فاصله‌ی دلخواهت قبل از ساعت ۲۲ جا نمی‌شد؛ "
                "فقط زمان استراحت‌ها رو کمی فشرده‌تر کردم، نه خودِ پارت‌ها رو."
            )

        sections = [
            (
                f"{start_note}"
                f"<b>🗓 برنامه شخصی امروز</b>\n\n"
                f"🎯 هدف کلی: <b>{total_minutes_all} دقیقه</b> در {sessions_count} پارت "
                f"(طول هر پارت با اولویت فعالیت و ریتم روزت تنظیم می‌شه، نه یه عدد ثابت)\n\n"
                f"{schedule_lines}"
            ),
            alloc_section,
            break_note,
            f"{mode_lines.get(mode, mode_lines['normal'])}",
            research_tip + (f"\n\n{activities_hint}" if activities_hint else ""),
            priority_note,
        ]
        message = divider.join(s for s in sections if s) + (
            "\n\n این برنامه ثابت نیست -- هر وقت خواستی می‌تونی با /session یه پارت بسازی که با این "
            "زمان‌بندی فرق داره، من خودم رو باهاش تطبیق می‌دم."
        )

        return {
            "has_data": True,
            "message": message,
            "mode": mode,
            "session_minutes": base_minutes,
            "durations": durations,
            "breaks": breaks,
            "sessions_count": sessions_count,
            "schedule": slots,
            "start_hour": start_hour,
            "parts_detail": parts_detail,
            "start_in_minutes": start_in_minutes,
        }


advisor = Advisor()


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
        except Exception:
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
    # Both of these existed before but were never actually wired to any
    # handler -- the admin panel showed a "🧠 الگوریتم" screen that told
    # you to run a `/set_algorithm` command that didn't exist anywhere in
    # the codebase, and there was no per-group settings screen at all
    # (GroupSetting's numeric fields -- default_duration, quiet hours,
    # poll window, etc. -- could only ever be changed by hand-editing the
    # DB). Both states are now used for real below.
    SET_ALGORITHM = State()
    SET_GROUP_SETTINGS = State()
    SET_TUNABLE = State()
    ADD_SENTENCE = State()
    SELECT_CATEGORY = State()
    SET_BACKUP_INTERVAL = State()
    SET_BACKUP_TARGET = State()
    RESTORE_UPLOAD = State()
    BROADCAST_NOTIFICATION = State()

class UserStates(StatesGroup):
    SET_DAILY_GOAL = State()
    SET_PREFERRED_DURATION = State()
    SET_PLAN_GAP = State()
    SET_PLAN_START_OFFSET = State()
    REPORT_MANUAL_DURATION = State()
    ADD_ACTIVITY_NAME = State()
    ADD_ACTIVITY_DEADLINE = State()
    ROUTINE_ADD_NAME = State()
    ROUTINE_ADD_ONCE_DAYS = State()
    ROUTINE_ADD_DURATION = State()
    ROUTINE_ADD_TIME = State()


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
                    if settings and settings.consecutive_misses > 0:
                        # Real human activity is exactly what should clear a
                        # pause built up from several missed polls in a row
                        # -- give the group a clean slate so the next
                        # automatic poll (triggered right below, or the next
                        # morning cron) isn't immediately skipped again.
                        settings.consecutive_misses = 0
                        await session.flush()

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
    kb.row(
        InlineKeyboardButton(text="🧠 تحلیل و توصیه", callback_data="menu:advice"),
        InlineKeyboardButton(text="🗓 برنامه امروز", callback_data="menu:plan"),
    )
    kb.row(InlineKeyboardButton(text="📋 فعالیت‌ها و اولویت‌ها", callback_data="act:list"))
    kb.row(InlineKeyboardButton(text="🔁 روتین‌ها", callback_data="menu:routine"))
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
        InlineKeyboardButton(text="🟢 انجام دادم", callback_data=f"result:{session_id}:COMPLETED"),
        InlineKeyboardButton(text="🔴 انجام ندادم", callback_data=f"result:{session_id}:MISSED"),
    )
    kb.row(InlineKeyboardButton(text="⏱ ثبت دستی میزان مطالعه", callback_data=f"result:{session_id}:MANUAL"))
    return kb.as_markup()


def topic_picker_keyboard(session_id: int, activities: List["UserActivity"]) -> InlineKeyboardMarkup:
    """Lets whoever created a part tag it with one of their own
    /activities entries (or skip). See cmd_session / cb_session_topic_pick
    and Session.topic. Up to 6 shown -- this is a quick tag, not a full
    activity picker."""
    kb = InlineKeyboardBuilder()
    for act in activities[:6]:
        kb.row(InlineKeyboardButton(text=f"📖 {act.name}", callback_data=f"stopic:{session_id}:{act.id}"))
    kb.row(InlineKeyboardButton(text="⏭ بدون موضوع خاص", callback_data=f"stopic:{session_id}:skip"))
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
        InlineKeyboardButton(text="⚙️ تنظیمات عددی", callback_data="admin:tunables"),
        InlineKeyboardButton(text="👥 تنظیمات گروه‌ها", callback_data="admin:groups"),
    )
    kb.row(
        InlineKeyboardButton(text="🛠 سیستم", callback_data="admin:system"),
        InlineKeyboardButton(text="📜 لاگ‌ها", callback_data="admin:logs"),
    )
    kb.row(InlineKeyboardButton(text="💾 پشتیبان", callback_data="admin:backup"))
    kb.row(
        InlineKeyboardButton(text="⏱ فاصله پشتیبان‌گیری", callback_data="admin:backup_interval"),
        InlineKeyboardButton(text="♻️ بازیابی", callback_data="admin:restore"),
    )
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


def tunables_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for key, meta in TUNABLE_SETTINGS.items():
        current = getattr(config, meta["attr"])
        kb.row(InlineKeyboardButton(text=f"{meta['label']}: {current}", callback_data=f"tunable:{key}"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


def algorithm_weights_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for key, value in config.ALGORITHM_WEIGHTS.items():
        kb.row(InlineKeyboardButton(text=f"{key}: {value}", callback_data=f"algw:{key}"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


def admin_groups_keyboard(groups: List["Group"]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for group in groups:
        title = group.title or f"گروه #{group.id}"
        kb.row(InlineKeyboardButton(text=f"👥 {title}", callback_data=f"admin:group:{group.id}"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


def group_settings_keyboard(group_id: int, settings: "GroupSetting") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(
        text=f"{'🟢 پارت خودکار: روشن' if settings.auto_poll_enabled else '⚪️ پارت خودکار: خاموش'}",
        callback_data=f"gpoll:toggle:{group_id}",
    ))
    for field, meta in GROUP_TUNABLE_FIELDS.items():
        current = getattr(settings, field)
        kb.row(InlineKeyboardButton(
            text=f"{meta['label']}: {current}", callback_data=f"gset:{group_id}:{field}"
        ))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:groups"))
    return kb.as_markup()


def group_autopoll_keyboard(group_id: int, enabled: bool) -> InlineKeyboardMarkup:
    """Standalone on/off toggle for GroupSetting.auto_poll_enabled, used by
    the in-group /autopoll command (see cmd_autopoll) so group admins can
    flip it themselves without going through the central admin panel.
    Deliberately a single button -- same on/off toggle pattern as
    calm_mode_keyboard / notification_settings_keyboard."""
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(
        text="⚪️ خاموش کردن پارت خودکار" if enabled else "🟢 روشن کردن پارت خودکار",
        callback_data=f"gpoll:toggle:{group_id}",
    ))
    return kb.as_markup()


def backup_menu_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📥 دریافت پشتیبان همین الان", callback_data="admin:backup"))
    kb.row(InlineKeyboardButton(text="⏱ تغییر فاصله زمانی", callback_data="admin:backup_interval"))
    kb.row(InlineKeyboardButton(text="🎯 تنظیم مقصد بک‌آپ", callback_data="admin:backup_target"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin:back"))
    return kb.as_markup()


def logs_menu_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="🔎 ۵۰ خط آخر", callback_data="admin:logs:tail:50"),
        InlineKeyboardButton(text="🔎 ۲۰۰ خط آخر", callback_data="admin:logs:tail:200"),
    )
    kb.row(InlineKeyboardButton(text="⚠️ فقط خطاها (ERROR)", callback_data="admin:logs:errors"))
    kb.row(InlineKeyboardButton(text="📄 ارسال فایل لاگ کامل", callback_data="admin:logs:file"))
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


def cancel_keyboard(target: str) -> InlineKeyboardMarkup:
    """A single '❌ لغو' button attached to every prompt that puts the user
    into an FSM state waiting for text/file input. `target` tells the
    generic cancel handler (cb_cancel_input) which menu to return to once
    the state is cleared."""
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="❌ لغو", callback_data=f"cancel:{target}"))
    return kb.as_markup()


# ------------------------------------------------------------
# Message categories -- glass-style chip picker
# Each entry: (code stored in DB, emoji, Persian label)
# ------------------------------------------------------------
MESSAGE_CATEGORIES: List[Tuple[str, str, str]] = [
    ("SESSION_START", "🟢", "شروع جلسه"),
    ("SESSION_END", "🏁", "پایان جلسه"),
    ("SUCCESS", "✅", "موفقیت"),
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
        "عالیه! {minutes} دقیقه مطالعه‌ی واقعی رو ثبت کردی 🎉",
        "این یعنی داری به هدفت نزدیک‌تر میشی، همینطور ادامه بده ✨ ({minutes} دقیقه ثبت شد)",
        "{minutes} دقیقه دیگه به کارنامه‌ت اضافه شد، آفرین 🏆",
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
    ("daily_plan_enabled", "🧭 برنامه شخصی روزانه (بدون درخواست)"),
]

# Cycle order for where the daily proactive plan gets delivered.
PROACTIVE_DELIVERY_OPTIONS: List[Tuple[str, str]] = [
    ("private", "💬 فقط پیام خصوصی"),
    ("group", "👥 فقط گروه(های) مطالعه"),
    ("both", "💬👥 هر دو"),
]


def proactive_delivery_keyboard(current: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    labels = dict(PROACTIVE_DELIVERY_OPTIONS)
    current_label = labels.get(current, labels["private"])
    kb.row(InlineKeyboardButton(
        text=f"محل ارسال فعلی: {current_label} (لمس کن برای تغییر)",
        callback_data="settings:delivery:cycle",
    ))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu:settings"))
    return kb.as_markup()


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


def calm_mode_keyboard(enabled: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    icon = "🟢" if enabled else "⚪️"
    label = "فعال" if enabled else "غیرفعال"
    kb.row(InlineKeyboardButton(text=f"{icon} حالت آرامش: {label} (لمس کن)", callback_data="settings:calm:toggle"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu:settings"))
    return kb.as_markup()


def importance_picker_keyboard(callback_prefix: str) -> InlineKeyboardMarkup:
    """Inline 1-5 importance picker, reused for both adding a new activity
    and re-rating an existing one. `callback_prefix` already contains
    whatever context (e.g. an activity id) the handler needs, so this just
    appends ':{n}'."""
    kb = InlineKeyboardBuilder()
    for level, label in config.IMPORTANCE_LABELS.items():
        kb.row(InlineKeyboardButton(text=label, callback_data=f"{callback_prefix}:{level}"))
    return kb.as_markup()


def activities_list_keyboard(activities: List["UserActivity"]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for activity in activities:
        label = f"{activity.name} — {config.IMPORTANCE_LABELS.get(activity.importance, '')}"
        kb.row(InlineKeyboardButton(text=label, callback_data=f"act:view:{activity.id}"))
    kb.row(InlineKeyboardButton(text="➕ افزودن فعالیت جدید", callback_data="act:add"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu:back"))
    return kb.as_markup()


def activity_detail_keyboard(activity_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="⭐ تغییر میزان اهمیت", callback_data=f"act:reimp:{activity_id}"))
    kb.row(InlineKeyboardButton(text="🗑 حذف این فعالیت", callback_data=f"act:del:{activity_id}"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت به لیست", callback_data="act:list"))
    return kb.as_markup()


# ------------------------------------------------------------
# Routines (بخش روتین) -- see UserRoutine model docstring for the
# kind/repeat_type design this keyboard set walks the user through.
# ------------------------------------------------------------
PERSIAN_WEEKDAYS: List[str] = ["شنبه", "یکشنبه", "دوشنبه", "سه‌شنبه", "چهارشنبه", "پنجشنبه", "جمعه"]
# Maps our 0=شنبه..6=جمعه indexing onto APScheduler's CronTrigger
# day_of_week names (which follow the Western mon..sun week).
PERSIAN_DAY_TO_CRON: Dict[int, str] = {0: "sat", 1: "sun", 2: "mon", 3: "tue", 4: "wed", 5: "thu", 6: "fri"}
# A handful of evidence-anchored durations (see config.CANDIDATE_DURATIONS /
# FOCUS_SWEET_SPOTS above) offered as one-tap options when a routine is set
# up as a study پارت, plus a free-text fallback for anything else.
ROUTINE_DURATION_PRESETS: List[int] = [15, 25, 30, 45, 60, 90]


def routines_list_keyboard(routines: List["UserRoutine"]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for r in routines:
        icon = "🔔" if r.kind == "reminder" else "📖"
        time_label = f"{r.hour:02d}:{r.minute:02d}"
        streak_label = f" 🔥{r.current_streak}" if r.current_streak else ""
        kb.row(InlineKeyboardButton(
            text=f"{icon} {r.title} — {time_label}{streak_label}",
            callback_data=f"routine:view:{r.id}",
        ))
    kb.row(InlineKeyboardButton(text="➕ افزودن روتین جدید", callback_data="routine:add"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu:back"))
    return kb.as_markup()


def routine_kind_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔔 فقط یادآوری", callback_data="routine:kind:reminder"))
    kb.row(InlineKeyboardButton(text="📖 به‌عنوان یه پارت مطالعه", callback_data="routine:kind:part"))
    kb.row(InlineKeyboardButton(text="❌ لغو", callback_data="cancel:routine"))
    return kb.as_markup()


def routine_duration_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    row: List[InlineKeyboardButton] = []
    for d in ROUTINE_DURATION_PRESETS:
        row.append(InlineKeyboardButton(text=f"{d} دقیقه", callback_data=f"routine:dur:{d}"))
        if len(row) == 3:
            kb.row(*row)
            row = []
    if row:
        kb.row(*row)
    kb.row(InlineKeyboardButton(text="✏️ عدد دلخواه", callback_data="routine:dur:custom"))
    kb.row(InlineKeyboardButton(text="❌ لغو", callback_data="cancel:routine"))
    return kb.as_markup()


def routine_repeat_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="📅 فقط یک‌بار", callback_data="routine:repeat:once"))
    kb.row(InlineKeyboardButton(text="🔁 هر روز", callback_data="routine:repeat:daily"))
    kb.row(InlineKeyboardButton(text="🗓 روزهای خاصی از هفته", callback_data="routine:repeat:weekly"))
    kb.row(InlineKeyboardButton(text="❌ لغو", callback_data="cancel:routine"))
    return kb.as_markup()


def routine_days_keyboard(selected: List[int]) -> InlineKeyboardMarkup:
    """Toggleable شنبه..جمعه picker for repeat_type == 'weekly'.
    `selected` is kept in FSMContext data between taps (see
    cb_routine_toggle_day) and re-rendered here with a ✅ on chosen days."""
    kb = InlineKeyboardBuilder()
    row: List[InlineKeyboardButton] = []
    for i, name in enumerate(PERSIAN_WEEKDAYS):
        mark = "✅ " if i in selected else ""
        row.append(InlineKeyboardButton(text=f"{mark}{name}", callback_data=f"routine:day:{i}"))
        if len(row) == 2:
            kb.row(*row)
            row = []
    if row:
        kb.row(*row)
    kb.row(InlineKeyboardButton(text="➡️ تایید و ادامه", callback_data="routine:days_done"))
    kb.row(InlineKeyboardButton(text="❌ لغو", callback_data="cancel:routine"))
    return kb.as_markup()


def routine_detail_keyboard(routine_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🗑 حذف این روتین", callback_data=f"routine:del:{routine_id}"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت به لیست", callback_data="routine:list"))
    return kb.as_markup()


def routine_fire_keyboard(routine_id: int, kind: str) -> InlineKeyboardMarkup:
    """Attached to the message sent when a routine actually fires (see
    _routine_fire). A 'part' gets the same completed/missed choice a
    real study پارت gets; a plain 'reminder' just gets a single
    acknowledge tap -- both feed the same streak counter."""
    kb = InlineKeyboardBuilder()
    if kind == "part":
        kb.row(
            InlineKeyboardButton(text="🟢 انجام دادم", callback_data=f"routdone:{routine_id}"),
            InlineKeyboardButton(text="🔴 انجام ندادم", callback_data=f"routmiss:{routine_id}"),
        )
    else:
        kb.row(InlineKeyboardButton(text="✅ انجام شد", callback_data=f"routdone:{routine_id}"))
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
    kb.row(InlineKeyboardButton(text="⏳ فاصله بین پارت‌ها", callback_data="settings:gap"))
    kb.row(InlineKeyboardButton(text="⏱ شروع برنامه جدید", callback_data="settings:plan_start_offset"))
    kb.row(InlineKeyboardButton(text="🧭 محل ارسال برنامه شخصی", callback_data="settings:delivery"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="settings:back"))
    return kb.as_markup()


def plan_keyboard(parts: List["UserPlanPart"]) -> InlineKeyboardMarkup:
    """Buttons under a daily plan message: one toggle per part (✅ انجام
    شد / ⬜ هنوز نه -- tap to flip), plus '🆕 برنامه جدید' to throw the
    whole plan away and build a fresh one starting from the user's
    configured offset from right now (see settings:plan_start_offset)."""
    kb = InlineKeyboardBuilder()
    for part in parts:
        mark = "✅" if part.is_done else "⬜"
        label = f"{mark} پارت {part.part_index + 1} ({part.scheduled_label} — {part.duration_minutes} دقیقه)"
        kb.row(InlineKeyboardButton(text=label, callback_data=f"planpart:toggle:{part.id}"))
    kb.row(InlineKeyboardButton(text="🆕 برنامه جدید", callback_data="plan:new"))
    kb.row(InlineKeyboardButton(text="🔙 بازگشت", callback_data="menu:back"))
    return kb.as_markup()


# ============================================================
# COMMAND HANDLERS
# ============================================================
@router.message(F.pinned_message)
async def on_pin_service_message(message: Message):
    """Whenever bot.pin_chat_message() succeeds, Telegram itself posts a
    'X pinned this message' service notice in the chat -- separate from
    the message that got pinned. It's just clutter right after the part
    -start announcement gets pinned (see _attendance_timeout), so remove
    it immediately. This fires for any pin in the chat, but the bot is
    the only thing that pins messages in this flow, so scoping it to
    "any pin notice" is equivalent in practice to "our own pin notice".
    """
    try:
        await message.delete()
    except Exception as e:
        logger.debug(f"Could not delete pin service message {message.message_id}: {e}")


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

        # Guard against a second, overlapping part. This was missing
        # entirely -- _create_readiness_poll (the automatic poll) already
        # refused to fire while a session was in flight, but /session
        # itself didn't check, so running it while an earlier part was
        # still in ATTENDANCE/READY/STARTED created two live sessions in
        # parallel: two roster messages, two "پارت شروع/پایان شد"
        # announcements interleaved in the same chat, people tapping the
        # wrong session's buttons -- reported as parts' data looking
        # "mixed together". One part in flight at a time per group, same
        # as the automatic poll.
        active_check = await session.execute(
            select(Session).where(
                Session.group_id == group_id,
                Session.status.in_(["ATTENDANCE", "READY", "STARTED"]),
            )
        )
        active_sess = active_check.scalar_one_or_none()
        if active_sess:
            await message.answer(
                "⏳ یه پارت دیگه توی همین گروه الان در جریانه (یا منتظر اعلام آماده‌گیه).\n"
                "بذار همون یکی تموم بشه، بعد پارت جدید رو شروع کن -- وگرنه گزارش‌ها و پیام‌ها با هم قاطی می‌شن."
            )
            return

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

        sent = await message.answer(
            f"📚 پارت مطالعاتی جدید!\n\n"
            f"🕐 زمان: {time_service.format_datetime(start_time)}\n"
            f"🎯 هدف: {rec['target']} دقیقه\n"
            f"🌱 حداقل: {rec['minimum']} دقیقه\n"
            f"🔥 حداکثر: {rec['extension']} دقیقه\n\n"
            f"چه کسانی هستن؟ 🟢",
            reply_markup=attendance_keyboard(sess.id)
        )
        sess.attendance_message_id = sent.message_id
        await session.flush()

        # Optional subject/lesson tag for this part -- purely so /report
        # can later break down study time per درس instead of just a
        # total. Pulled from the initiator's own /activities list; skip
        # if they don't use one or don't care for this part. Can also be
        # set/changed later with /topic (e.g. for auto-poll-started parts,
        # which have no single "initiator" to ask).
        activity_repo = ActivityRepository(session)
        activities = await activity_repo.get_active_by_user(user.id)
        if activities:
            await message.answer(
                "📖 این پارت رو برای کدوم درس/فعالیت می‌ذاری؟ (اختیاریه)",
                reply_markup=topic_picker_keyboard(sess.id, activities),
            )


@router.message(Command("autopoll"))
async def cmd_autopoll(message: Message):
    """Lets a group's own Telegram admins/owner (or a global bot admin)
    switch that group's automatic readiness poll (auto_poll_enabled) on
    or off from right inside the group -- no need to go through the
    central admin panel. Run only in the group whose setting you want to
    change; in private chat it just explains that.

    Off means: no more automatic "آماده پارت هستید؟" poll -- not the
    morning one, not the one after a session ends. /session still works
    as a manual, on-demand way to start a part any time."""
    if message.chat.type not in ("group", "supergroup"):
        await message.answer(
            "این دستور فقط داخل خود گروه کار می‌کنه -- توی همون گروهی که "
            "می‌خوای پارت خودکارش رو روشن/خاموش کنی بزنش."
        )
        return

    if not await _is_group_chat_admin(message.bot, message.chat.id, message.from_user.id):
        await message.answer("⛔ این تنظیم رو فقط ادمین‌های همین گروه می‌تونن تغییر بدن.")
        return

    async with get_session() as session:
        group_repo = GroupRepository(session)
        group = await group_repo.get_by_chat_id(message.chat.id)
        if not group:
            await message.answer("❌ هنوز این گروه ثبت نشده -- یه پیام دیگه بفرست تا ثبت بشه، بعد دوباره امتحان کن.")
            return
        result = await session.execute(select(GroupSetting).where(GroupSetting.group_id == group.id))
        settings = result.scalar_one_or_none()
        if not settings:
            await message.answer("❌ این گروه تنظیماتی ندارد.")
            return
        enabled = settings.auto_poll_enabled
        group_id = group.id

    await message.answer(
        f"🔔 پارت خودکار این گروه الان <b>{'روشنه' if enabled else 'خاموشه'}</b>.\n\n"
        "وقتی روشنه، هر روز صبح و بین پارت‌ها ربات خودش نظرسنجی «آماده پارت هستید؟» می‌فرسته. "
        "وقتی خاموشه، فقط با دستور دستی /session پارت شروع می‌شه.",
        reply_markup=group_autopoll_keyboard(group_id, enabled),
    )


@router.message(Command("today"))
async def cmd_today(message: Message):
    await _send_today_status(message.from_user.id, message)


async def _send_today_status(telegram_user_id: int, target: Message):
    """Shared by /today and the 'امروز' button. Takes the acting user's
    Telegram id explicitly, since when this is called from the button
    callback, target.from_user would be the bot itself (the owner of the
    message the button is attached to), not the person who tapped it."""
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(telegram_user_id)
        if not user:
            await target.answer("❌ لطفاً ابتدا /start را بزنید.")
            return
        assistant = Assistant()
        status = await assistant.get_today_status(user.id, session)
        await target.answer(status["message"], reply_markup=main_menu())


@router.message(Command("advice"))
async def cmd_advice(message: Message):
    await _send_advice(message.from_user.id, message)


async def _send_advice(telegram_user_id: int, target: Message):
    """Shared by /advice and the '🧠 تحلیل و توصیه' button. Looks up the
    caller's own behavior profile and returns both the 'why' (Advisor's
    performance analysis) and the 'what next' (a concrete part-length
    suggestion), picked from whichever mode -- recovery/growth/calm/normal
    -- actually fits this person right now."""
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(telegram_user_id)
        if not user:
            await target.answer("❌ لطفاً ابتدا /start را بزنید.")
            return
        result = await advisor.get_personalized_recommendation(user.id, session)
        await target.answer(result["message"], reply_markup=main_menu())


@router.message(Command("status"))
async def cmd_admin_status(message: Message):
    """Admin-only: /status <آیدی عددی یا یوزرنیم> -- looks up any user
    (by their numeric Telegram id or their @username, with or without the
    @) and returns the same depth of behavioral analysis /advice gives
    someone about themselves, plus this week's raw numbers and a set of
    citation-backed study-method recommendations picked to match that
    specific person's data (see Advisor.admin_status_report). Works in DM
    or in a group; only checks config.ADMIN_IDS, not group role."""
    if message.from_user.id not in config.ADMIN_IDS:
        await message.answer("⛔️ این دستور فقط برای ادمین‌هاست.")
        return

    arg = (message.text or "").split(maxsplit=1)
    query = arg[1].strip() if len(arg) > 1 else None
    if not query:
        await message.answer(
            "مثال:\n"
            "/status 123456789  (آیدی عددی تلگرام)\n"
            "/status @username\n"
            "/status username"
        )
        return

    async with get_session() as session:
        repo = UserRepository(session)
        user = None
        # A numeric-only query is almost certainly the Telegram id (the
        # thing an admin would actually have handy from a forwarded
        # message or a group member list); usernames can't be purely
        # numeric on Telegram, so there's no ambiguity here.
        if query.lstrip("@").isdigit():
            user = await repo.get_by_telegram_id(int(query.lstrip("@")))
        else:
            user = await repo.get_by_username(query)

        if not user:
            await message.answer(
                f"❌ کاربری با «{html.escape(query)}» پیدا نشد. "
                f"مطمئن شو آیدی عددی درسته یا این فرد قبلاً با ربات /start زده."
            )
            return

        result = await advisor.admin_status_report(user.id, session)
        display_name = _display_name(user) if "_display_name" in globals() else (
            user.username and f"@{user.username}" or user.first_name or str(user.telegram_id)
        )
        header = f"👤 <b>وضعیت مطالعاتی {html.escape(display_name)}</b> (آیدی: <code>{user.telegram_id}</code>)\n\n"
        await message.answer(header + result["message"])


@router.message(Command("plan"))
async def cmd_plan(message: Message):
    await _send_personalized_plan(message.from_user.id, message)


async def _send_personalized_plan(telegram_user_id: int, target: Message,
                                   start_in_minutes: Optional[int] = None):
    """Shared by /plan, the '🗓 برنامه امروز' button, the '🆕 برنامه جدید'
    rebuild (cb_plan_new), and the daily proactive push -- builds a
    full-day plan (how many parts, roughly when, how long each) instead of
    just the single next-part suggestion /advice gives, and folds in one
    research-backed tip from config.RESEARCH_LIBRARY.

    The parts are persisted (see PlanRepository) so each one can be
    checked off individually from the reply keyboard, and -- when
    `start_in_minutes` is given -- the first part starts that many minutes
    from now instead of the usual history-estimated hour."""
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(telegram_user_id)
        if not user:
            await target.answer("❌ لطفاً ابتدا /start را بزنید.")
            return
        result = await advisor.generate_personalized_plan(user.id, session, start_in_minutes=start_in_minutes)
        if not result.get("has_data"):
            await target.answer(result["message"], reply_markup=main_menu())
            return
        plan_repo = PlanRepository(session)
        _, plan_parts = await plan_repo.replace_today_plan(
            user.id, result.get("parts_detail", []),
            source="manual" if start_in_minutes is not None else "auto",
        )
        await session.commit()
        await target.answer(result["message"], reply_markup=plan_keyboard(plan_parts))


@router.message(Command("activities"))
async def cmd_activities(message: Message):
    await _send_activities_list(message.from_user.id, message)


# Persian weekday name -> Python weekday() int (Monday=0 ... Sunday=6).
# Accepts the name with or without the ZWNJ / a plain space (e.g. both
# "سه‌شنبه" and "سه شنبه"), see _parse_persian_weekday.
PERSIAN_WEEKDAY_ALIASES: Dict[str, int] = {
    "شنبه": 5,
    "یکشنبه": 6,
    "دوشنبه": 0,
    "سهشنبه": 1,
    "چهارشنبه": 2,
    "پنجشنبه": 3,
    "جمعه": 4,
}
PERSIAN_WEEKDAY_LABELS: Dict[int, str] = {
    5: "شنبه", 6: "یکشنبه", 0: "دوشنبه", 1: "سه‌شنبه",
    2: "چهارشنبه", 3: "پنجشنبه", 4: "جمعه",
}


def _parse_persian_weekday(text: str) -> Optional[int]:
    normalized = text.strip().replace("‌", "").replace(" ", "")
    return PERSIAN_WEEKDAY_ALIASES.get(normalized)


def _most_recent_date_for_weekday(weekday_target: int) -> date:
    """Today if it already is that weekday, otherwise the most recent
    past date that was."""
    today_dt = time_service.now()
    delta = (today_dt.weekday() - weekday_target) % 7
    return (today_dt - timedelta(days=delta)).date()


@router.message(Command("report"))
async def cmd_report(message: Message):
    """Unified per-subject/lesson report -- the two places subject info
    actually lives in this bot are unrelated tables, so this picks
    whichever applies to where the command was run: the personal daily
    plan (UserPlanPart.activity_name) in a private chat, or this group's
    tagged parts (Session.topic, see /topic and cb_session_topic_pick)
    in a group chat.

    Inside a group, an optional weekday name switches to a detailed
    per-part log for that day instead of the 7-day topic summary --
    e.g. "/report دوشنبه" lists every part held the most recent Monday,
    each with its actual start/end clock time and (if tagged) its
    subject, plus a per-subject breakdown at the bottom if more than
    one topic shows up that day."""
    arg = (message.text or "").split(maxsplit=1)
    weekday_arg = arg[1].strip() if len(arg) > 1 else None

    if message.chat.type in ("group", "supergroup"):
        if weekday_arg:
            weekday_target = _parse_persian_weekday(weekday_arg)
            if weekday_target is None:
                await message.answer(
                    "❌ روز هفته رو نشناختم. یکی از این‌ها رو بنویس:\n"
                    "شنبه، یکشنبه، دوشنبه، سه‌شنبه، چهارشنبه، پنجشنبه، جمعه\n\n"
                    "مثال: /report دوشنبه"
                )
                return
            await _send_group_day_report(message, weekday_target)
        else:
            await _send_group_topic_report(message)
    else:
        await _send_personal_topic_report(message.from_user.id, message)


async def _send_group_day_report(message: Message, weekday_target: int):
    """Detailed per-part log for the most recent occurrence of
    weekday_target in this group: every part that actually started that
    day, its start/end clock time, and its subject if tagged -- plus a
    per-subject minute breakdown at the bottom when more than one topic
    shows up. See PERSIAN_WEEKDAY_ALIASES / cmd_report."""
    target_date = _most_recent_date_for_weekday(weekday_target)
    day_label = PERSIAN_WEEKDAY_LABELS[weekday_target]
    naive_start = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0)
    day_start = time_service.tz.localize(naive_start)
    day_end = day_start + timedelta(days=1) - timedelta(seconds=1)

    async with get_session() as session:
        group_repo = GroupRepository(session)
        group = await group_repo.get_by_chat_id(message.chat.id)
        if not group:
            await message.answer("❌ این گروه هنوز ثبت نشده.")
            return
        manager = SessionManager(session)
        result = await session.execute(
            select(Session)
            .where(
                Session.group_id == group.id,
                Session.actual_start != None,
                Session.actual_start >= day_start,
                Session.actual_start <= day_end,
            )
            .order_by(Session.actual_start)
        )
        sessions = result.scalars().all()
        if not sessions:
            await message.answer(
                f"📅 توی {day_label} ({target_date.isoformat()}) هیچ پارتی توی این گروه شروع نشده."
            )
            return

        rows: List[str] = []
        topic_minutes: Dict[str, int] = {}
        topic_counts: Dict[str, int] = {}
        for sess in sessions:
            part_number = await manager.get_group_part_number(group.id, sess.id)
            start_str = time_service.format_datetime(sess.actual_start, "%H:%M")
            if sess.actual_end:
                end_str = time_service.format_datetime(sess.actual_end, "%H:%M")
                duration = round((sess.actual_end - sess.actual_start).total_seconds() / 60)
            else:
                end_str = "هنوز در جریانه"
                duration = None
            topic = sess.topic or "بدون موضوع خاص"
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
            if duration is not None:
                topic_minutes[topic] = topic_minutes.get(topic, 0) + duration
            duration_note = f" ({duration} دقیقه)" if duration is not None else ""
            topic_note = f" — 📖 {html.escape(sess.topic)}" if sess.topic else ""
            rows.append(f"{part_number}. 🚀 {start_str} → 🏁 {end_str}{duration_note}{topic_note}")

        distinct_topics = {sess.topic for sess in sessions if sess.topic}

    lines = [f"📅 <b>گزارش {day_label} ({target_date.isoformat()})</b>", "➖➖➖➖➖➖➖➖➖➖"]
    lines.extend(rows)
    lines.append("➖➖➖➖➖➖➖➖➖➖")
    lines.append(f"🧮 مجموع: {len(sessions)} پارت")
    if distinct_topics:
        lines.append("\n📊 <b>به تفکیک درس:</b>")
        for topic, minutes in sorted(topic_minutes.items(), key=lambda kv: -kv[1]):
            lines.append(f"📖 {html.escape(topic)}: {minutes} دقیقه ({topic_counts[topic]} پارت)")
    await message.answer("\n".join(lines))


async def _send_personal_topic_report(telegram_user_id: int, target: Message):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(telegram_user_id)
        if not user:
            await target.answer("❌ لطفاً ابتدا /start را بزنید.")
            return
        plan_repo = PlanRepository(session)
        plan, parts = await plan_repo.get_today_plan_with_parts(user.id)

    if not plan or not parts:
        await target.answer("📊 امروز هنوز برنامه‌ای ثبت نشده. با /plan یکی بساز تا بشه ازش گزارش گرفت.")
        return

    by_activity: Dict[str, Dict[str, int]] = {}
    for p in parts:
        key = p.activity_name or "بدون موضوع خاص"
        bucket = by_activity.setdefault(key, {"done": 0, "total": 0, "done_count": 0, "total_count": 0})
        bucket["total"] += p.duration_minutes
        bucket["total_count"] += 1
        if p.is_done:
            bucket["done"] += p.duration_minutes
            bucket["done_count"] += 1

    lines = ["📊 <b>گزارش درسی امروز (برنامه شخصی)</b>", "➖➖➖➖➖➖➖➖➖➖"]
    for name, b in sorted(by_activity.items(), key=lambda kv: -kv[1]["total"]):
        lines.append(
            f"📖 {html.escape(name)}: <b>{b['done']}/{b['total']}</b> دقیقه "
            f"({b['done_count']}/{b['total_count']} پارت)"
        )
    total_done = sum(b["done"] for b in by_activity.values())
    total_all = sum(b["total"] for b in by_activity.values())
    lines.append("➖➖➖➖➖➖➖➖➖➖")
    lines.append(f"🧮 مجموع: {total_done}/{total_all} دقیقه")
    await target.answer("\n".join(lines))


async def _send_group_topic_report(message: Message):
    async with get_session() as session:
        group_repo = GroupRepository(session)
        group = await group_repo.get_by_chat_id(message.chat.id)
        if not group:
            await message.answer("❌ این گروه هنوز ثبت نشده.")
            return
        cutoff = time_service.days_ago(7)
        sessions_result = await session.execute(
            select(Session).where(
                Session.group_id == group.id,
                Session.created_at >= cutoff,
                Session.status == "ENDED",
            )
        )
        sessions = sessions_result.scalars().all()
        if not sessions:
            await message.answer("📊 هنوز هیچ پارت تموم‌شده‌ای توی این ۷ روز ثبت نشده.")
            return
        session_ids = [s.id for s in sessions]
        topic_by_session = {s.id: (s.topic or "بدون موضوع خاص") for s in sessions}
        results_result = await session.execute(
            select(SessionResult).where(SessionResult.session_id.in_(session_ids))
        )
        results = results_result.scalars().all()

    by_topic: Dict[str, int] = {}
    parts_by_topic: Dict[str, set] = {}
    for r in results:
        if r.actual_duration is None:
            continue
        topic = topic_by_session.get(r.session_id, "بدون موضوع خاص")
        by_topic[topic] = by_topic.get(topic, 0) + r.actual_duration
        parts_by_topic.setdefault(topic, set()).add(r.session_id)

    if not by_topic:
        await message.answer("📊 برای پارت‌های این هفته هنوز کسی نتیجه‌ای ثبت نکرده.")
        return

    lines = ["📊 <b>گزارش درسی گروه (۷ روز اخیر)</b>", "➖➖➖➖➖➖➖➖➖➖"]
    for topic, minutes in sorted(by_topic.items(), key=lambda kv: -kv[1]):
        part_count = len(parts_by_topic[topic])
        lines.append(f"📖 {html.escape(topic)}: <b>{minutes}</b> دقیقه ({part_count} پارت)")
    lines.append("➖➖➖➖➖➖➖➖➖➖")
    lines.append(f"🧮 مجموع: {sum(by_topic.values())} دقیقه")
    lines.append("\n💡 پارت‌های «بدون موضوع خاص» رو با /topic <نام درس> می‌تونی برچسب بزنی.")
    await message.answer("\n".join(lines))


async def _send_activities_list(telegram_user_id: int, target: Message, edit: bool = False):
    """Shared by /activities, the list-back button, and after
    add/edit/delete actions -- shows every active activity with its
    importance, plus an 'add new' button. This list is what
    Advisor.generate_personalized_plan reads from when it builds a
    topic-aware daily plan."""
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(telegram_user_id)
        if not user:
            await target.answer("❌ لطفاً ابتدا /start را بزنید.")
            return
        activity_repo = ActivityRepository(session)
        activities = await activity_repo.get_active_by_user(user.id)

    if activities:
        text = (
            "📋 فعالیت‌ها/درس‌های فعال تو:\n\n"
            "روی هرکدوم بزنی جزئیاتش (و گزینه‌ی حذف/تغییر اهمیت) رو می‌بینی.\n"
            "این‌ها همون چیزی هستن که بر اساسشون برنامه‌ی روزانه‌ت رو اولویت‌بندی می‌کنم."
        )
    else:
        text = (
            "📋 هنوز هیچ فعالیت/درسی ثبت نکردی.\n\n"
            "با «➕ افزودن فعالیت جدید» شروع کن -- اسمش رو بگو، بعد میزان اهمیتش (۱ تا ۵) رو مشخص کن. "
            "اگه ددلاین هم داشته باشه، توی اولویت‌بندی روزانه در نظرش می‌گیرم."
        )

    if edit and isinstance(target, Message):
        try:
            await target.edit_text(text, reply_markup=activities_list_keyboard(activities))
            return
        except Exception:
            pass
    await target.answer(text, reply_markup=activities_list_keyboard(activities))


@router.callback_query(F.data == "act:list")
async def cb_act_list(callback: CallbackQuery):
    await _send_activities_list(callback.from_user.id, callback.message, edit=True)
    await callback.answer()


@router.callback_query(F.data == "act:add")
async def cb_act_add(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "✏️ اسم فعالیت/درس رو بنویس (مثلاً «ریاضی»، «پایان‌نامه»، «آیلتس»):",
        reply_markup=cancel_keyboard("activities"),
    )
    await state.set_state(UserStates.ADD_ACTIVITY_NAME)
    await callback.answer()


@router.message(UserStates.ADD_ACTIVITY_NAME)
async def add_activity_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("❌ لطفاً یک اسم معتبر بنویس.")
        return
    await state.update_data(activity_name=name[:100])
    await state.set_state(None)
    await message.answer(
        f"چقدر «{name}» برات مهمه؟",
        reply_markup=importance_picker_keyboard("actadd:imp"),
    )


@router.callback_query(F.data.startswith("actadd:imp:"))
async def cb_actadd_importance(callback: CallbackQuery, state: FSMContext):
    try:
        importance = int(callback.data.split(":")[-1])
    except ValueError:
        await callback.answer("❌ خطا", show_alert=True)
        return
    await state.update_data(activity_importance=importance)
    await callback.message.answer(
        "چند روز دیگه ددلاینشه؟ یه عدد بفرست (مثلاً 3)، یا 0 اگه ددلاین مشخصی نداره:",
        reply_markup=cancel_keyboard("activities"),
    )
    await state.set_state(UserStates.ADD_ACTIVITY_DEADLINE)
    await callback.answer()


@router.message(UserStates.ADD_ACTIVITY_DEADLINE)
async def add_activity_deadline(message: Message, state: FSMContext):
    try:
        days = int((message.text or "").strip())
        if days < 0 or days > 365:
            await message.answer("❌ یک عدد بین ۰ تا ۳۶۵ بفرست.")
            return
    except ValueError:
        await message.answer("❌ لطفاً یک عدد بفرست (۰ یعنی ددلاین مشخصی نداره).")
        return

    data = await state.get_data()
    name = data.get("activity_name")
    importance = data.get("activity_importance", 3)
    if not name:
        await message.answer("❌ چیزی گم شد، لطفاً دوباره با /activities امتحان کن.")
        await state.clear()
        return

    deadline = (time_service.now() + timedelta(days=days)) if days > 0 else None

    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer("❌ لطفاً ابتدا /start را بزنید.")
            await state.clear()
            return
        activity_repo = ActivityRepository(session)
        await activity_repo.create(user.id, name, importance, deadline)
        await session.commit()

    await state.clear()
    deadline_note = f"، ددلاین {days} روز دیگه" if days > 0 else ""
    await message.answer(
        f"✅ «{name}» با اهمیت {config.IMPORTANCE_LABELS.get(importance, importance)}{deadline_note} اضافه شد.\n"
        "از این به بعد توی برنامه‌ی روزانه‌ت در نظرش می‌گیرم. 🎯"
    )
    await _send_activities_list(message.from_user.id, message)


@router.callback_query(F.data.startswith("act:view:"))
async def cb_act_view(callback: CallbackQuery):
    activity_id = int(callback.data.split(":")[-1])
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ کاربر پیدا نشد.", show_alert=True)
            return
        activity_repo = ActivityRepository(session)
        activity = await activity_repo.get_by_id(activity_id, user.id)
        if not activity or not activity.active:
            await callback.answer("❌ این فعالیت پیدا نشد.", show_alert=True)
            return
        weight = advisor._activity_weight(activity)
        deadline_text = (
            f"📅 ددلاین: {activity.deadline.strftime('%Y-%m-%d')}" if activity.deadline
            else "📅 ددلاین: ندارد"
        )
        text = (
            f"📌 {activity.name}\n\n"
            f"{config.IMPORTANCE_LABELS.get(activity.importance, '')}\n"
            f"{deadline_text}\n"
            f"🧮 اولویت محاسبه‌شده: {weight:.1f}"
        )
    await callback.message.edit_text(text, reply_markup=activity_detail_keyboard(activity_id))
    await callback.answer()


@router.callback_query(F.data.startswith("act:reimp:"))
async def cb_act_reimp(callback: CallbackQuery):
    activity_id = callback.data.split(":")[-1]
    await callback.message.edit_text(
        "میزان اهمیت جدید رو انتخاب کن:",
        reply_markup=importance_picker_keyboard(f"actedit:imp:{activity_id}"),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("actedit:imp:"))
async def cb_actedit_importance(callback: CallbackQuery):
    parts = callback.data.split(":")
    activity_id, importance = int(parts[2]), int(parts[3])
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ کاربر پیدا نشد.", show_alert=True)
            return
        activity_repo = ActivityRepository(session)
        activity = await activity_repo.set_importance(activity_id, user.id, importance)
        if not activity:
            await callback.answer("❌ این فعالیت پیدا نشد.", show_alert=True)
            return
        await session.commit()
    await callback.answer("✅ اهمیت به‌روزرسانی شد")
    await _send_activities_list(callback.from_user.id, callback.message, edit=True)


@router.callback_query(F.data.startswith("act:del:"))
async def cb_act_delete(callback: CallbackQuery):
    activity_id = int(callback.data.split(":")[-1])
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ کاربر پیدا نشد.", show_alert=True)
            return
        activity_repo = ActivityRepository(session)
        ok = await activity_repo.deactivate(activity_id, user.id)
        await session.commit()
    if not ok:
        await callback.answer("❌ این فعالیت پیدا نشد.", show_alert=True)
        return
    await callback.answer("🗑 حذف شد")
    await _send_activities_list(callback.from_user.id, callback.message, edit=True)


# ============================================================
# ROUTINES (بخش روتین) -- handlers
# ------------------------------------------------------------
# Flow: name -> kind (یادآوری/پارت) -> [duration, if پارت] ->
# repeat (یک‌بار/هر روز/روزهای خاص) -> [days, if روزهای خاص] -> time.
# Everything collected along the way lives in FSMContext data under
# routine_* keys until the final HH:MM message, which is what actually
# writes the row and calls schedule_routine().
# ============================================================
def _routine_repeat_label(routine: "UserRoutine") -> str:
    if routine.repeat_type == "once":
        when = routine.once_date.strftime("%Y-%m-%d") if routine.once_date else "؟"
        return f"یک‌بار، {when}"
    if routine.repeat_type == "daily":
        return "هر روز"
    days = routine.days_of_week or []
    names = [PERSIAN_WEEKDAYS[d] for d in sorted(days) if 0 <= d < 7]
    return "، ".join(names) if names else "هیچ روزی انتخاب نشده"


async def _send_routines_list(telegram_user_id: int, target: Message, edit: bool = False):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(telegram_user_id)
        if not user:
            await target.answer("❌ لطفاً ابتدا /start را بزنید.")
            return
        routine_repo = RoutineRepository(session)
        routines = await routine_repo.get_active_by_user(user.id)

    if routines:
        text = (
            "🔁 روتین‌های فعال تو:\n\n"
            "روی هرکدوم بزنی جزئیات و گزینه‌ی حذفش رو می‌بینی. 🔥 یعنی رشته‌ی پیوستگی (streak)."
        )
    else:
        text = (
            "🔁 هنوز هیچ روتینی نساختی.\n\n"
            "با «➕ افزودن روتین جدید» شروع کن: اول اسم کاری که می‌خوای تکرار کنی رو بگو، "
            "بعد انتخاب کن که فقط یادآوری باشه یا یه پارت مطالعه‌ی واقعی، "
            "و در آخر مشخص کن چقدر تکرار بشه (یک‌بار، هر روز، یا روزهای خاصی از هفته)."
        )

    if edit and isinstance(target, Message):
        try:
            await target.edit_text(text, reply_markup=routines_list_keyboard(routines))
            return
        except Exception:
            pass
    await target.answer(text, reply_markup=routines_list_keyboard(routines))


@router.callback_query(F.data == "menu:routine")
async def cb_menu_routine(callback: CallbackQuery):
    await _send_routines_list(callback.from_user.id, callback.message, edit=True)
    await callback.answer()


@router.callback_query(F.data == "routine:list")
async def cb_routine_list(callback: CallbackQuery):
    await _send_routines_list(callback.from_user.id, callback.message, edit=True)
    await callback.answer()


@router.callback_query(F.data == "routine:add")
async def cb_routine_add(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer(
        "✏️ چه کاری رو می‌خوای به روتین اضافه کنی؟ (مثلاً «ورزش صبحگاهی»، «مرور لغات انگلیسی»):",
        reply_markup=cancel_keyboard("routine"),
    )
    await state.set_state(UserStates.ROUTINE_ADD_NAME)
    await callback.answer()


@router.message(UserStates.ROUTINE_ADD_NAME)
async def routine_add_name(message: Message, state: FSMContext):
    title = (message.text or "").strip()
    if not title:
        await message.answer("❌ لطفاً یک اسم معتبر بنویس.")
        return
    await state.update_data(routine_title=title[:100])
    await state.set_state(None)
    await message.answer(
        f"«{title}» رو چطور می‌خوای دنبالش کنم؟",
        reply_markup=routine_kind_keyboard(),
    )


@router.callback_query(F.data.startswith("routine:kind:"))
async def cb_routine_kind(callback: CallbackQuery, state: FSMContext):
    kind = callback.data.split(":")[-1]
    await state.update_data(routine_kind=kind)
    if kind == "part":
        await callback.message.edit_text(
            "⏱ این پارت چقدر طول بکشه؟",
            reply_markup=routine_duration_keyboard(),
        )
    else:
        await callback.message.edit_text(
            "این کار چقدر تکرار بشه؟",
            reply_markup=routine_repeat_keyboard(),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("routine:dur:"))
async def cb_routine_duration(callback: CallbackQuery, state: FSMContext):
    value = callback.data.split(":")[-1]
    if value == "custom":
        await callback.message.edit_text(
            "چند دقیقه؟ یه عدد بفرست (مثلاً 20):",
        )
        await callback.message.answer("👇", reply_markup=cancel_keyboard("routine"))
        await state.set_state(UserStates.ROUTINE_ADD_DURATION)
        await callback.answer()
        return
    await state.update_data(routine_duration=int(value))
    await callback.message.edit_text(
        "این پارت چقدر تکرار بشه؟",
        reply_markup=routine_repeat_keyboard(),
    )
    await callback.answer()


@router.message(UserStates.ROUTINE_ADD_DURATION)
async def routine_add_duration(message: Message, state: FSMContext):
    try:
        minutes = int((message.text or "").strip())
        if minutes < 1 or minutes > 300:
            raise ValueError
    except ValueError:
        await message.answer("❌ یک عدد بین ۱ تا ۳۰۰ (دقیقه) بفرست.")
        return
    await state.update_data(routine_duration=minutes)
    await state.set_state(None)
    await message.answer(
        "این پارت چقدر تکرار بشه؟",
        reply_markup=routine_repeat_keyboard(),
    )


@router.callback_query(F.data.startswith("routine:repeat:"))
async def cb_routine_repeat(callback: CallbackQuery, state: FSMContext):
    repeat_type = callback.data.split(":")[-1]
    await state.update_data(routine_repeat=repeat_type)
    if repeat_type == "once":
        await callback.message.edit_text(
            "چند روز دیگه انجامش بدی؟ یه عدد بفرست (۰ یعنی امروز):",
        )
        await callback.message.answer("👇", reply_markup=cancel_keyboard("routine"))
        await state.set_state(UserStates.ROUTINE_ADD_ONCE_DAYS)
    elif repeat_type == "daily":
        await callback.message.edit_text(
            "ساعتش چنده؟ به فرمت HH:MM بفرست (مثلاً 07:30):",
        )
        await callback.message.answer("👇", reply_markup=cancel_keyboard("routine"))
        await state.set_state(UserStates.ROUTINE_ADD_TIME)
    elif repeat_type == "weekly":
        await state.update_data(routine_days=[])
        await callback.message.edit_text(
            "چه روزهایی از هفته؟ هرچند روزی که می‌خوای رو بزن، بعد «تایید و ادامه»:",
            reply_markup=routine_days_keyboard([]),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("routine:day:"))
async def cb_routine_toggle_day(callback: CallbackQuery, state: FSMContext):
    day = int(callback.data.split(":")[-1])
    data = await state.get_data()
    days: List[int] = list(data.get("routine_days", []))
    if day in days:
        days.remove(day)
    else:
        days.append(day)
    await state.update_data(routine_days=days)
    try:
        await callback.message.edit_reply_markup(reply_markup=routine_days_keyboard(days))
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    await callback.answer()


@router.callback_query(F.data == "routine:days_done")
async def cb_routine_days_done(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    days: List[int] = list(data.get("routine_days", []))
    if not days:
        await callback.answer("❌ حداقل یک روز را انتخاب کن.", show_alert=True)
        return
    await callback.message.edit_text(
        "ساعتش چنده؟ به فرمت HH:MM بفرست (مثلاً 21:00):",
    )
    await callback.message.answer("👇", reply_markup=cancel_keyboard("routine"))
    await state.set_state(UserStates.ROUTINE_ADD_TIME)
    await callback.answer()


@router.message(UserStates.ROUTINE_ADD_ONCE_DAYS)
async def routine_add_once_days(message: Message, state: FSMContext):
    try:
        days_ahead = int((message.text or "").strip())
        if days_ahead < 0 or days_ahead > 365:
            raise ValueError
    except ValueError:
        await message.answer("❌ یک عدد بین ۰ تا ۳۶۵ بفرست.")
        return
    once_date = time_service.now() + timedelta(days=days_ahead)
    await state.update_data(routine_once_date=once_date.isoformat())
    await state.set_state(UserStates.ROUTINE_ADD_TIME)
    await message.answer(
        "ساعتش چنده؟ به فرمت HH:MM بفرست (مثلاً 09:00):",
        reply_markup=cancel_keyboard("routine"),
    )


_TIME_RE = re.compile(r"^([01]?[0-9]|2[0-3]):([0-5][0-9])$")


@router.message(UserStates.ROUTINE_ADD_TIME)
async def routine_add_time(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    match = _TIME_RE.match(text)
    if not match:
        await message.answer("❌ فرمت درست نیست. یک ساعت مثل 07:30 یا 21:00 بفرست.")
        return
    hour, minute = int(match.group(1)), int(match.group(2))

    data = await state.get_data()
    title = data.get("routine_title")
    kind = data.get("routine_kind", "reminder")
    repeat_type = data.get("routine_repeat", "daily")
    duration = data.get("routine_duration")
    days = data.get("routine_days") or []
    once_date_raw = data.get("routine_once_date")
    once_date = datetime.fromisoformat(once_date_raw) if once_date_raw else None

    if not title:
        await message.answer("❌ چیزی گم شد، لطفاً دوباره با «🔁 روتین‌ها» امتحان کن.")
        await state.clear()
        return

    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer("❌ لطفاً ابتدا /start را بزنید.")
            await state.clear()
            return
        routine_repo = RoutineRepository(session)
        routine = await routine_repo.create(
            user_id=user.id, title=title, kind=kind, repeat_type=repeat_type,
            hour=hour, minute=minute, days_of_week=days, once_date=once_date,
            duration_minutes=duration,
        )
        await session.commit()
        await schedule_routine(routine)
        repeat_label = _routine_repeat_label(routine)

    await state.clear()
    kind_label = "یادآوری" if kind == "reminder" else f"پارت مطالعه ({duration} دقیقه)"
    await message.answer(
        f"✅ روتین «{title}» ساخته شد!\n\n"
        f"نوع: {kind_label}\n"
        f"تکرار: {repeat_label}\n"
        f"ساعت: {hour:02d}:{minute:02d}"
    )
    await _send_routines_list(message.from_user.id, message)


@router.callback_query(F.data.startswith("routine:view:"))
async def cb_routine_view(callback: CallbackQuery):
    routine_id = int(callback.data.split(":")[-1])
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ کاربر پیدا نشد.", show_alert=True)
            return
        routine_repo = RoutineRepository(session)
        routine = await routine_repo.get_by_id(routine_id, user.id)
        if not routine or not routine.active:
            await callback.answer("❌ این روتین پیدا نشد.", show_alert=True)
            return
        kind_label = "🔔 فقط یادآوری" if routine.kind == "reminder" else f"📖 پارت مطالعه ({routine.duration_minutes} دقیقه)"
        text = (
            f"🔁 {routine.title}\n\n"
            f"نوع: {kind_label}\n"
            f"تکرار: {_routine_repeat_label(routine)}\n"
            f"ساعت: {routine.hour:02d}:{routine.minute:02d}\n"
            f"🔥 رشته‌ی پیوستگی فعلی: {routine.current_streak} (رکورد: {routine.longest_streak})"
        )
    await callback.message.edit_text(text, reply_markup=routine_detail_keyboard(routine_id))
    await callback.answer()


@router.callback_query(F.data.startswith("routine:del:"))
async def cb_routine_delete(callback: CallbackQuery):
    routine_id = int(callback.data.split(":")[-1])
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ کاربر پیدا نشد.", show_alert=True)
            return
        routine_repo = RoutineRepository(session)
        ok = await routine_repo.deactivate(routine_id, user.id)
        await session.commit()
    if not ok:
        await callback.answer("❌ این روتین پیدا نشد.", show_alert=True)
        return
    unschedule_routine(routine_id)
    await callback.answer("🗑 حذف شد")
    await _send_routines_list(callback.from_user.id, callback.message, edit=True)


@router.callback_query(F.data.startswith("routdone:"))
async def cb_routine_done(callback: CallbackQuery):
    routine_id = int(callback.data.split(":")[-1])
    async with get_session() as session:
        routine_repo = RoutineRepository(session)
        streak = await routine_repo.mark_done(routine_id)
        await session.commit()
    if streak is None:
        await callback.answer("❌ این روتین دیگه فعال نیست.", show_alert=True)
        return
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.answer(f"🎉 آفرین! رشته‌ی پیوستگی این روتین الان {streak} روزه. 🔥")
    await callback.answer()


@router.callback_query(F.data.startswith("routmiss:"))
async def cb_routine_miss(callback: CallbackQuery):
    routine_id = int(callback.data.split(":")[-1])
    async with get_session() as session:
        routine_repo = RoutineRepository(session)
        await routine_repo.mark_missed(routine_id)
        await session.commit()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.answer("مشکلی نیست، دفعه‌ی بعد. 🔁 روتین همچنان فعاله، فقط رشته‌ی پیوستگی صفر شد.")
    await callback.answer()


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
        "/advice - تحلیل رفتار و توصیه شخصی‌سازی‌شده\n"
        "/plan - برنامه شخصی امروز (چند پارت، چه ساعتی، بر پایه آخرین یافته‌های علمی)\n"
        "/activities - فعالیت‌ها/درس‌هات و میزان اهمیتشون رو مدیریت کن\n"
        "/session - ایجاد پارت جدید\n"
        "/report - گزارش دقیق درسی (شخصی در چت خصوصی، گروهی داخل گروه)\n"
        "/report [روز هفته] - مثلاً «/report دوشنبه»: لیست دقیق پارت‌های همون روز با ساعت شروع/پایان\n"
        "/topic - (داخل گروه) تگ‌کردن موضوع پارت در جریان\n"
        "/autopoll - (داخل گروه) روشن/خاموش کردن نظرسنجی خودکار پارت\n"
        "/status [آیدی/یوزرنیم] - (فقط ادمین) وضعیت مطالعاتی دقیق هر فرد + توصیه‌های علمی مخصوص اون فرد\n"
        "/help - این راهنما\n\n"
        "🌱 اصول من:\n"
        "• آرامش و استمرار\n"
        "• پیشرفت تدریجی\n"
        "• همراهی بدون قضاوت\n"
        "• تنظیم هوشمند بر اساس رفتار تو\n"
        "• توصیه‌ها بر پایه‌ی روش‌های اثبات‌شده‌ی علم مطالعه (یادآوری فعال، اثر فاصله‌گذاری و...)\n\n"
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
    except (IndexError, ValueError):
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
            # Show the *full* roster of everyone who has declared
            # readiness so far -- not just a headcount -- every time this
            # "کیا هستن؟" poll updates, so the group can see exactly who's
            # in without having to ask.
            ready_users = await _get_ready_users(session, session_id)
            count = len(ready_users)
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
            # subsequent tap just bumps the roster, so keep the time
            # line visible but don't reintroduce it as if it were news.
            time_line = f"⏰ شروع: ساعت {start_clock} (حدود {minutes_left} دقیقه دیگر)\n"
            roster = _format_roster(ready_users)
            await callback.message.edit_text(
                f"📚 <b>پارت #{part_number}</b>\n"
                f"{time_line}"
                f"➖➖➖➖➖➖➖➖➖➖\n"
                f"✅ <b>{_display_name(user)}</b> اعلام آمادگی کرد!\n\n"
                f"👥 <b>آمادگان ({count} نفر):</b>\n"
                f"{roster}",
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


@router.callback_query(F.data.startswith("stopic:"))
async def cb_session_topic_pick(callback: CallbackQuery):
    """Handles the topic_picker_keyboard tap right after /session -- tags
    Session.topic with the chosen activity's name (or leaves it unset on
    skip). Purely a label for /report; doesn't affect the part itself."""
    try:
        _, session_id_raw, choice = callback.data.split(":", 2)
        session_id = int(session_id_raw)
    except (ValueError, IndexError):
        await callback.answer("❌ خطا", show_alert=True)
        return
    async with get_session() as session:
        sess = await session.get(Session, session_id)
        if not sess:
            await callback.answer("❌ این پارت پیدا نشد", show_alert=True)
            return
        if choice == "skip":
            topic_label = None
        else:
            try:
                activity_id = int(choice)
            except ValueError:
                await callback.answer("❌ خطا", show_alert=True)
                return
            activity = await session.get(UserActivity, activity_id)
            topic_label = activity.name if activity else None
        sess.topic = topic_label
        await session.flush()
    if topic_label:
        await callback.message.edit_text(f"📖 موضوع این پارت: <b>{html.escape(topic_label)}</b>")
    else:
        await callback.message.edit_text("⏭ این پارت بدون موضوع خاص ثبت شد.")
    await callback.answer()


@router.message(Command("topic"))
async def cmd_topic(message: Message):
    """Tags (or retags) the group's currently in-flight part with a
    subject/lesson -- for parts that had no single initiator to ask at
    creation time (an automatic poll) or when the first pick was wrong.
    Usage: /topic ریاضی"""
    if message.chat.type not in ("group", "supergroup"):
        await message.answer("این دستور فقط داخل خود گروه کار می‌کنه.")
        return
    label = (message.text or "").split(maxsplit=1)
    if len(label) < 2 or not label[1].strip():
        await message.answer("بعد از دستور، اسم درس/موضوع رو بنویس. مثال:\n/topic ریاضی")
        return
    topic_label = label[1].strip()[:64]
    async with get_session() as session:
        group_repo = GroupRepository(session)
        group = await group_repo.get_by_chat_id(message.chat.id)
        if not group:
            await message.answer("❌ این گروه هنوز ثبت نشده.")
            return
        result = await session.execute(
            select(Session).where(
                Session.group_id == group.id,
                Session.status.in_(["ATTENDANCE", "READY", "STARTED"]),
            )
        )
        active_sess = result.scalar_one_or_none()
        if not active_sess:
            await message.answer("⛔ الان پارتی در جریان نیست که موضوعش رو تگ کنم.")
            return
        active_sess.topic = topic_label
        await session.flush()
    await message.answer(f"📖 موضوع پارت جاری روی «{html.escape(topic_label)}» ثبت شد.")


@router.callback_query(F.data.startswith("result:"))
async def cb_result(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer("❌ خطا")
        return
    session_id = int(parts[1])
    status = parts[2].upper()

    if status == "MANUAL":
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
        prompt = await callback.message.answer(
            "⏱ چند دقیقه مطالعه کردی؟ فقط عدد بفرست:",
            reply_markup=cancel_keyboard("main"),
        )
        # Both this prompt and the number the person replies with get
        # deleted once submitted (see report_manual_duration) -- the
        # updated 🟢 dot on the pinned roster is confirmation enough, no
        # need for either to sit permanently in the group.
        await state.update_data(session_id=session_id, prompt_message_id=prompt.message_id)
        await state.set_state(UserStates.REPORT_MANUAL_DURATION)
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
            # Categories in the admin panel are "SUCCESS"/"MISSED" (not
            # the raw "COMPLETED"/"MISSED" button values), so map before
            # looking a message up -- otherwise the query never matches
            # anything and silently falls back to GENERAL every time.
            # {minutes} is filled in with the honestly-computed duration
            # from report_result (elapsed time, not a trusted tap), so the
            # popup reflects the real number just recorded, not a guess.
            recorded = await session.execute(
                select(SessionResult.actual_duration).where(
                    SessionResult.session_id == session_id,
                    SessionResult.user_id == user.id,
                )
            )
            recorded_minutes = recorded.scalar_one_or_none() or 0
            popup_category = "SUCCESS" if status == "COMPLETED" else status
            popup_text = await message_selector.get_message(
                session, popup_category, user.id, minutes=recorded_minutes
            )
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

    if success:
        # Reflect the new 🟢/🟡/🔴 immediately in the pinned roster, so the
        # group can see at a glance who's finished without waiting for the
        # part to end.
        await _refresh_roster_message(session_id)

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


@router.message(UserStates.REPORT_MANUAL_DURATION)
async def report_manual_duration(message: Message, state: FSMContext):
    data = await state.get_data()
    session_id = data.get("session_id")
    prompt_message_id = data.get("prompt_message_id")

    try:
        typed_minutes = int(message.text)
        if typed_minutes < 1:
            raise ValueError
    except (TypeError, ValueError):
        warn = await message.answer("❌ لطفاً فقط یک عدد معتبر (دقیقه) بفرست.")
        asyncio.create_task(_delete_message_later(warn, delay_seconds=5))
        return

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
        success = await manager.report_result(session_id, user.id, "COMPLETED", manual_duration=typed_minutes)
        should_finish_early = False
        capped_at = None
        if success:
            # report_result silently capped the number if it exceeded the
            # max window or real elapsed time -- surface that so the
            # person knows why their number didn't stick as typed.
            sess_after = await manager.get_session(session_id)
            recorded = await session.execute(
                select(SessionResult.actual_duration).where(
                    SessionResult.session_id == session_id,
                    SessionResult.user_id == user.id,
                )
            )
            recorded_minutes = recorded.scalar_one_or_none()
            if recorded_minutes is not None and recorded_minutes < typed_minutes:
                capped_at = recorded_minutes

            # Group stays clean either way: delete both the prompt and the
            # person's typed reply, and let the updated dot on the pinned
            # roster be the confirmation.
            try:
                await message.delete()
            except Exception as e:
                logger.debug(f"Could not delete manual-duration reply {message.message_id}: {e}")
            if prompt_message_id:
                try:
                    await message.bot.delete_message(message.chat.id, prompt_message_id)
                except Exception as e:
                    logger.debug(f"Could not delete manual-duration prompt {prompt_message_id}: {e}")

            if capped_at is not None:
                warn = await message.answer(
                    f"ℹ️ چون بیشتر از سقف مجاز پارت بود، {capped_at} دقیقه برات ثبت شد."
                )
                asyncio.create_task(_delete_message_later(warn, delay_seconds=6))
            else:
                # Same motivational-message pool as the button taps (see
                # cb_result) -- manual entry deserves the same encouragement,
                # it's just a short-lived message here instead of a popup
                # since there's no callback to answer.
                popup_text = await message_selector.get_message(
                    session, "SUCCESS", user.id, minutes=recorded_minutes or typed_minutes
                )
                if popup_text:
                    note = await message.answer(popup_text)
                    asyncio.create_task(_delete_message_later(note, delay_seconds=6))

            await _refresh_roster_message(session_id)
            participants = await manager.get_participants(session_id)
            ready_ids = {p.user_id for p in participants if p.attendance_status == "READY"}
            if sess_after and sess_after.status == "STARTED" and ready_ids:
                reported_result = await session.execute(
                    select(SessionResult.user_id).where(SessionResult.session_id == session_id)
                )
                reported_ids = {row[0] for row in reported_result.all()}
                should_finish_early = ready_ids.issubset(reported_ids)
        else:
            warn = await message.answer("❌ قبلاً برای این پارت ثبت کرده‌ای.")
            asyncio.create_task(_delete_message_later(warn, delay_seconds=5))

    await state.clear()
    if should_finish_early:
        await _finish_session_early(session_id)


@router.callback_query(F.data == "menu:today")
async def cb_menu_today(callback: CallbackQuery):
    await _send_today_status(callback.from_user.id, callback.message)
    await callback.answer()


@router.callback_query(F.data == "menu:advice")
async def cb_menu_advice(callback: CallbackQuery):
    await _send_advice(callback.from_user.id, callback.message)
    await callback.answer()


@router.callback_query(F.data == "menu:plan")
async def cb_menu_plan(callback: CallbackQuery):
    await _send_personalized_plan(callback.from_user.id, callback.message)
    await callback.answer()


@router.callback_query(F.data.startswith("planpart:toggle:"))
async def cb_toggle_plan_part(callback: CallbackQuery):
    """Flips a single part's done/not-done checkbox and refreshes just the
    keyboard in place (no need to resend the whole plan text)."""
    try:
        part_id = int(callback.data.split(":")[-1])
    except ValueError:
        await callback.answer()
        return
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ لطفاً ابتدا /start را بزنید.", show_alert=True)
            return
        plan_repo = PlanRepository(session)
        part = await plan_repo.toggle_part(part_id, user.id)
        if not part:
            await callback.answer("❌ این پارت پیدا نشد (شاید برنامه‌ی جدیدی ساخته شده).", show_alert=True)
            return
        is_done = part.is_done
        await session.commit()
        _, plan_parts = await plan_repo.get_today_plan_with_parts(user.id)
    try:
        await callback.message.edit_reply_markup(reply_markup=plan_keyboard(plan_parts))
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e):
            raise
    await callback.answer("✅ به‌عنوان انجام‌شده ثبت شد!" if is_done else "⬜ به‌عنوان انجام‌نشده علامت خورد.")


@router.callback_query(F.data == "plan:new")
async def cb_plan_new(callback: CallbackQuery):
    """'🆕 برنامه جدید' -- throws away today's plan and builds a fresh one
    starting `plan_new_start_offset_minutes` minutes from right now
    (user-tunable, see settings:plan_start_offset)."""
    offset = 10
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if user:
            result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
            prefs = result.scalar_one_or_none()
            offset = prefs.plan_new_start_offset_minutes if prefs else 10
    await _send_personalized_plan(callback.from_user.id, callback.message, start_in_minutes=offset)
    await callback.answer(f"🆕 برنامه جدید از {offset} دقیقه دیگه شروع می‌شه.")


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
        try:
            await callback.message.edit_text(msg, reply_markup=main_menu())
        except TelegramBadRequest as e:
            # Telegram rejects edit_text when the new content is byte-for-byte
            # identical to what's already on the message (e.g. tapping the
            # reports button twice with no new data in between). That's not
            # an actual failure, so swallow only that specific case.
            if "message is not modified" not in str(e):
                raise
    await callback.answer()


@router.callback_query(F.data == "menu:goal")
async def cb_menu_goal(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🎯 لطفاً هدف روزانه خود را به دقیقه وارد کنید (مثلاً 90):",
        reply_markup=cancel_keyboard("main"),
    )
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


@router.callback_query(F.data == "menu:back")
async def cb_menu_back(callback: CallbackQuery):
    is_admin = callback.from_user.id in config.ADMIN_IDS
    await callback.message.edit_text("📋 منوی اصلی:", reply_markup=main_menu(is_admin))
    await callback.answer()


@router.callback_query(F.data == "settings:back")
async def cb_settings_back(callback: CallbackQuery):
    is_admin = callback.from_user.id in config.ADMIN_IDS
    await callback.message.edit_text("📋 منوی اصلی:", reply_markup=main_menu(is_admin))
    await callback.answer()


@router.callback_query(F.data == "settings:goal")
async def cb_settings_goal(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer(
        "🎯 هدف روزانه جدید را به دقیقه وارد کنید:",
        reply_markup=cancel_keyboard("settings"),
    )
    await state.set_state(UserStates.SET_DAILY_GOAL)
    await callback.answer()


@router.callback_query(F.data == "settings:duration")
async def cb_settings_duration(callback: CallbackQuery, state: FSMContext):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ لطفاً ابتدا /start را بزنید.", show_alert=True)
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        current = prefs.preferred_session_duration if prefs else 45
    await callback.message.answer(
        f"⏱ مدت ترجیحی فعلی هر پارت مطالعه: {current} دقیقه.\n\n"
        "مدت ترجیحی جدید را به دقیقه وارد کن (بین ۱۰ تا ۱۲۰). "
        "این مقدار توی پیشنهاد زمان جلسه‌های بعدی هم در نظر گرفته میشه:",
        reply_markup=cancel_keyboard("settings"),
    )
    await state.set_state(UserStates.SET_PREFERRED_DURATION)
    await callback.answer()


@router.message(UserStates.SET_PREFERRED_DURATION)
async def set_preferred_duration(message: Message, state: FSMContext):
    try:
        duration = int(message.text.strip())
        if duration < 10 or duration > 120:
            await message.answer("❌ مدت باید بین ۱۰ تا ۱۲۰ دقیقه باشد.")
            return
    except (ValueError, AttributeError):
        await message.answer("❌ لطفاً یک عدد معتبر وارد کنید.")
        return
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer("❌ لطفاً ابتدا /start را بزنید.")
            await state.clear()
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        if prefs:
            prefs.preferred_session_duration = duration
        else:
            prefs = UserPreference(user_id=user.id, preferred_session_duration=duration)
            session.add(prefs)
        await session.commit()
    await message.answer(
        f"✅ مدت ترجیحی روی {duration} دقیقه تنظیم شد. 🎯",
        reply_markup=settings_keyboard(),
    )
    await state.clear()


@router.callback_query(F.data == "settings:gap")
async def cb_settings_gap(callback: CallbackQuery, state: FSMContext):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ لطفاً ابتدا /start را بزنید.", show_alert=True)
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        current = prefs.plan_gap_minutes if prefs else 60
    await callback.message.answer(
        f"⏳ فاصله فعلی بین پارت‌های برنامه‌ی روزانه (/plan): {current} دقیقه.\n\n"
        "این فاصله همراه با مدت خود پارت تعیین می‌کنه که پارت بعدی چه ساعتی پیشنهاد بشه "
        "(اثر فاصله‌گذاری/spacing effect). فاصله‌ی جدید رو به دقیقه وارد کن (بین ۱۵ تا ۱۸۰):",
        reply_markup=cancel_keyboard("settings"),
    )
    await state.set_state(UserStates.SET_PLAN_GAP)
    await callback.answer()


@router.message(UserStates.SET_PLAN_GAP)
async def set_plan_gap(message: Message, state: FSMContext):
    try:
        gap = int(message.text.strip())
        if gap < 15 or gap > 180:
            await message.answer("❌ فاصله باید بین ۱۵ تا ۱۸۰ دقیقه باشد.")
            return
    except (ValueError, AttributeError):
        await message.answer("❌ لطفاً یک عدد معتبر وارد کنید.")
        return
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer("❌ لطفاً ابتدا /start را بزنید.")
            await state.clear()
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        if prefs:
            prefs.plan_gap_minutes = gap
        else:
            prefs = UserPreference(user_id=user.id, plan_gap_minutes=gap)
            session.add(prefs)
        await session.commit()
    await message.answer(
        f"✅ فاصله بین پارت‌ها روی {gap} دقیقه تنظیم شد. برنامه‌ی بعدی‌ات بر همین اساس چیده می‌شه. 🎯",
        reply_markup=settings_keyboard(),
    )
    await state.clear()


@router.callback_query(F.data == "settings:plan_start_offset")
async def cb_settings_plan_start_offset(callback: CallbackQuery, state: FSMContext):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ لطفاً ابتدا /start را بزنید.", show_alert=True)
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        current = prefs.plan_new_start_offset_minutes if prefs else 10
    await callback.message.answer(
        f"⏱ الان وقتی دکمه‌ی «🆕 برنامه جدید» رو بزنی، پارت اول {current} دقیقه بعد شروع می‌شه.\n\n"
        "مقدار جدید رو به دقیقه وارد کن (بین ۱ تا ۱۸۰):",
        reply_markup=cancel_keyboard("settings"),
    )
    await state.set_state(UserStates.SET_PLAN_START_OFFSET)
    await callback.answer()


@router.message(UserStates.SET_PLAN_START_OFFSET)
async def set_plan_start_offset(message: Message, state: FSMContext):
    try:
        offset = int(message.text.strip())
        if offset < 1 or offset > 180:
            await message.answer("❌ مقدار باید بین ۱ تا ۱۸۰ دقیقه باشد.")
            return
    except (ValueError, AttributeError):
        await message.answer("❌ لطفاً یک عدد معتبر وارد کنید.")
        return
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(message.from_user.id)
        if not user:
            await message.answer("❌ لطفاً ابتدا /start را بزنید.")
            await state.clear()
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        if prefs:
            prefs.plan_new_start_offset_minutes = offset
        else:
            prefs = UserPreference(user_id=user.id, plan_new_start_offset_minutes=offset)
            session.add(prefs)
        await session.commit()
    await message.answer(
        f"✅ از این به بعد «🆕 برنامه جدید» از {offset} دقیقه دیگه شروع می‌شه. 🎯",
        reply_markup=settings_keyboard(),
    )
    await state.clear()


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


@router.callback_query(F.data == "settings:delivery")
async def cb_settings_delivery(callback: CallbackQuery):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ لطفاً ابتدا /start را بزنید.", show_alert=True)
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        if not prefs:
            prefs = UserPreference(user_id=user.id)
            session.add(prefs)
            await session.flush()
        await callback.message.edit_text(
            "🧭 محل ارسال برنامه شخصی روزانه:\n\n"
            "هر روز صبح یک برنامه‌ی شخصی‌سازی‌شده (بر پایه‌ی رفتارت و یافته‌های علمی روز) بدون این‌که "
            "خودت درخواست بدی برات آماده می‌شه. اینجا مشخص کن کجا برات بفرستم.",
            reply_markup=proactive_delivery_keyboard(prefs.proactive_delivery),
        )
    await callback.answer()


@router.callback_query(F.data == "settings:delivery:cycle")
async def cb_settings_delivery_cycle(callback: CallbackQuery):
    order = [code for code, _ in PROACTIVE_DELIVERY_OPTIONS]
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
        current_idx = order.index(prefs.proactive_delivery) if prefs.proactive_delivery in order else 0
        prefs.proactive_delivery = order[(current_idx + 1) % len(order)]
        await session.commit()
        await callback.message.edit_reply_markup(reply_markup=proactive_delivery_keyboard(prefs.proactive_delivery))
    await callback.answer("✅ تغییر کرد")


@router.callback_query(F.data == "settings:calm")
async def cb_settings_calm(callback: CallbackQuery):
    async with get_session() as session:
        repo = UserRepository(session)
        user = await repo.get_by_telegram_id(callback.from_user.id)
        if not user:
            await callback.answer("❌ لطفاً ابتدا /start را بزنید.", show_alert=True)
            return
        result = await session.execute(select(UserPreference).where(UserPreference.user_id == user.id))
        prefs = result.scalar_one_or_none()
        if not prefs:
            prefs = UserPreference(user_id=user.id)
            session.add(prefs)
            await session.flush()
        await callback.message.edit_text(
            "🌙 حالت آرامش:\n\n"
            "وقتی روشنه، پارت‌های مطالعه‌ت کوتاه‌تر و کم‌فشارتر پیشنهاد داده میشه -- "
            "مناسب روزهای پرمشغله یا وقتی حسابی خسته‌ای. تا وقتی خودت خاموشش نکنی، فعال می‌مونه.",
            reply_markup=calm_mode_keyboard(prefs.calm_mode_enabled),
        )
    await callback.answer()


@router.callback_query(F.data == "settings:calm:toggle")
async def cb_settings_calm_toggle(callback: CallbackQuery):
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
        prefs.calm_mode_enabled = not prefs.calm_mode_enabled
        new_value = prefs.calm_mode_enabled
        await session.commit()
        await callback.message.edit_reply_markup(reply_markup=calm_mode_keyboard(new_value))
    await callback.answer("🟢 حالت آرامش روشن شد" if new_value else "⚪️ حالت آرامش خاموش شد")


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
    await callback.message.edit_text(
        f"📝 متن پیام رو برای دسته‌بندی «{label}» بفرست:",
        reply_markup=cancel_keyboard("messages"),
    )
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
    await callback.message.edit_text(
        "✏️ متن جدید پیام رو بفرست:",
        reply_markup=cancel_keyboard("messages"),
    )
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
    text = (
        "🧠 وزن‌های الگوریتم پیشنهاد مدت پارت:\n\n"
        "روی هرکدوم بزن تا مقدارش رو عوض کنی. این وزن‌ها همون لحظه در پیشنهادهای بعدی اثر می‌ذارن.\n\n"
        "➕ وزن‌هایی که با علامت جمع میان (GROUP_FIT, PERSONAL_FIT, TIME_FIT, METHOD_FIT, "
        "COMPLETION_PROB, PROGRESS, INTENT) هرچی بیشتر باشن، تاثیرشون روی بالابردن امتیاز بیشتره.\n"
        "➖ وزن‌هایی که با علامت منفی میان (OVERLOAD_PENALTY, FAILURE_PENALTY) هرچی بیشتر باشن، "
        "پیشنهاد از مدت‌های پرریسک‌تر بیشتر فاصله می‌گیره."
    )
    await callback.message.edit_text(text, reply_markup=algorithm_weights_keyboard())
    await callback.answer()


@router.callback_query(F.data.startswith("algw:"))
async def cb_algorithm_weight_pick(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    key = callback.data.split(":", 1)[1]
    if key not in config.ALGORITHM_WEIGHTS:
        await callback.answer("❌ وزن نامعتبر", show_alert=True)
        return
    current = config.ALGORITHM_WEIGHTS[key]
    await state.update_data(algw_key=key)
    await callback.message.answer(
        f"🧠 مقدار فعلی {key}: {current}\n\nمقدار جدید رو وارد کن (عددی بین ۰ تا ۲، مثلاً 0.15):",
        reply_markup=cancel_keyboard("algorithm"),
    )
    await state.set_state(AdminStates.SET_ALGORITHM)
    await callback.answer()


@router.message(AdminStates.SET_ALGORITHM)
async def admin_set_algorithm_weight(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    data = await state.get_data()
    key = data.get("algw_key")
    if not key:
        await message.answer("❌ خطا: مشخص نیست کدوم وزن. دوباره از منوی الگوریتم شروع کن.")
        await state.clear()
        return
    ok, feedback = await set_algorithm_weight(key, (message.text or "").strip())
    await message.answer(feedback, reply_markup=admin_panel() if ok else None)
    if ok:
        await state.clear()


@router.callback_query(F.data == "admin:tunables")
async def cb_admin_tunables(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    await callback.message.edit_text(
        "⚙️ تنظیمات عددی سراسری ربات:\n\nروی هرکدوم بزن تا مقدارش رو عوض کنی. "
        "تغییر بلافاصله اعمال می‌شه و بعد از ری‌استارت هم می‌مونه.",
        reply_markup=tunables_keyboard(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tunable:"))
async def cb_tunable_pick(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    key = callback.data.split(":", 1)[1]
    meta = TUNABLE_SETTINGS.get(key)
    if not meta:
        await callback.answer("❌ تنظیم نامعتبر", show_alert=True)
        return
    current = getattr(config, meta["attr"])
    await state.update_data(tunable_key=key)
    await callback.message.answer(
        f"⚙️ {meta['label']}\nمقدار فعلی: {current} {meta['unit']}\n\n"
        f"مقدار جدید رو وارد کن (بین {meta['min']} تا {meta['max']}):",
        reply_markup=cancel_keyboard("tunables"),
    )
    await state.set_state(AdminStates.SET_TUNABLE)
    await callback.answer()


@router.message(AdminStates.SET_TUNABLE)
async def admin_set_tunable(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    data = await state.get_data()
    key = data.get("tunable_key")
    if not key or key not in TUNABLE_SETTINGS:
        await message.answer("❌ خطا: مشخص نیست کدوم تنظیم. دوباره از منوی تنظیمات عددی شروع کن.")
        await state.clear()
        return
    ok, feedback = await set_tunable(key, (message.text or "").strip())
    await message.answer(feedback, reply_markup=admin_panel() if ok else None)
    if ok:
        await state.clear()


@router.callback_query(F.data == "admin:groups")
async def cb_admin_groups(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    async with get_session() as session:
        group_repo = GroupRepository(session)
        groups = await group_repo.get_all_active()
    if not groups:
        await callback.message.edit_text("👥 هنوز هیچ گروه فعالی ثبت نشده.", reply_markup=admin_panel())
        await callback.answer()
        return
    await callback.message.edit_text(
        "👥 یکی از گروه‌ها رو برای دیدن/تغییر تنظیماتش انتخاب کن:",
        reply_markup=admin_groups_keyboard(groups),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("admin:group:"))
async def cb_admin_group_detail(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    try:
        group_id = int(callback.data.split(":")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ خطا", show_alert=True)
        return
    async with get_session() as session:
        group = await session.get(Group, group_id)
        result = await session.execute(select(GroupSetting).where(GroupSetting.group_id == group_id))
        settings = result.scalar_one_or_none()
        if not group or not settings:
            await callback.answer("❌ این گروه یافت نشد", show_alert=True)
            return
        title = group.title or f"گروه #{group.id}"
        await callback.message.edit_text(
            f"👥 تنظیمات «{title}»:\n\nروی هرکدوم بزن تا مقدارش رو عوض کنی.",
            reply_markup=group_settings_keyboard(group_id, settings),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("gset:"))
async def cb_group_setting_pick(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    try:
        _, group_id_raw, field = callback.data.split(":", 2)
        group_id = int(group_id_raw)
    except (ValueError, IndexError):
        await callback.answer("❌ خطا", show_alert=True)
        return
    meta = GROUP_TUNABLE_FIELDS.get(field)
    if not meta:
        await callback.answer("❌ تنظیم نامعتبر", show_alert=True)
        return
    async with get_session() as session:
        result = await session.execute(select(GroupSetting).where(GroupSetting.group_id == group_id))
        settings = result.scalar_one_or_none()
        if not settings:
            await callback.answer("❌ این گروه یافت نشد", show_alert=True)
            return
        current = getattr(settings, field)
    await state.update_data(gset_group_id=group_id, gset_field=field)
    await callback.message.answer(
        f"👥 {meta['label']}\nمقدار فعلی: {current} {meta['unit']}\n\n"
        f"مقدار جدید رو وارد کن (بین {meta['min']} تا {meta['max']}):",
        reply_markup=cancel_keyboard("groups"),
    )
    await state.set_state(AdminStates.SET_GROUP_SETTINGS)
    await callback.answer()


@router.message(AdminStates.SET_GROUP_SETTINGS)
async def admin_set_group_setting(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    data = await state.get_data()
    group_id = data.get("gset_group_id")
    field = data.get("gset_field")
    if not group_id or field not in GROUP_TUNABLE_FIELDS:
        await message.answer("❌ خطا: مشخص نیست کدوم گروه/تنظیم. دوباره از منوی گروه‌ها شروع کن.")
        await state.clear()
        return
    ok, feedback = await set_group_field(group_id, field, (message.text or "").strip())
    await message.answer(feedback, reply_markup=admin_panel() if ok else None)
    if ok:
        await state.clear()


@router.callback_query(F.data.startswith("gpoll:toggle:"))
async def cb_group_autopoll_toggle(callback: CallbackQuery):
    """Toggles GroupSetting.auto_poll_enabled. Reachable from two places:
    the central admin panel's per-group settings screen (any global admin,
    from their private chat with the bot -- see group_settings_keyboard),
    and the in-group /autopoll command (see cmd_autopoll) for that group's
    own Telegram admins/owner, with no need for the panel at all."""
    try:
        group_id = int(callback.data.split(":")[2])
    except (IndexError, ValueError):
        await callback.answer("❌ خطا", show_alert=True)
        return

    is_global_admin = callback.from_user.id in config.ADMIN_IDS
    is_in_group_context = callback.message.chat.type in ("group", "supergroup")

    if not is_global_admin:
        if not is_in_group_context:
            await callback.answer("⛔ دسترسی غیرمجاز!", show_alert=True)
            return
        async with get_session() as session:
            group_repo = GroupRepository(session)
            group = await group_repo.get_by_chat_id(callback.message.chat.id)
        if not group or group.id != group_id:
            # Button doesn't belong to this chat's group -- reject rather
            # than silently toggling the wrong group.
            await callback.answer("❌ خطا", show_alert=True)
            return
        if not await _is_group_chat_admin(callback.bot, callback.message.chat.id, callback.from_user.id):
            await callback.answer("⛔ فقط ادمین‌های همین گروه می‌تونن این رو تغییر بدن.", show_alert=True)
            return

    ok, new_value, feedback = await toggle_group_auto_poll(group_id)
    if not ok:
        await callback.answer(feedback, show_alert=True)
        return

    if is_in_group_context:
        await callback.message.edit_reply_markup(reply_markup=group_autopoll_keyboard(group_id, new_value))
    else:
        async with get_session() as session:
            result = await session.execute(select(GroupSetting).where(GroupSetting.group_id == group_id))
            settings = result.scalar_one_or_none()
        if settings:
            await callback.message.edit_reply_markup(reply_markup=group_settings_keyboard(group_id, settings))
    await callback.answer("🟢 روشن شد" if new_value else "⚪️ خاموش شد")


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
        "(فقط برای کاربرانی که «پیام‌های انگیزشی» رو در تنظیماتشون روشن نگه داشتن ارسال میشه)",
        reply_markup=cancel_keyboard("notifications"),
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


@router.callback_query(F.data == "admin:logs")
async def cb_admin_logs(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    size_note = ""
    if os.path.exists(_log_file_path):
        size_kb = os.path.getsize(_log_file_path) / 1024
        size_note = f"\n\n📦 حجم فایل لاگ فعلی: {size_kb:.0f} کیلوبایت"
    else:
        size_note = "\n\n⚠️ هنوز فایل لاگی نوشته نشده."
    await callback.message.edit_text(
        f"📜 لاگ‌های ربات{size_note}\n\n"
        "می‌تونی چند خط آخر رو همین‌جا سریع ببینی، فقط خطاها رو فیلتر کنی، یا کل فایل لاگ فعلی "
        "(از وقتی که چرخش/rotation آخرین بار اتفاق افتاده) رو به‌صورت فایل دریافت کنی.",
        reply_markup=logs_menu_keyboard(),
    )
    await callback.answer()


async def _send_log_preview(callback: CallbackQuery, lines: List[str], title: str) -> None:
    """Shared renderer for the tail/errors views: joins lines into one
    HTML <pre> block, escaping each line first (log content -- tracebacks,
    stored message text, etc. -- can legitimately contain '<', '&', ...
    and must not be interpreted as HTML), then trims from the *front* if
    the result would exceed Telegram's ~4096-char message limit so the
    most recent lines (the ones you actually want in a quick glance) are
    always what's kept."""
    if not lines:
        await callback.message.edit_text(f"{title}\n\n(چیزی برای نمایش نیست.)", reply_markup=logs_menu_keyboard())
        return
    body = "\n".join(html.escape(ln) for ln in lines)
    max_body_len = 3800  # leaves headroom for title + <pre> tags under 4096
    if len(body) > max_body_len:
        body = "…\n" + body[-max_body_len:]
    text = f"{title}\n<pre>{body}</pre>"
    await callback.message.edit_text(text, reply_markup=logs_menu_keyboard())


@router.callback_query(F.data.startswith("admin:logs:tail:"))
async def cb_admin_logs_tail(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    try:
        n = int(callback.data.split(":")[3])
    except (IndexError, ValueError):
        n = 50
    lines = _tail_log_lines(n)
    await _send_log_preview(callback, lines, f"📜 {n} خط آخر لاگ:")
    await callback.answer()


@router.callback_query(F.data == "admin:logs:errors")
async def cb_admin_logs_errors(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    lines = _tail_log_lines(2000, level_filter="ERROR")
    await _send_log_preview(callback, lines[-50:], "⚠️ ۵۰ خطای آخر (از بین ۲۰۰۰ خط آخر لاگ):")
    await callback.answer()


@router.callback_query(F.data == "admin:logs:file")
async def cb_admin_logs_file(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    if not os.path.exists(_log_file_path) or os.path.getsize(_log_file_path) == 0:
        await callback.answer("⚠️ فایل لاگی هنوز وجود نداره.", show_alert=True)
        return
    await callback.answer()
    stamp = time_service.now().strftime("%Y%m%d_%H%M%S")
    await callback.message.answer_document(
        document=FSInputFile(_log_file_path, filename=f"bot_log_{stamp}.log"),
        caption="📄 فایل کامل لاگ فعلی (خطوط قدیمی‌تری که قبلاً چرخش/rotation شدن توی این فایل نیستن).",
    )


async def _format_backup_target_label() -> str:
    raw = await get_setting("backup_target_chat_id")
    if raw:
        return f"آیدی عددی {raw} (تنظیم‌شده توسط ادمین)"
    owner = config.ADMIN_IDS[0] if config.ADMIN_IDS else None
    return f"مالک ربات، آیدی {owner} (پیش‌فرض -- چیزی تنظیم نشده)" if owner else "❌ هیچ مقصدی تنظیم نشده و مالکی هم تعریف نشده"


@router.callback_query(F.data == "admin:backup")
async def cb_admin_backup(callback: CallbackQuery):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    current_minutes = await get_backup_interval_minutes()
    await callback.message.edit_text("💾 در حال ایجاد پشتیبان JSON...", reply_markup=backup_menu_keyboard())
    backup_path = await backup_service.export_json()
    if backup_path:
        sent = await backup_service.send_backup_to_admins(bot, backup_path)
        await callback.message.edit_text(
            f"✅ پشتیبان ساخته و برای {sent} ادمین ارسال شد.\n"
            f"⏱ فاصله پشتیبان‌گیری خودکار فعلی: هر {current_minutes} دقیقه.",
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
    current_minutes = await get_backup_interval_minutes()
    await callback.message.edit_text(
        f"⏱ فاصله فعلی پشتیبان‌گیری خودکار: هر {current_minutes} دقیقه.\n\n"
        "عدد دقیقه‌ی جدید را وارد کنید (بین ۱ تا ۱۰۰۸۰ -- یعنی حداکثر هر ۷ روز):",
        reply_markup=cancel_keyboard("backup"),
    )
    await state.set_state(AdminStates.SET_BACKUP_INTERVAL)
    await callback.answer()


@router.message(AdminStates.SET_BACKUP_INTERVAL)
async def admin_set_backup_interval(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    try:
        minutes = int(message.text.strip())
        if not (1 <= minutes <= 10080):
            raise ValueError
    except ValueError:
        await message.answer("❌ لطفاً عددی صحیح بین ۱ تا ۱۰۰۸۰ (دقیقه) وارد کنید.")
        return
    await set_setting("backup_interval_minutes", str(minutes))
    await reschedule_backup_job(minutes)
    await message.answer(f"✅ فاصله پشتیبان‌گیری خودکار روی هر {minutes} دقیقه تنظیم شد.", reply_markup=admin_panel())
    await state.clear()


@router.callback_query(F.data == "admin:backup_target")
async def cb_admin_backup_target(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in config.ADMIN_IDS:
        await callback.answer("⛔ دسترسی غیرمجاز!")
        return
    label = await _format_backup_target_label()
    await callback.message.edit_text(
        f"🎯 مقصد فعلی بک‌آپ خودکار: {label}\n\n"
        "آیدی عددی گروه، کانال یا شخص موردنظر رو بفرست (گروه/کانال معمولاً با یه عدد منفی مثل -1001234567890 شروع می‌شه).\n"
        "برای برگردوندن به حالت پیش‌فرض (ارسال به خود مالک)، عدد 0 رو بفرست.\n\n"
        "⚠️ ربات باید از قبل عضو اون گروه/کانال باشه، وگرنه ارسال بک‌آپ باهاش شکست می‌خوره.",
        reply_markup=cancel_keyboard("backup"),
    )
    await state.set_state(AdminStates.SET_BACKUP_TARGET)
    await callback.answer()


@router.message(AdminStates.SET_BACKUP_TARGET)
async def admin_set_backup_target(message: Message, state: FSMContext):
    if message.from_user.id not in config.ADMIN_IDS:
        return
    try:
        chat_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("❌ لطفاً یک آیدی عددی معتبر بفرست (یا 0 برای پیش‌فرض).")
        return
    if chat_id == 0:
        await set_backup_target_chat_id(None)
        await message.answer("✅ مقصد بک‌آپ به حالت پیش‌فرض (مالک ربات) برگشت.", reply_markup=admin_panel())
    else:
        await set_backup_target_chat_id(chat_id)
        await message.answer(f"✅ مقصد بک‌آپ خودکار روی آیدی {chat_id} تنظیم شد.", reply_markup=admin_panel())
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
    await callback.message.edit_text(
        "📤 فایل پشتیبان (JSON) را همینجا ارسال کنید:",
        reply_markup=cancel_keyboard("restore"),
    )
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


@router.callback_query(F.data.startswith("cancel:"))
async def cb_cancel_input(callback: CallbackQuery, state: FSMContext):
    """Generic handler for every '❌ لغو' button attached to an FSM text/file
    prompt. Clears whatever state the user was in (so a stray message sent
    afterward isn't misinterpreted as the cancelled input) and returns them
    to the menu they came from."""
    await state.clear()
    target = callback.data.split("cancel:", 1)[1]
    is_admin = callback.from_user.id in config.ADMIN_IDS

    if target == "admin" and is_admin:
        await callback.message.edit_text("🛠 پنل مدیریت:", reply_markup=admin_panel())
    elif target == "messages" and is_admin:
        await callback.message.edit_text("💬 مدیریت پیام‌ها:", reply_markup=message_management())
    elif target == "notifications" and is_admin:
        await callback.message.edit_text("🔔 مدیریت اعلان‌ها:", reply_markup=admin_notifications_keyboard())
    elif target == "backup" and is_admin:
        current_minutes = await get_backup_interval_minutes()
        label = await _format_backup_target_label()
        await callback.message.edit_text(
            f"💾 پشتیبان‌گیری\n⏱ فاصله فعلی: هر {current_minutes} دقیقه.\n🎯 مقصد فعلی: {label}",
            reply_markup=backup_menu_keyboard(),
        )
    elif target == "restore" and is_admin:
        backups = backup_service.list_backups()
        if backups:
            text = "♻️ بازیابی از دیتابیس:\n\nیکی از پشتیبان‌های موجود روی سرور را انتخاب کنید یا فایل جدیدی آپلود کنید."
        else:
            text = "♻️ بازیابی از دیتابیس:\n\nهیچ پشتیبانی روی سرور یافت نشد. می‌توانید فایل پشتیبان JSON را آپلود کنید."
        await callback.message.edit_text(text, reply_markup=restore_menu_keyboard(backups))
    elif target == "settings":
        await callback.message.edit_text("⚙️ تنظیمات:", reply_markup=settings_keyboard())
    elif target == "tunables" and is_admin:
        await callback.message.edit_text("⚙️ تنظیمات عددی سراسری ربات:", reply_markup=tunables_keyboard())
    elif target == "algorithm" and is_admin:
        await callback.message.edit_text("🧠 وزن‌های الگوریتم:", reply_markup=algorithm_weights_keyboard())
    elif target == "groups" and is_admin:
        async with get_session() as session:
            group_repo = GroupRepository(session)
            groups = await group_repo.get_all_active()
        if groups:
            await callback.message.edit_text("👥 گروه‌ها:", reply_markup=admin_groups_keyboard(groups))
        else:
            await callback.message.edit_text("👥 هنوز هیچ گروه فعالی ثبت نشده.", reply_markup=admin_panel())
    elif target == "activities":
        await _send_activities_list(callback.from_user.id, callback.message, edit=True)
    elif target == "routine":
        await _send_routines_list(callback.from_user.id, callback.message, edit=True)
    else:
        await callback.message.edit_text("📋 منوی اصلی:", reply_markup=main_menu(is_admin))
    await callback.answer("لغو شد")


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
    # Reuse the same module-level mapping _cancel_scheduled_job relies on,
    # plus readiness_poll (which is never individually cancelled, only
    # replaced -- see _create_readiness_poll's "session already in flight"
    # guard -- so it was never part of the shared map).
    job_funcs = {**JOB_FUNCS, "readiness_poll": (_create_readiness_poll, "group_id")}

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
        entry = job_funcs.get(job.job_type)
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


async def _daily_personal_plan_broadcast():
    """Proactive counterpart to /plan: once a day, build every user's
    personalized plan (behavior profile + a research-backed tip) and push
    it out without waiting for them to ask, to wherever
    UserPreference.proactive_delivery says -- their DM, every active group
    they're in, or both. Users with daily_plan_enabled == False, or with
    not enough data yet for a plan, are skipped quietly (no spam, no
    "not enough data" message shown unsolicited)."""
    async with get_session() as session:
        result = await session.execute(select(User.id, User.telegram_id))
        all_users = result.all()

    for internal_id, telegram_id in all_users:
        try:
            async with get_session() as session:
                pref_result = await session.execute(
                    select(UserPreference).where(UserPreference.user_id == internal_id)
                )
                prefs = pref_result.scalar_one_or_none()
                enabled = prefs.daily_plan_enabled if prefs else True
                if not enabled:
                    continue
                delivery = prefs.proactive_delivery if prefs else "private"

                plan = await advisor.generate_personalized_plan(internal_id, session)
                if not plan.get("has_data"):
                    continue

                plan_repo = PlanRepository(session)
                _, plan_parts = await plan_repo.replace_today_plan(
                    internal_id, plan.get("parts_detail", []), source="auto"
                )
                await session.commit()

                groups = []
                if delivery in ("group", "both"):
                    group_repo = GroupRepository(session)
                    groups = await group_repo.get_user_groups(internal_id)

            if bot is None:
                continue

            text = "🌅 صبح بخیر! برنامه شخصی امروزت آماده‌ست:\n\n" + plan["message"]

            if delivery in ("private", "both"):
                try:
                    # Attaches the same done/not-done toggle keyboard the
                    # on-demand /plan gets, so a part can be checked off
                    # straight from this proactive push too.
                    await bot.send_message(telegram_id, text, reply_markup=plan_keyboard(plan_parts))
                except Exception as e:
                    logger.debug(f"Daily plan DM failed for user {telegram_id}: {e}")

            if delivery in ("group", "both"):
                for group in groups:
                    try:
                        await bot.send_message(group.telegram_chat_id, text)
                    except Exception as e:
                        logger.debug(
                            f"Daily plan group send failed for group {group.telegram_chat_id}: {e}"
                        )
        except Exception as e:
            logger.error(f"Daily personal plan failed for user {telegram_id}: {e}")


async def _run_scheduled_backup():
    """Job body for the recurring backup: export to JSON, then push the
    file to its configured target -- a specific group/channel/person an
    admin picked by numeric ID (⏱ فاصله پشتیبان‌گیری -> 🎯 تنظیم مقصد),
    or the bot owner if nothing was ever set (see
    get_backup_target_chat_id)."""
    backup_path = await backup_service.export_json()
    if not backup_path:
        logger.error("Scheduled backup failed to generate a file")
        return
    if bot is None:
        logger.warning("Scheduled backup created but bot is not ready to send it")
        return
    target = await get_backup_target_chat_id()
    if target is None:
        logger.warning("Scheduled backup created but no target/owner is configured to send it to")
        return
    sent = await backup_service.send_backup_to_admins(bot, backup_path, admin_ids=[target])
    if sent:
        logger.info(f"Scheduled backup sent to {target}")
    else:
        logger.error(f"Scheduled backup could not be delivered to {target}")


async def reschedule_backup_job(minutes: Optional[int] = None) -> int:
    """(Re)installs the recurring backup job at the given interval
    (minutes). If omitted, reads the persisted admin setting (falling
    back to config.BACKUP_INTERVAL_MINUTES, default 30 -- i.e. every
    half hour). Safe to call any time, including from the admin panel,
    to apply a new interval immediately."""
    if minutes is None:
        minutes = await get_backup_interval_minutes()
    scheduler.add_job(
        _run_scheduled_backup,
        trigger=IntervalTrigger(minutes=minutes, timezone="Asia/Tehran"),
        id="periodic_backup",
        replace_existing=True,
    )
    logger.info(f"Backup job scheduled every {minutes} minute(s)")
    return minutes


async def reschedule_morning_poll_job() -> None:
    """(Re)installs the daily morning-poll cron job at
    config.MORNING_POLL_HOUR/MINUTE. Called once at startup and again any
    time an admin changes either value from the panel, so the new time
    takes effect immediately -- no restart needed."""
    scheduler.add_job(
        _morning_poll_all_groups,
        trigger=CronTrigger(hour=config.MORNING_POLL_HOUR, minute=config.MORNING_POLL_MINUTE, timezone="Asia/Tehran"),
        id="morning_reminder",
        replace_existing=True,
    )
    logger.info(f"Morning poll job scheduled at {config.MORNING_POLL_HOUR:02d}:{config.MORNING_POLL_MINUTE:02d}")


async def reschedule_daily_plan_job() -> None:
    """(Re)installs the proactive daily-personal-plan cron job at
    config.DAILY_PLAN_HOUR/MINUTE. Same pattern as
    reschedule_morning_poll_job -- applies an admin change immediately."""
    scheduler.add_job(
        _daily_personal_plan_broadcast,
        trigger=CronTrigger(hour=config.DAILY_PLAN_HOUR, minute=config.DAILY_PLAN_MINUTE, timezone="Asia/Tehran"),
        id="daily_personal_plan",
        replace_existing=True,
    )
    logger.info(f"Daily plan job scheduled at {config.DAILY_PLAN_HOUR:02d}:{config.DAILY_PLAN_MINUTE:02d}")


async def on_startup():
    logger.info("Starting Study Coach...")
    await init_db()
    # Pull in any admin-saved overrides (session length bounds, morning
    # poll time, algorithm weights, ...) before anything else reads
    # `config`, so a change made from the panel before the last restart is
    # actually in effect from the first job/scoring call onward.
    await load_persisted_tunables()
    SchedulerService.start()
    await recover_from_crash()
    # Fixed morning readiness poll for every active group (the "اول صبح
    # فقط" cadence the user asked for) -- everything past this point in the
    # day is driven by _schedule_next_poll() after a session completes, or
    # by real group activity waking a quiet group back up. Time is an
    # admin-tunable setting (config.MORNING_POLL_HOUR/MINUTE), not
    # hardcoded -- see reschedule_morning_poll_job.
    await reschedule_morning_poll_job()
    # Proactive daily personal plan -- shortly after the morning poll, so
    # it reflects "today". Independent of /plan; see
    # _daily_personal_plan_broadcast and UserPreference.daily_plan_enabled
    # / proactive_delivery for the per-user opt-out and delivery target.
    await reschedule_daily_plan_job()
    # Personal routines (بخش روتین) -- every active reminder/part a user
    # has set up needs its own APScheduler job re-registered, same
    # reasoning as the two calls above.
    await reschedule_all_routines()
    # Recurring JSON backup, sent to its configured target (a specific
    # group/channel/person, or the bot owner by default) -- interval is
    # configurable at runtime from the admin panel (persisted setting),
    # defaulting to BACKUP_INTERVAL_MINUTES (30, i.e. every half hour).
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
        except (ValueError, OSError) as e:
            logger.warning(f"Could not read/validate existing pid file, ignoring it: {e}")
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

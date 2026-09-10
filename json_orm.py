"""
json_orm.py
============
A minimal, self-contained, dependency-free storage engine that mimics just
enough of the SQLAlchemy Core/ORM surface used by study_coach.py to be a
drop-in replacement -- but persists everything to a single JSON file
instead of SQLite.

WHY THIS EXISTS
---------------
The original app hit "database is locked" errors from SQLite under
concurrent async access. Rather than patch around SQLite's single-writer
limitation, this module replaces the storage layer entirely: all state
lives in memory as plain Python objects, guarded by a single asyncio.Lock,
and is atomically flushed to disk as JSON on every commit. Because there
is no second process and no OS-level file lock being contended, a
"database is locked" error is structurally impossible -- concurrent
writers simply queue on the in-process lock instead of erroring.

TRADE-OFF: everything now serializes through one lock. If a caller holds
a session open across a slow network call (e.g. `await message.answer()`
inside `async with get_session():`), other DB operations will *wait*
instead of failing -- slower under load, but never broken. See the
"KNOWN HOTSPOT" note in study_coach.py where this actually happens.

SUPPORTED QUERY SURFACE (only what study_coach.py actually uses):
  - declarative models via `class Foo(Base): ...` with Column/Integer/
    String/Boolean/DateTime/Float/Text/JSON, ForeignKey, UniqueConstraint,
    Index (Index is accepted but not functionally enforced -- it's a
    performance hint in SQL and has no equivalent need here).
  - select(Model), select(Model.col), select(col1, col2, func.count(...))
  - .where(a, b, ...)                (implicit AND, same as SQLAlchemy)
  - .join(Model2, Model.a == Model2.b)
  - .order_by(col) / .order_by(desc(col))
  - .limit(n)
  - .group_by(col)
  - Model.col.in_(list) / ~Model.col.in_(list)
  - func.count(), func.count(col), func.sum(col), select(...).select_from(Model)
  - session.execute(query) -> Result with .scalar_one_or_none(),
    .scalars().all(), .all(), .scalar(), .mappings().all()
  - session.add(obj) / .add_all([...]) / .delete(obj) / .get(Model, pk)
  - session.flush() / .commit() / .rollback()
  - IntegrityError raised on unique-constraint violations at flush time
  - delete(Model)  (bulk delete, used by the backup/restore feature)
  - Model.insert() + session.execute(Model.insert(), [rows...])  (bulk load)
  - Base.metadata.sorted_tables  (used by BackupService to iterate tables)

NOT supported (and not used anywhere in study_coach.py, verified by
grepping the whole file before writing this): and_(), or_(), subqueries,
outer joins, relationship()/joinedload() eager loading, raw SQL strings.
"""

from __future__ import annotations

import asyncio
import json as _json
import logging
import os
import tempfile
from datetime import datetime, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type


# ============================================================
# EXCEPTIONS
# ============================================================
class IntegrityError(Exception):
    """Raised on a unique-constraint violation at flush time, mirroring
    sqlalchemy.exc.IntegrityError closely enough for the existing
    try/except IntegrityError blocks in study_coach.py to keep working."""
    pass


# ============================================================
# COLUMN TYPES (markers only -- used for (de)serialization hints)
# ============================================================
class _Type:
    def __init__(self, *args, **kwargs):
        pass


class Integer(_Type):
    pass


class String(_Type):
    pass


class Text(_Type):
    pass


class Boolean(_Type):
    pass


class Float(_Type):
    pass


class DateTime(_Type):
    pass


class JSON(_Type):
    pass


_TYPE_INSTANCES = (Integer, String, Text, Boolean, Float, DateTime, JSON)


# ============================================================
# CONSTRAINT / METADATA MARKERS
# ============================================================
class ForeignKey:
    def __init__(self, target: str, *args, **kwargs):
        self.target = target


class UniqueConstraint:
    def __init__(self, *columns: str, name: Optional[str] = None):
        self.columns = columns
        self.name = name


class Index:
    """Accepted for source compatibility; not functionally needed since
    every table here is a small in-memory list/dict, not a B-tree index."""
    def __init__(self, *args, **kwargs):
        pass


# ============================================================
# COLUMN
# ============================================================
class Column:
    """Mirrors sqlalchemy.Column's constructor shape closely enough that
    every existing `Column(...)` declaration in study_coach.py works
    unmodified. Supports the `Column("db_name", Type, ...)` form used for
    the metadata/event_meta and metadata/job_meta aliasing."""

    def __init__(self, *args, primary_key: bool = False, default: Any = None,
                 nullable: bool = True, unique: bool = False, index: bool = False,
                 onupdate: Any = None, autoincrement: Optional[bool] = None):
        self.db_name: Optional[str] = None
        self.type_: Optional[type] = None
        self.foreign_key: Optional[ForeignKey] = None
        for a in args:
            if isinstance(a, str):
                self.db_name = a
            elif isinstance(a, ForeignKey):
                self.foreign_key = a
            elif isinstance(a, _TYPE_INSTANCES) or (isinstance(a, type) and issubclass(a, _Type)):
                self.type_ = a if isinstance(a, type) else type(a)
            # bare type classes like `Integer` (not `Integer()`) also land here
        self.primary_key = primary_key
        self.default = default
        self.nullable = nullable
        self.unique = unique
        self.onupdate = onupdate
        self.attr_name: Optional[str] = None  # set by __set_name__

    def __set_name__(self, owner, name):
        self.attr_name = name
        if self.db_name is None:
            self.db_name = name

    def get_default(self):
        if self.default is None:
            return None
        if callable(self.default):
            return self.default()
        return self.default

    # --- descriptor protocol ---
    def __get__(self, instance, owner):
        if instance is None:
            return ColumnRef(owner, self)
        return instance.__dict__.get(self.attr_name)

    def __set__(self, instance, value):
        instance.__dict__[self.attr_name] = value
        if getattr(instance, "_in_init", False):
            return
        # Mimic onupdate=... : touch other onupdate columns whenever any
        # field changes on an already-constructed instance.
        for name, col in getattr(type(instance), "_onupdate_cols", []):
            if name != self.attr_name and col.onupdate is not None:
                instance.__dict__[name] = col.onupdate() if callable(col.onupdate) else col.onupdate


# ============================================================
# QUERY-BUILDING: ColumnRef / Predicate / func / desc
# ============================================================
class ColumnRef:
    """What `Model.some_column` evaluates to at the class level. Supports
    the comparison operators used throughout study_coach.py to build
    Predicate objects, and can also appear on the *right* side of a
    predicate for join conditions (Model.a == Model2.b)."""

    def __init__(self, model: type, column: Column):
        self.model = model
        self.column = column

    @property
    def attr_name(self):
        return self.column.attr_name

    def resolve(self, context: Dict[type, Any]):
        inst = context.get(self.model)
        if inst is None:
            return None
        return getattr(inst, self.attr_name)

    def __eq__(self, other): return Predicate(self, "==", other)
    def __ne__(self, other): return Predicate(self, "!=", other)
    def __ge__(self, other): return Predicate(self, ">=", other)
    def __le__(self, other): return Predicate(self, "<=", other)
    def __gt__(self, other): return Predicate(self, ">", other)
    def __lt__(self, other): return Predicate(self, "<", other)
    def __hash__(self): return id(self)

    def in_(self, values):
        return Predicate(self, "in", list(values))


class Predicate:
    def __init__(self, left, op: str, right):
        self.left = left
        self.op = op
        self.right = right

    @staticmethod
    def _val(side, context):
        if isinstance(side, ColumnRef):
            return side.resolve(context)
        return side

    def matches(self, context: Dict[type, Any]) -> bool:
        lv = self._val(self.left, context)
        rv = self._val(self.right, context)
        op = self.op
        try:
            if op == "==":
                return lv == rv
            if op == "!=":
                return lv != rv
            if op == ">=":
                return lv is not None and rv is not None and lv >= rv
            if op == "<=":
                return lv is not None and rv is not None and lv <= rv
            if op == ">":
                return lv is not None and rv is not None and lv > rv
            if op == "<":
                return lv is not None and rv is not None and lv < rv
            if op == "in":
                return lv in rv
        except TypeError:
            return False
        return False

    def __invert__(self):
        return NotPredicate(self)


class NotPredicate:
    def __init__(self, inner: Predicate):
        self.inner = inner

    def matches(self, context):
        return not self.inner.matches(context)


class DescMarker:
    def __init__(self, colref: ColumnRef):
        self.colref = colref


def desc(colref: ColumnRef) -> DescMarker:
    return DescMarker(colref)


# --- aggregate functions ---
class _CountFunc:
    def __init__(self, colref: Optional[ColumnRef] = None):
        self.colref = colref


class _SumFunc:
    def __init__(self, colref: ColumnRef):
        self.colref = colref


class _FuncNamespace:
    def count(self, colref: Optional[ColumnRef] = None):
        return _CountFunc(colref)

    def sum(self, colref: ColumnRef):
        return _SumFunc(colref)


func = _FuncNamespace()


# ============================================================
# SELECT / DELETE / INSERT CONSTRUCTS
# ============================================================
class Select:
    def __init__(self, *entities):
        self.entities = entities
        self._wheres: List[Any] = []
        self._joins: List[Tuple[type, Any]] = []
        self._order_by: Optional[ColumnRef] = None
        self._order_desc = False
        self._limit: Optional[int] = None
        self._group_by: Optional[ColumnRef] = None
        self._select_from_model: Optional[type] = None

    def where(self, *preds):
        self._wheres.extend(preds)
        return self

    def join(self, model, predicate):
        self._joins.append((model, predicate))
        return self

    def order_by(self, col_or_desc):
        if isinstance(col_or_desc, DescMarker):
            self._order_by = col_or_desc.colref
            self._order_desc = True
        else:
            self._order_by = col_or_desc
            self._order_desc = False
        return self

    def limit(self, n: int):
        self._limit = n
        return self

    def group_by(self, colref: ColumnRef):
        self._group_by = colref
        return self

    def select_from(self, model):
        self._select_from_model = model
        return self


def select(*entities) -> Select:
    return Select(*entities)


class Delete:
    def __init__(self, model):
        self.model = model
        self._wheres: List[Any] = []

    def where(self, *preds):
        self._wheres.extend(preds)
        return self


def delete(model) -> Delete:
    return Delete(model)


class Insert:
    def __init__(self, model):
        self.model = model


# ============================================================
# RESULT WRAPPER
# ============================================================
class _ScalarsResult:
    def __init__(self, values: list):
        self._values = values

    def all(self):
        return self._values


class _MappingsResult:
    def __init__(self, dict_rows: list):
        self._dict_rows = dict_rows

    def all(self):
        return self._dict_rows


class Result:
    """rows: list of tuples. is_single_entity: True when the caller did
    select(Model) (as opposed to select(Model.col, ...)) so .scalars()
    unwraps each 1-tuple to the bare model instance."""

    def __init__(self, rows: List[tuple], is_single_entity: bool = False,
                 mapping_rows: Optional[list] = None):
        self._rows = rows
        self._is_single_entity = is_single_entity
        self._mapping_rows = mapping_rows

    def scalar_one_or_none(self):
        if not self._rows:
            return None
        return self._rows[0][0]

    def scalar(self):
        if not self._rows:
            return None
        return self._rows[0][0]

    def scalars(self):
        return _ScalarsResult([r[0] for r in self._rows])

    def all(self):
        if self._is_single_entity:
            return [r[0] for r in self._rows]
        return self._rows

    def mappings(self):
        return _MappingsResult(self._mapping_rows or [])


# ============================================================
# METADATA REGISTRY
# ============================================================
class _TableRef:
    """What Base.metadata.sorted_tables yields -- wraps a Model class so
    BackupService's generic `for table in ... : table.name / table.insert()`
    loop keeps working without caring that "tables" are really just model
    classes here."""

    def __init__(self, model: type):
        self.model = model
        self.name = model._table_name

    def insert(self):
        return Insert(self.model)


class _Metadata:
    def __init__(self):
        self._registry: Dict[str, type] = {}

    def register(self, model: type):
        self._registry[model._table_name] = model

    @property
    def sorted_tables(self) -> List[_TableRef]:
        # Registration order == declaration order == FK-dependency order,
        # since every model in study_coach.py is declared after the
        # tables it references (verified against the actual file).
        return [_TableRef(m) for m in self._registry.values()]


_metadata = _Metadata()


def _unwrap_table(obj):
    """select(table)/delete(table) may receive either a raw Model class
    or a _TableRef (from Base.metadata.sorted_tables) -- normalize."""
    if isinstance(obj, _TableRef):
        return obj.model
    return obj


# ============================================================
# BASE MODEL
# ============================================================
class Base:
    metadata = _metadata

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        columns: Dict[str, Column] = {}
        for base in reversed(cls.__mro__[1:]):
            columns.update(getattr(base, "_columns", {}))
        for key, val in list(vars(cls).items()):
            if isinstance(val, Column):
                columns[key] = val
        cls._columns = columns
        cls._pk_fields = [name for name, col in columns.items() if col.primary_key]
        cls._table_name = getattr(cls, "__tablename__", cls.__name__.lower())
        cls._onupdate_cols = [(name, col) for name, col in columns.items() if col.onupdate is not None]

        # unique constraints: column-level `unique=True` (single-field) +
        # UniqueConstraint(...) tuples from __table_args__ (multi-field)
        uniques: List[Tuple[str, ...]] = []
        for name, col in columns.items():
            if col.unique:
                uniques.append((name,))
        for arg in getattr(cls, "__table_args__", ()) or ():
            if isinstance(arg, UniqueConstraint):
                uniques.append(tuple(arg.columns))
        cls._unique_groups = uniques

        # autoincrement pk: exactly one PK field, Integer type, no FK
        cls._autopk = None
        if len(cls._pk_fields) == 1:
            only = cls._pk_fields[0]
            col = columns[only]
            if col.foreign_key is None and (col.type_ is Integer or col.type_ is None):
                cls._autopk = only

        _metadata.register(cls)

    def __init__(self, **kwargs):
        object.__setattr__(self, "_in_init", True)
        for name, col in type(self)._columns.items():
            setattr(self, name, kwargs[name] if name in kwargs else col.get_default())
        object.__setattr__(self, "_in_init", False)

    # --- helpers ---
    def pk_value(self):
        cls = type(self)
        if len(cls._pk_fields) == 1:
            return getattr(self, cls._pk_fields[0])
        return tuple(getattr(self, f) for f in cls._pk_fields)

    def to_row_dict(self) -> Dict[str, Any]:
        """Keyed by DB column name (matches the historical backup-file
        format, e.g. 'metadata' rather than 'event_meta')."""
        out = {}
        for name, col in type(self)._columns.items():
            out[col.db_name] = getattr(self, name)
        return out

    @classmethod
    def from_row_dict(cls, row: Dict[str, Any]):
        by_db_name = {col.db_name: name for name, col in cls._columns.items()}
        kwargs = {}
        for db_name, value in row.items():
            attr = by_db_name.get(db_name)
            if attr is None:
                continue
            col = cls._columns[attr]
            kwargs[attr] = _coerce_in(col, value)
        return cls(**kwargs)

    @classmethod
    def insert(cls):
        return Insert(cls)


def declarative_base():
    return Base


def _coerce_in(col: Column, value: Any) -> Any:
    """When loading from JSON, ISO datetime strings need to become real
    datetime objects so `.strftime()`, comparisons, etc. keep working."""
    if value is None:
        return None
    if col.type_ is DateTime and isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def _serialize_out(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


# ============================================================
# STORAGE ENGINE
# ============================================================
class JsonStore:
    FORMAT_VERSION = "1.0.0-json"

    def __init__(self, path: str):
        self.path = Path(path)
        self.lock = asyncio.Lock()
        # table_name -> {pk: instance}
        self.tables: Dict[str, Dict[Any, Any]] = {}
        self._seq: Dict[str, int] = {}
        self._loaded = False
        # Background-save state -- see request_save() below.
        self._pending_snapshot: Optional[dict] = None
        self._save_task: Optional[asyncio.Task] = None

    # ---------------- load / save ----------------
    def _ensure_all_tables(self):
        for name, model in _metadata._registry.items():
            self.tables.setdefault(name, {})
            self._seq.setdefault(name, 0)

    def load(self):
        self._ensure_all_tables()
        if not self.path.exists():
            self._loaded = True
            return
        with open(self.path, "r", encoding="utf-8") as f:
            payload = _json.load(f)
        tables_data = payload.get("tables", {})
        for name, model in _metadata._registry.items():
            rows = tables_data.get(name, [])
            table: Dict[Any, Any] = {}
            max_id = 0
            for row in rows:
                inst = model.from_row_dict(row)
                table[inst.pk_value()] = inst
                if model._autopk:
                    try:
                        max_id = max(max_id, int(getattr(inst, model._autopk)))
                    except (TypeError, ValueError):
                        pass
            self.tables[name] = table
            self._seq[name] = max_id
        self._loaded = True

    def _snapshot(self) -> dict:
        tables_data = {}
        for name, table in self.tables.items():
            rows = []
            for inst in table.values():
                row = {k: _serialize_out(v) for k, v in inst.to_row_dict().items()}
                rows.append(row)
            tables_data[name] = rows
        return {
            "format_version": self.FORMAT_VERSION,
            "saved_at": datetime.utcnow().isoformat(),
            "tables": tables_data,
        }

    def save_sync(self):
        """Atomic write: temp file in the same directory + os.replace, so
        a crash mid-write can never leave a truncated/corrupt JSON file."""
        self._write_payload(self._snapshot())

    async def save(self):
        """Synchronous-feeling, fully-awaited save. Serializes and fsyncs
        the *entire* database to disk before returning. Slow by nature --
        cost grows with total DB size -- so this is only for places that
        must guarantee the write landed before continuing (e.g. shutdown).
        Everyday commits should use request_save() instead (see below)."""
        await asyncio.to_thread(self.save_sync)

    # ---------------- debounced background persistence ----------------
    # PERFORMANCE NOTE: every get_session() commit used to `await
    # store.save()` directly -- a full snapshot-serialize + JSON dump +
    # os.fsync of the *whole* database, on *every* Telegram button tap,
    # while still holding store.lock. Two problems compounded: (1) that
    # full-file write is O(total DB size), so it gets slower as more
    # users/sessions/logs accumulate, and (2) because it happened before
    # the lock was released, *every other* button press anywhere in the
    # bot had to queue behind it. That's what made buttons feel like they
    # "open slowly" -- it wasn't the network, it was every interaction
    # blocking on a full-database fsync before Telegram even got a reply.
    #
    # request_save() fixes this without weakening durability much: it
    # takes a cheap in-memory snapshot immediately (cheap: no I/O, just
    # building dicts) while the caller still holds the lock, so the
    # snapshot is guaranteed consistent -- then hands the actual disk
    # write off to a background task and returns immediately. Concurrent
    # commits that land while a write is already in flight coalesce onto
    # the same task instead of piling up redundant writes; the loop just
    # keeps writing the latest pending snapshot until none remain.
    def request_save(self):
        self._pending_snapshot = self._snapshot()
        if self._save_task is None or self._save_task.done():
            self._save_task = asyncio.create_task(self._background_save_loop())

    async def _background_save_loop(self):
        while self._pending_snapshot is not None:
            payload = self._pending_snapshot
            self._pending_snapshot = None
            try:
                await asyncio.to_thread(self._write_payload, payload)
            except Exception:
                logging.getLogger(__name__).exception(
                    "Background DB save failed -- will retry on next commit"
                )

    def _write_payload(self, payload: dict):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                _json.dump(payload, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.path)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass

    async def wait_for_pending_save(self):
        """Used on shutdown: block until any in-flight background write
        finishes, before the final synchronous save() runs."""
        if self._save_task is not None and not self._save_task.done():
            try:
                await self._save_task
            except Exception:
                pass

    def next_id(self, table_name: str) -> int:
        self._seq[table_name] = self._seq.get(table_name, 0) + 1
        return self._seq[table_name]

    def export_snapshot(self) -> dict:
        """Used by BackupService for point-in-time exports."""
        return self._snapshot()

    def restore_snapshot(self, payload: dict):
        tables_data = payload.get("tables", {})
        new_tables: Dict[str, Dict[Any, Any]] = {}
        new_seq: Dict[str, int] = {}
        for name, model in _metadata._registry.items():
            rows = tables_data.get(name, [])
            table: Dict[Any, Any] = {}
            max_id = 0
            for row in rows:
                inst = model.from_row_dict(row)
                table[inst.pk_value()] = inst
                if model._autopk:
                    try:
                        max_id = max(max_id, int(getattr(inst, model._autopk)))
                    except (TypeError, ValueError):
                        pass
            new_tables[name] = table
            new_seq[name] = max_id
        self.tables = new_tables
        self._seq = new_seq


_store: Optional[JsonStore] = None


def get_store() -> JsonStore:
    if _store is None:
        raise RuntimeError("JsonStore not initialized -- call init_db() first")
    return _store


def _set_store(store: JsonStore):
    global _store
    _store = store


# ============================================================
# SESSION (mimics sqlalchemy.ext.asyncio.AsyncSession)
# ============================================================
class JsonSession:
    def __init__(self, store: JsonStore):
        self.store = store
        self._pending_adds: List[Any] = []
        self._pending_deletes: List[Any] = []
        self._dirty = False

    # ---------------- mutation API ----------------
    def add(self, obj):
        self._pending_adds.append(obj)
        self._dirty = True

    def add_all(self, objs):
        for o in objs:
            self.add(o)

    async def delete(self, obj):
        self._pending_deletes.append(obj)
        self._dirty = True

    async def get(self, model: type, pk):
        table = self.store.tables.get(model._table_name, {})
        return table.get(pk)

    async def flush(self):
        if not self._pending_adds and not self._pending_deletes:
            return
        # Validate ALL pending adds before inserting ANY of them, so a
        # single conflict aborts the whole flush() call atomically --
        # matching the two-caller pattern in study_coach.py that adds one
        # object then immediately flushes.
        for obj in self._pending_adds:
            self._check_unique(obj)
        for obj in self._pending_adds:
            model = type(obj)
            table = self.store.tables.setdefault(model._table_name, {})
            if model._autopk and getattr(obj, model._autopk) is None:
                setattr(obj, model._autopk, self.store.next_id(model._table_name))
            table[obj.pk_value()] = obj
        for obj in self._pending_deletes:
            model = type(obj)
            table = self.store.tables.get(model._table_name, {})
            table.pop(obj.pk_value(), None)
        self._pending_adds = []
        self._pending_deletes = []

    def _check_unique(self, obj):
        model = type(obj)
        table = self.store.tables.get(model._table_name, {})
        for group in model._unique_groups:
            new_vals = tuple(getattr(obj, f) for f in group)
            if any(v is None for v in new_vals):
                continue
            for other in table.values():
                if other is obj:
                    continue
                other_vals = tuple(getattr(other, f) for f in group)
                if other_vals == new_vals:
                    raise IntegrityError(
                        f"UNIQUE constraint failed: {model._table_name}.{'+'.join(group)}"
                    )
            # also check against other pending adds in this same flush batch
            for other in self._pending_adds:
                if other is obj:
                    continue
                if type(other) is not model:
                    continue
                other_vals = tuple(getattr(other, f) for f in group)
                if other_vals == new_vals:
                    raise IntegrityError(
                        f"UNIQUE constraint failed: {model._table_name}.{'+'.join(group)}"
                    )

    async def commit(self):
        await self.flush()

    async def rollback(self):
        self._pending_adds = []
        self._pending_deletes = []

    # ---------------- query API ----------------
    async def execute(self, query, params: Optional[list] = None):
        if isinstance(query, Select):
            return self._exec_select(query)
        if isinstance(query, Delete):
            return self._exec_delete(query)
        if isinstance(query, Insert):
            return self._exec_insert(query, params or [])
        raise TypeError(f"Unsupported query construct: {query!r}")

    def _exec_delete(self, query: Delete) -> Result:
        model = _unwrap_table(query.model)
        table = self.store.tables.get(model._table_name, {})
        if not query._wheres:
            table.clear()
            return Result([])
        to_remove = []
        for pk, inst in table.items():
            ctx = {model: inst}
            if all(p.matches(ctx) for p in query._wheres):
                to_remove.append(pk)
        for pk in to_remove:
            del table[pk]
        return Result([])

    def _exec_insert(self, query: Insert, rows: list) -> Result:
        model = query.model
        table = self.store.tables.setdefault(model._table_name, {})
        for row in rows:
            inst = model.from_row_dict(row)
            if model._autopk and getattr(inst, model._autopk) is not None:
                try:
                    self.store._seq[model._table_name] = max(
                        self.store._seq.get(model._table_name, 0),
                        int(getattr(inst, model._autopk)),
                    )
                except (TypeError, ValueError):
                    pass
            table[inst.pk_value()] = inst
        return Result([])

    def _exec_select(self, query: Select) -> Result:
        # Backup/restore passes _TableRef wrappers (from Base.metadata.sorted_tables)
        # instead of raw model classes -- normalize up front.
        entities = tuple(_unwrap_table(e) for e in query.entities)

        # trivial connectivity probe: select(1)
        if len(entities) == 1 and isinstance(entities[0], int):
            return Result([(entities[0],)])

        # select(func.count()).select_from(Model), optionally filtered by
        # .where(...) -- e.g. select(func.count()).select_from(UserPreference)
        # .where(column == True). Previously this branch returned the raw
        # table size and silently ignored any .where() clause, so a
        # filtered count call quietly returned the count of ALL rows
        # instead of the matching ones.
        if len(entities) == 1 and isinstance(entities[0], _CountFunc) and query._select_from_model is not None:
            model = query._select_from_model
            instances = list(self.store.tables.get(model._table_name, {}).values())
            if query._wheres:
                instances = [
                    inst for inst in instances
                    if all(p.matches({model: inst}) for p in query._wheres)
                ]
            colref = entities[0].colref
            if colref is not None:
                # func.count(some_column) counts non-NULL values of that
                # column, same as SQL COUNT(column).
                count = sum(1 for inst in instances if colref.resolve({model: inst}) is not None)
            else:
                count = len(instances)
            return Result([(count,)])

        # figure out base model (first entity, or the model behind a ColumnRef/func)
        base_model = self._entity_model(entities[0])
        if base_model is None:
            raise TypeError("Could not determine base model for select()")

        contexts: List[Dict[type, Any]] = [
            {base_model: inst} for inst in self.store.tables.get(base_model._table_name, {}).values()
        ]

        for join_model, on_pred in query._joins:
            join_table = self.store.tables.get(join_model._table_name, {})
            new_contexts = []
            for ctx in contexts:
                for cand in join_table.values():
                    merged = dict(ctx)
                    merged[join_model] = cand
                    if on_pred.matches(merged):
                        new_contexts.append(merged)
            contexts = new_contexts

        if query._wheres:
            contexts = [c for c in contexts if all(p.matches(c) for p in query._wheres)]

        # grouping + aggregates
        if query._group_by is not None:
            groups: Dict[Any, List[Dict[type, Any]]] = {}
            for ctx in contexts:
                key = query._group_by.resolve(ctx)
                groups.setdefault(key, []).append(ctx)
            out_rows = []
            for key, ctxs in groups.items():
                row = []
                for ent in entities:
                    if isinstance(ent, ColumnRef) and ent is query._group_by or (
                        isinstance(ent, ColumnRef) and ent.attr_name == query._group_by.attr_name
                        and ent.model == query._group_by.model
                    ):
                        row.append(key)
                    elif isinstance(ent, _CountFunc):
                        row.append(len(ctxs))
                    elif isinstance(ent, _SumFunc):
                        vals = [ent.colref.resolve(c) for c in ctxs]
                        row.append(sum(v for v in vals if v is not None))
                    elif isinstance(ent, ColumnRef):
                        row.append(ent.resolve(ctxs[0]) if ctxs else None)
                out_rows.append(tuple(row))
            return Result(out_rows)

        # ordering
        if query._order_by is not None:
            ob = query._order_by
            contexts.sort(
                key=lambda c: _sort_key(ob.resolve(c)),
                reverse=query._order_desc,
            )

        # limit
        if query._limit is not None:
            contexts = contexts[: query._limit]

        is_single_entity = len(entities) == 1 and isinstance(entities[0], type)
        out_rows = []
        mapping_rows = None
        if is_single_entity:
            out_rows = [(c[entities[0]],) for c in contexts]
            mapping_rows = [c[entities[0]].to_row_dict() for c in contexts]
        else:
            for c in contexts:
                row = []
                for ent in entities:
                    if isinstance(ent, ColumnRef):
                        row.append(ent.resolve(c))
                    elif isinstance(ent, type):
                        row.append(c.get(ent))
                    elif isinstance(ent, _CountFunc):
                        row.append(1)
                    else:
                        row.append(None)
                out_rows.append(tuple(row))

        return Result(out_rows, is_single_entity=is_single_entity, mapping_rows=mapping_rows)

    @staticmethod
    def _entity_model(entity):
        entity = _unwrap_table(entity)
        if isinstance(entity, type):
            return entity
        if isinstance(entity, ColumnRef):
            return entity.model
        if isinstance(entity, (_CountFunc, _SumFunc)):
            return entity.colref.model if entity.colref else None
        return None


def _sort_key(value):
    """Allow sorting mixed None/real values without TypeError (None sorts first)."""
    return (value is None, value)


# ============================================================
# ENGINE-LEVEL HELPERS (mirror the small slice of create_async_engine /
# async_sessionmaker surface that study_coach.py's init_db()/get_session()
# actually touch)
# ============================================================
AsyncSession = JsonSession  # type-hint compatibility alias


def resolve_json_path(database_url: str) -> str:
    """Accepts the existing DATABASE_URL env var unchanged (e.g.
    'sqlite:///data/study_coach.db') and maps it to a JSON file path, so
    no .env edits are required when switching engines."""
    url = database_url
    for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
        if url.startswith(prefix):
            url = url[len(prefix):]
            break
    if url.endswith(".db"):
        url = url[:-3] + ".json"
    elif not url.endswith(".json"):
        url = url + ".json"
    return url

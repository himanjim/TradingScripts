#!/usr/bin/env python3
"""
Compact, restart-safe Kite option spike collector.

What it stores
--------------
For the nearest expiry of every configured index, the collector watches:

    ATM-10 ... ATM ... ATM+10, both CE and PE

It stores one compact row for each instrument-second in which the last traded
price changed. The row contains:

* previous observed price;
* one-second open, high, low and close;
* strike offset from the live ATM;
* a small flags field identifying anchors/underlyings/reconnects.

A brief spike that reverses within the same second is therefore retained in the
one-second high or low. Seconds containing no LTP change are omitted by default.
This is intentional and is the main storage reduction.

Storage design
--------------
The earlier collector could exceed 1 GB/day because it retained a row for every
WebSocket packet in ``event_registry``, repeated long text fields in every bar,
created several large indexes, and optionally stored full quote/depth data.

This version uses a highly compact SQLite schema:

* one INTEGER primary key packs ``second_of_day`` and ``instrument_token``;
* prices are exact integer paise rather than 64-bit floating-point values;
* symbols, expiry, strike and option type are stored once in ``instruments``;
* no raw ticks, depth, OI, quote snapshots, per-tick hashes or event registry;
* no secondary index on the large bar table;
* option packets use Kite ``MODE_QUOTE`` (44-byte packet) instead of
  ``MODE_FULL`` (184-byte packet);
* underlying indices use ``MODE_LTP`` (8-byte packet).

Even the theoretical maximum for NIFTY+SENSEX, 86 instruments for every second
of the 09:15-15:30 session, is normally well below 100 MB with this schema.
With the default ``SAVE_ONLY_PRICE_CHANGES=true``, the actual database should be
considerably smaller.

Accuracy boundary
-----------------
The code preserves all LTP transitions delivered to this KiteTicker connection
at one-second OHLC resolution. It cannot reconstruct packets missed during a
network/API outage and Kite Connect is not an exchange tick-by-tick archival
feed. Time buckets use local receipt time in IST because QUOTE mode does not
contain the exchange timestamp.

Restart safety
--------------
SQLite WAL journalling and bounded batch transactions protect committed rows.
A restart on the same day reopens the same database and appends/upserts safely.
The first observation after a connection/subscription boundary is stored as an
ANCHOR row so analysis can avoid treating a feed gap as a normal one-second move.

Existing API dependency
-----------------------
The script preserves the project convention:

    import OptionTradeUtils as oUtils
    kite = oUtils.intialize_kite_api()

The returned KiteConnect object must contain a valid current-day access token.

Recommended installation
------------------------
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip kiteconnect twisted

Recommended execution
---------------------
    python -u KiteOptions1SecSpikeCollector_v3.py

Important environment variables
-------------------------------
TARGET_INDEXES=NIFTY,SENSEX
ATM_WINGS=10
SUBSCRIPTION_BUFFER_WINGS=1
PREOPEN_EXTRA_WINGS=10
OUTPUT_DIR=./kite_spike_data
SAVE_ONLY_PRICE_CHANGES=true
DB_SYNCHRONOUS=FULL
DB_FLUSH_INTERVAL_SEC=0.50
DB_BATCH_TICKS=5000
QUEUE_MAX_FRAMES=20000
SESSION_START=09:15:00
SESSION_END=15:30:00
PRECONNECT_SECONDS=5
STRICT_OPTION_BAND=true
REFRESH_INSTRUMENT_CACHE=false

Optional expiry overrides
-------------------------
EXPIRY_NIFTY=2026-08-04
EXPIRY_SENSEX=2026-08-06
EXPIRY_BANKNIFTY=2026-08-25

Compact flags
-------------
1 = ANCHOR: first stored price after startup/reconnect/new subscription
2 = UNDERLYING
4 = RECONNECT_ANCHOR

The SQL view ``option_1sec_data`` exposes readable timestamps, symbols and rupee
prices while the physical table remains compact.
"""

from __future__ import annotations

import gzip
import json
import logging
import math
import os
import pickle
import queue
import random
import signal
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, TypeVar

from kiteconnect import KiteTicker
from kiteconnect import exceptions as kite_exceptions
from twisted.internet import reactor

# Preserve the user's existing API initialisation convention.
import OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo
except ImportError as exc:  # pragma: no cover
    raise RuntimeError("Python 3.9+ is required") from exc


# =============================================================================
# Version, constants and configuration
# =============================================================================

SCRIPT_VERSION = "3.0.0-compact-spikes"
SCHEMA_VERSION = 3
IST = ZoneInfo("Asia/Kolkata")
T = TypeVar("T")

PRICE_SCALE = 100  # Store exact paise: rupee price = stored integer / 100.0.
TOKEN_MASK = 0xFFFFFFFF
OFFSET_UNDERLYING = 127

FLAG_ANCHOR = 1
FLAG_UNDERLYING = 2
FLAG_RECONNECT_ANCHOR = 4


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be true/false; got {raw!r}")


def env_int(
    name: str,
    default: int,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else int(raw)
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}; got {value}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}; got {value}")
    return value


def env_float(
    name: str,
    default: float,
    *,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> float:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else float(raw)
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}; got {value}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be <= {maximum}; got {value}")
    return value


def parse_clock(value: str) -> dtime:
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(value.strip(), fmt).time()
        except ValueError:
            continue
    raise ValueError(f"Invalid clock time {value!r}; expected HH:MM[:SS]")


def parse_db_synchronous(value: str) -> str:
    parsed = value.strip().upper()
    allowed = {"OFF", "NORMAL", "FULL", "EXTRA"}
    if parsed not in allowed:
        raise ValueError(f"DB_SYNCHRONOUS must be one of {sorted(allowed)}")
    return parsed


SESSION_START = parse_clock(os.getenv("SESSION_START", "09:15:00"))
SESSION_END = parse_clock(os.getenv("SESSION_END", "15:30:00"))
if SESSION_START >= SESSION_END:
    raise ValueError("SESSION_START must be earlier than SESSION_END")

PRECONNECT_SECONDS = env_int("PRECONNECT_SECONDS", 5, minimum=0, maximum=300)
ATM_WINGS = env_int("ATM_WINGS", 10, minimum=0, maximum=100)
SUBSCRIPTION_BUFFER_WINGS = env_int(
    "SUBSCRIPTION_BUFFER_WINGS", 1, minimum=0, maximum=20
)
PREOPEN_EXTRA_WINGS = env_int("PREOPEN_EXTRA_WINGS", 10, minimum=0, maximum=100)
STRICT_OPTION_BAND = env_bool("STRICT_OPTION_BAND", True)
SAVE_ONLY_PRICE_CHANGES = env_bool("SAVE_ONLY_PRICE_CHANGES", True)
REFRESH_INSTRUMENT_CACHE = env_bool("REFRESH_INSTRUMENT_CACHE", False)
ALLOW_WEEKEND_RUN = env_bool("ALLOW_WEEKEND_RUN", False)
VACUUM_ON_CLOSE = env_bool("VACUUM_ON_CLOSE", False)

MAX_API_ATTEMPTS = env_int("MAX_API_ATTEMPTS", 6, minimum=1, maximum=20)
API_RETRY_BASE_SEC = env_float("API_RETRY_BASE_SEC", 1.5, minimum=0.1, maximum=30.0)
WS_OPERATION_ATTEMPTS = env_int("WS_OPERATION_ATTEMPTS", 4, minimum=1, maximum=10)
WS_OPERATION_TIMEOUT_SEC = env_float(
    "WS_OPERATION_TIMEOUT_SEC", 10.0, minimum=1.0, maximum=60.0
)
WS_MAX_RETRIES = env_int("WS_MAX_RETRIES", 300, minimum=1, maximum=300)
WS_MAX_DELAY_SEC = env_int("WS_MAX_DELAY_SEC", 60, minimum=5, maximum=300)

DB_SYNCHRONOUS = parse_db_synchronous(os.getenv("DB_SYNCHRONOUS", "FULL"))
DB_BUSY_TIMEOUT_MS = env_int("DB_BUSY_TIMEOUT_MS", 30_000, minimum=1000, maximum=300_000)
DB_FLUSH_INTERVAL_SEC = env_float(
    "DB_FLUSH_INTERVAL_SEC", 0.50, minimum=0.05, maximum=10.0
)
DB_BATCH_TICKS = env_int("DB_BATCH_TICKS", 5000, minimum=1, maximum=100_000)
DB_WRITE_ATTEMPTS = env_int("DB_WRITE_ATTEMPTS", 3, minimum=1, maximum=10)
QUEUE_MAX_FRAMES = env_int("QUEUE_MAX_FRAMES", 20_000, minimum=100, maximum=200_000)
QUEUE_PUT_TIMEOUT_SEC = env_float(
    "QUEUE_PUT_TIMEOUT_SEC", 0.25, minimum=0.0, maximum=5.0
)
WAL_AUTOCHECKPOINT_PAGES = env_int(
    "WAL_AUTOCHECKPOINT_PAGES", 1000, minimum=100, maximum=50_000
)
JOURNAL_SIZE_LIMIT_MB = env_int("JOURNAL_SIZE_LIMIT_MB", 16, minimum=1, maximum=256)
SIZE_WARNING_MB = env_int("SIZE_WARNING_MB", 90, minimum=10, maximum=10_000)

NO_TICK_WARNING_SEC = env_int("NO_TICK_WARNING_SEC", 30, minimum=5, maximum=600)
NO_CONNECTION_WARNING_SEC = env_int(
    "NO_CONNECTION_WARNING_SEC", 30, minimum=5, maximum=600
)

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./kite_spike_data")).expanduser().resolve()
CACHE_DIR = OUTPUT_DIR / "instrument_cache"
LOG_DIR = OUTPUT_DIR / "logs"

SUPPORTED_INDEXES = ("NIFTY", "SENSEX", "BANKNIFTY")
TARGET_INDEXES = tuple(
    dict.fromkeys(
        item.strip().upper()
        for item in os.getenv("TARGET_INDEXES", "NIFTY,SENSEX").split(",")
        if item.strip()
    )
)
if not TARGET_INDEXES:
    raise ValueError("TARGET_INDEXES cannot be empty")
for target in TARGET_INDEXES:
    if target not in SUPPORTED_INDEXES:
        raise ValueError(
            f"Unsupported index {target!r}. Allowed: {', '.join(SUPPORTED_INDEXES)}"
        )


@dataclass(frozen=True)
class IndexConfig:
    key: str
    index_id: int
    index_exchange: str
    index_tradingsymbol: str
    option_exchange: str
    option_name: str
    strike_step: int


INDEX_CONFIGS: Dict[str, IndexConfig] = {
    "NIFTY": IndexConfig("NIFTY", 1, "NSE", "NIFTY 50", "NFO", "NIFTY", 50),
    "SENSEX": IndexConfig("SENSEX", 2, "BSE", "SENSEX", "BFO", "SENSEX", 100),
    "BANKNIFTY": IndexConfig(
        "BANKNIFTY", 3, "NSE", "NIFTY BANK", "NFO", "BANKNIFTY", 100
    ),
}


# =============================================================================
# Logging, lock and small helpers
# =============================================================================


class ISTFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        current = datetime.fromtimestamp(record.created, tz=IST)
        return current.strftime(datefmt) if datefmt else current.isoformat(timespec="milliseconds")


def configure_logging(trading_day: date) -> Path:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / f"kite_spikes_v3_{trading_day:%Y%m%d}.log"
    formatter = ISTFormatter(
        "%(asctime)s.%(msecs)03d %(levelname)-8s %(threadName)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    stream = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler(path, encoding="utf-8")
    stream.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.INFO)
    root.addHandler(stream)
    root.addHandler(file_handler)
    return path


class SingleInstanceLock:
    """Cross-platform advisory lock for one collector per trading-day database."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle: Optional[Any] = None

    def __enter__(self) -> "SingleInstanceLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+b")
        self.handle.seek(0)
        if not self.handle.read(1):
            self.handle.seek(0)
            self.handle.write(b"0")
            self.handle.flush()
        self.handle.seek(0)
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, IOError) as exc:
            self.handle.close()
            self.handle = None
            raise RuntimeError(f"Another collector is using {self.path}") from exc
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def now_ist() -> datetime:
    return datetime.now(IST)


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def price_to_paise(price: Any) -> Optional[int]:
    value = safe_float(price)
    if value is None or value <= 0:
        return None
    return int(math.floor(value * PRICE_SCALE + 0.5))


def normalize_expiry(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        raw = value.strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(raw).date()
        except ValueError:
            pass
    raise ValueError(f"Cannot parse expiry value: {value!r}")


def parse_expiry_override(index_key: str) -> Optional[date]:
    raw = os.getenv(f"EXPIRY_{index_key}", "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"EXPIRY_{index_key} must be YYYY-MM-DD or DD-MM-YYYY")


def round_to_atm(price: float, step: int) -> int:
    if price <= 0:
        raise ValueError(f"Price must be positive; got {price}")
    return int(math.floor(price / step + 0.5) * step)


def seconds_since_midnight(value: datetime) -> int:
    local = value.astimezone(IST)
    return local.hour * 3600 + local.minute * 60 + local.second


def packed_key(second_of_day: int, instrument_token: int) -> int:
    if not 0 <= second_of_day <= 86_399:
        raise ValueError(f"Invalid second_of_day: {second_of_day}")
    if not 0 <= instrument_token <= TOKEN_MASK:
        raise ValueError(f"Instrument token outside uint32 range: {instrument_token}")
    return (second_of_day << 32) | instrument_token


def package_version_or_unknown(name: str) -> str:
    try:
        return package_version(name)
    except PackageNotFoundError:
        return "unknown"


def compact_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False, default=str)


# =============================================================================
# Bounded REST retries and daily instrument cache
# =============================================================================


def is_retryable_exception(exc: BaseException) -> bool:
    non_retryable = (
        kite_exceptions.TokenException,
        kite_exceptions.PermissionException,
        kite_exceptions.InputException,
    )
    if isinstance(exc, non_retryable):
        return False
    code = getattr(exc, "code", None)
    return not (isinstance(code, int) and 400 <= code < 500 and code != 429)


def retry_call(
    label: str,
    function: Callable[[], T],
    *,
    attempts: int = MAX_API_ATTEMPTS,
    base_delay: float = API_RETRY_BASE_SEC,
    max_delay: float = 30.0,
) -> T:
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            result = function()
            if attempt > 1:
                logging.info("%s succeeded on attempt %d/%d", label, attempt, attempts)
            return result
        except Exception as exc:
            last_error = exc
            if not is_retryable_exception(exc):
                raise RuntimeError(f"{label} failed with non-retryable error: {exc}") from exc
            if attempt >= attempts:
                break
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay += random.uniform(0.0, min(1.0, delay * 0.20))
            logging.warning(
                "%s failed on attempt %d/%d: %s; retrying in %.2fs",
                label,
                attempt,
                attempts,
                exc,
                delay,
            )
            time.sleep(delay)
    raise RuntimeError(f"{label} failed after {attempts} attempts") from last_error


def atomic_gzip_pickle_dump(value: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=5) as zipped:
            pickle.dump(value, zipped, protocol=pickle.HIGHEST_PROTOCOL)
        raw.flush()
        os.fsync(raw.fileno())
    os.replace(temporary, path)


def read_gzip_pickle(path: Path) -> Any:
    with gzip.open(path, "rb") as handle:
        return pickle.load(handle)


def validate_instrument_rows(value: Any, exchange: str) -> List[Dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise RuntimeError(f"Invalid/empty instrument data for {exchange}")
    first = value[0]
    if not isinstance(first, Mapping) or "instrument_token" not in first:
        raise RuntimeError(f"Malformed instrument data for {exchange}")
    return value


def load_instruments_with_cache(
    kite: Any,
    exchange: str,
    trading_day: date,
) -> List[Dict[str, Any]]:
    exchange = exchange.upper().strip()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"{exchange}_{trading_day:%Y%m%d}.pkl.gz"

    if cache_path.exists() and not REFRESH_INSTRUMENT_CACHE:
        try:
            rows = validate_instrument_rows(read_gzip_pickle(cache_path), exchange)
            logging.info("Loaded %d %s instruments from %s", len(rows), exchange, cache_path)
            return rows
        except Exception:
            logging.exception("Ignoring corrupt instrument cache %s", cache_path)

    try:
        rows = retry_call(
            f"kite.instruments({exchange})",
            lambda: kite.instruments(exchange),
        )
        rows = validate_instrument_rows(rows, exchange)
        atomic_gzip_pickle_dump(rows, cache_path)
        logging.info("Downloaded %d %s instruments", len(rows), exchange)
        return rows
    except Exception:
        if cache_path.exists():
            logging.exception("Instrument API failed; using same-day cache %s", cache_path)
            return validate_instrument_rows(read_gzip_pickle(cache_path), exchange)
        raise


# =============================================================================
# Instrument universe
# =============================================================================


@dataclass(frozen=True)
class InstrumentMeta:
    instrument_token: int
    index_key: str
    index_id: int
    exchange: str
    tradingsymbol: str
    instrument_kind: str  # UNDERLYING or OPTION
    expiry: Optional[date]
    strike: Optional[int]
    option_type: str
    strike_step: int
    lot_size: Optional[int]


@dataclass
class IndexRuntime:
    config: IndexConfig
    expiry: date
    underlying: InstrumentMeta
    option_by_strike_type: Dict[Tuple[int, str], InstrumentMeta]

    def option_band(
        self,
        center: int,
        wings: int,
    ) -> Tuple[List[InstrumentMeta], List[Tuple[int, str]]]:
        found: List[InstrumentMeta] = []
        missing: List[Tuple[int, str]] = []
        for offset in range(-wings, wings + 1):
            strike = center + offset * self.config.strike_step
            for option_type in ("CE", "PE"):
                meta = self.option_by_strike_type.get((strike, option_type))
                if meta is None:
                    missing.append((strike, option_type))
                else:
                    found.append(meta)
        return found, missing


def symbol_matches_option_root(tradingsymbol: str, root: str) -> bool:
    symbol = tradingsymbol.upper().strip()
    root_u = root.upper().strip()
    return (
        symbol.startswith(root_u)
        and len(symbol) > len(root_u)
        and symbol[len(root_u)].isdigit()
    )


def find_underlying(rows: Sequence[Mapping[str, Any]], config: IndexConfig) -> InstrumentMeta:
    wanted = config.index_tradingsymbol.upper()
    for row in rows:
        if str(row.get("tradingsymbol", "")).upper() != wanted:
            continue
        return InstrumentMeta(
            instrument_token=int(row["instrument_token"]),
            index_key=config.key,
            index_id=config.index_id,
            exchange=str(row.get("exchange") or config.index_exchange),
            tradingsymbol=str(row["tradingsymbol"]),
            instrument_kind="UNDERLYING",
            expiry=None,
            strike=None,
            option_type="",
            strike_step=config.strike_step,
            lot_size=safe_int(row.get("lot_size")),
        )
    raise RuntimeError(
        f"Underlying not found: {config.index_exchange}:{config.index_tradingsymbol}"
    )


def build_index_runtime(
    config: IndexConfig,
    index_rows: Sequence[Mapping[str, Any]],
    option_rows: Sequence[Mapping[str, Any]],
    trading_day: date,
) -> IndexRuntime:
    underlying = find_underlying(index_rows, config)
    override = parse_expiry_override(config.key)

    candidates: List[Tuple[date, Mapping[str, Any]]] = []
    for row in option_rows:
        option_type = str(row.get("instrument_type", "")).upper()
        if option_type not in {"CE", "PE"}:
            continue
        name = str(row.get("name", "")).upper().strip()
        symbol = str(row.get("tradingsymbol", "")).upper().strip()
        if name:
            if name != config.option_name:
                continue
        elif not symbol_matches_option_root(symbol, config.option_name):
            continue
        try:
            expiry = normalize_expiry(row.get("expiry"))
        except Exception:
            continue
        if expiry >= trading_day:
            candidates.append((expiry, row))

    expiries = sorted({expiry for expiry, _ in candidates})
    if not expiries:
        raise RuntimeError(f"No non-expired {config.key} option expiry found")
    selected_expiry = override or expiries[0]
    if selected_expiry not in expiries:
        preview = ", ".join(str(item) for item in expiries[:10])
        raise RuntimeError(
            f"EXPIRY_{config.key}={selected_expiry} absent. Available: {preview}"
        )

    option_map: Dict[Tuple[int, str], InstrumentMeta] = {}
    for expiry, row in candidates:
        if expiry != selected_expiry:
            continue
        strike_float = safe_float(row.get("strike"))
        if strike_float is None:
            continue
        strike = int(round(strike_float))
        option_type = str(row.get("instrument_type", "")).upper()
        meta = InstrumentMeta(
            instrument_token=int(row["instrument_token"]),
            index_key=config.key,
            index_id=config.index_id,
            exchange=str(row.get("exchange") or config.option_exchange),
            tradingsymbol=str(row["tradingsymbol"]),
            instrument_kind="OPTION",
            expiry=expiry,
            strike=strike,
            option_type=option_type,
            strike_step=config.strike_step,
            lot_size=safe_int(row.get("lot_size")),
        )
        key = (strike, option_type)
        existing = option_map.get(key)
        if existing and existing.instrument_token != meta.instrument_token:
            raise RuntimeError(
                f"Duplicate {config.key} {strike}{option_type}: "
                f"{existing.tradingsymbol} and {meta.tradingsymbol}"
            )
        option_map[key] = meta

    if not option_map:
        raise RuntimeError(f"No {config.key} contracts built for {selected_expiry}")

    logging.info(
        "%s: underlying=%s token=%d expiry=%s contracts=%d",
        config.key,
        underlying.tradingsymbol,
        underlying.instrument_token,
        selected_expiry,
        len(option_map),
    )
    return IndexRuntime(config, selected_expiry, underlying, option_map)


# =============================================================================
# Compact price messages and one-second accumulation
# =============================================================================


@dataclass(frozen=True)
class CompactTick:
    second_of_day: int
    instrument_token: int
    price_paise: int
    strike_offset: int
    stream_epoch: int
    instrument_kind: str


@dataclass
class CompactBar:
    second_of_day: int
    instrument_token: int
    previous_paise: Optional[int]
    open_paise: int
    high_paise: int
    low_paise: int
    close_paise: int
    strike_offset: int
    flags: int

    @classmethod
    def from_tick(
        cls,
        tick: CompactTick,
        previous_paise: Optional[int],
        flags: int,
    ) -> "CompactBar":
        price = tick.price_paise
        return cls(
            second_of_day=tick.second_of_day,
            instrument_token=tick.instrument_token,
            previous_paise=previous_paise,
            open_paise=price,
            high_paise=price,
            low_paise=price,
            close_paise=price,
            strike_offset=tick.strike_offset,
            flags=flags,
        )

    def update(self, tick: CompactTick, flags: int) -> None:
        price = tick.price_paise
        self.high_paise = max(self.high_paise, price)
        self.low_paise = min(self.low_paise, price)
        self.close_paise = price
        self.strike_offset = tick.strike_offset
        self.flags |= flags

    def as_row(self) -> Tuple[Any, ...]:
        return (
            packed_key(self.second_of_day, self.instrument_token),
            self.previous_paise,
            self.open_paise,
            self.high_paise,
            self.low_paise,
            self.close_paise,
            self.strike_offset,
            self.flags,
        )


COMPACT_BAR_UPSERT_SQL = """
INSERT INTO bars(k,p,o,h,l,c,off,f)
VALUES (?,?,?,?,?,?,?,?)
ON CONFLICT(k) DO UPDATE SET
    p   = COALESCE(bars.p, excluded.p),
    o   = bars.o,
    h   = MAX(bars.h, excluded.h),
    l   = MIN(bars.l, excluded.l),
    c   = excluded.c,
    off = excluded.off,
    f   = bars.f | excluded.f
"""


# =============================================================================
# Compact restart-safe SQLite store
# =============================================================================


class CompactSQLiteStore:
    """Single-writer compact store.

    ``bars`` deliberately contains no token/time text columns and no secondary
    indexes. The 64-bit key layout is:

        upper bits = second_of_day
        lower 32 bits = instrument_token

    The readable SQL view decodes the key and joins the tiny instrument table.
    """

    def __init__(self, db_path: Path, trading_day: date) -> None:
        self.db_path = db_path
        self.trading_day = trading_day
        self.frame_queue: "queue.Queue[Optional[List[CompactTick]]]" = queue.Queue(
            maxsize=QUEUE_MAX_FRAMES
        )
        self.writer_stop_event = threading.Event()
        self.fatal_event = threading.Event()
        self.writer_thread: Optional[threading.Thread] = None

        self.last_price: Dict[int, int] = {}
        self.last_epoch: Dict[int, int] = {}
        self.received_ticks = 0
        self.price_change_ticks = 0
        self.anchor_ticks = 0
        self.omitted_unchanged_ticks = 0
        self.rows_upserted = 0
        self.queue_overflows = 0
        self.max_queue_depth = 0
        self._last_size_warning = 0.0

        self._initialise_database()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=DB_BUSY_TIMEOUT_MS / 1000.0,
            isolation_level=None,
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(f"PRAGMA synchronous={DB_SYNCHRONOUS}")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute(f"PRAGMA busy_timeout={DB_BUSY_TIMEOUT_MS}")
        conn.execute(f"PRAGMA wal_autocheckpoint={WAL_AUTOCHECKPOINT_PAGES}")
        conn.execute(f"PRAGMA journal_size_limit={JOURNAL_SIZE_LIMIT_MB * 1024 * 1024}")
        conn.execute("PRAGMA cache_size=-32768")
        return conn

    def _initialise_database(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        is_new = not self.db_path.exists()
        conn = sqlite3.connect(str(self.db_path))
        try:
            if is_new:
                # page_size is honoured only before the first table is created.
                conn.execute("PRAGMA page_size=4096")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS run_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                ) WITHOUT ROWID;

                CREATE TABLE IF NOT EXISTS instruments (
                    token INTEGER PRIMARY KEY,
                    index_name TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    kind INTEGER NOT NULL,
                    option_type TEXT NOT NULL,
                    strike INTEGER,
                    expiry TEXT,
                    strike_step INTEGER NOT NULL,
                    lot_size INTEGER
                );

                CREATE TABLE IF NOT EXISTS feed_events (
                    id INTEGER PRIMARY KEY,
                    event_time TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    details TEXT
                );

                -- Large table: intentionally only one INTEGER primary key and
                -- seven compact integer payload columns. INTEGER PRIMARY KEY is
                -- the SQLite rowid, so no duplicate primary-key index is created.
                CREATE TABLE IF NOT EXISTS bars (
                    k INTEGER PRIMARY KEY,
                    p INTEGER,
                    o INTEGER NOT NULL,
                    h INTEGER NOT NULL,
                    l INTEGER NOT NULL,
                    c INTEGER NOT NULL,
                    off INTEGER NOT NULL,
                    f INTEGER NOT NULL DEFAULT 0
                );

                DROP VIEW IF EXISTS option_1sec_data;
                CREATE VIEW option_1sec_data AS
                WITH decoded AS (
                    SELECT
                        (k >> 32) AS second_of_day,
                        (k & 4294967295) AS instrument_token,
                        p,o,h,l,c,off,f
                    FROM bars
                ), day_value AS (
                    SELECT value AS trading_day
                    FROM run_metadata
                    WHERE key='trading_day'
                )
                SELECT
                    printf(
                        '%sT%02d:%02d:%02d+05:30',
                        day_value.trading_day,
                        CAST(decoded.second_of_day / 3600 AS INTEGER),
                        CAST((decoded.second_of_day % 3600) / 60 AS INTEGER),
                        CAST(decoded.second_of_day % 60 AS INTEGER)
                    ) AS date,
                    instruments.symbol AS instrument,
                    instruments.exchange,
                    instruments.index_name AS name,
                    CASE instruments.kind WHEN 1 THEN 'UNDERLYING' ELSE 'OPTION' END AS type,
                    instruments.option_type,
                    instruments.strike,
                    instruments.expiry,
                    decoded.off AS strike_offset,
                    CASE
                        WHEN instruments.kind=2 AND instruments.strike IS NOT NULL
                        THEN instruments.strike - decoded.off * instruments.strike_step
                    END AS atm_strike,
                    decoded.p / 100.0 AS previous_price,
                    decoded.o / 100.0 AS open,
                    decoded.h / 100.0 AS high,
                    decoded.l / 100.0 AS low,
                    decoded.c / 100.0 AS close,
                    (decoded.f & 1) != 0 AS is_anchor,
                    (decoded.f & 4) != 0 AS is_reconnect_anchor,
                    decoded.second_of_day,
                    decoded.instrument_token,
                    decoded.f AS flags
                FROM decoded
                JOIN instruments ON instruments.token=decoded.instrument_token
                CROSS JOIN day_value;
                """
            )
            conn.commit()
        finally:
            conn.close()

        # Apply WAL settings after schema creation.
        conn = self._connect()
        conn.close()

    def set_metadata_many(self, values: Mapping[str, Any]) -> None:
        rows = [(str(k), str(v)) for k, v in values.items()]
        if not rows:
            return
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                """
                INSERT INTO run_metadata(key,value) VALUES (?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                rows,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_metadata(self, key: str) -> Optional[str]:
        """Read one small persisted runtime value, such as the last known ATM."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT value FROM run_metadata WHERE key=?",
                (key,),
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()

    def upsert_instruments(self, instruments: Iterable[InstrumentMeta]) -> None:
        rows = []
        for meta in instruments:
            rows.append(
                (
                    meta.instrument_token,
                    meta.index_key,
                    meta.exchange,
                    meta.tradingsymbol,
                    1 if meta.instrument_kind == "UNDERLYING" else 2,
                    meta.option_type,
                    meta.strike,
                    meta.expiry.isoformat() if meta.expiry else None,
                    meta.strike_step,
                    meta.lot_size,
                )
            )
        if not rows:
            return
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.executemany(
                """
                INSERT INTO instruments(
                    token,index_name,exchange,symbol,kind,option_type,
                    strike,expiry,strike_step,lot_size
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(token) DO UPDATE SET
                    index_name=excluded.index_name,
                    exchange=excluded.exchange,
                    symbol=excluded.symbol,
                    kind=excluded.kind,
                    option_type=excluded.option_type,
                    strike=excluded.strike,
                    expiry=excluded.expiry,
                    strike_step=excluded.strike_step,
                    lot_size=excluded.lot_size
                """,
                rows,
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def record_feed_event(self, event_type: str, details: Optional[Mapping[str, Any]] = None) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO feed_events(event_time,event_type,details) VALUES (?,?,?)",
                (
                    now_ist().isoformat(timespec="seconds"),
                    event_type,
                    compact_json(details) if details else None,
                ),
            )
        finally:
            conn.close()

    def start_writer(self) -> None:
        if self.writer_thread is not None:
            return
        self.writer_thread = threading.Thread(
            target=self._writer_loop,
            name="compact-sqlite-writer",
            daemon=True,
        )
        self.writer_thread.start()

    def enqueue(self, ticks: List[CompactTick]) -> bool:
        if not ticks:
            return True
        if self.fatal_event.is_set():
            return False
        try:
            self.frame_queue.put(ticks, timeout=QUEUE_PUT_TIMEOUT_SEC)
            self.received_ticks += len(ticks)
            self.max_queue_depth = max(self.max_queue_depth, self.frame_queue.qsize())
            return True
        except queue.Full:
            self.queue_overflows += 1
            self.fatal_event.set()
            logging.critical(
                "Writer queue overflow: dropping %d ticks and stopping to avoid silent gaps",
                len(ticks),
            )
            return False

    def stop_writer(self) -> None:
        self.writer_stop_event.set()
        try:
            self.frame_queue.put(None, timeout=2.0)
        except queue.Full:
            pass
        if self.writer_thread is not None:
            self.writer_thread.join(timeout=60.0)
            if self.writer_thread.is_alive():
                logging.error("SQLite writer did not stop within 60 seconds")

        try:
            conn = self._connect()
            try:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                conn.execute("PRAGMA optimize")
                if VACUUM_ON_CLOSE:
                    logging.info("VACUUM_ON_CLOSE=true; compacting final database")
                    conn.execute("VACUUM")
            finally:
                conn.close()
        except Exception:
            logging.exception("Final SQLite checkpoint/optimise failed")

    def _writer_loop(self) -> None:
        conn = self._connect()
        pending: List[CompactTick] = []
        last_flush = time.monotonic()
        try:
            while True:
                timeout = max(0.05, DB_FLUSH_INTERVAL_SEC - (time.monotonic() - last_flush))
                try:
                    item = self.frame_queue.get(timeout=timeout)
                except queue.Empty:
                    item = []

                if item is None:
                    if pending:
                        self._flush_with_retries(conn, pending)
                    break
                if item:
                    pending.extend(item)

                due_size = len(pending) >= DB_BATCH_TICKS
                due_time = pending and time.monotonic() - last_flush >= DB_FLUSH_INTERVAL_SEC
                if due_size or due_time:
                    self._flush_with_retries(conn, pending)
                    pending.clear()
                    last_flush = time.monotonic()
                    self._warn_on_size()

                if self.writer_stop_event.is_set() and self.frame_queue.empty():
                    if pending:
                        self._flush_with_retries(conn, pending)
                    break
        except Exception:
            logging.exception("Fatal compact SQLite writer failure")
            self.fatal_event.set()
        finally:
            conn.close()

    def _flush_with_retries(
        self,
        conn: sqlite3.Connection,
        ticks: Sequence[CompactTick],
    ) -> None:
        last_error: Optional[BaseException] = None
        for attempt in range(1, DB_WRITE_ATTEMPTS + 1):
            try:
                rows = self._build_rows(ticks)
                if rows:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.executemany(COMPACT_BAR_UPSERT_SQL, rows)
                    conn.commit()
                    self.rows_upserted += len(rows)
                return
            except Exception as exc:
                last_error = exc
                try:
                    conn.rollback()
                except Exception:
                    pass
                if attempt >= DB_WRITE_ATTEMPTS:
                    break
                delay = min(2.0, 0.20 * (2 ** (attempt - 1)))
                logging.warning(
                    "SQLite batch failed %d/%d: %s; retrying in %.2fs",
                    attempt,
                    DB_WRITE_ATTEMPTS,
                    exc,
                    delay,
                )
                time.sleep(delay)
        raise RuntimeError(
            f"SQLite batch failed after {DB_WRITE_ATTEMPTS} attempts"
        ) from last_error

    def _build_rows(self, ticks: Sequence[CompactTick]) -> List[Tuple[Any, ...]]:
        """Convert ordered ticks into price-change bars.

        Duplicate/replayed packets need no separate event table: a repeated price
        does not alter OHLC, and no volume/tick counter is stored. This removes the
        largest table in the old schema without compromising spike high/low.
        """

        bars: Dict[Tuple[int, int], CompactBar] = {}
        for tick in ticks:
            token = tick.instrument_token
            previous = self.last_price.get(token)
            old_epoch = self.last_epoch.get(token)
            epoch_changed = old_epoch is None or old_epoch != tick.stream_epoch
            is_anchor = previous is None or epoch_changed
            price_changed = previous is not None and tick.price_paise != previous

            flags = 0
            if is_anchor:
                flags |= FLAG_ANCHOR
                self.anchor_ticks += 1
                if old_epoch is not None and epoch_changed:
                    flags |= FLAG_RECONNECT_ANCHOR
            if tick.instrument_kind == "UNDERLYING":
                flags |= FLAG_UNDERLYING

            # Always update stream state, even when the row is omitted.
            self.last_price[token] = tick.price_paise
            self.last_epoch[token] = tick.stream_epoch

            should_store = is_anchor or price_changed or not SAVE_ONLY_PRICE_CHANGES
            if not should_store:
                self.omitted_unchanged_ticks += 1
                continue
            if price_changed:
                self.price_change_ticks += 1

            key = (tick.second_of_day, token)
            bar = bars.get(key)
            if bar is None:
                bars[key] = CompactBar.from_tick(
                    tick=tick,
                    previous_paise=None if is_anchor else previous,
                    flags=flags,
                )
            else:
                bar.update(tick, flags)

        return [bar.as_row() for bar in bars.values()]

    def _warn_on_size(self) -> None:
        now_mono = time.monotonic()
        if now_mono - self._last_size_warning < 300:
            return
        size_mb = self.total_disk_bytes() / (1024 * 1024)
        if size_mb >= SIZE_WARNING_MB:
            self._last_size_warning = now_mono
            logging.warning(
                "Database plus WAL is %.1f MB (warning threshold %d MB)",
                size_mb,
                SIZE_WARNING_MB,
            )

    def total_disk_bytes(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            path = Path(str(self.db_path) + suffix)
            if path.exists():
                total += path.stat().st_size
        return total

    def counts(self) -> Dict[str, int]:
        conn = self._connect()
        try:
            return {
                "bars": int(conn.execute("SELECT COUNT(*) FROM bars").fetchone()[0]),
                "instruments": int(
                    conn.execute("SELECT COUNT(*) FROM instruments").fetchone()[0]
                ),
            }
        finally:
            conn.close()


# =============================================================================
# Live WebSocket collector
# =============================================================================


class KiteOptionSpikeCollector:
    def __init__(
        self,
        kite: Any,
        runtimes: Mapping[str, IndexRuntime],
        store: CompactSQLiteStore,
        market_open: datetime,
        market_close: datetime,
    ) -> None:
        self.kite = kite
        self.runtimes = dict(runtimes)
        self.store = store
        self.market_open = market_open
        self.market_close = market_close

        api_key = getattr(kite, "api_key", None) or os.getenv("KITE_API_KEY")
        access_token = getattr(kite, "access_token", None) or os.getenv("KITE_ACCESS_TOKEN")
        if not api_key or not access_token:
            raise RuntimeError("Authenticated Kite object lacks api_key/access_token")

        self.kws = KiteTicker(
            str(api_key),
            str(access_token),
            reconnect=True,
            reconnect_max_tries=WS_MAX_RETRIES,
            reconnect_max_delay=WS_MAX_DELAY_SEC,
        )

        self.stop_event = threading.Event()
        self.connected_event = threading.Event()
        self.refresh_event = threading.Event()
        self.state_lock = threading.RLock()
        self.shutdown_lock = threading.Lock()
        self.shutdown_done = False

        self.latest_underlying_price: Dict[str, float] = {}
        self.actual_atm_by_index: Dict[str, Optional[int]] = {
            key: None for key in self.runtimes
        }
        self.subscription_center_by_index: Dict[str, Optional[int]] = {
            key: None for key in self.runtimes
        }
        self.has_live_underlying_tick: Dict[str, bool] = {
            key: False for key in self.runtimes
        }
        self.subscribed_option_tokens: Set[int] = set()

        self.stream_epoch_counter = 0
        self.token_stream_epoch: Dict[int, int] = {}
        self.connection_sequence = 0

        self.last_tick_monotonic: Optional[float] = None
        self.last_warning_monotonic = 0.0
        self.start_monotonic = time.monotonic()
        self.frames_received = 0
        self.ticks_received = 0
        self.ticks_eligible = 0
        self.buffer_ticks_ignored = 0
        self.unknown_ticks_ignored = 0

        self.token_meta: Dict[int, InstrumentMeta] = {}
        self.underlying_token_to_index: Dict[int, str] = {}
        for key, runtime in self.runtimes.items():
            self.token_meta[runtime.underlying.instrument_token] = runtime.underlying
            self.underlying_token_to_index[runtime.underlying.instrument_token] = key
            for meta in runtime.option_by_strike_type.values():
                self.token_meta[meta.instrument_token] = meta

        self.subscription_thread = threading.Thread(
            target=self._subscription_loop,
            name="subscription-manager",
            daemon=True,
        )

        self.kws.on_connect = self._on_connect
        self.kws.on_open = self._on_open
        self.kws.on_ticks = self._on_ticks
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error
        self.kws.on_reconnect = self._on_reconnect
        self.kws.on_noreconnect = self._on_noreconnect

    def seed_atm_values(self) -> None:
        instrument_names = [
            f"{runtime.config.index_exchange}:{runtime.config.index_tradingsymbol}"
            for runtime in self.runtimes.values()
        ]
        quotes: Dict[str, Any] = {}
        try:
            quotes = retry_call(
                "kite.ltp(underlyings)",
                lambda: self.kite.ltp(instrument_names),
            )
        except Exception:
            logging.exception("Initial underlying LTP unavailable; waiting for live ticks")

        with self.state_lock:
            for key, runtime in self.runtimes.items():
                quote_key = (
                    f"{runtime.config.index_exchange}:"
                    f"{runtime.config.index_tradingsymbol}"
                )
                price = safe_float((quotes.get(quote_key) or {}).get("last_price"))
                if price is not None and price > 0:
                    atm = round_to_atm(price, runtime.config.strike_step)
                    self.latest_underlying_price[key] = price
                    self.actual_atm_by_index[key] = atm
                    self.subscription_center_by_index[key] = atm
                    logging.info("Seeded %s %.2f -> ATM %d", key, price, atm)
                    continue

                persisted = self.store.get_metadata(f"atm:{key}")
                if persisted is not None:
                    try:
                        atm = int(persisted)
                    except ValueError:
                        logging.warning("Ignoring invalid persisted ATM %s=%r", key, persisted)
                    else:
                        self.actual_atm_by_index[key] = atm
                        self.subscription_center_by_index[key] = atm
                        logging.warning("Using persisted %s ATM %d until live tick", key, atm)
        self.refresh_event.set()

    def start(self) -> None:
        self.store.start_writer()
        self.subscription_thread.start()
        self.seed_atm_values()
        logging.info("Starting KiteTicker in threaded mode")
        self.kws.connect(threaded=True)

    def stop(self) -> None:
        with self.shutdown_lock:
            if self.shutdown_done:
                return
            self.shutdown_done = True

        self.stop_event.set()
        self.refresh_event.set()
        logging.info("Stopping KiteTicker and flushing compact database")
        try:
            self.store.record_feed_event("SHUTDOWN_REQUESTED")
        except Exception:
            logging.debug("Could not record shutdown event", exc_info=True)

        try:
            if reactor.running and hasattr(self.kws, "close"):
                self._call_on_reactor_thread(lambda: self.kws.close())
            elif hasattr(self.kws, "close"):
                self.kws.close()
        except Exception:
            logging.exception("KiteTicker close failed")
        try:
            if reactor.running and hasattr(self.kws, "stop"):
                reactor.callFromThread(self.kws.stop)
            elif hasattr(self.kws, "stop"):
                self.kws.stop()
        except Exception:
            logging.debug("KiteTicker stop raised", exc_info=True)

        if self.subscription_thread.is_alive():
            self.subscription_thread.join(timeout=15.0)
        self.store.stop_writer()

    def run_until_close(self) -> None:
        self.start()
        try:
            while not self.stop_event.is_set():
                if self.store.fatal_event.is_set():
                    raise RuntimeError("Compact writer/queue entered fatal state")
                current = now_ist()
                if current >= self.market_close:
                    logging.info("Session close reached: %s", self.market_close)
                    break
                self._warn_if_feed_unhealthy(current)
                time.sleep(1.0)
        finally:
            self.stop()

    def _warn_if_feed_unhealthy(self, current: datetime) -> None:
        if current < self.market_open + timedelta(seconds=10):
            return
        now_mono = time.monotonic()
        if not self.connected_event.is_set():
            stale = (
                now_mono - self.start_monotonic
                if self.last_tick_monotonic is None
                else now_mono - self.last_tick_monotonic
            )
            if (
                stale >= NO_CONNECTION_WARNING_SEC
                and now_mono - self.last_warning_monotonic >= NO_CONNECTION_WARNING_SEC
            ):
                self.last_warning_monotonic = now_mono
                logging.error("WebSocket disconnected/no live data for about %.1fs", stale)
            return
        stale = (
            float("inf")
            if self.last_tick_monotonic is None
            else now_mono - self.last_tick_monotonic
        )
        if (
            stale >= NO_TICK_WARNING_SEC
            and now_mono - self.last_warning_monotonic >= NO_TICK_WARNING_SEC
        ):
            self.last_warning_monotonic = now_mono
            logging.error("WebSocket open but no tick for %.1fs", stale)

    def _assign_new_stream_epoch(self, tokens: Iterable[int], reason: str) -> int:
        token_list = list(tokens)
        if not token_list:
            return 0
        with self.state_lock:
            self.stream_epoch_counter += 1
            epoch = self.stream_epoch_counter
            for token in token_list:
                self.token_stream_epoch[token] = epoch
        logging.debug("Assigned stream epoch %d to %d tokens: %s", epoch, len(token_list), reason)
        return epoch

    # ------------------------------------------------------------------
    # KiteTicker callbacks: reactor/WebSocket thread
    # ------------------------------------------------------------------

    def _on_connect(self, ws: KiteTicker, response: Any) -> None:
        self.connection_sequence += 1
        reason = "CONNECT" if self.connection_sequence == 1 else "RECONNECT"
        with self.state_lock:
            active = {
                runtime.underlying.instrument_token for runtime in self.runtimes.values()
            }
            active.update(self.subscribed_option_tokens)
            sdk_tokens = getattr(self.kws, "subscribed_tokens", {})
            if isinstance(sdk_tokens, Mapping):
                active.update(int(token) for token in sdk_tokens)
        epoch = self._assign_new_stream_epoch(active, reason)
        logging.info(
            "KiteTicker connected: %s connection=%d epoch=%d response=%s",
            reason,
            self.connection_sequence,
            epoch,
            response,
        )
        try:
            self.store.record_feed_event(
                reason,
                {"connection": self.connection_sequence, "stream_epoch": epoch},
            )
        except Exception:
            logging.debug("Could not record connection event", exc_info=True)

        underlying_tokens = [
            runtime.underlying.instrument_token for runtime in self.runtimes.values()
        ]
        try:
            if ws.subscribe(underlying_tokens) is False:
                raise RuntimeError("Underlying subscribe returned False")
            # Only LTP is required for ATM calculation and underlying spike context.
            if ws.set_mode(ws.MODE_LTP, underlying_tokens) is False:
                raise RuntimeError("Underlying LTP mode returned False")
        except Exception:
            logging.exception("Could not establish underlying subscriptions")
            self.connected_event.clear()
            try:
                ws.close()
            except Exception:
                pass

    def _on_open(self, ws: KiteTicker) -> None:
        self.connected_event.set()
        self.last_tick_monotonic = time.monotonic()
        logging.info("KiteTicker WebSocket open")
        self.refresh_event.set()

    def _on_ticks(self, ws: KiteTicker, ticks: List[Dict[str, Any]]) -> None:
        received_at = now_ist()
        if received_at < self.market_open or received_at > self.market_close + timedelta(seconds=5):
            return
        if not ticks:
            return

        self.last_tick_monotonic = time.monotonic()
        self.frames_received += 1
        self.ticks_received += len(ticks)

        refresh_needed = False
        metadata_changed = False
        with self.state_lock:
            # Update all underlying ATMs before classifying options in this frame.
            for tick in ticks:
                token = safe_int(tick.get("instrument_token"))
                if token is None:
                    continue
                index_key = self.underlying_token_to_index.get(token)
                if index_key is None:
                    continue
                price = safe_float(tick.get("last_price"))
                if price is None or price <= 0:
                    continue

                runtime = self.runtimes[index_key]
                new_atm = round_to_atm(price, runtime.config.strike_step)
                old_atm = self.actual_atm_by_index.get(index_key)
                old_center = self.subscription_center_by_index.get(index_key)
                first_live = not self.has_live_underlying_tick[index_key]

                self.latest_underlying_price[index_key] = price
                self.actual_atm_by_index[index_key] = new_atm
                self.has_live_underlying_tick[index_key] = True

                if old_atm != new_atm:
                    metadata_changed = True
                    logging.info(
                        "%s ATM changed %s -> %d at %.2f",
                        index_key,
                        old_atm,
                        new_atm,
                        price,
                    )

                should_recenter = first_live or old_center is None
                if not should_recenter and new_atm != old_center:
                    displacement = abs(new_atm - old_center) // runtime.config.strike_step
                    # A buffer of N strikes safely covers displacement through N;
                    # recenter only on the next strike. With buffer=0, recenter on
                    # every ATM change.
                    should_recenter = displacement >= SUBSCRIPTION_BUFFER_WINGS + 1

                if first_live:
                    refresh_needed = True
                if should_recenter and old_center != new_atm:
                    self.subscription_center_by_index[index_key] = new_atm
                    refresh_needed = True
                    logging.info(
                        "%s subscription centre %s -> %d",
                        index_key,
                        old_center,
                        new_atm,
                    )

            atm_snapshot = dict(self.actual_atm_by_index)
            epoch_snapshot = dict(self.token_stream_epoch)

        sec = seconds_since_midnight(received_at)
        compact_ticks: List[CompactTick] = []
        for tick in ticks:
            token = safe_int(tick.get("instrument_token"))
            if token is None:
                continue
            meta = self.token_meta.get(token)
            if meta is None:
                self.unknown_ticks_ignored += 1
                continue
            price_paise = price_to_paise(tick.get("last_price"))
            if price_paise is None:
                continue

            if meta.instrument_kind == "OPTION":
                atm = atm_snapshot.get(meta.index_key)
                if atm is None or meta.strike is None:
                    continue
                difference = meta.strike - atm
                if difference % meta.strike_step != 0:
                    logging.error(
                        "Misaligned strike %s versus ATM %s for %s",
                        meta.strike,
                        atm,
                        meta.tradingsymbol,
                    )
                    continue
                offset = difference // meta.strike_step
                if abs(offset) > ATM_WINGS:
                    self.buffer_ticks_ignored += 1
                    continue
            else:
                offset = OFFSET_UNDERLYING

            compact_ticks.append(
                CompactTick(
                    second_of_day=sec,
                    instrument_token=token,
                    price_paise=price_paise,
                    strike_offset=offset,
                    stream_epoch=epoch_snapshot.get(token, 0),
                    instrument_kind=meta.instrument_kind,
                )
            )

        self.ticks_eligible += len(compact_ticks)
        if not self.store.enqueue(compact_ticks):
            self.stop_event.set()
            self.refresh_event.set()
            return

        if refresh_needed or metadata_changed:
            self.refresh_event.set()

    def _on_close(self, ws: KiteTicker, code: int, reason: str) -> None:
        self.connected_event.clear()
        logging.warning("KiteTicker closed: code=%s reason=%s", code, reason)
        try:
            self.store.record_feed_event("CLOSE", {"code": code, "reason": str(reason)})
        except Exception:
            logging.debug("Could not record close event", exc_info=True)
        # Do not call stop() here; it would disable SDK auto-reconnection.

    def _on_error(self, ws: KiteTicker, code: int, reason: str) -> None:
        logging.error("KiteTicker error: code=%s reason=%s", code, reason)
        try:
            self.store.record_feed_event("ERROR", {"code": code, "reason": str(reason)})
        except Exception:
            logging.debug("Could not record error event", exc_info=True)

    def _on_reconnect(self, ws: KiteTicker, attempts_count: int) -> None:
        self.connected_event.clear()
        logging.warning("KiteTicker reconnect attempt %s", attempts_count)

    def _on_noreconnect(self, ws: KiteTicker) -> None:
        logging.critical("KiteTicker exhausted reconnect attempts")
        self.stop_event.set()
        self.refresh_event.set()

    # ------------------------------------------------------------------
    # Dynamic option subscriptions: separate management thread
    # ------------------------------------------------------------------

    def _subscription_loop(self) -> None:
        while not self.stop_event.is_set():
            self.refresh_event.wait(timeout=1.0)
            self.refresh_event.clear()
            if self.stop_event.is_set():
                break
            if not self.connected_event.is_set() or not self.kws.is_connected():
                continue
            try:
                self._refresh_option_subscriptions()
            except Exception:
                logging.exception("Option subscription refresh failed; retrying")
                if not self.stop_event.wait(2.0):
                    self.refresh_event.set()

    def _build_desired_option_meta(
        self,
    ) -> Tuple[Dict[int, InstrumentMeta], Dict[str, Optional[int]], Dict[str, Optional[int]]]:
        desired: Dict[int, InstrumentMeta] = {}
        with self.state_lock:
            actual_snapshot = dict(self.actual_atm_by_index)
            center_snapshot = dict(self.subscription_center_by_index)
            live_snapshot = dict(self.has_live_underlying_tick)

        for key, runtime in self.runtimes.items():
            actual = actual_snapshot.get(key)
            center = center_snapshot.get(key)
            if actual is None or center is None:
                continue

            target_meta, target_missing = runtime.option_band(actual, ATM_WINGS)
            if target_missing:
                preview = ", ".join(
                    f"{strike}{option_type}" for strike, option_type in target_missing[:10]
                )
                message = (
                    f"{key} expiry {runtime.expiry}: missing {len(target_missing)} "
                    f"required contracts around ATM {actual}: {preview}"
                )
                if STRICT_OPTION_BAND:
                    raise RuntimeError(message)
                logging.error(message)

            subscription_wings = ATM_WINGS + (
                SUBSCRIPTION_BUFFER_WINGS
                if live_snapshot.get(key)
                else PREOPEN_EXTRA_WINGS
            )
            subscription_meta, optional_missing = runtime.option_band(
                center,
                subscription_wings,
            )
            if optional_missing:
                logging.warning(
                    "%s: %d optional buffer contracts missing around %d",
                    key,
                    len(optional_missing),
                    center,
                )
            for meta in subscription_meta:
                desired[meta.instrument_token] = meta
            for meta in target_meta:
                desired[meta.instrument_token] = meta

        return desired, actual_snapshot, center_snapshot

    def _refresh_option_subscriptions(self) -> None:
        desired_meta, actual_snapshot, center_snapshot = self._build_desired_option_meta()
        desired_tokens = set(desired_meta)
        with self.state_lock:
            current_tokens = set(self.subscribed_option_tokens)

        to_add = sorted(desired_tokens - current_tokens)
        to_remove = sorted(current_tokens - desired_tokens)

        if to_add:
            self._assign_new_stream_epoch(to_add, "SUBSCRIBE")
            self._ws_operation(
                f"subscribe {len(to_add)} options",
                lambda: self.kws.subscribe(to_add),
            )
            # QUOTE contains LTP and daily OHLC/volume but excludes depth/OI/time.
            # We persist only LTP-derived one-second OHLC.
            self._ws_operation(
                f"set {len(to_add)} options QUOTE mode",
                lambda: self.kws.set_mode(self.kws.MODE_QUOTE, to_add),
            )

        # Add first, then remove stale edges, preventing an intentional gap.
        if to_remove:
            self._ws_operation(
                f"unsubscribe {len(to_remove)} stale options",
                lambda: self.kws.unsubscribe(to_remove),
            )

        with self.state_lock:
            self.subscribed_option_tokens = desired_tokens

        metadata: Dict[str, Any] = {
            "active_option_tokens": len(desired_tokens),
            "last_subscription_refresh": now_ist().isoformat(timespec="seconds"),
        }
        for key, value in actual_snapshot.items():
            if value is not None:
                metadata[f"atm:{key}"] = value
        for key, value in center_snapshot.items():
            if value is not None:
                metadata[f"subscription_center:{key}"] = value
        try:
            self.store.set_metadata_many(metadata)
        except Exception:
            logging.exception("Could not persist subscription metadata")

        if to_add or to_remove:
            logging.info(
                "Option basket refreshed: +%d -%d active=%d ATM=%s centres=%s",
                len(to_add),
                len(to_remove),
                len(desired_tokens),
                actual_snapshot,
                center_snapshot,
            )

    def _call_on_reactor_thread(self, operation: Callable[[], Any]) -> Any:
        websocket_thread = getattr(self.kws, "websocket_thread", None)
        if websocket_thread is not None and threading.current_thread() is websocket_thread:
            return operation()

        completed = threading.Event()
        result: Dict[str, Any] = {}

        def invoke() -> None:
            try:
                result["value"] = operation()
            except BaseException as exc:
                result["error"] = exc
            finally:
                completed.set()

        reactor.callFromThread(invoke)
        if not completed.wait(timeout=WS_OPERATION_TIMEOUT_SEC):
            raise TimeoutError(
                f"WebSocket operation exceeded {WS_OPERATION_TIMEOUT_SEC:.1f}s"
            )
        if "error" in result:
            raise result["error"]
        return result.get("value")

    def _ws_operation(self, label: str, operation: Callable[[], Any]) -> None:
        websocket_thread = getattr(self.kws, "websocket_thread", None)
        on_reactor = (
            websocket_thread is not None
            and threading.current_thread() is websocket_thread
        )
        attempts = 1 if on_reactor else WS_OPERATION_ATTEMPTS
        last_error: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                result = self._call_on_reactor_thread(operation)
                if result is False:
                    raise RuntimeError("KiteTicker operation returned False")
                if attempt > 1:
                    logging.info("%s succeeded on attempt %d/%d", label, attempt, attempts)
                return
            except Exception as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                delay = min(5.0, 0.50 * (2 ** (attempt - 1)))
                logging.warning(
                    "%s failed %d/%d: %s; retrying in %.2fs",
                    label,
                    attempt,
                    attempts,
                    exc,
                    delay,
                )
                time.sleep(delay)
        raise RuntimeError(f"{label} failed after {attempts} attempts") from last_error



# =============================================================================
# Entrypoint
# =============================================================================


def build_market_times(trading_day: date) -> Tuple[datetime, datetime, datetime]:
    market_open = datetime.combine(trading_day, SESSION_START, tzinfo=IST)
    market_close = datetime.combine(trading_day, SESSION_END, tzinfo=IST)
    return market_open - timedelta(seconds=PRECONNECT_SECONDS), market_open, market_close


def wait_until(target: datetime) -> None:
    next_log = 0.0
    while True:
        current = now_ist()
        remaining = (target - current).total_seconds()
        if remaining <= 0:
            return
        if remaining > 60 and time.monotonic() >= next_log:
            logging.info(
                "Waiting for WebSocket start at %s (%.1f minutes)",
                target,
                remaining / 60,
            )
            next_log = time.monotonic() + 300.0
        time.sleep(min(30.0, remaining))


def initialise_kite() -> Any:
    logging.info("Initialising Kite API through OptionTradeUtils")
    kite = retry_call("oUtils.intialize_kite_api()", oUtils.intialize_kite_api)
    retry_call("kite.profile()", kite.profile)
    logging.info("Kite authentication verified")
    return kite


def build_runtimes(kite: Any, trading_day: date) -> Dict[str, IndexRuntime]:
    required_exchanges: Set[str] = set()
    for key in TARGET_INDEXES:
        config = INDEX_CONFIGS[key]
        required_exchanges.add(config.index_exchange)
        required_exchanges.add(config.option_exchange)

    by_exchange: Dict[str, List[Dict[str, Any]]] = {}
    for exchange in sorted(required_exchanges):
        by_exchange[exchange] = load_instruments_with_cache(
            kite,
            exchange,
            trading_day,
        )

    result: Dict[str, IndexRuntime] = {}
    for key in TARGET_INDEXES:
        config = INDEX_CONFIGS[key]
        result[key] = build_index_runtime(
            config,
            by_exchange[config.index_exchange],
            by_exchange[config.option_exchange],
            trading_day,
        )
    return result


def all_runtime_instruments(runtimes: Mapping[str, IndexRuntime]) -> List[InstrumentMeta]:
    result: Dict[int, InstrumentMeta] = {}
    for runtime in runtimes.values():
        result[runtime.underlying.instrument_token] = runtime.underlying
        for meta in runtime.option_by_strike_type.values():
            result[meta.instrument_token] = meta
    return list(result.values())


def install_signal_handlers(
    collector_holder: Dict[str, Optional[KiteOptionSpikeCollector]],
) -> None:
    def request_shutdown(signum: int, _frame: Any) -> None:
        logging.warning("Received signal %s; requesting clean shutdown", signum)
        collector = collector_holder.get("collector")
        if collector is not None:
            collector.stop_event.set()
            collector.refresh_event.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)


def main() -> int:
    trading_day = now_ist().date()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log_path = configure_logging(trading_day)

    if trading_day.weekday() >= 5 and not ALLOW_WEEKEND_RUN:
        logging.error("%s is a weekend; refusing normal live run", trading_day)
        return 2

    db_path = OUTPUT_DIR / f"kite_option_spikes_v3_{trading_day:%Y%m%d}.sqlite3"
    lock_path = db_path.with_suffix(db_path.suffix + ".lock")

    logging.info("=" * 88)
    logging.info("Kite compact option spike collector %s", SCRIPT_VERSION)
    logging.info("Trading day: %s", trading_day)
    logging.info("Targets: %s", ", ".join(TARGET_INDEXES))
    logging.info("Saved band: ATM-%d through ATM+%d, CE+PE", ATM_WINGS, ATM_WINGS)
    logging.info("Store only price-changing seconds: %s", SAVE_ONLY_PRICE_CHANGES)
    logging.info("Options mode: QUOTE; underlyings mode: LTP")
    logging.info("Database: %s", db_path)
    logging.info("=" * 88)

    connect_time, market_open, market_close = build_market_times(trading_day)
    current = now_ist()
    if current >= market_close:
        logging.error("Current time %s is after session close %s", current, market_close)
        return 2

    try:
        with SingleInstanceLock(lock_path):
            kite = initialise_kite()
            runtimes = build_runtimes(kite, trading_day)
            store = CompactSQLiteStore(db_path, trading_day)
            store.set_metadata_many(
                {
                    "schema_version": SCHEMA_VERSION,
                    "script_version": SCRIPT_VERSION,
                    "kiteconnect_version": package_version_or_unknown("kiteconnect"),
                    "trading_day": trading_day.isoformat(),
                    "target_indexes": ",".join(TARGET_INDEXES),
                    "atm_wings": ATM_WINGS,
                    "subscription_buffer_wings": SUBSCRIPTION_BUFFER_WINGS,
                    "preopen_extra_wings": PREOPEN_EXTRA_WINGS,
                    "session_start": SESSION_START.isoformat(),
                    "session_end": SESSION_END.isoformat(),
                    "save_only_price_changes": SAVE_ONLY_PRICE_CHANGES,
                    "price_scale": PRICE_SCALE,
                    "time_basis": "receipt_time_IST",
                    "option_stream_mode": "quote",
                    "underlying_stream_mode": "ltp",
                    "db_synchronous": DB_SYNCHRONOUS,
                }
            )
            for key, runtime in runtimes.items():
                store.set_metadata_many({f"expiry:{key}": runtime.expiry.isoformat()})
            store.upsert_instruments(all_runtime_instruments(runtimes))

            current = now_ist()
            if current < connect_time:
                wait_until(connect_time)
            elif current < market_open:
                logging.info("Pre-open preparation complete; connecting now")
            else:
                logging.warning(
                    "Mid-session start/restart at %s; missed downtime cannot be backfilled",
                    current,
                )

            holder: Dict[str, Optional[KiteOptionSpikeCollector]] = {"collector": None}
            install_signal_handlers(holder)
            collector = KiteOptionSpikeCollector(
                kite,
                runtimes,
                store,
                market_open,
                market_close,
            )
            holder["collector"] = collector

            try:
                collector.run_until_close()
            except Exception:
                logging.exception("Collector terminated with an unhandled exception")
                collector.stop()
                return 1

            counts = store.counts()
            total_mb = store.total_disk_bytes() / (1024 * 1024)
            logging.info("=" * 88)
            logging.info("Collection complete")
            logging.info("Database: %s", db_path)
            logging.info("Log: %s", log_path)
            logging.info("WebSocket frames received: %d", collector.frames_received)
            logging.info("WebSocket ticks received: %d", collector.ticks_received)
            logging.info("Ticks eligible for ATM band: %d", collector.ticks_eligible)
            logging.info("Buffer-only option ticks ignored: %d", collector.buffer_ticks_ignored)
            logging.info("Unknown ticks ignored: %d", collector.unknown_ticks_ignored)
            logging.info("Ticks offered to writer: %d", store.received_ticks)
            logging.info("Price-change ticks: %d", store.price_change_ticks)
            logging.info("Anchor ticks: %d", store.anchor_ticks)
            logging.info("Unchanged ticks omitted: %d", store.omitted_unchanged_ticks)
            logging.info("Bar upsert operations: %d", store.rows_upserted)
            logging.info("Final unique bars: %d", counts["bars"])
            logging.info("Instrument metadata rows: %d", counts["instruments"])
            logging.info("Queue overflows: %d", store.queue_overflows)
            logging.info("Maximum queued frames: %d", store.max_queue_depth)
            logging.info("Database + WAL size: %.2f MB", total_mb)
            logging.info("=" * 88)
            return 0
    except RuntimeError as exc:
        logging.error("%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

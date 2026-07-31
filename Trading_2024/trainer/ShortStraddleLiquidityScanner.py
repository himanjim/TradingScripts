#!/usr/bin/env python3
"""
ATM short-straddle liquidity scanner for Zerodha Kite Connect.

Purpose
=======
Observe the live order books of an ATM CE and ATM PE for a configurable period
(default: 10 minutes) and estimate how much equal quantity can be SOLD in both
legs without exceeding configured spread and price-impact limits.

The script NEVER places, modifies, or cancels an order. It is a market-data-only
scanner.

What "liquid quantity" means here
=================================
For a short straddle, entry orders sell the CE and PE. Therefore the relevant
immediate liquidity is on the BID side of both order books.

For every one-second sample, this script:

1. Reads the latest five bid levels and five ask levels received in KiteTicker
   MODE_FULL.
2. Computes the maximum equal CE/PE quantity, in whole lots, that can be sold
   into the visible five-level bid books.
3. Rejects quantities that exceed configured per-leg or combined VWAP slippage.
4. Rejects stale books and books with excessive bid/ask spreads.
5. Records the result to a crash-tolerant, line-flushed CSV file.

At the end it reports:

* Maximum quantity observed at any snapshot.
* Median and sustained depth capacity.
* Quantity available in at least X% of valid snapshots (default: 90%).
* A turnover-participation cap based on actual traded-volume increase during
  the scan (default: 5% of the lower-volume leg).
* A final conservative recommendation after a safety haircut.
* Lots and estimated number of child orders per leg, when an exchange order
  freeze quantity is known.

Critical limitation
===================
Kite Connect exposes only five market-depth levels. The output is therefore an
estimate of IMMEDIATELY VISIBLE executable liquidity, not the total quantity the
market could absorb through replenishment, iceberg behaviour, passive limit
orders, or sliced execution. Displayed orders can also be cancelled before your
orders reach the exchange. Treat the recommendation as a conservative execution
input, not as a guarantee and not as a risk/margin limit.

Expected existing project helper
================================
The script preserves the API initialisation convention used in the user's other
trading programs:

    import Trading_2024.OptionTradeUtils as oUtils
    kite = oUtils.intialize_kite_api()

The returned KiteConnect object must have a valid current-day access token.

Quick start
===========
NIFTY, nearest expiry, locked starting ATM, 10-minute scan:

    TARGET_INDEX=NIFTY python -u ShortStraddleLiquidityScanner.py

SENSEX:

    TARGET_INDEX=SENSEX python -u ShortStraddleLiquidityScanner.py

Windows Command Prompt:

    set TARGET_INDEX=SENSEX
    python -u ShortStraddleLiquidityScanner.py

Useful environment variables
============================
TARGET_INDEX=NIFTY                 # NIFTY or SENSEX
EXPIRY_DATE=2026-08-04             # optional; nearest expiry if omitted
SCAN_MINUTES=10
SAMPLE_INTERVAL_SEC=1.0
LOCK_ATM=true                      # true = assess one fixed straddle pair
OUTPUT_DIR=./liquidity_scans

MAX_SPREAD_PCT_PER_LEG=2.0         # reject snapshots above this spread
MAX_SLIPPAGE_PCT_PER_LEG=1.0       # VWAP impact vs best bid
MAX_COMBINED_SLIPPAGE_PCT=0.50     # CE+PE VWAP impact vs combined best bid
MAX_TICK_AGE_SEC=3.0
REQUIRED_AVAILABILITY_PCT=90.0     # sustained depth requirement
MAX_VOLUME_PARTICIPATION_PCT=5.0   # cap against 10-minute traded volume
SAFETY_HAIRCUT_PCT=20.0            # haircut after depth/volume caps
MIN_VALID_SNAPSHOT_RATIO=0.80

ACCOUNT_MAX_QTY=0                  # optional external risk/margin quantity cap
MAX_TEST_QTY=0                     # optional computational/reporting cap
ORDER_FREEZE_QTY=0                 # optional override; 0 = auto/unknown

For NIFTY, the script attempts to read the current quantity-freeze value from
NSE's official CSV. For SENSEX, set ORDER_FREEZE_QTY when you want child-order
counting; the liquidity estimate itself does not require this field.

Outputs
=======
A timestamped run directory containing:

* snapshots.csv              every one-second observation
* lot_fill_probability.csv   fraction of valid samples supporting each lot size
* summary.json               final machine-readable assessment
* scanner.log                detailed execution log

If interrupted, snapshots.csv remains usable and a partial summary is generated.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import math
import os
import pickle
import random
import signal
import sys
import threading
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, TypeVar

from kiteconnect import KiteTicker

# Preserve the API initialisation convention from the supplied historical script.
import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo
except ImportError as exc:  # Python < 3.9
    raise RuntimeError("Python 3.9+ is required.") from exc


# =============================================================================
# Environment configuration
# =============================================================================

IST = ZoneInfo("Asia/Kolkata")
T = TypeVar("T")


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int, minimum: Optional[int] = None) -> int:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else int(raw)
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}; got {value}")
    return value


def env_float(name: str, default: float, minimum: Optional[float] = None) -> float:
    raw = os.getenv(name)
    value = default if raw is None or not raw.strip() else float(raw)
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be >= {minimum}; got {value}")
    return value


TARGET_INDEX = os.getenv("TARGET_INDEX", "NIFTY").strip().upper()
if TARGET_INDEX not in {"NIFTY", "SENSEX"}:
    raise ValueError("TARGET_INDEX must be NIFTY or SENSEX")

SCAN_MINUTES = env_float("SCAN_MINUTES", 10.0, minimum=0.25)
SAMPLE_INTERVAL_SEC = env_float("SAMPLE_INTERVAL_SEC", 1.0, minimum=0.20)
INITIAL_QUOTE_TIMEOUT_SEC = env_float("INITIAL_QUOTE_TIMEOUT_SEC", 45.0, minimum=5.0)
LOCK_ATM = env_bool("LOCK_ATM", True)

MAX_SPREAD_PCT_PER_LEG = env_float("MAX_SPREAD_PCT_PER_LEG", 2.0, minimum=0.0)
MAX_SLIPPAGE_PCT_PER_LEG = env_float("MAX_SLIPPAGE_PCT_PER_LEG", 1.0, minimum=0.0)
MAX_COMBINED_SLIPPAGE_PCT = env_float(
    "MAX_COMBINED_SLIPPAGE_PCT", 0.50, minimum=0.0
)
MAX_TICK_AGE_SEC = env_float("MAX_TICK_AGE_SEC", 3.0, minimum=0.25)
REQUIRED_AVAILABILITY_PCT = env_float(
    "REQUIRED_AVAILABILITY_PCT", 90.0, minimum=1.0
)
if REQUIRED_AVAILABILITY_PCT > 100:
    raise ValueError("REQUIRED_AVAILABILITY_PCT cannot exceed 100")

MAX_VOLUME_PARTICIPATION_PCT = env_float(
    "MAX_VOLUME_PARTICIPATION_PCT", 5.0, minimum=0.0
)
if MAX_VOLUME_PARTICIPATION_PCT > 100:
    raise ValueError("MAX_VOLUME_PARTICIPATION_PCT cannot exceed 100")

SAFETY_HAIRCUT_PCT = env_float("SAFETY_HAIRCUT_PCT", 20.0, minimum=0.0)
if SAFETY_HAIRCUT_PCT >= 100:
    raise ValueError("SAFETY_HAIRCUT_PCT must be below 100")

MIN_VALID_SNAPSHOT_RATIO = env_float(
    "MIN_VALID_SNAPSHOT_RATIO", 0.80, minimum=0.0
)
if MIN_VALID_SNAPSHOT_RATIO > 1:
    raise ValueError("MIN_VALID_SNAPSHOT_RATIO must be between 0 and 1")

ACCOUNT_MAX_QTY = env_int("ACCOUNT_MAX_QTY", 0, minimum=0)
MAX_TEST_QTY = env_int("MAX_TEST_QTY", 0, minimum=0)
ORDER_FREEZE_QTY_OVERRIDE = env_int("ORDER_FREEZE_QTY", 0, minimum=0)
MAX_CURVE_LOTS = env_int("MAX_CURVE_LOTS", 500, minimum=1)

MAX_API_ATTEMPTS = env_int("MAX_API_ATTEMPTS", 6, minimum=1)
API_RETRY_BASE_SEC = env_float("API_RETRY_BASE_SEC", 1.5, minimum=0.1)
WS_MAX_RETRIES = env_int("WS_MAX_RETRIES", 300, minimum=1)
WS_MAX_DELAY_SEC = env_int("WS_MAX_DELAY_SEC", 60, minimum=5)
PRINT_EVERY_SEC = env_float("PRINT_EVERY_SEC", 10.0, minimum=1.0)

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./liquidity_scans")).expanduser().resolve()
CACHE_DIR = OUTPUT_DIR / "instrument_cache"

# Current official NSE quantity-freeze CSV linked from NSE contract information.
NSE_QUANTITY_FREEZE_URL = (
    "https://nsearchives.nseindia.com/content/fo/qtyfreeze.csv"
)


@dataclass(frozen=True)
class IndexSpec:
    key: str
    underlying_exchange: str
    underlying_symbol: str
    option_exchange: str
    option_name: str
    strike_step: int


INDEX_SPECS: Dict[str, IndexSpec] = {
    "NIFTY": IndexSpec(
        key="NIFTY",
        underlying_exchange="NSE",
        underlying_symbol="NIFTY 50",
        option_exchange="NFO",
        option_name="NIFTY",
        strike_step=50,
    ),
    "SENSEX": IndexSpec(
        key="SENSEX",
        underlying_exchange="BSE",
        underlying_symbol="SENSEX",
        option_exchange="BFO",
        option_name="SENSEX",
        strike_step=100,
    ),
}


# =============================================================================
# General helpers
# =============================================================================


def now_ist() -> datetime:
    return datetime.now(IST)


def safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


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


def first_present(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) is not None:
            return mapping.get(key)
    return None


def floor_to_lot(quantity: float, lot_size: int) -> int:
    if quantity <= 0 or lot_size <= 0:
        return 0
    return int(math.floor(quantity / lot_size) * lot_size)


def ceil_div(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        return 0
    return (numerator + denominator - 1) // denominator


def round_half_up_to_step(price: float, step: int) -> int:
    if price <= 0:
        raise ValueError(f"Underlying price must be positive; got {price}")
    return int(math.floor(price / step + 0.5) * step)


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
    raise ValueError(f"Cannot parse expiry: {value!r}")


def parse_expiry_override() -> Optional[date]:
    raw = os.getenv("EXPIRY_DATE", "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    raise ValueError("EXPIRY_DATE must be YYYY-MM-DD or DD-MM-YYYY")


def retry_call(
    label: str,
    function: Callable[[], T],
    attempts: int = MAX_API_ATTEMPTS,
    base_delay: float = API_RETRY_BASE_SEC,
    max_delay: float = 30.0,
) -> T:
    """Bounded exponential-backoff wrapper for Kite REST calls."""
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            result = function()
            if attempt > 1:
                logging.info("%s succeeded on attempt %d/%d", label, attempt, attempts)
            return result
        except Exception as exc:  # Kite SDK exposes several exception subclasses.
            last_error = exc
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


def atomic_write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, ensure_ascii=False, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def atomic_pickle_dump(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as handle:
        pickle.dump(value, handle, protocol=pickle.HIGHEST_PROTOCOL)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def percentile(values: Sequence[float], pct: float) -> Optional[float]:
    """Linear-interpolated percentile without a numpy dependency."""
    clean = sorted(float(v) for v in values if math.isfinite(float(v)))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    pct = max(0.0, min(100.0, pct))
    position = (len(clean) - 1) * pct / 100.0
    lower = int(math.floor(position))
    upper = int(math.ceil(position))
    if lower == upper:
        return clean[lower]
    weight = position - lower
    return clean[lower] * (1.0 - weight) + clean[upper] * weight


def configure_logging(run_dir: Path) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "scanner.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)-8s %(threadName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, encoding="utf-8"),
        ],
        force=True,
    )
    return log_path


# =============================================================================
# Instruments and contract resolution
# =============================================================================


@dataclass(frozen=True)
class Contract:
    instrument_token: int
    exchange: str
    tradingsymbol: str
    expiry: date
    strike: int
    option_type: str
    lot_size: int
    tick_size: float


@dataclass(frozen=True)
class PairDefinition:
    atm_strike: int
    ce: Contract
    pe: Contract

    @property
    def pair_id(self) -> str:
        return f"{self.expiry_text}:{self.atm_strike}:{self.ce.tradingsymbol}:{self.pe.tradingsymbol}"

    @property
    def expiry_text(self) -> str:
        return self.ce.expiry.isoformat()

    @property
    def lot_size(self) -> int:
        if self.ce.lot_size != self.pe.lot_size:
            raise RuntimeError(
                f"CE/PE lot-size mismatch: {self.ce.lot_size} vs {self.pe.lot_size}"
            )
        return self.ce.lot_size


class InstrumentUniverse:
    def __init__(self, kite: Any, spec: IndexSpec, trading_day: date) -> None:
        self.kite = kite
        self.spec = spec
        self.trading_day = trading_day
        self._cache: Dict[str, List[Dict[str, Any]]] = {}

        self.underlying_token = self._find_underlying_token()
        self.expiry, self.contracts = self._build_option_map()

    def _load_exchange(self, exchange: str) -> List[Dict[str, Any]]:
        exchange = exchange.upper()
        if exchange in self._cache:
            return self._cache[exchange]

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path = CACHE_DIR / f"{exchange}_{self.trading_day:%Y%m%d}.pkl"
        try:
            rows = retry_call(
                f"kite.instruments({exchange})",
                lambda: self.kite.instruments(exchange),
            )
            if not rows:
                raise RuntimeError(f"Empty instrument dump for {exchange}")
            atomic_pickle_dump(cache_path, rows)
            logging.info("Loaded %d instruments for %s", len(rows), exchange)
        except Exception:
            if not cache_path.exists():
                raise
            logging.exception(
                "Instrument API failed; using current-day cache %s", cache_path
            )
            with cache_path.open("rb") as handle:
                rows = pickle.load(handle)
            if not isinstance(rows, list) or not rows:
                raise RuntimeError(f"Invalid instrument cache: {cache_path}")

        self._cache[exchange] = rows
        return rows

    def _find_underlying_token(self) -> int:
        wanted = self.spec.underlying_symbol.upper()
        for row in self._load_exchange(self.spec.underlying_exchange):
            if str(row.get("tradingsymbol", "")).upper() == wanted:
                token = int(row["instrument_token"])
                logging.info(
                    "Underlying resolved: %s:%s token=%d",
                    self.spec.underlying_exchange,
                    self.spec.underlying_symbol,
                    token,
                )
                return token
        raise RuntimeError(
            f"Underlying not found: {self.spec.underlying_exchange}:"
            f"{self.spec.underlying_symbol}"
        )

    def _build_option_map(self) -> Tuple[date, Dict[Tuple[int, str], Contract]]:
        candidates: List[Tuple[date, Mapping[str, Any]]] = []
        for row in self._load_exchange(self.spec.option_exchange):
            option_type = str(row.get("instrument_type", "")).upper()
            if option_type not in {"CE", "PE"}:
                continue

            name = str(row.get("name", "")).upper().strip()
            symbol = str(row.get("tradingsymbol", "")).upper().strip()
            if name != self.spec.option_name and not symbol.startswith(self.spec.option_name):
                continue

            try:
                expiry = normalize_expiry(row.get("expiry"))
            except Exception:
                continue
            if expiry < self.trading_day:
                continue
            candidates.append((expiry, row))

        expiries = sorted({expiry for expiry, _ in candidates})
        if not expiries:
            raise RuntimeError(f"No non-expired {self.spec.key} options found")

        override = parse_expiry_override()
        selected_expiry = override or expiries[0]
        if selected_expiry not in expiries:
            raise RuntimeError(
                f"Requested expiry {selected_expiry} not found. Available: "
                + ", ".join(str(item) for item in expiries[:12])
            )

        contracts: Dict[Tuple[int, str], Contract] = {}
        for expiry, row in candidates:
            if expiry != selected_expiry:
                continue
            strike_float = safe_float(row.get("strike"))
            lot_size = safe_int(row.get("lot_size"))
            tick_size = safe_float(row.get("tick_size"))
            if strike_float is None or not lot_size or lot_size <= 0:
                continue
            strike = int(round(strike_float))
            option_type = str(row.get("instrument_type", "")).upper()
            contract = Contract(
                instrument_token=int(row["instrument_token"]),
                exchange=str(row.get("exchange") or self.spec.option_exchange),
                tradingsymbol=str(row["tradingsymbol"]),
                expiry=expiry,
                strike=strike,
                option_type=option_type,
                lot_size=lot_size,
                tick_size=tick_size or 0.05,
            )
            key = (strike, option_type)
            previous = contracts.get(key)
            if previous and previous.instrument_token != contract.instrument_token:
                raise RuntimeError(
                    f"Duplicate contract at {strike}{option_type}: "
                    f"{previous.tradingsymbol}, {contract.tradingsymbol}"
                )
            contracts[key] = contract

        logging.info(
            "%s selected expiry=%s, contracts=%d",
            self.spec.key,
            selected_expiry,
            len(contracts),
        )
        return selected_expiry, contracts

    def resolve_pair(self, requested_atm: int) -> PairDefinition:
        """Return the closest strike having both CE and PE contracts."""
        available_strikes = sorted(
            {
                strike
                for strike, option_type in self.contracts
                if option_type == "CE" and (strike, "PE") in self.contracts
            },
            key=lambda strike: (abs(strike - requested_atm), strike),
        )
        if not available_strikes:
            raise RuntimeError(f"No complete CE/PE pair found for {self.expiry}")

        strike = available_strikes[0]
        if abs(strike - requested_atm) > self.spec.strike_step * 3:
            raise RuntimeError(
                f"Nearest complete pair {strike} is too far from requested ATM {requested_atm}"
            )
        if strike != requested_atm:
            logging.warning(
                "Exact ATM pair %d unavailable; using nearest complete strike %d",
                requested_atm,
                strike,
            )
        return PairDefinition(
            atm_strike=strike,
            ce=self.contracts[(strike, "CE")],
            pe=self.contracts[(strike, "PE")],
        )


# =============================================================================
# Order-book mathematics
# =============================================================================


@dataclass(frozen=True)
class DepthLevel:
    price: float
    quantity: int
    orders: int


@dataclass(frozen=True)
class LegMetrics:
    last_price: Optional[float]
    best_bid: float
    best_ask: float
    spread_points: float
    spread_pct: float
    visible_bid_qty: int
    visible_ask_qty: int
    bid_levels: int
    ask_levels: int
    top_bid_qty: int
    top_bid_concentration_pct: float
    cumulative_volume: Optional[int]
    oi: Optional[int]
    tick_age_sec: float


@dataclass(frozen=True)
class CapacityResult:
    quantity: int
    lots: int
    ce_vwap: Optional[float]
    pe_vwap: Optional[float]
    combined_vwap: Optional[float]
    ce_slippage_pct: Optional[float]
    pe_slippage_pct: Optional[float]
    combined_slippage_pct: Optional[float]


def normalize_depth(raw_levels: Any) -> List[DepthLevel]:
    result: List[DepthLevel] = []
    if not isinstance(raw_levels, list):
        return result
    for item in raw_levels[:5]:
        if not isinstance(item, Mapping):
            continue
        price = safe_float(item.get("price"))
        quantity = safe_int(item.get("quantity"))
        orders = safe_int(item.get("orders")) or 0
        if price is None or price <= 0 or quantity is None or quantity <= 0:
            continue
        result.append(DepthLevel(price=price, quantity=quantity, orders=orders))
    return result


def vwap_for_sell(bids: Sequence[DepthLevel], quantity: int) -> Optional[float]:
    """VWAP obtained by selling `quantity` into bids from best to worst."""
    if quantity <= 0:
        return None
    remaining = quantity
    value = 0.0
    for level in bids:
        take = min(remaining, level.quantity)
        value += take * level.price
        remaining -= take
        if remaining <= 0:
            return value / quantity
    return None


def percentage_drop(reference: float, execution: float) -> float:
    if reference <= 0:
        return float("inf")
    return max(0.0, (reference - execution) / reference * 100.0)


def compute_leg_metrics(
    tick: Mapping[str, Any],
    received_monotonic: float,
    sample_monotonic: float,
) -> Tuple[LegMetrics, List[DepthLevel], List[DepthLevel]]:
    depth = tick.get("depth") or {}
    bids = normalize_depth(depth.get("buy") if isinstance(depth, Mapping) else None)
    asks = normalize_depth(depth.get("sell") if isinstance(depth, Mapping) else None)

    best_bid = bids[0].price if bids else 0.0
    best_ask = asks[0].price if asks else 0.0
    spread_points = best_ask - best_bid if best_bid > 0 and best_ask > 0 else float("inf")
    midpoint = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0
    spread_pct = spread_points / midpoint * 100.0 if midpoint > 0 else float("inf")

    visible_bid_qty = sum(level.quantity for level in bids)
    visible_ask_qty = sum(level.quantity for level in asks)
    top_bid_qty = bids[0].quantity if bids else 0
    top_bid_concentration = (
        top_bid_qty / visible_bid_qty * 100.0 if visible_bid_qty > 0 else 0.0
    )

    volume = safe_int(first_present(tick, "volume_traded", "volume"))
    oi = safe_int(tick.get("oi"))
    age = max(0.0, sample_monotonic - received_monotonic)

    metrics = LegMetrics(
        last_price=safe_float(tick.get("last_price")),
        best_bid=best_bid,
        best_ask=best_ask,
        spread_points=spread_points,
        spread_pct=spread_pct,
        visible_bid_qty=visible_bid_qty,
        visible_ask_qty=visible_ask_qty,
        bid_levels=len(bids),
        ask_levels=len(asks),
        top_bid_qty=top_bid_qty,
        top_bid_concentration_pct=top_bid_concentration,
        cumulative_volume=volume,
        oi=oi,
        tick_age_sec=age,
    )
    return metrics, bids, asks


def calculate_max_capacity(
    ce_bids: Sequence[DepthLevel],
    pe_bids: Sequence[DepthLevel],
    lot_size: int,
) -> CapacityResult:
    """
    Find the largest lot-multiple satisfying all slippage thresholds.

    Capacity is monotonic: VWAP cannot improve as more bid levels are consumed.
    Therefore a binary search is used instead of iterating through potentially
    thousands of lots.
    """
    if not ce_bids or not pe_bids or lot_size <= 0:
        return CapacityResult(0, 0, None, None, None, None, None, None)

    available_qty = min(
        sum(level.quantity for level in ce_bids),
        sum(level.quantity for level in pe_bids),
    )
    if MAX_TEST_QTY > 0:
        available_qty = min(available_qty, MAX_TEST_QTY)
    if ACCOUNT_MAX_QTY > 0:
        available_qty = min(available_qty, ACCOUNT_MAX_QTY)
    available_lots = floor_to_lot(available_qty, lot_size) // lot_size
    if available_lots <= 0:
        return CapacityResult(0, 0, None, None, None, None, None, None)

    ce_best = ce_bids[0].price
    pe_best = pe_bids[0].price
    combined_best = ce_best + pe_best

    def evaluate(lots: int) -> Tuple[bool, CapacityResult]:
        quantity = lots * lot_size
        ce_vwap = vwap_for_sell(ce_bids, quantity)
        pe_vwap = vwap_for_sell(pe_bids, quantity)
        if ce_vwap is None or pe_vwap is None:
            return False, CapacityResult(0, 0, None, None, None, None, None, None)

        ce_slippage = percentage_drop(ce_best, ce_vwap)
        pe_slippage = percentage_drop(pe_best, pe_vwap)
        combined_vwap = ce_vwap + pe_vwap
        combined_slippage = percentage_drop(combined_best, combined_vwap)

        result = CapacityResult(
            quantity=quantity,
            lots=lots,
            ce_vwap=ce_vwap,
            pe_vwap=pe_vwap,
            combined_vwap=combined_vwap,
            ce_slippage_pct=ce_slippage,
            pe_slippage_pct=pe_slippage,
            combined_slippage_pct=combined_slippage,
        )
        acceptable = (
            ce_slippage <= MAX_SLIPPAGE_PCT_PER_LEG
            and pe_slippage <= MAX_SLIPPAGE_PCT_PER_LEG
            and combined_slippage <= MAX_COMBINED_SLIPPAGE_PCT
        )
        return acceptable, result

    low = 0
    high = available_lots
    best = CapacityResult(0, 0, None, None, None, None, None, None)

    while low <= high:
        middle = (low + high) // 2
        if middle == 0:
            low = 1
            continue
        acceptable, result = evaluate(middle)
        if acceptable:
            best = result
            low = middle + 1
        else:
            high = middle - 1
    return best


# =============================================================================
# Live scanner
# =============================================================================


@dataclass
class StoredTick:
    tick: Dict[str, Any]
    received_at: datetime
    received_monotonic: float


SNAPSHOT_COLUMNS = [
    "sample_time",
    "pair_id",
    "index_name",
    "expiry",
    "atm_strike",
    "lot_size",
    "ce_symbol",
    "pe_symbol",
    "valid",
    "invalid_reason",
    "ce_tick_age_sec",
    "pe_tick_age_sec",
    "ce_last_price",
    "pe_last_price",
    "ce_best_bid",
    "ce_best_ask",
    "pe_best_bid",
    "pe_best_ask",
    "ce_spread_points",
    "pe_spread_points",
    "ce_spread_pct",
    "pe_spread_pct",
    "ce_visible_bid_qty",
    "pe_visible_bid_qty",
    "pair_visible_bid_qty",
    "ce_visible_ask_qty",
    "pe_visible_ask_qty",
    "ce_bid_levels",
    "pe_bid_levels",
    "ce_top_bid_qty",
    "pe_top_bid_qty",
    "ce_top_bid_concentration_pct",
    "pe_top_bid_concentration_pct",
    "ce_cumulative_volume",
    "pe_cumulative_volume",
    "ce_oi",
    "pe_oi",
    "max_liquid_qty",
    "max_liquid_lots",
    "ce_vwap_at_max",
    "pe_vwap_at_max",
    "combined_best_bid",
    "combined_vwap_at_max",
    "ce_slippage_pct_at_max",
    "pe_slippage_pct_at_max",
    "combined_slippage_pct_at_max",
]


class LiquidityScanner:
    def __init__(
        self,
        kite: Any,
        spec: IndexSpec,
        universe: InstrumentUniverse,
        initial_pair: PairDefinition,
        run_dir: Path,
    ) -> None:
        self.kite = kite
        self.spec = spec
        self.universe = universe
        self.run_dir = run_dir

        api_key = getattr(kite, "api_key", None) or os.getenv("KITE_API_KEY")
        access_token = getattr(kite, "access_token", None) or os.getenv("KITE_ACCESS_TOKEN")
        if not api_key or not access_token:
            raise RuntimeError(
                "api_key/access_token not available on the initialised KiteConnect object"
            )

        self.kws = KiteTicker(
            str(api_key),
            str(access_token),
            reconnect=True,
            reconnect_max_tries=WS_MAX_RETRIES,
            reconnect_max_delay=WS_MAX_DELAY_SEC,
        )

        self.state_lock = threading.RLock()
        self.stop_event = threading.Event()
        self.connected_event = threading.Event()
        self.quotes_ready_event = threading.Event()
        self.pair_change_event = threading.Event()

        self.active_pair = initial_pair
        self.latest_ticks: Dict[int, StoredTick] = {}
        self.current_underlying_price: Optional[float] = None
        self.pending_pair: Optional[PairDefinition] = None
        self.subscribed_option_tokens: set[int] = set()

        self.frames_received = 0
        self.ticks_received = 0
        self.reconnect_count = 0
        self.rows: List[Dict[str, Any]] = []
        self.scan_start: Optional[datetime] = None
        self.scan_end: Optional[datetime] = None

        self.snapshots_path = run_dir / "snapshots.csv"
        self.curve_path = run_dir / "lot_fill_probability.csv"
        self.summary_path = run_dir / "summary.json"

        self._csv_handle: Optional[Any] = None
        self._csv_writer: Optional[csv.DictWriter] = None
        self.subscription_thread = threading.Thread(
            target=self._subscription_manager,
            name="subscription-manager",
            daemon=True,
        )

        self.kws.on_connect = self._on_connect
        self.kws.on_ticks = self._on_ticks
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error
        self.kws.on_reconnect = self._on_reconnect
        self.kws.on_noreconnect = self._on_noreconnect

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self) -> Dict[str, Any]:
        self._open_snapshot_file()
        self.subscription_thread.start()
        logging.info("Connecting KiteTicker")
        self.kws.connect(threaded=True)

        try:
            if not self.connected_event.wait(timeout=INITIAL_QUOTE_TIMEOUT_SEC):
                raise RuntimeError("KiteTicker did not connect within timeout")

            if not self.quotes_ready_event.wait(timeout=INITIAL_QUOTE_TIMEOUT_SEC):
                raise RuntimeError(
                    "Both CE and PE full-depth quotes were not received within timeout"
                )

            self.scan_start = now_ist()
            deadline = time.monotonic() + SCAN_MINUTES * 60.0
            logging.info(
                "10-minute-style scan started at %s; duration %.2f minutes",
                self.scan_start.isoformat(),
                SCAN_MINUTES,
            )

            next_sample = time.monotonic()
            next_print = next_sample
            while not self.stop_event.is_set() and time.monotonic() < deadline:
                current_mono = time.monotonic()
                if current_mono < next_sample:
                    self.stop_event.wait(min(0.10, next_sample - current_mono))
                    continue

                row = self._sample_once(current_mono)
                self._write_snapshot(row)
                self.rows.append(row)

                if current_mono >= next_print:
                    self._print_live_status(row)
                    next_print = current_mono + PRINT_EVERY_SEC

                next_sample += SAMPLE_INTERVAL_SEC
                # If the system paused, do not emit a burst of stale catch-up rows.
                if next_sample < time.monotonic() - SAMPLE_INTERVAL_SEC:
                    next_sample = time.monotonic() + SAMPLE_INTERVAL_SEC

            self.scan_end = now_ist()
        except KeyboardInterrupt:
            logging.warning("Interrupted by user; generating a partial summary")
            self.scan_end = now_ist()
        finally:
            self.stop_event.set()
            self.pair_change_event.set()
            self._close_websocket()
            self.subscription_thread.join(timeout=10.0)
            self._close_snapshot_file()

        summary = self._build_summary()
        atomic_write_json(self.summary_path, summary)
        self._write_fill_curve(summary)
        return summary

    def _open_snapshot_file(self) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self._csv_handle = self.snapshots_path.open(
            "w", newline="", encoding="utf-8", buffering=1
        )
        self._csv_writer = csv.DictWriter(
            self._csv_handle,
            fieldnames=SNAPSHOT_COLUMNS,
            extrasaction="ignore",
        )
        self._csv_writer.writeheader()
        self._csv_handle.flush()
        os.fsync(self._csv_handle.fileno())

    def _write_snapshot(self, row: Mapping[str, Any]) -> None:
        if self._csv_writer is None or self._csv_handle is None:
            raise RuntimeError("Snapshot CSV is not open")
        self._csv_writer.writerow(row)
        self._csv_handle.flush()
        # fsync each row because the scan is short and restart/crash safety is
        # more important than negligible write overhead at one row per second.
        os.fsync(self._csv_handle.fileno())

    def _close_snapshot_file(self) -> None:
        if self._csv_handle is not None:
            try:
                self._csv_handle.flush()
                os.fsync(self._csv_handle.fileno())
            finally:
                self._csv_handle.close()
                self._csv_handle = None
                self._csv_writer = None

    def _close_websocket(self) -> None:
        try:
            if hasattr(self.kws, "close"):
                self.kws.close()
        except Exception:
            logging.exception("Error while closing KiteTicker")
        try:
            if hasattr(self.kws, "stop"):
                self.kws.stop()
        except Exception:
            logging.debug("KiteTicker stop() raised during shutdown", exc_info=True)

    # ------------------------------------------------------------------
    # KiteTicker callbacks
    # ------------------------------------------------------------------

    def _on_connect(self, ws: KiteTicker, response: Any) -> None:
        logging.info("KiteTicker connected: %s", response)
        self.connected_event.set()
        with self.state_lock:
            pair = self.active_pair
        tokens = [
            self.universe.underlying_token,
            pair.ce.instrument_token,
            pair.pe.instrument_token,
        ]
        self._ws_retry("subscribe active instruments", lambda: ws.subscribe(tokens))
        self._ws_retry(
            "set active instruments FULL",
            lambda: ws.set_mode(ws.MODE_FULL, tokens),
        )
        with self.state_lock:
            self.subscribed_option_tokens.update(
                {pair.ce.instrument_token, pair.pe.instrument_token}
            )

    def _on_ticks(self, ws: KiteTicker, ticks: List[Dict[str, Any]]) -> None:
        if not ticks:
            return
        received_at = now_ist()
        received_mono = time.monotonic()
        self.frames_received += 1
        self.ticks_received += len(ticks)

        pair_update_needed = False
        with self.state_lock:
            for tick in ticks:
                token = safe_int(tick.get("instrument_token"))
                if token is None:
                    continue
                if token == self.universe.underlying_token:
                    price = safe_float(tick.get("last_price"))
                    if price and price > 0:
                        self.current_underlying_price = price
                        if not LOCK_ATM:
                            requested_atm = round_half_up_to_step(
                                price, self.spec.strike_step
                            )
                            if requested_atm != self.active_pair.atm_strike:
                                candidate = self.universe.resolve_pair(requested_atm)
                                if candidate.atm_strike != self.active_pair.atm_strike:
                                    self.pending_pair = candidate
                                    pair_update_needed = True
                    continue

                self.latest_ticks[token] = StoredTick(
                    tick=dict(tick),
                    received_at=received_at,
                    received_monotonic=received_mono,
                )

            pair = self.active_pair
            ce_stored = self.latest_ticks.get(pair.ce.instrument_token)
            pe_stored = self.latest_ticks.get(pair.pe.instrument_token)
            if ce_stored and pe_stored:
                ce_depth = ce_stored.tick.get("depth") or {}
                pe_depth = pe_stored.tick.get("depth") or {}
                if (
                    isinstance(ce_depth, Mapping)
                    and isinstance(pe_depth, Mapping)
                    and normalize_depth(ce_depth.get("buy"))
                    and normalize_depth(pe_depth.get("buy"))
                ):
                    self.quotes_ready_event.set()

        if pair_update_needed:
            self.pair_change_event.set()

    def _on_close(self, ws: KiteTicker, code: int, reason: str) -> None:
        self.connected_event.clear()
        logging.warning("KiteTicker closed: code=%s reason=%s", code, reason)
        # Do not call ws.stop() here; doing so disables automatic reconnection.

    def _on_error(self, ws: KiteTicker, code: int, reason: str) -> None:
        logging.error("KiteTicker error: code=%s reason=%s", code, reason)

    def _on_reconnect(self, ws: KiteTicker, attempts_count: int) -> None:
        self.connected_event.clear()
        self.reconnect_count += 1
        logging.warning("KiteTicker reconnect attempt %s", attempts_count)

    def _on_noreconnect(self, ws: KiteTicker) -> None:
        logging.critical("KiteTicker exhausted reconnect attempts")
        self.stop_event.set()
        self.pair_change_event.set()

    # ------------------------------------------------------------------
    # ATM pair switching
    # ------------------------------------------------------------------

    def _subscription_manager(self) -> None:
        while not self.stop_event.is_set():
            self.pair_change_event.wait(timeout=1.0)
            self.pair_change_event.clear()
            if self.stop_event.is_set():
                break
            with self.state_lock:
                pending = self.pending_pair
                current = self.active_pair
                self.pending_pair = None
            if pending is None or pending.atm_strike == current.atm_strike:
                continue
            if not self.connected_event.is_set():
                with self.state_lock:
                    self.pending_pair = pending
                self.pair_change_event.set()
                time.sleep(1.0)
                continue

            new_tokens = [pending.ce.instrument_token, pending.pe.instrument_token]
            old_tokens = [current.ce.instrument_token, current.pe.instrument_token]
            try:
                # Subscribe new pair first to avoid a temporary coverage gap.
                self._ws_retry(
                    f"subscribe new ATM {pending.atm_strike}",
                    lambda: self.kws.subscribe(new_tokens),
                )
                self._ws_retry(
                    f"set new ATM {pending.atm_strike} FULL",
                    lambda: self.kws.set_mode(self.kws.MODE_FULL, new_tokens),
                )
                self._ws_retry(
                    f"unsubscribe old ATM {current.atm_strike}",
                    lambda: self.kws.unsubscribe(old_tokens),
                )
                with self.state_lock:
                    self.active_pair = pending
                    self.subscribed_option_tokens.difference_update(old_tokens)
                    self.subscribed_option_tokens.update(new_tokens)
                logging.info(
                    "Dynamic ATM switched %d -> %d (%s / %s)",
                    current.atm_strike,
                    pending.atm_strike,
                    pending.ce.tradingsymbol,
                    pending.pe.tradingsymbol,
                )
            except Exception:
                logging.exception("ATM subscription switch failed; retaining old pair")

    @staticmethod
    def _ws_retry(label: str, operation: Callable[[], Any], attempts: int = 5) -> None:
        last_error: Optional[BaseException] = None
        for attempt in range(1, attempts + 1):
            try:
                operation()
                if attempt > 1:
                    logging.info("%s succeeded on attempt %d", label, attempt)
                return
            except Exception as exc:
                last_error = exc
                if attempt >= attempts:
                    break
                delay = min(5.0, 0.5 * (2 ** (attempt - 1)))
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

    # ------------------------------------------------------------------
    # Sampling
    # ------------------------------------------------------------------

    def _sample_once(self, sample_mono: float) -> Dict[str, Any]:
        sample_time = now_ist()
        with self.state_lock:
            pair = self.active_pair
            ce_stored = self.latest_ticks.get(pair.ce.instrument_token)
            pe_stored = self.latest_ticks.get(pair.pe.instrument_token)

        base: Dict[str, Any] = {
            "sample_time": sample_time.isoformat(timespec="milliseconds"),
            "pair_id": pair.pair_id,
            "index_name": self.spec.key,
            "expiry": pair.expiry_text,
            "atm_strike": pair.atm_strike,
            "lot_size": pair.lot_size,
            "ce_symbol": pair.ce.tradingsymbol,
            "pe_symbol": pair.pe.tradingsymbol,
            "valid": False,
            "invalid_reason": "",
        }

        if ce_stored is None or pe_stored is None:
            base["invalid_reason"] = "missing_ce_or_pe_tick"
            return base

        ce_metrics, ce_bids, _ = compute_leg_metrics(
            ce_stored.tick, ce_stored.received_monotonic, sample_mono
        )
        pe_metrics, pe_bids, _ = compute_leg_metrics(
            pe_stored.tick, pe_stored.received_monotonic, sample_mono
        )

        base.update(
            {
                "ce_tick_age_sec": round(ce_metrics.tick_age_sec, 3),
                "pe_tick_age_sec": round(pe_metrics.tick_age_sec, 3),
                "ce_last_price": ce_metrics.last_price,
                "pe_last_price": pe_metrics.last_price,
                "ce_best_bid": ce_metrics.best_bid,
                "ce_best_ask": ce_metrics.best_ask,
                "pe_best_bid": pe_metrics.best_bid,
                "pe_best_ask": pe_metrics.best_ask,
                "ce_spread_points": ce_metrics.spread_points,
                "pe_spread_points": pe_metrics.spread_points,
                "ce_spread_pct": ce_metrics.spread_pct,
                "pe_spread_pct": pe_metrics.spread_pct,
                "ce_visible_bid_qty": ce_metrics.visible_bid_qty,
                "pe_visible_bid_qty": pe_metrics.visible_bid_qty,
                "pair_visible_bid_qty": min(
                    ce_metrics.visible_bid_qty, pe_metrics.visible_bid_qty
                ),
                "ce_visible_ask_qty": ce_metrics.visible_ask_qty,
                "pe_visible_ask_qty": pe_metrics.visible_ask_qty,
                "ce_bid_levels": ce_metrics.bid_levels,
                "pe_bid_levels": pe_metrics.bid_levels,
                "ce_top_bid_qty": ce_metrics.top_bid_qty,
                "pe_top_bid_qty": pe_metrics.top_bid_qty,
                "ce_top_bid_concentration_pct": ce_metrics.top_bid_concentration_pct,
                "pe_top_bid_concentration_pct": pe_metrics.top_bid_concentration_pct,
                "ce_cumulative_volume": ce_metrics.cumulative_volume,
                "pe_cumulative_volume": pe_metrics.cumulative_volume,
                "ce_oi": ce_metrics.oi,
                "pe_oi": pe_metrics.oi,
            }
        )

        invalid_reasons: List[str] = []
        if ce_metrics.tick_age_sec > MAX_TICK_AGE_SEC:
            invalid_reasons.append("stale_ce")
        if pe_metrics.tick_age_sec > MAX_TICK_AGE_SEC:
            invalid_reasons.append("stale_pe")
        if not ce_bids:
            invalid_reasons.append("no_ce_bids")
        if not pe_bids:
            invalid_reasons.append("no_pe_bids")
        if ce_metrics.best_ask <= 0:
            invalid_reasons.append("no_ce_asks")
        if pe_metrics.best_ask <= 0:
            invalid_reasons.append("no_pe_asks")
        if ce_metrics.spread_pct > MAX_SPREAD_PCT_PER_LEG:
            invalid_reasons.append("ce_spread_too_wide")
        if pe_metrics.spread_pct > MAX_SPREAD_PCT_PER_LEG:
            invalid_reasons.append("pe_spread_too_wide")

        if invalid_reasons:
            base["invalid_reason"] = ";".join(invalid_reasons)
            base["max_liquid_qty"] = 0
            base["max_liquid_lots"] = 0
            return base

        capacity = calculate_max_capacity(ce_bids, pe_bids, pair.lot_size)
        base.update(
            {
                "valid": True,
                "invalid_reason": "" if capacity.quantity > 0 else "no_lot_within_slippage",
                "max_liquid_qty": capacity.quantity,
                "max_liquid_lots": capacity.lots,
                "ce_vwap_at_max": capacity.ce_vwap,
                "pe_vwap_at_max": capacity.pe_vwap,
                "combined_best_bid": ce_metrics.best_bid + pe_metrics.best_bid,
                "combined_vwap_at_max": capacity.combined_vwap,
                "ce_slippage_pct_at_max": capacity.ce_slippage_pct,
                "pe_slippage_pct_at_max": capacity.pe_slippage_pct,
                "combined_slippage_pct_at_max": capacity.combined_slippage_pct,
            }
        )
        return base

    @staticmethod
    def _print_live_status(row: Mapping[str, Any]) -> None:
        if row.get("valid"):
            logging.info(
                "ATM %s | %s/%s | bid %.2f+%.2f | visible=%s | liquid=%s qty (%s lots)",
                row.get("atm_strike"),
                row.get("ce_symbol"),
                row.get("pe_symbol"),
                safe_float(row.get("ce_best_bid")) or 0.0,
                safe_float(row.get("pe_best_bid")) or 0.0,
                row.get("pair_visible_bid_qty"),
                row.get("max_liquid_qty"),
                row.get("max_liquid_lots"),
            )
        else:
            logging.warning(
                "ATM %s snapshot invalid: %s",
                row.get("atm_strike"),
                row.get("invalid_reason"),
            )

    # ------------------------------------------------------------------
    # Final report
    # ------------------------------------------------------------------

    def _build_summary(self) -> Dict[str, Any]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in self.rows:
            grouped.setdefault(str(row.get("pair_id", "unknown")), []).append(row)

        pair_summaries: List[Dict[str, Any]] = []
        for pair_id, rows in grouped.items():
            pair_summaries.append(self._summarise_pair(pair_id, rows))
        pair_summaries.sort(
            key=lambda item: (item.get("valid_samples", 0), item.get("total_samples", 0)),
            reverse=True,
        )

        overall = pair_summaries[0] if pair_summaries else None
        summary: Dict[str, Any] = {
            "status": "complete" if self.rows else "no_samples",
            "generated_at": now_ist().isoformat(),
            "target_index": self.spec.key,
            "selected_expiry": self.universe.expiry.isoformat(),
            "locked_atm": LOCK_ATM,
            "scan_start": self.scan_start.isoformat() if self.scan_start else None,
            "scan_end": self.scan_end.isoformat() if self.scan_end else None,
            "configured_scan_minutes": SCAN_MINUTES,
            "sample_interval_sec": SAMPLE_INTERVAL_SEC,
            "frames_received": self.frames_received,
            "ticks_received": self.ticks_received,
            "reconnect_count": self.reconnect_count,
            "criteria": {
                "max_spread_pct_per_leg": MAX_SPREAD_PCT_PER_LEG,
                "max_slippage_pct_per_leg": MAX_SLIPPAGE_PCT_PER_LEG,
                "max_combined_slippage_pct": MAX_COMBINED_SLIPPAGE_PCT,
                "max_tick_age_sec": MAX_TICK_AGE_SEC,
                "required_availability_pct": REQUIRED_AVAILABILITY_PCT,
                "max_volume_participation_pct": MAX_VOLUME_PARTICIPATION_PCT,
                "safety_haircut_pct": SAFETY_HAIRCUT_PCT,
                "min_valid_snapshot_ratio": MIN_VALID_SNAPSHOT_RATIO,
                "account_max_qty": ACCOUNT_MAX_QTY,
                "max_test_qty": MAX_TEST_QTY,
            },
            "primary_pair_summary": overall,
            "all_pair_summaries": pair_summaries,
            "files": {
                "snapshots_csv": str(self.snapshots_path),
                "fill_probability_csv": str(self.curve_path),
                "summary_json": str(self.summary_path),
            },
            "interpretation": {
                "observed_max_quantity": (
                    "Largest equal CE/PE quantity visible within the five-level bid books "
                    "at any valid snapshot and within the configured slippage thresholds."
                ),
                "availability_depth_quantity": (
                    "Largest quantity supported in at least REQUIRED_AVAILABILITY_PCT "
                    "of valid snapshots."
                ),
                "recommended_quantity": (
                    "Minimum of sustained depth, turnover-participation cap and optional "
                    "account cap, reduced by the configured safety haircut."
                ),
            },
        }
        return summary

    def _summarise_pair(
        self, pair_id: str, rows: Sequence[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        if not rows:
            return {"pair_id": pair_id, "total_samples": 0, "valid_samples": 0}

        first = rows[0]
        lot_size = int(first.get("lot_size") or 0)
        valid_rows = [row for row in rows if bool(row.get("valid"))]
        positive_rows = [
            row for row in valid_rows if int(row.get("max_liquid_qty") or 0) > 0
        ]
        total_samples = len(rows)
        valid_samples = len(valid_rows)
        valid_ratio = valid_samples / total_samples if total_samples else 0.0

        quantities = [int(row.get("max_liquid_qty") or 0) for row in valid_rows]
        lots_values = [int(row.get("max_liquid_lots") or 0) for row in valid_rows]

        observed_max_qty = max(quantities, default=0)
        observed_max_lots = observed_max_qty // lot_size if lot_size else 0
        median_qty = floor_to_lot(percentile(quantities, 50) or 0, lot_size)
        p25_qty = floor_to_lot(percentile(quantities, 25) or 0, lot_size)
        p10_qty = floor_to_lot(percentile(quantities, 10) or 0, lot_size)

        availability_lots = 0
        if lots_values:
            # Largest lot count supported by at least the required fraction of
            # valid samples. Unlike the display curve, this calculation is not
            # capped by MAX_CURVE_LOTS.
            required_count = max(
                1,
                int(math.ceil(len(lots_values) * REQUIRED_AVAILABILITY_PCT / 100.0)),
            )
            descending = sorted(lots_values, reverse=True)
            availability_lots = descending[required_count - 1]
        availability_qty = availability_lots * lot_size

        ce_volumes = [
            int(value)
            for value in (safe_int(row.get("ce_cumulative_volume")) for row in valid_rows)
            if value is not None
        ]
        pe_volumes = [
            int(value)
            for value in (safe_int(row.get("pe_cumulative_volume")) for row in valid_rows)
            if value is not None
        ]
        ce_volume_delta = max(ce_volumes) - min(ce_volumes) if len(ce_volumes) >= 2 else None
        pe_volume_delta = max(pe_volumes) - min(pe_volumes) if len(pe_volumes) >= 2 else None

        volume_cap_qty: Optional[int] = None
        if ce_volume_delta is not None and pe_volume_delta is not None:
            lower_leg_volume_delta = max(0, min(ce_volume_delta, pe_volume_delta))
            volume_cap_qty = floor_to_lot(
                lower_leg_volume_delta * MAX_VOLUME_PARTICIPATION_PCT / 100.0,
                lot_size,
            )

        caps = [availability_qty]
        if volume_cap_qty is not None:
            caps.append(volume_cap_qty)
        if ACCOUNT_MAX_QTY > 0:
            caps.append(floor_to_lot(ACCOUNT_MAX_QTY, lot_size))
        pre_haircut_qty = min(caps) if caps else 0

        data_quality_pass = valid_ratio >= MIN_VALID_SNAPSHOT_RATIO
        if not data_quality_pass:
            pre_haircut_qty = 0

        recommended_qty = floor_to_lot(
            pre_haircut_qty * (1.0 - SAFETY_HAIRCUT_PCT / 100.0),
            lot_size,
        )
        recommended_lots = recommended_qty // lot_size if lot_size else 0

        freeze_qty = determine_order_freeze_qty(self.spec.key, lot_size)
        max_child_qty = floor_to_lot(freeze_qty or 0, lot_size) if freeze_qty else None
        child_orders_per_leg = (
            ceil_div(recommended_qty, max_child_qty)
            if recommended_qty > 0 and max_child_qty and max_child_qty > 0
            else None
        )

        spreads_ce = [safe_float(row.get("ce_spread_pct")) for row in valid_rows]
        spreads_pe = [safe_float(row.get("pe_spread_pct")) for row in valid_rows]
        combined_slippage = [
            safe_float(row.get("combined_slippage_pct_at_max"))
            for row in positive_rows
        ]

        return {
            "pair_id": pair_id,
            "index_name": first.get("index_name"),
            "expiry": first.get("expiry"),
            "atm_strike": first.get("atm_strike"),
            "ce_symbol": first.get("ce_symbol"),
            "pe_symbol": first.get("pe_symbol"),
            "lot_size": lot_size,
            "total_samples": total_samples,
            "valid_samples": valid_samples,
            "positive_capacity_samples": len(positive_rows),
            "valid_snapshot_ratio": round(valid_ratio, 6),
            "data_quality_pass": data_quality_pass,
            "observed_max_quantity": observed_max_qty,
            "observed_max_lots": observed_max_lots,
            "median_depth_quantity": median_qty,
            "median_depth_lots": median_qty // lot_size if lot_size else 0,
            "p25_depth_quantity": p25_qty,
            "p10_depth_quantity": p10_qty,
            "availability_required_pct": REQUIRED_AVAILABILITY_PCT,
            "availability_depth_quantity": availability_qty,
            "availability_depth_lots": availability_lots,
            "ce_volume_delta_during_scan": ce_volume_delta,
            "pe_volume_delta_during_scan": pe_volume_delta,
            "volume_participation_pct": MAX_VOLUME_PARTICIPATION_PCT,
            "turnover_participation_cap_quantity": volume_cap_qty,
            "pre_haircut_quantity": pre_haircut_qty,
            "safety_haircut_pct": SAFETY_HAIRCUT_PCT,
            "recommended_quantity": recommended_qty,
            "recommended_lots": recommended_lots,
            "order_freeze_quantity": freeze_qty,
            "max_lot_multiple_per_child_order": max_child_qty,
            "estimated_child_orders_per_leg": child_orders_per_leg,
            "median_ce_spread_pct": percentile(
                [value for value in spreads_ce if value is not None], 50
            ),
            "median_pe_spread_pct": percentile(
                [value for value in spreads_pe if value is not None], 50
            ),
            "median_combined_slippage_pct_at_capacity": percentile(
                [value for value in combined_slippage if value is not None], 50
            ),
            "warning": (
                None
                if data_quality_pass
                else "Insufficient valid/stable snapshots; recommendation forced to zero."
            ),
        }

    def _write_fill_curve(self, summary: Mapping[str, Any]) -> None:
        with self.curve_path.open("w", newline="", encoding="utf-8") as handle:
            fieldnames = [
                "pair_id",
                "atm_strike",
                "lot_size",
                "lots",
                "quantity",
                "supporting_valid_samples",
                "valid_samples",
                "fill_availability_pct",
            ]
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()

            grouped: Dict[str, List[Mapping[str, Any]]] = {}
            for row in self.rows:
                if bool(row.get("valid")):
                    grouped.setdefault(str(row.get("pair_id")), []).append(row)

            for pair_id, rows in grouped.items():
                if not rows:
                    continue
                lot_size = int(rows[0].get("lot_size") or 0)
                max_lots = min(
                    max((int(row.get("max_liquid_lots") or 0) for row in rows), default=0),
                    MAX_CURVE_LOTS,
                )
                for lots in range(1, max_lots + 1):
                    supporting = sum(
                        int(row.get("max_liquid_lots") or 0) >= lots for row in rows
                    )
                    writer.writerow(
                        {
                            "pair_id": pair_id,
                            "atm_strike": rows[0].get("atm_strike"),
                            "lot_size": lot_size,
                            "lots": lots,
                            "quantity": lots * lot_size,
                            "supporting_valid_samples": supporting,
                            "valid_samples": len(rows),
                            "fill_availability_pct": round(
                                supporting / len(rows) * 100.0, 4
                            ),
                        }
                    )
            handle.flush()
            os.fsync(handle.fileno())


# =============================================================================
# Freeze quantity and entrypoint
# =============================================================================


def fetch_nse_quantity_freeze(symbol: str) -> Optional[int]:
    """Read the current NSE quantity-freeze CSV with retries."""
    for attempt in range(1, 4):
        try:
            request = urllib.request.Request(
                NSE_QUANTITY_FREEZE_URL,
                headers={"User-Agent": "Mozilla/5.0 liquidity-scanner"},
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                text = response.read().decode("utf-8-sig", errors="replace")
            reader = csv.DictReader(text.splitlines())
            for row in reader:
                cleaned = {
                    str(key).strip().upper(): str(value).strip()
                    for key, value in row.items()
                    if key is not None
                }
                if cleaned.get("SYMBOL") == symbol.upper():
                    value = safe_int(cleaned.get("VOL_FRZ_QTY"))
                    if value and value > 0:
                        logging.info(
                            "NSE quantity freeze for %s: %d", symbol.upper(), value
                        )
                        return value
            logging.warning("%s not found in NSE quantity-freeze CSV", symbol)
            return None
        except Exception as exc:
            if attempt >= 3:
                logging.warning("Could not fetch NSE quantity freeze: %s", exc)
                return None
            time.sleep(1.0 * attempt)
    return None


def determine_order_freeze_qty(index_key: str, lot_size: int) -> Optional[int]:
    if ORDER_FREEZE_QTY_OVERRIDE > 0:
        return ORDER_FREEZE_QTY_OVERRIDE
    if index_key == "NIFTY":
        return fetch_nse_quantity_freeze("NIFTY")
    # No hardcoded SENSEX freeze value: exchange limits can change. The user can
    # supply ORDER_FREEZE_QTY. This does not affect the liquidity calculation.
    return None


def initialise_kite() -> Any:
    logging.info("Initialising Kite API through Trading_2024.OptionTradeUtils")
    kite = retry_call("oUtils.intialize_kite_api()", oUtils.intialize_kite_api)
    retry_call("kite.profile()", kite.profile)
    logging.info("Kite authentication verified")
    return kite


def resolve_initial_pair(
    kite: Any,
    spec: IndexSpec,
    universe: InstrumentUniverse,
) -> Tuple[float, PairDefinition]:
    quote_key = f"{spec.underlying_exchange}:{spec.underlying_symbol}"
    quote_map = retry_call("kite.ltp(underlying)", lambda: kite.ltp([quote_key]))
    quote = quote_map.get(quote_key) or {}
    price = safe_float(quote.get("last_price"))
    if price is None or price <= 0:
        raise RuntimeError(f"No valid LTP returned for {quote_key}")
    requested_atm = round_half_up_to_step(price, spec.strike_step)
    pair = universe.resolve_pair(requested_atm)
    logging.info(
        "%s LTP %.2f -> ATM %d | %s / %s | lot_size=%d",
        spec.key,
        price,
        pair.atm_strike,
        pair.ce.tradingsymbol,
        pair.pe.tradingsymbol,
        pair.lot_size,
    )
    return price, pair


def print_final_summary(summary: Mapping[str, Any]) -> None:
    primary = summary.get("primary_pair_summary") or {}
    print("\n" + "=" * 80)
    print("SHORT-STRADDLE LIQUIDITY SCAN RESULT")
    print("=" * 80)
    if not primary:
        print("No valid pair summary was generated.")
        return
    print(f"Index / expiry : {primary.get('index_name')} / {primary.get('expiry')}")
    print(f"ATM pair       : {primary.get('ce_symbol')} + {primary.get('pe_symbol')}")
    print(f"Lot size       : {primary.get('lot_size')}")
    print(
        f"Valid samples  : {primary.get('valid_samples')}/{primary.get('total_samples')} "
        f"({(primary.get('valid_snapshot_ratio') or 0) * 100:.1f}%)"
    )
    print(
        f"Observed max   : {primary.get('observed_max_quantity')} qty / "
        f"{primary.get('observed_max_lots')} lots"
    )
    print(
        f"Sustained      : {primary.get('availability_depth_quantity')} qty / "
        f"{primary.get('availability_depth_lots')} lots at "
        f"{primary.get('availability_required_pct')}% availability"
    )
    print(
        f"Volume cap     : {primary.get('turnover_participation_cap_quantity')} qty "
        f"at {primary.get('volume_participation_pct')}% participation"
    )
    print(
        f"Recommended    : {primary.get('recommended_quantity')} qty / "
        f"{primary.get('recommended_lots')} lots after safety haircut"
    )
    print(f"Freeze qty     : {primary.get('order_freeze_quantity')}")
    print(f"Child orders   : {primary.get('estimated_child_orders_per_leg')} per leg")
    if primary.get("warning"):
        print(f"WARNING        : {primary.get('warning')}")
    print("=" * 80)
    print(f"Snapshots: {summary.get('files', {}).get('snapshots_csv')}")
    print(f"Curve    : {summary.get('files', {}).get('fill_probability_csv')}")
    print(f"Summary  : {summary.get('files', {}).get('summary_json')}")
    print("=" * 80)


def install_signal_handlers(scanner_holder: Dict[str, Optional[LiquidityScanner]]) -> None:
    def request_stop(signum: int, _frame: Any) -> None:
        logging.warning("Received signal %s; finishing with partial data", signum)
        scanner = scanner_holder.get("scanner")
        if scanner is not None:
            scanner.stop_event.set()
            scanner.pair_change_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)


def main() -> int:
    run_started = now_ist()
    run_id = f"{TARGET_INDEX}_{run_started:%Y%m%d_%H%M%S}"
    run_dir = OUTPUT_DIR / run_id
    log_path = configure_logging(run_dir)

    logging.info("=" * 80)
    logging.info("ATM short-straddle liquidity scanner")
    logging.info("Target: %s", TARGET_INDEX)
    logging.info("Duration: %.2f minutes", SCAN_MINUTES)
    logging.info("LOCK_ATM: %s", LOCK_ATM)
    logging.info(
        "Limits: spread<=%.3f%%/leg, slippage<=%.3f%%/leg, combined<=%.3f%%",
        MAX_SPREAD_PCT_PER_LEG,
        MAX_SLIPPAGE_PCT_PER_LEG,
        MAX_COMBINED_SLIPPAGE_PCT,
    )
    logging.info("Output: %s", run_dir)
    logging.info("=" * 80)

    try:
        kite = initialise_kite()
        spec = INDEX_SPECS[TARGET_INDEX]
        universe = InstrumentUniverse(kite, spec, run_started.date())
        _, pair = resolve_initial_pair(kite, spec, universe)

        scanner = LiquidityScanner(
            kite=kite,
            spec=spec,
            universe=universe,
            initial_pair=pair,
            run_dir=run_dir,
        )
        holder: Dict[str, Optional[LiquidityScanner]] = {"scanner": scanner}
        install_signal_handlers(holder)

        summary = scanner.run()
        print_final_summary(summary)
        logging.info("Log file: %s", log_path)
        return 0 if summary.get("primary_pair_summary") else 2
    except Exception:
        logging.exception("Liquidity scanner failed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

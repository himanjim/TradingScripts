"""
OptionTradeUtils_env.py
=======================
Utilities shared by the V3OPT live/paper short-straddle trader.

Key responsibilities
--------------------
1. Load Zerodha credentials from a local ``.env`` file.
2. Resolve the index to trade:
      * ``UNDERLYING_MODE=AUTO`` selects the nearest eligible expiry among
        NIFTY and SENSEX, using ``ALLOWED_DTE``. With ``ALLOWED_DTE=0`` this
        means the bot trades only the index expiring today.
      * ``UNDERLYING_MODE=NIFTY`` or ``SENSEX`` forces one index.
      * Legacy ``choice=1`` / ``choice=2`` remains supported.
3. Calculate holiday-adjusted weekly/monthly expiry symbols.
4. Return the same tuple structure used by older trading scripts.
5. Keep Kite credentials out of Python source code.

Safety and maintenance
----------------------
* The bundled holiday set covers 2026 only because that is the calendar supplied
  for this strategy. Update ``TRADING_HOLIDAYS`` before running in later years,
  or provide additional dates through ``EXTRA_TRADING_HOLIDAYS=YYYY-MM-DD,...``.
* Keep ``.env`` out of Git.
* ``KITE_ACCESS_CODE`` is the Kite access token and normally changes daily.
"""

from __future__ import annotations

import calendar
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from kiteconnect import KiteConnect


# -----------------------------------------------------------------------------
# Environment loading
# -----------------------------------------------------------------------------
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH, override=False)

# Keep the existing tag so restart reconciliation can identify strategy orders.
SS_ORDER_TAG = "SSSTRADDLE"
LS_ORDER_TAG = "LSOTMSTRADDLE"

IST = ZoneInfo("Asia/Kolkata")


# -----------------------------------------------------------------------------
# Trading calendar
# -----------------------------------------------------------------------------
TRADING_HOLIDAYS_2026 = {
    date(2026, 1, 15),
    date(2026, 1, 26),
    date(2026, 3, 3),
    date(2026, 3, 26),
    date(2026, 3, 31),
    date(2026, 4, 3),
    date(2026, 4, 14),
    date(2026, 5, 1),
    date(2026, 5, 28),
    date(2026, 6, 26),
    date(2026, 9, 14),
    date(2026, 10, 2),
    date(2026, 10, 20),
    date(2026, 11, 10),
    date(2026, 11, 24),
    date(2026, 12, 25),
}


def _parse_extra_holidays(raw: Optional[str]) -> set[date]:
    """Parse ``EXTRA_TRADING_HOLIDAYS=YYYY-MM-DD,...`` safely."""
    out: set[date] = set()
    if raw is None or not str(raw).strip():
        return out
    for item in str(raw).split(","):
        value = item.strip()
        if not value:
            continue
        try:
            out.add(date.fromisoformat(value))
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid EXTRA_TRADING_HOLIDAYS date '{value}'. Use YYYY-MM-DD."
            ) from exc
    return out


TRADING_HOLIDAYS = set(TRADING_HOLIDAYS_2026)
TRADING_HOLIDAYS.update(_parse_extra_holidays(os.getenv("EXTRA_TRADING_HOLIDAYS")))

# Python weekday: Monday=0 ... Sunday=6.
NIFTY_EXPIRY_WEEKDAY = 1    # Tuesday
SENSEX_EXPIRY_WEEKDAY = 3  # Thursday

KITE_WEEKLY_MONTH_CODES = {
    1: "1", 2: "2", 3: "3", 4: "4", 5: "5", 6: "6",
    7: "7", 8: "8", 9: "9", 10: "O", 11: "N", 12: "D",
}
KITE_MONTHLY_MONTH_CODES = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC",
}


class NoTradeDay(RuntimeError):
    """Raised when AUTO mode finds no underlying eligible under ALLOWED_DTE."""


@dataclass(frozen=True)
class InstrumentSelection:
    """Resolved index/expiry/instrument settings for one trading day."""

    underlying_name: str
    choice: int
    underlying_exchange: str
    underlying_symbol: str
    options_exchange: str
    part_symbol: str
    quantity_units: int
    strike_multiple: int
    stoploss_points_legacy: int
    minimum_lots_legacy: int
    long_straddle_distance_legacy: int
    expiry_date: date
    days_to_expiry: int

    @property
    def underlying_quote_key(self) -> str:
        return f"{self.underlying_exchange}{self.underlying_symbol}"

    def legacy_tuple(self) -> Tuple[str, str, str, str, int, int, int, int, int]:
        """Return the tuple expected by older scripts."""
        return (
            self.underlying_exchange,
            self.underlying_symbol,
            self.options_exchange,
            self.part_symbol,
            self.quantity_units,
            self.strike_multiple,
            self.stoploss_points_legacy,
            self.minimum_lots_legacy,
            self.long_straddle_distance_legacy,
        )


# -----------------------------------------------------------------------------
# Environment helpers
# -----------------------------------------------------------------------------
def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        raise RuntimeError(
            f"Missing required environment variable {name}. "
            f"Set it in {ENV_PATH} or in the process environment."
        )
    return str(value).strip()


def _optional_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        return None
    return str(value).strip()


def _parse_int_list(raw: Optional[str], default: Iterable[int]) -> list[int]:
    if raw is None or not str(raw).strip():
        return list(default)
    try:
        values = [int(round(float(x.strip()))) for x in str(raw).split(",") if x.strip()]
    except ValueError as exc:
        raise RuntimeError(f"Invalid integer list: {raw}") from exc
    return values or list(default)


def _normalise_mode(raw: Optional[str]) -> str:
    """Normalise AUTO/NIFTY/SENSEX and legacy choice values."""
    value = (raw or "AUTO").strip().upper()
    if value in {"AUTO", "0", "EXPIRY", "EXPIRY_DAY"}:
        return "AUTO"
    if value in {"1", "NIFTY", "NSE", "NIFTY50", "NIFTY 50"}:
        return "NIFTY"
    if value in {"2", "SENSEX", "BSE"}:
        return "SENSEX"
    raise RuntimeError(
        f"Invalid UNDERLYING_MODE/choice '{raw}'. Use AUTO, NIFTY/1, or SENSEX/2."
    )


def _normalise_part_symbol(value: str) -> str:
    symbol = str(value).strip().upper()
    if not symbol:
        raise RuntimeError("PART_SYMBOL override is blank.")
    return symbol if symbol.startswith(":") else f":{symbol}"


# Kept as module globals for compatibility with existing code.
KITE_API_KEY = _required_env("KITE_API_KEY")
KITE_API_SECRET = _required_env("KITE_API_SECRET")
KITE_ACCESS_CODE = _required_env("KITE_ACCESS_CODE")

# Common aliases used by some older scripts.
API_KEY = KITE_API_KEY
API_SECRET = KITE_API_SECRET
ACCESS_TOKEN = KITE_ACCESS_CODE


# -----------------------------------------------------------------------------
# Expiry helpers
# -----------------------------------------------------------------------------
def today_ist() -> date:
    return datetime.now(IST).date()


def is_trading_day(day: date) -> bool:
    return day.weekday() < 5 and day not in TRADING_HOLIDAYS


def prepone_to_previous_trading_day(day: date) -> date:
    adjusted = day
    while not is_trading_day(adjusted):
        adjusted -= timedelta(days=1)
    return adjusted


def _next_weekday_on_or_after(start_day: date, target_weekday: int) -> date:
    return start_day + timedelta(days=(target_weekday - start_day.weekday()) % 7)


def _last_weekday_of_month(year: int, month: int, target_weekday: int) -> date:
    day = date(year, month, calendar.monthrange(year, month)[1])
    while day.weekday() != target_weekday:
        day -= timedelta(days=1)
    return day


def monthly_expiry_date(year: int, month: int, expiry_weekday: int) -> date:
    normal = _last_weekday_of_month(year, month, expiry_weekday)
    return prepone_to_previous_trading_day(normal)


def is_monthly_expiry(expiry_day: date, expiry_weekday: int) -> bool:
    return expiry_day == monthly_expiry_date(
        expiry_day.year, expiry_day.month, expiry_weekday
    )


def next_adjusted_weekly_expiry(as_of_day: date, expiry_weekday: int) -> date:
    """Return the next still-tradeable adjusted weekly expiry."""
    candidate = _next_weekday_on_or_after(as_of_day, expiry_weekday)
    for _ in range(12):
        adjusted = prepone_to_previous_trading_day(candidate)
        if adjusted >= as_of_day:
            return adjusted
        candidate += timedelta(days=7)
    raise RuntimeError(
        f"Could not resolve expiry from {as_of_day}; check the holiday calendar."
    )


def _expiry_weekday(underlying_name: str) -> int:
    if underlying_name == "NIFTY":
        return NIFTY_EXPIRY_WEEKDAY
    if underlying_name == "SENSEX":
        return SENSEX_EXPIRY_WEEKDAY
    raise RuntimeError(f"Unsupported underlying: {underlying_name}")


def calculate_expiry_date(underlying_name: str, as_of_day: Optional[date] = None) -> date:
    day = as_of_day or today_ist()
    return next_adjusted_weekly_expiry(day, _expiry_weekday(underlying_name))


def _format_kite_part_symbol(underlying_name: str, expiry_day: date) -> str:
    weekday = _expiry_weekday(underlying_name)
    yy = f"{expiry_day.year % 100:02d}"
    if is_monthly_expiry(expiry_day, weekday):
        return f":{underlying_name}{yy}{KITE_MONTHLY_MONTH_CODES[expiry_day.month]}"
    return (
        f":{underlying_name}{yy}"
        f"{KITE_WEEKLY_MONTH_CODES[expiry_day.month]}{expiry_day.day:02d}"
    )


def calculate_part_symbol(
    choice_or_underlying: int | str,
    as_of_day: Optional[date] = None,
) -> str:
    """Compatibility helper accepting 1/2 or NIFTY/SENSEX."""
    mode = _normalise_mode(str(choice_or_underlying))
    if mode == "AUTO":
        name, expiry = select_underlying_for_day(as_of_day=as_of_day)
    else:
        name = mode
        expiry = calculate_expiry_date(name, as_of_day)
    return _format_kite_part_symbol(name, expiry)


def get_calculated_expiry_date(
    choice_or_underlying: int | str,
    as_of_day: Optional[date] = None,
) -> date:
    mode = _normalise_mode(str(choice_or_underlying))
    if mode == "AUTO":
        _, expiry = select_underlying_for_day(as_of_day=as_of_day)
        return expiry
    return calculate_expiry_date(mode, as_of_day)


def select_underlying_for_day(
    *,
    as_of_day: Optional[date] = None,
    allowed_dte: Optional[Iterable[int]] = None,
    requested_mode: Optional[str] = None,
) -> tuple[str, date]:
    """
    Select the strategy underlying for the day.

    AUTO mode mirrors the backtest's actual-trade selector:
    1. retain indices whose DTE is allowed;
    2. choose the nearest expiry;
    3. prefer NIFTY if both have the same expiry date.
    """
    day = as_of_day or today_ist()
    allowed = list(allowed_dte) if allowed_dte is not None else _parse_int_list(
        os.getenv("ALLOWED_DTE"), [0]
    )
    mode = _normalise_mode(
        requested_mode
        or os.getenv("UNDERLYING_MODE")
        or os.getenv("choice")
        or "AUTO"
    )

    names = [mode] if mode in {"NIFTY", "SENSEX"} else ["NIFTY", "SENSEX"]
    candidates: list[tuple[int, int, str, date]] = []
    for name in names:
        expiry = calculate_expiry_date(name, day)
        dte = (expiry - day).days
        if dte in allowed:
            priority = 0 if name == "NIFTY" else 1
            candidates.append((dte, priority, name, expiry))

    if not candidates:
        details = ", ".join(
            f"{name} DTE={(calculate_expiry_date(name, day) - day).days}"
            for name in names
        )
        raise NoTradeDay(
            f"No eligible underlying on {day.isoformat()} for ALLOWED_DTE={allowed}. "
            f"Resolved: {details}."
        )

    _, _, name, expiry = sorted(candidates)[0]
    return name, expiry


# -----------------------------------------------------------------------------
# Instrument configuration
# -----------------------------------------------------------------------------
def resolve_instrument_selection(
    kite_: KiteConnect,
    *,
    as_of_day: Optional[date] = None,
    allowed_dte: Optional[Iterable[int]] = None,
) -> InstrumentSelection:
    day = as_of_day or today_ist()
    name, expiry = select_underlying_for_day(
        as_of_day=day,
        allowed_dte=allowed_dte,
    )
    dte = (expiry - day).days

    override = _optional_env("PART_SYMBOL")
    part_symbol = _normalise_part_symbol(override) if override else _format_kite_part_symbol(name, expiry)

    if name == "NIFTY":
        return InstrumentSelection(
            underlying_name="NIFTY",
            choice=1,
            underlying_exchange=kite_.EXCHANGE_NSE,
            underlying_symbol=":NIFTY 50",
            options_exchange=kite_.EXCHANGE_NFO,
            part_symbol=part_symbol,
            quantity_units=325,
            strike_multiple=50,
            stoploss_points_legacy=10,
            minimum_lots_legacy=65,
            long_straddle_distance_legacy=1000,
            expiry_date=expiry,
            days_to_expiry=dte,
        )

    return InstrumentSelection(
        underlying_name="SENSEX",
        choice=2,
        underlying_exchange=kite_.EXCHANGE_BSE,
        underlying_symbol=":SENSEX",
        options_exchange=kite_.EXCHANGE_BFO,
        part_symbol=part_symbol,
        quantity_units=100,
        strike_multiple=100,
        stoploss_points_legacy=30,
        minimum_lots_legacy=20,
        long_straddle_distance_legacy=3000,
        expiry_date=expiry,
        days_to_expiry=dte,
    )


def get_instruments(
    kite_: KiteConnect,
) -> Tuple[str, str, str, str, int, int, int, int, int]:
    """Backward-compatible wrapper used by older scripts."""
    return resolve_instrument_selection(kite_).legacy_tuple()


# -----------------------------------------------------------------------------
# Kite API initialisation
# -----------------------------------------------------------------------------
def intialize_kite_api() -> KiteConnect:
    """Initialise KiteConnect; misspelling retained for backward compatibility."""
    kite = KiteConnect(api_key=KITE_API_KEY)
    try:
        kite.set_access_token(KITE_ACCESS_CODE)
    except Exception as exc:
        raise RuntimeError(f"Kite authentication failed: {exc}") from exc
    return kite


def initialize_kite_api() -> KiteConnect:
    return intialize_kite_api()


if __name__ == "__main__":
    allowed = _parse_int_list(os.getenv("ALLOWED_DTE"), [0])
    try:
        name, expiry = select_underlying_for_day(allowed_dte=allowed)
        print(f"Underlying      : {name}")
        print(f"Expiry date     : {expiry.isoformat()}")
        print(f"Days to expiry  : {(expiry - today_ist()).days}")
        print(f"PART_SYMBOL     : {_format_kite_part_symbol(name, expiry)}")
    except NoTradeDay as exc:
        print(f"No trade today  : {exc}")

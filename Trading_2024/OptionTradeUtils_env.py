"""
OptionTradeUtils.py

Purpose
-------
Utility functions for Zerodha Kite based option trading scripts.

This version:
1. Reads Kite credentials from environment variables / .env.
2. Reads instrument choice from environment variable `choice`.
3. Calculates PART_SYMBOL automatically from the current IST date.
4. Uses hard-coded 2026 trading holidays supplied by the user.
5. Handles expiry preponement when the normal expiry day is a trading holiday.
6. Keeps old public function names and return structure to avoid breaking existing
   scripts that import this file.

Required environment variables
------------------------------
KITE_API_KEY       Zerodha Kite Connect API key.
KITE_API_SECRET    Zerodha Kite Connect API secret.
KITE_ACCESS_CODE   Existing Kite access token used by kite.set_access_token().
choice             1/NIFTY or 2/SENSEX.

Optional environment variable
-----------------------------
PART_SYMBOL         Manual override. Example: :NIFTY26623 or :SENSEX26618.
                    If this is present, automatic expiry calculation is bypassed.
                    Keep this unset for automatic mode.

Important notes
---------------
- NIFTY expiry anchor day is assumed to be Tuesday.
- SENSEX expiry anchor day is assumed to be Thursday.
- If the normal expiry date is a holiday, expiry is shifted backwards to the
  previous trading day.
- If the calculated expiry is the monthly expiry, the Kite monthly symbol format
  is used, e.g. :NIFTY26JUN.
- Otherwise the weekly symbol format is used, e.g. :NIFTY26623.
- Do not hardcode live credentials in Python files.
- Keep .env outside Git, or add `.env` to `.gitignore`.
"""

from __future__ import annotations

import calendar
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from kiteconnect import KiteConnect


# -----------------------------------------------------------------------------
# .env loading
# -----------------------------------------------------------------------------
# Load a .env file kept in the same folder as this utility file.
# This allows both local laptop use and server/tmux use without manually exporting
# variables each time.
ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)


# -----------------------------------------------------------------------------
# Hard-coded 2026 trading holidays supplied by the user
# -----------------------------------------------------------------------------
# These holidays are used only for expiry preponement.
# Format: date(year, month, day)
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

# Keep a generic name so future yearly holiday sets can be added without changing
# downstream function names.
TRADING_HOLIDAYS = set(TRADING_HOLIDAYS_2026)


# -----------------------------------------------------------------------------
# Expiry rules
# -----------------------------------------------------------------------------
# Python weekday convention:
# Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4, Saturday=5, Sunday=6.
NIFTY_EXPIRY_WEEKDAY = 1       # Tuesday
SENSEX_EXPIRY_WEEKDAY = 3      # Thursday

# Kite weekly option month code convention:
# Jan-Sep use 1-9, Oct uses O, Nov uses N, Dec uses D.
KITE_WEEKLY_MONTH_CODES = {
    1: "1",
    2: "2",
    3: "3",
    4: "4",
    5: "5",
    6: "6",
    7: "7",
    8: "8",
    9: "9",
    10: "O",
    11: "N",
    12: "D",
}

KITE_MONTHLY_MONTH_CODES = {
    1: "JAN",
    2: "FEB",
    3: "MAR",
    4: "APR",
    5: "MAY",
    6: "JUN",
    7: "JUL",
    8: "AUG",
    9: "SEP",
    10: "OCT",
    11: "NOV",
    12: "DEC",
}

IST = ZoneInfo("Asia/Kolkata")


# -----------------------------------------------------------------------------
# Environment variable helpers
# -----------------------------------------------------------------------------

def _get_required_env(name: str) -> str:
    """
    Read a required environment variable.

    The script fails immediately with a clear error if the variable is missing.
    This is safer for trading code because a half-configured bot should not run.
    """
    value = os.getenv(name)

    if value is None or str(value).strip() == "":
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            f"Set it before running the trading script."
        )

    return str(value).strip()


def _get_optional_env(name: str) -> Optional[str]:
    """
    Read an optional environment variable.

    Returns None when the value is missing or blank.
    """
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return None
    return str(value).strip()


def _normalise_choice(raw_choice: str) -> int:
    """
    Convert the environment variable `choice` to an internal integer.

    Accepted values:
        1, NIFTY, NSE, NIFTY50, NIFTY 50 -> NIFTY setup
        2, SENSEX, BSE                   -> SENSEX setup

    The environment variable name requested by the user is lowercase: `choice`.
    Values are accepted case-insensitively for convenience.
    """
    value = str(raw_choice).strip().upper()

    if value in {"1", "NIFTY", "NSE", "NIFTY50", "NIFTY 50"}:
        return 1

    if value in {"2", "SENSEX", "BSE"}:
        return 2

    raise RuntimeError(
        "Invalid environment variable `choice`. "
        "Use one of: 1, NIFTY, NSE, 2, SENSEX, BSE."
    )


def _normalise_part_symbol(part_symbol: str) -> str:
    """
    Ensure PART_SYMBOL uses the same leading-colon format as the old code.

    Examples:
        NIFTY26623  -> :NIFTY26623
        :NIFTY26623 -> :NIFTY26623
    """
    value = str(part_symbol).strip().upper()
    if not value:
        raise RuntimeError("PART_SYMBOL override is blank.")

    if not value.startswith(":"):
        value = f":{value}"

    return value


# -----------------------------------------------------------------------------
# Kite credentials read from environment
# -----------------------------------------------------------------------------
# These names are kept as module-level variables because existing code may import
# them from OptionTradeUtils.py. They are now read from environment variables.
KITE_API_KEY = _get_required_env("KITE_API_KEY")
KITE_API_SECRET = _get_required_env("KITE_API_SECRET")
KITE_ACCESS_CODE = _get_required_env("KITE_ACCESS_CODE")


# -----------------------------------------------------------------------------
# Trading-calendar helpers
# -----------------------------------------------------------------------------

def _today_ist() -> date:
    """Return today's date in Indian Standard Time."""
    return datetime.now(IST).date()


def _is_trading_day(day: date) -> bool:
    """
    Return True if the date is a normal trading day.

    This function treats Saturday/Sunday and the hard-coded 2026 holidays as
    non-trading days.
    """
    is_weekend = day.weekday() >= 5
    is_holiday = day in TRADING_HOLIDAYS
    return not is_weekend and not is_holiday


def _prepone_to_previous_trading_day(day: date) -> date:
    """
    Move an expiry date backwards until it falls on a trading day.

    Example:
        If normal expiry is Thursday and that Thursday is a holiday, the expiry
        becomes Wednesday. If Wednesday is also a holiday, it moves to Tuesday,
        and so on.
    """
    adjusted = day
    while not _is_trading_day(adjusted):
        adjusted -= timedelta(days=1)
    return adjusted


def _next_weekday_on_or_after(start_day: date, target_weekday: int) -> date:
    """
    Find the next target weekday on or after start_day.

    Example:
        start_day Monday, target Tuesday -> next Tuesday.
        start_day Tuesday, target Tuesday -> same Tuesday.
        start_day Wednesday, target Tuesday -> next week's Tuesday.
    """
    days_ahead = (target_weekday - start_day.weekday()) % 7
    return start_day + timedelta(days=days_ahead)


def _last_weekday_of_month(year: int, month: int, target_weekday: int) -> date:
    """
    Return the last occurrence of target_weekday in a given month.

    Used to determine monthly expiry.
    """
    last_day_number = calendar.monthrange(year, month)[1]
    day = date(year, month, last_day_number)

    while day.weekday() != target_weekday:
        day -= timedelta(days=1)

    return day


def _monthly_expiry_date(year: int, month: int, expiry_weekday: int) -> date:
    """
    Calculate the adjusted monthly expiry date for a month.

    Monthly expiry is taken as the last expiry weekday of the month, adjusted
    backwards if it falls on a holiday/non-trading day.
    """
    normal_monthly_expiry = _last_weekday_of_month(year, month, expiry_weekday)
    return _prepone_to_previous_trading_day(normal_monthly_expiry)


def _is_monthly_expiry(expiry_day: date, expiry_weekday: int) -> bool:
    """
    Check whether the given adjusted expiry date is the monthly expiry.

    This matters because Kite uses different symbol formats:
        Weekly : NIFTY26623
        Monthly: NIFTY26JUN
    """
    return expiry_day == _monthly_expiry_date(
        expiry_day.year,
        expiry_day.month,
        expiry_weekday,
    )


def _next_adjusted_weekly_expiry(as_of_day: date, expiry_weekday: int) -> date:
    """
    Calculate the next valid adjusted weekly expiry.

    Process:
    1. Find the normal expiry weekday on or after as_of_day.
    2. Prepone it if that expiry day is a holiday/non-trading day.
    3. If the adjusted date is already before as_of_day, move to the next week.

    The third step is important for cases where the regular expiry day is a
    holiday and the actual expiry was preponed to the previous trading day.
    """
    candidate = _next_weekday_on_or_after(as_of_day, expiry_weekday)

    # Try enough weeks to avoid edge cases around holidays/weekends.
    for _ in range(10):
        adjusted = _prepone_to_previous_trading_day(candidate)
        if adjusted >= as_of_day:
            return adjusted
        candidate += timedelta(days=7)

    raise RuntimeError(
        f"Could not calculate next expiry from {as_of_day} for weekday "
        f"{expiry_weekday}. Check holiday calendar."
    )


def _format_kite_part_symbol(
    underlying_symbol: str,
    expiry_day: date,
    expiry_weekday: int,
) -> str:
    """
    Format the calculated expiry date as the Kite PART_SYMBOL prefix.

    Parameters
    ----------
    underlying_symbol:
        "NIFTY" or "SENSEX".
    expiry_day:
        Adjusted expiry date.
    expiry_weekday:
        Normal expiry weekday for the underlying.

    Returns
    -------
    str
        Example weekly : :NIFTY26623
        Example monthly: :SENSEX26JUN
    """
    yy = f"{expiry_day.year % 100:02d}"

    if _is_monthly_expiry(expiry_day, expiry_weekday):
        month_code = KITE_MONTHLY_MONTH_CODES[expiry_day.month]
        return f":{underlying_symbol}{yy}{month_code}"

    weekly_month_code = KITE_WEEKLY_MONTH_CODES[expiry_day.month]
    dd = f"{expiry_day.day:02d}"
    return f":{underlying_symbol}{yy}{weekly_month_code}{dd}"


def calculate_part_symbol(choice: int, as_of_day: Optional[date] = None) -> str:
    """
    Calculate PART_SYMBOL automatically for the selected underlying.

    Parameters
    ----------
    choice:
        1 for NIFTY, 2 for SENSEX.
    as_of_day:
        Optional date used for testing. If None, today's IST date is used.

    Returns
    -------
    str
        Auto-calculated PART_SYMBOL in the same format used by the old code.

    Examples
    --------
    If as_of_day is 2026-06-18 and choice is SENSEX:
        :SENSEX26618

    If as_of_day is 2026-06-24 and choice is NIFTY:
        :NIFTY26JUN   # 30-Jun-2026 is the monthly Tuesday expiry
    """
    day = as_of_day or _today_ist()

    if choice == 1:
        underlying_symbol = "NIFTY"
        expiry_weekday = NIFTY_EXPIRY_WEEKDAY
    elif choice == 2:
        underlying_symbol = "SENSEX"
        expiry_weekday = SENSEX_EXPIRY_WEEKDAY
    else:
        raise RuntimeError(f"Unsupported choice for expiry calculation: {choice}")

    expiry_day = _next_adjusted_weekly_expiry(day, expiry_weekday)
    return _format_kite_part_symbol(
        underlying_symbol=underlying_symbol,
        expiry_day=expiry_day,
        expiry_weekday=expiry_weekday,
    )


def get_calculated_expiry_date(choice: int, as_of_day: Optional[date] = None) -> date:
    """
    Return the adjusted expiry date for logging/debugging.

    This is not required by the old scripts, but it is useful when validating
    that PART_SYMBOL has been generated for the intended expiry.
    """
    day = as_of_day or _today_ist()

    if choice == 1:
        expiry_weekday = NIFTY_EXPIRY_WEEKDAY
    elif choice == 2:
        expiry_weekday = SENSEX_EXPIRY_WEEKDAY
    else:
        raise RuntimeError(f"Unsupported choice for expiry date calculation: {choice}")

    return _next_adjusted_weekly_expiry(day, expiry_weekday)


# -----------------------------------------------------------------------------
# Instrument configuration
# -----------------------------------------------------------------------------

def get_instruments(
    kite_: KiteConnect,
) -> Tuple[
    str,      # UNDER_LYING_EXCHANGE
    str,      # UNDERLYING
    str,      # OPTIONS_EXCHANGE
    str,      # PART_SYMBOL
    int,      # NO_OF_LOTS
    int,      # STRIKE_MULTIPLE
    int,      # STOPLOSS_POINTS
    int,      # MINIMUM_LOTS
    int,      # LONG_STRADDLE_STRIKE_DISTANCE
]:
    """
    Return exchange and option parameters for the selected underlying.

    The selected underlying is read from environment variable `choice`.

    PART_SYMBOL behavior:
    - If environment variable PART_SYMBOL is set, it is used as a manual override.
    - If PART_SYMBOL is not set, it is calculated automatically using today's
      IST date, the expiry weekday, and the hard-coded holiday calendar.
    """

    choice = _normalise_choice(_get_required_env("choice"))

    # Optional emergency/manual override.
    # Normal automatic mode: keep PART_SYMBOL unset in .env.
    manual_part_symbol = _get_optional_env("PART_SYMBOL")
    if manual_part_symbol:
        PART_SYMBOL = _normalise_part_symbol(manual_part_symbol)
    else:
        PART_SYMBOL = calculate_part_symbol(choice)

    if choice == 1:
        # NIFTY configuration
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_NSE
        UNDERLYING = ":NIFTY 50"
        OPTIONS_EXCHANGE = kite_.EXCHANGE_NFO

        NO_OF_LOTS = 325
        STRIKE_MULTIPLE = 50
        STOPLOSS_POINTS = 10
        MINIMUM_LOTS = 65
        LONG_STRADDLE_STRIKE_DISTANCE = 1000

    elif choice == 2:
        # SENSEX configuration
        UNDER_LYING_EXCHANGE = kite_.EXCHANGE_BSE
        UNDERLYING = ":SENSEX"
        OPTIONS_EXCHANGE = kite_.EXCHANGE_BFO

        NO_OF_LOTS = 100
        STRIKE_MULTIPLE = 100
        STOPLOSS_POINTS = 30
        MINIMUM_LOTS = 20
        LONG_STRADDLE_STRIKE_DISTANCE = 3000

    else:
        # This should never execute because _normalise_choice() validates choice.
        raise RuntimeError(f"Unsupported choice: {choice}")

    return (
        UNDER_LYING_EXCHANGE,
        UNDERLYING,
        OPTIONS_EXCHANGE,
        PART_SYMBOL,
        NO_OF_LOTS,
        STRIKE_MULTIPLE,
        STOPLOSS_POINTS,
        MINIMUM_LOTS,
        LONG_STRADDLE_STRIKE_DISTANCE,
    )


# -----------------------------------------------------------------------------
# Kite API initialization
# -----------------------------------------------------------------------------

def intialize_kite_api() -> KiteConnect:
    """
    Initialize KiteConnect using credentials from environment variables.

    Note:
    - The function name is intentionally kept as `intialize_kite_api` because
      existing code may already be calling this misspelled name.
    - `KITE_ACCESS_CODE` is passed to kite.set_access_token(), matching the
      behavior of the earlier file.
    - `KITE_API_SECRET` is read from environment for completeness and for use
      by other code that may generate access tokens. It is not needed by
      kite.set_access_token() once a valid access token already exists.
    """
    kite = KiteConnect(api_key=KITE_API_KEY)

    try:
        kite.set_access_token(KITE_ACCESS_CODE)
    except Exception as exc:
        print("Authentication failed", str(exc))
        raise

    return kite


# Correctly spelled alias for new code. Existing old code can continue using
# intialize_kite_api(), while new code may use initialize_kite_api().
def initialize_kite_api() -> KiteConnect:
    """Correctly spelled wrapper around intialize_kite_api()."""
    return intialize_kite_api()


# -----------------------------------------------------------------------------
# Optional direct-run debug helper
# -----------------------------------------------------------------------------
# Running this file directly prints the calculated PART_SYMBOL without requiring
# instrument lookup. This is useful before market open:
#     python OptionTradeUtils.py
if __name__ == "__main__":
    selected_choice = _normalise_choice(_get_required_env("choice"))
    expiry_date = get_calculated_expiry_date(selected_choice)
    part_symbol = calculate_part_symbol(selected_choice)

    name = "NIFTY" if selected_choice == 1 else "SENSEX"
    print(f"Underlying      : {name}")
    print(f"Expiry date     : {expiry_date.isoformat()}")
    print(f"PART_SYMBOL     : {part_symbol}")

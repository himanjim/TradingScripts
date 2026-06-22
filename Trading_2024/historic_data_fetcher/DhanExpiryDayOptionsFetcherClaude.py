"""
DhanExpiryDayOptionsFetcher_ACompatible.py
=========================================

Purpose
-------
Download Dhan expired index-option intraday data and save it in the exact
pickle schema expected by `atm_straddle_claude_reattempt.py`.

This script is a revised version of `DhanExpiryDayOptionsFetcherClaude.py`.
The key difference is output compatibility:

    A expects these columns in every pickle:
        date, name, type, option_type, strike, expiry, instrument,
        high, low, close

    This script writes those columns directly, while retaining useful Dhan
    diagnostic columns such as dt_ist, date_ist, spot, oi, iv, strikeSelector,
    strike_offset, expiry_date, dte, melt_ok, etc.

Important data model point
--------------------------
Dhan's rolling-option endpoint accepts relative strike selectors:

    ATM, ATM+1, ATM-1, ..., ATM+10, ATM-10

These are rolling selectors. The actual fixed strike can change over the day.
Therefore, this script synthesizes a fixed-contract-like `instrument` using:

    SYMBOL + EXPIRY + STRIKE + CE/PE

Example:

    NIFTY20260623_24500CE

When all selectors are combined, the same fixed strike may appear under
multiple selectors at different times as ATM moves. That is exactly what the
backtest needs: a fixed strike series by `instrument`.

Default behaviour
-----------------
- Downloads NIFTY and SENSEX weekly expiry sessions.
- Keeps the expiry day (DTE 0) AND the day before expiry (DTE 1) by default,
  so the backtest can trade either. Configure A to match:

      ALLOWED_DTE = [0, 1]

- To download the expiry day only (the old behaviour), run with:

      set DHAN_KEEP_DTE=0          (Windows)
      export DHAN_KEEP_DTE=0       (Linux/macOS)

  NOTE: DTE here is CALENDAR days from the resolved expiry. The straddle-melt
  verification validates ONLY the DTE=0 session, because a straddle melts to
  intrinsic value at expiry; DTE=1 rows still carry a full day of time value
  and are intentionally NOT melt-checked. On weeks where the day before
  expiry is a holiday there is simply no DTE=1 row (consistent with A, which
  also filters on calendar DTE).

- Writes one A-compatible pickle per symbol-expiry.
- Also writes an optional coverage CSV per pickle to help detect missing
  fixed-strike minute coverage.

Security
--------
Do not hard-code Dhan tokens in source code. Set these environment variables:

    DHAN_ACCESS_TOKEN
    DHAN_CLIENT_ID

Windows CMD example:

    set DHAN_ACCESS_TOKEN=your_token_here
    set DHAN_CLIENT_ID=your_client_id_here
    python DhanExpiryDayOptionsFetcher_ACompatible.py

PowerShell example:

    $env:DHAN_ACCESS_TOKEN="your_token_here"
    $env:DHAN_CLIENT_ID="your_client_id_here"
    python DhanExpiryDayOptionsFetcher_ACompatible.py
"""

from __future__ import annotations

import argparse
import csv
import os
import pickle
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests


# =============================================================================
# CONFIG
# =============================================================================

# IMPORTANT:
# `atm_straddle_claude_reattempt.py` currently reads this folder by default:
#     G:\My Drive\Trading\Historical_Options_Data
# Therefore this downloader writes to the same default folder so A can read the
# generated pickles without code changes.
OUT_DIR = os.getenv(
    "DHAN_EXPIRED_OUTDIR",
    r"G:\My Drive\Trading\Dhan_Historical_Options_Data_New_0_1",
)

# How many calendar days to go back from today while generating scheduled
# weekly expiries.
LOOKBACK_DAYS = int(os.getenv("DHAN_LOOKBACK_DAYS", str(365 * 4)))

# If a scheduled expiry is a holiday or has no data, try earlier dates up to
# this many days. The actual expiry is ultimately derived from max(date_ist)
# returned by the API.
MAX_SHIFT_BACK_DAYS = int(os.getenv("DHAN_MAX_SHIFT_BACK_DAYS", "7"))

# Throttle to reduce Dhan rate-limit errors.
SLEEP_BETWEEN_CALLS = float(os.getenv("DHAN_SLEEP_BETWEEN_CALLS", "0.15"))

TIMEZONE_IST = "Asia/Kolkata"
PROCESS_NEWEST_FIRST = os.getenv("DHAN_NEWEST_FIRST", "1").strip().lower() not in ("0", "false", "no")

# Index options near expiry usually support ATM +/- 10 selectors through the
# Dhan rolling API. Keep it configurable.
STRIKE_BAND = int(os.getenv("DHAN_STRIKE_BAND", "10"))

# Dhan rolling option logic:
# With a 7-day window [E-7, E+1), two weekly contracts usually overlap.
# expiryCode=1 selects the target E contract. Do not widen WINDOW_BACK_DAYS
# casually; a wider window may overlap a third weekly contract and shift the
# meaning of expiryCode=1.
EXPIRY_CODE = int(os.getenv("DHAN_EXPIRY_CODE", "1"))
WINDOW_BACK_DAYS = int(os.getenv("DHAN_WINDOW_BACK_DAYS", "7"))

# Rows retained from the fetched window, expressed as calendar DTE versus the
# resolved expiry date.
# Default "0,1" keeps the expiry day (DTE 0) AND the day before expiry (DTE 1).
# IMPORTANT: set ALLOWED_DTE=[0,1] in A as well, otherwise the DTE=1 rows are
# downloaded but never traded. Use DHAN_KEEP_DTE=0 to revert to expiry-day only.
def _parse_int_csv(s: str, default: Iterable[int]) -> List[int]:
    try:
        vals = [int(x.strip()) for x in str(s).split(",") if x.strip() != ""]
        return vals if vals else list(default)
    except Exception:
        return list(default)


KEEP_DTE = sorted(set(_parse_int_csv(os.getenv("DHAN_KEEP_DTE", "0,1"), [0, 1])))

# Fields requested from Dhan. `spot` is needed for expiry-session validation
# and for debugging ATM selection.
REQUIRED_DATA = ["open", "high", "low", "close", "iv", "volume", "strike", "oi", "spot"]

ROLLING_URL = "https://api.dhan.co/v2/charts/rollingoption"
IDX_I_URL = "https://api.dhan.co/v2/instrument/IDX_I"

# Optional sidecar CSV showing whether fixed strike contracts have full-ish
# minute coverage. A ignores this CSV; it is only for sanity checking.
WRITE_COVERAGE_CSV = os.getenv("DHAN_WRITE_COVERAGE_CSV", "1").strip().lower() not in ("0", "false", "no")

# Expiry-day straddle-melt validation:
# residual_tv_close = ATM straddle close - |spot close - ATM strike|.
# On true expiry this should be small relative to the morning straddle premium.
MELT_MAX_RESIDUAL_FRAC = float(os.getenv("DHAN_MELT_MAX_RESIDUAL_FRAC", "0.40"))
MELT_MIN_BARS = int(os.getenv("DHAN_MELT_MIN_BARS", "20"))
MELT_STRICT = os.getenv("DHAN_MELT_STRICT", "0").strip().lower() not in ("0", "false", "no")

# Credentials: environment only. Do not paste tokens into this file.
ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzgyMTk4NTg5LCJpYXQiOjE3ODIxMTIxODksInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NTg4OTMyIn0.YGJJ5S60DMTmNcrehY9gyi5YDaeLaHYLiXPWr-_goW5Q3FFF_0Ag3Pqz1jjxh0GXOzOUIUYsghnjn39D4cFfeg").strip()
CLIENT_ID = os.getenv("DHAN_CLIENT_ID", "1108588932").strip()


# =============================================================================
# EXPIRY CALENDAR RULES
# =============================================================================
# Weekday integers: Mon=0 ... Sun=6
TUE, THU, FRI = 1, 3, 4

# Known weekly expiry regime changes.
NIFTY_TUE_CUTOVER = date(2025, 9, 1)       # NIFTY weekly: Thu -> Tue
SENSEX_TUE_CUTOVER = date(2025, 1, 1)      # SENSEX weekly: Fri -> Tue
SENSEX_THU_CUTOVER = date(2025, 9, 1)      # SENSEX weekly: Tue -> Thu

# Avoid generating useless SENSEX weekly expiries before the practical start.
EARLIEST = {
    "NIFTY": date(2021, 8, 1),
    "SENSEX": date(2023, 5, 1),
}

SYMBOLS = {
    "NIFTY": {"exchangeSegment": "NSE_FNO", "idx_name": "NIFTY 50"},
    "SENSEX": {"exchangeSegment": "BSE_FNO", "idx_name": "SENSEX"},
}

# Manual overrides for exceptional expiry dates, if needed.
# Example:
# EXPIRY_OVERRIDES = {"NIFTY": [date(2025, 6, 17)]}
EXPIRY_OVERRIDES: Dict[str, List[date]] = {}


# =============================================================================
# EXPIRY CALENDAR GENERATION
# =============================================================================
def _weeklies(start: date, end: date, weekday_fn) -> List[date]:
    """Generate weekly scheduled expiry dates using a function of week Monday."""
    out: List[date] = []
    monday = start - timedelta(days=start.weekday())
    while monday <= end:
        cand = monday + timedelta(days=weekday_fn(monday))
        if start <= cand <= end:
            out.append(cand)
        monday += timedelta(days=7)
    return out


def _nifty_wd(week_monday: date) -> int:
    """NIFTY weekly expiry weekday for the week starting `week_monday`."""
    return TUE if week_monday >= NIFTY_TUE_CUTOVER else THU


def _sensex_wd(week_monday: date) -> int:
    """SENSEX weekly expiry weekday for the week starting `week_monday`."""
    if week_monday >= SENSEX_THU_CUTOVER:
        return THU
    if week_monday >= SENSEX_TUE_CUTOVER:
        return TUE
    return FRI


def generate_expiries(sym: str, start: date, end: date) -> List[date]:
    """Generate scheduled weekly expiries for a symbol."""
    if EXPIRY_OVERRIDES.get(sym):
        return sorted(d for d in EXPIRY_OVERRIDES[sym] if start <= d <= end)

    start = max(start, EARLIEST.get(sym, start))
    if sym == "NIFTY":
        return _weeklies(start, end, _nifty_wd)
    if sym == "SENSEX":
        return _weeklies(start, end, _sensex_wd)
    raise KeyError(sym)


# =============================================================================
# HTTP HELPERS
# =============================================================================
def _post_json(
    session: requests.Session,
    url: str,
    headers: dict,
    payload: dict,
    retries: int = 6,
    base_sleep: float = 0.5,
) -> dict:
    """
    Robust POST with exponential backoff for Dhan rate limits and transient
    server errors.
    """
    for attempt in range(retries):
        r = session.post(url, headers=headers, json=payload, timeout=60)

        if r.status_code == 200:
            j = r.json()
            if isinstance(j, dict) and (j.get("status") == "failed" or j.get("errorCode")):
                # DH-904 is rate-limit/throttle-like. Retry.
                if j.get("errorCode") == "DH-904":
                    time.sleep(base_sleep * (2 ** attempt))
                    continue
                raise RuntimeError(f"HTTP 200 failed payload: {j}")
            return j

        try:
            j = r.json()
        except Exception:
            j = {"raw": r.text[:300]}

        err = None
        if isinstance(j, dict):
            if j.get("errorCode"):
                err = j["errorCode"]
            elif j.get("status") == "failed" and isinstance(j.get("data"), dict) and j["data"]:
                err = next(iter(j["data"].keys()))

        if r.status_code in (429, 500, 502, 503, 504) or err == "DH-904":
            time.sleep(base_sleep * (2 ** attempt))
            continue

        raise RuntimeError(f"HTTP {r.status_code}: {j}")

    raise RuntimeError(f"Failed after retries: {url}")


_NO_DATA_CODES = ("DH-905", "DH-907", "811", "812", "no data", "No Data")


def _is_no_data(err: Exception) -> bool:
    """Return True if an exception looks like a no-data response."""
    m = str(err)
    return any(c in m for c in _NO_DATA_CODES)


# =============================================================================
# PARSING AND NORMALIZATION HELPERS
# =============================================================================
def _leg_to_df(j: dict, leg: str) -> pd.DataFrame:
    """
    Convert one Dhan CE/PE response into a row-wise DataFrame.

    The returned frame is not yet A-compatible. It still uses Dhan/native
    columns. A-compatible columns are added later after all selectors are
    combined and deduplicated.
    """
    data = j.get("data") or {}
    series = data.get("ce" if leg == "CALL" else "pe") or {}
    ts = series.get("timestamp") or []

    if not ts:
        return pd.DataFrame()

    df = pd.DataFrame({
        "timestamp": ts,
        "open": series.get("open"),
        "high": series.get("high"),
        "low": series.get("low"),
        "close": series.get("close"),
        "volume": series.get("volume"),
        "oi": series.get("oi"),
        "iv": series.get("iv"),
        "strike": series.get("strike"),
        "spot": series.get("spot"),
    })

    # Dhan rolling timestamps are Unix epoch seconds. Convert to IST.
    dt_utc = pd.to_datetime(df["timestamp"], unit="s", utc=True, errors="coerce")
    df["dt_ist"] = dt_utc.dt.tz_convert(TIMEZONE_IST)
    df["date_ist"] = df["dt_ist"].dt.date
    df["timestamp_str"] = df["dt_ist"].dt.strftime("%Y-%m-%d %H:%M:%S")

    # A expects CE/PE in `option_type` later. Keep leg now also.
    df["leg"] = "CE" if leg == "CALL" else "PE"
    return df


def _strike_offset(sel: str) -> int:
    """Convert selector strings like ATM+3 / ATM-5 into signed offsets."""
    s = sel.upper().strip()
    if s == "ATM":
        return 0
    if s.startswith("ATM+"):
        return int(s[4:])
    if s.startswith("ATM-"):
        return -int(s[4:])
    return 10 ** 9


def _strike_selectors(band: int) -> List[str]:
    """Return ATM, ATM+1, ATM-1, ..., ATM+band, ATM-band."""
    out = ["ATM"]
    for k in range(1, band + 1):
        out.append(f"ATM+{k}")
        out.append(f"ATM-{k}")
    return out


def _score_index_row(target: str, name: str) -> float:
    """Score IDX_I rows to resolve NIFTY 50 and SENSEX security IDs reliably."""
    n = "".join(ch for ch in (name or "").upper() if ch.isalnum() or ch.isspace()).strip()
    t = target.upper()
    sc = 0.0

    if n == t:
        sc += 1000

    if t == "NIFTY 50":
        if "NIFTY" in n:
            sc += 200
        if "50" in n:
            sc += 150
        for bad in ["BANK", "FIN", "MID", "SMALL", "IT", "NEXT", "100", "200", "500"]:
            if bad in n:
                sc -= 80

    elif t == "SENSEX":
        if "SENSEX" in n:
            sc += 300
        for bad in ["50", "BANKEX", "NEXT"]:
            if bad in n:
                sc -= 80

    sc -= max(0, len(n) - 12) * 0.5
    return sc


# =============================================================================
# A-COMPATIBLE OUTPUT ADAPTER
# =============================================================================
def _make_instrument_name(sym: str, expiry_d: date, strike: float, leg: str) -> str:
    """
    Build a stable fixed-contract-like instrument name.

    A only needs this to distinguish one CE/PE contract from another. It does
    not require the exact exchange trading symbol.
    """
    k = int(round(float(strike)))
    return f"{sym}{expiry_d.strftime('%Y%m%d')}_{k}{leg}"


def _to_a_compatible_schema(df: pd.DataFrame, sym: str, actual_expiry: date, scheduled_expiry: date) -> pd.DataFrame:
    """
    Add/normalize all columns expected by `atm_straddle_claude_reattempt.py`.

    Required by A:
        date, name, type, option_type, strike, expiry, instrument,
        high, low, close

    Extra diagnostic columns are retained because they are useful and A ignores
    unknown columns.
    """
    if df.empty:
        return df

    out = df.copy()

    # Numeric cleanup. Bad rows are removed before writing because A assumes
    # strike/high/low/close are usable numbers.
    for c in ["open", "high", "low", "close", "volume", "oi", "iv", "strike", "spot"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["dt_ist", "strike", "high", "low", "close", "leg"]).copy()
    if out.empty:
        return out

    # Canonical date/DTE fields.
    out["date"] = out["dt_ist"]                       # A expects this exact column.
    out["name"] = sym                                  # A normalizes this to NIFTY/SENSEX.
    out["type"] = "OPTION"                             # A filters type == OPTION.
    out["option_type"] = out["leg"].astype(str).str.upper()  # CE / PE.
    out["expiry"] = actual_expiry                       # A converts this with pd.to_datetime.
    out["expiry_date"] = actual_expiry                  # Diagnostic/explicit actual expiry.
    out["scheduled_expiry"] = scheduled_expiry          # Useful when holiday-shifted.
    out["dte"] = out["date_ist"].map(lambda d: int((actual_expiry - d).days))
    out["source"] = "DHAN_ROLLINGOPTION"

    # Fixed-contract-like identifier used by A's _pick_symbol/_build_leg_series.
    out["instrument"] = [
        _make_instrument_name(sym, actual_expiry, k, leg)
        for k, leg in zip(out["strike"].tolist(), out["option_type"].tolist())
    ]

    # Deduplicate possible overlaps caused by rolling selectors.
    # If the same fixed instrument appears at the same minute from more than one
    # selector, keep the row whose selector is closest to ATM.
    out["abs_strike_offset"] = out["strike_offset"].abs()
    out = out.sort_values(
        ["date", "instrument", "abs_strike_offset", "strike_offset"],
        ascending=[True, True, True, True],
    )
    out = out.drop_duplicates(subset=["date", "instrument"], keep="first")

    # Final safety check for A's minimum required columns.
    required_for_a = ["date", "name", "type", "option_type", "strike", "expiry", "instrument", "high", "low", "close"]
    missing = [c for c in required_for_a if c not in out.columns]
    if missing:
        raise RuntimeError(f"A-compatible output missing required columns: {missing}")

    # Put A-critical columns first, diagnostics after.
    front = required_for_a + [
        "open", "volume", "oi", "iv", "spot",
        "timestamp", "dt_ist", "date_ist", "timestamp_str",
        "symbol", "exchangeSegment", "scheduled_expiry", "expiry_date", "dte",
        "strikeSelector", "strike_offset", "abs_strike_offset", "source",
    ]
    cols = [c for c in front if c in out.columns] + [c for c in out.columns if c not in front]
    return out[cols].reset_index(drop=True)


# =============================================================================
# STRADDLE-MELT EXPIRY VERIFICATION
# =============================================================================
def _straddle_melt_metrics(df: pd.DataFrame) -> Optional[dict]:
    """
    Compute ATM straddle residual time value near close.

    This is evaluated only on DTE=0 rows and only for strike_offset==0.
    It helps detect a bad rolling-window/expiryCode combination.
    """
    d0 = df[(df.get("dte") == 0) & (df.get("strike_offset") == 0)]
    if d0.empty:
        return None

    ce = (
        d0[d0["leg"] == "CE"][["timestamp", "open", "close", "strike", "spot"]]
        .rename(columns={"open": "ce_o", "close": "ce_c", "strike": "ce_k"})
    )
    pe = (
        d0[d0["leg"] == "PE"][["timestamp", "open", "close", "strike"]]
        .rename(columns={"open": "pe_o", "close": "pe_c", "strike": "pe_k"})
    )

    m = ce.merge(pe, on="timestamp", how="inner").sort_values("timestamp")
    if len(m) < MELT_MIN_BARS:
        return None

    straddle_open = float((m["ce_o"] + m["pe_o"]).iloc[:3].median())
    tail = m.tail(5)
    straddle_close = float((tail["ce_c"] + tail["pe_c"]).median())
    spot_close = float(tail["spot"].median())
    atm_strike = float(tail["ce_k"].median())
    intrinsic_close = abs(spot_close - atm_strike)
    residual_tv = max(0.0, straddle_close - intrinsic_close)
    frac = (residual_tv / straddle_open) if straddle_open > 0 else float("nan")

    return {
        "atm_straddle_open": round(straddle_open, 2),
        "atm_straddle_close": round(straddle_close, 2),
        "intrinsic_close": round(intrinsic_close, 2),
        "residual_tv_close": round(residual_tv, 2),
        "residual_tv_frac": round(frac, 3),
    }


def _check_melt(metrics: Optional[dict], sym: str, expiry_d: date) -> bool:
    """Print and optionally enforce the expiry-day straddle-melt check."""
    if metrics is None:
        print(f"[MELT?] {sym} {expiry_d}: too few ATM D0 bars to verify melt")
        return False

    frac = metrics["residual_tv_frac"]
    ok = (frac == frac) and (frac <= MELT_MAX_RESIDUAL_FRAC)
    print(
        f"[{'MELT-OK' if ok else 'MELT-FAIL'}] {sym} {expiry_d}: "
        f"residual_tv={metrics['residual_tv_close']} frac={frac} "
        f"(open={metrics['atm_straddle_open']} close={metrics['atm_straddle_close']})"
    )

    if not ok and MELT_STRICT:
        raise RuntimeError(
            f"straddle-melt check failed for {sym} {expiry_d}: "
            f"residual_tv_frac={frac} > {MELT_MAX_RESIDUAL_FRAC}; likely not an expiry session"
        )

    return ok


# =============================================================================
# COVERAGE DIAGNOSTICS
# =============================================================================
def _expected_session_minutes(dy: date) -> int:
    """Expected Indian cash/F&O minute bars from 09:15 to 15:30 inclusive."""
    start = pd.Timestamp(f"{dy} 09:15:00", tz=TIMEZONE_IST)
    end = pd.Timestamp(f"{dy} 15:30:00", tz=TIMEZONE_IST)
    return len(pd.date_range(start, end, freq="1min"))


def _coverage_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Produce a fixed-strike coverage summary.

    A can still run if some contracts have incomplete coverage, because it uses
    forward fill after entry. But incomplete coverage must be visible because it
    can distort stop-loss/profit-target detection.
    """
    if df.empty:
        return pd.DataFrame()

    gcols = ["name", "date_ist", "expiry", "dte", "strike", "option_type", "instrument"]
    tmp = df.copy()
    tmp["minute"] = pd.to_datetime(tmp["date"], errors="coerce")

    out = (
        tmp.groupby(gcols, dropna=False)
        .agg(
            bars=("minute", "nunique"),
            first_bar=("minute", "min"),
            last_bar=("minute", "max"),
            min_close=("close", "min"),
            max_close=("close", "max"),
        )
        .reset_index()
    )

    out["expected_bars"] = out["date_ist"].map(_expected_session_minutes)
    out["coverage_pct"] = (100.0 * out["bars"] / out["expected_bars"]).round(2)
    out = out.sort_values(["date_ist", "strike", "option_type"]).reset_index(drop=True)
    return out


# =============================================================================
# FETCHER
# =============================================================================
@dataclass(frozen=True)
class SymbolCfg:
    symbol: str
    exchange_segment: str
    underlying_security_id: int


class ExpiryDayDownloader:
    """Dhan rolling-option downloader that writes A-compatible pickles."""

    def __init__(self):
        if not ACCESS_TOKEN or not CLIENT_ID:
            raise SystemExit(
                "Missing Dhan credentials. Set DHAN_ACCESS_TOKEN and DHAN_CLIENT_ID environment variables."
            )

        self.session = requests.Session()
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": ACCESS_TOKEN,
            "client-id": CLIENT_ID,
        }

    def resolve_underlying_ids(self) -> Dict[str, SymbolCfg]:
        """Resolve NIFTY 50 and SENSEX underlying security IDs from IDX_I."""
        r = self.session.get(
            IDX_I_URL,
            headers={"access-token": ACCESS_TOKEN, "client-id": CLIENT_ID},
            timeout=60,
        )
        r.raise_for_status()

        rows = list(csv.DictReader(r.text.splitlines()))
        if not rows:
            raise RuntimeError("IDX_I returned no rows.")

        cols = [c.strip() for c in rows[0].keys()]

        def pick(cands: List[str], subs: List[str]) -> Optional[str]:
            for c in cands:
                if c in cols:
                    return c
            for c in cols:
                if any(s.lower() in c.lower() for s in subs):
                    return c
            return None

        sec_col = pick(["SECURITY_ID", "security_id"], ["security"])
        name_col = pick(["DISPLAY_NAME", "SYMBOL_NAME", "TRADING_SYMBOL"], ["display", "symbol", "trading"])

        if not sec_col or not name_col:
            raise RuntimeError(f"IDX_I columns not detected: {cols}")

        out: Dict[str, SymbolCfg] = {}
        for sym, meta in SYMBOLS.items():
            best, best_sc = None, -1e9
            for row in rows:
                sc = _score_index_row(meta["idx_name"], row.get(name_col, ""))
                if sc > best_sc:
                    best_sc, best = sc, row

            if not best or best_sc < 100:
                raise RuntimeError(f"Could not resolve underlying id for {sym}")

            out[sym] = SymbolCfg(
                symbol=sym,
                exchange_segment=meta["exchangeSegment"],
                underlying_security_id=int(float(str(best[sec_col]).strip())),
            )

        return out

    def _fetch_window(self, cfg: SymbolCfg, hint_expiry: date, strike_sel: str) -> Tuple[pd.DataFrame, Optional[date]]:
        """
        Fetch CE+PE for one rolling strike selector.

        Steps:
        1. Fetch [hint-WINDOW_BACK_DAYS, hint+1) using expiryCode.
        2. Drop rows after the hint.
        3. Resolve actual expiry as max(date_ist) in returned data.
        4. Keep only rows whose DTE is configured in KEEP_DTE.
        """
        from_date = (hint_expiry - timedelta(days=WINDOW_BACK_DAYS)).isoformat()
        to_date = (hint_expiry + timedelta(days=1)).isoformat()

        base = {
            "exchangeSegment": cfg.exchange_segment,
            "interval": "1",
            "securityId": cfg.underlying_security_id,
            "instrument": "OPTIDX",
            "expiryFlag": "WEEK",
            "expiryCode": EXPIRY_CODE,
            "strike": strike_sel,
            "requiredData": REQUIRED_DATA,
            "fromDate": from_date,
            "toDate": to_date,
        }

        legs: List[pd.DataFrame] = []
        for leg in ("CALL", "PUT"):
            payload = dict(base)
            payload["drvOptionType"] = leg
            j = _post_json(self.session, ROLLING_URL, self.headers, payload)
            time.sleep(SLEEP_BETWEEN_CALLS)
            leg_df = _leg_to_df(j, leg)
            if not leg_df.empty:
                legs.append(leg_df)

        if not legs:
            return pd.DataFrame(), None

        df = pd.concat(legs, ignore_index=True)
        df = df[df["date_ist"] <= hint_expiry].copy()
        if df.empty:
            return pd.DataFrame(), None

        actual_expiry = df["date_ist"].max()
        df["dte"] = df["date_ist"].map(lambda d: int((actual_expiry - d).days))
        df = df[df["dte"].isin(KEEP_DTE)].copy()
        if df.empty:
            return pd.DataFrame(), actual_expiry

        df["strikeSelector"] = strike_sel
        df["strike_offset"] = _strike_offset(strike_sel)
        return df, actual_expiry

    def resolve_actual_expiry(self, cfg: SymbolCfg, scheduled: date) -> Tuple[Optional[date], pd.DataFrame]:
        """
        Resolve actual expiry by probing ATM data.

        If scheduled expiry is a holiday, max(date_ist) from Dhan data typically
        becomes the previous trading day. If the whole window is empty, shift
        the hint backwards up to MAX_SHIFT_BACK_DAYS.
        """
        for shift in range(0, MAX_SHIFT_BACK_DAYS + 1):
            hint = scheduled - timedelta(days=shift)
            if hint.weekday() >= 5:
                continue

            try:
                df, actual = self._fetch_window(cfg, hint, "ATM")
            except RuntimeError as e:
                if _is_no_data(e):
                    continue
                raise

            if actual is not None and not df.empty:
                if actual != scheduled:
                    print(f"[HOLIDAY-SHIFT] {cfg.symbol} {scheduled} -> {actual}")
                return actual, df

        return None, pd.DataFrame()

    def fetch_expiry(self, cfg: SymbolCfg, scheduled: date) -> Optional[pd.DataFrame]:
        """
        Fetch all configured rolling selectors for one scheduled expiry and
        return one A-compatible DataFrame.
        """
        actual, atm = self.resolve_actual_expiry(cfg, scheduled)
        if actual is None or atm.empty:
            return None

        parts: List[pd.DataFrame] = [atm]

        for sel in _strike_selectors(STRIKE_BAND):
            if sel == "ATM":
                continue

            try:
                df, d0 = self._fetch_window(cfg, actual, sel)
            except RuntimeError as e:
                if _is_no_data(e):
                    continue
                raise

            # Only accept data that resolves to the same actual expiry.
            if not df.empty and d0 == actual:
                parts.append(df)

        raw = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        if raw.empty:
            return None

        raw["symbol"] = cfg.symbol
        raw["exchangeSegment"] = cfg.exchange_segment

        # Convert into A's expected schema and deduplicate fixed instrument rows.
        out = _to_a_compatible_schema(raw, cfg.symbol, actual, scheduled)
        if out.empty:
            return None

        # Attach DTE=0 straddle-melt metrics to every row of this expiry file.
        # Verification is anchored to the expiry session ONLY; the DTE=1 rows
        # (if KEEP_DTE includes 1) carry full time value and are not melt-checked.
        metrics = _straddle_melt_metrics(out)
        for kk, vv in (metrics or {}).items():
            out[kk] = vv
        out["melt_ok"] = _check_melt(metrics, cfg.symbol, actual)

        # Sort in a way that is stable and convenient for A.
        out = out.sort_values(["date", "strike", "option_type", "instrument"]).reset_index(drop=True)
        return out


# =============================================================================
# OUTPUT
# =============================================================================
def _dte_tag() -> str:
    return "_".join(str(x) for x in KEEP_DTE)


def _out_path(sym: str, expiry_d: date) -> str:
    """A-compatible pickle filename; one file per symbol-expiry."""
    return os.path.join(
        OUT_DIR,
        f"{sym}_EXP_{expiry_d.strftime('%Y%m%d')}_DTE{_dte_tag()}_ATMpm{STRIKE_BAND}_Acompat.pkl",
    )


def _coverage_path(pickle_path: str) -> str:
    base, _ = os.path.splitext(pickle_path)
    return base + "_coverage.csv"


def _write_pickle(df: pd.DataFrame, path: str) -> None:
    """Atomic-ish pickle write: temp file then rename."""
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        pickle.dump(df, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, path)


def _write_coverage_csv(df: pd.DataFrame, path: str) -> None:
    """Write optional fixed-strike coverage summary."""
    if not WRITE_COVERAGE_CSV:
        return
    cov = _coverage_summary(df)
    if not cov.empty:
        cov.to_csv(_coverage_path(path), index=False)


# =============================================================================
# MAIN
# =============================================================================
def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)

    end = date.today()
    start = end - timedelta(days=LOOKBACK_DAYS)

    dl = ExpiryDayDownloader()
    cfgs = dl.resolve_underlying_ids()

    print(f"[CONFIG] OUT_DIR={os.path.abspath(OUT_DIR)}")
    print(f"[CONFIG] KEEP_DTE={KEEP_DTE} | STRIKE_BAND=ATM±{STRIKE_BAND} | EXPIRY_CODE={EXPIRY_CODE}")
    print("[CONFIG] Output schema: A-compatible for atm_straddle_claude_reattempt.py")

    for sym in ("NIFTY", "SENSEX"):
        cfg = cfgs[sym]
        expiries = generate_expiries(sym, start, end)
        if PROCESS_NEWEST_FIRST:
            expiries = list(reversed(expiries))

        print(
            f"[INFO] {sym}: scheduled expiries={len(expiries)} "
            f"window=-{WINDOW_BACK_DAYS}d -> {os.path.abspath(OUT_DIR)}"
        )

        for scheduled in expiries:
            try:
                df = dl.fetch_expiry(cfg, scheduled)
            except Exception as e:
                print(f"[ERR ] {sym} {scheduled}: {e}")
                continue

            if df is None or df.empty:
                print(f"[WARN] {sym} {scheduled}: no data; skipped")
                continue

            actual = df["expiry_date"].iloc[0]
            if isinstance(actual, pd.Timestamp):
                actual = actual.date()

            path = _out_path(sym, actual)
            if os.path.exists(path):
                print(f"[SKIP] exists: {os.path.basename(path)}")
                continue

            _write_pickle(df, path)
            _write_coverage_csv(df, path)

            # Report fixed-strike coverage at a high level.
            n_strikes = int(pd.Series(df["strike"]).nunique())
            n_instruments = int(pd.Series(df["instrument"]).nunique())
            n_days = int(pd.Series(df["date_ist"]).nunique())
            print(
                f"[OK  ] {sym} expiry={actual} rows={len(df)} "
                f"days={n_days} strikes={n_strikes} instruments={n_instruments} "
                f"file={os.path.basename(path)}"
            )


# =============================================================================
# OFFLINE SELFTEST
# =============================================================================
def _selftest() -> None:
    """Offline checks: calendar rules and A-compatible schema conversion."""
    def has(sym: str, y: int, m: int, d: int) -> None:
        assert date(y, m, d) in generate_expiries(sym, date(y, 1, 1), date(2026, 6, 30)), \
            f"{sym}: expected expiry {date(y, m, d)} not generated"

    def absent(sym: str, y: int, m: int, d: int) -> None:
        assert date(y, m, d) not in generate_expiries(sym, date(y, 1, 1), date(2026, 6, 30)), \
            f"{sym}: {date(y, m, d)} should NOT be an expiry"

    has("NIFTY", 2024, 1, 4)
    has("NIFTY", 2025, 8, 28)
    has("NIFTY", 2025, 9, 2)
    absent("NIFTY", 2025, 9, 4)
    has("SENSEX", 2024, 12, 27)
    has("SENSEX", 2025, 1, 7)
    has("SENSEX", 2025, 9, 4)

    # Synthetic schema conversion check.
    ts = pd.date_range("2026-06-18 09:15", periods=3, freq="1min", tz=TIMEZONE_IST)
    raw = pd.DataFrame({
        "timestamp": [int(x.timestamp()) for x in ts] * 2,
        "open": [10, 11, 12, 9, 8, 7],
        "high": [11, 12, 13, 10, 9, 8],
        "low": [9, 10, 11, 8, 7, 6],
        "close": [10.5, 11.5, 12.5, 8.5, 7.5, 6.5],
        "volume": [100] * 6,
        "oi": [1000] * 6,
        "iv": [12] * 6,
        "strike": [24000] * 6,
        "spot": [24010] * 6,
        "dt_ist": list(ts) * 2,
        "date_ist": [date(2026, 6, 18)] * 6,
        "timestamp_str": [x.strftime("%Y-%m-%d %H:%M:%S") for x in ts] * 2,
        "leg": ["CE", "CE", "CE", "PE", "PE", "PE"],
        "strikeSelector": ["ATM"] * 6,
        "strike_offset": [0] * 6,
        "symbol": ["NIFTY"] * 6,
        "exchangeSegment": ["NSE_FNO"] * 6,
    })
    out = _to_a_compatible_schema(raw, "NIFTY", date(2026, 6, 18), date(2026, 6, 18))
    required = ["date", "name", "type", "option_type", "strike", "expiry", "instrument", "high", "low", "close"]
    assert all(c in out.columns for c in required), out.columns.tolist()
    assert set(out["option_type"]) == {"CE", "PE"}
    assert out["type"].eq("OPTION").all()
    assert out["name"].eq("NIFTY").all()

    print("SELFTEST OK")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true", help="run offline calendar and schema tests only")
    args = ap.parse_args()

    if args.selftest:
        _selftest()
    else:
        main()

"""
backtest_cpr_spike_straddle_oUtils_kite_v3.py

Fixes for suspicious 100% win-rate:
1) NO "close-only" exits: uses intrabar HIGH/LOW to trigger STOP/TARGET (conservative).
2) Trade strike is FIXED after entry: PnL is computed on the SAME strike throughout.
3) CE & PE always taken from the SAME strike (no mixed strikes).
4) Baseline median uses ONLY prior minutes (no leakage of current minute into baseline).
5) Fetches SPOT 1-min + DAILY (for CPR) from Kite.
6) Uses Kite init via oUtils.intialize_kite_api() (fallback initialize_kite_api()).
7) Includes pickle compatibility loader for numpy.core vs numpy._core issues.

Outputs:
  out/trades.csv
  out/summary.csv

Run:
  python backtest_cpr_spike_straddle_oUtils_kite_v3.py --data_dir . --out_dir out

Notes:
- Qty here is your TOTAL quantity (your "lots" terminology).
- STOPLOSS is ₹6000 default (configurable).
- Target default ₹3000 (configurable).
"""

from __future__ import annotations

import argparse
import gzip
import logging
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import deque
import statistics

import pandas as pd
import pytz

# =============================================================================
# IMPORT oUtils (match your environment)
# =============================================================================
try:
    import Trading_2024.OptionTradeUtils as oUtils
except Exception:
    import OptionTradeUtils as oUtils  # type: ignore


# =============================================================================
# YOUR FIXED RULES
# =============================================================================

INDIA_TZ = pytz.timezone("Asia/Kolkata")

QTY_BY_UNDERLYING = {"NIFTY": 650, "BANKNIFTY": 240, "SENSEX": 200}
STRIKE_STEP_BY_UNDERLYING = {"NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100}
NEAR_CPR_POINTS_BY_UNDERLYING = {"NIFTY": 10.0, "BANKNIFTY": 40.0, "SENSEX": 80.0}

SPOT_SYMBOL_BY_UNDERLYING = {
    "NIFTY": "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK",
    "SENSEX": "BSE:SENSEX",
}

DEFAULT_JUMP_PCT = 0.05
DEFAULT_BASELINE_WINDOW_MIN = 20
DEFAULT_MIN_BASE_SAMPLES = 10

DEFAULT_PROFIT_TARGET_RS = 9000.0
DEFAULT_STOPLOSS_RS = 6000.0
DEFAULT_TIME_STOP_MIN = 20
DEFAULT_COOLDOWN_MIN = 10

SESSION_START = "09:15"
SESSION_END = "15:29"
EOD_EXIT_TIME = "15:25"

MAX_STRIKE_HOPS = 3  # ONLY used for SIGNAL strike discovery if exact ATM strike is missing


# =============================================================================
# LOGGING
# =============================================================================

def setup_logger(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("backtest")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(h)
    return logger


log = setup_logger("INFO")


# =============================================================================
# PICKLE COMPAT LOADER (numpy.core vs numpy._core)
# =============================================================================

_NUMPY_MODULE_MAP = {
    "numpy._core.numeric": "numpy.core.numeric",
    "numpy._core.multiarray": "numpy.core.multiarray",
    "numpy._core._multiarray_umath": "numpy.core._multiarray_umath",
    "numpy._core.umath": "numpy.core.umath",
    "numpy.core.numeric": "numpy._core.numeric",
    "numpy.core.multiarray": "numpy._core.multiarray",
    "numpy.core._multiarray_umath": "numpy._core._multiarray_umath",
    "numpy.core.umath": "numpy._core.umath",
}

class _NumpyCompatUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        module2 = _NUMPY_MODULE_MAP.get(module, module)
        return super().find_class(module2, name)

def read_pickle_compat(path: Path):
    try:
        return pd.read_pickle(path)
    except ModuleNotFoundError as e:
        msg = str(e)
        if "numpy._core" not in msg and "numpy.core" not in msg:
            raise

        with open(path, "rb") as f:
            head = f.read(2)
        opener = gzip.open if head == b"\x1f\x8b" else open
        with opener(path, "rb") as f:
            return _NumpyCompatUnpickler(f).load()


# =============================================================================
# CPR MODEL
# =============================================================================

@dataclass(frozen=True)
class CPR:
    P: float
    BC: float
    TC: float
    R1: float
    S1: float

    def levels(self) -> List[Tuple[str, float]]:
        return [("R1", self.R1), ("BC", self.BC), ("P", self.P), ("TC", self.TC), ("S1", self.S1)]


def compute_cpr(H: float, L: float, C: float) -> CPR:
    P = (H + L + C) / 3.0
    BC = (H + L) / 2.0
    TC = 2.0 * P - BC
    R1 = 2.0 * P - L
    S1 = 2.0 * P - H
    return CPR(P=P, BC=BC, TC=TC, R1=R1, S1=S1)


# =============================================================================
# NORMALIZATION HELPERS
# =============================================================================

def norm_underlying(raw: str) -> str:
    x = str(raw).upper().strip()
    aliases = {
        "NIFTY 50": "NIFTY",
        "NIFTY": "NIFTY",
        "NIFTY BANK": "BANKNIFTY",
        "BANKNIFTY": "BANKNIFTY",
        "SENSEX": "SENSEX",
        "BSE SENSEX": "SENSEX",
    }
    return aliases.get(x, x)


def ensure_ist(ts: pd.Timestamp) -> pd.Timestamp:
    ts = pd.to_datetime(ts)
    if ts.tzinfo is None:
        return ts.tz_localize(INDIA_TZ)
    return ts.tz_convert(INDIA_TZ)


def within_session(ts: pd.Timestamp) -> bool:
    ts = ensure_ist(ts)
    hhmm = ts.strftime("%H:%M")
    return SESSION_START <= hhmm <= SESSION_END


def round_to_step(price: float, step: int) -> int:
    return int(round(price / step) * step)


# =============================================================================
# KITE INIT VIA oUtils
# =============================================================================

def get_kite_via_oUtils():
    if hasattr(oUtils, "intialize_kite_api"):
        log.info("[STEP] Initializing Kite via oUtils.intialize_kite_api()")
        return oUtils.intialize_kite_api()
    if hasattr(oUtils, "initialize_kite_api"):
        log.info("[STEP] Initializing Kite via oUtils.initialize_kite_api()")
        return oUtils.initialize_kite_api()
    raise RuntimeError("oUtils missing intialize_kite_api()/initialize_kite_api(). Check OptionTradeUtils.")


# =============================================================================
# KITE FETCH HELPERS + CACHES
# =============================================================================

TOKEN_CACHE: Dict[str, int] = {}
SPOT1M_CACHE: Dict[Tuple[str, str, str], pd.DataFrame] = {}
DAILY_CACHE: Dict[Tuple[str, str, str], pd.DataFrame] = {}

def _cache_key(underlying: str, from_ts: pd.Timestamp, to_ts: pd.Timestamp) -> Tuple[str, str, str]:
    # cache by day boundaries to improve reuse across pickles
    a = ensure_ist(from_ts).floor("D").isoformat()
    b = ensure_ist(to_ts).floor("D").isoformat()
    return (underlying, a, b)

def get_spot_token(kite, underlying: str) -> int:
    if underlying in TOKEN_CACHE:
        return TOKEN_CACHE[underlying]
    sym = SPOT_SYMBOL_BY_UNDERLYING[underlying]
    tok = int(kite.ltp([sym])[sym]["instrument_token"])
    TOKEN_CACHE[underlying] = tok
    return tok

def fetch_historical_in_chunks(kite, token: int, from_ts: pd.Timestamp, to_ts: pd.Timestamp,
                               interval: str, chunk_days: int) -> pd.DataFrame:
    from_ts = ensure_ist(from_ts)
    to_ts = ensure_ist(to_ts)
    rows: List[dict] = []
    cur = from_ts
    while cur < to_ts:
        end = min(cur + pd.Timedelta(days=chunk_days), to_ts)
        part = kite.historical_data(
            instrument_token=token,
            from_date=cur.to_pydatetime(),
            to_date=end.to_pydatetime(),
            interval=interval,
            continuous=False,
            oi=False,
        )
        if part:
            rows.extend(part)
        cur = end
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    if df["date"].dt.tz is None:
        df["date"] = df["date"].dt.tz_localize(INDIA_TZ)
    else:
        df["date"] = df["date"].dt.tz_convert(INDIA_TZ)
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return df

def get_spot_1m(kite, underlying: str, from_ts: pd.Timestamp, to_ts: pd.Timestamp) -> pd.DataFrame:
    key = _cache_key(underlying, from_ts, to_ts)
    if key in SPOT1M_CACHE:
        return SPOT1M_CACHE[key]
    tok = get_spot_token(kite, underlying)
    df = fetch_historical_in_chunks(kite, tok, from_ts, to_ts, interval="minute", chunk_days=20)
    df = df[df["date"].map(within_session)].copy()
    df["day"] = df["date"].dt.normalize()
    SPOT1M_CACHE[key] = df
    return df

def get_daily(kite, underlying: str, from_ts: pd.Timestamp, to_ts: pd.Timestamp) -> pd.DataFrame:
    key = _cache_key(underlying, from_ts, to_ts)
    if key in DAILY_CACHE:
        return DAILY_CACHE[key]
    tok = get_spot_token(kite, underlying)
    df = fetch_historical_in_chunks(kite, tok, from_ts, to_ts, interval="day", chunk_days=180)
    if not df.empty:
        df["day"] = df["date"].dt.normalize()
    DAILY_CACHE[key] = df
    return df

def build_cpr_map(daily_df: pd.DataFrame, trade_days: List[pd.Timestamp]) -> Dict[pd.Timestamp, CPR]:
    if daily_df.empty:
        return {}
    daily = daily_df.set_index("day", drop=False).sort_index()
    out: Dict[pd.Timestamp, CPR] = {}
    for d in trade_days:
        d = ensure_ist(d).normalize()
        prev = daily.index[daily.index < d]
        if len(prev) == 0:
            continue
        pd_day = prev.max()
        r = daily.loc[pd_day]
        out[d] = compute_cpr(float(r["high"]), float(r["low"]), float(r["close"]))
    return out


# =============================================================================
# OPTIONS INDEXING
# =============================================================================

def index_options(opt: pd.DataFrame) -> pd.DataFrame:
    df = opt.copy()
    df["date"] = pd.to_datetime(df["date"])
    if df["date"].dt.tz is None:
        df["date"] = df["date"].dt.tz_localize(INDIA_TZ)
    else:
        df["date"] = df["date"].dt.tz_convert(INDIA_TZ)
    df = df[df["date"].map(within_session)].copy()

    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["option_type"] = df["option_type"].astype(str).str.upper().str.strip()
    df = df[df["option_type"].isin(["CE", "PE"])].dropna(subset=["date", "strike", "open", "high", "low", "close"])

    df = df.sort_values("date").drop_duplicates(subset=["date", "strike", "option_type"], keep="last")
    return df.set_index(["date", "strike", "option_type"]).sort_index()

def get_straddle_bar_exact(opt_idx: pd.DataFrame, ts: pd.Timestamp, strike: int) -> Optional[dict]:
    """
    Strict: must use EXACT strike for both CE and PE.
    Returns dict with ce_*, pe_*, and straddle_* prices.
    """
    ts = ensure_ist(ts)
    k = float(strike)
    try:
        ce = opt_idx.loc[(ts, k, "CE")]
        pe = opt_idx.loc[(ts, k, "PE")]
        # each is a Series with open/high/low/close
        return {
            "strike": strike,
            "ce_open": float(ce["open"]), "ce_high": float(ce["high"]), "ce_low": float(ce["low"]), "ce_close": float(ce["close"]),
            "pe_open": float(pe["open"]), "pe_high": float(pe["high"]), "pe_low": float(pe["low"]), "pe_close": float(pe["close"]),
            "str_open": float(ce["open"]) + float(pe["open"]),
            "str_high": float(ce["high"]) + float(pe["high"]),
            "str_low":  float(ce["low"])  + float(pe["low"]),
            "str_close":float(ce["close"])+ float(pe["close"]),
        }
    except KeyError:
        return None

def get_straddle_bar_same_strike_near_atm(opt_idx: pd.DataFrame, ts: pd.Timestamp, atm: int,
                                          step: int, max_hops: int) -> Optional[dict]:
    """
    Used ONLY for SIGNAL discovery if exact ATM strike is missing.
    Ensures CE & PE are from SAME strike.
    """
    ts = ensure_ist(ts)
    candidates = [atm]
    for hop in range(1, max_hops + 1):
        candidates.extend([atm + hop * step, atm - hop * step])
    for k in candidates:
        bar = get_straddle_bar_exact(opt_idx, ts, k)
        if bar is not None:
            return bar
    return None


# =============================================================================
# BACKTEST
# =============================================================================

def backtest_one_pickle(kite, pkl_path: Path,
                        jump_pct: float, baseline_window_min: int, min_base_samples: int,
                        profit_target_rs: float, stoploss_rs: float,
                        time_stop_min: int, cooldown_min: int) -> pd.DataFrame:

    log.info(f"Loading pickle: {pkl_path.name}")
    df = read_pickle_compat(pkl_path)

    opt = df[df["type"].astype(str).str.upper() == "OPTION"].copy()
    if opt.empty:
        log.warning(f"{pkl_path.name}: no OPTION rows. Skipping.")
        return pd.DataFrame()

    underlying = norm_underlying(opt["name"].iloc[0])
    if underlying not in QTY_BY_UNDERLYING:
        log.warning(f"{pkl_path.name}: unsupported underlying '{underlying}'. Skipping.")
        return pd.DataFrame()

    qty = int(QTY_BY_UNDERLYING[underlying])
    step = int(STRIKE_STEP_BY_UNDERLYING[underlying])
    near_cpr_pts = float(NEAR_CPR_POINTS_BY_UNDERLYING[underlying])

    # Determine pickle date range (only to decide what to fetch from Kite)
    opt["date"] = pd.to_datetime(opt["date"])
    if opt["date"].dt.tz is None:
        opt["date"] = opt["date"].dt.tz_localize(INDIA_TZ)
    else:
        opt["date"] = opt["date"].dt.tz_convert(INDIA_TZ)
    opt = opt[opt["date"].map(within_session)].copy()
    if opt.empty:
        log.warning(f"{pkl_path.name}: no option candles in session window. Skipping.")
        return pd.DataFrame()

    min_ts = opt["date"].min()
    max_ts = opt["date"].max()

    spot_from = min_ts.floor("D")
    spot_to = max_ts.floor("D") + pd.Timedelta(days=1)

    log.info(
        f"{pkl_path.name}: underlying={underlying} qty={qty} step={step} nearCPR={near_cpr_pts} "
        f"range={min_ts} -> {max_ts}"
    )

    # Fetch spot 1-min and daily for CPR
    spot_1m = get_spot_1m(kite, underlying, spot_from, spot_to)
    if spot_1m.empty:
        log.error(f"{pkl_path.name}: empty SPOT 1-min from Kite. Skipping.")
        return pd.DataFrame()

    trade_days = sorted(spot_1m["day"].unique())

    daily = get_daily(kite, underlying, spot_from - pd.Timedelta(days=45), spot_to + pd.Timedelta(days=2))
    if daily.empty:
        log.error(f"{pkl_path.name}: empty DAILY from Kite. Skipping.")
        return pd.DataFrame()

    cpr_by_day = build_cpr_map(daily, trade_days)
    if not cpr_by_day:
        log.error(f"{pkl_path.name}: CPR map empty. Skipping.")
        return pd.DataFrame()

    # Index options once
    opt_idx = index_options(opt)
    opt_times = set(opt_idx.index.get_level_values(0).unique())

    trades: List[dict] = []

    # Convert fixed Rs thresholds into POINTS relative to qty (used for intrabar triggers)
    sl_points = stoploss_rs / qty
    tgt_points = profit_target_rs / qty

    for d in trade_days:
        if d not in cpr_by_day:
            continue
        cpr = cpr_by_day[d]

        sday = spot_1m[spot_1m["day"] == d].sort_values("date")
        if len(sday) < 2:
            continue

        times = sday["date"].to_list()
        spot_close_arr = sday["close"].to_numpy(dtype=float)

        # Baseline: rolling median of straddle CLOSE values from prior minutes
        win = deque(maxlen=baseline_window_min)

        in_trade = False
        cooldown_until: Optional[pd.Timestamp] = None

        entry_time: Optional[pd.Timestamp] = None
        entry_strike: Optional[int] = None
        entry_prem: Optional[float] = None
        entry_near_line: Optional[str] = None
        entry_dist: Optional[float] = None

        for i in range(len(times) - 1):
            ts = times[i]
            nxt = times[i + 1]
            spot_close = float(spot_close_arr[i])

            # Baseline should be based on PAST ONLY (no leakage)
            baseline = statistics.median(win) if len(win) >= min_base_samples else None

            # ---- If a trade is open, monitor using ENTRY STRIKE ONLY ----
            if in_trade:
                bar = get_straddle_bar_exact(opt_idx, ts, entry_strike)
                if bar is None:
                    # missing candles for entry strike -> skip this minute (log at DEBUG if needed)
                    continue

                # Conservative intrabar logic for a SHORT straddle:
                # - Worst case for you is straddle HIGH (premium spikes)
                # - Best case for you is straddle LOW (premium falls)
                str_high = bar["str_high"]
                str_low = bar["str_low"]
                str_close = bar["str_close"]

                stop_price = entry_prem + sl_points
                tgt_price = entry_prem - tgt_points

                hold_min = (ts - entry_time).total_seconds() / 60.0
                exit_reason = None
                exit_prem = None
                pnl_rs = None

                # If both target and stop occur in same candle, assume STOP first (conservative)
                if str_high >= stop_price:
                    exit_reason = "STOP"
                    exit_prem = stop_price
                    pnl_rs = -stoploss_rs
                elif str_low <= tgt_price:
                    exit_reason = "TARGET"
                    exit_prem = tgt_price
                    pnl_rs = profit_target_rs
                elif hold_min >= time_stop_min:
                    exit_reason = "TIME"
                    exit_prem = str_close
                    pnl_rs = (entry_prem - exit_prem) * qty
                elif ts.strftime("%H:%M") >= EOD_EXIT_TIME:
                    exit_reason = "EOD"
                    exit_prem = str_close
                    pnl_rs = (entry_prem - exit_prem) * qty

                if exit_reason:
                    trades.append({
                        "file": pkl_path.name,
                        "underlying": underlying,
                        "trade_day": str(d.date()),
                        "entry_time": entry_time,
                        "exit_time": ts,
                        "strike": entry_strike,
                        "qty": qty,
                        "entry_premium": entry_prem,
                        "exit_premium": exit_prem,
                        "pnl_rs": pnl_rs,
                        "exit_reason": exit_reason,
                        "entry_nearest_cpr": entry_near_line,
                        "entry_dist_to_cpr": entry_dist,
                    })
                    in_trade = False
                    cooldown_until = ts + pd.Timedelta(minutes=cooldown_min)

                # Do NOT update baseline using trade strike; baseline is based on SIGNAL premiums,
                # but we still want baseline to reflect the market. We’ll update baseline below
                # using current ATM straddle close if available.
                # Continue to baseline update logic.
                # (no 'continue' here)

            # ---- Compute SIGNAL premium at this minute (ATM based on spot close) ----
            if ts not in opt_times:
                continue

            atm = round_to_step(spot_close, step)
            sig_bar = get_straddle_bar_same_strike_near_atm(
                opt_idx=opt_idx, ts=ts, atm=atm, step=step, max_hops=MAX_STRIKE_HOPS
            )
            if sig_bar is None:
                continue

            sig_prem_close = sig_bar["str_close"]

            # Update baseline window using SIGNAL close (always, even during cooldown or in_trade)
            win.append(sig_prem_close)

            # If trade already open, don’t open a new one
            if in_trade:
                continue

            # Cooldown: block new entries
            if cooldown_until is not None and ts < cooldown_until:
                continue

            # Need baseline for signal
            if baseline is None or baseline <= 0:
                continue

            jump = (sig_prem_close - baseline) / baseline

            # Near CPR using spot close
            nearest_name, nearest_lvl = min(cpr.levels(), key=lambda kv: abs(spot_close - kv[1]))
            dist = abs(spot_close - nearest_lvl)
            near_cpr = dist <= near_cpr_pts

            if jump >= jump_pct and near_cpr:
                # Enter at next minute OPEN on the SAME strike used for signal (sig_bar["strike"])
                strike_for_trade = int(sig_bar["strike"])

                # Need nxt candle for entry strike
                entry_bar = get_straddle_bar_exact(opt_idx, nxt, strike_for_trade)
                if entry_bar is None:
                    continue

                entry_time = nxt
                entry_strike = strike_for_trade
                entry_prem = entry_bar["str_open"]  # next minute OPEN (no lookahead)
                entry_near_line = nearest_name
                entry_dist = dist
                in_trade = True

                log.info(
                    f"[ENTER] {pkl_path.name} {underlying} {entry_time} strike={entry_strike} "
                    f"prem={entry_prem:.2f} jump={jump*100:.1f}% nearCPR {nearest_name} "
                    f"dist={dist:.1f}(<= {near_cpr_pts}) qty={qty}"
                )

    return pd.DataFrame(trades)


# =============================================================================
# SUMMARY
# =============================================================================

def summarize(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame([{
            "trades": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "avg_pnl": 0.0,
            "median_pnl": 0.0,
            "max_win": 0.0,
            "max_loss": 0.0,
        }])

    pnl = trades["pnl_rs"].astype(float)
    return pd.DataFrame([{
        "trades": int(len(trades)),
        "win_rate": float((pnl > 0).mean()),
        "total_pnl": float(pnl.sum()),
        "avg_pnl": float(pnl.mean()),
        "median_pnl": float(pnl.median()),
        "max_win": float(pnl.max()),
        "max_loss": float(pnl.min()),
    }])


# =============================================================================
# RUNNER
# =============================================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data_dir", type=str, default=r"G:\My Drive\Trading\Historical_Options_Data", help="Folder containing *_minute.pkl")
    ap.add_argument("--out_dir", type=str, default=r"G:\My Drive\Trading\Historical_Options_Data", help="Output folder")
    ap.add_argument("--jump_pct", type=float, default=DEFAULT_JUMP_PCT)
    ap.add_argument("--baseline_window_min", type=int, default=DEFAULT_BASELINE_WINDOW_MIN)
    ap.add_argument("--min_base_samples", type=int, default=DEFAULT_MIN_BASE_SAMPLES)
    ap.add_argument("--profit_target_rs", type=float, default=DEFAULT_PROFIT_TARGET_RS)
    ap.add_argument("--stoploss_rs", type=float, default=DEFAULT_STOPLOSS_RS)
    ap.add_argument("--time_stop_min", type=int, default=DEFAULT_TIME_STOP_MIN)
    ap.add_argument("--cooldown_min", type=int, default=DEFAULT_COOLDOWN_MIN)
    ap.add_argument("--log_level", type=str, default="INFO")
    args = ap.parse_args()

    global log
    log = setup_logger(args.log_level)

    data_dir = Path(args.data_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    pkl_files = sorted(data_dir.glob("*_minute.pkl"))
    if not pkl_files:
        raise SystemExit(f"No *_minute.pkl found in {data_dir}")

    kite = get_kite_via_oUtils()
    log.info("[INFO] Kite initialized via oUtils.")

    all_trades = []
    for pkl in pkl_files:
        try:
            tdf = backtest_one_pickle(
                kite=kite,
                pkl_path=pkl,
                jump_pct=args.jump_pct,
                baseline_window_min=args.baseline_window_min,
                min_base_samples=args.min_base_samples,
                profit_target_rs=args.profit_target_rs,
                stoploss_rs=args.stoploss_rs,
                time_stop_min=args.time_stop_min,
                cooldown_min=args.cooldown_min,
            )
            if not tdf.empty:
                all_trades.append(tdf)
        except Exception as e:
            log.exception(f"Error processing {pkl.name}: {e}")

    trades = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    summary = summarize(trades)

    trades_csv = out_dir / "trades.csv"
    summary_csv = out_dir / "summary.csv"
    trades.to_csv(trades_csv, index=False)
    summary.to_csv(summary_csv, index=False)

    log.info(f"Saved trades:  {trades_csv}")
    log.info(f"Saved summary: {summary_csv}")

    if not trades.empty:
        log.info(
            f"TOTAL trades={len(trades)} | TOTAL PnL=₹{trades['pnl_rs'].sum():,.0f} | "
            f"WinRate={(trades['pnl_rs']>0).mean():.1%}"
        )
    else:
        log.info("No trades triggered with current parameters.")


if __name__ == "__main__":
    main()

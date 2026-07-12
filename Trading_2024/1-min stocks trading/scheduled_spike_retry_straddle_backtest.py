"""
scheduled_spike_retry_straddle_backtest.py  (Corrected + Optimized)

Backtest on your saved 1-minute OPTIONS pickle files (same schema),
while fetching SPOT 1-minute from Kite (because pickle has no spot series).

Implements:
- Entry after ENTRY_AFTER time
- Spike: current ATM-ish straddle close >= baseline*(1+JUMP_PCT)
  baseline = median of last N samples (NO leakage), tracked per (expiry,strike)
- Enter short straddle at NEXT minute OPEN on the SIGNAL strike (same CE/PE strike)
- Exit:
    * TARGET (₹): target_prem = entry_prem - target_points
    * HARD SL (₹): max loss cap (converted to premium points)
    * TRAILING giveback (₹): floor_pnl = max_profit_pnl - GIVEBACK_RS
      effective floor = max(floor_pnl, -STOPLOSS_RS)
      ==> if profit was +5000, floor becomes -1000; exit if pnl falls to -1000 or worse
- Retry:
    * only after a losing STOP/TRAIL exit
    * wait COOLDOWN_MIN minutes
    * max retries = MAX_RETRIES (total attempts/day = 1 + MAX_RETRIES)
- ARMED/RESET:
    * after an entry, disarm that (expiry,strike)
    * re-arm only when premium cools back near baseline: prem <= baseline*(1+RESET_PCT)

Key correctness fixes in this version:
✅ Expiry handled: option index includes expiry; nearest expiry chosen per trading day
✅ No intrabar look-ahead for trailing: trailing floor uses max_profit from previous bars only
✅ Faster baseline median (statistics.median)

Run example:
  python scheduled_spike_retry_straddle_backtest.py ^
    --pkl_dir "C:\\data\\options_pickles" ^
    --out_csv "trades_retry.csv" ^
    --entry_after "10:00" ^
    --jump_pct 0.05 --baseline_n 10 ^
    --target_rs 6000 --giveback_rs 6000 --stoploss_rs 6000 ^
    --cooldown_min 10 --max_retries 2 --max_hold_min 20 ^
    --reset_pct 0.01
"""

from __future__ import annotations

import argparse
import gzip
import logging
import pickle
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from collections import defaultdict, deque

import pandas as pd
import pytz

# -----------------------------------------------------------------------------
# Try importing your OptionTradeUtils (oUtils)
# -----------------------------------------------------------------------------
try:
    import Trading_2024.OptionTradeUtils as oUtils
except Exception:
    import OptionTradeUtils as oUtils  # type: ignore


# =============================================================================
# DEFAULTS / CONSTANTS
# =============================================================================

INDIA_TZ = pytz.timezone("Asia/Kolkata")

# Total quantities you use (NOT lot-count)
QTY_BY_UNDERLYING = {"NIFTY": 650, "BANKNIFTY": 240, "SENSEX": 200}

# Strike step for rounding ATM
STRIKE_STEP_BY_UNDERLYING = {"NIFTY": 50, "BANKNIFTY": 100, "SENSEX": 100}

# Spot symbols for Kite
SPOT_SYMBOL_BY_UNDERLYING = {
    "NIFTY": "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK",
    "SENSEX": "BSE:SENSEX",
}

SESSION_START = "09:15"
SESSION_END = "15:29"
EOD_EXIT_TIME = "15:25"

# When exact ATM strike row missing, probe nearby strikes
MAX_STRIKE_HOPS = 3


# =============================================================================
# LOGGING
# =============================================================================

def setup_logger(level: str = "INFO") -> logging.Logger:
    lg = logging.getLogger("retry_straddle")
    lg.setLevel(getattr(logging, level.upper(), logging.INFO))
    if not lg.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        lg.addHandler(h)
    return lg


log = setup_logger("INFO")


# =============================================================================
# PICKLE COMPAT (numpy._core vs numpy.core)
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
        return super().find_class(_NUMPY_MODULE_MAP.get(module, module), name)


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
# HELPERS
# =============================================================================

def ensure_ist(ts) -> pd.Timestamp:
    ts = pd.to_datetime(ts)
    if ts.tzinfo is None:
        return ts.tz_localize(INDIA_TZ)
    return ts.tz_convert(INDIA_TZ)


def within_session(ts: pd.Timestamp) -> bool:
    ts = ensure_ist(ts)
    hhmm = ts.strftime("%H:%M")
    return SESSION_START <= hhmm <= SESSION_END


def norm_underlying(name: str) -> str:
    x = str(name).strip().upper()
    aliases = {
        "NIFTY 50": "NIFTY",
        "NIFTY": "NIFTY",
        "NIFTY BANK": "BANKNIFTY",
        "BANKNIFTY": "BANKNIFTY",
        "BSE SENSEX": "SENSEX",
        "SENSEX": "SENSEX",
    }
    return aliases.get(x, x)


def round_to_step(price: float, step: int) -> int:
    return int(round(price / step) * step)


def median_last_n(dq: deque, n: int) -> Optional[float]:
    if len(dq) < n:
        return None
    # dq is small; slicing via list is fine
    vals = list(dq)[-n:]
    return float(statistics.median(vals))


# =============================================================================
# KITE (via oUtils) + SPOT fetch
# =============================================================================

def get_kite_via_oUtils():
    if hasattr(oUtils, "intialize_kite_api"):
        return oUtils.intialize_kite_api()
    if hasattr(oUtils, "initialize_kite_api"):
        return oUtils.initialize_kite_api()
    raise RuntimeError("oUtils missing intialize_kite_api()/initialize_kite_api()")


TOKEN_CACHE: Dict[str, int] = {}
SPOT1M_CACHE: Dict[Tuple[str, str, str], pd.DataFrame] = {}


def get_spot_token(kite, underlying: str) -> int:
    if underlying in TOKEN_CACHE:
        return TOKEN_CACHE[underlying]
    sym = SPOT_SYMBOL_BY_UNDERLYING[underlying]
    tok = int(kite.ltp([sym])[sym]["instrument_token"])
    TOKEN_CACHE[underlying] = tok
    return tok


def fetch_spot_1m(kite, underlying: str, from_ts: pd.Timestamp, to_ts: pd.Timestamp) -> pd.DataFrame:
    from_ts = ensure_ist(from_ts)
    to_ts = ensure_ist(to_ts)

    key = (underlying, from_ts.floor("D").isoformat(), to_ts.floor("D").isoformat())
    if key in SPOT1M_CACHE:
        return SPOT1M_CACHE[key]

    token = get_spot_token(kite, underlying)

    rows: List[dict] = []
    cur = from_ts
    while cur < to_ts:
        end = min(cur + pd.Timedelta(days=20), to_ts)
        part = kite.historical_data(
            instrument_token=token,
            from_date=cur.to_pydatetime(),
            to_date=end.to_pydatetime(),
            interval="minute",
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

    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    df = df[df["date"].map(within_session)].copy()
    df["day"] = df["date"].dt.normalize()

    SPOT1M_CACHE[key] = df
    return df


# =============================================================================
# OPTIONS indexing (from pickle)  -- FIXED: includes expiry
# =============================================================================

def index_options(opt: pd.DataFrame) -> pd.DataFrame:
    df = opt.copy()

    df["date"] = pd.to_datetime(df["date"])
    if df["date"].dt.tz is None:
        df["date"] = df["date"].dt.tz_localize(INDIA_TZ)
    else:
        df["date"] = df["date"].dt.tz_convert(INDIA_TZ)

    # expiry stored as date-like in your pickle; normalize to python date
    df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce").dt.date

    df = df[df["date"].map(within_session)].copy()

    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df["option_type"] = df["option_type"].astype(str).str.upper().str.strip()

    df = df[df["option_type"].isin(["CE", "PE"])].dropna(subset=["expiry", "strike", "open", "high", "low", "close"])
    df = df.sort_values("date").drop_duplicates(subset=["date", "expiry", "strike", "option_type"], keep="last")

    # MultiIndex includes expiry to prevent mixing multiple expiries
    return df.set_index(["date", "expiry", "strike", "option_type"]).sort_index()


def get_straddle_bar_exact(opt_idx: pd.DataFrame, ts: pd.Timestamp, expiry, strike: int) -> Optional[dict]:
    ts = ensure_ist(ts)
    k = float(strike)
    try:
        ce = opt_idx.loc[(ts, expiry, k, "CE")]
        pe = opt_idx.loc[(ts, expiry, k, "PE")]
        return {
            "strike": int(strike),
            "str_open": float(ce["open"]) + float(pe["open"]),
            "str_high": float(ce["high"]) + float(pe["high"]),
            "str_low": float(ce["low"]) + float(pe["low"]),
            "str_close": float(ce["close"]) + float(pe["close"]),
        }
    except KeyError:
        return None


def get_signal_straddle_near_atm(opt_idx: pd.DataFrame, ts: pd.Timestamp, expiry, atm: int, step: int, max_hops: int) -> Optional[dict]:
    candidates = [atm]
    for hop in range(1, max_hops + 1):
        candidates.extend([atm + hop * step, atm - hop * step])

    for k in candidates:
        bar = get_straddle_bar_exact(opt_idx, ts, expiry, k)
        if bar is not None:
            return bar
    return None


def nearest_expiry_for_day(expiries: List, day_date) -> Optional:
    future = [e for e in expiries if e is not None and e >= day_date]
    return min(future) if future else None


# =============================================================================
# BACKTEST CORE
# =============================================================================

@dataclass
class Trade:
    file: str
    underlying: str
    day: str
    expiry: str
    attempt_no: int

    entry_time: str
    entry_strike: int
    entry_prem: float

    exit_time: str
    exit_prem: float
    exit_reason: str
    pnl_rs: float

    # signal context
    jump_pct: float
    baseline: float
    atm_strike_signal: int
    signal_strike: int

    # trailing context
    max_profit_rs: float
    eff_floor_rs: float
    trailing_floor_rs: float


def backtest_pickle(
    kite,
    pkl_path: Path,
    entry_after_hhmm: str,
    jump_pct: float,
    baseline_n: int,
    qty_override: Optional[int],
    stoploss_rs: float,
    target_rs: float,
    giveback_rs: float,
    max_hold_min: int,
    cooldown_min: int,
    max_retries: int,
    reset_pct: float,
) -> List[Trade]:
    df = read_pickle_compat(pkl_path)

    opt = df[df["type"].astype(str).str.upper() == "OPTION"].copy()
    if opt.empty:
        log.warning(f"{pkl_path.name}: no OPTION rows; skipping.")
        return []

    underlying = norm_underlying(opt["name"].iloc[0])
    if underlying not in SPOT_SYMBOL_BY_UNDERLYING:
        log.warning(f"{pkl_path.name}: unsupported underlying {underlying}; skipping.")
        return []

    qty = int(qty_override) if qty_override else int(QTY_BY_UNDERLYING.get(underlying, 0))
    if qty <= 0:
        log.warning(f"{pkl_path.name}: qty not known for {underlying}; pass --qty_override.")
        return []

    step = int(STRIKE_STEP_BY_UNDERLYING.get(underlying, 100))

    opt_idx = index_options(opt)
    if opt_idx.empty:
        log.warning(f"{pkl_path.name}: empty options index after filtering.")
        return []

    # Pull expiry universe from index level
    expiries = sorted(set(opt_idx.index.get_level_values(1)))

    # Determine date window from options
    opt_times = opt_idx.index.get_level_values(0).unique()
    min_ts = ensure_ist(opt_times.min())
    max_ts = ensure_ist(opt_times.max())

    spot_from = min_ts.floor("D")
    spot_to = max_ts.floor("D") + pd.Timedelta(days=1)

    spot_1m = fetch_spot_1m(kite, underlying, spot_from, spot_to)
    if spot_1m.empty:
        log.warning(f"{pkl_path.name}: empty spot 1m; skipping.")
        return []

    entry_after_time = pd.to_datetime(entry_after_hhmm).time()

    # Convert ₹ targets to premium points
    stop_points = float(stoploss_rs) / float(qty)
    target_points = float(target_rs) / float(qty)

    trades: List[Trade] = []

    for day in sorted(spot_1m["day"].unique()):
        day_date = day.date()

        chosen_exp = nearest_expiry_for_day(expiries, day_date)
        if chosen_exp is None:
            continue

        sday = spot_1m[spot_1m["day"] == day].sort_values("date")
        if sday.empty:
            continue

        times = [ensure_ist(x) for x in sday["date"].to_list()]
        spot_close = sday["close"].astype(float).to_list()

        # baseline + armed are per (expiry,strike)
        base_by_key: Dict[Tuple, deque] = defaultdict(lambda: deque(maxlen=max(60, baseline_n + 15)))
        armed_by_key: Dict[Tuple, bool] = defaultdict(lambda: True)

        attempts_made = 0
        max_attempts = 1 + max_retries
        cooldown_until: Optional[pd.Timestamp] = None

        in_trade = False

        # Trade state
        entry_time: Optional[pd.Timestamp] = None
        entry_strike: Optional[int] = None
        entry_prem: Optional[float] = None

        # signal context
        entry_jump: float = 0.0
        entry_base: float = 0.0
        entry_atm: int = 0
        entry_sig_strike: int = 0

        # trailing context (₹) - IMPORTANT: this is only updated AFTER a bar completes with no exit
        max_profit_rs: float = 0.0

        for i in range(len(times) - 1):
            ts = times[i]
            nxt = times[i + 1]

            # Pick ATM, compute signal straddle for this timestamp (fresh)
            atm = round_to_step(float(spot_close[i]), step)
            sig_now = get_signal_straddle_near_atm(opt_idx, ts, chosen_exp, atm, step, MAX_STRIKE_HOPS)
            if sig_now is None:
                continue

            sig_strike = int(sig_now["strike"])
            curr_close = float(sig_now["str_close"])
            sig_key = (chosen_exp, sig_strike)

            # Baseline (NO leakage): compute from previous samples
            dq = base_by_key[sig_key]
            baseline_val = median_last_n(dq, baseline_n)

            # Re-arm if cooled down near baseline
            if baseline_val is not None and curr_close <= baseline_val * (1.0 + float(reset_pct)):
                armed_by_key[sig_key] = True

            # Append current sample AFTER computing baseline
            dq.append(curr_close)

            # -------------------------------
            # In-trade monitoring (no intrabar look-ahead for trailing)
            # -------------------------------
            if in_trade:
                bar = get_straddle_bar_exact(opt_idx, ts, chosen_exp, int(entry_strike))
                if bar is None:
                    continue

                str_open = float(bar["str_open"])
                str_high = float(bar["str_high"])
                str_low = float(bar["str_low"])
                str_close = float(bar["str_close"])
                hold_min = (ts - entry_time).total_seconds() / 60.0

                # Trailing floor based on max_profit_rs achieved in *previous completed bars*
                trailing_floor_rs = max_profit_rs - float(giveback_rs)
                hard_floor_rs = -float(stoploss_rs)
                eff_floor_rs = max(trailing_floor_rs, hard_floor_rs)

                breach_prem = float(entry_prem) - (eff_floor_rs / float(qty))
                target_prem = float(entry_prem) - target_points

                # Gap checks (open)
                if str_open >= breach_prem:
                    # floor breached at open
                    exit_reason = "STOP" if eff_floor_rs == hard_floor_rs else "TRAIL"
                    exit_prem = max(breach_prem, str_open)
                elif str_open <= target_prem:
                    # gapped beyond target (conservative fill at target)
                    exit_reason = "TARGET"
                    exit_prem = target_prem
                else:
                    hit_floor = str_high >= breach_prem
                    hit_target = str_low <= target_prem

                    if hit_floor and hit_target:
                        # Worst-case: adverse first
                        exit_reason = "STOP" if eff_floor_rs == hard_floor_rs else "TRAIL"
                        exit_prem = max(breach_prem, str_open)
                    elif hit_target:
                        exit_reason = "TARGET"
                        exit_prem = target_prem
                    elif hit_floor:
                        exit_reason = "STOP" if eff_floor_rs == hard_floor_rs else "TRAIL"
                        exit_prem = max(breach_prem, str_open)
                    elif hold_min >= float(max_hold_min):
                        exit_reason = "TIME"
                        exit_prem = str_close
                    elif ts.strftime("%H:%M") >= EOD_EXIT_TIME:
                        exit_reason = "EOD"
                        exit_prem = str_close
                    else:
                        # No exit in this bar => now we may update max_profit using bar low (completed bar)
                        best_profit_now = (float(entry_prem) - str_low) * float(qty)
                        if best_profit_now > max_profit_rs:
                            max_profit_rs = best_profit_now
                        continue  # go next minute

                pnl_rs = (float(entry_prem) - float(exit_prem)) * float(qty)

                trades.append(Trade(
                    file=pkl_path.name,
                    underlying=underlying,
                    day=str(day_date),
                    expiry=str(chosen_exp),
                    attempt_no=attempts_made,
                    entry_time=str(entry_time),
                    entry_strike=int(entry_strike),
                    entry_prem=float(entry_prem),
                    exit_time=str(ts),
                    exit_prem=float(exit_prem),
                    exit_reason=exit_reason,
                    pnl_rs=float(pnl_rs),
                    jump_pct=float(entry_jump),
                    baseline=float(entry_base),
                    atm_strike_signal=int(entry_atm),
                    signal_strike=int(entry_sig_strike),
                    max_profit_rs=float(max_profit_rs),
                    eff_floor_rs=float(eff_floor_rs),
                    trailing_floor_rs=float(trailing_floor_rs),
                ))

                in_trade = False

                # Retry only after a losing STOP/TRAIL
                is_fail = (pnl_rs < 0) and (exit_reason in ("STOP", "TRAIL"))
                if is_fail and attempts_made < max_attempts:
                    cooldown_until = ts + pd.Timedelta(minutes=cooldown_min)
                    armed_by_key[(chosen_exp, int(entry_strike))] = False
                    max_profit_rs = 0.0
                else:
                    # After TARGET/TIME/EOD or non-fail exit, stop for the day
                    break

                continue

            # -------------------------------
            # Not in trade -> entry checks
            # -------------------------------
            if attempts_made >= max_attempts:
                continue

            if ts.time() < entry_after_time:
                continue

            # Ensure entry minute exists + same day + within session
            if nxt.normalize() != ts.normalize():
                continue
            if not within_session(nxt):
                continue

            if cooldown_until is not None and ts < cooldown_until:
                continue

            if baseline_val is None or baseline_val <= 0:
                continue

            jp = (curr_close - baseline_val) / baseline_val
            if jp < float(jump_pct):
                continue

            if not armed_by_key[sig_key]:
                continue

            # Entry at next minute OPEN on signal strike
            entry_bar = get_straddle_bar_exact(opt_idx, nxt, chosen_exp, sig_strike)
            if entry_bar is None:
                continue

            attempts_made += 1
            in_trade = True

            entry_time = nxt
            entry_strike = sig_strike
            entry_prem = float(entry_bar["str_open"])

            entry_jump = float(jp)
            entry_base = float(baseline_val)
            entry_atm = int(atm)
            entry_sig_strike = int(sig_strike)

            armed_by_key[sig_key] = False
            max_profit_rs = 0.0  # reset per trade

        # end day loop

    return trades


# =============================================================================
# MAIN
# =============================================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pkl_dir", type=str, default=r"G:\My Drive\Trading\Historical_Options_Data",help="Folder containing *_minute.pkl files")
    ap.add_argument("--out_csv", type=str, default="trades_retry.csv", help="Output trades CSV")
    ap.add_argument("--log_level", type=str, default="INFO")

    # Entry scheduling
    ap.add_argument("--entry_after", type=str, default="09:30", help="Earliest entry time HH:MM IST")

    # Spike detection
    ap.add_argument("--jump_pct", type=float, default=0.05, help="Jump threshold (0.05 = 5%)")
    ap.add_argument("--baseline_n", type=int, default=10, help="No. of samples for baseline (rolling)")

    # Reset logic
    ap.add_argument("--reset_pct", type=float, default=0.01, help="Re-arm when prem <= baseline*(1+reset_pct)")

    # Risk + target + trailing-giveback
    ap.add_argument("--stoploss_rs", type=float, default=6000.0, help="Hard max loss cap in ₹")
    ap.add_argument("--target_rs", type=float, default=6000.0, help="Profit target in ₹")
    ap.add_argument("--giveback_rs", type=float, default=6000.0, help="Max giveback from peak profit in ₹")

    ap.add_argument("--max_hold_min", type=int, default=60, help="Time stop in minutes")
    ap.add_argument("--cooldown_min", type=int, default=10, help="Gap after losing STOP/TRAIL before retry (minutes)")
    ap.add_argument("--max_retries", type=int, default=2, help="Retries after fail (2 => total 3 attempts/day)")

    # Quantity override (optional)
    ap.add_argument("--qty_override", type=int, default=None, help="Force total quantity (else uses defaults)")

    args = ap.parse_args()

    global log
    log = setup_logger(args.log_level)

    pkl_dir = Path(args.pkl_dir).resolve()
    out_csv = Path(args.out_csv).resolve()

    pkls = sorted(pkl_dir.glob("*_minute.pkl"))
    if not pkls:
        raise SystemExit(f"No *_minute.pkl files found in: {pkl_dir}")

    kite = get_kite_via_oUtils()
    log.info("Kite initialized via oUtils.")
    log.info(
        f"Params: entry_after={args.entry_after} jump_pct={args.jump_pct} baseline_n={args.baseline_n} reset_pct={args.reset_pct} | "
        f"TARGET=₹{args.target_rs} GIVEBACK=₹{args.giveback_rs} HARD_SL=₹{args.stoploss_rs} | "
        f"max_hold={args.max_hold_min} cooldown={args.cooldown_min} max_retries={args.max_retries}"
    )

    all_trades: List[Trade] = []

    for pkl in pkls:
        try:
            t = backtest_pickle(
                kite=kite,
                pkl_path=pkl,
                entry_after_hhmm=args.entry_after,
                jump_pct=args.jump_pct,
                baseline_n=args.baseline_n,
                qty_override=args.qty_override,
                stoploss_rs=args.stoploss_rs,
                target_rs=args.target_rs,
                giveback_rs=args.giveback_rs,
                max_hold_min=args.max_hold_min,
                cooldown_min=args.cooldown_min,
                max_retries=args.max_retries,
                reset_pct=args.reset_pct,
            )
            all_trades.extend(t)
            log.info(f"{pkl.name}: trades={len(t)}")
        except Exception as e:
            log.exception(f"Error processing {pkl.name}: {e}")

    if not all_trades:
        log.warning("No trades produced with current parameters.")
        pd.DataFrame().to_csv(out_csv, index=False)
        return

    out_df = pd.DataFrame([t.__dict__ for t in all_trades])
    out_df = out_df.sort_values(["day", "underlying", "entry_time"]).reset_index(drop=True)
    out_df.to_csv(out_csv, index=False)

    pnl = out_df["pnl_rs"].astype(float)
    log.info(f"TOTAL trades={len(out_df)} | TOTAL PnL=₹{pnl.sum():,.0f} | WinRate={(pnl > 0).mean():.1%}")
    log.info(f"Saved: {out_csv}")


if __name__ == "__main__":
    main()

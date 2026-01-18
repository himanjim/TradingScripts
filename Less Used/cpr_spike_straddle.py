"""
cpr_spike_straddle.py  (UPDATED: safer live logic + trailing profit lock)

What’s improved vs your earlier live version:

1) **Critical fix**: Exit always squares off the **same strike** you entered.
   - We use symbols stored in OpenTrade (tr.ce_sym / tr.pe_sym), NOT the current ATM spec.

2) **Cleaner spike detection**:
   - Baseline (median) is computed **before** appending current sample (no leakage).

3) **More realistic PnL monitoring**:
   - Signal uses MID price (stable).
   - PnL uses **ASK** (buyback) for each leg (conservative for short positions).
   - Entry prices stored using **BID** (sell) in PAPER mode (conservative).

4) **Trailing profit lock (profit retention)**:
   - When max profit crosses activation, lock a floor:
       locked = max(TRAIL_MIN_PROFIT_RS, max_pnl - TRAIL_GIVEBACK_RS)
     Exit if pnl <= locked (i.e., profit gives back beyond allowed).

5) **Two simple quality filters** (huge impact on STOP rate):
   - Skip entries before NO_TRADE_BEFORE (opening volatility).
   - Momentum filter: skip entries if spot moved too much in last MOM_WINDOW_SEC.

6) Optional: require spike condition to persist for N polls (reduces one-tick false signals).

Dependencies:
  pip install kiteconnect pandas pytz python-dateutil

Environment:
  KITE_API_KEY, KITE_ACCESS_TOKEN
  (Optional) UNIVERSE_CSV -> defaults to ./universe.csv (current working directory)
"""

from __future__ import annotations

import os
import time
import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, time as dtime, date
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytz
import pandas as pd
from kiteconnect import KiteConnect


# =============================================================================
# USER CONFIG (keep these few knobs simple)
# =============================================================================

# Load UNIVERSE_CSV from current path (working directory) by default
UNIVERSE_CSV = Path(os.environ.get("UNIVERSE_CSV", "universe.csv")).resolve()

PAPER_TRADING = True             # True = no real orders, only simulated monitoring/logging
PRODUCT = "MIS"                  # MIS for intraday
ORDER_TYPE = "MARKET"
VARIETY = "regular"

POLL_SEC = 2                     # quote poll frequency (seconds)

# Jump detection threshold for straddle premium (per-lot premium)
JUMP_PCT = 0.05                  # 5% jump (configurable)
BASELINE_WINDOW_SEC = 60         # baseline median window (seconds)
MIN_BASE_SAMPLES = 10            # minimum samples before acting
CONFIRM_POLLS = 2                # require spike condition this many consecutive polls (0/1 disables)

# Exit config (absolute P&L in INR)
PROFIT_TGT_RS = 1500.0
STOP_LOSS_RS = 1500.0
MAX_TRADE_MINUTES = 20           # time-stop

COOLDOWN_MINUTES = 10            # after exit, do not re-enter for this underlying for N minutes

# Trailing profit lock (profit retention)
TRAIL_ENABLE = True
TRAIL_ACTIVATE_RS = 800.0        # start trailing once max_pnl >= this
TRAIL_GIVEBACK_RS = 500.0        # allow giving back this much from max profit
TRAIL_MIN_PROFIT_RS = 300.0      # once trailing active, lock at least this profit

# Trade time window (IST)
INDIA_TZ = pytz.timezone("Asia/Kolkata")
TRADE_START = dtime(9, 20)
TRADE_END = dtime(15, 20)

# Simple entry filter: avoid first minutes (opening volatility)
NO_TRADE_BEFORE = dtime(9, 30)

# Momentum filter window and thresholds (spot points over last window)
MOM_WINDOW_SEC = 300  # 5 minutes
MOM_THRESHOLDS = {
    "NIFTY": 40.0,
    "BANKNIFTY": 140.0,
    "SENSEX": 220.0,
    "BANKEX": 250.0,
}
DEFAULT_MOM_THRESHOLD = 50.0  # for stocks (fallback)

# How often to reconsider ATM strike (seconds)
ATM_REFRESH_SEC = 20

# Logging
LOG_DIR = Path("logs").resolve()
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / f"cpr_spike_straddle_{datetime.now().strftime('%Y%m%d')}.log"


# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logger() -> logging.Logger:
    logger = logging.getLogger("cpr_spike_straddle")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(logging.INFO)

    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)

    if not logger.handlers:
        logger.addHandler(sh)
        logger.addHandler(fh)

    return logger


log = setup_logger()


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass(frozen=True)
class UniverseRow:
    exchange: str        # e.g., NSE/BSE
    instrument: str      # e.g., NIFTY/BANKNIFTY/SENSEX or stock symbol
    lots: int            # number of lots to trade
    cpr_points: float    # "near CPR" threshold in points (from CSV)

    @property
    def key(self) -> str:
        return normalize_underlying_key(self.instrument)


@dataclass(frozen=True)
class CPR:
    P: float
    BC: float
    TC: float
    R1: float
    S1: float

    def levels_named(self) -> List[Tuple[str, float]]:
        return [("R1", self.R1), ("BC", self.BC), ("P", self.P), ("TC", self.TC), ("S1", self.S1)]

    def levels(self) -> List[float]:
        return [self.R1, self.BC, self.P, self.TC, self.S1]


@dataclass(frozen=True)
class OptLeg:
    exchange: str
    tradingsymbol: str
    strike: float
    expiry: date
    lot_size: int


@dataclass(frozen=True)
class StraddleSpec:
    underlying: str
    expiry: date
    strike: float
    lot_size: int
    ce: OptLeg
    pe: OptLeg


@dataclass
class OpenTrade:
    """Represents an open short straddle position (SELL CE + SELL PE)."""
    underlying: str
    lots: int
    qty: int
    spot_symbol: str

    # Store the EXACT leg symbols used at entry (critical for correct exit)
    ce_sym: str  # "EXCHANGE:TRADINGSYMBOL"
    pe_sym: str  # "EXCHANGE:TRADINGSYMBOL"

    entry_ce: float
    entry_pe: float
    entry_time: datetime

    expiry: date
    strike: float

    # Trailing state
    max_pnl: float = 0.0
    trail_locked: float = 0.0


# =============================================================================
# TIME/SESSION HELPERS
# =============================================================================

def now_ist() -> datetime:
    return datetime.now(INDIA_TZ)


def in_trade_window(t: datetime) -> bool:
    tt = t.time()
    return TRADE_START <= tt <= TRADE_END


# =============================================================================
# SYMBOL NORMALIZATION + SPOT/DERIV EXCHANGE MAPPING
# =============================================================================

def normalize_underlying_key(instr: str) -> str:
    x = instr.strip().upper()
    aliases = {
        "NIFTY 50": "NIFTY",
        "NIFTY": "NIFTY",
        "NIFTY BANK": "BANKNIFTY",
        "BANKNIFTY": "BANKNIFTY",
        "SENSEX": "SENSEX",
        "BSE SENSEX": "SENSEX",
        "BANKEX": "BANKEX",
        "BSE BANKEX": "BANKEX",
    }
    return aliases.get(x, x)


def spot_tradingsymbol(exchange: str, underlying_key: str) -> str:
    u = normalize_underlying_key(underlying_key)
    ex = exchange.strip().upper()

    if u == "NIFTY":
        return "NSE:NIFTY 50"
    if u == "BANKNIFTY":
        return "NSE:NIFTY BANK"
    if u == "SENSEX":
        return "BSE:SENSEX"
    if u == "BANKEX":
        return "BSE:BANKEX"

    return f"{ex}:{underlying_key.strip().upper()}"


def derivatives_exchange_for_underlying(underlying_key: str) -> str:
    u = normalize_underlying_key(underlying_key)
    if u in ("SENSEX", "BANKEX"):
        return "BFO"
    return "NFO"


# =============================================================================
# CPR CALCULATION
# =============================================================================

def compute_cpr_from_prevday(H: float, L: float, C: float) -> CPR:
    P = (H + L + C) / 3.0
    BC = (H + L) / 2.0
    TC = 2.0 * P - BC
    R1 = 2.0 * P - L
    S1 = 2.0 * P - H
    return CPR(P=P, BC=BC, TC=TC, R1=R1, S1=S1)


def get_prev_trading_day_ohlc(kite: KiteConnect, instrument_token: int) -> Tuple[float, float, float]:
    to_dt = now_ist()
    from_dt = to_dt - timedelta(days=10)

    candles = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_dt,
        to_date=to_dt,
        interval="day",
        continuous=False,
        oi=False,
    )
    if len(candles) < 2:
        raise RuntimeError("Not enough daily candles returned to compute prev-day OHLC.")

    last = candles[-1]
    last_day = last["date"].astimezone(INDIA_TZ).date()
    today = now_ist().date()

    prev = candles[-2] if last_day == today else candles[-1]
    return float(prev["high"]), float(prev["low"]), float(prev["close"])


# =============================================================================
# QUOTE PRICE HELPERS (MID/BID/ASK)
# =============================================================================

def best_bid(q: dict) -> float:
    """Best bid from depth; fallback to last_price."""
    d = q.get("depth") or {}
    b = d.get("buy") or []
    if b and b[0].get("price") is not None:
        return float(b[0]["price"])
    return float(q.get("last_price") or 0.0)


def best_ask(q: dict) -> float:
    """Best ask from depth; fallback to last_price."""
    d = q.get("depth") or {}
    s = d.get("sell") or []
    if s and s[0].get("price") is not None:
        return float(s[0]["price"])
    return float(q.get("last_price") or 0.0)


def mid_from_quote(q: dict) -> float:
    """Mid from best bid/ask if available; else last_price."""
    try:
        d = q.get("depth") or {}
        b = d.get("buy") or []
        s = d.get("sell") or []
        if b and s and b[0].get("price") and s[0].get("price"):
            return (float(b[0]["price"]) + float(s[0]["price"])) / 2.0
    except Exception:
        pass
    return float(q.get("last_price") or 0.0)


def split_sym(sym: str) -> Tuple[str, str]:
    ex, ts = sym.split(":", 1)
    return ex, ts


# =============================================================================
# KITE CALL WRAPPER (tiny retry)
# =============================================================================

def kite_call(fn, *args, tries: int = 3, sleep_sec: float = 0.7, **kwargs):
    last_err = None
    for i in range(tries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_err = e
            log.warning(f"Kite call failed ({fn.__name__}) attempt {i+1}/{tries}: {e}")
            time.sleep(sleep_sec)
    raise last_err


# =============================================================================
# OPTION CHAIN LOOKUP
# =============================================================================

def load_derivative_instruments(kite: KiteConnect, ex: str) -> List[dict]:
    log.info(f"Downloading instruments for {ex} (one-time)...")
    return kite_call(kite.instruments, ex)


def index_derivatives(instruments: List[dict]) -> Dict[str, List[dict]]:
    idx: Dict[str, List[dict]] = defaultdict(list)
    for ins in instruments:
        name = (ins.get("name") or "").upper()
        if not name:
            continue
        if ins.get("instrument_type") not in ("CE", "PE"):
            continue
        if ins.get("expiry") is None or ins.get("strike") is None:
            continue
        idx[name].append(ins)
    return idx


def pick_nearest_expiry_and_atm_straddle(chain_rows: List[dict], underlying_key: str, spot: float, today: date) -> StraddleSpec:
    u = normalize_underlying_key(underlying_key)

    rows = [r for r in chain_rows if (r.get("name", "").upper() == u)]
    if not rows:
        raise RuntimeError(f"No derivatives rows found for underlying '{u}'.")

    expiries = sorted({r["expiry"] for r in rows if r["expiry"] >= today})
    if not expiries:
        raise RuntimeError(f"No future expiries found for '{u}' (today={today}).")
    expiry = expiries[0]

    e_rows = [r for r in rows if r["expiry"] == expiry]
    strikes = sorted({float(r["strike"]) for r in e_rows})

    def has_both(strk: float) -> bool:
        ce_ok = any(float(r["strike"]) == strk and r["instrument_type"] == "CE" for r in e_rows)
        pe_ok = any(float(r["strike"]) == strk and r["instrument_type"] == "PE" for r in e_rows)
        return ce_ok and pe_ok

    ordered = sorted(strikes, key=lambda k: abs(k - spot))
    strike = None
    for s in ordered:
        if has_both(s):
            strike = s
            break
    if strike is None:
        raise RuntimeError(f"Could not find any strike with both CE/PE for {u} {expiry}.")

    ce_row = next(r for r in e_rows if float(r["strike"]) == strike and r["instrument_type"] == "CE")
    pe_row = next(r for r in e_rows if float(r["strike"]) == strike and r["instrument_type"] == "PE")

    lot = int(ce_row.get("lot_size") or pe_row.get("lot_size") or 1)

    ce = OptLeg(exchange=ce_row["exchange"], tradingsymbol=ce_row["tradingsymbol"], strike=strike, expiry=expiry, lot_size=lot)
    pe = OptLeg(exchange=pe_row["exchange"], tradingsymbol=pe_row["tradingsymbol"], strike=strike, expiry=expiry, lot_size=lot)

    return StraddleSpec(underlying=u, expiry=expiry, strike=strike, lot_size=lot, ce=ce, pe=pe)


# =============================================================================
# ORDER WRAPPER (PAPER/LIVE)
# =============================================================================

def place_market_order(kite: KiteConnect, exchange: str, tradingsymbol: str, txn: str, qty: int) -> Optional[str]:
    """Place a MARKET order. In PAPER_TRADING mode, do nothing."""
    if PAPER_TRADING:
        return None

    return kite_call(
        kite.place_order,
        variety=VARIETY,
        exchange=exchange,
        tradingsymbol=tradingsymbol,
        transaction_type=txn,   # "BUY" / "SELL"
        quantity=qty,
        product=PRODUCT,
        order_type=ORDER_TYPE,
    )


# =============================================================================
# CSV LOADING
# =============================================================================

def load_universe(csv_path: Path) -> List[UniverseRow]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Universe CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df.columns = [c.strip().upper() for c in df.columns]

    required = {"EXCHANGE", "INSTRUMENT", "LOTS", "CPR_POINTS"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {missing}. Found: {list(df.columns)}")

    rows: List[UniverseRow] = []
    for _, r in df.iterrows():
        ex = str(r["EXCHANGE"]).strip().upper()
        instr = str(r["INSTRUMENT"]).strip()
        lots = int(r["LOTS"])
        cpr_pts = float(r["CPR_POINTS"])
        rows.append(UniverseRow(exchange=ex, instrument=instr, lots=lots, cpr_points=cpr_pts))

    return rows


# =============================================================================
# MAIN LOOP
# =============================================================================

def main():
    api_key = os.environ.get("KITE_API_KEY")
    access_token = os.environ.get("KITE_ACCESS_TOKEN")
    if not api_key or not access_token:
        raise SystemExit("Set KITE_API_KEY and KITE_ACCESS_TOKEN environment variables.")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    universe = load_universe(UNIVERSE_CSV)
    log.info(f"Loaded {len(universe)} instruments from {UNIVERSE_CSV}")
    log.info(
        f"PAPER_TRADING={PAPER_TRADING} | JUMP_PCT={JUMP_PCT*100:.1f}% | "
        f"BaselineWindow={BASELINE_WINDOW_SEC}s | Profit=₹{PROFIT_TGT_RS} | SL=₹{STOP_LOSS_RS} | "
        f"TRAIL={'ON' if TRAIL_ENABLE else 'OFF'} act=₹{TRAIL_ACTIVATE_RS} giveback=₹{TRAIL_GIVEBACK_RS} minlock=₹{TRAIL_MIN_PROFIT_RS}"
    )

    # Batch spot quotes
    spot_symbols = sorted(set(spot_tradingsymbol(r.exchange, r.instrument) for r in universe))

    # Fetch spot instrument_tokens once (for historical CPR)
    ltp_map = kite_call(kite.ltp, spot_symbols)
    spot_tokens: Dict[str, int] = {sym: int(ltp_map[sym]["instrument_token"]) for sym in spot_symbols}

    # Determine derivative exchanges needed
    need_nfo = any(derivatives_exchange_for_underlying(r.key) == "NFO" for r in universe)
    need_bfo = any(derivatives_exchange_for_underlying(r.key) == "BFO" for r in universe)

    nfo_idx: Dict[str, List[dict]] = {}
    bfo_idx: Dict[str, List[dict]] = {}

    if need_nfo:
        nfo_ins = load_derivative_instruments(kite, "NFO")
        nfo_idx = index_derivatives(nfo_ins)
        log.info(f"NFO derivatives indexed for {len(nfo_idx)} underlyings.")
    if need_bfo:
        bfo_ins = load_derivative_instruments(kite, "BFO")
        bfo_idx = index_derivatives(bfo_ins)
        log.info(f"BFO derivatives indexed for {len(bfo_idx)} underlyings.")

    # Runtime caches
    cprs: Dict[str, CPR] = {}
    cpr_computed_for_date: Dict[str, date] = {}
    straddles: Dict[str, StraddleSpec] = {}
    last_atm_refresh: Dict[str, float] = {}

    # Premium baseline history per underlying (MID straddle premium per-lot)
    maxlen = max(50, int(BASELINE_WINDOW_SEC / max(1, POLL_SEC)) + 10)
    premium_hist: Dict[str, deque] = defaultdict(lambda: deque(maxlen=maxlen))

    # Spot momentum history per underlying
    momlen = max(10, int(MOM_WINDOW_SEC / max(1, POLL_SEC)) + 1)
    spot_hist: Dict[str, deque] = defaultdict(lambda: deque(maxlen=momlen))

    # Signal confirmation counter (to reduce one-tick spikes)
    confirm_ctr: Dict[str, int] = defaultdict(int)

    # Trade state
    open_trades: Dict[str, OpenTrade] = {}
    last_trade_exit: Dict[str, datetime] = {}

    while True:
        tnow = now_ist()

        # Outside trade window -> sleep
        if not in_trade_window(tnow):
            time.sleep(POLL_SEC)
            continue

        # Entry time filter (opening volatility)
        if tnow.time() < NO_TRADE_BEFORE:
            # Still keep the loop alive, but don't attempt entries; monitoring is harmless.
            pass

        # 1) Quote all spot symbols in one call
        spot_quotes = kite_call(kite.quote, spot_symbols)

        # 2) Ensure CPR computed once per day per underlying
        for r in universe:
            ukey = r.key
            spot_sym = spot_tradingsymbol(r.exchange, r.instrument)

            if cpr_computed_for_date.get(ukey) != tnow.date():
                tok = spot_tokens[spot_sym]
                H, L, C = get_prev_trading_day_ohlc(kite, tok)
                cprs[ukey] = compute_cpr_from_prevday(H, L, C)
                cpr_computed_for_date[ukey] = tnow.date()
                lvls = [(n, round(v, 2)) for (n, v) in cprs[ukey].levels_named()]
                log.info(f"{ukey} CPR = {lvls}")

        # 3) Ensure ATM straddle specs exist / refresh periodically
        now_ts = time.time()
        option_symbols_needed: List[str] = []
        cycle_rows: List[Tuple[UniverseRow, str, float, CPR, StraddleSpec, str, str]] = []

        for r in universe:
            ukey = r.key
            spot_sym = spot_tradingsymbol(r.exchange, r.instrument)

            spot = float(spot_quotes[spot_sym]["last_price"])
            spot_hist[ukey].append(spot)

            # Deriv exchange and chain
            deriv_ex = derivatives_exchange_for_underlying(ukey)
            chain_idx = nfo_idx if deriv_ex == "NFO" else bfo_idx
            chain_rows = chain_idx.get(ukey, [])
            if not chain_rows:
                log.error(f"No option chain rows for {ukey} in {deriv_ex}. Skipping.")
                continue

            # Refresh ATM
            refresh_due = (now_ts - last_atm_refresh.get(ukey, 0.0)) >= ATM_REFRESH_SEC
            spec = straddles.get(ukey)

            if spec is None or spec.expiry < tnow.date() or refresh_due:
                try:
                    new_spec = pick_nearest_expiry_and_atm_straddle(chain_rows, ukey, spot, tnow.date())
                except Exception as e:
                    log.error(f"Failed selecting ATM straddle for {ukey}: {e}")
                    continue

                if spec is None or (spec.expiry != new_spec.expiry or spec.strike != new_spec.strike):
                    log.info(
                        f"{ukey} ATM update -> {deriv_ex} expiry={new_spec.expiry} strike={new_spec.strike} "
                        f"lot={new_spec.lot_size} CE={new_spec.ce.tradingsymbol} PE={new_spec.pe.tradingsymbol}"
                    )

                straddles[ukey] = new_spec
                last_atm_refresh[ukey] = now_ts
                spec = new_spec

            cpr = cprs[ukey]

            ce_sym = f"{spec.ce.exchange}:{spec.ce.tradingsymbol}"
            pe_sym = f"{spec.pe.exchange}:{spec.pe.tradingsymbol}"
            option_symbols_needed.extend([ce_sym, pe_sym])

            cycle_rows.append((r, spot_sym, spot, cpr, spec, ce_sym, pe_sym))

        option_symbols_needed = sorted(set(option_symbols_needed))
        if not option_symbols_needed:
            time.sleep(POLL_SEC)
            continue

        # 4) Quote all option symbols in one call
        opt_quotes = kite_call(kite.quote, option_symbols_needed)

        # 5) Process: baseline, spike detect, manage trades
        for (r, spot_sym, spot, cpr, spec, ce_sym, pe_sym) in cycle_rows:
            ukey = r.key

            if ce_sym not in opt_quotes or pe_sym not in opt_quotes:
                log.warning(f"Missing option quotes for {ukey} {ce_sym}/{pe_sym}, skipping cycle.")
                continue

            # For signal: MID straddle premium
            ce_mid = mid_from_quote(opt_quotes[ce_sym])
            pe_mid = mid_from_quote(opt_quotes[pe_sym])
            straddle_mid = ce_mid + pe_mid

            # Baseline computed BEFORE appending current sample (no leakage)
            hist = premium_hist[ukey]
            baseline_ok = len(hist) >= MIN_BASE_SAMPLES
            baseline = statistics.median(hist) if baseline_ok else None
            hist.append(straddle_mid)

            # CPR proximity and nearest line name (use spot)
            nearest_name, nearest_lvl = min(cpr.levels_named(), key=lambda kv: abs(spot - kv[1]))
            dist = abs(spot - nearest_lvl)
            near_cpr = dist <= float(r.cpr_points)

            # Momentum filter: skip entries if spot moved too much over MOM_WINDOW_SEC
            mom_ok = True
            if len(spot_hist[ukey]) >= 2:
                move = abs(spot_hist[ukey][-1] - spot_hist[ukey][0])
                thr = MOM_THRESHOLDS.get(ukey, DEFAULT_MOM_THRESHOLD)
                if move > thr:
                    mom_ok = False

            # Jump check vs baseline
            jump = False
            jump_pct = 0.0
            if baseline_ok and baseline and baseline > 0:
                jump_pct = (straddle_mid - baseline) / baseline
                jump = jump_pct >= JUMP_PCT

            # -------------------------------
            # If trade open -> monitor & exit
            # -------------------------------
            if ukey in open_trades:
                tr = open_trades[ukey]

                # Use ASK for buyback (conservative) on the SAME traded symbols
                if tr.ce_sym not in opt_quotes or tr.pe_sym not in opt_quotes:
                    # If current batch didn't include them for some reason, fall back to fetching directly
                    q = kite_call(kite.quote, [tr.ce_sym, tr.pe_sym])
                    ce_buy = best_ask(q[tr.ce_sym])
                    pe_buy = best_ask(q[tr.pe_sym])
                else:
                    ce_buy = best_ask(opt_quotes[tr.ce_sym])
                    pe_buy = best_ask(opt_quotes[tr.pe_sym])

                # PnL for short legs: (entry - current_buyback) * qty
                pnl = (tr.entry_ce - ce_buy) * tr.qty + (tr.entry_pe - pe_buy) * tr.qty
                tr.max_pnl = max(tr.max_pnl, pnl)

                age_min = (tnow - tr.entry_time).total_seconds() / 60.0

                exit_reason = None

                # Trailing profit lock
                if TRAIL_ENABLE and tr.max_pnl >= TRAIL_ACTIVATE_RS:
                    tr.trail_locked = max(TRAIL_MIN_PROFIT_RS, tr.max_pnl - TRAIL_GIVEBACK_RS)
                    # If pnl drops below locked floor, exit to retain profit
                    if pnl <= tr.trail_locked:
                        exit_reason = "TRAIL"

                # Hard exits
                if exit_reason is None:
                    if pnl >= PROFIT_TGT_RS:
                        exit_reason = "TARGET"
                    elif pnl <= -STOP_LOSS_RS:
                        exit_reason = "STOP"
                    elif age_min >= MAX_TRADE_MINUTES:
                        exit_reason = "TIME"
                    elif tnow.time() >= TRADE_END:
                        exit_reason = "EOD"

                if exit_reason:
                    log.warning(
                        f"[EXIT-{exit_reason}] {ukey} pnl=₹{pnl:,.0f} "
                        f"(max=₹{tr.max_pnl:,.0f}, lock=₹{tr.trail_locked:,.0f}) | "
                        f"CE {tr.entry_ce:.2f}->{ce_buy:.2f} | PE {tr.entry_pe:.2f}->{pe_buy:.2f} | "
                        f"spot={spot:.2f} near={nearest_name} distCPR={dist:.2f} expiry={tr.expiry} strike={tr.strike}"
                    )

                    # Buy back both legs using EXACT entry symbols (bugfix)
                    if not PAPER_TRADING:
                        ce_ex, ce_ts = split_sym(tr.ce_sym)
                        pe_ex, pe_ts = split_sym(tr.pe_sym)
                        place_market_order(kite, ce_ex, ce_ts, "BUY", tr.qty)
                        place_market_order(kite, pe_ex, pe_ts, "BUY", tr.qty)

                    del open_trades[ukey]
                    last_trade_exit[ukey] = tnow

                continue  # don't re-enter if already in a trade

            # -----------------------------------
            # Entry checks
            # -----------------------------------

            # Cooldown check
            if ukey in last_trade_exit:
                if (tnow - last_trade_exit[ukey]).total_seconds() < COOLDOWN_MINUTES * 60:
                    confirm_ctr[ukey] = 0
                    continue

            # Time filter
            if tnow.time() < NO_TRADE_BEFORE:
                confirm_ctr[ukey] = 0
                continue

            # Momentum filter
            if not mom_ok:
                confirm_ctr[ukey] = 0
                continue

            # Confirm logic to avoid one-tick spikes
            if jump and near_cpr:
                confirm_ctr[ukey] += 1
            else:
                confirm_ctr[ukey] = 0

            confirmed = (CONFIRM_POLLS <= 1) or (confirm_ctr[ukey] >= CONFIRM_POLLS)

            if confirmed and jump and near_cpr:
                qty = r.lots * spec.lot_size

                log.warning(
                    f"[SIGNAL] {ukey} spot={spot:.2f} near={nearest_name} distCPR={dist:.2f} (<= {r.cpr_points}) | "
                    f"premMID={straddle_mid:.2f} baseline={baseline:.2f} jump={jump_pct*100:.1f}% (>= {JUMP_PCT*100:.1f}%) | "
                    f"expiry={spec.expiry} strike={spec.strike} lots={r.lots} qty={qty}"
                )

                # Entry prices (paper realism): use BID for SELL fills
                ce_bid = best_bid(opt_quotes[ce_sym])
                pe_bid = best_bid(opt_quotes[pe_sym])

                # Place SELL straddle
                if not PAPER_TRADING:
                    place_market_order(kite, spec.ce.exchange, spec.ce.tradingsymbol, "SELL", qty)
                    place_market_order(kite, spec.pe.exchange, spec.pe.tradingsymbol, "SELL", qty)

                open_trades[ukey] = OpenTrade(
                    underlying=ukey,
                    lots=r.lots,
                    qty=qty,
                    spot_symbol=spot_sym,
                    ce_sym=ce_sym,          # IMPORTANT: store exact symbols for exit
                    pe_sym=pe_sym,
                    entry_ce=ce_bid,
                    entry_pe=pe_bid,
                    entry_time=tnow,
                    expiry=spec.expiry,
                    strike=spec.strike,
                    max_pnl=0.0,
                    trail_locked=0.0,
                )

                log.warning(
                    f"[ENTER] {ukey} SHORT STRADDLE qty={qty} @ CE(bid)={ce_bid:.2f}, PE(bid)={pe_bid:.2f} | "
                    f"TRAIL={'ON' if TRAIL_ENABLE else 'OFF'}"
                )

                confirm_ctr[ukey] = 0  # reset after entry

        time.sleep(POLL_SEC)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Stopped by user (Ctrl+C).")
    except Exception as e:
        log.exception(f"Fatal error: {e}")
        raise

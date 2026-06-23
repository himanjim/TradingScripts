"""
live_swell_fade.py  -  LIVE second-by-second ATM short-straddle swell-fade
===========================================================================

Monitors the ATM straddle premium (CE+PE LTP) once per second via Zerodha Kite.
When the premium RISES past a threshold band (5/7.5/10/.../30%) versus ~N seconds
ago AND that rise SUSTAINS for a few seconds (spike rejection), it SELLS a short
straddle. It exits when premium either FALLS BACK to the pre-swell level (profit)
or RISES a further same-% above entry (stop). A hard square-off time prevents
carrying overnight.

SAFETY
------
* PAPER_TRADING = True by default. No real orders are sent. Fills are simulated
  at the observed LTP and logged exactly as a live run would be.
* Real orders require BOTH PAPER_TRADING=False AND ALLOW_LIVE_ORDERS=True, set
  deliberately. Even then, every order goes through place_order() which you can
  inspect/extend for product type, validity, etc.

PERSISTENCE (survives a script restart)
---------------------------------------
* State (open position, armed thresholds, day's realized PnL, ATM, references)
  is written to STATE_FILE (JSON) after every change.
* Trades are appended to EXCEL_FILE continuously.
* On startup the script reloads both; if it died with a position OPEN, it
  resumes monitoring that position rather than losing it.

NOTE ON DATA: 1-minute bars hide intra-minute premium swings; this polls LTP
every second so the swell/fallback is measured on the real per-second path.

Run:  python live_swell_fade.py
"""

from __future__ import annotations
import os, json, time, signal
from collections import deque
from dataclasses import dataclass, asdict, field
from datetime import datetime, date, time as dtime
from typing import Dict, Optional, Deque, Tuple, Any

import pandas as pd

import Trading_2024.OptionTradeUtils as oUtils

try:
    from zoneinfo import ZoneInfo
    _TZ = ZoneInfo("Asia/Kolkata")
except Exception:
    try:
        import pytz
        _TZ = pytz.timezone("Asia/Kolkata")
    except Exception:
        _TZ = None


# =============================================================================
# CONFIG
# =============================================================================
# ---- SAFETY: paper trading is ON by default ----
PAPER_TRADING     = os.getenv("PAPER_TRADING", "1").strip() != "0"   # default True
ALLOW_LIVE_ORDERS = os.getenv("ALLOW_LIVE_ORDERS", "0").strip() == "1"  # must be explicit

# ---- Instrument config comes from oUtils.get_instruments(kite) ----
# That returns (under_exch, underlying, opt_exch, part_symbol, no_of_lots,
# strike_multiple, stoploss_points, minimum_lots, long_straddle_distance).
# We resolve it at startup inside Broker so there's a single source of truth.
# QTY override (optional). If 0, use NO_OF_LOTS from oUtils.
QTY_OVERRIDE = int(os.getenv("QTY", "0"))

# ---- Swell detection (per second) ----
POLL_SECONDS      = float(os.getenv("POLL_SECONDS", "1.0"))
LOOKBACK_SECONDS  = int(os.getenv("LOOKBACK_SECONDS", "10"))   # premium now vs N s ago
SWELL_THRESHOLDS  = [5, 7.5, 10, 12.5, 15, 20, 25, 30]         # percent bands
SUSTAIN_SECONDS   = int(os.getenv("SUSTAIN_SECONDS", "3"))     # rise must hold this long
# Require the threshold to be continuously met for SUSTAIN_SECONDS consecutive
# polls -> rejects one-tick spikes.

# ---- Exit ----
# Profit: premium falls back to pre-swell level (the premium LOOKBACK_SECONDS
# before the trigger). Stop: premium rises a further STOP_SAME_PCT above entry.
# By request the stop % equals the same band that triggered entry, but you can
# override with a fixed value here (0 = use the triggering band %).
STOP_SAME_PCT_OVERRIDE = float(os.getenv("STOP_SAME_PCT_OVERRIDE", "0"))

# ---- Session ----
SESSION_START = dtime(9, 15)
SESSION_END   = dtime(15, 30)
SQUARE_OFF    = dtime(15, 20)     # force-close any open position by this time
LAST_ENTRY    = dtime(15, 0)      # no new entries after this

# ---- Re-attempt cool-down (mirror the backtest behaviour) ----
COOLDOWN_TO_PRESWELL_FRAC = float(os.getenv("COOLDOWN_TO_PRESWELL_FRAC", "1.05"))
REENTRY_WAIT_SECONDS      = int(os.getenv("REENTRY_WAIT_SECONDS", "600"))  # 10 min
MAX_TRADES_PER_DAY        = int(os.getenv("MAX_TRADES_PER_DAY", "4"))
DAILY_LOSS_CAP_RUPEES     = int(os.getenv("DAILY_LOSS_CAP_RUPEES", "15000"))

# ---- Files (persist across restart) ----
def _base_dir():
    d = os.getenv("LIVE_DIR", os.path.join(os.path.expanduser("~"), "live_swell_fade"))
    os.makedirs(d, exist_ok=True)
    return d

_TODAY = date.today().isoformat()
# File paths are set once the underlying name is known (see set_files()).
STATE_FILE = ""
EXCEL_FILE = ""
LOG_FILE   = ""

def set_files(name: str):
    global STATE_FILE, EXCEL_FILE, LOG_FILE
    STATE_FILE = os.path.join(_base_dir(), f"state_{name}_{_TODAY}.json")
    EXCEL_FILE = os.path.join(_base_dir(), f"live_swell_fade_{name}_{_TODAY}.xlsx")
    LOG_FILE   = os.path.join(_base_dir(), f"log_{name}_{_TODAY}.txt")


# =============================================================================
# small helpers
# =============================================================================
def now_ist() -> datetime:
    return datetime.now(_TZ) if _TZ else datetime.now()

def hhmmss() -> str:
    return now_ist().strftime("%H:%M:%S")

def log(msg: str):
    line = f"[{now_ist().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    try:
        if LOG_FILE:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception:
        pass

def round_to_step(x: float, step: int) -> int:
    return int(round(x / step) * step)

def in_session(t: dtime) -> bool:
    return SESSION_START <= t <= SESSION_END

def which_bucket(rise_pct: float) -> Optional[float]:
    cleared = [th for th in SWELL_THRESHOLDS if rise_pct >= th]
    return max(cleared) if cleared else None


# =============================================================================
# STATE  (serialised to JSON for restart safety)
# =============================================================================
@dataclass
class Position:
    open: bool = False
    entry_time: str = ""
    atm_strike: int = 0
    ce_symbol: str = ""
    pe_symbol: str = ""
    entry_ce: float = 0.0
    entry_pe: float = 0.0
    entry_premium: float = 0.0
    pre_swell_premium: float = 0.0
    trigger_bucket: float = 0.0
    rise_pct_at_entry: float = 0.0
    tp_level: float = 0.0      # fall-back target (profit)
    sl_level: float = 0.0      # further-rise stop

@dataclass
class DayState:
    day: str = field(default_factory=lambda: date.today().isoformat())
    trades_done: int = 0
    realized_pnl: float = 0.0
    cooldown_until_epoch: float = 0.0
    cooldown_target: float = 0.0      # premium must fall to <= this to re-arm
    position: Dict[str, Any] = field(default_factory=lambda: asdict(Position()))

def load_state() -> DayState:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                d = json.load(f)
            st = DayState(**d)
            if st.day == date.today().isoformat():
                log(f"State reloaded: trades_done={st.trades_done} "
                    f"realized={st.realized_pnl:.0f} position_open={st.position.get('open')}")
                return st
            log("State file is from a previous day; starting fresh.")
        except Exception as e:
            log(f"Could not load state ({e}); starting fresh.")
    return DayState()

def save_state(st: DayState):
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(asdict(st), f, indent=2)
        os.replace(tmp, STATE_FILE)   # atomic
    except Exception as e:
        log(f"WARN save_state failed: {e}")


# =============================================================================
# EXCEL logging (append, survives restart)
# =============================================================================
TRADE_COLUMNS = ["day", "mode", "underlying", "atm_strike", "trigger_bucket",
                 "rise_pct_at_entry", "entry_time", "exit_time", "exit_reason",
                 "entry_ce", "entry_pe", "entry_premium", "pre_swell_premium",
                 "tp_level", "sl_level", "exit_ce", "exit_pe", "exit_premium",
                 "qty", "gross_pnl", "net_pnl"]

def append_trade_to_excel(row: Dict[str, Any]):
    try:
        if os.path.exists(EXCEL_FILE):
            existing = pd.read_excel(EXCEL_FILE, sheet_name="trades")
            df = pd.concat([existing, pd.DataFrame([row])], ignore_index=True)
        else:
            df = pd.DataFrame([row], columns=TRADE_COLUMNS)
        with pd.ExcelWriter(EXCEL_FILE, engine="openpyxl") as xw:
            df.to_excel(xw, sheet_name="trades", index=False)
            # quick summary
            if not df.empty and "net_pnl" in df:
                summ = pd.DataFrame({
                    "metric": ["trades", "net_pnl", "wins", "win_rate_pct"],
                    "value": [len(df), round(df["net_pnl"].sum(), 1),
                              int((df["net_pnl"] > 0).sum()),
                              round(100 * (df["net_pnl"] > 0).mean(), 1)],
                })
                summ.to_excel(xw, sheet_name="summary", index=False)
        log(f"Trade logged to Excel: {row.get('exit_reason')} net={row.get('net_pnl'):.0f}")
    except Exception as e:
        log(f"WARN append_trade_to_excel failed: {e}")


# =============================================================================
# COSTS (Zerodha F&O options) - for paper PnL realism
# =============================================================================
def trade_charges(entry_ce, entry_pe, exit_ce, exit_pe, qty) -> float:
    entry_turn = (entry_ce + entry_pe) * qty
    exit_turn = (exit_ce + exit_pe) * qty
    total = entry_turn + exit_turn
    brokerage = 20.0 * 4
    stt = entry_turn * 0.001
    txn = total * 0.0003553
    sebi = total * 10.0 / 1_00_00_000
    stamp = exit_turn * 0.00003
    ipft = total * 0.010 / 1_00_00_000
    gst = (brokerage + txn + sebi) * 0.18
    return round(brokerage + stt + txn + sebi + stamp + ipft + gst, 2)


# =============================================================================
# KITE wrapper
# =============================================================================
class Broker:
    def __init__(self):
        self.kite = oUtils.intialize_kite_api()
        # Single source of truth for instrument config.
        (self.under_exch, self.underlying_sym, self.opt_exch, self.part_symbol,
         self.no_of_lots, self.strike_multiple, self.stoploss_points,
         self.minimum_lots, self.long_straddle_dist) = oUtils.get_instruments(self.kite)
        # underlying_sym / part_symbol carry a leading ':'; normalise.
        self.underlying_sym = self.underlying_sym.lstrip(":")
        self.part_symbol = self.part_symbol.lstrip(":")
        # Friendly name + sizing.
        self.name = "SENSEX" if "SENSEX" in self.underlying_sym.upper() else \
                    ("BANKNIFTY" if "BANK" in self.underlying_sym.upper() else "NIFTY")
        self.qty = QTY_OVERRIDE if QTY_OVERRIDE > 0 else int(self.no_of_lots)
        self.step = int(self.strike_multiple)
        log(f"Instruments from oUtils: name={self.name} under={self.under_exch}:{self.underlying_sym} "
            f"opt_exch={self.opt_exch} part={self.part_symbol} qty={self.qty} step={self.step}")

    def underlying_ltp(self) -> float:
        key = f"{self.under_exch}:{self.underlying_sym}"
        q = self.kite.ltp([key])
        return float(q[key]["last_price"])

    def atm_option_symbols(self, atm: int) -> Tuple[str, str]:
        """Build CE/PE tradingsymbols from PART_SYMBOL + strike (oUtils convention)."""
        ce = f"{self.part_symbol}{atm}CE"
        pe = f"{self.part_symbol}{atm}PE"
        return ce, pe

    def straddle_ltp(self, ce_sym: str, pe_sym: str) -> Tuple[float, float]:
        ck, pk = f"{self.opt_exch}:{ce_sym}", f"{self.opt_exch}:{pe_sym}"
        q = self.kite.ltp([ck, pk])
        return float(q[ck]["last_price"]), float(q[pk]["last_price"])

    def place_order(self, tradingsymbol: str, side: str, qty: int) -> Optional[str]:
        """side = 'SELL' or 'BUY'. Returns order_id or None. Gated for safety."""
        if PAPER_TRADING or not ALLOW_LIVE_ORDERS:
            log(f"[PAPER] would {side} {qty} {tradingsymbol}")
            return None
        oid = self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR, exchange=self.opt_exch, tradingsymbol=tradingsymbol,
            transaction_type=(self.kite.TRANSACTION_TYPE_SELL if side == "SELL"
                              else self.kite.TRANSACTION_TYPE_BUY),
            quantity=qty, product=self.kite.PRODUCT_MIS,
            order_type=self.kite.ORDER_TYPE_MARKET,
            tag=oUtils.SS_ORDER_TAG)
        log(f"[LIVE] {side} {qty} {tradingsymbol} -> order_id={oid}")
        return oid


# =============================================================================
# ENGINE
# =============================================================================
class Engine:
    def __init__(self):
        self.broker = Broker()
        set_files(self.broker.name)          # now that we know the underlying
        self.underlying = self.broker.name
        self.qty = self.broker.qty
        self.state = load_state()
        self.pos = Position(**self.state.position)
        self.step = int(self.broker.step)
        # rolling premium buffer: list of (epoch, premium)
        self.buf: Deque[Tuple[float, float]] = deque(maxlen=LOOKBACK_SECONDS + SUSTAIN_SECONDS + 5)
        # per-threshold sustain counters
        self.sustain_count: Dict[float, int] = {th: 0 for th in SWELL_THRESHOLDS}
        self._stop = False
        # resolve today's ATM symbols once (re-resolved if ATM moves a lot)
        self.atm = 0
        self.ce_sym = ""
        self.pe_sym = ""
        self._running_min_premium = None  # for cool-down tracking while flat
        signal.signal(signal.SIGINT, self._sigint)

    def _sigint(self, *a):
        log("SIGINT received; will stop after this loop. Position (if any) is persisted.")
        self._stop = True

    # ---- ATM resolution ----
    def refresh_atm(self):
        spot = self.broker.underlying_ltp()
        atm = round_to_step(spot, self.step)
        if atm != self.atm:
            self.atm = atm
            self.ce_sym, self.pe_sym = self.broker.atm_option_symbols(atm)
            log(f"ATM set to {atm} (spot {spot:.1f}) CE={self.ce_sym} PE={self.pe_sym}")

    # ---- persistence ----
    def persist(self):
        self.state.position = asdict(self.pos)
        save_state(self.state)

    # ---- premium reference ----
    def premium_n_sec_ago(self) -> Optional[float]:
        if not self.buf:
            return None
        target = now_ist().timestamp() - LOOKBACK_SECONDS
        # earliest sample at or after target; if buffer shorter, use oldest
        for epoch, prem in self.buf:
            if epoch >= target:
                return prem
        return self.buf[0][1]

    # ---- entry ----
    def try_enter(self, now_prem: float):
        t = now_ist().time()
        if t > LAST_ENTRY:
            return
        if self.state.trades_done >= MAX_TRADES_PER_DAY:
            return
        if DAILY_LOSS_CAP_RUPEES > 0 and self.state.realized_pnl <= -float(DAILY_LOSS_CAP_RUPEES):
            return
        # cool-down gates
        if self.state.cooldown_until_epoch and now_ist().timestamp() < self.state.cooldown_until_epoch:
            return
        if self.state.cooldown_target and now_prem > self.state.cooldown_target:
            return  # premium hasn't cooled back near pre-swell yet

        past = self.premium_n_sec_ago()
        if not past or past <= 0:
            return
        rise_pct = (now_prem / past - 1.0) * 100.0
        bucket = which_bucket(rise_pct)

        # update sustain counters: a band is "sustained" if rise stayed >= it
        for th in SWELL_THRESHOLDS:
            if rise_pct >= th:
                self.sustain_count[th] += 1
            else:
                self.sustain_count[th] = 0

        if bucket is None:
            return
        # require the triggering band to have sustained for SUSTAIN_SECONDS polls
        if self.sustain_count[bucket] < SUSTAIN_SECONDS:
            return

        # ---- ENTER short straddle ----
        ce, pe = self.broker.straddle_ltp(self.ce_sym, self.pe_sym)
        entry_prem = ce + pe
        stop_pct = STOP_SAME_PCT_OVERRIDE if STOP_SAME_PCT_OVERRIDE > 0 else bucket
        self.pos = Position(
            open=True, entry_time=hhmmss(), atm_strike=self.atm,
            ce_symbol=self.ce_sym, pe_symbol=self.pe_sym,
            entry_ce=ce, entry_pe=pe, entry_premium=entry_prem,
            pre_swell_premium=float(past), trigger_bucket=float(bucket),
            rise_pct_at_entry=round(rise_pct, 2),
            tp_level=float(past),                                  # fall back to pre-swell
            sl_level=entry_prem * (1.0 + stop_pct / 100.0),        # further same-% rise
        )
        self.broker.place_order(self.ce_sym, "SELL", self.qty)
        self.broker.place_order(self.pe_sym, "SELL", self.qty)
        log(f"ENTER short straddle @ {entry_prem:.2f} (rise {rise_pct:.1f}% band {bucket}) "
            f"TP={self.pos.tp_level:.2f} SL={self.pos.sl_level:.2f} [{'PAPER' if PAPER_TRADING else 'LIVE'}]")
        # reset sustain so we don't immediately re-trigger
        self.sustain_count = {th: 0 for th in SWELL_THRESHOLDS}
        self.persist()

    # ---- exit ----
    def try_exit(self, now_prem: float, force_reason: Optional[str] = None):
        if not self.pos.open:
            return
        reason = force_reason
        if reason is None:
            if now_prem <= self.pos.tp_level:
                reason = "FALLBACK_TP"
            elif now_prem >= self.pos.sl_level:
                reason = "STOPLOSS"
        if reason is None:
            return

        ce, pe = self.broker.straddle_ltp(self.pos.ce_symbol, self.pos.pe_symbol)
        exit_prem = ce + pe
        self.broker.place_order(self.pos.ce_symbol, "BUY", self.qty)
        self.broker.place_order(self.pos.pe_symbol, "BUY", self.qty)
        gross = (self.pos.entry_premium - exit_prem) * self.qty
        charges = trade_charges(self.pos.entry_ce, self.pos.entry_pe, ce, pe, self.qty)
        net = gross - charges

        row = {
            "day": self.state.day, "mode": "PAPER" if PAPER_TRADING else "LIVE",
            "underlying": self.underlying, "atm_strike": self.pos.atm_strike,
            "trigger_bucket": self.pos.trigger_bucket,
            "rise_pct_at_entry": self.pos.rise_pct_at_entry,
            "entry_time": self.pos.entry_time, "exit_time": hhmmss(),
            "exit_reason": reason, "entry_ce": self.pos.entry_ce, "entry_pe": self.pos.entry_pe,
            "entry_premium": round(self.pos.entry_premium, 2),
            "pre_swell_premium": round(self.pos.pre_swell_premium, 2),
            "tp_level": round(self.pos.tp_level, 2), "sl_level": round(self.pos.sl_level, 2),
            "exit_ce": round(ce, 2), "exit_pe": round(pe, 2), "exit_premium": round(exit_prem, 2),
            "qty": self.qty, "gross_pnl": round(gross, 2), "net_pnl": round(net, 2),
        }
        append_trade_to_excel(row)
        log(f"EXIT {reason} @ {exit_prem:.2f} net={net:.0f}")

        # update day state + cool-down
        self.state.trades_done += 1
        self.state.realized_pnl += net
        self.state.cooldown_until_epoch = now_ist().timestamp() + REENTRY_WAIT_SECONDS
        self.state.cooldown_target = self.pos.pre_swell_premium * COOLDOWN_TO_PRESWELL_FRAC
        self.pos = Position()  # flat
        self.persist()

    # ---- main loop ----
    def run(self):
        log(f"=== LIVE SWELL-FADE START === mode={'PAPER' if PAPER_TRADING else 'LIVE'} "
            f"underlying={self.underlying} qty={self.qty} thresholds={SWELL_THRESHOLDS} "
            f"lookback={LOOKBACK_SECONDS}s sustain={SUSTAIN_SECONDS}s")
        if not PAPER_TRADING and not ALLOW_LIVE_ORDERS:
            log("PAPER_TRADING is off but ALLOW_LIVE_ORDERS is not set -> still simulating, no real orders.")
        if not PAPER_TRADING and ALLOW_LIVE_ORDERS:
            log("!!! LIVE ORDER MODE ACTIVE - real orders will be placed !!!")

        while not self._stop:
            try:
                t = now_ist().time()
                if not in_session(t):
                    if t < SESSION_START:
                        log("Before session; sleeping 30s."); time.sleep(30); continue
                    else:
                        # after close: square off if needed, then stop
                        if self.pos.open:
                            self.refresh_atm()
                            ce, pe = self.broker.straddle_ltp(self.pos.ce_symbol, self.pos.pe_symbol)
                            self.try_exit(ce + pe, force_reason="SESSION_END")
                        log("Session over; exiting loop."); break

                self.refresh_atm()
                ce, pe = self.broker.straddle_ltp(self.ce_sym, self.pe_sym)
                prem = ce + pe
                self.buf.append((now_ist().timestamp(), prem))

                # square-off window
                if t >= SQUARE_OFF and self.pos.open:
                    self.try_exit(prem, force_reason="SQUARE_OFF")
                elif self.pos.open:
                    self.try_exit(prem)            # check TP/SL
                else:
                    self.try_enter(prem)           # look for a fresh swell

                time.sleep(POLL_SECONDS)
            except KeyboardInterrupt:
                self._stop = True
            except Exception as e:
                log(f"WARN loop error: {e}")
                time.sleep(2.0)

        log(f"=== STOPPED === trades_done={self.state.trades_done} "
            f"realized={self.state.realized_pnl:.0f}  (state persisted to {STATE_FILE})")


def main():
    eng = Engine()
    eng.run()


if __name__ == "__main__":
    main()

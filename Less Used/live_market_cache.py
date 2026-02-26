"""
live_market_cache.py  (UPDATED + HEARTBEAT THREAD + SESSION CLAMP + SAFE QUEUE + BACKFILL THROTTLE)

Fixes / adds:
- Dedicated heartbeat thread: gap fill + force-close runs even when no ticks arrive.
- Session clamp: never fill candles beyond SESSION_END (last candle at 15:29 if end=15:30).
- Safe non-blocking queue writes: prevents websocket callback from freezing if queue is full.
- Backfiller throttling: caps historical calls per sweep for TOP_N=500.
- Locking: thread-safe shared state between tick callbacks and heartbeat thread.
"""

from __future__ import annotations

import os
import json
import time
import sqlite3
import threading
import queue
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from kiteconnect import KiteConnect, KiteTicker

import Trading_2024.OptionTradeUtils as oUtils


# ===================== USER CONFIG =====================

DEFAULT_UNIVERSE_CSV = r"C:\Users\Local User\Downloads\stock_list.csv"
UNIVERSE_CSV = os.environ.get("UNIVERSE_CSV", DEFAULT_UNIVERSE_CSV)

TOP_N = 500

QUOTE_BATCH_SIZE = 100
QUOTE_RPS = 1.0

CACHE_ROOT = r"./live_cache"

SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)

IST_OFFSET = timedelta(hours=5, minutes=30)

WS_MODE = "full"  # full => has volume_traded; quote => may not

HIST_SLEEP_SEC = 0.40
HIST_MAX_RETRIES = 3

# We don't need full-day; we need enough to plot + scan reliably
DISPLAY_BARS_TARGET = 240
BACKFILL_LOOKBACK_MIN = DISPLAY_BARS_TARGET + 30  # floor = now - lookback (>= display)

WRITE_FLUSH_SEC = 1.0
WRITE_BATCH_MAX = 5000

BACKFILL_LOG_EVERY_N_SYMBOLS = 10
BACKFILL_SWEEP_SUMMARY_EVERY_SEC = 15
BACKFILL_WAIT_LOG_EVERY_SEC = 10

# ---- HEARTBEAT GAP FILL ----
HEARTBEAT_FILL_ENABLED = True
HEARTBEAT_MAX_FILL_MINUTES = 20
HEARTBEAT_SEED_BUDGET = 50
HEARTBEAT_LOG = True

# ---- HEARTBEAT THREAD ----
HEARTBEAT_THREAD_ENABLED = True
HEARTBEAT_POLL_SEC = 0.8

# ---- SAFE QUEUE ----
DROP_LOG_EVERY = 1000

# ---- BACKFILL THROTTLE ----
BACKFILL_MAX_FETCH_PER_SWEEP = 25     # <=25 historical calls per loop; prevents API hammering
BACKFILL_LOOP_SLEEP_SEC = 5.0


# ===================== LOG =====================

def log(level: str, msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ===================== TIME HELPERS =====================

def ist_now_naive() -> datetime:
    return datetime.now()

def today_ist() -> date:
    return ist_now_naive().date()

def combine(d: date, t: dtime) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second)

def floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)

def is_utc_like_session_start(ts: datetime) -> bool:
    return (ts.hour == 3 and 35 <= ts.minute <= 55)

def to_ist_naive_auto(ts: datetime) -> datetime:
    if ts is None:
        return ts
    if ts.tzinfo is not None:
        ts = ts.replace(tzinfo=None)
    if is_utc_like_session_start(ts):
        return ts + IST_OFFSET
    return ts

def session_bounds(d: date) -> Tuple[datetime, datetime, datetime]:
    """
    Returns:
      session_start_dt,
      session_end_dt,
      session_last_closed_dt (last 1-min candle close time, i.e. end-1min)
    """
    ss = combine(d, SESSION_START)
    se = combine(d, SESSION_END)
    last_closed = floor_minute(se) - timedelta(minutes=1)
    return ss, se, last_closed


# ===================== PATHS =====================

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def day_dir(d: date) -> str:
    return os.path.join(CACHE_ROOT, d.isoformat())

def db_path(d: date) -> str:
    return os.path.join(day_dir(d), "candles.sqlite")

def manifest_path(d: date) -> str:
    return os.path.join(day_dir(d), "manifest.json")

def top_liquid_path(d: date, top_n: int) -> str:
    return os.path.join(day_dir(d), f"top_{top_n}_liquid.csv")

def day_open_path(d: date) -> str:
    return os.path.join(day_dir(d), "day_open.json")


# ===================== UNIVERSE =====================

def load_universe_csv(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Universe CSV not found: {path}")

    df = pd.read_csv(path)
    if "exchange" not in df.columns or "tradingsymbol" not in df.columns:
        raise ValueError("Universe CSV must have columns: exchange, tradingsymbol")

    df["exchange"] = df["exchange"].astype(str).str.upper().str.strip()
    df["tradingsymbol"] = df["tradingsymbol"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["exchange", "tradingsymbol"]).reset_index(drop=True)

    log("INFO", f"Universe loaded: {len(df)} instruments from {path}")
    return df

def instrument_key(exchange: str, tradingsymbol: str) -> str:
    return f"{exchange.upper()}:{tradingsymbol.strip()}"


# ===================== LIQUIDITY (Top N) + Day Open Cache =====================

def chunked(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def compute_liquidity_metrics(sym_key: str, q: Dict) -> Dict:
    exch, ts = sym_key.split(":", 1)

    last_price = float(q.get("last_price") or 0.0)
    volume = int(q.get("volume") or 0)

    ohlc = q.get("ohlc") or {}
    day_open = ohlc.get("open")
    day_open = float(day_open) if day_open is not None else None

    depth = q.get("depth") or {}
    buy = depth.get("buy") or []
    sell = depth.get("sell") or []

    best_bid = buy[0]["price"] if buy else None
    best_ask = sell[0]["price"] if sell else None

    bid_qty = sum(int(x.get("quantity", 0) or 0) for x in buy)
    ask_qty = sum(int(x.get("quantity", 0) or 0) for x in sell)
    total_depth_qty = bid_qty + ask_qty

    if best_bid and best_ask and best_bid > 0 and best_ask > 0:
        mid = (best_bid + best_ask) / 2.0
        spread_pct = ((best_ask - best_bid) / mid) * 100.0
    else:
        spread_pct = float("inf")

    turnover = last_price * volume

    return {
        "exchange": exch,
        "tradingsymbol": ts,
        "symbol": f"{exch}:{ts}",
        "day_open": day_open,
        "last_price": last_price,
        "volume": volume,
        "turnover": turnover,
        "spread_pct": spread_pct,
        "total_depth_qty": total_depth_qty,
    }

def fetch_liquidity_snapshot(kite: KiteConnect, sym_keys: List[str]) -> pd.DataFrame:
    rows = []
    for i, batch in enumerate(chunked(sym_keys, QUOTE_BATCH_SIZE), start=1):
        time.sleep(1.0 / max(QUOTE_RPS, 0.1))
        try:
            qd = kite.quote(batch)
        except Exception as e:
            log("WARN", f"quote batch {i} failed: {e}")
            continue

        for s in batch:
            q = qd.get(s)
            if not q:
                continue
            rows.append(compute_liquidity_metrics(s, q))

    return pd.DataFrame(rows)

def pick_top_liquid(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    if df.empty:
        raise RuntimeError("Liquidity snapshot is empty (quote returned nothing).")
    df = df.sort_values(
        by=["turnover", "spread_pct", "total_depth_qty"],
        ascending=[False, True, False]
    ).reset_index(drop=True)
    return df.head(top_n).copy()

def load_or_compute_top_liquid(kite: KiteConnect, universe_df: pd.DataFrame, d: date, top_n: int) -> pd.DataFrame:
    out_csv = top_liquid_path(d, top_n)
    ensure_dir(day_dir(d))

    if os.path.isfile(out_csv) and os.path.isfile(day_open_path(d)):
        log("INFO", f"Top-{top_n} + day_open already cached in {day_dir(d)}")
        return pd.read_csv(out_csv)

    sym_keys = [instrument_key(r.exchange, r.tradingsymbol) for r in universe_df.itertuples(index=False)]
    log("STEP", f"Computing liquidity snapshot for {len(sym_keys)} instruments (quote API)...")

    liq = fetch_liquidity_snapshot(kite, sym_keys)
    top = pick_top_liquid(liq, top_n)

    top.to_csv(out_csv, index=False)

    day_open_map = {}
    for r in top.itertuples(index=False):
        if getattr(r, "symbol", None) and getattr(r, "day_open", None) is not None:
            day_open_map[str(r.symbol)] = float(r.day_open)

    with open(day_open_path(d), "w", encoding="utf-8") as f:
        json.dump(day_open_map, f, indent=2)

    log("INFO", f"Saved Top-{len(top)} liquid stocks to {out_csv}")
    log("INFO", f"Saved day_open map to {day_open_path(d)} ({len(day_open_map)} symbols)")
    return top


# ===================== INSTRUMENT TOKENS =====================

INSTRUMENTS_CACHE_DIR = r"1-min stocks trading/kite_instruments_cache"
INSTRUMENTS_CACHE_TTL_DAYS = 7

def _instruments_cache_file(exchange: str) -> str:
    return os.path.join(INSTRUMENTS_CACHE_DIR, f"instruments_{exchange.upper()}.parquet")

def _cache_is_fresh(path: str, ttl_days: int) -> bool:
    if not os.path.isfile(path):
        return False
    mtime = datetime.fromtimestamp(os.path.getmtime(path))
    return (datetime.now() - mtime).days <= ttl_days

def load_instruments_df(kite: KiteConnect, exchange: str) -> pd.DataFrame:
    ensure_dir(INSTRUMENTS_CACHE_DIR)
    cache = _instruments_cache_file(exchange)

    if _cache_is_fresh(cache, INSTRUMENTS_CACHE_TTL_DAYS):
        return pd.read_parquet(cache, engine="pyarrow")

    log("STEP", f"Downloading instruments dump for {exchange} (cached {INSTRUMENTS_CACHE_TTL_DAYS} days)...")
    inst = kite.instruments(exchange)
    df = pd.DataFrame(inst)
    keep = ["exchange", "tradingsymbol", "instrument_token"]
    df = df[[c for c in keep if c in df.columns]].copy()
    df.to_parquet(cache, engine="pyarrow", index=False)
    return df

def build_token_map(kite: KiteConnect, symbols: List[Tuple[str, str]]) -> Dict[Tuple[str, str], int]:
    by_ex: Dict[str, List[str]] = {}
    for ex, ts in symbols:
        by_ex.setdefault(ex.upper(), []).append(ts)

    token_map: Dict[Tuple[str, str], int] = {}
    for ex, tss in by_ex.items():
        df = load_instruments_df(kite, ex)
        df["exchange"] = df["exchange"].astype(str).str.upper()
        df["tradingsymbol"] = df["tradingsymbol"].astype(str).str.strip()

        sub = df[df["tradingsymbol"].isin(set(tss))]
        for r in sub.itertuples(index=False):
            token_map[(r.exchange, r.tradingsymbol)] = int(r.instrument_token)

    missing = [(ex, ts) for (ex, ts) in symbols if (ex.upper(), ts) not in token_map]
    if missing:
        log("WARN", f"Missing instrument_token for {len(missing)} symbols (first 10): {missing[:10]}")
    return token_map


# ===================== SQLITE STORE =====================

def open_db(path: str, check_same_thread: bool) -> sqlite3.Connection:
    ensure_dir(os.path.dirname(path))
    conn = sqlite3.connect(path, check_same_thread=check_same_thread)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")
    conn.execute("PRAGMA busy_timeout=3000;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            symbol TEXT NOT NULL,
            ts TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low  REAL NOT NULL,
            close REAL NOT NULL,
            volume INTEGER NOT NULL,
            PRIMARY KEY(symbol, ts)
        );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_candles_symbol_ts ON candles(symbol, ts);")
    conn.commit()
    return conn

def open_db_ro(path: str) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=True)

def db_last_ts(conn: sqlite3.Connection, symbol: str) -> Optional[datetime]:
    cur = conn.execute("SELECT ts FROM candles WHERE symbol=? ORDER BY ts DESC LIMIT 1;", (symbol,))
    row = cur.fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row[0])

def db_count_since(conn: sqlite3.Connection, symbol: str, start_ts: datetime) -> int:
    cur = conn.execute("SELECT COUNT(1) FROM candles WHERE symbol=? AND ts>=?;", (symbol, start_ts.isoformat()))
    return int(cur.fetchone()[0])


# ===================== CANDLE BUILDER =====================

@dataclass
class CandleRow:
    symbol: str
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

class CandleBuilder:
    def __init__(self):
        self.cur_minute: Optional[datetime] = None
        self.o = self.h = self.l = self.c = None
        self.vol = 0
        self.last_cum_vol: Optional[int] = None
        self.last_close: Optional[float] = None

    def _start_minute(self, m: datetime, price: float) -> None:
        self.cur_minute = m
        self.o = self.h = self.l = self.c = float(price)
        self.vol = 0

    def _finalize_current(self) -> Optional[Tuple[datetime, float, float, float, float, int]]:
        if self.cur_minute is None or self.o is None:
            return None
        out = (self.cur_minute, float(self.o), float(self.h), float(self.l), float(self.c), int(self.vol))
        self.last_close = float(self.c)
        return out

    def update(self, ts: datetime, price: float, cum_vol: Optional[int]) -> List[Tuple[datetime, float, float, float, float, int]]:
        ts = to_ist_naive_auto(ts)
        m = floor_minute(ts)
        completed: List[Tuple[datetime, float, float, float, float, int]] = []

        inc_vol = 0
        if cum_vol is not None:
            if self.last_cum_vol is not None:
                inc_vol = max(0, int(cum_vol) - int(self.last_cum_vol))
            self.last_cum_vol = int(cum_vol)

        if self.cur_minute is None:
            self._start_minute(m, price)
            self.vol += inc_vol
            return completed

        if m == self.cur_minute:
            self.c = float(price)
            self.h = max(float(self.h), float(price))
            self.l = min(float(self.l), float(price))
            self.vol += inc_vol
            return completed

        prev = self._finalize_current()
        if prev:
            completed.append(prev)

        # Fill gaps until new minute (flat using last_close)
        gap = self.cur_minute + timedelta(minutes=1)
        while gap < m and self.last_close is not None:
            completed.append((gap, self.last_close, self.last_close, self.last_close, self.last_close, 0))
            gap += timedelta(minutes=1)

        self._start_minute(m, price)
        self.vol += inc_vol
        return completed

    def force_close_if_minute_closed(self, now_ts: datetime) -> List[Tuple[datetime, float, float, float, float, int]]:
        if self.cur_minute is None:
            return []
        now_ts = to_ist_naive_auto(now_ts)
        last_closed = floor_minute(now_ts) - timedelta(minutes=1)
        if self.cur_minute <= last_closed:
            prev = self._finalize_current()
            self.cur_minute = None
            self.o = self.h = self.l = self.c = None
            self.vol = 0
            return [prev] if prev else []
        return []


# ===================== WRITER THREAD =====================

class CandleWriter(threading.Thread):
    def __init__(self, conn: sqlite3.Connection, q: "queue.Queue[CandleRow]", stop_event: threading.Event):
        super().__init__(daemon=True)
        self.conn = conn
        self.q = q
        self.stop_event = stop_event

    def run(self) -> None:
        buf: List[CandleRow] = []
        last_flush = time.time()

        while not self.stop_event.is_set():
            try:
                item = self.q.get(timeout=0.25)
                buf.append(item)
                if len(buf) >= WRITE_BATCH_MAX:
                    self._flush(buf)
                    buf.clear()
                    last_flush = time.time()
            except queue.Empty:
                pass

            if buf and (time.time() - last_flush) >= WRITE_FLUSH_SEC:
                self._flush(buf)
                buf.clear()
                last_flush = time.time()

        if buf:
            self._flush(buf)

    def _flush(self, rows: List[CandleRow]) -> None:
        data = [(r.symbol, r.ts.isoformat(), r.open, r.high, r.low, r.close, int(r.volume)) for r in rows]
        self.conn.executemany("""
            INSERT OR REPLACE INTO candles(symbol, ts, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?);
        """, data)
        self.conn.commit()


# ===================== MANIFEST =====================

def load_manifest(d: date) -> Dict:
    p = manifest_path(d)
    if os.path.isfile(p):
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"day": d.isoformat(), "last_saved": {}}

def save_manifest(d: date, m: Dict) -> None:
    ensure_dir(os.path.dirname(manifest_path(d)))
    with open(manifest_path(d), "w", encoding="utf-8") as f:
        json.dump(m, f, indent=2)


# ===================== HISTORICAL BACKFILL =====================

class HistoricalBackfiller(threading.Thread):
    """
    Uses its own SQLite connection.
    Ensures at least DISPLAY_BARS_TARGET candles exist for recent window (now-lookback).
    Throttled to avoid hammering Kite historical API for TOP_N=500.
    """

    def __init__(
        self,
        kite: KiteConnect,
        db_file: str,
        d: date,
        symbols: List[Tuple[str, int]],
        stop_event: threading.Event,
        manifest: Dict,
    ):
        super().__init__(daemon=True)
        self.kite = kite
        self.db_file = db_file
        self.d = d
        self.symbols = symbols
        self.stop_event = stop_event
        self.manifest = manifest

        self._last_wait_log = 0.0
        self._last_sweep_log = 0.0
        self._rr_idx = 0  # round-robin pointer

    def _backfill_floor(self, now: datetime) -> datetime:
        ss, se, _ = session_bounds(self.d)
        now2 = min(now, se)  # never drift beyond market end
        lookback = now2 - timedelta(minutes=BACKFILL_LOOKBACK_MIN)
        return max(ss, lookback)

    def run(self) -> None:
        log("STEP", f"Historical backfiller started. target_bars={DISPLAY_BARS_TARGET} lookback_min={BACKFILL_LOOKBACK_MIN}")
        conn = open_db(self.db_file, check_same_thread=True)

        ss, se, last_session_closed = session_bounds(self.d)

        while not self.stop_event.is_set():
            now = ist_now_naive()
            last_closed = floor_minute(now) - timedelta(minutes=1)
            last_closed = min(last_closed, last_session_closed)

            if last_closed < ss:
                if (time.time() - self._last_wait_log) >= BACKFILL_WAIT_LOG_EVERY_SEC:
                    self._last_wait_log = time.time()
                    log("INFO", f"[BACKFILL] WAIT_MARKET now={now.time()} last_closed={last_closed.time()} session_start={ss.time()}")
                time.sleep(2.0)
                continue

            floor = self._backfill_floor(now)

            if (time.time() - self._last_sweep_log) >= BACKFILL_SWEEP_SUMMARY_EVERY_SEC:
                self._last_sweep_log = time.time()
                log("INFO", f"[BACKFILL] SWEEP now={now.time()} last_closed={last_closed.time()} floor={floor.time()} symbols={len(self.symbols)} rr={self._rr_idx}")

            fetched = skipped_have = skipped_uptodate = failed = 0
            calls_this_sweep = 0

            n = len(self.symbols)
            if n == 0:
                time.sleep(5.0)
                continue

            # Round-robin over symbols; stop after BACKFILL_MAX_FETCH_PER_SWEEP calls
            for step in range(n):
                if self.stop_event.is_set():
                    break
                if calls_this_sweep >= BACKFILL_MAX_FETCH_PER_SWEEP:
                    break

                idx = (self._rr_idx + step) % n
                sym, token = self.symbols[idx]

                have = db_count_since(conn, sym, floor)
                if have >= DISPLAY_BARS_TARGET:
                    skipped_have += 1
                    continue

                last_ts = db_last_ts(conn, sym)
                start = floor if last_ts is None else max(floor, last_ts + timedelta(minutes=1))
                end = last_closed

                if start > end:
                    skipped_uptodate += 1
                    continue

                calls_this_sweep += 1
                if (calls_this_sweep % BACKFILL_LOG_EVERY_N_SYMBOLS) == 0:
                    log("INFO", f"[BACKFILL] FETCH {sym} token={token} start={start.time()} end={end.time()} have={have} (call {calls_this_sweep}/{BACKFILL_MAX_FETCH_PER_SWEEP})")

                time.sleep(HIST_SLEEP_SEC)

                ok = False
                candles = None
                for attempt in range(1, HIST_MAX_RETRIES + 1):
                    try:
                        candles = self.kite.historical_data(
                            instrument_token=token,
                            from_date=start,
                            to_date=end,
                            interval="minute",
                            continuous=False,
                            oi=False,
                        )
                        ok = True
                        break
                    except Exception as e:
                        log("WARN", f"[BACKFILL] HIST_FAIL {sym} attempt {attempt}/{HIST_MAX_RETRIES} err={e}")
                        time.sleep(HIST_SLEEP_SEC * (attempt + 1))

                if not ok or not candles:
                    failed += 1
                    continue

                rows = []
                for c in candles:
                    ts = to_ist_naive_auto(c["date"])
                    ts = floor_minute(ts)
                    rows.append((
                        sym, ts.isoformat(),
                        float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]),
                        int(c.get("volume") or 0),
                    ))

                conn.executemany("""
                    INSERT OR REPLACE INTO candles(symbol, ts, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                """, rows)
                conn.commit()

                fetched += 1
                self.manifest.setdefault("last_saved", {})[sym] = end.isoformat()

            self._rr_idx = (self._rr_idx + 1) % n

            save_manifest(self.d, self.manifest)
            log("INFO", f"[BACKFILL] SWEEP_DONE fetched={fetched} skip_have={skipped_have} skip_uptodate={skipped_uptodate} failed={failed} calls={calls_this_sweep}")
            time.sleep(BACKFILL_LOOP_SLEEP_SEC)

        try:
            conn.close()
        except Exception:
            pass


# ===================== HEARTBEAT THREAD =====================

class MinuteHeartbeat(threading.Thread):
    def __init__(self, runner: "LiveCandleCache", stop_event: threading.Event):
        super().__init__(daemon=True)
        self.runner = runner
        self.stop_event = stop_event

    def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                self.runner._force_close_once_per_minute()
            except Exception as e:
                log("WARN", f"[HEARTBEAT_THREAD] err={e}")
            time.sleep(HEARTBEAT_POLL_SEC)


# ===================== LIVE RUNNER =====================

class LiveCandleCache:
    def __init__(self, kite: KiteConnect, d: date, top_df: pd.DataFrame):
        self.kite = kite
        self.d = d
        ensure_dir(day_dir(d))

        self.db_file = db_path(d)
        self.writer_conn = open_db(self.db_file, check_same_thread=False)
        self.ro_conn = open_db_ro(self.db_file)

        self.manifest = load_manifest(d)

        symbols = [(r.exchange, r.tradingsymbol) for r in top_df.itertuples(index=False)]
        token_map = build_token_map(self.kite, symbols)

        self.symbols: List[Tuple[str, int]] = []
        for r in top_df.itertuples(index=False):
            ex = str(r.exchange).upper()
            ts = str(r.tradingsymbol).strip()
            tok = token_map.get((ex, ts))
            if tok is None:
                continue
            self.symbols.append((instrument_key(ex, ts), int(tok)))

        log("INFO", f"Top liquid with tokens resolved: {len(self.symbols)} / {len(top_df)}")

        self.token_to_symbol: Dict[int, str] = {tok: sym for sym, tok in self.symbols}
        self.builders: Dict[int, CandleBuilder] = {tok: CandleBuilder() for (_, tok) in self.symbols}

        # thread-safe shared state (ticks thread + heartbeat thread)
        self.state_lock = threading.RLock()

        # heartbeat last written candle state
        self.last_ts_by_symbol: Dict[str, datetime] = {}
        self.last_close_by_symbol: Dict[str, float] = {}
        self._bootstrap_last_state_from_db()

        self.stop_event = threading.Event()
        self.write_q: "queue.Queue[CandleRow]" = queue.Queue(maxsize=200000)
        self.writer = CandleWriter(self.writer_conn, self.write_q, self.stop_event)

        self.backfiller = HistoricalBackfiller(
            kite=self.kite,
            db_file=self.db_file,
            d=self.d,
            symbols=self.symbols,
            stop_event=self.stop_event,
            manifest=self.manifest,
        )

        self.heartbeat_thread: Optional[MinuteHeartbeat] = None

        # dropped write stats
        self._dropped = 0

        self.kws = KiteTicker(self.kite.api_key, self.kite.access_token)
        self.kws.on_connect = self._on_connect
        self.kws.on_ticks = self._on_ticks
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error

        self._last_force_close_min: Optional[datetime] = None

    # ---------- Safe queue ----------

    def _safe_put(self, row: CandleRow) -> bool:
        try:
            self.write_q.put_nowait(row)
            return True
        except queue.Full:
            self._dropped += 1
            if (self._dropped % DROP_LOG_EVERY) == 1:
                log("WARN", f"[QUEUE] FULL. dropped={self._dropped} (latest symbol={row.symbol})")
            return False

    # ---------- Heartbeat helpers ----------

    def _bootstrap_last_state_from_db(self) -> None:
        try:
            cur = self.ro_conn.execute("""
                SELECT c.symbol, c.ts, c.close
                FROM candles c
                JOIN (
                    SELECT symbol, MAX(ts) AS mts
                    FROM candles
                    GROUP BY symbol
                ) m
                ON c.symbol=m.symbol AND c.ts=m.mts;
            """)
            rows = cur.fetchall()
            for sym, ts_s, close in rows:
                try:
                    ts = datetime.fromisoformat(ts_s)
                    self.last_ts_by_symbol[str(sym)] = ts
                    self.last_close_by_symbol[str(sym)] = float(close)
                except Exception:
                    continue
            if HEARTBEAT_LOG:
                log("INFO", f"[HEARTBEAT] bootstrapped {len(self.last_ts_by_symbol)} symbols from DB")
        except Exception as e:
            log("WARN", f"[HEARTBEAT] bootstrap failed: {e}")

        # Manifest hints
        try:
            last_saved = (self.manifest or {}).get("last_saved", {}) or {}
            for sym, ts_s in last_saved.items():
                if sym in self.last_ts_by_symbol:
                    continue
                try:
                    self.last_ts_by_symbol[sym] = datetime.fromisoformat(ts_s)
                except Exception:
                    pass
        except Exception:
            pass

    def _db_last_candle_for_symbol(self, sym: str) -> Optional[Tuple[datetime, float]]:
        try:
            cur = self.ro_conn.execute(
                "SELECT ts, close FROM candles WHERE symbol=? ORDER BY ts DESC LIMIT 1;",
                (sym,)
            )
            row = cur.fetchone()
            if not row:
                return None
            return datetime.fromisoformat(row[0]), float(row[1])
        except Exception:
            return None

    def _record_last_written(self, sym: str, ts: datetime, close: float) -> None:
        if ts is None:
            return
        prev = self.last_ts_by_symbol.get(sym)
        if prev is None or ts >= prev:
            self.last_ts_by_symbol[sym] = ts
            self.last_close_by_symbol[sym] = float(close)
        self.manifest.setdefault("last_saved", {})[sym] = ts.isoformat()

    def _heartbeat_fill_gaps(self, last_closed: datetime) -> None:
        if not HEARTBEAT_FILL_ENABLED:
            return

        ss, se, last_session_closed = session_bounds(self.d)
        last_closed = min(last_closed, last_session_closed)
        if last_closed < ss:
            return

        inserted_total = 0
        symbols_touched = 0
        seed_budget = HEARTBEAT_SEED_BUDGET

        for sym, _tok in self.symbols:
            last_ts = self.last_ts_by_symbol.get(sym)
            last_close = self.last_close_by_symbol.get(sym)

            if (last_ts is None or last_close is None) and seed_budget > 0:
                seeded = self._db_last_candle_for_symbol(sym)
                seed_budget -= 1
                if seeded:
                    last_ts, last_close = seeded
                    self.last_ts_by_symbol[sym] = last_ts
                    self.last_close_by_symbol[sym] = last_close

            if last_ts is None or last_close is None:
                continue

            # Don't fill past market close or before market start
            if last_ts < ss:
                last_ts = ss

            if last_ts >= last_closed:
                continue

            gap_minutes = int((last_closed - last_ts).total_seconds() // 60)
            if gap_minutes <= 0:
                continue

            fill_n = min(gap_minutes, HEARTBEAT_MAX_FILL_MINUTES)

            inserted = 0
            for k in range(1, fill_n + 1):
                ts_fill = last_ts + timedelta(minutes=k)
                if ts_fill > last_closed:
                    break
                if self._safe_put(CandleRow(sym, ts_fill, last_close, last_close, last_close, last_close, 0)):
                    self._record_last_written(sym, ts_fill, last_close)
                    inserted += 1
                else:
                    break

            if inserted > 0:
                inserted_total += inserted
                symbols_touched += 1

        if HEARTBEAT_LOG and inserted_total > 0:
            log("DEBUG", f"[HEARTBEAT] filled {inserted_total} candles across {symbols_touched} symbols up to {last_closed.time()}")

    # ---------- Websocket callbacks ----------

    def _on_connect(self, ws, response):
        tokens = [tok for (_, tok) in self.symbols]
        log("INFO", f"WebSocket connected. Subscribing to {len(tokens)} tokens...")
        ws.subscribe(tokens)

        if WS_MODE.lower() == "full":
            ws.set_mode(ws.MODE_FULL, tokens)
        else:
            ws.set_mode(ws.MODE_QUOTE, tokens)

        if not self.writer.is_alive():
            self.writer.start()
        if not self.backfiller.is_alive():
            self.backfiller.start()

        if HEARTBEAT_THREAD_ENABLED and self.heartbeat_thread is None:
            self.heartbeat_thread = MinuteHeartbeat(self, self.stop_event)
            self.heartbeat_thread.start()
            log("INFO", "[HEARTBEAT_THREAD] started")

    def _on_close(self, ws, code, reason):
        log("WARN", f"WebSocket closed: code={code} reason={reason}")
        self.stop_event.set()

    def _on_error(self, ws, code, reason):
        log("WARN", f"WebSocket error: code={code} reason={reason}")

    def _force_close_once_per_minute(self):
        with self.state_lock:
            now = ist_now_naive()
            now_min = floor_minute(now)
            if self._last_force_close_min == now_min:
                return
            self._last_force_close_min = now_min

            ss, se, last_session_closed = session_bounds(self.d)
            last_closed = now_min - timedelta(minutes=1)
            last_closed = min(last_closed, last_session_closed)

            if last_closed < ss:
                return

            # 1) force-close builders
            for sym, tok in self.symbols:
                b = self.builders.get(tok)
                if not b:
                    continue
                completed = b.force_close_if_minute_closed(now)
                for ts, o, h, l, cl, v in completed:
                    if self._safe_put(CandleRow(sym, ts, o, h, l, cl, v)):
                        self._record_last_written(sym, ts, cl)

            # 2) heartbeat fill gaps (no ticks case)
            self._heartbeat_fill_gaps(last_closed)

            save_manifest(self.d, self.manifest)

    def _on_ticks(self, ws, ticks: List[Dict]):
        with self.state_lock:
            for t in ticks:
                tok = t.get("instrument_token")
                if tok not in self.builders:
                    continue

                ts = t.get("exchange_timestamp") or t.get("timestamp")
                if not ts:
                    continue
                ts = to_ist_naive_auto(ts)

                price = t.get("last_price")
                if price is None:
                    continue

                cum_vol = t.get("volume_traded") if WS_MODE.lower() == "full" else None

                completed = self.builders[tok].update(ts, float(price), cum_vol)
                if completed:
                    sym = self.token_to_symbol.get(tok)
                    if not sym:
                        continue
                    for c_ts, o, h, l, cl, v in completed:
                        if self._safe_put(CandleRow(sym, c_ts, o, h, l, cl, v)):
                            self._record_last_written(sym, c_ts, cl)

        # call once; fast no-op if already done by heartbeat thread
        self._force_close_once_per_minute()

    # ---------- Lifecycle ----------

    def start(self):
        log("STEP", f"Starting live cache for {self.d}")
        log("INFO", f"Universe CSV: {UNIVERSE_CSV}")
        log("INFO", f"DB: {self.db_file}")
        self.kws.connect(threaded=False)

    def stop(self):
        self.stop_event.set()
        try:
            self.kws.close()
        except Exception:
            pass
        try:
            if self.heartbeat_thread is not None:
                self.heartbeat_thread.join(timeout=2.0)
        except Exception:
            pass
        try:
            self.writer.join(timeout=3.0)
        except Exception:
            pass
        save_manifest(self.d, self.manifest)
        try:
            self.ro_conn.close()
        except Exception:
            pass
        try:
            self.writer_conn.close()
        except Exception:
            pass


# ===================== MAIN =====================

def main():
    log("STEP", "Initializing Kite API via OptionTradeUtils.intialize_kite_api() ...")
    kite = oUtils.intialize_kite_api()
    log("INFO", "Kite API initialized.")

    d = today_ist()
    ensure_dir(day_dir(d))

    universe = load_universe_csv(UNIVERSE_CSV)
    top_df = load_or_compute_top_liquid(kite, universe, d, TOP_N)

    runner = LiveCandleCache(kite, d, top_df)
    try:
        runner.start()
    except KeyboardInterrupt:
        log("WARN", "Interrupted by user.")
    finally:
        runner.stop()
        log("INFO", "Stopped.")

if __name__ == "__main__":
    main()

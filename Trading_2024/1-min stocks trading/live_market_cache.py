"""
live_market_cache.py

Live market cache builder for Top-N liquid stocks.

Key design decisions (optimizations + corrections)
- Uses ONE SQLite DB per day (fast upserts) instead of rewriting 500 Parquets repeatedly.
- WebSocket builds 1-minute candles live (best way to avoid Kite historical API limits).
- Historical API backfills only missing segments and is rate-limited.
- Backfill can be limited to last X minutes (optional) because your scanner only needs last 60 candles.

Inputs
- Universe CSV with columns: exchange, tradingsymbol (same style as your LiquidStocksScanner.py)

Outputs
- ./live_cache/YYYY-MM-DD/candles.sqlite  (table: candles)
- ./live_cache/YYYY-MM-DD/top_500_liquid.csv (cached top list for the day)
- ./live_cache/YYYY-MM-DD/manifest.json  (last saved minute per symbol)

Requirements
pip install kiteconnect pandas numpy pyarrow plotly
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
from kiteconnect import KiteConnect, KiteTicker, exceptions as kite_ex

import Trading_2024.OptionTradeUtils as oUtils   # same style as your LiquidStocksScanner.py


# ===================== USER CONFIG =====================

# Use the same CSV concept as LiquidStocksScanner.py.
# Your message had a garbled path; by default we pick the one in your attached LiquidStocksScanner.py.
# Override via env var UNIVERSE_CSV if needed.
DEFAULT_UNIVERSE_CSV = r"C:\Users\himan\Downloads\stock_list.csv"
UNIVERSE_CSV = os.environ.get("UNIVERSE_CSV", DEFAULT_UNIVERSE_CSV)

TOP_N = 500

# Quote batching for liquidity ranking
QUOTE_BATCH_SIZE = 100
QUOTE_RPS = 1.0  # keep <=1 to avoid 429

# Cache root
CACHE_ROOT = r"./live_cache"

# Session times (IST-naive)
SESSION_START = dtime(9, 15)
SESSION_END = dtime(15, 30)

# AUTO timezone handling (your rule): if timestamps look like ~03:45 at open => UTC-naive => shift +5:30
IST_OFFSET = timedelta(hours=5, minutes=30)

# WebSocket
WS_MODE = "full"  # "full" recommended for volume_traded

# Historical backfill pacing (Kite limits)
HIST_SLEEP_SEC = 0.40      # increase if you see rate-limit errors
HIST_MAX_RETRIES = 3

# Backfill strategy:
# - "FULL_DAY": backfill from 09:15 to last closed minute (heavy for 500 symbols)
# - "LOOKBACK": backfill only last BACKFILL_LOOKBACK_MIN minutes (much lighter; enough for last-60 scan)
BACKFILL_MODE = "LOOKBACK"
BACKFILL_LOOKBACK_MIN = 140  # keep > 60; 120-180 is practical

# Candle DB write batching
WRITE_FLUSH_SEC = 1.0
WRITE_BATCH_MAX = 5000


# ===================== LOG =====================

def log(level: str, msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ===================== TIME HELPERS =====================

def ist_now_naive() -> datetime:
    # assume local machine time is IST; we keep everything IST-naive
    return datetime.now()

def today_ist() -> date:
    return ist_now_naive().date()

def combine(d: date, t: dtime) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second)

def floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)

def is_utc_like_session_start(ts: datetime) -> bool:
    # heuristic: UTC-naive shows ~03:45 for IST 09:15
    return (ts.hour == 3 and 35 <= ts.minute <= 55)

def to_ist_naive_auto(ts: datetime) -> datetime:
    if ts is None:
        return ts
    if ts.tzinfo is not None:
        ts = ts.replace(tzinfo=None)
    if is_utc_like_session_start(ts):
        return ts + IST_OFFSET
    return ts


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


# ===================== LIQUIDITY (Top 500) =====================

def chunked(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def compute_liquidity_metrics(sym_key: str, q: Dict) -> Dict:
    exch, ts = sym_key.split(":", 1)

    last_price = float(q.get("last_price") or 0.0)
    volume = int(q.get("volume") or 0)

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

    if os.path.isfile(out_csv):
        log("INFO", f"Top-{top_n} already cached: {out_csv}")
        return pd.read_csv(out_csv)

    # Reality check: if run too early, today's "volume" may be zero or not meaningful.
    now = ist_now_naive()
    if now < combine(d, SESSION_START) + timedelta(minutes=2):
        log("WARN", "You are running before/near market open. Liquidity ranking may be unstable early.")

    sym_keys = [instrument_key(r.exchange, r.tradingsymbol) for r in universe_df.itertuples(index=False)]
    log("STEP", f"Computing liquidity snapshot for {len(sym_keys)} instruments (quote API)...")

    liq = fetch_liquidity_snapshot(kite, sym_keys)
    top = pick_top_liquid(liq, top_n)

    top.to_csv(out_csv, index=False)
    log("INFO", f"Saved Top-{len(top)} liquid stocks to {out_csv}")
    return top


# ===================== INSTRUMENT TOKENS =====================

INSTRUMENTS_CACHE_DIR = r"./kite_instruments_cache"
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

def open_db(path: str) -> sqlite3.Connection:
    ensure_dir(os.path.dirname(path))
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-200000;")  # ~200MB page cache if available
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles (
            symbol TEXT NOT NULL,
            ts TEXT NOT NULL,           -- ISO minute (IST-naive)
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

def db_last_ts(conn: sqlite3.Connection, symbol: str) -> Optional[datetime]:
    cur = conn.execute("SELECT ts FROM candles WHERE symbol=? ORDER BY ts DESC LIMIT 1;", (symbol,))
    row = cur.fetchone()
    if not row:
        return None
    return datetime.fromisoformat(row[0])

def db_count_since(conn: sqlite3.Connection, symbol: str, start_ts: datetime) -> int:
    cur = conn.execute("SELECT COUNT(1) FROM candles WHERE symbol=? AND ts>=?;", (symbol, start_ts.isoformat()))
    return int(cur.fetchone()[0])

def db_distinct_symbols(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute("SELECT DISTINCT symbol FROM candles;")
    return [r[0] for r in cur.fetchall()]


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
    """
    Builds 1-minute candles from ticks.

    Fix vs earlier draft:
    - When we force-close a candle due to time (no further ticks), we do NOT keep finalizing same minute forever.
    """

    def __init__(self):
        self.cur_minute: Optional[datetime] = None
        self.o = self.h = self.l = self.c = None
        self.vol = 0

        # cumulative volume from FULL mode ticks (if available)
        self.last_cum_vol: Optional[int] = None

        # last known close (for gap fill if next tick comes late)
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

        # Minute advanced: close previous and fill gaps (flat candles) if needed
        prev = self._finalize_current()
        if prev:
            completed.append(prev)

        # gap fill minutes between cur_minute+1 and m-1
        gap = self.cur_minute + timedelta(minutes=1)
        while gap < m and self.last_close is not None:
            completed.append((gap, self.last_close, self.last_close, self.last_close, self.last_close, 0))
            gap += timedelta(minutes=1)

        # start new minute
        self._start_minute(m, price)
        self.vol += inc_vol
        return completed

    def force_close_if_minute_closed(self, now_ts: datetime) -> List[Tuple[datetime, float, float, float, float, int]]:
        """
        If current minute is fully closed (<= last closed minute), finalize it once and reset
        so we don't keep emitting duplicates.
        """
        if self.cur_minute is None:
            return []

        now_ts = to_ist_naive_auto(now_ts)
        last_closed = floor_minute(now_ts) - timedelta(minutes=1)
        if self.cur_minute <= last_closed:
            prev = self._finalize_current()
            # RESET: do not keep a "current minute" without tick
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

        # final flush
        if buf:
            self._flush(buf)

    def _flush(self, rows: List[CandleRow]) -> None:
        if not rows:
            return
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
    def __init__(
        self,
        kite: KiteConnect,
        conn: sqlite3.Connection,
        d: date,
        symbols: List[Tuple[str, int]],  # (symbol_key, token)
        stop_event: threading.Event,
        manifest: Dict,
    ):
        super().__init__(daemon=True)
        self.kite = kite
        self.conn = conn
        self.d = d
        self.symbols = symbols
        self.stop_event = stop_event
        self.manifest = manifest

    def _calc_backfill_start(self, now: datetime) -> datetime:
        session_start = combine(self.d, SESSION_START)
        if BACKFILL_MODE.upper() == "FULL_DAY":
            return session_start
        # LOOKBACK
        lookback = now - timedelta(minutes=BACKFILL_LOOKBACK_MIN)
        return max(session_start, lookback)

    def run(self) -> None:
        log("STEP", f"Historical backfiller started (mode={BACKFILL_MODE}).")

        while not self.stop_event.is_set():
            now = ist_now_naive()
            last_closed = floor_minute(now) - timedelta(minutes=1)
            session_start = combine(self.d, SESSION_START)
            if last_closed < session_start:
                time.sleep(2.0)
                continue

            backfill_floor = self._calc_backfill_start(now)

            for sym, token in self.symbols:
                if self.stop_event.is_set():
                    break

                # If we already have enough candles since backfill_floor, skip.
                have = db_count_since(self.conn, sym, backfill_floor)
                if have >= 80:  # >60; enough for scan + cushion
                    continue

                last_ts = db_last_ts(self.conn, sym)
                start = backfill_floor if last_ts is None else max(backfill_floor, last_ts + timedelta(minutes=1))
                end = last_closed
                if start > end:
                    continue

                time.sleep(HIST_SLEEP_SEC)

                ok = False
                candles = None
                for attempt in range(1, HIST_MAX_RETRIES + 1):
                    if self.stop_event.is_set():
                        break
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
                    except kite_ex.GeneralException as e:
                        log("WARN", f"HIST {sym} attempt {attempt}/{HIST_MAX_RETRIES} failed: {e}")
                        time.sleep(HIST_SLEEP_SEC * (attempt + 1))
                    except Exception as e:
                        log("WARN", f"HIST {sym} attempt {attempt}/{HIST_MAX_RETRIES} error: {e}")
                        time.sleep(HIST_SLEEP_SEC * (attempt + 1))

                if not ok or not candles:
                    continue

                # Upsert candles into DB
                rows = []
                for c in candles:
                    ts = to_ist_naive_auto(c["date"])
                    ts = floor_minute(ts)
                    rows.append((
                        sym, ts.isoformat(),
                        float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]),
                        int(c.get("volume") or 0),
                    ))

                self.conn.executemany("""
                    INSERT OR REPLACE INTO candles(symbol, ts, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                """, rows)
                self.conn.commit()

                # manifest update
                self.manifest.setdefault("last_saved", {})[sym] = end.isoformat()

            save_manifest(self.d, self.manifest)
            time.sleep(5.0)


# ===================== LIVE RUNNER =====================

class LiveCandleCache:
    def __init__(self, kite: KiteConnect, d: date, top_df: pd.DataFrame):
        self.kite = kite
        self.d = d
        ensure_dir(day_dir(d))

        self.conn = open_db(db_path(d))
        self.manifest = load_manifest(d)

        # Resolve instrument tokens
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

        # builders by token
        self.builders: Dict[int, CandleBuilder] = {tok: CandleBuilder() for (_, tok) in self.symbols}

        # writer
        self.stop_event = threading.Event()
        self.write_q: "queue.Queue[CandleRow]" = queue.Queue(maxsize=200000)
        self.writer = CandleWriter(self.conn, self.write_q, self.stop_event)

        # backfiller
        self.backfiller = HistoricalBackfiller(
            kite=self.kite,
            conn=self.conn,
            d=self.d,
            symbols=self.symbols,
            stop_event=self.stop_event,
            manifest=self.manifest,
        )

        # websocket
        self.kws = KiteTicker(self.kite.api_key, self.kite.access_token)
        self.kws.on_connect = self._on_connect
        self.kws.on_ticks = self._on_ticks
        self.kws.on_close = self._on_close
        self.kws.on_error = self._on_error

        self._last_force_close_min: Optional[datetime] = None

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

    def _on_close(self, ws, code, reason):
        log("WARN", f"WebSocket closed: code={code} reason={reason}")
        self.stop_event.set()

    def _on_error(self, ws, code, reason):
        log("WARN", f"WebSocket error: code={code} reason={reason}")

    def _force_close_once_per_minute(self):
        """
        Ensures we finalize a minute even if last tick of that minute never arrived.
        """
        now = ist_now_naive()
        now_min = floor_minute(now)
        if self._last_force_close_min == now_min:
            return
        self._last_force_close_min = now_min

        for sym, tok in self.symbols:
            b = self.builders.get(tok)
            if not b:
                continue
            completed = b.force_close_if_minute_closed(now)
            for c in completed:
                ts, o, h, l, cl, v = c
                self.write_q.put(CandleRow(sym, ts, o, h, l, cl, v))
                self.manifest.setdefault("last_saved", {})[sym] = ts.isoformat()

        save_manifest(self.d, self.manifest)

    def _on_ticks(self, ws, ticks: List[Dict]):
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

            cum_vol = None
            # FULL mode often provides volume_traded; if not present, keep volume 0
            if WS_MODE.lower() == "full":
                cum_vol = t.get("volume_traded")

            completed = self.builders[tok].update(ts, float(price), cum_vol)
            if completed:
                # map token -> symbol string
                # (small linear lookup; for 500 it's fine)
                sym = None
                for s, tk in self.symbols:
                    if tk == tok:
                        sym = s
                        break
                if sym is None:
                    continue

                for c in completed:
                    c_ts, o, h, l, cl, v = c
                    self.write_q.put(CandleRow(sym, c_ts, o, h, l, cl, v))
                    self.manifest.setdefault("last_saved", {})[sym] = c_ts.isoformat()

        # once per minute force-close
        self._force_close_once_per_minute()

    def start(self):
        log("STEP", f"Starting live cache for {self.d}")
        log("INFO", f"DB: {db_path(self.d)}")
        self.kws.connect(threaded=False)

    def stop(self):
        self.stop_event.set()
        try:
            self.kws.close()
        except Exception:
            pass
        try:
            self.writer.join(timeout=3.0)
        except Exception:
            pass
        save_manifest(self.d, self.manifest)
        try:
            self.conn.close()
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

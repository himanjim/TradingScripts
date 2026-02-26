import time
from datetime import datetime
from typing import List, Dict, Any, Iterable, Optional

import pandas as pd
from kiteconnect import KiteConnect, exceptions as kite_ex

import Trading_2024.OptionTradeUtils as oUtils  # your existing helper


# =========================
# USER CONFIG
# =========================

INPUT_CSV = r"C:\Users\himan\Downloads\stock_list.csv"  # required columns: exchange, tradingsymbol

TOP_N = 150                       # how many most liquid (tightest spread) to keep
BATCH_SIZE = 250                  # fewer API calls; keep comfortably below quote() max
QUOTE_RATE_LIMIT_RPS = 1.0        # keep conservative; tune if you know your safe rate
MAX_RETRIES = 3                   # retries per batch on network/temporary issues
RETRY_SLEEP_SEC = 2.0             # base backoff; will be multiplied by attempt index

SNAPSHOTS = 5                     # 3–5 recommended to avoid one-tick anomalies
SNAPSHOT_GAP_SEC = 2.0            # spacing between snapshots (can be 1–3 seconds)

OUTPUT_CSV = "./top_liquid_stocks_by_spread.csv"


# =========================
# SIMPLE LOGGER
# =========================

def log(level: str, msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# =========================
# HELPERS
# =========================

def load_stock_list(csv_path: str) -> pd.DataFrame:
    """
    Load list of stocks from CSV.
    Required columns: exchange, tradingsymbol
    """
    log("STEP", f"Reading stock list from CSV: {csv_path}")
    df = pd.read_csv(csv_path, dtype=str)

    required = {"exchange", "tradingsymbol"}
    missing = required - set(df.columns.str.lower())
    # allow case-insensitive columns
    cols = {c.lower(): c for c in df.columns}
    if missing:
        raise ValueError("CSV must contain columns: 'exchange', 'tradingsymbol' (case-insensitive).")

    exch_col = cols["exchange"]
    sym_col = cols["tradingsymbol"]

    df["exchange"] = df[exch_col].astype(str).str.upper().str.strip()
    df["tradingsymbol"] = df[sym_col].astype(str).str.strip()

    # Drop duplicates
    before = len(df)
    df = df.dropna(subset=["exchange", "tradingsymbol"])
    df = df.drop_duplicates(subset=["exchange", "tradingsymbol"]).reset_index(drop=True)
    after = len(df)
    log("INFO", f"Loaded {after} unique instruments (from {before} rows).")
    return df[["exchange", "tradingsymbol"]]


def instrument_key(exchange: str, tradingsymbol: str) -> str:
    """Build a Kite quote key like 'NSE:INFY'."""
    return f"{exchange}:{tradingsymbol}"


def chunked(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _best_price(depth_side: List[Dict[str, Any]]) -> Optional[float]:
    if not depth_side:
        return None
    p = depth_side[0].get("price")
    return float(p) if p is not None else None


def _sum_qty(depth_side: List[Dict[str, Any]]) -> int:
    return int(sum(int(level.get("quantity", 0) or 0) for level in depth_side))


def compute_metrics(symbol: str, q: Dict[str, Any]) -> Dict[str, Any]:
    """
    Spread-centric metrics from kite.quote() data for a single symbol key.
    """
    exch, ts = symbol.split(":", 1)

    last_price = float(q.get("last_price") or 0.0)
    volume = int(q.get("volume") or 0)

    depth = q.get("depth") or {}
    buy_depth = depth.get("buy") or []
    sell_depth = depth.get("sell") or []

    best_bid = _best_price(buy_depth)
    best_ask = _best_price(sell_depth)

    bid_qty = _sum_qty(buy_depth)
    ask_qty = _sum_qty(sell_depth)
    total_depth_qty = bid_qty + ask_qty

    # Spread in basis points (bps) is numerically stable
    if best_bid is not None and best_ask is not None and best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
        mid = (best_bid + best_ask) / 2.0
        spread_abs = best_ask - best_bid
        spread_bps = (spread_abs / mid) * 10_000.0  # 1% = 100 bps
        spread_pct = (spread_abs / mid) * 100.0
    else:
        spread_bps = float("inf")
        spread_pct = float("inf")

    # "Turnover" proxy (₹). LTP*volume is fine as a proxy; keep it secondary.
    turnover = last_price * float(volume)

    return {
        "instrument": symbol,
        "exchange": exch,
        "tradingsymbol": ts,
        "last_price": last_price,
        "volume": volume,
        "turnover": turnover,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_bps": spread_bps,
        "spread_pct": spread_pct,
        "bid_depth_qty": bid_qty,
        "ask_depth_qty": ask_qty,
        "total_depth_qty": total_depth_qty,
    }


class RateLimiter:
    """
    Enforces an average requests/sec ceiling without drift when retries happen.
    """
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.1)
        self._last = 0.0

    def wait(self):
        now = time.time()
        elapsed = now - self._last
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last = time.time()


def quote_with_retries(kite: KiteConnect, batch: List[str], limiter: RateLimiter) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            limiter.wait()
            return kite.quote(batch)
        except (kite_ex.NetworkException, kite_ex.TimeoutException) as e:
            last_err = e
            log("WARN", f"Network/Timeout on quote() attempt {attempt}/{MAX_RETRIES}: {e}")
        except kite_ex.TokenException as e:
            # Auth issues: no point retrying
            raise
        except Exception as e:
            # Temporary server errors can land here too; retry conservatively
            last_err = e
            log("WARN", f"Error on quote() attempt {attempt}/{MAX_RETRIES}: {e}")

        time.sleep(RETRY_SLEEP_SEC * attempt)

    raise RuntimeError(f"quote() failed after {MAX_RETRIES} retries. Last error: {last_err}")


def fetch_snapshot(kite: KiteConnect, symbols: List[str], limiter: RateLimiter) -> pd.DataFrame:
    rows = []
    for i, batch in enumerate(chunked(symbols, BATCH_SIZE), start=1):
        log("INFO", f"[BATCH {i}] Requesting quotes for {len(batch)} instruments")
        try:
            quotes = quote_with_retries(kite, batch, limiter)
        except Exception as e:
            log("ERROR", f"Batch {i} failed permanently: {e}. Marking these instruments as missing.")
            # Still keep placeholders so they don't silently vanish
            for sym in batch:
                rows.append({"instrument": sym, "spread_bps": float("inf"), "spread_pct": float("inf"),
                             "turnover": 0.0, "total_depth_qty": 0, "exchange": sym.split(":")[0],
                             "tradingsymbol": sym.split(":")[1], "last_price": 0.0, "volume": 0,
                             "best_bid": None, "best_ask": None, "bid_depth_qty": 0, "ask_depth_qty": 0})
            continue

        for sym in batch:
            q = quotes.get(sym)
            if not q:
                rows.append({"instrument": sym, "spread_bps": float("inf"), "spread_pct": float("inf"),
                             "turnover": 0.0, "total_depth_qty": 0, "exchange": sym.split(":")[0],
                             "tradingsymbol": sym.split(":")[1], "last_price": 0.0, "volume": 0,
                             "best_bid": None, "best_ask": None, "bid_depth_qty": 0, "ask_depth_qty": 0})
                continue
            rows.append(compute_metrics(sym, q))

    return pd.DataFrame(rows)


def aggregate_snapshots(all_snaps: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate SNAPSHOTS rows per instrument into robust medians.
    """
    if all_snaps.empty:
        return all_snaps

    g = all_snaps.groupby("instrument", as_index=False)

    agg = g.agg(
        exchange=("exchange", "first"),
        tradingsymbol=("tradingsymbol", "first"),
        last_price=("last_price", "median"),
        volume=("volume", "median"),
        turnover=("turnover", "median"),
        spread_bps=("spread_bps", "median"),
        spread_pct=("spread_pct", "median"),
        total_depth_qty=("total_depth_qty", "median"),
        best_bid=("best_bid", "median"),
        best_ask=("best_ask", "median"),
    )

    # Helpful flags
    agg["has_depth"] = (agg["spread_bps"] != float("inf"))
    return agg


def pick_top_liquid(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """
    Spread-first ranking:
      1) spread_bps ASC (tighter is better)
      2) turnover DESC (higher activity is better)
      3) total_depth_qty DESC (more depth is better)
    """
    if df.empty:
        return df

    df_sorted = df.sort_values(
        by=["spread_bps", "turnover", "total_depth_qty"],
        ascending=[True, False, False],
    ).reset_index(drop=True)

    return df_sorted.head(top_n)


# =========================
# MAIN
# =========================

def main():
    log("STEP", "Initializing Kite API via OptionTradeUtils.intialize_kite_api() ...")
    kite = oUtils.intialize_kite_api()
    log("INFO", "Kite API initialized.")

    stock_df = load_stock_list(INPUT_CSV)

    # vectorized key build (faster than apply)
    stock_df["instrument_key"] = stock_df["exchange"].str.upper() + ":" + stock_df["tradingsymbol"]
    symbols = stock_df["instrument_key"].tolist()
    log("INFO", f"Total instruments in universe: {len(symbols)}")

    limiter = RateLimiter(QUOTE_RATE_LIMIT_RPS)

    log("STEP", f"Taking {SNAPSHOTS} quote snapshots (gap={SNAPSHOT_GAP_SEC}s) ...")
    snaps = []
    for s in range(1, SNAPSHOTS + 1):
        log("INFO", f"Snapshot {s}/{SNAPSHOTS}")
        snap_df = fetch_snapshot(kite, symbols, limiter)
        snap_df["snapshot_idx"] = s
        snaps.append(snap_df)

        if s < SNAPSHOTS:
            time.sleep(SNAPSHOT_GAP_SEC)

    all_snaps = pd.concat(snaps, ignore_index=True)
    agg_df = aggregate_snapshots(all_snaps)

    # Spread-first top N
    top_df = pick_top_liquid(agg_df, TOP_N)

    log("INFO", "Top few (spread-first) liquid stocks:")
    cols = ["exchange", "tradingsymbol", "spread_bps", "spread_pct", "turnover", "total_depth_qty", "has_depth"]
    print(top_df[cols].head(25))

    log("STEP", f"Saving top {len(top_df)} to: {OUTPUT_CSV}")
    top_df.to_csv(OUTPUT_CSV, index=False)
    log("INFO", "Saved successfully.")


if __name__ == "__main__":
    main()

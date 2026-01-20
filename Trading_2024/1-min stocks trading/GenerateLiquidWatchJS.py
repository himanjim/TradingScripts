import json
import time
from datetime import datetime
from typing import List, Dict, Optional

import pandas as pd
from kiteconnect import KiteConnect, exceptions as kite_ex

import Trading_2024.OptionTradeUtils as oUtils   # your existing helper


# ========== USER CONFIG ==========

INPUT_CSV = r"C:\Users\Local User\Downloads\stock_list.csv"   # must have columns: exchange, tradingsymbol
EXCHANGE_FILTER = "NSE"                                 # rotator typically uses one exchange
TOP_N = 50                                               # output count
BATCH_SIZE = 100                                         # quote() instruments per call (<=500)
QUOTE_RATE_LIMIT_RPS = 1.0                               # keep <= 1 req/sec safe

# Multi-snapshot sampling (3–5 min recommended)
SNAPSHOTS = 5                                            # e.g., 5 snapshots
SNAPSHOT_INTERVAL_SEC = 60                               # e.g., 60 sec => ~4 min span (t0..t4)

# Outputs
OUTPUT_JS = "./top_50_liquid_watch.js"
PRINT_JS_TO_CONSOLE = True

# Optional (debug)
OUTPUT_CSV_DEBUG = "./top_50_liquid_debug.csv"           # set to "" to disable


# ========== SIMPLE LOGGER ==========

def log(level: str, msg: str):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# ========== HELPERS ==========

def load_stock_list(csv_path: str) -> pd.DataFrame:
    """
    Load list of stocks from CSV.
    Required columns: exchange, tradingsymbol
    """
    log("STEP", f"Reading stock list from CSV: {csv_path}")
    df = pd.read_csv(csv_path)

    if "exchange" not in df.columns or "tradingsymbol" not in df.columns:
        raise ValueError("CSV must contain columns: 'exchange', 'tradingsymbol'")

    df["exchange"] = df["exchange"].astype(str).str.upper().str.strip()
    df["tradingsymbol"] = df["tradingsymbol"].astype(str).str.strip()

    before = len(df)
    df = df.drop_duplicates(subset=["exchange", "tradingsymbol"]).reset_index(drop=True)
    after = len(df)
    log("INFO", f"Loaded {after} unique instruments (from {before} rows).")
    return df


def instrument_key(exchange: str, tradingsymbol: str) -> str:
    """Build a Kite quote key like 'NSE:INFY'"""
    return f"{exchange.upper()}:{tradingsymbol.strip()}"


def chunked(seq: List[str], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


class RateLimiter:
    """
    Keeps overall API rate <= QUOTE_RATE_LIMIT_RPS across ALL batches/snapshots.
    """
    def __init__(self, rps: float):
        self.min_interval = 1.0 / max(rps, 0.1)
        self._next_time = time.monotonic()

    def wait(self):
        now = time.monotonic()
        if now < self._next_time:
            time.sleep(self._next_time - now)
        self._next_time = time.monotonic() + self.min_interval


def compute_metrics(symbol: str, q: Dict, snapshot_id: int, ts_epoch: float) -> Dict:
    """
    Compute liquidity metrics from kite.quote() data for a single symbol key.
    Adds instrument_token (needed for CIQ chart URL).
    """
    exch, ts = symbol.split(":", 1)

    last_price = float(q.get("last_price") or 0.0)
    volume = int(q.get("volume") or 0)

    # instrument_token is typically present in quote() response
    instrument_token = q.get("instrument_token", None)

    depth = q.get("depth") or {}
    buy_depth = depth.get("buy") or []
    sell_depth = depth.get("sell") or []

    best_bid = buy_depth[0]["price"] if buy_depth else None
    best_ask = sell_depth[0]["price"] if sell_depth else None

    bid_qty = sum(int(level.get("quantity", 0) or 0) for level in buy_depth)
    ask_qty = sum(int(level.get("quantity", 0) or 0) for level in sell_depth)
    total_depth_qty = bid_qty + ask_qty

    if best_bid is not None and best_ask is not None and best_ask > 0 and best_bid > 0:
        mid = (best_bid + best_ask) / 2.0
        spread_pct = ((best_ask - best_bid) / mid) * 100.0
    else:
        spread_pct = float("inf")

    return {
        "snapshot_id": snapshot_id,
        "ts_epoch": ts_epoch,
        "exchange": exch,
        "tradingsymbol": ts,
        "instrument_token": instrument_token,
        "last_price": last_price,
        "volume": volume,
        "spread_pct": spread_pct,
        "total_depth_qty": total_depth_qty,
    }


def fetch_snapshot(kite: KiteConnect, limiter: RateLimiter, symbols: List[str], snapshot_id: int) -> pd.DataFrame:
    """
    Fetch quote snapshot for the whole universe; returns a DataFrame for this snapshot.
    """
    rows = []
    missing_count = 0
    ts_epoch = time.time()

    log("STEP", f"Snapshot {snapshot_id+1}/{SNAPSHOTS}: fetching quotes for {len(symbols)} instruments...")

    for i, batch in enumerate(chunked(symbols, BATCH_SIZE), start=1):
        try:
            limiter.wait()
            quotes = kite.quote(batch)
        except kite_ex.NetworkException as e:
            log("WARN", f"NetworkException on snapshot {snapshot_id+1}, batch {i}: {e}. Skipping batch.")
            continue
        except Exception as e:
            log("WARN", f"Error on snapshot {snapshot_id+1}, batch {i}: {e}. Skipping batch.")
            continue

        for sym in batch:
            q = quotes.get(sym)
            if not q:
                missing_count += 1
                continue
            rows.append(compute_metrics(sym, q, snapshot_id, ts_epoch))

    if missing_count:
        log("INFO", f"Snapshot {snapshot_id+1}: missing quotes for {missing_count} symbols (skipped).")

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    log("INFO", f"Snapshot {snapshot_id+1}: got {len(df)} rows.")
    return df


def backfill_tokens_from_instruments(kite: KiteConnect, exchange: str, symbols: List[str]) -> Dict[str, int]:
    """
    Backfill instrument_token for selected tradingsymbols using kite.instruments(exchange).
    Called ONLY if needed for the top set (keeps it fast).
    """
    need = set(s.upper() for s in symbols)
    token_map: Dict[str, int] = {}

    log("STEP", f"Backfilling tokens from kite.instruments('{exchange}') for {len(need)} symbols...")
    try:
        inst = kite.instruments(exchange)
    except Exception as e:
        log("WARN", f"kite.instruments('{exchange}') failed: {e}")
        return token_map

    for row in inst:
        ts = str(row.get("tradingsymbol", "")).upper()
        if ts in need:
            tok = row.get("instrument_token")
            if tok is not None:
                token_map[ts] = int(tok)
                if len(token_map) == len(need):
                    break

    log("INFO", f"Backfill token_map size: {len(token_map)}")
    return token_map


def aggregate_liquidity(long_df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates multi-snapshot data into stable metrics:
      - recent_volume: sum of positive volume deltas
      - recent_turnover: sum(delta_volume * last_price_at_snapshot)
      - avg_spread_pct: mean spread over snapshots (inf treated as NaN)
      - avg_depth_qty: mean total_depth_qty
      - day_turnover: last_price_last * volume_last (fallback ranking)
    """
    if long_df.empty:
        return long_df

    # Sort for diff calculations
    long_df = long_df.sort_values(by=["exchange", "tradingsymbol", "snapshot_id"]).reset_index(drop=True)

    # Volume deltas between snapshots (clip negative)
    long_df["delta_volume"] = (
        long_df.groupby(["exchange", "tradingsymbol"])["volume"]
        .diff()
        .fillna(0)
        .clip(lower=0)
    )

    # Turnover based on delta volume at that snapshot price
    long_df["delta_turnover"] = long_df["delta_volume"] * long_df["last_price"]

    # Spread: treat inf as NaN for averaging (illiquid depth)
    spread = long_df["spread_pct"].replace([float("inf")], pd.NA)
    long_df["spread_pct_for_avg"] = spread

    # Last snapshot per symbol
    last_rows = long_df.groupby(["exchange", "tradingsymbol"], as_index=False).tail(1).copy()
    last_rows = last_rows.rename(columns={
        "last_price": "last_price_last",
        "volume": "volume_last",
        "instrument_token": "instrument_token_last",
    })

    last_rows["day_turnover"] = last_rows["last_price_last"] * last_rows["volume_last"]

    agg = long_df.groupby(["exchange", "tradingsymbol"], as_index=False).agg(
        recent_volume=("delta_volume", "sum"),
        recent_turnover=("delta_turnover", "sum"),
        avg_spread_pct=("spread_pct_for_avg", "mean"),
        avg_depth_qty=("total_depth_qty", "mean"),
        avg_price=("last_price", "mean"),
        token_any=("instrument_token", lambda s: next((x for x in s if pd.notna(x) and x), None)),
    )

    # Merge last snapshot info
    out = agg.merge(
        last_rows[["exchange", "tradingsymbol", "last_price_last", "volume_last", "instrument_token_last", "day_turnover"]],
        on=["exchange", "tradingsymbol"],
        how="left"
    )

    # Prefer token_any; else token from last snapshot
    out["instrument_token"] = out["token_any"]
    out.loc[out["instrument_token"].isna(), "instrument_token"] = out.loc[out["instrument_token"].isna(), "instrument_token_last"]

    # If avg_spread_pct is NaN (all inf), set to inf
    out["avg_spread_pct"] = out["avg_spread_pct"].fillna(float("inf"))

    return out


def pick_top_liquid(df: pd.DataFrame, exchange: str, top_n: int) -> pd.DataFrame:
    """
    Ranking:
      1) recent_turnover desc (multi-snapshot, reduces one-tick anomaly)
      2) day_turnover desc (fallback stability)
      3) avg_spread_pct asc (tighter better)
      4) avg_depth_qty desc (more depth better)
    """
    df = df[df["exchange"].str.upper() == exchange.upper()].copy()
    if df.empty:
        return df

    df_sorted = df.sort_values(
        by=["recent_turnover", "day_turnover", "avg_spread_pct", "avg_depth_qty"],
        ascending=[False, False, True, False],
    ).reset_index(drop=True)

    return df_sorted.head(top_n).copy()


def format_watch_js(top_df: pd.DataFrame) -> str:
    """
    Output exactly:
    const WATCH = [
      { sym: "CANBK", token: "2763265" },
      ...
    ];
    """
    lines = ["const WATCH = ["]
    for _, r in top_df.iterrows():
        sym = str(r["tradingsymbol"])
        tok = str(int(r["instrument_token"]))  # ensure numeric-string
        lines.append(f'  {{ sym: "{sym}", token: "{tok}" }},')
    lines.append("];")
    return "\n".join(lines)


# ========== MAIN ==========

def main():
    log("STEP", "Initializing Kite API via OptionTradeUtils.intialize_kite_api() ...")
    kite = oUtils.intialize_kite_api()
    log("INFO", "Kite API initialized.")

    stock_df = load_stock_list(INPUT_CSV)
    stock_df = stock_df[stock_df["exchange"].str.upper() == EXCHANGE_FILTER.upper()].reset_index(drop=True)
    if stock_df.empty:
        raise ValueError(f"No rows found for exchange={EXCHANGE_FILTER} in {INPUT_CSV}")

    stock_df["instrument_key"] = stock_df.apply(
        lambda r: instrument_key(r["exchange"], r["tradingsymbol"]),
        axis=1
    )
    symbols = stock_df["instrument_key"].tolist()
    log("INFO", f"Universe size (filtered): {len(symbols)}")

    limiter = RateLimiter(QUOTE_RATE_LIMIT_RPS)

    # Multi-snapshot collection with consistent spacing
    frames = []
    start = time.monotonic()
    for s in range(SNAPSHOTS):
        df_snap = fetch_snapshot(kite, limiter, symbols, snapshot_id=s)
        if not df_snap.empty:
            frames.append(df_snap)

        # schedule next snapshot (keeps ~interval spacing)
        next_t = start + (s + 1) * SNAPSHOT_INTERVAL_SEC
        sleep_for = next_t - time.monotonic()
        if s < SNAPSHOTS - 1 and sleep_for > 0:
            log("INFO", f"Waiting {sleep_for:.1f}s for next snapshot...")
            time.sleep(sleep_for)

    if not frames:
        log("ERROR", "No snapshots fetched. Exiting.")
        return

    long_df = pd.concat(frames, ignore_index=True)
    log("INFO", f"Collected {len(long_df)} total rows across {len(frames)} snapshots.")

    agg_df = aggregate_liquidity(long_df)
    if agg_df.empty:
        log("ERROR", "Aggregation produced empty output. Exiting.")
        return

    top_df = pick_top_liquid(agg_df, EXCHANGE_FILTER, TOP_N)
    if top_df.empty:
        log("ERROR", "No top stocks selected (empty after filtering). Exiting.")
        return

    # Ensure token present; backfill only for missing in TOP set
    missing = top_df["instrument_token"].isna() | (top_df["instrument_token"] == 0)
    if missing.any():
        syms_missing = top_df.loc[missing, "tradingsymbol"].astype(str).tolist()
        token_map = backfill_tokens_from_instruments(kite, EXCHANGE_FILTER, syms_missing)
        for i, r in top_df.loc[missing].iterrows():
            tsu = str(r["tradingsymbol"]).upper()
            if tsu in token_map:
                top_df.at[i, "instrument_token"] = token_map[tsu]

    # Drop any still-missing tokens (cannot build CIQ chart URL)
    top_df = top_df.dropna(subset=["instrument_token"]).copy()

    # Optional debug CSV
    if OUTPUT_CSV_DEBUG:
        top_df.to_csv(OUTPUT_CSV_DEBUG, index=False)
        log("INFO", f"Saved debug CSV: {OUTPUT_CSV_DEBUG}")

    js_text = format_watch_js(top_df)

    with open(OUTPUT_JS, "w", encoding="utf-8") as f:
        f.write(js_text + "\n")
    log("INFO", f"Saved JS WATCH list: {OUTPUT_JS} (items={len(top_df)})")

    if PRINT_JS_TO_CONSOLE:
        print("\n===== COPY-PASTE WATCH ARRAY BELOW =====\n")
        print(js_text)
        print("\n===== COPY-PASTE WATCH ARRAY ABOVE =====\n")


if __name__ == "__main__":
    main()

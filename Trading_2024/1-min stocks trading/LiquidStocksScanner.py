import time
from datetime import datetime
from typing import List, Dict

import pandas as pd
from kiteconnect import KiteConnect, exceptions as kite_ex

import Trading_2024.OptionTradeUtils as oUtils   # your existing helper


# ========== USER CONFIG ==========

INPUT_CSV = r"C:\Users\himan\Downloads\stock_list.csv"  # must have columns: exchange, tradingsymbol
TOP_N = 500                       # number of most liquid stocks to keep
BATCH_SIZE = 100                  # number of instruments per quote() call (<=500 per docs)
QUOTE_RATE_LIMIT_RPS = 1          # quote API ~1 request per second; keep <=1 to be safe
OUTPUT_CSV = "./top_500_liquid_stocks.csv"


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

    # Drop duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["exchange", "tradingsymbol"]).reset_index(drop=True)
    after = len(df)
    log("INFO", f"Loaded {after} unique instruments (from {before} rows).")
    return df


def instrument_key(exchange: str, tradingsymbol: str) -> str:
    """
    Build a Kite quote key like 'NSE:INFY'
    """
    return f"{exchange.upper()}:{tradingsymbol.strip()}"


def chunked(seq: List[str], n: int):
    """Yield successive n-sized chunks from a list."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def compute_liquidity_metrics(symbol: str, q: Dict) -> Dict:
    """
    Compute liquidity metrics from kite.quote() data for a single symbol key.
    Returns a dict with fields for DataFrame construction.
    """
    # symbol is like "NSE:INFY"
    exch, ts = symbol.split(":", 1)

    last_price = q.get("last_price", 0.0) or 0.0
    volume = q.get("volume", 0) or 0

    # Depth
    depth = q.get("depth") or {}
    buy_depth = depth.get("buy") or []
    sell_depth = depth.get("sell") or []

    best_bid = buy_depth[0]["price"] if buy_depth else None
    best_ask = sell_depth[0]["price"] if sell_depth else None

    # Sum depth quantities (level 1–5)
    bid_qty = sum(level.get("quantity", 0) for level in buy_depth)
    ask_qty = sum(level.get("quantity", 0) for level in sell_depth)
    total_depth_qty = bid_qty + ask_qty

    # Spread %
    if best_bid is not None and best_ask is not None and best_ask > 0 and best_bid > 0:
        mid = (best_bid + best_ask) / 2.0
        spread_abs = best_ask - best_bid
        spread_pct = (spread_abs / mid) * 100.0
    else:
        # No proper depth -> treat as very illiquid
        spread_pct = float("inf")

    # Turnover (₹)
    turnover = float(last_price) * float(volume)

    return {
        "exchange": exch,
        "tradingsymbol": ts,
        "last_price": last_price,
        "volume": volume,
        "turnover": turnover,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_pct": spread_pct,
        "bid_depth_qty": bid_qty,
        "ask_depth_qty": ask_qty,
        "total_depth_qty": total_depth_qty,
    }


def fetch_liquidity_snapshot_for_universe(kite: KiteConnect, symbols: List[str]) -> pd.DataFrame:
    """
    Fetch quote data for all symbols (list of 'NSE:INFY', etc) in batches,
    compute liquidity metrics, and return a DataFrame.
    """
    results = []

    log("STEP", f"Fetching quote snapshot for {len(symbols)} instruments in batches of {BATCH_SIZE} ...")

    for i, batch in enumerate(chunked(symbols, BATCH_SIZE), start=1):
        log("INFO", f"[BATCH {i}] Requesting quotes for {len(batch)} instruments")
        try:
            # Respect rate limit: sleep to keep <=1 req/sec
            time.sleep(1.0 / max(QUOTE_RATE_LIMIT_RPS, 0.1))

            quotes = kite.quote(batch)  # dict keyed by symbol string
        except kite_ex.NetworkException as e:
            log("WARN", f"NetworkException on quote batch {i}: {e}. Skipping this batch.")
            continue
        except Exception as e:
            log("WARN", f"Error on quote batch {i}: {e}. Skipping this batch.")
            continue

        for sym in batch:
            q = quotes.get(sym)
            if not q:
                log("WARN", f"No quote data returned for {sym}, skipping.")
                continue

            metrics = compute_liquidity_metrics(sym, q)
            results.append(metrics)

    if not results:
        log("ERROR", "No liquidity metrics could be computed (no quotes).")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    log("INFO", f"Liquidity snapshot DataFrame shape: {df.shape[0]} rows × {df.shape[1]} columns")
    return df


def pick_top_liquid_stocks(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """
    Sort stocks by:
      1) turnover descending (higher is better)
      2) spread_pct ascending (tighter is better)
      3) total_depth_qty descending (more depth is better)
    and pick top_n.
    """
    if df.empty:
        log("ERROR", "Input DataFrame is empty. Cannot rank liquidity.")
        return df

    df_sorted = df.sort_values(
        by=["turnover", "spread_pct", "total_depth_qty"],
        ascending=[False, True, False],
    ).reset_index(drop=True)

    top_df = df_sorted.head(top_n)
    log("INFO", f"Selected top {len(top_df)} most liquid stocks.")
    return top_df


# ========== MAIN ==========

def main():
    # Init Kite
    log("STEP", "Initializing Kite API via OptionTradeUtils.intialize_kite_api() ...")
    kite = oUtils.intialize_kite_api()
    log("INFO", "Kite API initialized.")

    # Load universe
    stock_df = load_stock_list(INPUT_CSV)

    # Build instrument keys for quote()
    stock_df["instrument_key"] = stock_df.apply(
        lambda r: instrument_key(r["exchange"], r["tradingsymbol"]),
        axis=1,
    )
    symbols = stock_df["instrument_key"].tolist()
    log("INFO", f"Total instruments in universe: {len(symbols)}")

    # Fetch quote snapshot & compute liquidity metrics
    liq_df = fetch_liquidity_snapshot_for_universe(kite, symbols)

    if liq_df.empty:
        log("ERROR", "No data fetched; exiting.")
        return

    # Pick top N by liquidity
    top_liquid_df = pick_top_liquid_stocks(liq_df, TOP_N)

    # Show a small sample
    log("INFO", "Top few liquid stocks:")
    print(top_liquid_df.head(20))

    # Save to CSV
    log("STEP", f"Saving top {len(top_liquid_df)} liquid stocks to: {OUTPUT_CSV}")
    top_liquid_df.to_csv(OUTPUT_CSV, index=False)
    log("INFO", "Saved successfully.")


if __name__ == "__main__":
    main()
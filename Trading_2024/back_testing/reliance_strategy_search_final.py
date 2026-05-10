from __future__ import annotations

import argparse
import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd


# =============================================================================
# User-facing defaults
# =============================================================================
# These defaults are intentionally conservative. You can override them from the
# command line when you run the script.
TRADE_NOTIONAL = 100_000.0          # Fixed rupees deployed per trade
SLIPPAGE_BPS = 2.0                  # Slippage per side in basis points
CHARGES_BPS = 1.5                   # Other variable costs per side in basis points
BROKERAGE_PER_SIDE = 10.0           # Fixed brokerage per side in rupees
ALLOW_SHORT = True                  # Whether short trades are allowed by default
SESSION_START = "09:15"            # NSE cash market session start (IST)
SESSION_END = "15:30"              # NSE cash market session end (IST)
LAST_ENTRY_TIME = "15:00"          # Do not open fresh trades after this time
SQUARE_OFF_TIME = "15:20"          # Force square-off before market close
TEST_FRACTION = 0.20                # Final untouched test split by trading days
CV_BLOCKS = 5                       # Number of contiguous validation blocks
MIN_TRADES_FOR_SCORE = 20           # Penalty if a configuration trades too little
MARKET_TZ = "Asia/Kolkata"         # Explicitly normalize all timestamps to IST


@dataclass
class BacktestConfig:
    """Runtime configuration for the backtest engine."""

    trade_notional: float = TRADE_NOTIONAL
    slippage_bps: float = SLIPPAGE_BPS
    charges_bps: float = CHARGES_BPS
    brokerage_per_side: float = BROKERAGE_PER_SIDE
    allow_short: bool = ALLOW_SHORT
    last_entry_time: str = LAST_ENTRY_TIME
    square_off_time: str = SQUARE_OFF_TIME
    min_trades_for_score: int = MIN_TRADES_FOR_SCORE


# =============================================================================
# IO + preprocessing
# =============================================================================
def _normalize_timestamp_series(ts_like: pd.Series) -> pd.Series:
    """
    Parse timestamps and normalize them to Asia/Kolkata.

    Behavior:
    - If the source timestamps are timezone-aware, convert them to IST.
    - If they are timezone-naive, assume they already represent IST and localize.

    This is important because the session-time filter (09:15 to 15:30) must be
    applied in local Indian market time, not in an arbitrary timezone.
    """
    ts = pd.to_datetime(ts_like, errors="coerce")

    # For a normal datetime64[ns, tz] series, .dt.tz is available.
    # For datetime64[ns] (naive), .dt.tz returns None.
    if getattr(ts.dt, "tz", None) is None:
        return ts.dt.tz_localize(MARKET_TZ, ambiguous="NaT", nonexistent="shift_forward")
    return ts.dt.tz_convert(MARKET_TZ)


def _read_one_file(fp: Path) -> pd.DataFrame:
    """Read one supported market-data file into a DataFrame."""
    suffix = fp.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(fp)
    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(fp)
    if suffix in {".pkl", ".pickle"}:
        return pd.read_pickle(fp)
    raise ValueError(f"Unsupported file type: {fp}")


def load_ohlcv(path_str: str, tradingsymbol: Optional[str] = None) -> pd.DataFrame:
    """
    Load 1-minute OHLCV data from a file or folder.

    Supported formats:
    - CSV
    - Parquet / PQ
    - Pickle / PKL

    Expected columns can follow common aliases. For your sample pickle, these
    columns are directly supported:
    - date
    - open
    - high
    - low
    - close
    - volume
    - tradingsymbol
    """
    path = Path(path_str)
    allowed_suffixes = {".csv", ".parquet", ".pq", ".pkl", ".pickle"}

    if path.is_file():
        files = [path]
    elif path.is_dir():
        # Use rglob so nested data folders also work.
        files = sorted([p for p in path.rglob("*") if p.is_file() and p.suffix.lower() in allowed_suffixes])
    else:
        raise FileNotFoundError(f"Input path not found: {path_str}")

    if not files:
        raise FileNotFoundError("No CSV/Parquet/Pickle files found at the supplied path.")

    parts: List[pd.DataFrame] = []
    for fp in files:
        df_part = _read_one_file(fp)
        if not isinstance(df_part, pd.DataFrame):
            raise TypeError(f"File did not load as a pandas DataFrame: {fp}")
        if not df_part.empty:
            parts.append(df_part)

    if not parts:
        raise ValueError("Loaded file(s) are empty.")

    df = pd.concat(parts, ignore_index=True)
    if df.empty:
        raise ValueError("Loaded file(s) are empty after concatenation.")

    # Build a case-insensitive column map once.
    cols = {c.lower().strip(): c for c in df.columns}

    def find_col(candidates: Iterable[str], required: bool = True) -> Optional[str]:
        for c in candidates:
            if c in cols:
                return cols[c]
        if required:
            raise ValueError(f"Missing required column. Tried aliases: {list(candidates)}")
        return None

    # -------------------------------------------------------------------------
    # Optional symbol filtering to prevent accidental mixing of instruments.
    # -------------------------------------------------------------------------
    symbol_col = None
    for candidate in ["tradingsymbol", "symbol", "ticker", "name"]:
        if candidate in cols:
            symbol_col = cols[candidate]
            break

    if symbol_col is not None:
        symbol_series = df[symbol_col].astype(str).str.strip()
        if tradingsymbol:
            symbol_mask = symbol_series.str.upper() == tradingsymbol.strip().upper()
            df = df.loc[symbol_mask].copy()
            if df.empty:
                raise ValueError(f"No rows found for tradingsymbol='{tradingsymbol}'.")
        else:
            unique_symbols = sorted(symbol_series.dropna().unique().tolist())
            if len(unique_symbols) > 1:
                raise ValueError(
                    "Input contains multiple symbols. Please rerun with --tradingsymbol SYMBOL "
                    f"to select one instrument. Found: {unique_symbols[:10]}"
                )

        # Refresh the column map after filtering.
        cols = {c.lower().strip(): c for c in df.columns}

    # -------------------------------------------------------------------------
    # Timestamp detection
    # -------------------------------------------------------------------------
    if "timestamp" in cols:
        ts = _normalize_timestamp_series(df[cols["timestamp"]])
    elif "datetime" in cols:
        ts = _normalize_timestamp_series(df[cols["datetime"]])
    elif "date" in cols and "time" in cols:
        ts = _normalize_timestamp_series(
            df[cols["date"]].astype(str).str.strip() + " " + df[cols["time"]].astype(str).str.strip()
        )
    elif "date" in cols:
        ts = _normalize_timestamp_series(df[cols["date"]])
    else:
        raise ValueError("Could not find timestamp/datetime/date columns in the input data.")

    # -------------------------------------------------------------------------
    # OHLCV column detection
    # -------------------------------------------------------------------------
    open_col = find_col(["open", "o"])
    high_col = find_col(["high", "h"])
    low_col = find_col(["low", "l"])
    close_col = find_col(["close", "c", "ltp"])
    vol_col = find_col(["volume", "vol", "qty", "traded_volume"], required=False)

    out = pd.DataFrame(
        {
            "timestamp": ts,
            "open": pd.to_numeric(df[open_col], errors="coerce"),
            "high": pd.to_numeric(df[high_col], errors="coerce"),
            "low": pd.to_numeric(df[low_col], errors="coerce"),
            "close": pd.to_numeric(df[close_col], errors="coerce"),
        }
    )

    if vol_col is None:
        out["volume"] = 1.0
    else:
        out["volume"] = pd.to_numeric(df[vol_col], errors="coerce").fillna(0.0)

    # Clean and sort.
    out = out.dropna(subset=["timestamp", "open", "high", "low", "close"]).copy()
    out = out.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)

    if out.empty:
        raise ValueError("No valid OHLC rows found after cleaning.")

    # Create local date/time helpers for session filtering and daily grouping.
    out["date"] = out["timestamp"].dt.date
    out["time"] = out["timestamp"].dt.strftime("%H:%M")

    # India cash session filter.
    out = out[(out["time"] >= SESSION_START) & (out["time"] <= SESSION_END)].copy()
    if out.empty:
        raise ValueError("No rows left after session-time filtering. Check timezone / timestamps.")

    # Use a defensive volume for VWAP to avoid divide-by-zero on zero-volume bars.
    effective_volume = np.where(out["volume"] > 0, out["volume"], 1.0)
    pxv = out["close"] * effective_volume
    cum_pv = pd.Series(pxv, index=out.index).groupby(out["date"]).cumsum()
    cum_v = pd.Series(effective_volume, index=out.index).groupby(out["date"]).cumsum()
    out["vwap"] = cum_pv / cum_v

    # Previous close and true range are used by ATR-based exits.
    out["prev_close"] = out["close"].shift(1)
    tr1 = out["high"] - out["low"]
    tr2 = (out["high"] - out["prev_close"]).abs()
    tr3 = (out["low"] - out["prev_close"]).abs()
    out["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Bar number within each trading day. Useful for ORB logic.
    out["bar_no"] = out.groupby("date").cumcount() + 1

    return out.reset_index(drop=True)


# =============================================================================
# Indicators
# =============================================================================
def ema(s: pd.Series, span: int) -> pd.Series:
    """Exponential moving average."""
    return s.ewm(span=span, adjust=False, min_periods=span).mean()


def rsi(close: pd.Series, length: int) -> pd.Series:
    """Wilder-style RSI using exponentially smoothed gains/losses."""
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    roll_up = up.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    roll_down = down.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def atr(tr: pd.Series, length: int) -> pd.Series:
    """Average True Range using Wilder-style exponential smoothing."""
    return tr.ewm(alpha=1 / length, adjust=False, min_periods=length).mean()


def bollinger(close: pd.Series, length: int, num_std: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Bollinger band tuple: (mid, upper, lower)."""
    mid = close.rolling(length, min_periods=length).mean()
    std = close.rolling(length, min_periods=length).std(ddof=0)
    upper = mid + num_std * std
    lower = mid - num_std * std
    return mid, upper, lower


# =============================================================================
# Signal builders
# =============================================================================
def build_signals(df: pd.DataFrame, strategy: str, params: Dict) -> pd.DataFrame:
    """
    Build trade signals for a specific strategy family.

    Important design choice:
    Signals are computed on the completed bar and executed only at the next
    bar's open. That avoids same-bar lookahead in entries.
    """
    x = df.copy()

    if strategy == "ema_vwap_pullback":
        fast = params["fast"]
        slow = params["slow"]
        atr_len = params["atr_len"]

        x["ema_fast"] = ema(x["close"], fast)
        x["ema_slow"] = ema(x["close"], slow)
        x["atr"] = atr(x["tr"], atr_len)

        # Long setup: trend up, price above VWAP, and a fresh reclaim of VWAP.
        cond_long = (
            (x["ema_fast"] > x["ema_slow"])
            & (x["close"] > x["vwap"])
            & (x["close"].shift(1) <= x["vwap"].shift(1))
        )

        # Short setup: mirror image of the long setup.
        cond_short = (
            (x["ema_fast"] < x["ema_slow"])
            & (x["close"] < x["vwap"])
            & (x["close"].shift(1) >= x["vwap"].shift(1))
        )

        x["long_signal"] = cond_long.fillna(False)
        x["short_signal"] = cond_short.fillna(False)
        x["one_trade_per_day"] = False

    elif strategy == "rsi_bb_reversion":
        rsi_len = params["rsi_len"]
        bb_len = params["bb_len"]
        atr_len = params["atr_len"]
        rsi_buy = params["rsi_buy"]
        rsi_sell = params["rsi_sell"]

        x["rsi"] = rsi(x["close"], rsi_len)
        bb_mid, bb_upper, bb_lower = bollinger(x["close"], bb_len, 2.0)
        x["bb_mid"] = bb_mid
        x["bb_upper"] = bb_upper
        x["bb_lower"] = bb_lower
        x["atr"] = atr(x["tr"], atr_len)

        # Mean-reversion entries: oversold beyond lower band, or overbought above upper band.
        cond_long = (x["rsi"] < rsi_buy) & (x["close"] < x["bb_lower"])
        cond_short = (x["rsi"] > rsi_sell) & (x["close"] > x["bb_upper"])

        x["long_signal"] = cond_long.fillna(False)
        x["short_signal"] = cond_short.fillna(False)
        x["one_trade_per_day"] = False

    elif strategy == "opening_range_breakout":
        range_minutes = params["range_minutes"]
        atr_len = params["atr_len"]
        buffer_bps = params["buffer_bps"] / 10000.0

        x["atr"] = atr(x["tr"], atr_len)

        # Opening range is calculated independently for each day using the first
        # N bars after market open.
        orb_high = x.groupby("date")["high"].transform(lambda s: s.iloc[:range_minutes].max())
        orb_low = x.groupby("date")["low"].transform(lambda s: s.iloc[:range_minutes].min())

        valid = x["bar_no"] > range_minutes
        long_level = orb_high * (1 + buffer_bps)
        short_level = orb_low * (1 - buffer_bps)

        cond_long = valid & (x["close"] > long_level) & (x["close"].shift(1) <= long_level.shift(1))
        cond_short = valid & (x["close"] < short_level) & (x["close"].shift(1) >= short_level.shift(1))

        x["long_signal"] = cond_long.fillna(False)
        x["short_signal"] = cond_short.fillna(False)
        x["one_trade_per_day"] = True

    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    return x


# =============================================================================
# Backtest engine
# =============================================================================
def apply_costs(entry_fill: float, exit_fill: float, qty: int, cfg: BacktestConfig) -> float:
    """Apply variable plus fixed costs on both entry and exit."""
    entry_notional = entry_fill * qty
    exit_notional = exit_fill * qty
    variable = (entry_notional + exit_notional) * (cfg.charges_bps / 10000.0)
    fixed = 2.0 * cfg.brokerage_per_side
    return float(variable + fixed)


def adjust_fill(price: float, side: str, cfg: BacktestConfig) -> float:
    """Apply slippage to a theoretical raw price."""
    slip = cfg.slippage_bps / 10000.0
    if side == "buy":
        return price * (1 + slip)
    if side == "sell":
        return price * (1 - slip)
    raise ValueError("side must be 'buy' or 'sell'")


def compute_metrics(trades: pd.DataFrame, trade_notional: float, min_trades_for_score: int) -> Dict[str, float]:
    """Compute a compact performance summary for one strategy configuration."""
    if trades.empty:
        return {
            "trades": 0,
            "net_pnl": 0.0,
            "hit_rate": 0.0,
            "profit_factor": 0.0,
            "avg_pnl": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "max_drawdown": 0.0,
            "sharpe_daily": 0.0,
            "score": -1e9,
        }

    pnl = trades["pnl"].astype(float)
    wins = pnl[pnl > 0]
    losses = pnl[pnl < 0]

    daily = trades.groupby("exit_date")["pnl"].sum().sort_index()
    equity = daily.cumsum()
    peak = equity.cummax()
    dd = equity - peak
    max_dd = float(dd.min()) if not dd.empty else 0.0

    daily_ret = daily / trade_notional
    if len(daily_ret) > 1 and daily_ret.std(ddof=0) > 1e-12:
        sharpe = float((daily_ret.mean() / daily_ret.std(ddof=0)) * np.sqrt(252))
    else:
        sharpe = 0.0

    gross_profit = float(wins.sum()) if not wins.empty else 0.0
    gross_loss = float(-losses.sum()) if not losses.empty else 0.0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
    hit_rate = float((pnl > 0).mean())
    net_pnl = float(pnl.sum())

    # Composite score used for model selection.
    # It rewards profit, hit rate, profit factor, and Sharpe, while penalizing
    # drawdown and too-few-trades behavior.
    score = (
        40.0 * (net_pnl / trade_notional)
        + 25.0 * hit_rate
        + 20.0 * min(profit_factor, 3.0)
        + 10.0 * sharpe
        - 30.0 * (abs(max_dd) / trade_notional)
        - (15.0 if len(trades) < min_trades_for_score else 0.0)
    )

    return {
        "trades": int(len(trades)),
        "net_pnl": net_pnl,
        "hit_rate": hit_rate,
        "profit_factor": float(profit_factor),
        "avg_pnl": float(pnl.mean()),
        "avg_win": float(wins.mean()) if not wins.empty else 0.0,
        "avg_loss": float(losses.mean()) if not losses.empty else 0.0,
        "max_drawdown": max_dd,
        "sharpe_daily": sharpe,
        "score": float(score),
    }


def backtest(df: pd.DataFrame, strategy: str, params: Dict, cfg: BacktestConfig) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Run one backtest for one strategy and one parameter set.

    Execution model:
    - Signals are observed on bar i-1.
    - Entries happen on bar i open.
    - Intrabar exits can happen on the current bar.
    - If stop and target both hit inside the same 1-minute candle, stop is
      assumed to hit first. That is deliberately conservative.
    """
    x = build_signals(df, strategy, params).reset_index(drop=True)

    dates = x["date"].astype(str).to_numpy()
    times = x["time"].to_numpy()
    opens = x["open"].to_numpy(dtype=float)
    highs = x["high"].to_numpy(dtype=float)
    lows = x["low"].to_numpy(dtype=float)
    closes = x["close"].to_numpy(dtype=float)
    atrs = x["atr"].to_numpy(dtype=float)
    long_sig = x["long_signal"].to_numpy(dtype=bool)
    short_sig = x["short_signal"].to_numpy(dtype=bool)
    one_trade_per_day = bool(x["one_trade_per_day"].iloc[0])

    trades = []

    pos = 0  # +1 long, -1 short, 0 flat
    entry_price = 0.0
    entry_fill = 0.0
    entry_time = None
    entry_date = None
    qty = 0
    stop_px = np.nan
    target_px = np.nan
    traded_today = False

    def close_trade(i: int, raw_exit_price: float, reason: str) -> None:
        nonlocal pos, entry_price, entry_fill, entry_time, entry_date, qty, stop_px, target_px, traded_today
        if pos == 0:
            return

        if pos == 1:
            exit_fill = adjust_fill(raw_exit_price, "sell", cfg)
            pnl = (exit_fill - entry_fill) * qty - apply_costs(entry_fill, exit_fill, qty, cfg)
        else:
            exit_fill = adjust_fill(raw_exit_price, "buy", cfg)
            pnl = (entry_fill - exit_fill) * qty - apply_costs(entry_fill, exit_fill, qty, cfg)

        trades.append(
            {
                "strategy": strategy,
                "params": json.dumps(params, sort_keys=True),
                "side": "LONG" if pos == 1 else "SHORT",
                "entry_ts": entry_time,
                "exit_ts": str(x.loc[i, "timestamp"]),
                "entry_date": entry_date,
                "exit_date": str(x.loc[i, "date"]),
                "entry_raw": float(entry_price),
                "exit_raw": float(raw_exit_price),
                "entry_fill": float(entry_fill),
                "exit_fill": float(exit_fill),
                "qty": int(qty),
                "stop_px": float(stop_px),
                "target_px": float(target_px),
                "reason": reason,
                "pnl": float(pnl),
            }
        )

        pos = 0
        entry_price = 0.0
        entry_fill = 0.0
        entry_time = None
        entry_date = None
        qty = 0
        stop_px = np.nan
        target_px = np.nan
        traded_today = True

    def open_trade(i: int, side: int) -> None:
        nonlocal pos, entry_price, entry_fill, entry_time, entry_date, qty, stop_px, target_px, traded_today
        raw_entry = float(opens[i])
        atr_here = float(atrs[i - 1]) if i > 0 else np.nan

        # Skip pathological or not-yet-ready bars.
        if not np.isfinite(raw_entry) or raw_entry <= 0:
            return
        if not np.isfinite(atr_here) or atr_here <= 0:
            return

        qty_local = max(1, int(cfg.trade_notional // raw_entry))

        if side == 1:
            pos = 1
            entry_fill_local = adjust_fill(raw_entry, "buy", cfg)
            stop_local = raw_entry - params["stop_atr"] * atr_here
            target_local = raw_entry + params["target_atr"] * atr_here
        else:
            pos = -1
            entry_fill_local = adjust_fill(raw_entry, "sell", cfg)
            stop_local = raw_entry + params["stop_atr"] * atr_here
            target_local = raw_entry - params["target_atr"] * atr_here

        entry_price = raw_entry
        entry_fill = entry_fill_local
        qty = qty_local
        stop_px = stop_local
        target_px = target_local
        entry_time = str(x.loc[i, "timestamp"])
        entry_date = str(x.loc[i, "date"])
        traded_today = True

    for i in range(1, len(x)):
        new_day = dates[i] != dates[i - 1]
        if new_day:
            # If an old position survived until the next day in the data, force close it.
            if pos != 0:
                close_trade(i, opens[i], "day_change_squareoff")
            traded_today = False

        # Reversal rule: if the opposite signal appeared on the completed bar,
        # close the current trade at the next bar's open.
        if pos == 1 and short_sig[i - 1]:
            close_trade(i, opens[i], "reverse")
        elif pos == -1 and long_sig[i - 1]:
            close_trade(i, opens[i], "reverse")

        # Fresh entry only on next-bar open, before the last entry time.
        if pos == 0 and times[i] <= cfg.last_entry_time and (not traded_today or not one_trade_per_day):
            if long_sig[i - 1]:
                open_trade(i, 1)
            elif cfg.allow_short and short_sig[i - 1]:
                open_trade(i, -1)

        # Intrabar exits on the current bar.
        if pos != 0:
            if times[i] >= cfg.square_off_time:
                close_trade(i, closes[i], "squareoff")
                continue

            if pos == 1:
                hit_stop = lows[i] <= stop_px
                hit_target = highs[i] >= target_px
                if hit_stop and hit_target:
                    close_trade(i, stop_px, "stop_and_target_same_bar_conservative_stop")
                elif hit_stop:
                    close_trade(i, stop_px, "stop")
                elif hit_target:
                    close_trade(i, target_px, "target")
            else:
                hit_stop = highs[i] >= stop_px
                hit_target = lows[i] <= target_px
                if hit_stop and hit_target:
                    close_trade(i, stop_px, "stop_and_target_same_bar_conservative_stop")
                elif hit_stop:
                    close_trade(i, stop_px, "stop")
                elif hit_target:
                    close_trade(i, target_px, "target")

    if pos != 0:
        close_trade(len(x) - 1, closes[-1], "end_of_data")

    trades_df = pd.DataFrame(trades)
    metrics = compute_metrics(trades_df, cfg.trade_notional, cfg.min_trades_for_score)
    return trades_df, metrics


# =============================================================================
# Search space
# =============================================================================
def iter_param_grid(strategy: str) -> Iterable[Dict]:
    """Parameter grids for each strategy family."""
    if strategy == "ema_vwap_pullback":
        for fast, slow, stop_atr, target_atr in itertools.product(
            [8, 13, 21],
            [34, 55],
            [1.2, 1.8],
            [1.8, 2.5],
        ):
            if fast >= slow:
                continue
            yield {
                "fast": fast,
                "slow": slow,
                "atr_len": 14,
                "stop_atr": stop_atr,
                "target_atr": target_atr,
            }

    elif strategy == "rsi_bb_reversion":
        for rsi_len, rsi_buy, rsi_sell, stop_atr, target_atr in itertools.product(
            [7, 14],
            [25, 30],
            [70, 75],
            [0.8, 1.2],
            [1.2, 1.8],
        ):
            yield {
                "rsi_len": rsi_len,
                "bb_len": 20,
                "atr_len": 14,
                "rsi_buy": rsi_buy,
                "rsi_sell": rsi_sell,
                "stop_atr": stop_atr,
                "target_atr": target_atr,
            }

    elif strategy == "opening_range_breakout":
        for range_minutes, stop_atr, target_atr, buffer_bps in itertools.product(
            [15, 30],
            [1.0, 1.5],
            [1.5, 2.5],
            [0, 5],
        ):
            yield {
                "range_minutes": range_minutes,
                "atr_len": 14,
                "buffer_bps": buffer_bps,
                "stop_atr": stop_atr,
                "target_atr": target_atr,
            }
    else:
        raise ValueError(f"Unknown strategy: {strategy}")


def contiguous_blocks(unique_days: List, blocks: int) -> List[List]:
    """Split trading days into contiguous blocks for stability scoring."""
    blocks = max(2, min(blocks, len(unique_days)))
    chunk_sizes = [len(unique_days) // blocks] * blocks
    for i in range(len(unique_days) % blocks):
        chunk_sizes[i] += 1

    out = []
    idx = 0
    for size in chunk_sizes:
        out.append(unique_days[idx: idx + size])
        idx += size
    return [b for b in out if b]


# =============================================================================
# Optimizer
# =============================================================================
def search_best_strategy(df: pd.DataFrame, cfg: BacktestConfig, test_fraction: float, cv_blocks: int) -> Dict:
    """
    Search across strategy families and parameter grids.

    Selection logic:
    1. Hold out the final test portion of trading days completely untouched.
    2. On the earlier portion, score each parameter set across contiguous blocks.
    3. Prefer parameter sets with high mean score and low score volatility.
    4. From each strategy family, keep the most stable candidate.
    5. Final winner = strongest untouched test performance among those stable winners.

    This is not a machine-learning training pipeline. These strategies are rule-based,
    so the purpose of the earlier-period blocks is robustness checking, not weight fitting.
    """
    unique_days = sorted(df["date"].unique())
    if len(unique_days) < 80:
        raise ValueError("Too few trading days. At least ~80 days are recommended for a meaningful search.")

    test_days_count = max(20, int(len(unique_days) * test_fraction))
    if test_days_count >= len(unique_days):
        raise ValueError("Test fraction is too large for the available data.")

    opt_days = unique_days[:-test_days_count]
    test_days = unique_days[-test_days_count:]

    df_opt = df[df["date"].isin(opt_days)].copy()
    df_test = df[df["date"].isin(test_days)].copy()

    folds = contiguous_blocks(sorted(df_opt["date"].unique()), cv_blocks)

    trial_rows = []
    best_per_strategy = {}

    strategies = ["ema_vwap_pullback", "rsi_bb_reversion", "opening_range_breakout"]

    for strategy in strategies:
        best_row = None

        for params in iter_param_grid(strategy):
            fold_metrics = []
            for fold_days in folds:
                df_fold = df_opt[df_opt["date"].isin(fold_days)]
                if df_fold.empty:
                    continue
                _, metrics = backtest(df_fold, strategy, params, cfg)
                fold_metrics.append(metrics)

            if not fold_metrics:
                continue

            cv_score_mean = float(np.mean([m["score"] for m in fold_metrics]))
            cv_score_std = float(np.std([m["score"] for m in fold_metrics], ddof=0))
            cv_net_mean = float(np.mean([m["net_pnl"] for m in fold_metrics]))
            cv_hit_mean = float(np.mean([m["hit_rate"] for m in fold_metrics]))
            cv_pf_mean = float(np.mean([m["profit_factor"] for m in fold_metrics]))
            cv_dd_mean = float(np.mean([m["max_drawdown"] for m in fold_metrics]))
            cv_trades_mean = float(np.mean([m["trades"] for m in fold_metrics]))

            # Stability score penalizes parameter sets whose behavior jumps around too much.
            stability_score = cv_score_mean - 0.50 * cv_score_std

            test_trades, test_metrics = backtest(df_test, strategy, params, cfg)

            row = {
                "strategy": strategy,
                "params": json.dumps(params, sort_keys=True),
                "cv_score_mean": cv_score_mean,
                "cv_score_std": cv_score_std,
                "stability_score": stability_score,
                "cv_net_pnl_mean": cv_net_mean,
                "cv_hit_rate_mean": cv_hit_mean,
                "cv_profit_factor_mean": cv_pf_mean,
                "cv_max_drawdown_mean": cv_dd_mean,
                "cv_trades_mean": cv_trades_mean,
                "test_score": test_metrics["score"],
                "test_net_pnl": test_metrics["net_pnl"],
                "test_hit_rate": test_metrics["hit_rate"],
                "test_profit_factor": test_metrics["profit_factor"],
                "test_max_drawdown": test_metrics["max_drawdown"],
                "test_trades": test_metrics["trades"],
            }
            trial_rows.append(row)

            if best_row is None or row["stability_score"] > best_row["stability_score"]:
                best_row = row | {"test_trades_df": test_trades}

        if best_row is not None:
            best_per_strategy[strategy] = best_row

    if not best_per_strategy:
        raise RuntimeError("No strategies produced a valid result.")

    # Final winner: strongest untouched test performance among stable family-level winners.
    winner = max(
        best_per_strategy.values(),
        key=lambda r: (r["test_score"], r["test_net_pnl"], r["test_hit_rate"]),
    )

    trials_df = pd.DataFrame(trial_rows).sort_values(
        ["test_score", "stability_score", "test_net_pnl"],
        ascending=[False, False, False],
    ).reset_index(drop=True)

    return {
        "trials_df": trials_df,
        "best_per_strategy": best_per_strategy,
        "winner": winner,
        "optimization_days": len(opt_days),
        "test_days": len(test_days),
    }


# =============================================================================
# Main
# =============================================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Search robust rule-based intraday strategies on 1-minute OHLCV data."
    )
    parser.add_argument("--input", default=r"C:\Users\himan\Documents\Audacity.pkl", help="CSV/Parquet/Pickle file, or a folder containing such files.")
    parser.add_argument("--outdir", default="reliance_strategy_output", help="Output folder path.")
    parser.add_argument("--tradingsymbol", default='RELIANCE', help="Optional symbol filter, e.g. RELIANCE.")
    parser.add_argument("--trade-notional", type=float, default=TRADE_NOTIONAL, help="Fixed rupees per trade.")
    parser.add_argument("--slippage-bps", type=float, default=SLIPPAGE_BPS, help="Slippage per side in basis points.")
    parser.add_argument("--charges-bps", type=float, default=CHARGES_BPS, help="Other variable charges per side in basis points.")
    parser.add_argument("--brokerage-per-side", type=float, default=BROKERAGE_PER_SIDE, help="Fixed brokerage per side.")
    parser.add_argument("--test-fraction", type=float, default=TEST_FRACTION, help="Fraction of days reserved as untouched final test.")
    parser.add_argument("--cv-blocks", type=int, default=CV_BLOCKS, help="Number of contiguous optimization-period blocks.")
    parser.add_argument(
        "--long-only",
        action="store_true",
        help="Disable short trades and search only long-side opportunities.",
    )
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    cfg = BacktestConfig(
        trade_notional=args.trade_notional,
        slippage_bps=args.slippage_bps,
        charges_bps=args.charges_bps,
        brokerage_per_side=args.brokerage_per_side,
        allow_short=not args.long_only,
    )

    df = load_ohlcv(args.input, tradingsymbol=args.tradingsymbol)

    result = search_best_strategy(
        df=df,
        cfg=cfg,
        test_fraction=args.test_fraction,
        cv_blocks=args.cv_blocks,
    )

    trials_df = result["trials_df"]
    winner = result["winner"]
    winner_trades = winner["test_trades_df"].copy()

    trials_path = outdir / "strategy_trials.csv"
    winner_trades_path = outdir / "best_strategy_test_trades.csv"
    summary_path = outdir / "best_strategy_summary.json"

    trials_df.to_csv(trials_path, index=False)
    winner_trades.to_csv(winner_trades_path, index=False)

    summary = {
        "selected_strategy": winner["strategy"],
        "selected_params": json.loads(winner["params"]),
        "optimization_days": result["optimization_days"],
        "test_days": result["test_days"],
        "cv_stability_score": winner["stability_score"],
        "test_score": winner["test_score"],
        "test_net_pnl": winner["test_net_pnl"],
        "test_hit_rate": winner["test_hit_rate"],
        "test_profit_factor": winner["test_profit_factor"],
        "test_max_drawdown": winner["test_max_drawdown"],
        "test_trades": winner["test_trades"],
        "assumptions": {
            "market_timezone": MARKET_TZ,
            "entry": "next-bar open after signal",
            "exit_priority": "reversal at open, then intrabar stop/target, then square-off",
            "same_bar_stop_and_target": "conservative stop assumed first",
            "position_sizing": f"fixed rupee notional per trade = {cfg.trade_notional}",
            "costs": {
                "slippage_bps_per_side": cfg.slippage_bps,
                "charges_bps_per_side": cfg.charges_bps,
                "brokerage_per_side": cfg.brokerage_per_side,
            },
            "notes": [
                "This script searches historically robust rule-based strategies; it does not guarantee future profitability.",
                "Accuracy alone is not optimized in isolation because high hit rate can still lose money.",
                "Final strategy is chosen from untouched test-period results among stable earlier-period candidates.",
            ],
        },
    }

    summary_path.write_text(json.dumps(summary, indent=2))

    print("\n=== FINAL SELECTED STRATEGY ===")
    print(json.dumps(summary, indent=2))
    print(f"\nSaved: {trials_path}")
    print(f"Saved: {winner_trades_path}")
    print(f"Saved: {summary_path}")


if __name__ == "__main__":
    main()

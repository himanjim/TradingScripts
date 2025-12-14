import pandas as pd
import os
import re

# -----------------------------
# Config
# -----------------------------
STRADDLE_TIME_WINDOW_SEC = 5  # SELL CE + SELL PE must be within these seconds
EXPORT_FILENAME = "parsed_trades.xlsx"

# -----------------------------
# File locations (tries /mnt/data first, then Downloads)
# -----------------------------
def resolve_path(filename: str) -> str:
    mnt_path = os.path.join("/mnt/data", filename)
    if os.path.exists(mnt_path):
        return mnt_path
    downloads_path = os.path.join(os.environ.get("USERPROFILE", ""), "Downloads")
    return os.path.join(downloads_path, filename)

orders_csv = resolve_path("orders.csv")
positions_csv = resolve_path("positions (1).csv")

# -----------------------------
# Load data
# -----------------------------
df = pd.read_csv(orders_csv)
positions_df = pd.read_csv(positions_csv)

# Clean up any unnamed columns
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
positions_df = positions_df.loc[:, ~positions_df.columns.str.contains("^Unnamed")]

# Ensure required columns exist
required_orders_cols = {"Time", "Type", "Instrument", "Product", "Avg. price", "Status"}
missing = required_orders_cols - set(df.columns)
if missing:
    raise ValueError(f"Orders CSV missing columns: {missing}")

required_positions_cols = {"Instrument", "LTP"}
missing_pos = required_positions_cols - set(positions_df.columns)
if missing_pos:
    raise ValueError(f"Positions CSV missing columns: {missing_pos}")

# -----------------------------
# Preprocess
# -----------------------------
df["Time"] = pd.to_datetime(df["Time"])
df = df.sort_values("Time").reset_index(drop=True)

# Keep only completed trades
df = df[df["Status"] == "COMPLETE"].copy()

# Detect options vs stocks
# An option ends with ...<strike>(CE|PE). Example: NIFTY2582125100CE
df["OptionType"] = df["Instrument"].str.extract(r"(CE|PE)$")
df["Strike"] = df["Instrument"].str.extract(r"(\d{4,5})(?=(CE|PE)$)")[0]
df["IsOption"] = df["OptionType"].notna()

# Extract a "root" for options (everything before the strike+CE/PE)
# Helps ensure we match within SAME contract series (e.g., NIFTY25821 vs SENSEX25JUN)
df["Root"] = df["Instrument"].str.replace(r"(\d{4,5})(CE|PE)$", "", regex=True)

# LTP lookup (by FULL instrument to avoid strike collisions across underlyings)
ltp_lookup = positions_df.set_index("Instrument")["LTP"].to_dict()

# -----------------------------
# Helper functions
# -----------------------------
def add_straddle(call_leg, call_exit, put_leg, put_exit):
    """Builds a dict row for a short straddle (SELL CE + SELL PE)."""
    return {
        "TRADE ENTRY TIME": min(call_leg["Time"], put_leg["Time"]).time(),
        "TRADE EXIT TIME": max(call_exit["Time"], put_exit["Time"]).time(),
        "SYMBOL": call_leg["Root"],                 # contract root (e.g., NIFTY25821)
        "SEGMENT": "OPTIONS",
        "TRADE TYPE": "SHORT STRADDLE",
        # Call leg
        "SELL CALL SYMBOL": call_leg["Instrument"],
        "SELL CALL STRIKE": call_leg["Strike"],
        "SELL CALL ENTRY PRICE": call_leg["Avg. price"],
        "SELL CALL EXIT PRICE": call_exit["Avg. price"],
        "SELL CALL LTP (Positions)": ltp_lookup.get(call_leg["Instrument"]),
        # Put leg
        "SELL PUT SYMBOL": put_leg["Instrument"],
        "SELL PUT STRIKE": put_leg["Strike"],
        "SELL PUT ENTRY PRICE": put_leg["Avg. price"],
        "SELL PUT EXIT PRICE": put_exit["Avg. price"],
        "SELL PUT LTP (Positions)": ltp_lookup.get(put_leg["Instrument"]),
        # For consistency with older sheet naming
        "SELL CALL EXPIRY PRICE": ltp_lookup.get(call_leg["Instrument"]),
        "SELL PUT EXPIRY PRICE": ltp_lookup.get(put_leg["Instrument"]),
        # Generic fields (unused for straddles but kept for schema stability)
        "ENTRY PRICE": None,
        "EXIT PRICE": None,
        "STOCK SYMBOL": None,
        "STOCK ENTRY PRICE": None,
        "STOCK EXIT PRICE": None,
    }

def add_option_directional(entry, exit_):
    """Builds a dict row for an options directional trade (single CE or PE leg)."""
    trade_side = "BUY" if entry["Type"] == "BUY" else "SELL"
    # Normalize entry/exit price fields so they always mean sell-entry/buy-exit for SELL trades,
    # and buy-entry/sell-exit for BUY trades
    entry_price = entry["Avg. price"]
    exit_price = exit_["Avg. price"]

    # For your previous sheet layout:
    ce_cols = {"SELL CALL STRIKE": None, "SELL CALL ENTRY PRICE": None, "SELL CALL EXIT PRICE": None,
               "SELL CALL SYMBOL": None, "SELL CALL LTP (Positions)": None, "SELL CALL EXPIRY PRICE": None}
    pe_cols = {"SELL PUT STRIKE": None, "SELL PUT ENTRY PRICE": None, "SELL PUT EXIT PRICE": None,
               "SELL PUT SYMBOL": None, "SELL PUT LTP (Positions)": None, "SELL PUT EXPIRY PRICE": None}

    if entry["OptionType"] == "CE":
        ce_cols.update({
            "SELL CALL STRIKE": entry["Strike"],
            "SELL CALL ENTRY PRICE": entry_price if trade_side == "SELL" else exit_price,
            "SELL CALL EXIT PRICE": exit_price if trade_side == "SELL" else entry_price,
            "SELL CALL SYMBOL": entry["Instrument"],
            "SELL CALL LTP (Positions)": ltp_lookup.get(entry["Instrument"]),
            "SELL CALL EXPIRY PRICE": ltp_lookup.get(entry["Instrument"]),
        })
    else:
        pe_cols.update({
            "SELL PUT STRIKE": entry["Strike"],
            "SELL PUT ENTRY PRICE": entry_price if trade_side == "SELL" else exit_price,
            "SELL PUT EXIT PRICE": exit_price if trade_side == "SELL" else entry_price,
            "SELL PUT SYMBOL": entry["Instrument"],
            "SELL PUT LTP (Positions)": ltp_lookup.get(entry["Instrument"]),
            "SELL PUT EXPIRY PRICE": ltp_lookup.get(entry["Instrument"]),
        })

    return {
        "TRADE ENTRY TIME": entry["Time"].time(),
        "TRADE EXIT TIME": exit_["Time"].time(),
        "SYMBOL": entry["Root"],
        "SEGMENT": "OPTIONS",
        "TRADE TYPE": trade_side,
        **ce_cols,
        **pe_cols,
        # Generic
        "ENTRY PRICE": None,
        "EXIT PRICE": None,
        "STOCK SYMBOL": None,
        "STOCK ENTRY PRICE": None,
        "STOCK EXIT PRICE": None,
    }

def add_stock_directional(entry, exit_):
    """Builds a dict row for an equity (cash) directional trade."""
    trade_side = "BUY" if entry["Type"] == "BUY" else "SELL"
    entry_price = entry["Avg. price"]
    exit_price = exit_["Avg. price"]
    symbol = entry["Instrument"]

    return {
        "TRADE ENTRY TIME": entry["Time"].time(),
        "TRADE EXIT TIME": exit_["Time"].time(),
        "SYMBOL": symbol,
        "SEGMENT": "EQUITY",
        "TRADE TYPE": trade_side,
        # Options fields (unused for equity)
        "SELL CALL SYMBOL": None,
        "SELL CALL STRIKE": None,
        "SELL CALL ENTRY PRICE": None,
        "SELL CALL EXIT PRICE": None,
        "SELL CALL LTP (Positions)": None,
        "SELL CALL EXPIRY PRICE": None,
        "SELL PUT SYMBOL": None,
        "SELL PUT STRIKE": None,
        "SELL PUT ENTRY PRICE": None,
        "SELL PUT EXIT PRICE": None,
        "SELL PUT LTP (Positions)": None,
        "SELL PUT EXPIRY PRICE": None,
        # Generic / equity-specific
        "ENTRY PRICE": entry_price,
        "EXIT PRICE": exit_price,
        "STOCK SYMBOL": symbol,
        "STOCK ENTRY PRICE": entry_price if trade_side == "BUY" else exit_price,
        "STOCK EXIT PRICE": exit_price if trade_side == "BUY" else entry_price,
    }

# -----------------------------
# 1) Find SHORT STRADDLES (options only)
# -----------------------------
straddles = []
used_indices = set()

opts = df[df["IsOption"]].copy()
# Iterate SELL rows and look for the matching opposite option type SELL within time window at same Root+Strike
for i, row1 in opts.iterrows():
    if i in used_indices or row1["Type"] != "SELL":
        continue
    # candidate opposite leg: same Root+Strike, other OptionType, SELL, within STRADDLE_TIME_WINDOW_SEC
    mask = (
        (opts.index != i) &
        (~opts.index.isin(used_indices)) &
        (opts["Type"] == "SELL") &
        (opts["Root"] == row1["Root"]) &
        (opts["Strike"] == row1["Strike"]) &
        (opts["OptionType"] != row1["OptionType"]) &
        (opts["Time"].sub(row1["Time"]).abs().dt.total_seconds() <= STRADDLE_TIME_WINDOW_SEC)
    )
    candidates = opts[mask].sort_values("Time")
    if candidates.empty:
        continue
    row2 = candidates.iloc[0]

    # Find the first BUY exits for each leg occurring AFTER their entries
    buy_leg1 = opts[
        (opts["Type"] == "BUY") &
        (opts["Instrument"] == row1["Instrument"]) &
        (opts["Time"] > row1["Time"])
    ].sort_values("Time")

    buy_leg2 = opts[
        (opts["Type"] == "BUY") &
        (opts["Instrument"] == row2["Instrument"]) &
        (opts["Time"] > row2["Time"])
    ].sort_values("Time")

    if buy_leg1.empty or buy_leg2.empty:
        continue

    buy1 = buy_leg1.iloc[0]
    buy2 = buy_leg2.iloc[0]

    # Assign CE/PE legs properly
    call_leg = row1 if row1["OptionType"] == "CE" else row2
    call_exit = buy1 if call_leg["Instrument"] == buy1["Instrument"] else buy2
    put_leg = row2 if row2["OptionType"] == "PE" else row1
    put_exit = buy2 if put_leg["Instrument"] == buy2["Instrument"] else buy1

    straddles.append(add_straddle(call_leg, call_exit, put_leg, put_exit))
    used_indices.update({i, row2.name, buy1.name, buy2.name})

# -----------------------------
# 2) Directional trades (remaining options + equities)
# -----------------------------
df_remaining = df.drop(index=used_indices, errors="ignore").copy()

directional = []
used_dir = set()

# We'll pair trades within each key:
# - For options: key = full Instrument (safer than Strike+Type)
# - For equity: key = Instrument as well
for inst, g in df_remaining.groupby("Instrument", sort=False):
    g = g.sort_values("Time")
    # Greedy pairing: pick earliest trade, match with next opposite side
    used_local = set()
    rows = list(g.itertuples())
    for a_idx, a in enumerate(rows):
        if a.Index in used_dir or a.Index in used_local:
            continue
        # find opposite side after 'a'
        for b_idx in range(a_idx + 1, len(rows)):
            b = rows[b_idx]
            if b.Index in used_dir or b.Index in used_local:
                continue
            if a.Type != b.Type:  # opposite transaction
                entry_row = df_remaining.loc[a.Index] if a.Time <= b.Time else df_remaining.loc[b.Index]
                exit_row  = df_remaining.loc[b.Index] if a.Time <= b.Time else df_remaining.loc[a.Index]
                if entry_row["IsOption"]:
                    directional.append(add_option_directional(entry_row, exit_row))
                else:
                    directional.append(add_stock_directional(entry_row, exit_row))
                used_dir.update({a.Index, b.Index})
                used_local.update({a.Index, b.Index})
                break  # move to next 'a'

# -----------------------------
# Combine and export
# -----------------------------
final_df = pd.DataFrame(straddles + directional)
if not final_df.empty:
    final_df = final_df.sort_values(by=["TRADE ENTRY TIME", "SYMBOL"]).reset_index(drop=True)

# Add a couple of convenience columns (optional)
# Example: simple P&L for equity rows if you want (commented out by default)
# def pnl_row(r):
#     if r["SEGMENT"] == "EQUITY" and pd.notna(r["ENTRY PRICE"]) and pd.notna(r["EXIT PRICE"]):
#         mult = 1 if r["TRADE TYPE"] == "BUY" else -1
#         return (r["EXIT PRICE"] - r["ENTRY PRICE"]) * mult
#     return None
# final_df["P&L (Equity Only)"] = final_df.apply(pnl_row, axis=1)

# Where to write
out_downloads = os.path.join(os.environ.get("USERPROFILE", ""), "Downloads", EXPORT_FILENAME)
out_mnt = os.path.join("/mnt/data", EXPORT_FILENAME)

# Try writing to Downloads; fallback to /mnt/data
try:
    final_df.to_excel(out_downloads, index=False)
    print(f"✅ Trades written to: {out_downloads}")
except Exception as e:
    final_df.to_excel(out_mnt, index=False)
    print(f"✅ Trades written to: {out_mnt} (Downloads not writable)")

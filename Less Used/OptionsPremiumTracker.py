# OptionsPremiumTracker_pycharm_fix.py
# Windows + PyCharm friendly: forces GUI backend, shows chart immediately, animates smoothly.

import os
import time
import threading
import traceback
import datetime as dt
from pandas.api.types import DatetimeTZDtype

# --- force GUI backend so PyCharm shows a real window (not SciView) ---
import matplotlib
try:
    matplotlib.use("TkAgg")  # works with stock Python on Windows
except Exception:
    # fallback if Tk not present but PyQt is installed
    matplotlib.use("QtAgg")

import pytz
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.animation import FuncAnimation

import winsound  # Windows only
import OptionTradeUtils as oUtils  # your utility module

# ------------- Config -------------
INDIA_TZ = pytz.timezone("Asia/Calcutta")
REFRESH_MS = 1000                 # chart refresh (ms)
POLL_SEC = 2                      # quote poll sleep (s)
BEEP_INTERVAL_MIN = 10            # periodic beep after 09:15 IST
MAX_POINTS = 10_000               # cap points in memory/plot
DATA_SUBDIR = "PremiumsChartsData"

# ------------- File setup -------------
today_str = dt.datetime.now(INDIA_TZ).strftime("%Y-%m-%d")
downloads_path = os.path.join(os.environ["USERPROFILE"], "Downloads")
data_dir = os.path.join(downloads_path, DATA_SUBDIR)
os.makedirs(data_dir, exist_ok=True)
DATA_FILE = os.path.join(data_dir, f"premium_data_{today_str}.csv")

_TZ_RE = r"(Z|[+-]\d{2}:?\d{2})$"  # matches Z, +0530, +05:30

def parse_ts_to_ist(s: pd.Series) -> pd.Series:
    """
    Make any timestamp Series (strings/naive/aware/mixed) consistently tz-aware IST.
    - If row has tz suffix -> parse as UTC then convert to IST
    - Else -> parse as naive and localize as IST
    """
    # Already tz-aware dtype
    if isinstance(getattr(s, "dtype", None), DatetimeTZDtype):
        return s.dt.tz_convert(INDIA_TZ)

    # Work in string space for mixed/object columns
    s_str = s.astype("string")
    has_tz = s_str.str.contains(_TZ_RE, regex=True, na=False)

    aware = pd.to_datetime(s_str.where(has_tz), errors="coerce", utc=True).dt.tz_convert(INDIA_TZ)

    naive = pd.to_datetime(s_str.where(~has_tz), errors="coerce")
    naive = naive.dt.tz_localize(INDIA_TZ, nonexistent="shift_forward", ambiguous="NaT")

    return aware.fillna(naive)


# ------------- DataFrame init -------------
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE)
    df["timestamp"] = parse_ts_to_ist(df["timestamp"])
else:
    df = pd.DataFrame(columns=["timestamp", "value"])


lock = threading.Lock()

# ------------- Status overlay (thread-safe) -------------
_status_msg = "Starting…"
_status_lock = threading.Lock()

def set_status(msg: str):
    global _status_msg
    with _status_lock:
        _status_msg = msg

def get_status() -> str:
    with _status_lock:
        return _status_msg

def ist_now() -> dt.datetime:
    return dt.datetime.now(INDIA_TZ)

# ------------- Matplotlib setup -------------
fig, ax = plt.subplots(figsize=(10, 6))
(line,) = ax.plot([], [], lw=2)
ax.set_title("Options Premium (CE+PE) — Live")
ax.set_xlabel("Time (IST)")
ax.set_ylabel("Premium × Lots (₹)")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S", tz=INDIA_TZ))
ax.xaxis.set_major_locator(mdates.AutoDateLocator())

fig.autofmt_xdate()

# overlay shown when no data yet / pre-market, etc.
status_artist = ax.text(
    0.5, 0.5, "", transform=ax.transAxes, ha="center", va="center", fontsize=12, alpha=0.8
)

def add_data(ts: dt.datetime, val: float):
    """Thread-safe append to df and persist to CSV. Caps in-memory rows."""
    global df
    ts = pd.Timestamp(ts)
    if ts.tz is None:
        ts = ts.tz_localize(INDIA_TZ)
    else:
        ts = ts.tz_convert(INDIA_TZ)

    with lock:
        new_row = pd.DataFrame({"timestamp": [ts], "value": [val]})
        df = pd.concat([df, new_row], ignore_index=True)
        if len(df) > MAX_POINTS:
            df = df.iloc[-MAX_POINTS:].copy()

        write_header = not os.path.exists(DATA_FILE)
        row_csv = new_row.copy()
        row_csv["timestamp"] = row_csv["timestamp"].dt.tz_convert(INDIA_TZ).dt.strftime("%Y-%m-%d %H:%M:%S%z")
        row_csv.to_csv(DATA_FILE, mode="a", header=write_header, index=False)

def round_to_multiple(x: float, multiple: int) -> int:
    return int(round(x / multiple) * multiple)

def init_anim():
    line.set_data([], [])
    status_artist.set_text(get_status())
    status_artist.set_visible(True)
    return (line, status_artist)

def animate(_frame):
    with lock:
        local = df.copy()

    if local.empty:
        status_artist.set_text(get_status())
        status_artist.set_visible(True)
        # keep axes reasonable even when empty
        ax.relim()
        ax.autoscale_view()
        return (line, status_artist)

    x = parse_ts_to_ist(local["timestamp"])

    y = pd.to_numeric(local["value"], errors="coerce")

    line.set_data(x, y)
    ax.relim()
    ax.autoscale_view()

    status_artist.set_visible(False)
    return (line, status_artist)

# ------------- Fetch loop -------------
def fetch_data_loop():
    try:
        set_status("Initializing…")
        # periodic beep schedule (every 10 minutes after 09:15 IST)
        base = ist_now().replace(hour=9, minute=15, second=0, microsecond=0)
        if ist_now() > base:
            delta_min = int((ist_now() - base).total_seconds() // 60)
            next_min_mod = ((delta_min // BEEP_INTERVAL_MIN) + 1) * BEEP_INTERVAL_MIN
            next_beep = base + dt.timedelta(minutes=next_min_mod)
        else:
            next_beep = base

        original_prem = None
        highest_prem = None

        while True:
            now = ist_now()

            # Show pre-market status but DO NOT block the UI
            if now.time() < oUtils.MARKET_START_TIME:
                set_status(f"Waiting for market open at {oUtils.MARKET_START_TIME.strftime('%H:%M')} IST…")
                time.sleep(0.5)
                continue

            # Market end: stop fetch but leave figure open (user can close)
            if now.time() > oUtils.MARKET_END_TIME:
                set_status("Market closed. No new data.")
                time.sleep(1.0)
                continue

            # periodic beep
            if now >= next_beep:
                try:
                    winsound.Beep(1000, 800)  # 0.8s
                except Exception:
                    pass
                next_beep += dt.timedelta(minutes=BEEP_INTERVAL_MIN)

            # --- Quotes ---
            try:
                ul_live_quote = kite.quote(under_lying_symbol)
                ul_ltp = float(ul_live_quote[under_lying_symbol]["last_price"])
                ul_ltp_round = round_to_multiple(ul_ltp, STRIKE_MULTIPLE)

                option_pe = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + "PE"
                option_ce = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + "CE"
                option_quotes = kite.quote([option_pe, option_ce])
            except Exception as e:
                set_status(f"Quote fetch failed. Retrying… ({e})")
                time.sleep(2)
                continue

            try:
                ce_ltp = float(option_quotes[option_ce]["last_price"])
                pe_ltp = float(option_quotes[option_pe]["last_price"])
            except KeyError:
                set_status("CE/PE quote missing (symbol rolled?). Retrying…")
                time.sleep(POLL_SEC)
                continue

            option_premium_value = (ce_ltp + pe_ltp) * NO_OF_LOTS

            if highest_prem is None or option_premium_value > highest_prem:
                highest_prem = option_premium_value
            if original_prem is None:
                original_prem = option_premium_value

            print(
                f"Strike:{ul_ltp_round} | PREM:{option_premium_value:.2f} "
                f"(CE:{ce_ltp:.2f} PE:{pe_ltp:.2f}) | "
                f"orig:{original_prem:.2f} | high:{highest_prem:.2f} | {now.strftime('%H:%M:%S %Z')}"
            )

            set_status("")  # hide overlay once data is flowing
            add_data(now, option_premium_value)
            time.sleep(POLL_SEC)

    except Exception as e:
        print(f"[ERROR] Data fetch thread crashed: {e}")
        traceback.print_exc()
        set_status(f"Fetcher crashed: {e}")

# ------------- Main -------------
if __name__ == "__main__":
    # Init Kite
    kite = oUtils.intialize_kite_api()

    # Load instrument config
    (
        UNDER_LYING_EXCHANGE,
        UNDERLYING,
        OPTIONS_EXCHANGE,
        PART_SYMBOL,
        NO_OF_LOTS,
        STRIKE_MULTIPLE,
        STOPLOSS_POINTS,
        MIN_LOTS,
        LONG_STRADDLE_STRIKE_DISTANCE
    ) = oUtils.get_instruments(kite)

    under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING  # keep your original concat

    # Start fetcher immediately (UI shows status pre-market instead of blocking)
    t = threading.Thread(target=fetch_data_loop, daemon=True)
    t.start()

    # Start animation (suppress cache warning)
    ani = FuncAnimation(
        fig,
        animate,
        init_func=init_anim,
        interval=REFRESH_MS,
        blit=False,
        cache_frame_data=False,
        save_count=MAX_POINTS,
    )

    # Show chart (real GUI window via TkAgg/QtAgg)
    plt.show()

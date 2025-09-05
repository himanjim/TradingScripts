import time
from datetime import datetime, timedelta, time as dtime
import pytz
import pandas as pd

# Sound (Windows winsound if available; otherwise console bell fallback)
try:
    import winsound
    def ding():
        for _ in range(2):
            winsound.Beep(880, 180)  # frequency, ms
            time.sleep(0.08)
except Exception:
    def ding():
        for _ in range(2):
            print("\a", end="", flush=True)
            time.sleep(0.08)

# --- Zerodha Kite / your helpers ---
import OptionTradeUtils as oUtils
from kiteconnect import KiteConnect


IST = pytz.timezone("Asia/Kolkata")


def today_ist(dt_obj=None):
    now = dt_obj or datetime.now(IST)
    return now.astimezone(IST).date()


def ist_datetime(y, m, d, hh, mm, ss=0):
    return IST.localize(datetime(y, m, d, hh, mm, ss))


def market_window_for_today():
    """Return (from_dt, to_dt) covering today's market window 09:15 to now."""
    now = datetime.now(IST)
    start = datetime.combine(now.date(), dtime(9, 15, 0))
    start = IST.localize(start)
    return start, now


def find_instrument_token(kite: KiteConnect, exchange: str, tradingsymbol: str) -> int:
    """
    Look up instrument_token by exchange and tradingsymbol.
    Works for indices like 'NIFTY 50', 'NIFTY BANK', 'SENSEX'.
    """
    # Primary try: search within the given exchange
    try:
        dump = kite.instruments(exchange)
    except Exception:
        # Fallback to full dump if exchange-specific fails
        dump = kite.instruments()

    candidates = [row for row in dump if str(row.get("exchange")) == exchange and
                  (row.get("tradingsymbol") == tradingsymbol or row.get("name") == tradingsymbol)]
    if not candidates:
        # Wider search as ultimate fallback
        candidates = [row for row in dump if
                      (row.get("tradingsymbol") == tradingsymbol or row.get("name") == tradingsymbol)]
    if not candidates:
        raise RuntimeError(f"Instrument not found: {exchange}:{tradingsymbol}")

    # Prefer index/INDICES if present
    candidates.sort(key=lambda r: (r.get("segment") != "INDICES", r.get("instrument_type") != "INDEX"))
    return int(candidates[0]["instrument_token"])


def fetch_1min(kite: KiteConnect, token: int, from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
    """Fetch 1-minute historical candles. Returns DataFrame indexed by IST datetime."""
    tries = 0
    while True:
        tries += 1
        try:
            data = kite.historical_data(token, from_dt, to_dt, interval="minute", oi=False)
            df = pd.DataFrame(data)
            if df.empty:
                return df
            # Ensure timezone-aware index in IST
            df["date"] = pd.to_datetime(df["date"])
            if df["date"].dt.tz is None:
                df["date"] = df["date"].dt.tz_localize(IST)
            else:
                df["date"] = df["date"].dt.tz_convert(IST)
            df = df.set_index("date").sort_index()
            return df
        except Exception as e:
            if tries >= 3:
                raise
            # Mild backoff on transient rate limits
            time.sleep(1.2)


def add_emas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for p in (5, 8, 13):
        df[f"EMA{p}"] = df["close"].ewm(span=p, adjust=False).mean()
    return df


def crossover_signal(prev_row, curr_row, a, b):
    """
    Detect crossover between two series a (fast) and b (slow).
    Returns 'bullish' | 'bearish' | None
    """
    prev_above = prev_row[a] > prev_row[b]
    curr_above = curr_row[a] > curr_row[b]
    if prev_above != curr_above:
        return "bullish" if curr_above else "bearish"
    return None


def all_three_sloping_up(df: pd.DataFrame) -> bool:
    """True if EMA5, EMA8, EMA13 all increased on the latest closed bar."""
    if len(df) < 2:
        return False
    last = df.iloc[-1]
    prev = df.iloc[-2]
    return (last["EMA5"] > prev["EMA5"]) and (last["EMA8"] > prev["EMA8"]) and (last["EMA13"] > prev["EMA13"])


def monitor():
    # 1) Init Kite via your OptionTradeUtils credentials
    kite = oUtils.intialize_kite_api()

    # 2) Get chosen instrument from your OptionTradeUtils
    UNDER_LYING_EXCHANGE, UNDERLYING, *_ = oUtils.get_instruments(kite)
    tradingsymbol = UNDERLYING.lstrip(":")  # e.g., ':NIFTY BANK' -> 'NIFTY BANK'
    token = find_instrument_token(kite, UNDER_LYING_EXCHANGE, tradingsymbol)

    print(f"Monitoring {UNDER_LYING_EXCHANGE}:{tradingsymbol} (token={token})  –  1-min EMA(5/8/13) crossovers & slope-up alerts.")
    print("Two beeps = new signal. Press Ctrl+C to stop.\n")

    last_seen_bar = None
    last_alerted_bar_time = None

    while True:
        try:
            from_dt, to_dt = market_window_for_today()
            df = fetch_1min(kite, token, from_dt, to_dt)
            if df.empty:
                time.sleep(5)
                continue

            df = add_emas(df).dropna()
            if df.empty:
                time.sleep(5)
                continue

            # Work on the latest fully-formed bar
            curr_time = df.index[-1]
            if last_seen_bar is None:
                last_seen_bar = curr_time

            # Only evaluate once per new bar
            if curr_time != last_seen_bar:
                prev_row = df.iloc[-2]
                curr_row = df.iloc[-1]

                events = []

                sig_58 = crossover_signal(prev_row, curr_row, "EMA5", "EMA8")
                if sig_58:
                    events.append(f"5–8 EMA {sig_58} crossover")

                sig_813 = crossover_signal(prev_row, curr_row, "EMA8", "EMA13")
                if sig_813:
                    events.append(f"8–13 EMA {sig_813} crossover")

                sig_513 = crossover_signal(prev_row, curr_row, "EMA5", "EMA13")
                if sig_513:
                    events.append(f"5–13 EMA {sig_513} crossover")

                if all_three_sloping_up(df):
                    events.append("All three EMAs sloping up")

                if events and last_alerted_bar_time != curr_time:
                    print(f"[{curr_time.strftime('%Y-%m-%d %H:%M')}] " + " | ".join(events))
                    ding()  # two beeps
                    last_alerted_bar_time = curr_time

                last_seen_bar = curr_time

            # Sleep until next refresh (15s keeps API load light but responsive)
            time.sleep(15)

        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(2)


if __name__ == "__main__":
    monitor()

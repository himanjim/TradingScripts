import time as tm
import pytz
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from datetime import datetime
from matplotlib.animation import FuncAnimation
import threading
import time
import os
import OptionTradeUtils as oUtils
import winsound  # Use only on Windows
import datetime as dt
from scipy.stats import zscore
import traceback

# --- Time and file setup ---
indian_timezone = pytz.timezone('Asia/Calcutta')
today_str = datetime.now(indian_timezone).strftime('%Y-%m-%d')

downloads_path = os.path.join(os.environ["USERPROFILE"], "Downloads")
DIRECTORY = downloads_path

DATA_FILE = DIRECTORY + f"/PremiumsChartsData/premium_data_{today_str}.csv"

# --- Load persisted data if available ---
if os.path.exists(DATA_FILE):
    df = pd.read_csv(DATA_FILE, parse_dates=['timestamp'])
else:
    df = pd.DataFrame(columns=['timestamp', 'value'])

# --- Setup Plot ---
fig, ax = plt.subplots(figsize=(10, 6))
line, = ax.plot([], [], lw=2)
ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
plt.title('Live Data Chart (Smooth & Realtime)')
plt.xlabel('Time')
plt.ylabel('Value')
lock = threading.Lock()

# --- Function to add new data point ---
def add_data(new_time, new_value):
    global df
    with lock:
        new_row = pd.DataFrame({'timestamp': [new_time], 'value': [new_value]})
        df = pd.concat([df, new_row], ignore_index=True)
        new_row.to_csv(DATA_FILE, mode='a', header=not os.path.exists(DATA_FILE), index=False)


# --- Animation Update Function ---
arrow_annotations = []
last_alert_time = None
alert_cooldown_sec = 120  # avoid repeated alerts within 2 minutes

def animate(frame):
    global last_alert_time
    with lock:
        if len(df) < 20:
            return

        line.set_data(df['timestamp'], df['value'])
        ax.relim()
        ax.autoscale_view()
        fig.autofmt_xdate()

        # Clear old arrows
        for ann in arrow_annotations:
            ann.remove()
        arrow_annotations.clear()

        # Detect unusual premium rise
        recent_df = df.iloc[-20:]
        avg_recent = recent_df['value'].iloc[:-1].mean()
        current_value = recent_df['value'].iloc[-1]
        current_time = recent_df['timestamp'].iloc[-1]

        # # Only alert if rise is > threshold (e.g., ₹3000 above average)
        # if current_value > avg_recent + 5000:
        #     if not last_alert_time or (current_time - last_alert_time).total_seconds() > alert_cooldown_sec:
        #         print(f"🚨 Premium rise alert at {current_time} → Value: {current_value:.2f}, Avg: {avg_recent:.2f}")
        #
        #         # 3 beeps
        #         threading.Thread(target=lambda: [winsound.Beep(2000, 300) for _ in range(3)]).start()
        #
        #         # Red upward arrow
        #         ann = ax.annotate(
        #             '⬆',
        #             xy=(current_time, current_value),
        #             xytext=(0, 30),
        #             textcoords='offset points',
        #             arrowprops=dict(facecolor='red', arrowstyle='->'),
        #             ha='center', color='red', fontsize=12
        #         )
        #         arrow_annotations.append(ann)
        #
        #         last_alert_time = current_time


# --- Data fetching loop ---
def fetch_data_loop():
    try:
        next_beep = None
        original_options_premium_value = None
        highest_options_premium_value = None
        last_beep_index_for_5k_prem = 0
        trigger_value = None
        beep_count = 0  # Add this at the beginning of fetch_data_loop() function

        while True:
            now = datetime.now()

            if datetime.now(indian_timezone).time() > oUtils.MARKET_END_TIME:
                print(f"Market is closed. Hence exiting.")
                exit(0)

            if next_beep is None:
                base = dt.datetime.combine(dt.date.today(), dt.time(9, 15))
                next_beep = base + dt.timedelta(minutes=((dt.datetime.now() - base).seconds // 600 + 1) * 10)
            if dt.datetime.now() >= next_beep:
                winsound.Beep(1000, 2000)  # 3 seconds
                next_beep += dt.timedelta(minutes=10)

            try:
                ul_live_quote = kite.quote(under_lying_symbol)
                ul_ltp = ul_live_quote[under_lying_symbol]['last_price']
                ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE
                option_pe = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + 'PE'
                option_ce = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + 'CE'
                option_quotes = kite.quote([option_pe, option_ce])
            except Exception as e:
                print(f"An error occurred: {e}")
                tm.sleep(2)
                continue

            # Replace the trigger check with:
            if trigger_value and ul_ltp > trigger_value and beep_count < 3:
                winsound.Beep(2000, 1500)
                beep_count += 1

            option_premium_value = 0
            for trading_symbol, live_quote in option_quotes.items():
                option_premium_value += (live_quote['last_price'] * NO_OF_LOTS)

            # # Detect statistical jump using z-score
            # if len(df) >= 10:
            #     recent_diffs = df['value'].diff().fillna(0)
            #     recent_z = zscore(recent_diffs)
            #     if recent_z[-1] > 3:  # Use array-style access, not .iloc
            #         print("Z-score spike detected — significant premium jump!")
            #         winsound.Beep(2500, 2000)

            # # Slice df from last beep index to current end
            # df_slice = df.iloc[last_beep_index_for_5k_prem:] if last_beep_index_for_5k_prem < len(df) else df
            #
            # recent_values = df_slice['value'].iloc[-10:] if len(df_slice) >= 10 else df_slice['value']
            # if not recent_values.empty and option_premium_value - recent_values.any() >= 5000:
            #     print("Premium jumped ₹5000+ above 10-period any value — Beeping!")
            #     winsound.Beep(2500, 500)
            #     last_beep_index_for_5k_prem = len(df)  # Update to current end after beep

            if highest_options_premium_value is None or option_premium_value > highest_options_premium_value:
                highest_options_premium_value = option_premium_value

            if original_options_premium_value is None:
                original_options_premium_value = option_premium_value

            print(
                f"Strike:{ul_ltp_round}. Current PREM is: {option_premium_value}(CE:{option_quotes[option_ce]['last_price']} PE:{option_quotes[option_pe]['last_price']}),  original : {original_options_premium_value} and highest : {highest_options_premium_value} at {datetime.now(indian_timezone).time()}.")

            add_data(now, option_premium_value)
            print(f"Fetched at {now.strftime('%H:%M:%S')}: {option_premium_value:.2f}")
            time.sleep(2)
    except Exception as e:
        print(f"Error in data fetching thread: {e}")
        traceback.print_exc()


# --- Main execution ---
if __name__ == '__main__':

    kite = oUtils.intialize_kite_api()

    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, PART_SYMBOL, NO_OF_LOTS, STRIKE_MULTIPLE, STOPLOSS_POINTS = oUtils.get_instruments(
        kite)

    under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING

    while datetime.now(indian_timezone).time() < oUtils.MARKET_START_TIME:
        pass

    threading.Thread(target=fetch_data_loop, daemon=True).start()
    ani = FuncAnimation(fig, animate, interval=500)
    plt.show()

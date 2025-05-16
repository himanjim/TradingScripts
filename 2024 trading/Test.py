import OptionTradeUtils as oUtils

kite = oUtils.intialize_kite_api()

print(kite.instruments('NSE'))
exit(0)
# h_data = kite.historical_data('SENSEX2552082200PE', '2025-05-15 09:15:00', '2025-05-15 10:15:00', '1min')
h_data = kite.historical_data(265, '2024-05-15 09:15:00', '2024-05-15 15:15:00', 'minute')
# h_data = kite.historical_data()
# h_data = kite.quote('BFO:SENSEX2552082200PE')
print(h_data)
# print(kite.instruments('BSE'))
exit(0)

import datetime as dt
import winsound  # Use only on Windows
winsound.Beep(2000, 2000)
print(1380//600)
exit(0)
while True:
    if 'next_beep' not in globals():
        base = dt.datetime.combine(dt.date.today(), dt.time(8,10))
        next_beep = base + dt.timedelta(minutes=((dt.datetime.now() - base).seconds // 600 + 1)*10)
    print('About to beep.' + str(next_beep))
    print('Now:' + str(dt.datetime.now()))

    if dt.datetime.now() >= next_beep:
        winsound.Beep(1000, 3000)  # 3 seconds
        next_beep += dt.timedelta(minutes=10)
    print(next_beep)
# winsound.Beep(1000, 3000)
exit(0)

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
from datetime import datetime
import random
from matplotlib.animation import FuncAnimation
import threading
import time

# --- Initialize empty DataFrame ---
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
        df = pd.concat([df, pd.DataFrame({'timestamp': [new_time], 'value': [new_value]})], ignore_index=True)

# --- Animation Update Function ---
def animate(frame):
    with lock:
        if not df.empty:
            line.set_data(df['timestamp'], df['value'])
            ax.relim()
            ax.autoscale_view()
            fig.autofmt_xdate()

# --- Background thread to simulate API fetching ---
def fetch_data_loop():
    try:
        while True:
            now = datetime.now()
            value = random.uniform(1, 100)  # <-- Replace with API fetching
            add_data(now, value)
            print(f"Fetched at {now.strftime('%H:%M:%S')}: {value:.2f}")
            time.sleep(2)
    except Exception as e:
        print(f"Error in data fetching thread: {e}")

# --- Start background data fetching ---
threading.Thread(target=fetch_data_loop, daemon=True).start()

# --- Setup FuncAnimation ---
ani = FuncAnimation(fig, animate, interval=500)

# --- Start the plot (this will now show instantly and update!) ---
plt.show()

exit(0)
import re

original_string = "NIFTY2521323200CE"
new_number = "24000"  # Replace the number with this new one

# Use regex to match the number before 'CE' or 'PE' and replace it
modified_string = re.sub(r'(\d{5})(?=CE|PE)', new_number, original_string)

print(modified_string)

exit(0)

import yfinance as yf



# Get options data for Reliance Industries (RELIANCE.NS)

reliance_ticker = yf.Ticker("RELIANCE.NS")

options_data = reliance_ticker.options_chain()



# Access specific options data (e.g., call options with a specific strike price)

calls = options_data.calls

print(calls)




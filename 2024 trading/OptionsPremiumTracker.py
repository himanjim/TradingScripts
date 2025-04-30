import Utils as util
import time as tm
from datetime import datetime
import pytz
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


def fetch_data_loop():
    try:
        while True:
            now = datetime.now()

            original_options_premium_value = None
            highest_options_premium_value = None

            if datetime.now(indian_timezone).time() > util.MARKET_END_TIME:
                print(f"Market is closed. Hence exiting.")
                exit(0)

            try:

                ul_live_quote = kite.quote(under_lying_symbol)

                ul_ltp = ul_live_quote[under_lying_symbol]['last_price']

                # nifty_ltp_round_50 = round(nifty_ltp / 50) * 50
                ul_ltp_round = round(ul_ltp / STRIKE_MULTIPLE) * STRIKE_MULTIPLE

                option_pe = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + 'PE'
                option_ce = OPTIONS_EXCHANGE + PART_SYMBOL + str(ul_ltp_round) + 'CE'

                option_quotes = kite.quote([option_pe, option_ce])

            except Exception as e:
                # This will catch any exception and print the error message
                print(f"An error occurred: {e}")
                tm.sleep(2)
                continue

            option_premium_value = 0
            for trading_symbol, live_quote in option_quotes.items():
                option_premium_value += (live_quote['last_price'] * NO_OF_LOTS)

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


if __name__ == '__main__':

    indian_timezone = pytz.timezone('Asia/Calcutta')

    kite = util.intialize_kite_api()

    choice = 1

    premium_difference_for_action = 5000
    ###############################
    if choice == 1:
        # NIFTY24D1924700PE
        ###############################
        # UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY 50'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        # PART_SYMBOL = ':NIFTY25123'
        # PART_SYMBOL = ':NIFTY25220'
        PART_SYMBOL = ':NIFTY25430'
        NO_OF_LOTS = 300
        STRIKE_MULTIPLE = 50
    elif choice == 2:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_BSE
        UNDERLYING = ':SENSEX'
        OPTIONS_EXCHANGE = kite.EXCHANGE_BFO
        # PART_SYMBOL = ':SENSEX25225'
        PART_SYMBOL = ':SENSEX25APR'
        NO_OF_LOTS = 100
        STRIKE_MULTIPLE = 100

    else:
        UNDER_LYING_EXCHANGE = kite.EXCHANGE_NSE
        UNDERLYING = ':NIFTY BANK'
        OPTIONS_EXCHANGE = kite.EXCHANGE_NFO
        PART_SYMBOL = ':BANKNIFTY25APR'
        NO_OF_LOTS = 120
        STRIKE_MULTIPLE = 100

    ###############################

    # under_lying_symbol = kite.EXCHANGE_NSE + ':NIFTY 50'
    under_lying_symbol = UNDER_LYING_EXCHANGE + UNDERLYING

    while datetime.now(indian_timezone).time() < util.MARKET_START_TIME:
        pass

    # --- Start background data fetching ---
    threading.Thread(target=fetch_data_loop, daemon=True).start()

    # --- Setup FuncAnimation ---
    ani = FuncAnimation(fig, animate, interval=500)

    # --- Start the plot (this will now show instantly and update!) ---
    plt.show()


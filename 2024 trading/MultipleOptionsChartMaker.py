import datetime as dt
import pytz
import OptionTradeUtils as oUtils
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf

# Initialize kite API
kite = oUtils.intialize_kite_api()

# Constants
exchange = 'NFO'
symbol = 'BANKNIFTY'  # Change to 'NIFTY' or 'SENSEX' if needed
lot_size = 15  # for BANKNIFTY
timezone = pytz.timezone('Asia/Kolkata')


# Get current expiry
def get_nearest_expiry():
    instruments = kite.instruments(exchange)
    expiry_dates = sorted({i['expiry'] for i in instruments if symbol in i['tradingsymbol']})
    return expiry_dates[0], instruments


# Get option instruments for 3 ITM CE and PE
def get_option_tokens(underlying_value):
    _, _, exchange, part_symbol, _, strike_multiple, _ = oUtils.get_instruments(kite)

    atm_strike = round(underlying_value / strike_multiple) * strike_multiple

    # 3 ITM Calls = lower strikes
    ce_strikes = [atm_strike - i * strike_multiple for i in range(1, 4)]
    # 3 ITM Puts = higher strikes
    pe_strikes = [atm_strike + i * strike_multiple for i in range(1, 4)]

    # Construct option symbols like NIFTY2561924800CE
    ce_symbols = [f"{part_symbol}{strike}CE" for strike in ce_strikes]
    pe_symbols = [f"{part_symbol}{strike}PE" for strike in pe_strikes]

    option_list = []

    for sym in ce_symbols + pe_symbols:
        try:
            full_symbol = f"{exchange}{sym}"
            inst = kite.ltp([full_symbol])
            token = list(inst.values())[0]['instrument_token']
            option_list.append((sym, token))
        except Exception as e:
            print(f"⚠️ Failed to get token for {sym}: {e}")

    return option_list



# Fetch 2-minute candle data
def fetch_2min_candles(instrument_token):
    now = dt.datetime.now(timezone)
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now < start:
        raise ValueError("Market hasn't opened yet.")
    data = kite.historical_data(instrument_token, start, now, "2minute")
    df = pd.DataFrame(data)
    df.set_index('date', inplace=True)
    return df


# Plot all charts in single window
def plot_all_charts(option_list):
    fig, axes = plt.subplots(2, 3, figsize=(18, 8), sharex=True)
    axes = axes.flatten()

    while True:
        for ax in axes:
            ax.clear()

        for i, (name, token) in enumerate(option_list):
            df = fetch_2min_candles(token)
            df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
            mpf.plot(df, type='candle', ax=axes[i], axtitle=name, style='charles', volume=False, show_nontrading=False)

        plt.pause(1)  # pause for 1 second before refresh


# -------------------------------
# 🔽 MAIN EXECUTION
# -------------------------------
if __name__ == "__main__":
    underlying = float(input("Enter underlying value: "))
    option_list = get_option_tokens(underlying)
    plot_all_charts(option_list)

import datetime as dt
import pytz
import OptionTradeUtils as oUtils
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
import matplotlib.gridspec as gridspec

# Initialize kite API
kite = oUtils.intialize_kite_api()

# Constants
exchange = 'NFO'
symbol = 'BANKNIFTY'  # Change to 'NIFTY' or 'SENSEX' if needed
lot_size = 15  # for BANKNIFTY
timezone = pytz.timezone('Asia/Kolkata')


# Get option instruments for 3 ITM CE and PE
def get_option_tokens(underlying_value):
    UNDER_LYING_EXCHANGE ,UNDERLYING, exchange, part_symbol, _, strike_multiple, _ = oUtils.get_instruments(kite)

    atm_strike = round(underlying_value / strike_multiple) * strike_multiple

    # 2 ITM + ATM CE and PE
    ce_strikes = [atm_strike, atm_strike - strike_multiple, atm_strike - 2 * strike_multiple]
    pe_strikes = [atm_strike, atm_strike + strike_multiple, atm_strike + 2 * strike_multiple]

    ce_symbols = [f"{part_symbol}{strike}CE" for strike in ce_strikes]
    pe_symbols = [f"{part_symbol}{strike}PE" for strike in pe_strikes]

    option_list = []

    # Add CE and PE options
    for sym in ce_symbols + pe_symbols:
        try:
            full_symbol = f"{exchange}{sym}"
            inst = kite.ltp([full_symbol])
            token = list(inst.values())[0]['instrument_token']
            option_list.append((sym, token))
        except Exception as e:
            print(f"⚠️ Failed to get token for {sym}: {e}")

    # Add underlying index (e.g., BANKNIFTY) using LTP
    try:
        inst = kite.ltp([f"{UNDER_LYING_EXCHANGE}{UNDERLYING}"])
        token = list(inst.values())[0]['instrument_token']
        option_list.insert(0, (f"{symbol}_INDEX", token))
    except Exception as e:
        print(f"⚠️ Failed to get underlying token for {symbol}: {e}")

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
    fig = plt.figure(figsize=(16, 12))
    gs = gridspec.GridSpec(4, 2, height_ratios=[1, 1, 1, 1])

    axes = []

    # First 6 option charts
    for i in range(6):
        ax = fig.add_subplot(gs[i // 2, i % 2])
        axes.append(ax)

    # Last (7th) chart – underlying – spans both columns
    ax_underlying = fig.add_subplot(gs[3, :])
    axes.append(ax_underlying)

    while True:
        for ax in axes:
            ax.clear()

        for i, (name, token) in enumerate(option_list):
            df = fetch_2min_candles(token)
            df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)
            mpf.plot(df, type='candle', ax=axes[i], axtitle=name, style='charles', volume=False, show_nontrading=False)

        plt.pause(1)

# -------------------------------
# 🔽 MAIN EXECUTION
# -------------------------------
if __name__ == "__main__":
    underlying = float(input("Enter underlying value: "))
    option_list = get_option_tokens(underlying)
    plot_all_charts(option_list)

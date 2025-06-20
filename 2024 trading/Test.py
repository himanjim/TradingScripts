import datetime as dt
import pandas as pd
import plotly.graph_objects as go
import OptionTradeUtils as oUtils
# --- Zerodha API Setup ---
# Initialize kite API
kite = oUtils.intialize_kite_api()

# --- Get instrument token ---
def get_instrument_token(exchange: str, tradingsymbol: str):
    instruments = kite.instruments(exchange)
    df_instruments = pd.DataFrame(instruments)
    row = df_instruments[df_instruments['tradingsymbol'] == tradingsymbol]
    if row.empty:
        raise Exception(f"Symbol {tradingsymbol} not found in {exchange}")
    return int(row.iloc[0]['instrument_token'])

# --- Set symbol and date range ---
symbol = "NIFTY 50"
exchange = "NSE"
instrument_token = get_instrument_token(exchange, symbol)

today = dt.datetime.now().date()
from_time = dt.datetime.combine(today, dt.time(9, 15))
to_time = dt.datetime.combine(today, dt.time(15, 30))

# --- Fetch 2-minute data ---
data = kite.historical_data(
    instrument_token=instrument_token,
    from_date=from_time,
    to_date=to_time,
    interval="2minute"
)

df = pd.DataFrame(data)
df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)

# --- Detect Swing Highs and Lows ---
def find_swings(df, window=2):
    highs, lows = [], []
    for i in range(window, len(df) - window):
        if df['high'].iloc[i] == max(df['high'].iloc[i-window:i+window+1]):
            highs.append((df.index[i], df['high'].iloc[i]))
        if df['low'].iloc[i] == min(df['low'].iloc[i-window:i+window+1]):
            lows.append((df.index[i], df['low'].iloc[i]))
    return highs, lows

highs, lows = find_swings(df)

# --- Plot with Plotly ---
fig = go.Figure()

# Price line
fig.add_trace(go.Scatter(
    x=df.index, y=df['close'],
    mode='lines',
    name='Close Price',
    line=dict(color='black')
))

# Add trendlines for swing highs
for _, price in highs:
    fig.add_shape(type='line',
                  x0=df.index[0], x1=df.index[-1],
                  y0=price, y1=price,
                  line=dict(color='red', width=1, dash='dash'))

# Add trendlines for swing lows
for _, price in lows:
    fig.add_shape(type='line',
                  x0=df.index[0], x1=df.index[-1],
                  y0=price, y1=price,
                  line=dict(color='green', width=1, dash='dash'))

# Layout
fig.update_layout(
    title=f"{symbol} - Trendlines (2-min) - {today}",
    xaxis_title="Time",
    yaxis_title="Price",
    xaxis_rangeslider_visible=False,
    template='plotly_white',
    height=600
)

fig.show()

import dash
from dash import dcc, html
from dash.dependencies import Output, Input
import plotly.graph_objects as go
import datetime as dt
import pandas as pd
import pytz
import OptionTradeUtils as oUtils

# Initialize kite API
kite = oUtils.intialize_kite_api()

# Constants
timezone = pytz.timezone('Asia/Kolkata')


def compute_pivots(instrument_token):
    # Use previous day's full data: 9:15 to 15:30
    today = dt.datetime.now(timezone).date()
    yesterday = today - dt.timedelta(days=1)
    start = timezone.localize(dt.datetime.combine(yesterday, dt.time(9, 15)))
    end = timezone.localize(dt.datetime.combine(yesterday, dt.time(15, 30)))

    try:
        data = kite.historical_data(instrument_token, start, end, "day")
    except Exception as e:
        print(f"⚠️ Error fetching previous day's data for pivots: {e}")
        return {}

    if not data:
        return {}

    high = data[0]['high']
    low = data[0]['low']
    close = data[0]['close']

    P = (high + low + close) / 3
    R1 = 2 * P - low
    S1 = 2 * P - high
    R2 = P + (high - low)
    S2 = P - (high - low)
    R3 = high + 2 * (P - low)
    S3 = low - 2 * (high - P)

    return {
        'P': P, 'R1': R1, 'R2': R2, 'R3': R3,
        'S1': S1, 'S2': S2, 'S3': S3
    }


def fetch_candles(instrument_token, interval="minute"):
    now = dt.datetime.now(timezone)
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    data = kite.historical_data(instrument_token, start, now, interval)
    df = pd.DataFrame(data)
    df.set_index('date', inplace=True)
    return df.tail(60)  # Last 60 candles (1 hour for 1-min)


def get_tokens():
    UNDER_LYING_EXCHANGE, UNDERLYING, OPTIONS_EXCHANGE, part_symbol, _, strike_multiple, _ = oUtils.get_instruments(kite)

    # Underlying token
    ltp = kite.ltp([f"{UNDER_LYING_EXCHANGE}{UNDERLYING}"])
    underlying_token = list(ltp.values())[0]['instrument_token']

    # Get open price
    df_underlying = fetch_candles(underlying_token)
    open_price = df_underlying.iloc[0]['open']
    atm_strike = round(open_price / strike_multiple) * strike_multiple

    ce_symbol = f"{part_symbol}{atm_strike}CE"
    pe_symbol = f"{part_symbol}{atm_strike}PE"

    # Option tokens
    ce_token = list(kite.ltp([f"{OPTIONS_EXCHANGE}{ce_symbol}"]).values())[0]['instrument_token']
    pe_token = list(kite.ltp([f"{OPTIONS_EXCHANGE}{pe_symbol}"]).values())[0]['instrument_token']

    return {
        "underlying": (f"{UNDERLYING}", underlying_token),
        "ce": (ce_symbol, ce_token),
        "pe": (pe_symbol, pe_token)
    }


def get_figure(df, title, instrument_token, xaxis='x'):
    pivots = compute_pivots(instrument_token)

    fig = go.Figure(data=[go.Candlestick(
        x=df.index,
        open=df['open'],
        high=df['high'],
        low=df['low'],
        close=df['close'],
        xaxis=xaxis,
        yaxis='y'
    )])

    # Add pivot lines
    for name, y in pivots.items():
        fig.add_hline(
            y=y,
            line_dash='dot',
            line_color='green' if name.startswith('R') else 'red' if name.startswith('S') else 'blue',
            annotation_text=name,
            annotation_position="right"
        )

    fig.update_layout(
        title=title,
        xaxis_rangeslider_visible=False,
        margin=dict(t=40, b=20, l=40, r=20),
        height=280,
        autosize=True
    )

    return fig


# Get tokens initially
tokens = get_tokens()

# Create Dash app
app = dash.Dash(__name__)
app.title = "Options Dashboard"

app.layout = html.Div([
    html.H2("Live BANKNIFTY Options Dashboard with Pivot Lines", style={"textAlign": "center"}),
    dcc.Interval(id='interval-component', interval=15 * 1000, n_intervals=0),
    dcc.Graph(id='underlying-chart'),
    dcc.Graph(id='ce-chart'),
    dcc.Graph(id='pe-chart')
], style={'padding': '10px', 'maxWidth': '950px', 'margin': 'auto'})


@app.callback(
    Output('underlying-chart', 'figure'),
    Output('ce-chart', 'figure'),
    Output('pe-chart', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_charts(n):
    name_u, token_u = tokens["underlying"]
    df_u = fetch_candles(token_u)
    fig_u = get_figure(df_u, name_u, instrument_token=token_u, xaxis='x')

    name_ce, token_ce = tokens["ce"]
    df_ce = fetch_candles(token_ce)
    fig_ce = get_figure(df_ce, name_ce, instrument_token=token_ce, xaxis='x')

    name_pe, token_pe = tokens["pe"]
    df_pe = fetch_candles(token_pe)
    fig_pe = get_figure(df_pe, name_pe, instrument_token=token_pe, xaxis='x')

    # Explicitly reuse xaxis config
    common_xaxis = fig_u['layout']['xaxis']
    fig_ce.update_layout(xaxis=common_xaxis)
    fig_pe.update_layout(xaxis=common_xaxis)

    return fig_u, fig_ce, fig_pe


if __name__ == '__main__':
    app.run(debug=True)

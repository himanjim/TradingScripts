import plotly.graph_objects as go
import pandas as pd
import os
from datetime import datetime
from dateutil.tz import tzoffset
import OptionTradeUtils as oUtils

kite = oUtils.intialize_kite_api()
# kite.instruments('BFO')
# h_data = kite.historical_data('SENSEX2552082200PE', '2025-05-15 09:15:00', '2025-05-15 10:15:00', '1min')
interval = 'minute'
interval = '10minute'
data = kite.historical_data(265, '2024-05-15 09:15:00', '2024-05-15 15:15:00', interval)
# # Sample data
# data = [
#     {'date': datetime(2024, 5, 15, 9, 15, tzinfo=tzoffset(None, 19800)), 'open': 73200.23, 'high': 73242.76, 'low': 73158.53, 'close': 73183.7, 'volume': 0},
#     {'date': datetime(2024, 5, 15, 9, 16, tzinfo=tzoffset(None, 19800)), 'open': 73182.04, 'high': 73184.65, 'low': 73135.35, 'close': 73180.08, 'volume': 0},
#     {'date': datetime(2024, 5, 15, 9, 17, tzinfo=tzoffset(None, 19800)), 'open': 73185.78, 'high': 73203.4, 'low': 73164.6, 'close': 73203.4, 'volume': 0}
# ]

# Times you want to mark on the chart
# Add as datetime objects (must match or fall within range of df['date'])
marked_times = [
    datetime(2024, 5, 15, 9, 16, tzinfo=tzoffset(None, 19800)),
]

# Convert to DataFrame
df = pd.DataFrame(data)

# Create the candlestick chart
fig = go.Figure(data=[go.Candlestick(
    x=df['date'],
    open=df['open'],
    high=df['high'],
    low=df['low'],
    close=df['close']
)])

for mark_time in marked_times:
    # Get closest earlier time row
    df_before = df[df['date'] <= mark_time]
    if not df_before.empty:
        nearest_row = df_before.iloc[-1]
        price = nearest_row['close']
        time = nearest_row['date']

        # Small horizontal segment: ±30 seconds (adjust for 10-min candles if needed)
        if interval == '10minute':
            left = time - pd.Timedelta(seconds=240)
            right = time + pd.Timedelta(seconds=240)
            mark_time_line_width = 5
            mark_time_line_dash = None
        else:
            left = time - pd.Timedelta(seconds=60)
            right = time + pd.Timedelta(seconds=60)
            mark_time_line_width = 2
            mark_time_line_dash = "dash"

        fig.add_trace(go.Scatter(
            x=[left, right],
            y=[price, price],
            mode="lines",
            line=dict(color="black", width=mark_time_line_width, dash=mark_time_line_dash),
            showlegend=False
        ))


# Customize layout
fig.update_layout(
    title='Candlestick Chart with Marked Times',
    xaxis_title='Time',
    yaxis_title='Price',
    xaxis_rangeslider_visible=False
)

# Timestamped filename
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
output_folder = "C:/Users/USER/Downloads/"
output_filename = f"candlestick_chart_{timestamp_str}.png"
os.makedirs(output_folder, exist_ok=True)
output_path = os.path.join(output_folder, output_filename)

# Save chart
fig.write_image(output_path, width=1600, height=900, scale=3)

# Optional: Show chart
# fig.show()

print(f"Chart saved to: {output_path}")

import plotly.graph_objects as go
import pandas as pd
import os
from datetime import datetime, timedelta, time
from dateutil.tz import gettz
import OptionTradeUtils as oUtils  # ensure this file has intialize_kite_api()

# Initialize Kite API
kite = oUtils.intialize_kite_api()

DIR = "C:/Users/himan/Downloads/"
excel_path = DIR + "Technical analysis testing_min_2.xlsx"
output_folder = DIR + "generated_charts"
os.makedirs(output_folder, exist_ok=True)

# Timezone
ist = gettz("Asia/Kolkata")

# Intervals to process
intervals = ['minute', '10minute']

def is_valid_time_object(value):
    return isinstance(value, (datetime, time))

def add_vertical_line(fig, df, mark_time, color):
    df_before = df[df['date'] <= mark_time]
    if not df_before.empty:
        nearest = df_before.iloc[-1]
        x = nearest['date']
        fig.add_vline(
            x=x,
            line=dict(color=color, width=1),
            opacity=1,
            layer="above"
        )

if __name__ == '__main__':
    df_trades = pd.read_excel(excel_path, sheet_name='StraddleTrades')

    # Group by (ENTRY DATE, Instrument Token)
    grouped = df_trades.groupby(['ENTRY  DATE', 'Instrument Token'])

    for (entry_date, instrument_token), group in grouped:
        entry_date = pd.to_datetime(entry_date).date()
        instrument_token = int(instrument_token)

        entry_datetime = datetime.combine(entry_date, datetime.strptime("09:15", "%H:%M").time()).replace(tzinfo=ist)
        exit_datetime = datetime.combine(entry_date, datetime.strptime("15:30", "%H:%M").time()).replace(tzinfo=ist)

        # Collect all mark times and colors
        entry_exit_marks = []
        for _, row in group.iterrows():
            try:
                pl_value = float(row.get('P/L', 0))
                entry_color = "orange" if pl_value > 0 else "blue"
            except:
                entry_color = "blue"

            if is_valid_time_object(row['Entry Time']):
                entry_mark_time = datetime.combine(entry_date, row['Entry Time']).replace(tzinfo=ist)
                entry_exit_marks.append(("entry", entry_mark_time, entry_color))

            if is_valid_time_object(row['Time of exit']):
                exit_mark_time = datetime.combine(entry_date, row['Time of exit']).replace(tzinfo=ist)
                entry_exit_marks.append(("exit", exit_mark_time, "black"))

        for interval in intervals:
            filename = f"{entry_date}_{instrument_token}_{interval}.png"
            output_path = os.path.join(output_folder, filename)
            if os.path.exists(output_path):
                print(f"Skipping {filename} (already exists).")
                continue

            try:
                hist_data = kite.historical_data(
                    instrument_token, entry_datetime, exit_datetime, interval)
                if not hist_data:
                    print(f"No data for {filename}")
                    continue

                df = pd.DataFrame(hist_data)
                fig = go.Figure(data=[go.Candlestick(
                    x=df['date'],
                    open=df['open'],
                    high=df['high'],
                    low=df['low'],
                    close=df['close']
                )])

                for mark_type, mark_time, color in entry_exit_marks:
                    add_vertical_line(fig, df, mark_time, color)

                fig.update_layout(
                    title=f"{instrument_token} - {entry_date} - {interval}",
                    xaxis_title="Time",
                    yaxis_title="Price",
                    xaxis_rangeslider_visible=False,
                    xaxis=dict(
                        tickformat="%H:%M",
                        tickangle=45,
                        dtick=600000,  # 5 minutes = 300,000 milliseconds
                        tickfont=dict(size=10),
                        showgrid=True
                    )
                )
                fig.write_image(output_path, width=1600, height=900, scale=3)
                print(f"Saved: {output_path}")

            except Exception as e:
                print(f"Error for {filename}: {e}")

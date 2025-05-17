import plotly.graph_objects as go
import pandas as pd
import os
from datetime import datetime, timedelta
from dateutil.tz import gettz
import OptionTradeUtils as oUtils  # Make sure this exists and has intialize_kite_api()

# Initialize Kite API
kite = oUtils.intialize_kite_api()

DIR = "C:/Users/USER/Downloads/"

# Load Excel
excel_path = DIR + "Technical analysis testing_min_2.xlsx"
df_trades = pd.read_excel(excel_path)

# Timezone & Output folder
ist = gettz("Asia/Kolkata")
output_folder = DIR + "generated_charts"
os.makedirs(output_folder, exist_ok=True)

# Intervals to process
intervals = ['minute', '10minute']

def normalize_time_string(time_str):
    try:
        # Try parsing with full format
        datetime.strptime(time_str, "%H:%M:%S")
        return time_str  # already in correct format
    except ValueError:
        try:
            # Try parsing with missing seconds
            dt = datetime.strptime(time_str, "%H:%M")
            return dt.strftime("%H:%M:00")  # append seconds
        except ValueError:
            return None  # invalid format

if __name__ == '__main__':

    # Loop over each row in Excel
    for index, row in df_trades.iterrows():
        option_name = str(row['OPTION']).strip()
        entry_date = pd.to_datetime(row['ENTRY  DATE']).date()
        exit_date = pd.to_datetime(row['ENTRY  DATE']).date()

        entry_datetime = datetime.combine(entry_date, datetime.strptime("09:15", "%H:%M").time()).replace(tzinfo=ist)
        exit_datetime = datetime.combine(exit_date, datetime.strptime("15:30", "%H:%M").time()).replace(tzinfo=ist)

        # You must set actual instrument_token from option_name mapping
        instrument_token = int(row['Instrument Token'].strip())  # Placeholder - replace with real token lookup

        # Use actual mark times if available or simulate
        entry_mark_time = normalize_time_string(row['Entry Time'].strip())
        exit_mark_time = normalize_time_string(row['Time of exit'].strip())

        if entry_mark_time:
            entry_mark_time = datetime.combine(entry_date, entry_mark_time).replace(tzinfo=ist)

        if exit_mark_time:
            exit_mark_time = datetime.combine(entry_date, exit_mark_time).replace(tzinfo=ist)

        pl_value = row.get('P/L', '').strip()
        try:
            is_profit = float(pl_value) > 0
        except:
            is_profit = False
        entry_color = "orange" if is_profit else "black"

        for interval in intervals:
            filename = f"{entry_date}_{instrument_token}_{interval}.png"
            output_path = os.path.join(output_folder, filename)
            if os.path.exists(output_path):
                print(f"Skipping {filename} (already exists).")
                continue

            try:
                # Fetch data from Kite
                hist_data = kite.historical_data(instrument_token, entry_datetime, exit_datetime, interval)
                if not hist_data:
                    print(f"No data for {filename}")
                    continue

                df = pd.DataFrame(hist_data)

                # Create candlestick chart
                fig = go.Figure(data=[go.Candlestick(
                    x=df['date'],
                    open=df['open'],
                    high=df['high'],
                    low=df['low'],
                    close=df['close']
                )])

                # Helper to add short horizontal line
                def add_horizontal_line(mark_time, color):
                    df_before = df[df['date'] <= mark_time]
                    if not df_before.empty:
                        nearest = df_before.iloc[-1]
                        y = nearest['close']
                        x = nearest['date']
                        width = timedelta(minutes=8 if interval == '10minute' else 2)
                        fig.add_trace(go.Scatter(
                            x=[x - width / 2, x + width / 2],
                            y=[y, y],
                            mode="lines",
                            line=dict(color=color, width=3),
                            showlegend=False
                        ))

                if entry_mark_time:
                    # Add entry and exit lines
                    add_horizontal_line(entry_mark_time, entry_color)

                if exit_mark_time:
                    add_horizontal_line(exit_mark_time, "black")

                # Save chart
                fig.update_layout(
                    title=f"{option_name} - {interval}",
                    xaxis_title="Time",
                    yaxis_title="Price",
                    xaxis_rangeslider_visible=False
                )
                fig.write_image(output_path, width=1600, height=900, scale=3)
                print(f"Saved: {output_path}")

            except Exception as e:
                print(f"Error for {filename}: {e}")

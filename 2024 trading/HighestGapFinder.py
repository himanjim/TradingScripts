import yfinance as yf
import pandas as pd

if __name__ == '__main__':
    # Step 1: Fetch data and generate charts for all stocks
    file_path =  'C:/Users/himan/Downloads/ind_nifty50list.csv'
    df = pd.read_csv(file_path)

    nifty50_symbols = [symbol + '.NS' for symbol in df['Symbol']]
    # Initialize a dictionary to store gap-up and gap-down data
    gap_data = {}

    # Loop through each stock and calculate gap-ups and gap-downs
    for ticker in nifty50_symbols:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="5d")  # Get last two days' data

        if len(hist) >= 2:
            prev_close = hist['Close'].iloc[-2]  # Previous day's close
            today_open = hist['Open'].iloc[-1]  # Today's open

            # Calculate percentage gap
            gap = ((today_open - prev_close) / prev_close) * 100

            # Store the result
            gap_data[ticker] = gap

    # Convert to DataFrame
    gap_df = pd.DataFrame(list(gap_data.items()), columns=['Ticker', 'Gap %'])

    # Sort by gap percentage
    gap_df_sorted = gap_df.sort_values(by='Gap %', ascending=False)

    # Highest Gap-Ups
    print("Highest Gap-Ups:")
    print(gap_df_sorted.head())

    # Highest Gap-Downs
    print("\nHighest Gap-Downs:")
    print(gap_df_sorted.tail())

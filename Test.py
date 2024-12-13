import yfinance as yf
import mplfinance as mpf

# Download stock data using yfinance
ticker = 'AAPL'  # Replace with your desired ticker symbol
data = yf.download(ticker, start='2023-01-01', end='2023-12-31', interval='1d')
print(data['Open'].T.head())
# Check for any missing data and drop such rows
data = data.dropna()

# Check data types and correct them if needed (convert columns to float)
data['Open'] = data['Open'].astype(float)
data['High'] = data['High'].astype(float)
data['Low'] = data['Low'].astype(float)
data['Close'] = data['Close'].astype(float)
data['Volume'] = data['Volume'].astype(float)

# Rename columns to match mplfinance's expected format
data.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)

print(data.head())

# exit(0)

# Plot the candlestick chart using mplfinance
mpf.plot(data, type='candle', volume=True, style='yahoo', title=f'{ticker} Candlestick Chart', mav=(10, 20))

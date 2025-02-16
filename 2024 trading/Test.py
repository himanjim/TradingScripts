import re

original_string = "NIFTY2521323200CE"
new_number = "24000"  # Replace the number with this new one

# Use regex to match the number before 'CE' or 'PE' and replace it
modified_string = re.sub(r'(\d{5})(?=CE|PE)', new_number, original_string)

print(modified_string)

exit(0)

import yfinance as yf
import mplfinance as mpf

# Step 1: Download stock data for Apple (AAPL) from yfinance
ticker = 'TCS.NS'
aapl = yf.Ticker(ticker)
stock_data = aapl.history(interval='5m', period='1d')

# Step 2: Visualize the data using mplfinance
# We can customize the chart style, moving averages, and more.
mpf.plot(stock_data, type='candle', volume=True, mav=(10, 20),
         title=f'{ticker} Stock Price',
         style='yahoo',
         show_nontrading=True)




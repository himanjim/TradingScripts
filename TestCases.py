import yfinance as yf
import mplfinance as mpf
import matplotlib.pyplot as plt
from PIL import Image
import pandas as pd
import os

# List of NIFTY50 stock symbols
nifty50_symbols = [
    'RELIANCE.NS', 'TCS.NS', 'INFY.NS', 'HDFCBANK.NS', 'ICICIBANK.NS', 'HINDUNILVR.NS',
    'HDFC.NS', 'BAJFINANCE.NS', 'KOTAKBANK.NS', 'LT.NS', 'AXISBANK.NS', 'ITC.NS',
    # Add more NIFTY50 symbols here
]


# Step 1: Fetch the 1-minute data for today for each stock
def fetch_data(symbol):
    data = yf.download(tickers=symbol, interval='5m', period='1d')

    # Clean the data by dropping any rows with missing or NaN values
    data.dropna(inplace=True)

    # Ensure all relevant columns ('Open', 'High', 'Low', 'Close', 'Volume') are float types
    numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume']

    # # Convert to numeric (float), force errors to NaN, then drop rows with NaN
    for col in numeric_columns:
        # data[col] = pd.to_numeric(data[col], errors='coerce')
        data[col] = data[col].apply(pd.to_numeric, errors='coerce')

    # Drop any rows with NaN values
    data.dropna(inplace=True)

    return data


# Step 2: Generate candlestick charts and save as images
def save_candlestick_chart(data, symbol):
    if not data.empty:
        mpf.plot(data, type='candle', style='charles', title=symbol, savefig=f"{symbol}.png")
    else:
        print(f"No valid data for {symbol}")


# Step 3: Create a collage of the charts
def create_collage(image_folder, output_image, grid_size=(5, 10), image_size=(300, 300)):
    images = []

    # Load images from folder
    for symbol in nifty50_symbols:
        img_path = os.path.join(image_folder, f"{symbol}.png")
        if os.path.exists(img_path):
            img = Image.open(img_path).resize(image_size)
            images.append(img)

    # Create blank canvas for collage
    collage_width = grid_size[1] * image_size[0]
    collage_height = grid_size[0] * image_size[1]
    collage_image = Image.new('RGB', (collage_width, collage_height), (255, 255, 255))

    # Paste each image in the grid
    for idx, img in enumerate(images):
        row = idx // grid_size[1]
        col = idx % grid_size[1]
        x_offset = col * image_size[0]
        y_offset = row * image_size[1]
        collage_image.paste(img, (x_offset, y_offset))

    collage_image.save(output_image)
    collage_image.show()


# Main function to run the process
def main():
    # Step 1: Fetch data and generate charts for all stocks
    for symbol in nifty50_symbols:
        data = fetch_data(symbol)
        save_candlestick_chart(data, symbol)

    # Step 2: Create the collage of all candlestick charts
    create_collage(image_folder=".", output_image="nifty50_collage.png")


if __name__ == "__main__":
    main()

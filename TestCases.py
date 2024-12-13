import os
import pandas as pd
import mplfinance as mpf
import yfinance as yf
from PIL import Image


# Step 1: Fetch the 1-minute data for today for each stock
def fetch_data(symbol):
    ticker_data = yf.Ticker(symbol)
    data = ticker_data.history(interval='5m', period='1d')
    return data


# Step 2: Generate candlestick charts and save as images
def save_candlestick_chart(data, symbol):
    if not data.empty:
        mpf.plot(data, type='candle', style='charles', title=symbol, savefig=f"C:/Users/USER/Downloads/Collages/{symbol}.png")
    else:
        print(f"No valid data for {symbol}")


# Step 3: Create a collage of the charts
def create_collage(image_folder, output_image, grid_size=(5, 10), image_size=(300, 300), nifty50_symbols= None):
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
    file_path = 'C:/Users/USER/Downloads/ind_nifty50list.csv'
    df = pd.read_csv(file_path)

    nifty50_symbols = [symbol + '.NS' for symbol in df['Symbol']]

    for symbol in nifty50_symbols:
        data = fetch_data(symbol)
        save_candlestick_chart(data, symbol)

    # Step 2: Create the collage of all candlestick charts
    create_collage(image_folder="C:/Users/USER/Downloads/Collages", output_image="nifty50_collage.png", nifty50_symbols= nifty50_symbols)


if __name__ == "__main__":
    main()

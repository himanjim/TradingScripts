import os
import pandas as pd
import mplfinance as mpf
import yfinance as yf
from PIL import Image, ImageDraw

# DIRECTORY = 'C:/Users/himan/Downloads/'
DIRECTORY = 'C:/Users/USER/Downloads/'
# Dictionary to store the ratios for each stock
ratios = {}


# Step 1: Fetch the 1-minute data for today for each stock
def fetch_data(symbol):
    ticker_data = yf.Ticker(symbol)
    data = ticker_data.history(interval='1m', period='1d')
    return data


# Step 2: Generate candlestick charts and save as images
def save_candlestick_chart(data, symbol):
    if not data.empty:
        # Calculate the day's open (first open), high (max high), low (min low)
        day_open = data['Open'].iloc[0]
        day_high = data['High'].max()
        day_low = data['Low'].min()

        # Calculate the ratios
        high_open_ratio = (day_high - day_open) / day_open if day_open != 0 else 0
        open_low_ratio = (day_open - day_low) / day_open if day_open != 0 else 0

        # Choose the higher of the two ratios
        higher_ratio = max(high_open_ratio, open_low_ratio)
        ratio_label = "H-O/O" if high_open_ratio >= open_low_ratio else "O-L/O"

        # Store the ratios for later sorting
        ratios[symbol] = {
            'higher_ratio': higher_ratio,
            'high_open_ratio': high_open_ratio,
            'open_low_ratio': open_low_ratio
        }

        # Format the title to show the higher ratio
        title = f"{symbol} | {ratio_label}: {higher_ratio:.2%}"

        # Plot and save the candlestick chart with the ratios in the title
        mpf.plot(data, type='candle', style='charles', title=title, savefig=(DIRECTORY + f"Collages/{symbol}.png"))
    else:
        print(f"No valid data for {symbol}")


# Step 3: Create a collage of the charts
def create_collage(image_folder, output_image, grid_size=(5, 10), image_size=(300, 300), nifty50_symbols=None):
    images = []

    # Load images from folder
    for symbol in nifty50_symbols:
        img_path = os.path.join(image_folder, f"{symbol}.png")
        if os.path.exists(img_path):
            img = Image.open(img_path).resize(image_size)

            # Draw border around the image
            draw = ImageDraw.Draw(img)
            draw.rectangle([0, 0, image_size[0] - 1, image_size[1] - 1], outline="red", width=1)

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

    collage_image.save(image_folder + output_image)
    collage_image.show()


# Step 4: Identify stocks with the highest ratios
def print_highest_ratios():
    # Find the stock with the highest high-open ratio
    highest_ho_stock = max(ratios, key=lambda x: ratios[x]['high_open_ratio'])
    highest_ho_value = ratios[highest_ho_stock]['high_open_ratio']

    # Find the stock with the highest open-low ratio
    highest_ol_stock = max(ratios, key=lambda x: ratios[x]['open_low_ratio'])
    highest_ol_value = ratios[highest_ol_stock]['open_low_ratio']

    print(f"Highest (High-Open)/Open ratio: {highest_ho_stock} with {highest_ho_value:.2%}")
    print(f"Highest (Open-Low)/Open ratio: {highest_ol_stock} with {highest_ol_value:.2%}")

    sorted_ratios = sorted(ratios.items(), key=lambda x: x[1]['higher_ratio'], reverse=True)

    return sorted_ratios  # Return sorted list of tuples (symbol, ratio data)


# Main function to run the process
def main():
    # Step 1: Fetch data and generate charts for all stocks
    file_path = DIRECTORY + 'ind_nifty50list.csv'
    df = pd.read_csv(file_path)

    nifty50_symbols = [symbol + '.NS' for symbol in df['Symbol']]
    # nifty50_symbols = [nifty50_symbols[0]]

    for symbol in nifty50_symbols:
        data = fetch_data(symbol)
        save_candlestick_chart(data, symbol)

    # Step 2: Print the stocks with the highest ratios
    sorted_ratios = print_highest_ratios()

    # Step 3: Create the collage of all candlestick charts with borders in descending order of ratios
    sorted_symbols = [symbol for symbol, _ in sorted_ratios]  # Extract sorted symbols
    create_collage(image_folder= (DIRECTORY + 'Collages'), output_image="nifty50_collage.png",
                   nifty50_symbols=sorted_symbols)


if __name__ == "__main__":
    main()

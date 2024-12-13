import pandas as pd
import glob

# Load all CSVs into a single Pandas DataFrame
csv_files = glob.glob('C:/Users/USER/Downloads/N50 Historical Data/*.csv')  # Specify the folder containing the CSVs
dfs = []

for file in csv_files:
    df = pd.read_csv(file)
    dfs.append(df)

# Combine all data into one DataFrame
data = pd.concat(dfs, ignore_index=True)

# Convert the 'Date' column to datetime
data['Date'] = pd.to_datetime(data['Date  '])

# Sort data by Symbol and Date to ensure the next day's data is easy to access
data = data.sort_values(by=['Symbol  ', 'Date  '])
# Convert 'Open Price' and 'Close Price' columns to numeric, forcing errors to NaN
data['Open Price  '] = pd.to_numeric(data['Open Price  '], errors='coerce')
data['Close Price  '] = pd.to_numeric(data['Close Price  '], errors='coerce')
# Calculate the percentage movement for each stock
data['% Movement'] = ((data['Close Price  '] - data['Open Price  ']) / data['Open Price  ']) * 100

# Find the minimum and maximum dates
min_date = data['Date'].min()
max_date = data['Date'].max()

# Empty list to store results
results = []

# Get the list of unique dates to loop through
unique_dates = data['Date'].unique()

# Loop over each date
for i, date in enumerate(unique_dates[:-1]):  # Exclude the last date as we need the next trading day
    # Filter the data for the current date
    current_data = data[data['Date'] == date]

    # Find the stock with the highest % movement for the current date
    highest_movement_stock = current_data.loc[current_data['% Movement'].idxmax()]

    # Get the next trading day's data for this stock
    next_day_data = data[(data['Symbol  '] == highest_movement_stock['Symbol  ']) & (data['Date'] == unique_dates[i + 1])]

    if not next_day_data.empty:
        next_day_movement = ((next_day_data['Close Price  '].values[0] - next_day_data['Open Price  '].values[0]) / next_day_data['Open Price  '].values[0]) * 100
        open_equal_low = next_day_data['Open Price  '].values[0] == next_day_data['Low Price  '].values[0]
        open_equal_high = next_day_data['Open Price  '].values[0] == next_day_data['High Price  '].values[0]

        # Append result for this stock
        results.append({
            'Stock': highest_movement_stock['Symbol  '],
            'Date': highest_movement_stock['Date'].strftime('%Y-%m-%d'),
            'Open': highest_movement_stock['Open Price  '],
            'Close': highest_movement_stock['Close Price  '],
            '% Movement': highest_movement_stock['% Movement'],
            'Next Date': next_day_data['Date'].values[0],
            'Next Date % Movement': next_day_movement,
            'Open=Low or Open=High': 'Open=Low' if open_equal_low else 'Open=High' if open_equal_high else 'None'
        })

# Create a DataFrame from the results
results_df = pd.DataFrame(results)

# Write the results to an Excel file
results_df.to_excel('C:/Users/USER/Downloads/N50 Historical Data/nifty50_stock_movement_analysis.xlsx', index=False)

print("Analysis complete and saved to 'nifty50_stock_movement_analysis.xlsx'.")

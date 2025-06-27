import pandas as pd
import os

# Get the Downloads path (Windows style)
downloads_path = os.path.join(os.environ["USERPROFILE"], "Downloads")
DIRECTORY = downloads_path
# Step 1: Load the CSV file containing trade data
df = pd.read_csv(DIRECTORY + "/orders (5).csv")
positions_df = pd.read_csv(DIRECTORY + "/positions (26).csv")

# Step 2: Convert 'Time' column to datetime format and sort trades by time
df['Time'] = pd.to_datetime(df['Time'])
df = df.sort_values(by='Time').reset_index(drop=True)

# Step 3: Extract Strike and Option Type (CE/PE) from the Instrument column
df['Strike'] = df['Instrument'].str.extract(r'(\d{4,5})(?=CE|PE)')[0]
df['OptionType'] = df['Instrument'].str.extract(r'(CE|PE)')

# Step 4: Only keep trades that are marked as 'COMPLETE'
df = df[df['Status'] == 'COMPLETE'].copy()

# Step 5: Initialize storage for matched short straddles and used trade indices
used_indices = set()  # To track trades already used in a pair
straddles = []  # To store short straddle trades

# Step 6: Identify short straddle trades (SELL CE + SELL PE with same strike and within 5 seconds)
for i, row1 in df.iterrows():
    if i in used_indices or row1['Type'] != 'SELL':
        continue  # Skip if already used or not a SELL trade

    for j, row2 in df.iterrows():
        if j <= i:
            continue

        # Check conditions for a short straddle
        if (
                j not in used_indices and
                row2['Type'] == 'SELL' and
                row1['Strike'] == row2['Strike'] and
                row1['OptionType'] != row2['OptionType'] and
                abs((row1['Time'] - row2['Time']).total_seconds()) <= 5
        ):
            # Try finding matching BUY trades for each SELL leg within 5 minutes (300 seconds)

            buy_leg1 = df[
                (df['Type'] == 'BUY') &
                (df['OptionType'] == row1['OptionType']) &
                (df['Strike'] == row1['Strike']) &
                (df['Time'] > row1['Time'])
                ]

            buy_leg2 = df[
                (df['Type'] == 'BUY') &
                (df['OptionType'] == row2['OptionType']) &
                (df['Strike'] == row2['Strike']) &
                (df['Time'] > row2['Time'])]

            # Only proceed if both BUY legs are found
            if not buy_leg1.empty and not buy_leg2.empty:
                buy1 = buy_leg1.iloc[0]
                buy2 = buy_leg2.iloc[0]

                # Assign correct legs
                call_leg = row1 if row1['OptionType'] == 'CE' else row2
                call_exit = buy1 if row1['OptionType'] == 'CE' else buy2
                put_leg = row2 if row2['OptionType'] == 'PE' else row1
                put_exit = buy2 if row2['OptionType'] == 'PE' else buy1

                # Store the short straddle trade in desired format
                straddles.append({
                    "TRADE ENTRY TIME": min(row1['Time'], row2['Time']).time(),
                    "TRADE EXIT TIME": max(buy1['Time'], buy2['Time']).time(),
                    "SELL CALL STRIKE": call_leg['Strike'],
                    "SELL CALL ENTRY PRICE": call_leg['Avg. price'],
                    "SELL CALL EXIT PRICE": call_exit['Avg. price'],
                    "SELL PUT STRIKE": put_leg['Strike'],
                    "SELL PUT ENTRY PRICE": put_leg['Avg. price'],
                    "SELL PUT EXIT PRICE": put_exit['Avg. price'],
                    "TRADE TYPE": 'SHORT STRADDLE'
                })

                # Mark all 4 trades as used
                used_indices.update({i, j, buy1.name, buy2.name})
                break  # Exit inner loop after match

# Step 7: Remove used trades to process remaining direction trades
df_remaining = df.drop(index=used_indices).copy()

# Step 8: Initialize storage for directional trades
directional = []
used_indices_dir = set()

# Step 9: Find matching BUY for each SELL or vice versa (same strike & option type)
for i, row1 in df_remaining.iterrows():
    if i in used_indices_dir:
        continue

    for j, row2 in df_remaining.iterrows():
        if j <= i:
            continue
        if (
                j not in used_indices_dir and
                row1['Strike'] == row2['Strike'] and
                row1['OptionType'] == row2['OptionType'] and
                row1['Type'] != row2['Type']
        ):
            # Determine which is entry and exit
            entry, exit = (row1, row2) if row1['Time'] < row2['Time'] else (row2, row1)

            trade_type = 'BUY' if entry['Type'] == 'BUY' else 'SELL'
            entry_price = entry['Avg. price']
            exit_price = exit['Avg. price']
            # Swap prices if trade_type is BUY to maintain entry < exit
            if trade_type == 'BUY':
                entry_price, exit_price = exit_price, entry_price

            # Save the trade based on option type
            if entry['OptionType'] == 'CE':
                directional.append({
                    "TRADE ENTRY TIME": entry['Time'].time(),
                    "TRADE EXIT TIME": exit['Time'].time(),
                    "SELL CALL STRIKE": entry['Strike'],
                    "SELL CALL ENTRY PRICE": entry_price,
                    "SELL CALL EXIT PRICE": exit_price,
                    "SELL PUT STRIKE": None,
                    "SELL PUT ENTRY PRICE": None,
                    "SELL PUT EXIT PRICE": None,
                    "TRADE TYPE": trade_type
                })
            else:
                directional.append({
                    "TRADE ENTRY TIME": entry['Time'].time(),
                    "TRADE EXIT TIME": exit['Time'].time(),
                    "SELL CALL STRIKE": None,
                    "SELL CALL ENTRY PRICE": None,
                    "SELL CALL EXIT PRICE": None,
                    "SELL PUT STRIKE": entry['Strike'],
                    "SELL CALL ENTRY PRICE": entry_price,
                    "SELL CALL EXIT PRICE": exit_price,
                    "TRADE TYPE": trade_type
                })

            # Mark both trades as used
            used_indices_dir.update({i, j})
            break


# Function to get expiry price for a given strike and option type
def get_expiry_price(strike, opt_type):
    if pd.isna(strike):
        return None
    return expiry_lookup.get((str(strike), opt_type), None)

positions_df['OptionType'] = positions_df['Instrument'].str.extract(r'(CE|PE)')
positions_df['Strike'] = positions_df['Instrument'].str.extract(r'(\d{4,5})(?=CE|PE)')[0]
positions_df['Strike'] = positions_df['Strike'].astype(str)
expiry_lookup = positions_df.set_index(['Strike', 'OptionType'])['LTP'].to_dict()

# Step 10: Combine straddle and directional trades into a single DataFrame
final_df = pd.DataFrame(straddles + directional)
final_df = final_df.sort_values(by="TRADE ENTRY TIME")
final_df["SELL CALL EXPIRY PRICE"] = final_df["SELL CALL STRIKE"].apply(lambda s: get_expiry_price(s, "CE"))
final_df["SELL PUT EXPIRY PRICE"] = final_df["SELL PUT STRIKE"].apply(lambda s: get_expiry_price(s, "PE"))

# Step 11: Export the final DataFrame to an Excel file
final_df.to_excel(DIRECTORY + "/parsed_trades.xlsx", index=False)
print("✅ Trades successfully written to 'parsed_trades.xlsx'")

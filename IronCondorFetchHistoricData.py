import pandas as pd
from tabulate import tabulate
import glob


if __name__ == '__main__':

    ####################################
    DRIVE = 'D:'
    files_pattern = DRIVE + '/BN OLD DATA/*.csv'
    underlying_file = DRIVE + '/NIFTY BANK_Historical_PR_01042017to29052024.csv'
    UNDERLYING = 'BN'
    ####################################

    # Get a list of all CSV files matching the pattern
    csv_files = glob.glob(files_pattern)

    # Initialize an empty list to store DataFrames
    dfs = []

    # Loop through each CSV file and read it into a DataFrame
    for file in csv_files:
        dfs.append(pd.read_csv(file))

    # Concatenate all DataFrames into a single DataFrame
    df = pd.concat(dfs, ignore_index=True)

    underlying_df = pd.read_csv(underlying_file)

    df['Strike Price  '] = df['Strike Price  '].round(0).astype(int)

    df_part = df.loc[(df['Date  '] == '29-May-2024') & (df['Expiry  '] == '29-May-2024') & (df['Strike Price  '] == 48800) & (df['Option type  '] == 'PE')]

    df_part = df_part[['Symbol  ', 'Date  ', 'Expiry  ', 'Option type  ', 'Strike Price  ', 'Open  ', 'High  ', 'Low  ', 'Close  ', 'Underlying Value  ']]

    print(tabulate(df_part, headers='keys', tablefmt='psql'))

    exit(0)



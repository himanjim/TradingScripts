import pandas as pd
from datetime import datetime, timedelta
import glob
import traceback

if __name__ == '__main__':
    DRIVE = 'C:/Users/himan/'

    # Get a list of all CSV files matching the pattern
    tower_files = glob.glob(DRIVE + 'Site Master_29042024/Site Master_29042024/*.xlsx')

    # Initialize an empty list to store DataFrames
    dfs = []

    # Loop through each CSV file and read it into a DataFrame
    for file in tower_files:
        dfs.append(pd.read_excel(file))

    # Concatenate all DataFrames into a single DataFrame
    towers_df = pd.concat(dfs, ignore_index=True)

    print(towers_df.shape)

    towers_df = towers_df[['TS Site ID', 'LSA', 'District', 'Latitude', 'Longitude', 'Sharing TSPS\'s']]

    ec_districts_df = pd.read_excel(DRIVE + 'Phase 5 to 7.xlsx')

    ec_districts_df['District'] = ec_districts_df['District'].str.lower()

    ec_districts_df.loc[ec_districts_df['District'] == 'khurda', 'District'] = 'Khorda'
    ec_districts_df.loc[ec_districts_df['District'] == 'sangroor ', 'District'] = 'Sangrur'
    ec_districts_df.loc[ec_districts_df['District'] == 'sangroor', 'District'] = 'Sangrur'
    ec_districts_df.loc[ec_districts_df['District'] == 'sas nagar', 'District'] = 'S.A.S Nagar'
    ec_districts_df.loc[ec_districts_df['District'] == 'ashok nagar', 'District'] = 'Ashoknagar'
    ec_districts_df.loc[ec_districts_df['District'] == 'kolkata north', 'District'] = 'Kolkata'
    ec_districts_df.loc[ec_districts_df['District'] == 'sahebganj', 'District'] = 'Sahibganj'
    ec_districts_df.loc[ec_districts_df['District'] == 'keonjhar', 'District'] = 'Kendujhar'
    ec_districts_df.loc[ec_districts_df['District'] == 'puri ', 'District'] = 'puri'
    ec_districts_df.loc[ec_districts_df['District'] == 'rup nagar', 'District'] = 'Rupnagar'
    ec_districts_df.loc[ec_districts_df['District'] == 'mumbai city - bhatapara', 'District'] = 'mumbai'
    ec_districts_df.loc[ec_districts_df['District'] == 'sant kabir nagar', 'District'] = 'Sant Kabirnagar'
    ec_districts_df.loc[ec_districts_df['District'] == 'sri muktsar sahib', 'District'] = 'muktsar'
    ec_districts_df.loc[ec_districts_df['District'] == 'srimuktsar sahib', 'District'] = 'muktsar'
    ec_districts_df.loc[ec_districts_df['District'] == 'kinnaur ', 'District'] = 'kinnaur'
    ec_districts_df.loc[ec_districts_df['District'] == 'jahanabad', 'District'] = 'Jehanabad'
    ec_districts_df.loc[ec_districts_df['District'] == 'paschim champaran', 'District'] = 'West Champaran'
    ec_districts_df.loc[ec_districts_df['District'] == 'north-west delhi', 'District'] = 'north west delhi'
    ec_districts_df.loc[ec_districts_df['District'] == 'rae bareli', 'District'] = 'Raebareli'
    ec_districts_df.loc[ec_districts_df['District'] == 'seraikella-kharsawan', 'District'] = 'Saraikela-Kharsawan'
    ec_districts_df.loc[ec_districts_df['District'] == 'seraikela kharsawan', 'District'] = 'Saraikela-Kharsawan'
    ec_districts_df.loc[ec_districts_df['District'] == 'purvi champaran', 'District'] = 'East Champaran'
    ec_districts_df.loc[ec_districts_df['District'] == 'solan ', 'District'] = 'solan'
    ec_districts_df.loc[ec_districts_df['District'] == 'central  delhi', 'District'] = 'central delhi'
    ec_districts_df.loc[ec_districts_df['District'] == 'bhadrak ', 'District'] = 'bhadrak'
    ec_districts_df.loc[ec_districts_df['District'] == 'mumbai suburban', 'District'] = 'mumbai'
    ec_districts_df.loc[ec_districts_df['District'] == 'mumbai city', 'District'] = 'mumbai'
    ec_districts_df.loc[ec_districts_df['District'] == 'shimla ', 'District'] = 'shimla'
    ec_districts_df.loc[ec_districts_df['District'] == 'north-east delhi', 'District'] = 'north east delhi'
    ec_districts_df.loc[ec_districts_df['District'] == 'shrawasti', 'District'] = 'Shravasti'
    ec_districts_df.loc[ec_districts_df['District'] == 'kaimur (bhabua)', 'District'] = 'kaimur'
    ec_districts_df.loc[ec_districts_df['District'] == 'chitrakoot', 'District'] = 'Chitrkoot'
    ec_districts_df.loc[ec_districts_df['District'] == 'purba medinipur', 'District'] = 'East Midnapore'
    ec_districts_df.loc[ec_districts_df['District'] == 'purbo medinipur', 'District'] = 'East Midnapore'
    ec_districts_df.loc[ec_districts_df['District'] == 'hosiarpur', 'District'] = 'Hoshiarpur'
    ec_districts_df.loc[ec_districts_df['District'] == 'south-east delhi', 'District'] = 'south east delhi'
    ec_districts_df.loc[ec_districts_df['District'] == 'fatehgarh sahib ', 'District'] = 'fatehgarh sahib'
    ec_districts_df.loc[ec_districts_df['District'] == 'balasore', 'District'] = 'Baleswar'
    ec_districts_df.loc[ec_districts_df['District'] == 'gurgaon', 'District'] = 'Gurugram'
    ec_districts_df.loc[ec_districts_df['District'] == 'chandigarh', 'District'] = 'Chandigarh (U.T.)'
    ec_districts_df.loc[ec_districts_df['District'] == 'bolangir', 'District'] = 'Balangir'
    ec_districts_df.loc[ec_districts_df['District'] == 'bhatinda', 'District'] = 'Bathinda'
    ec_districts_df.loc[ec_districts_df['District'] == 'paschim medinipur', 'District'] = 'West Midnapore'
    ec_districts_df.loc[ec_districts_df['District'] == 'tarn-taran', 'District'] = 'Tarntaran'
    ec_districts_df.loc[ec_districts_df['District'] == 'tarn taran', 'District'] = 'Tarntaran'
    ec_districts_df.loc[ec_districts_df['District'] == 'purba bardhaman', 'District'] = 'Bardhaman (East)'
    ec_districts_df.loc[ec_districts_df['District'] == 'mewat', 'District'] = 'Nuh'
    ec_districts_df.loc[ec_districts_df['District'] == 'kushi nagar', 'District'] = 'Kushinagar'
    ec_districts_df.loc[ec_districts_df['District'] == 'sbs nagar', 'District'] = 'S.B.S Nagar'
    ec_districts_df.loc[ec_districts_df['District'] == 'east singhbhum', 'District'] = 'East-Singhbhum'
    ec_districts_df.loc[ec_districts_df['District'] == 'cuttack ', 'District'] = 'cuttack'
    ec_districts_df.loc[ec_districts_df['District'] == 'firozpur ', 'District'] = 'Ferozepur'
    ec_districts_df.loc[ec_districts_df['District'] == 'firozpur', 'District'] = 'Ferozepur'
    ec_districts_df.loc[ec_districts_df['District'] == 'south-west delhi ', 'District'] = 'south west delhi'
    ec_districts_df.loc[ec_districts_df['District'] == 'south-west delhi', 'District'] = 'south west delhi'






    ec_districts_df['District'] = ec_districts_df['District'].str.lower()
    towers_df['District'] = towers_df['District'].str.lower()

    ec_districts = ec_districts_df['District'].unique()

    print(ec_districts_df.shape)

    print(ec_districts.shape)

    # Converting DataFrame column and list to sets
    ec_district_set = set(ec_districts)
    tower_districts_set = set(towers_df['District'])

    # Finding elements not in the DataFrame column
    # not_in_column = list(ec_district_set - tower_districts_set)
    print(list(ec_district_set - tower_districts_set))

    # exit(0)

    total_fetched_towers = 0

    for ec_district in ec_districts:
        towers_df_part = towers_df.loc[towers_df['District'] == ec_district]

        if len(towers_df_part) > 0:
            total_fetched_towers += len(towers_df_part)
            towers_df_part.to_excel(DRIVE + '/EC_DistrictWise_Towers/' + ec_district + '_towers.xlsx', index=False)
        else:
            print('No results for district:', ec_district)

    print('Total Towers fetched:', total_fetched_towers)

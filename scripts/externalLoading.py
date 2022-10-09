from io import BytesIO
import os
import requests, zipfile
import pandas as pd

def mkdir(directory):
    """
    Creates a directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)


def merge_tables(table_names, index):
    """
    Reads in specified tables in table_names and joins on post code. Returns the resulting table.
    """
    input_dir = '../data/raw/census/2021 Census GCP Postal Areas for AUS/'
    if index == len(table_names) - 1:
        return pd.read_csv(input_dir + table_names[index], index_col = False)
    else:
        return pd.read_csv(
            input_dir + table_names[index], index_col = False
        ).merge(
            merge_tables(table_names, index + 1),
            on = 'POA_CODE_2021'
        )

def get_census_df(table_names):
    """
    Merges and alters the table to match census data.
    """
    df = merge_tables(table_names, 0)
    df.columns = df.columns.str.lower()
    df['poa'] = df['poa_code_2021'].apply(lambda x : x[-4:])
    return df.drop('poa_code_2021', axis = 1)

def external_loading():
    """
    Loads in external ABS data and postcode data.

    Reads
    -----------------------
    - None (fetches from url)

    Writes
    -----------------------
    - ../data/curated/census/age_data.parquet
    - ../data/raw/postcodes/abs_postal_areas.zip
    - ../data/raw/postcodes/postcodes.csv
    - ../data/raw/census/2021 Census GCP Postal Areas for AUS

    """

    output_dir = '../data/raw/census/'
    mkdir(output_dir)

    response = requests.get("https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_POA_for_AUS_short-header.zip")
    file = zipfile.ZipFile(BytesIO(response.content))
    file.extractall(output_dir)

    age_df = get_census_df([f'2021Census_{code}_AUST_POA.csv' for code in ['G04A', 'G04B']])
    age_df.to_parquet('../data/curated/census/age_data.parquet', index = False)

    output_dir = '../data/raw/postcodes/'
    mkdir(output_dir)

    # Download and extract zip file
    response = requests.get('https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/digital-boundary-files/POA_2021_AUST_GDA2020_SHP.zip')
    with open(output_dir + 'abs_postal_areas.zip', 'wb') as f:
        f.write(response.content)

    output_dir = '../data/raw/postcodes/'
    mkdir(output_dir)
    response = requests.get('https://www.matthewproctor.com/Content/postcodes/australian_postcodes.csv')

    with open(output_dir + 'postcodes.csv', 'wb') as f:
        f.write(response.content)
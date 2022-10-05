""" ETL Script

MAST30034 Project 2 - Buy Now Pay Later project

This script performs all data extraction and preprocessing.

"""

# Imports
from io import BytesIO
import os
import re
import requests, zipfile
import pandas as pd
import geopandas as gpd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("MAST30034 Project 2")
    .config("spark.driver.memory", '4g')
    .config("spark.executor.memory", '8g')
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.repl.eagerEval.enabled", True)
    .config("spark.sql.parquet.cacheMetadata", "true")
    .getOrCreate()
)

def consumer_preprocessing():
    """
    Processes consumer data.

    Reads
    -----------------------
    - ../data/tables/tbl_consumer.csv (source customer data)

    Writes
    -----------------------
    - ../data/curated/cleaned_consumers.parquet

    """
    sdf_consumer = spark.read.csv('../data/tables/tbl_consumer.csv', sep='|', header=True)
    sdf_consumer = sdf_consumer.withColumn('postcode', F.format_string("%04d", F.col('postcode').cast('int')))

    VIC_sdf = sdf_consumer.where((F.col('state') == 'VIC' ))
    NSW_sdf = sdf_consumer.where((F.col('state') == 'NSW' ))
    QLD_sdf = sdf_consumer.where((F.col('state') == 'QLD' ))
    WA_sdf = sdf_consumer.where((F.col('state') == 'WA' ))
    TAS_sdf = sdf_consumer.where((F.col('state') == 'TAS' ))
    SA_sdf = sdf_consumer.where((F.col('state') == 'SA' ))
    ACT_sdf = sdf_consumer.where((F.col('state') == 'ACT' ))
    NT_sdf = sdf_consumer.where((F.col('state') == 'NT' ))

    VIC_sdf = VIC_sdf.where(
        ((F.col('postcode') >=  '3000') & (F.col('postcode') <= '3999'))
        | ((F.col('postcode') >=  '8000') & (F.col('postcode') <= '8999'))
    )
    NT_sdf = NT_sdf.where(
        (F.col('postcode') >=  '0800') & (F.col('postcode') <= '0999')
    )
    TAS_sdf = TAS_sdf.where(
        (F.col('postcode') >=  '7000') & (F.col('postcode') <= '7999')
    )
    WA_sdf = WA_sdf.where(
        ((F.col('postcode') >=  '6000') & (F.col('postcode') <= '6797'))
        | ((F.col('postcode') >=  '6800') & (F.col('postcode') <= '6999'))
    )
    SA_sdf = SA_sdf.where(
        (F.col('postcode') >=  '5000') & (F.col('postcode') <= '5999')
    )
    QLD_sdf = QLD_sdf.where(
        ((F.col('postcode') >=  '4000') & (F.col('postcode') <= '4999'))
        | ((F.col('postcode') >=  '9000') & (F.col('postcode') <= '9999'))
    )
    NSW_sdf = NSW_sdf.where(
        ((F.col('postcode') >=  '1000') & (F.col('postcode') <= '2599'))
        | ((F.col('postcode') >=  '2619') & (F.col('postcode') <= '2899'))
        | ((F.col('postcode') >=  '2921') & (F.col('postcode') <= '2999'))
    )
    ACT_sdf = ACT_sdf.where(
        ((F.col('postcode') >=  '0200') & (F.col('postcode') <= '0300'))
        | ((F.col('postcode') >=  '2600') & (F.col('postcode') <= '2618'))
        | ((F.col('postcode') >=  '2900') & (F.col('postcode') <= '2920'))
    )

    cleaned_consumer = VIC_sdf.union(NSW_sdf) \
        .union(QLD_sdf) \
        .union(WA_sdf) \
        .union(TAS_sdf) \
        .union(SA_sdf) \
        .union(ACT_sdf) \
        .union(NT_sdf)

    cleaned_consumer.write.mode('overwrite').parquet('../data/curated/cleaned_consumers.parquet')

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

def separate_tags(row):
    """
    Separates merchant tags into 3 features (split by () or []).
    """
    features = re.findall(r'[\(|\[][\(|\[](.*)[\)|\]],\s[\(|\[](.*)[\)|\]],\s[\(|\[](.*)[\)|\]][\)|\]]', row['tags'])
    row['category'] = features[0][0]
    row['revenue_level'] = features[0][1]
    row['feature_3'] = features[0][2]
    return row


def get_take_rate(rate):
    """
    Convert the take rate feature to float type.
    """
    feature = re.findall(r'take rate: (\d+\.\d+)', rate)
    return float(feature[0])

def merchant_preprocessing():
    """
    Processes merchant data.

    Reads
    -----------------------
    - ../data/tables/tbl_merchants.parquet (source merchant data)

    Writes
    -----------------------
    - ../data/curated/merchants_df.parquet

    """
    merchants_df = pd.read_parquet('../data/tables/tbl_merchants.parquet').reset_index()
    merchants_df = merchants_df.apply(separate_tags, axis = 1)

    merchants_df['take_rate'] = merchants_df['feature_3'].apply(get_take_rate)
    merchants_df = merchants_df.drop(columns = ['tags', 'feature_3'])

    merchants_df['category'] = merchants_df['category'].str.lower()
    merchants_df['category'] = merchants_df['category'].replace('  ', ' ', regex = True)

    merchants_df.to_parquet('../data/curated/merchants_df.parquet')

def postcode_to_str(col):
    """
    Engineers postcode columns to be in the right format.
    """
    return col.astype(str).str.zfill(4)

def external_preprocessing():
    """
    Processes external data.

    Reads
    -----------------------
    - ../data/raw/postcodes/abs_postal_areas.zip
    - ../data/curated/cleaned_consumers.parquet
    - ../data/raw/postcodes/postcodes.csv

    Writes
    -----------------------
    - ../data/curated/census/postcode_poa.parquet
    """
    # Read in data
    postal_areas_gdf = gpd.read_file('../data/raw/postcodes/abs_postal_areas.zip')
    consumer_details_df = pd.read_parquet('../data/curated/cleaned_consumers.parquet')
    postcode_df = pd.read_csv('../data/raw/postcodes/postcodes.csv').drop_duplicates('postcode')

    consumer_details_df['postcode'] = postcode_to_str(consumer_details_df['postcode'])
    postcode_df['postcode'] = postcode_to_str(postcode_df['postcode'])

    # Convert postcode dataframe to geodataframe
    postcode_gdf = gpd.GeoDataFrame(
        postcode_df, geometry=gpd.points_from_xy(postcode_df['long'], postcode_df['lat'])
    )
    postcode_gdf.crs = postal_areas_gdf.crs

    # Get list of postcodes not listed as abs postal areas and filter geodataframe to just these postcodes
    unmapped = consumer_details_df[~consumer_details_df['postcode'].astype(str).str.zfill(4).isin(postal_areas_gdf['POA_CODE21'])]['postcode'].unique()
    postcodes_gdf = postcode_gdf[postcode_gdf['postcode'].isin(unmapped)]

    # Spatially join unmapped postcodes and abs postal areas
    postcode_poa_gdf = postcodes_gdf.sjoin(postal_areas_gdf, how = 'inner')

    # Remove and rename columns 
    postcode_poa_df = postcode_poa_gdf[['postcode', 'POA_CODE21']]
    postcode_poa_df = postcode_poa_df.rename(columns = {'POA_CODE21' : 'poa'})

    # Combine abs mapped postcodes with unmapped postcodes
    postcode_poa_df = pd.concat([postcode_poa_df, postal_areas_gdf[['POA_CODE21', 'POA_CODE21']].set_axis(['postcode', 'poa'], axis = 1)], ignore_index = True).reset_index(drop = True)

    output_dir = '../data/curated/census/'
    mkdir(output_dir)

    postcode_poa_df.to_parquet(output_dir + 'postcode_poa.parquet', index = False)

def main():
    """
    Main controller.
    """
    consumer_preprocessing()
    external_loading()
    merchant_preprocessing()
    external_preprocessing()

if __name__ == '__main__':
    main()

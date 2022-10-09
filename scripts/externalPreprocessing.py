import os
import pandas as pd
import geopandas as gpd


def mkdir(directory):
    """
    Creates a directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)

def postcode_to_str(col):
    """
    Engineers postcode columns to be in the right format.
    """
    return col.astype(str).str.zfill(4)

def get_gender(variable):
    """
    Takes column name, returns the associated gender.
    """
    g = variable[-1]
    if g == 'm':
        return 'Male'
    if g == 'f':
        return 'Female'
    if g == 'p':
        return 'Person'

def get_prob_bnpl(row, aus_age_m, aus_prob_m, aus_age_f, aus_age):
    """
    Calculates the probability of a user using BNPL, using our metric (as in summary.ipynb)
    """
    # Constants for probability calculations
    prob_bnpl = 0.25
    prob_female_g_bnpl = 0.57
    prob_male_g_bnpl = 0.43
    prob_age_g_bnpl = pd.Series(data = {'age_18_24' : 0.26, 'age_25_34' : 0.35, 'age_35_44' : 0.2, 'age_45_54' : 0.12, 'age_55_64' : 0.04,'age_65+' : 0.01})

    if row.name[1] == 'Male':
        return (row*prob_age_g_bnpl*prob_male_g_bnpl*prob_bnpl/aus_age_m/aus_prob_m).sum()
    if row.name[1] == 'Female':
        return (row*prob_age_g_bnpl*prob_female_g_bnpl*prob_bnpl/aus_age_f/(1-aus_prob_m)).sum()
    if row.name[1] == 'Person':
        return (row*prob_age_g_bnpl*prob_bnpl/aus_age).sum()

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
    - ../data/curated/census/age_proportions.parquet
    - ../data/curated/demographic_weights.parquet
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

    age_df = pd.read_parquet('../data/curated/census/age_data.parquet')

    # Find the number of people per age group and gender
    cols = []
    for start_yr, end_yr in zip([18,25,35,45,55], [25,35,45,55,65]):
        for g in ['m', 'f', 'p']:
            col = f'age_{start_yr}_{end_yr - 1}_{g}'
            cols.append(col)
            age_df[col] = age_df.filter(regex = '|'.join([f'(age_yr_{x}_{g})'for x in range(start_yr,end_yr)])).astype(int).sum(axis = 1)


    for g in ['m', 'f', 'p']:
        col = f'age_65+_{g}'
        cols.append(col)
        age_df[col] = age_df.filter(regex = '|'.join([f'(age_yr_{x}_{x+4}_{g})'for x in range(65,100, 5)] + ['age_yr_100_yr_over_[mf]'])).astype(int).sum(axis = 1)

    # Rotate the data to group by POA, gender
    age_df = age_df[['poa'] + cols].melt(id_vars = 'poa')
    age_df['gender'] = age_df['variable'].apply(get_gender)
    age_df['variable'] = age_df['variable'].apply(lambda x : x[:-2])
    age_df = pd.pivot_table(age_df, values = 'value', index =['poa', 'gender'], columns='variable')

    # Calculate the total number of people per age group, for each gender
    idx = pd.IndexSlice
    aus_age = age_df.loc[idx[:, 'Person'], :].sum()
    aus_age_m = age_df.loc[idx[:, 'Male'], :].sum()
    aus_age_f = age_df.loc[idx[:, 'Female'], :].sum()

    # Calculates the proportion of males in Australia
    aus_prob_m = aus_age_m.sum()/(aus_age.sum())

    # Calculate the proportion of people age group, for each gender
    aus_age *= 1/aus_age.sum()
    aus_age_m *= 1/aus_age_m.sum()
    aus_age_f *= 1/aus_age_f.sum()

    # Find the proportion of people in each age group for a given POA and gender
    age_df = age_df.apply(lambda x : x/x.sum(), axis = 1)
    age_df = age_df.fillna(age_df.mean())

    age_df.reset_index().to_parquet('../data/curated/census/age_proportions.parquet')

    age_df['weight'] = age_df.apply(get_prob_bnpl, args=(aus_age_m, aus_prob_m, aus_age_f, aus_age), axis = 1)
    age_df = age_df[['weight']]

    age_df = age_df.reset_index()
    age_df['gender'] = age_df['gender'].apply(lambda x : 'Undisclosed' if x == 'Person' else x)

    age_df.to_parquet('../data/curated/demographic_weights.parquet', index = False)
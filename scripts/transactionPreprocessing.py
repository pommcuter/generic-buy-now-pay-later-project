import pandas as pd
from pyspark.sql import functions as F
from pyspark.ml.feature import Imputer

def transaction_preprocessing(spark):
    """
    Processes transaction data.

    Reads
    -----------------------
    - ../data/curated/demographic_weights.parquet
    - ../data/curated/cleaned_consumers.parquet
    - ../data/tables/consumer_user_details.parquet
    - ../data/curated/merchants.parquet
    - ../data/tables/consumer_fraud_probability.csv
    - ../data/tables/merchant_fraud_probability.csv
    - ../data/curated/census/postcode_poa.parquet
    - ../data/tables/transactions_20210228_20210827_snapshot
    - ../data/tables/transactions_20210828_20220227_snapshot
    - ../data/tables/transactions_20220228_20220828_snapshot

    Writes
    -----------------------
    - ../data/curated/weighted_monthly_transactions.parquet
    """

    # Read in all the data
    weights_sdf = spark.read.parquet(
        '../data/curated/demographic_weights.parquet'
    )
    consumers_sdf = spark.read.parquet(
        '../data/curated/cleaned_consumers.parquet'
    )

    user_details_sdf = spark.read.parquet(
        '../data/tables/consumer_user_details.parquet'
    )

    merchants_sdf = spark.read.parquet(
        '../data/curated/merchants.parquet'
    )

    consumer_fraud_prob_sdf = spark.read.option('header', True).csv(
        '../data/tables/consumer_fraud_probability.csv'
    ).withColumn(
        'order_datetime',
        F.to_date('order_datetime')
    ).withColumn(
        'fraud_probability',
        F.col('fraud_probability')/100
    ).withColumnRenamed(
        'fraud_probability',
        'consumer_fraud_prob'
    )

    merchant_fraud_prob_sdf = spark.read.option('header', True).csv(
        '../data/tables/merchant_fraud_probability.csv'
    ).withColumn(
        'order_datetime',
        F.to_date('order_datetime')
    ).withColumn(
        'fraud_probability',
        F.col('fraud_probability')/100
    ).withColumnRenamed(
        'fraud_probability',
        'merchant_fraud_prob'
    )

    postcode_poa_sdf = spark.read.parquet(
        '../data/curated/census/postcode_poa.parquet'
    )

    transactions_sdf = spark.read.parquet(
        '../data/tables/transactions_20210228_20210827_snapshot'
    ).union(
        spark.read.parquet(
            '../data/tables/transactions_20210828_20220227_snapshot'
        )
    ).union(
        spark.read.parquet(
            '../data/tables/transactions_20220228_20220828_snapshot'
        )
    )

    # Remove transactions outside valid BNPL range (based on our research)
    min_value = 10
    max_value = 10000

    transactions_sdf = transactions_sdf.where(
        (F.col('dollar_value') >= min_value)
        & (F.col('dollar_value') <= max_value)
    )

    # Join with consumer data (weights and fraud probabilities)
    transactions_sdf = transactions_sdf.join(
        user_details_sdf,
        on = 'user_id',
        how = 'left'
    ).join(
        consumers_sdf.select(
            'consumer_id', 'postcode', 'gender'
        ),
        on = 'consumer_id',
        how = 'left'
    ).join(
        postcode_poa_sdf,
        on = 'postcode',
        how = 'left'
    ).join(
        weights_sdf,
        on = ['poa' ,'gender'],
        how = 'left'
    ).join(
        consumer_fraud_prob_sdf,
        on = ['user_id', 'order_datetime'],
        how = 'left'
    ).na.fill(
        0, 
        subset = 'consumer_fraud_prob'
    )

    # Impute null weights with column average
    imputer = Imputer(inputCol = 'weight', outputCol='weight', strategy = 'mean')
    transactions_sdf = imputer.fit(transactions_sdf).transform(transactions_sdf)

    # Apply transaction level weighting
    transactions_sdf = transactions_sdf.withColumn(
        'weighted_dollar_value',
        F.col('dollar_value')*F.col('weight')*(1 - F.col('consumer_fraud_prob'))
    )

    # Group by merchant and day and join with merchant data
    transactions_sdf = transactions_sdf.groupby(
        'merchant_abn', 'order_datetime'
    ).agg(
        F.sum('weighted_dollar_value').alias('weighted_dollar_value'),
        F.sum('dollar_value').alias('dollar_value')
    ).join(
        merchants_sdf.select(
            'merchant_abn', 'take_rate'
        ),
        on = 'merchant_abn',
    ).join(
        merchant_fraud_prob_sdf,
        on = ['merchant_abn', 'order_datetime'],
        how = 'left'
    ).na.fill(
        0,
        subset = 'merchant_fraud_prob'
    )

    # Apply merchant level weighting
    transactions_sdf = transactions_sdf.withColumn(
        'weighted_dollar_value',
        F.col('weighted_dollar_value')*F.col('take_rate')*(1 - F.col('merchant_fraud_prob'))
    ).select(
        'merchant_abn', 'order_datetime', 'weighted_dollar_value', 'dollar_value'
    )

    transactions_df = transactions_sdf.toPandas()

    # Implement time steps for modelling
    time_steps = transactions_df[['order_datetime']].drop_duplicates()
    merchants = transactions_df[['merchant_abn']].drop_duplicates()
    time_steps['key'] = 1
    merchants['key'] = 1
    merchant_time_steps = pd.merge(
        merchants,
        time_steps,
        on = 'key'
    ).drop('key', axis = 1)

    transactions_df = pd.merge(
        transactions_df,
        merchant_time_steps,
        on = ['merchant_abn', 'order_datetime'],
        how = 'outer'
    ).fillna(0)

    transactions_df['order_datetime'] = pd.to_datetime(transactions_df['order_datetime'])
    transactions_df['month'] = transactions_df['order_datetime'].dt.month
    transactions_df['year'] = transactions_df['order_datetime'].dt.year

    transactions_df = transactions_df.groupby(
        ['merchant_abn', 'year', 'month']
    ).agg({'order_datetime' : 'min', 'weighted_dollar_value' : 'sum', 'dollar_value' : 'sum'}).reset_index()

    # Binary flags for nov and dec, due to their revenue spikes
    transactions_df['november'] = transactions_df['month'].apply(lambda x : 1 if x == 11 else 0)
    transactions_df['december'] = transactions_df['month'].apply(lambda x : 1 if x == 12 else 0)

    # Filtering for the training range
    transactions_df = transactions_df.query('order_datetime > "2021-02-28" and order_datetime < "2022-10-01"')

    transactions_df.to_parquet('../data/curated/weighted_monthly_transactions.parquet', index = False)
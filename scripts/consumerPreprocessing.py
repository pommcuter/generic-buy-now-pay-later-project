from pyspark.sql import functions as F


def consumer_preprocessing(spark):
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

    VIC_sdf = VIC_sdf.withColumn(
        'postcode',
        F.when((F.col('postcode') >= '3000') & (F.col('postcode') <= '3999')
        | ((F.col('postcode') >=  '8000') & (F.col('postcode') <= '8999')), F.col('postcode'))
    )
    NT_sdf = NT_sdf.withColumn(
        'postcode',
        F.when((F.col('postcode') >= '0800') & (F.col('postcode') <= '0999'), F.col('postcode'))
    )
    TAS_sdf = TAS_sdf.withColumn(
        'postcode',
        F.when((F.col('postcode') >=  '7000') & (F.col('postcode') <= '7999'), F.col('postcode'))
    )
    WA_sdf = WA_sdf.withColumn(
        'postcode',
        F.when((F.col('postcode') >=  '6000') & (F.col('postcode') <= '6797')
        | (F.col('postcode') >=  '6800') & (F.col('postcode') <= '6999'), F.col('postcode'))
    )
    SA_sdf = SA_sdf.withColumn(
        'postcode',
        F.when((F.col('postcode') >=  '5000') & (F.col('postcode') <= '5999'), F.col('postcode'))
    )
    QLD_sdf = QLD_sdf.withColumn(
        'postcode',
        F.when((F.col('postcode') >=  '4000') & (F.col('postcode') <= '4999')
        | (F.col('postcode') >=  '9000') & (F.col('postcode') <= '9999'), F.col('postcode'))
    )
    NSW_sdf = NSW_sdf.withColumn(
        'postcode',
        F.when((F.col('postcode') >=  '1000') & (F.col('postcode') <= '2599')
        | (F.col('postcode') >=  '2619') & (F.col('postcode') <= '2899')
        | (F.col('postcode') >=  '2921') & (F.col('postcode') <= '2999'), F.col('postcode'))
    )
    ACT_sdf = ACT_sdf.withColumn(
        'postcode',
        F.when((F.col('postcode') >=  '0200') & (F.col('postcode') <= '0300')
        | (F.col('postcode') >=  '2600') & (F.col('postcode') <= '2618')
        | (F.col('postcode') >=  '2900') & (F.col('postcode') <= '2920'), F.col('postcode'))
    )

    cleaned_consumer = VIC_sdf.union(NSW_sdf) \
        .union(QLD_sdf) \
        .union(WA_sdf) \
        .union(TAS_sdf) \
        .union(SA_sdf) \
        .union(ACT_sdf) \
        .union(NT_sdf)

    cleaned_consumer.write.mode('overwrite').parquet('../data/curated/cleaned_consumers.parquet')
""" ETL Script

MAST30034 Project 2 - Buy Now Pay Later project

This script performs all data extraction and preprocessing.

After running this script, data is used in modelling and visualisations.

"""

# Imports
import os
from pyspark.sql import SparkSession
from consumerPreprocessing import consumer_preprocessing
from externalLoading import external_loading
from merchantPreprocessing import merchant_preprocessing
from externalPreprocessing import external_preprocessing
from transactionPreprocessing import transaction_preprocessing


def main():
    """
    Main controller.
    """
    # Navigate to the scripts folder
    if os.path.exists("./scripts"):
        os.chdir("./scripts")

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
    consumer_preprocessing(spark)
    external_loading()
    merchant_preprocessing()
    external_preprocessing()
    transaction_preprocessing(spark)

if __name__ == '__main__':
    main()

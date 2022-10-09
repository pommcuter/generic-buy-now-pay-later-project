# Generic Buy Now, Pay Later Project

Group 40 for Applied Data Science attempted to rank top merchants for a generic buy now, pay later (BNPL) service. Our group members consisted of:

- William Fox
- Ben Georgakas
- Dash Park
- Dinuk Epa
- Aditya Ajit

## General appraoch

Datasets for transaction were released on a weekly basis. an ETL pipeline was built to clean the differnet types of data:

1. Consumer data set
2. External data set
3. Merchant data set
4. Transaction data set

After cleaning, the most important features which would impact consumers using BNPL services more often were considered. These included gender, age and location.

Finally, a model was built using a Temporal Fusion Transformer (TFT) as this model excells at predicting data with time series. This resulted in a ranking system based on heuristic values our group decided upon. Then, segments were created and not only was a top 100 ranking of merchants found, but also top 10 per segments.

## Notebooks and Scripts

- `loadExternalData.ipynb`: This notebook downloads and loads in the external data
- `consumerPreprocessing.ipynb`: This notebook preprocesses the consumer data
- `externalPreprocessing.ipynb`: This noteobok preprocesses the external data
- `merchantPreprocessing.ipynb`: This notebook preprocesses the merchant data
- `transactionPreprocessing.ipynb`: This notebook preprocesses the transactional data
- `ETL.py`: This script runs the entire pipeline for all datasets
- `distributionAnalysis.ipynb`: This notebook contains information regarding the distribution of merchants per revenue band
- `exploratoryAnalysis.ipynb`: This notebook contains analysis on the transactional data
- `fraudAnalysis.ipynb`: This notebook contains analysis on the fraud data

- `summary.ipynb`: This noteobok is a summary of all the group's findings and results
- `businessInformation.ipynb`: This notebook conatins further insights as a business perspective

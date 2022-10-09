# Generic Buy Now, Pay Later Project

Group 40 for Applied Data Science attempted to rank top merchants for a generic buy now, pay later (BNPL) service. Our group members consisted of:

- William Fox
- Ben Georgakas
- Dash Park
- Dinuk Epa
- Aditya Ajit

## General approach

Datasets for transactions were released on a weekly basis. An ETL pipeline was built to clean the different types of data:

1. Consumer data set
2. External data set
3. Merchant data set
4. Transaction data set

After cleaning, the most important features which would impact consumers using BNPL services more often were considered. These included gender and age (which was based on location). 

Finally, a model was built using linear regression, to predict weighted revenue over month long intervals. This resulted in a ranking system based on the weights our group decided upon. The top 100 merchants were found using this system, as well as the top 10 per market segment. Segments were created manually, based on merchant tags.

A more detailed look into our approach (and other steps along the way) can be found in `notebooks/summary.ipynb`.

## Scripts

- `ETL.py`: This script runs the entire pipeline for all datasets, calling the functions in the files below. It performs the preprocessing needed for modelling.
- `externalLoading.py`: This file contains a function that downloads and loads in the external data.
- `consumerPreprocessing.py`: This file contains a function that cleans the consumer data.
- `externalPreprocessing.py`: This file contains a function that preprocesses the external data and assigns weights to each age group and gender, for each postal area.
- `merchantPreprocessing.py`: This file contains a function that preprocesses the merchant data and assigns each merchant a market segment.
- `transactionPreprocessing.py`: This file contains a function that preprocesses the transactional data and assigns a weight to each transaction.

## Notebooks 

Preprocessing:
- `loadExternalData.ipynb`: This notebook performs the same function as the file `external_loading.py`.
- `consumerPreprocessing.ipynb`: This notebook performs the same function as the file of the same name.
- `externalPreprocessing.ipynb`: This notebook performs the same function as the file of the same name.
- `merchantPreprocessing.ipynb`: This notebook performs the same function as the file of the same name.
- `transactionPreprocessing.ipynb`: This notebook performs the same function as the file of the same name.

Exploratory analysis:
- `distributionAnalysis.ipynb`: This notebook contains information regarding the distribution of transactions per revenue band.
- `exploratoryAnalysis.ipynb`: This notebook contains exploratory analysis on the transactional data.
- `fraudAnalysis.ipynb`: This notebook contains analysis on the fraud data.

Summarised info:
- `summary.ipynb`: This noteobok is a summary of all the group's findings and results.
- `businessInformation.ipynb`: This notebook conatins further insights from a business perspective.

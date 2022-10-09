import re
import pandas as pd
from sklearn.preprocessing import LabelEncoder

def separate_tags(row):
    """
    Separates merchant tags into 3 features (split by () or []).
    """
    features = re.findall(r'[\(|\[][\(|\[](.*)[\)|\]],\s[\(|\[](.*)[\)|\]],\s[\(|\[](.*)[\)|\]][\)|\]]', row['tags'])
    row['feature_1'] = features[0][0]
    row['feature_2'] = features[0][1]
    row['feature_3'] = features[0][2]
    return row


def get_take_rate(val):
    """
    Convert the take rate feature to float type.
    """
    feature = re.findall(r'take rate: (\d+\.\d+)', val)
    return float(feature[0])/100

def merchant_preprocessing():
    """
    Processes merchant data.

    Reads
    -----------------------
    - ../data/tables/tbl_merchants.parquet (source merchant data)
    - ../data/curated/segments.csv

    Writes
    -----------------------
    - ../data/curated/merchants_df.parquet

    """
    merchants_df = pd.read_parquet('../data/tables/tbl_merchants.parquet').reset_index()
    # Check ABN length
    merchants_df = merchants_df[merchants_df['merchant_abn'].astype(str).str.len() == 11]

    merchants_df = merchants_df.apply(separate_tags, axis = 1)
    merchants_df['feature_3'] = merchants_df['feature_3'].apply(get_take_rate)
    merchants_df = merchants_df.rename(columns = {'feature_2': 'revenue_level', 'feature_1': 'category', 'feature_3' : 'take_rate'})
    merchants_df = merchants_df.drop(columns = 'tags')

    # 971 Unique instances of the unpreprocessed tags
    # Preprocessing 'category':
    merchants_df['category'] = merchants_df['category'].str.lower()
    merchants_df['category'] = merchants_df['category'].str.split(' and ')

    new_category = []
    for e in merchants_df['category']:
        new = ', '.join(e)
        new_category.append(new)
    merchants_df['category'] = new_category

    merchants_df['category'] = merchants_df['category'].str.split(',')

    new_category = []
    for e in merchants_df['category']: 
        new_e = []
        # Removing leading and trailing whitespace
        for i in e:
            word = i.lstrip().rstrip()
            word = re.sub(r'\s{2,}', ' ', word)
            new_e.append(word)

        # Removing empty options in list
        removeIndex = []
        for i in range(len(new_e)):
            if not new_e[i]:
                removeIndex.insert(0, i)
        for i in removeIndex:
            new_e.pop(i)     

        new_category.append(new_e)

    merchants_df['category'] = new_category
    merged_categories = []
    for e in merchants_df['category']:
        merged_categories.extend(e)

    # Encoding category as a number
    le = LabelEncoder()
    merchants_df['category_indexed'] = le.fit_transform(merchants_df['category'].astype(str))

    # Joining merchants to segments
    segments_df = pd.read_csv('../data/curated/segments.csv')
    segments_df = segments_df.set_index('category_indexed')

    merchants_segments_df = merchants_df.join(segments_df, on='category_indexed', how='left', lsuffix='_merchant', rsuffix='_segment')
    merchants_segments_df = merchants_segments_df[['merchant_abn','name','category_merchant','revenue_level','take_rate','category_indexed','segment']]
    merchants_segments_df.rename(columns = {'category_merchant':'category'}, inplace = True)

    merchants_segments_df.to_parquet('../data/curated/merchants.parquet', index = False)
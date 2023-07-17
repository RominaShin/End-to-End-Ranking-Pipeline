import pandas as pd
import numpy as np
from tqdm import tqdm
from pymongo import MongoClient
import pickle
import time

CONNECTION_STRING = "mongodb://root:rootpassword@mongodb_container:27017/?authSource=admin&readPreference=primary&ssl=false&directConnection=true"
client = MongoClient(CONNECTION_STRING)

 
db = client["LTR"]
cursor = db["actions"].find()


def get_feature_by_user(df):
    res = list()
    for i, v in tqdm(df.groupby('account_id')):
        res.append(
            (
                i,
                len(v['book_id']),
                (v['interaction'] == 1).sum(),
                (v['creation_date'].dt.dayofweek == 0).sum(),
                (v['creation_date'].dt.dayofweek == 1).sum(),
                (v['creation_date'].dt.dayofweek == 2).sum(),
                (v['creation_date'].dt.dayofweek == 3).sum(),
                (v['creation_date'].dt.dayofweek == 4).sum(),
                (v['creation_date'].dt.dayofweek == 5).sum(),
                (v['creation_date'].dt.dayofweek == 6).sum(),
                (v['creation_date'].dt.hour > 17).sum()

            )
        )

    res = pd.DataFrame(
        res,
        columns=[
            'account_id', 'books', 'interaction', 
            'monday_review_count_user', 'tuesday_review_count_user', 'wednesday_review_count_user', 'thursday_review_count_user',
            'friday_review_count_user', 'saturday_review_count_user', 'sunday_review_count_user','evening_reviews_by_user'
        ])
    return res



def get_feature_by_book(df):
    res = list()
    for i, v in tqdm(df.groupby('book_id')):
        res.append(
            (
                i,
                len(v['account_id']),
                (v['interaction'] == 1).sum(),
                (v['creation_date'].dt.dayofweek == 0).sum(),
                (v['creation_date'].dt.dayofweek == 1).sum(),
                (v['creation_date'].dt.dayofweek == 2).sum(),
                (v['creation_date'].dt.dayofweek == 3).sum(),
                (v['creation_date'].dt.dayofweek == 4).sum(),
                (v['creation_date'].dt.dayofweek == 5).sum(),
                (v['creation_date'].dt.dayofweek == 6).sum(),
                (v['creation_date'].dt.hour > 17).sum()
            )
        )
    
    res = pd.DataFrame(
        res,
        columns=[
            'book_id', 'user_count', 'interaction',
            'monday_review_count_book', 'tuesday_review_count_book', 'wednesday_review_count_book', 'thursday_review_count_book',
            'friday_review_count_book', 'saturday_review_count_book', 'sunday_review_count_book','evening_reviews_by_book'
        ])
    return res

def get_model_input(X_u, X_m, y, tgt_users):

    merged = pd.merge(X_u, y, on=['account_id'], how='inner')
    merged = pd.merge(X_m, merged, on=['book_id'], how='outer')
    merged = merged.query('account_id in @tgt_users')

    merged.fillna(0, inplace=True)
    features_cols = list(merged.drop(columns=['account_id', 'book_id', 'creation_date', 'interaction']).columns)

    query_list = merged['account_id'].value_counts()

    merged = merged.set_index(['account_id', 'book_id'])

    query_list = query_list.sort_index()

    merged.sort_index(inplace=True)

    df_x = merged[features_cols]

    df_y = merged['interaction']

    return df_x, df_y, query_list

def save_model_to_mongo(model):
    pkl_model = pickle.dumps(model)
    db['xgb_ranker'].insert_one({'model' : pkl_model, 'version': 0 , 'created_time': time.time()})

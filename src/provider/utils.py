import pandas as pd
from tqdm import tqdm
from pymongo import MongoClient


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
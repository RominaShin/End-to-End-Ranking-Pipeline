import joblib
from pymongo import MongoClient
import pandas as pd
import numpy as np
import pickle

CONNECTION_STRING = "mongodb://root:rootpassword@mongodb_container:27017/?authSource=admin&readPreference=primary&ssl=false&directConnection=true"
client = MongoClient(CONNECTION_STRING)
 
db = client["LTR"]

def load_model_from_mongodb(model_version):
    json_data = {}
    data = db["xgb_ranker"].find({'version' : model_version})
    for i in data:
        json_data = i
    pkl_model = json_data['model']
    return pkl_model

# model = joblib.load("../model/xgbrank.pkl")


def get_user(uid):
    users_features = pd.DataFrame(list(db["users_features"].find(
    {'account_id' : str(uid)} 
    )))
    if not users_features.empty : 
        users_features = users_features.drop("_id", axis =1)
        users_features["account_id"] = users_features["account_id"].astype(int)

    return users_features


def get_books(book_list_str):
    books_features = pd.DataFrame(list(db["books_features"].find(
    {'book_id' : {"$in": book_list_str}} 
    )))
    if not books_features.empty : 
        books_features = books_features.drop("_id", axis =1)
        books_features["book_id"] = books_features["book_id"].astype(int)

    return books_features


def get_interactions(uid, book_list_str):
    interactions = pd.DataFrame(list(db["actions"].find(
    {
        "$and":[
            {'book_id' : {"$in": book_list_str}}, 
            {'account_id' : str(uid)}
        ]
    }
     )))
    if not interactions.empty : 
        interactions = interactions.drop("_id", axis =1)
        interactions["account_id"] = interactions["account_id"].astype(int)
        interactions["book_id"] = interactions["book_id"].astype(int)
    else:
        interactions = pd.DataFrame.from_dict({
            "account_id": [1],
            "book_id" : [-1],
            "creation_date" : [-1], 
            "interaction" : [-1]

        })

    return interactions

def pre_process_request_data(user_features, books_features, interactions):

    merged = pd.merge(user_features, interactions, on=['account_id'], how='inner')
    merged = pd.merge(books_features, merged, on=['book_id'], how='outer')
    merged.fillna(0, inplace=True)
    features_cols = list(merged.drop(columns=['account_id', 'book_id', 'creation_date', 'interaction']).columns)

    query_list = merged['account_id'].value_counts()

    merged = merged.set_index(['account_id', 'book_id'])

    query_list = query_list.sort_index()

    merged.sort_index(inplace=True)

    df_x = merged[features_cols]

    return df_x.loc[0]


def predict_at_k(data, k):
    model = pickle.loads(load_model_from_mongodb(0))
    book_ids = list()
    ranks = list()

    pred = model.predict(data)
    book_id = np.array(data.reset_index()['book_id'])
    topK_index = np.argsort(pred)[::-1][:k]
    book_ids.extend(list(book_id[topK_index]))
    ranks.extend(list(range(1, len(topK_index)+1)))

    results = pd.DataFrame( {'book_id': book_ids, 'rank': ranks})

    return results

def get_results(predicted_results, book_list):
    results = []
    for i in book_list:
        if i in list(predicted_results["book_id"].values):
            results.append(
                {
                    "book_id": i, 
                    "rank": predicted_results[predicted_results["book_id"] == i]["rank"].iloc[0]
                }
            )
        else:
            results.append(
                {
                    "book_id": i, 
                    "rank": -1 
                }
            )
    return results


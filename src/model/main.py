from xgboost import XGBRanker
import pandas as pd
import numpy as np
from pymongo import MongoClient
import joblib
from utils import *


actions =  pd.DataFrame(list(cursor))
actions = actions.drop("_id", axis =1)
actions["CreationDate"] = pd.to_datetime(actions["CreationDate"])
actions.columns = ["account_id", "book_id", "creation_date"]
actions["interaction"] = 1


start = min(actions['creation_date'])
end = max(actions['creation_date'])
interval = end - start

train = actions[actions['creation_date'] <= (end - interval/3)]
test = actions[actions['creation_date'] >= (start + interval/3)]

train_y = train[train['creation_date'] >= (start + interval/3)]
train_X = train[train['creation_date'] < (start + interval/3)]
test_y = test[test['creation_date'] >= (end - interval/3)]
test_X = test[test['creation_date'] < (end - interval/3)]

train_tgt_user = set(train_X['account_id']) & set(train_y['account_id'])
test_tgt_user = set(test_X['account_id']) & set(test_y['account_id'])

train_X_u = get_feature_by_user(train_X)
test_X_u = get_feature_by_user(test_X)

train_X_b = get_feature_by_book(train_X)
test_X_b = get_feature_by_book(test_X)


X_train, y_train, query_list_train = get_model_input(train_X_u, train_X_b, train_y, train_tgt_user)
X_test, y_test, query_list_test = get_model_input(test_X_u, test_X_b, test_y, test_tgt_user)


model = XGBRanker(objective='rank:ndcg', n_estimators=100, random_state=0,learning_rate=0.1)
model.fit(
    X_train,
    y_train,
    group=query_list_train,
    eval_metric='ndcg',
    eval_set=[(X_test, y_test)],
    eval_group=[list(query_list_test)],
    verbose =10)

joblib.dump(model, 'xgbrank.pkl')
save_model_to_mongo(model)

print("model part is done")
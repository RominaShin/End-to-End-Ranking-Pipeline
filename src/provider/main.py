from utils import *


actions =  pd.DataFrame(list(cursor))
actions = actions.drop("_id", axis =1)
actions["CreationDate"] = pd.to_datetime(actions["CreationDate"])
actions.columns = ["account_id", "book_id", "creation_date"]
actions["interaction"] = 1


users_features = get_feature_by_user(actions)
db["users_features"].delete_many({}) 
db["users_features"].insert_many(users_features.to_dict('records'))

books_features = get_feature_by_book(actions)
db["books_features"].delete_many({}) 
db["books_features"].insert_many(books_features.to_dict('records'))

print("provider part is done")

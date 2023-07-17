from pymongo import MongoClient
from utils import *
import os

# client = MongoClient(os.environ.get('DB')) 
print('change detected')
client = MongoClient(CONNECTION_STRING) 
db = client["LTR"]

create_schema("books", books_schema, db)
extract_data_from_csv(BOOKS_CSV_PATH, "books", books_header, db)

create_schema("actions", actions_schema, db)
extract_data_from_csv(ACTIONS_CSV_PATH, "actions", actions_header, db)
print("extractor part is done")
from pymongo import MongoClient
import pandas as pd
from pymongo.errors import CollectionInvalid
import json
import csv


CONNECTION_STRING = "mongodb://root:rootpassword@mongodb_container:27017/?authSource=admin&readPreference=primary&ssl=false&directConnection=true"
ACTIONS_CSV_PATH = "src/extractor/data/actions.csv"
BOOKS_CSV_PATH = "src/extractor/data/book_data.csv"

actions_schema = {
    "AccountId": {
          "type": "int",
          "required" : True, 
          "description": "Account Id - Required."
        },
        "BookId": {
          "type": "int",
          "required" : True, 
          "description": "Book Id - Required."
        },
        "CreationDate": {
          "type": "date",
          "description": "interactionDate - Optional."
        }
}
books_schema = {
    "book_id" :{
        "type": "int",
        "required" : True, 
    } ,
    "title" :{
        "type": "string"
    } ,
    "description" :{
        "type": "string"
    } ,
    "price" :{
        "type": "double"
    } ,
    "number_of_page" :{
        "type": "int"
    } ,
    "PhysicalPrice" :{
        "type": "double"
    } ,
    "publishDate" :{
        "type": "string"
    } ,
    "rating" :{
        "type": "double"
    } ,
    "publisher" :{
        "type": "string"
    } ,
    "categories" :{
        "type": "string"
    } ,
    "author_name" :{
        "type": "string"
    } ,
    "translator_name" :{
        "type": "string"
    } ,
    "lang" :{
        "type": "string"
    } 
}

actions_header = [ 
        "AccountId", "BookId", "CreationDate"
    ]
books_header = [ 
        "book_id","title","description","price",
        "number_of_page","PhysicalPrice","publishDate",
        "rating","publisher","categories","author_name","translator_name","lang"
]
 

def create_schema(collection_name, schema, db):
    collection = collection_name
    validator = {'$jsonSchema': {'bsonType': 'object', 'properties': {}}}
    required = []


    for field_key in schema:
        field = schema[field_key]
        properties = {'bsonType': field['type']}
        minimum = field.get('minlength')

        if type(minimum) == int:
            properties['minimum'] = minimum

        if field.get('required') is True: required.append(field_key)

        validator['$jsonSchema']['properties'][field_key] = properties

    if len(required) > 0:
        validator['$jsonSchema']['required'] = required

    query = [('collMod', collection),
            ('validator', validator)]

    try:
        db.create_collection(collection)
    except CollectionInvalid:
        pass


def extract_data_from_csv(csv_file_name, collection_name, header, db):
    csvfile = open(csv_file_name, 'r',  encoding="utf8")
    reader = csv.DictReader( csvfile )
    for each in reader:
        row={}
        for field in header:
            try:
                row[field]= each[field]
            except Exception as err:
                print(err)
        db[collection_name].insert_one(row)




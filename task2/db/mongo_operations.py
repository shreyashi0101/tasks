from db.mongo_connection import get_mongo_client
from config import MONGO_DB_NAME, MONGO_COLLECTION_NAME

def get_collection():
    client = get_mongo_client()
    db = client[MONGO_DB_NAME]
    return db[MONGO_COLLECTION_NAME]

def insert_documents(documents):
    collection = get_collection()
    collection.insert_many(documents)

def count_documents():
    collection = get_collection()
    return collection.count_documents({})

def fetch_all_documents():
    collection = get_collection()
    return list(collection.find({}))

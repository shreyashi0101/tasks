import pandas as pd
from db.mongo_operations import  insert_documents, count_documents, fetch_all_documents
from config import MONGO_COLLECTION_NAME

def load_csv_to_mongodb(csv_file):
    df = pd.read_csv(csv_file)

    
    documents = []
    for _, row in df.iterrows():
        documents.append(list(row))

    insert_documents(documents)

    print(f"\n{len(documents)} records inserted into MongoDB collection '{MONGO_COLLECTION_NAME}'.")
    print(f"Verified count: {count_documents()}")


    print("\nDisplaying the inserted documents:")
    display_data()

def display_data():
    documents = fetch_all_documents()
    if documents:
        print("\nInserted Data:")
        for doc in documents:
            print(doc)
    else:
        print("\nNo data found in MongoDB.")

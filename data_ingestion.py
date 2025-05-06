import json
import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def upload_json_to_mongodb(json_path: str, db_name: str, collection_name: str):
    """
    Uploads data from a JSON file to a MongoDB collection.

    Parameters:
    - json_path: Path to the JSON file.
    - db_name: Name of the MongoDB database.
    - collection_name: Name of the MongoDB collection.
    """

    # Load MongoDB URI from environment variable
    mongo_uri = os.environ.get("MONGO_URI")
    print(mongo_uri)
    # Load JSON data
    with open(json_path, "r") as file:
        data = json.load(file)

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Insert data
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

    print(f"Data inserted into '{db_name}.{collection_name}' successfully.")


upload_json_to_mongodb("attendance.json", "ems", "dummy_attendences")

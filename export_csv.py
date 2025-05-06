import pymongo
import pandas as pd
from dotenv import load_dotenv
import os
# os.environ.clear()
load_dotenv()

# MongoDB connection URI (change if needed)
MONGO_URI = os.environ.get("MONGO_URI")
print(MONGO_URI)

# Database and collection names
DB_NAME = "ems"
COLLECTION_NAME = "attendances"

# Output CSV file name
OUTPUT_CSV = "attendances_export2.csv"

def export_mongo_to_csv():
    # Connect to MongoDB
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Fetch all documents
    documents = list(collection.find())

    if not documents:
        print("No documents found in the collection.")
        return

    # Remove MongoDB internal '_id' field (optional)
    for doc in documents:
        if '_id' in doc:
            doc['_id'] = str(doc['_id'])  # Keep _id as string
            # Or remove it: del doc['_id']

    # Convert list of dicts to DataFrame
    df = pd.DataFrame(documents)

    # Save DataFrame to CSV
    df.to_csv(OUTPUT_CSV, index=False)

    print(f"Exported {len(documents)} documents to '{OUTPUT_CSV}'.")

if __name__ == "__main__":
    export_mongo_to_csv()

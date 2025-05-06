import pymongo
import pandas as pd
import os
from dotenv import load_dotenv

# os.environ.clear()
load_dotenv()

# MongoDB connection URI (change if needed)
MONGO_URI = os.environ.get("MONGO_URI")
print(MONGO_URI)

DB_NAME = "ems"
ATTENDANCE_COLLECTION = "attendances"
USER_COLLECTION = "users"
OUTPUT_CSV = "attendances_export.csv"

def export_merged_data_to_csv():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Fetch documents
    attendances = list(db[ATTENDANCE_COLLECTION].find())
    users = list(db[USER_COLLECTION].find())

    if not attendances or not users:
        print("One or both collections are empty.")
        return

    # Create DataFrames
    attend_df = pd.DataFrame(attendances)
    users_df = pd.DataFrame(users)

    # Convert ObjectId to string for merging
    attend_df['user'] = attend_df['user'].astype(str)
    users_df['_id'] = users_df['_id'].astype(str)

    # Select only necessary user fields
    user_fields = ['_id', 'name', 'position', 'joiningDate', 'linkedInId', 'githubId', 'leaveDate', 'address']
    users_df = users_df[user_fields]

    # Merge: attendances.user -> users._id
    merged_df = pd.merge(attend_df, users_df, left_on='user', right_on='_id', how='left')
    print(merged_df.head())
    # Export to CSV
    merged_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Exported merged data to '{OUTPUT_CSV}' with {len(merged_df)} records.")

if __name__ == "__main__":
    export_merged_data_to_csv()

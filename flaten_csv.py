import pymongo
import pandas as pd
import json
import os
from dotenv import load_dotenv

# os.environ.clear()
load_dotenv()

# MongoDB connection URI (change if needed)
MONGO_URI = os.environ.get("MONGO_URI")

DB_NAME = "ems"
ATTENDANCE_COLLECTION = "attendances"
USER_COLLECTION = "users"
OUTPUT_CSV = "attendances_export_flat.csv"

def flatten_column(df, col_name, prefix):
    """Flatten a column if it contains dicts or lists"""
    if col_name not in df.columns:
        return df
    flattened = pd.json_normalize(df[col_name]).add_prefix(f"{prefix}_")
    df = df.drop(columns=[col_name])
    df = pd.concat([df, flattened], axis=1)
    return df

def export_fully_flattened_data_to_csv():
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

    # Convert ObjectId to string
    attend_df['_id'] = attend_df['_id'].astype(str)
    attend_df['user'] = attend_df['user'].astype(str)
    users_df['_id'] = users_df['_id'].astype(str)

    # Rename attendance ID
    attend_df = attend_df.rename(columns={'_id': 'attendance_id', 'user': 'employee_id'})

    # Flatten address if present
    if 'address' in users_df.columns and isinstance(users_df['address'].iloc[0], dict):
        users_df = flatten_column(users_df, 'address', 'employee_address')

    # Convert tags to string
    if 'tags' in users_df.columns:
        users_df['employee_tags'] = users_df['tags'].apply(lambda x: ";".join(map(str, x)) if isinstance(x, list) else str(x))
        users_df = users_df.drop(columns=['tags'])

    # Rename and prefix user fields to employee_
    users_df = users_df.rename(columns={
        '_id': 'employee_id',
        'name': 'employee_name',
        'position': 'employee_position',
        'joiningDate': 'employee_joiningDate',
        'linkedInId': 'employee_linkedInId',
        'githubId': 'employee_githubId',
        'leaveDate': 'employee_leaveDate',
        'email': 'employee_email',
        'password': 'employee_password',
        'aadhar': 'employee_aadhar',
        'panNo': 'employee_panNo',
        'isSuperUser': 'employee_isSuperUser',
        'isApproved': 'employee_isApproved',
        'invitedBy': 'employee_invitedBy',
        'image': 'employee_image',
        'address': 'employee_address',
        'phone': 'employee_phone',
        'dateOfBirth': 'employee_dateOfBirth',
        'gender': 'employee_gender',
        '__v': 'employee___v',
        'approvalDate': 'employee_approvalDate',
        'addedBy': 'employee_addedBy',
        'user_tags': 'employee_tags'
    })

    # Merge
    merged_df = pd.merge(attend_df, users_df, on='employee_id', how='left')

    # Flatten remaining dicts
    for col in merged_df.columns:
        if merged_df[col].apply(lambda x: isinstance(x, dict)).any():
            merged_df = flatten_column(merged_df, col, col)

    # Rename for clarity
    merged_df = merged_df.rename(columns={
        'attendance_totalWorkingHours': 'employee_total_workingTime',
        'status': 'employee_status',
        'breaks': 'breaks_taken',
        'overtimeHours': 'employee_overtimeHours',
        'checkIn.time': 'checkIn_time',
        'checkIn.timezone': 'checkIn_timezone',
        'checkOut.time': 'checkOut_time',
        'checkOut.timezone': 'checkOut_timezone'
    })

    # Add column: number of leaves taken (based on leaveDate list length)
    merged_df['employee_leaves_taken'] = merged_df['employee_leaveDate'].apply(
        lambda x: len(x) if isinstance(x, list) else 0
    )

    # Drop unnecessary columns
    columns_to_drop = [
        'employee_password', 'employee_aadhar', 'employee_panNo', 'employee_isSuperUser',
        'employee_isApproved', 'employee_image', 'employee_address', 'employee_linkedInId',
        'employee_phone', 'employee_githubId', 'employee_dateOfBirth', 'employee___v',
        'employee_approvalDate', 'employee_addedBy', 'employee_email',
        'checkIn_location_latitude', 'checkIn_location_longitude',
        'checkIn_timezone', 'checkOut_time', 'checkOut_location_latitude',
        'checkOut_location_longitude', 'checkOut_timezone', '__v', 'updatedAt',
        'checkIn_location.latitude', 'checkIn_location.longitude',
        'checkOut_location.latitude', 'checkOut_location.longitude'
    ]
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df.columns])

    # Export
    merged_df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Exported cleaned employee attendance data to '{OUTPUT_CSV}' with {len(merged_df)} records.")

if __name__ == "__main__":
    export_fully_flattened_data_to_csv()
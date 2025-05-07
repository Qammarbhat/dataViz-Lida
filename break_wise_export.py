import pymongo
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = "ems"
ATTENDANCE_COLLECTION = "attendances"
USER_COLLECTION = "users"
OUTPUT_CSV = "clean_attendances_export.csv"
client = pymongo.MongoClient(MONGO_URI)

def calculate_break_duration(breaks):
    if not isinstance(breaks, list):
        return 0, 0
    total_duration = 0
    for b in breaks:
        try:
            start = datetime.fromisoformat(b.get("start"))
            end = datetime.fromisoformat(b.get("end"))
            duration = (end - start).total_seconds() / 60  # in minutes
            total_duration += duration
        except Exception:
            continue
    return len(breaks), round(total_duration, 2)

def clean_datetime(value):
    if isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    return str(value)

def export_enriched_attendance_csv():
    db = client[DB_NAME]
    attendances = list(db[ATTENDANCE_COLLECTION].find())
    users = list(db[USER_COLLECTION].find())

    if not attendances or not users:
        print("One or both collections are empty.")
        return

    attend_df = pd.DataFrame(attendances)
    users_df = pd.DataFrame(users)

    # Prepare IDs
    attend_df['_id'] = attend_df['_id'].astype(str)
    attend_df['user'] = attend_df['user'].astype(str)
    users_df['_id'] = users_df['_id'].astype(str)

    # Rename columns
    attend_df = attend_df.rename(columns={'_id': 'attendance_id', 'user': 'employee_id'})

    # Break stats
    attend_df[['break_count', 'break_total_duration_minutes']] = attend_df['breaks'].apply(
        lambda b: pd.Series(calculate_break_duration(b))
    )

    # Convert date fields to string
    for col in ['checkIn', 'checkOut', 'createdAt']:
        if col in attend_df.columns:
            attend_df[col] = attend_df[col].apply(clean_datetime)

    # Remove location-related fields
    for field in list(attend_df.columns):
        if 'location' in field.lower():
            attend_df = attend_df.drop(columns=field)

    # Clean user DataFrame
    if 'tags' in users_df.columns:
        users_df['employee_tags'] = users_df['tags'].apply(lambda x: ";".join(map(str, x)) if isinstance(x, list) else "")
        users_df = users_df.drop(columns=['tags'])

    users_df.columns = ['employee_id' if col == '_id' else f"employee_{col}" for col in users_df.columns]

    # Merge
    merged_df = pd.merge(attend_df, users_df, on='employee_id', how='left')

    # Drop sensitive or irrelevant fields
    columns_to_drop = [
        'employee_password', 'employee_aadhar', 'employee_panNo', 'employee_isSuperUser',
        'employee_isApproved', 'employee_image', 'employee_address', 'employee_linkedInId',
        'employee_phone', 'employee_githubId', 'employee_dateOfBirth', 'employee___v',
        'employee_approvalDate', 'employee_addedBy', 'employee_email', '__v', 'updatedAt'
    ]
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df.columns])

    # Rename for clarity
    merged_df = merged_df.rename(columns={
        'totalWorkingHours': 'employee_total_workingTime',
        'overtimeHours': 'employee_overtimeHours',
        'status': 'employee_status',
        'breaks': 'breaks_taken'
    })

    # Add leave count
    if 'employee_leaveDate' in merged_df.columns:
        merged_df['employee_leaves_taken'] = merged_df['employee_leaveDate'].apply(lambda x: len(x) if isinstance(x, list) else 0)

    # Convert all fields to string
    merged_df = merged_df.astype(str)

    # Export
    merged_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Exported clean attendance data to '{OUTPUT_CSV}' with {len(merged_df)} records.")

if __name__ == "__main__":
    export_enriched_attendance_csv()

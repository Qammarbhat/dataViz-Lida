import pymongo
import pandas as pd

from dotenv import load_dotenv
import os
# os.environ.clear()
load_dotenv()

# MongoDB connection URI (change if needed)
MONGO_URI = os.environ.get("MONGO_URI")

# MongoDB Config (replace with your actual values)
DB_NAME = "ems"
ATTENDANCE_COLLECTION = "attendances"
USER_COLLECTION = "users"
ATTENDANCE_COLLECTION = "attendances"

def flatten_column(df, col_name, prefix):
    nested_df = pd.json_normalize(df[col_name])
    nested_df.columns = [f"{prefix}_{subcol}" for subcol in nested_df.columns]
    df = df.drop(columns=[col_name]).join(nested_df)
    return df

def export_employees_csv(db):
    users = list(db[USER_COLLECTION].find())
    if not users:
        print("No users found.")
        return

    users_df = pd.DataFrame(users)
    users_df['_id'] = users_df['_id'].astype(str)
    users_df = users_df.rename(columns={'_id': 'employee_id', 'name': 'employee_name',
                                        'position': 'employee_position', 'joiningDate': 'employee_joiningDate',
                                        'leaveDate': 'employee_leaveDate'})

    # Flatten address if exists
    if 'address' in users_df.columns and isinstance(users_df['address'].iloc[0], dict):
        users_df = flatten_column(users_df, 'address', 'employee_address')

    # Convert tags list to string
    if 'tags' in users_df.columns:
        users_df['employee_tags'] = users_df['tags'].apply(lambda x: ";".join(map(str, x)) if isinstance(x, list) else str(x))
        users_df = users_df.drop(columns=['tags'])

    # Count leaves
    users_df['employee_leaveCount'] = users_df['employee_leaveDate'].apply(lambda x: len(x) if isinstance(x, list) else 0)

    # Drop sensitive/unwanted fields
    drop_cols = ['password', 'aadhar', 'panNo', 'isSuperUser', 'isApproved', 'image', 'address',
                 'linkedInId', 'phone', 'githubId', 'dateOfBirth', '__v', 'approvalDate',
                 'addedBy', 'email', 'user_password', 'user_image']
    users_df = users_df.drop(columns=[col for col in drop_cols if col in users_df.columns])

    users_df.to_csv('employees.csv', index=False)
    print("Exported employees.csv")

def export_attendance_logs_csv(db):
    attendances = list(db[ATTENDANCE_COLLECTION].find())
    if not attendances:
        print("No attendance logs found.")
        return

    df = pd.DataFrame(attendances)
    df['_id'] = df['_id'].astype(str)
    df['user'] = df['user'].astype(str)
    df = df.rename(columns={
        '_id': 'attendance_id',
        'user': 'employee_id',
        'totalHours': 'employee_total_workingTime',
        'overtimeHours': 'employee_overtimeHours',
        'status': 'employee_status',
        'breaks': 'breaks_taken'
    })

    # Drop unwanted fields
    columns_to_drop = ['updatedAt']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    # Remove location/timezone fields if already handled separately
    for col in ['checkIn_location', 'checkOut_location']:
        if col in df.columns:
            df = df.drop(columns=col)

    df.to_csv('attendance_logs.csv', index=False)
    print("Exported attendance_logs.csv")

def export_geolocation_logs_csv(db):
    attendances = list(db[ATTENDANCE_COLLECTION].find())
    if not attendances:
        print("No geolocation logs found.")
        return

    df = pd.DataFrame(attendances)
    df['_id'] = df['_id'].astype(str)
    df['user'] = df['user'].astype(str)
    df = df.rename(columns={'_id': 'attendance_id', 'user': 'employee_id'})

    # Extract nested location and timezone
    geo_data = pd.DataFrame({
        'attendance_id': df['attendance_id'],
        'employee_id': df['employee_id'],
        'checkIn_latitude': df['checkIn'].apply(lambda x: x.get('location', {}).get('latitude') if isinstance(x, dict) else None),
        'checkIn_longitude': df['checkIn'].apply(lambda x: x.get('location', {}).get('longitude') if isinstance(x, dict) else None),
        'checkIn_timezone': df['checkIn'].apply(lambda x: x.get('timezone') if isinstance(x, dict) else None),
        'checkOut_time': df['checkOut'].apply(lambda x: x.get('time') if isinstance(x, dict) else None),
        'checkOut_latitude': df['checkOut'].apply(lambda x: x.get('location', {}).get('latitude') if isinstance(x, dict) else None),
        'checkOut_longitude': df['checkOut'].apply(lambda x: x.get('location', {}).get('longitude') if isinstance(x, dict) else None),
        'checkOut_timezone': df['checkOut'].apply(lambda x: x.get('timezone') if isinstance(x, dict) else None),
    })

    geo_data.to_csv('geolocation_logs.csv', index=False)
    print("Exported geolocation_logs.csv")

if __name__ == "__main__":
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    export_employees_csv(db)
    export_attendance_logs_csv(db)
    export_geolocation_logs_csv(db)

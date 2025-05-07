import pymongo
import pandas as pd
import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

from pymongo import MongoClient
from dotenv import load_dotenv
from redis_setup import save_csv_to_redis

# os.environ.clear()
load_dotenv()


# MongoDB connection URI (change if needed)
MONGO_URI = os.environ.get("MONGO_URI")
DATABASE_NAME = "ems"  # Replace with your database name
DB_NAME = "ems"
ATTENDANCE_COLLECTION = "attendances"
USER_COLLECTION = "users"
OUTPUT_CSV = "attendances_export.csv"
client = pymongo.MongoClient(MONGO_URI)

def export_merged_data_to_csv():
    
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

    # Include 'tags' from user fields
    user_fields = ['_id', 'name', 'position', 'joiningDate', 'linkedInId', 'githubId', 'leaveDate', 'address', 'tags']
    users_df = users_df[user_fields]

    # Merge: attendances.user -> users._id
    merged_df = pd.merge(attend_df, users_df, left_on='user', right_on='_id', how='left')

    # Drop unwanted columns
    columns_to_drop = ['_id_x', '_id_y', '__v']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df.columns])

    # Rename columns
    merged_df = merged_df.rename(columns={
        'user': 'unique_user_id',
        'checkIn': 'checkInTime',
        'totalHours': 'totalWorkingHours',
        'tags': 'department'
    })

    # Export to CSV
    merged_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Exported merged data to '{OUTPUT_CSV}' with {len(merged_df)} records.")

def remove_invalid_attendance_users(attendances_col, users_col):
    """
    Removes documents from the attendance collection where the `user` field
    references a non-existent user in the users collection.
    """
    # Get all valid user IDs
    valid_user_ids = set(user["_id"] for user in users_col.find({}, {"_id": 1}))

    # Find attendance records with invalid user references
    invalid_attendances = attendances_col.find({
        "user": {"$nin": list(valid_user_ids)}
    }, {"_id": 1})

    invalid_ids = [doc["_id"] for doc in invalid_attendances]

    if invalid_ids:
        result = attendances_col.delete_many({"_id": {"$in": invalid_ids}})
        print(f"✅ Deleted {result.deleted_count} invalid attendance records.")
    else:
        print("✅ No invalid attendance records found.")


def generate_employee_data():
    """
    Generates employee attendance data, similar to your previous script.

    Returns:
        tuple: (data: list of dicts, df: pandas.DataFrame)
            - data:  A list of dictionaries, where each dictionary represents an attendance record.
            - df:    A Pandas DataFrame containing the same data.
    """
    try:
        client = pymongo.MongoClient(MONGO_URI)
        db = client[DATABASE_NAME]
        attendances_collection = db[ATTENDANCE_COLLECTION]
        users_collection = db[USER_COLLECTION]

        # Fetch all attendance records
        attendances_data = list(attendances_collection.find())

        # Prepare a list to store attendance data
        attendance_data = []

        for attendance in attendances_data:
            user_id = attendance['user']
            user = users_collection.find_one({"_id": user_id})  # Find the user

            if user:
                user_name = user.get('name', 'N/A')
                user_joining_date = user.get('joiningDate', None)
                user_joining_date_str = user_joining_date.strftime('%Y-%m-%d') if isinstance(user_joining_date, datetime) else 'N/A'
                user_gender = user.get('gender', 'N/A')
                leave_count = len(user.get('leaveDate', []))
                leave_dates_raw = user.get('leaveDate', [])
                leave_dates_list = [
                    f"From {leave.get('startDate', 'N/A')} to {leave.get('leaveDate', 'N/A')}"
                    for leave in leave_dates_raw] if leave_dates_raw else ['N/A']
                employee_status = user.get('isApproved', 'N/A')
            else:
                user_name = 'N/A'
                user_joining_date_str = 'N/A'
                user_gender = 'N/A'
                leave_count = 0
                leave_dates_list = ['N/A']
                employee_status = 'N/A'

            check_in_check_out_count = 1  # Each attendance record
            working_days = {attendance['date'].strftime('%Y-%m-%d')}
            total_breaks = len(attendance.get('breaks', []))
            total_break_duration = sum(b.get('duration', 0) for b in attendance.get('breaks', []))
            break_start_end_times = []
            for b in attendance.get('breaks', []):
                start_time = b.get('startTime')
                end_time = b.get('endTime')
                start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(start_time, datetime) else 'N/A'
                end_time_str = end_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(end_time, datetime) else 'N/A'
                break_start_end_times.append(f"{start_time_str} - {end_time_str}")

            attendance_info = {
                "Employee ID": str(user_id),
                "Employee Name": user_name,
                "Employee Joining Date": user_joining_date_str,
                "Employee Gender": user_gender,
                "Employee Leave Count": leave_count,
                "Total Working Hours Recorded": attendance.get('totalHours', 0),
                "Total Check-in Check-out Records": check_in_check_out_count,
                "Working Days Count": len(working_days),
                "Employee Status": employee_status,
                "Leave Dates": ", ".join(leave_dates_list),
                "Total Break Count": total_breaks,
                "Total Break Duration (Hours)": total_break_duration,
                "Break Start - End Times": "; ".join(break_start_end_times) if break_start_end_times else 'N/A',
                "Leave Details": ", ".join(leave_dates_list)
            }
            attendance_data.append(attendance_info)

        # Create a Pandas DataFrame
        df = pd.DataFrame(attendance_data)
        return attendance_data, df

    except ConnectionError as e:
        print(f"Could not connect to MongoDB: {e}")
        return [], pd.DataFrame()  # Return empty data on error
    except Exception as e:
        print(f"An error occurred: {e}")
        return [], pd.DataFrame()
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    # export_merged_data_to_csv()

    # db = client[DB_NAME]
    # attendances = db[ATTENDANCE_COLLECTION]
    # users = db[USER_COLLECTION]
    # # Call the function
    # # remove_invalid_attendance_users(attendances, users)
    generate_employee_data()
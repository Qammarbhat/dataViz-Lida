import asyncio
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse
import pymongo
import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any, Optional
from bson import ObjectId
from dotenv import load_dotenv
import redis
from io import StringIO

load_dotenv()
app = FastAPI()

# MongoDB connection URI (change if needed)
MONGO_URI = os.environ.get("MONGO_URI")
DATABASE_NAME = "ems"  # Replace with your database name
DB_NAME = "ems"
ATTENDANCE_COLLECTION = "attendances"
USER_COLLECTION = "users"
OUTPUT_CSV = "attendances_export.csv"

# Initialize MongoDB client
client = pymongo.MongoClient(MONGO_URI)

# Initialize Redis client
redis_url = os.environ.get("REDIS_URL")
redis_client = redis.StrictRedis.from_url(redis_url, decode_responses=True)


def save_csv_to_redis(df: pd.DataFrame, key: str):
    """
    Saves a Pandas DataFrame to Redis as a CSV string.
    Args:
        df (pd.DataFrame): The DataFrame to save.
        key (str): The Redis key to use.
    """
    csv_data = df.to_csv(index=False, encoding='utf-8')
    redis_client.set(key, csv_data)
    print(f"Saved CSV to Redis with key: {key}")


def get_df_from_redis(key: str = "merged_attendance_csv") -> pd.DataFrame:
    """
    Retrieve and return a pandas DataFrame from Redis-stored CSV string.
    """
    csv_data = redis_client.get(key)
    if not csv_data:
        return pd.DataFrame()  # Return an empty DataFrame if no data in Redis

    try:
        df = pd.read_csv(StringIO(csv_data))
        return df
    except Exception as e:
        print(f"Error reading CSV from Redis: {e}")
        return pd.DataFrame() # Return empty DataFrame on error


def export_merged_data_to_csv():
    """
    Exports merged attendance and user data to a CSV file.
    """
    db = client[DB_NAME]
    attendances = list(db[ATTENDANCE_COLLECTION].find())
    users = list(db[USER_COLLECTION].find())

    if not attendances or not users:
        print("One or both collections are empty.")
        return

    attend_df = pd.DataFrame(attendances)
    users_df = pd.DataFrame(users)

    attend_df['user'] = attend_df['user'].astype(str)
    users_df['_id'] = users_df['_id'].astype(str)

    user_fields = ['_id', 'name', 'position', 'joiningDate', 'linkedInId', 'githubId', 'leaveDate', 'address', 'tags']
    users_df = users_df[user_fields]

    merged_df = pd.merge(attend_df, users_df, left_on='user', right_on='_id', how='left')

    columns_to_drop = ['_id_x', '_id_y', '__v']
    merged_df = merged_df.drop(columns=[col for col in columns_to_drop if col in merged_df.columns])

    merged_df = merged_df.rename(columns={
        'user': 'unique_user_id',
        'checkIn': 'checkInTime',
        'totalHours': 'totalWorkingHours',
        'tags': 'department'
    })

    merged_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Exported merged data to '{OUTPUT_CSV}' with {len(merged_df)} records.")



def remove_invalid_attendance_users(attendances_col, users_col):
    """
    Removes documents from the attendance collection where the `user` field
    references a non-existent user in the users collection.
    """
    valid_user_ids = set(user["_id"] for user in users_col.find({}, {"_id": 1}))

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
    Generates employee attendance data.  This function is used to get the data.
    Returns:
        tuple: (data: list of dicts, df: pandas.DataFrame)
            - data:  A list of dictionaries, where each dictionary represents an attendance record.
            - df:    A Pandas DataFrame containing the same data.
    """
    try:
        db = client[DATABASE_NAME]
        attendances_collection = db[ATTENDANCE_COLLECTION]
        users_collection = db[USER_COLLECTION]

        attendances_data = list(attendances_collection.find())

        attendance_data = []

        for attendance in attendances_data:
            user_id = attendance['user']
            user = users_collection.find_one({"_id": user_id})

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

            check_in_check_out_count = 1
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

        df = pd.DataFrame(attendance_data)
        return attendance_data, df

    except ConnectionError as e:
        print(f"Could not connect to MongoDB: {e}")
        return [], pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return [], pd.DataFrame()



# Your existing API endpoint
@app.get("/merged-attendance")
def fetch_data():
    merged_data, merged_df = generate_employee_data()
    return JSONResponse(content=merged_data)



# New WebSocket endpoint for real-time updates
@app.websocket("/ws/merged-attendance")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    db = client[DATABASE_NAME]
    try:
        attendance_collection = db[ATTENDANCE_COLLECTION]
        users_collection = db[USER_COLLECTION]

        # Initial data load and send
        merged_data, merged_df = generate_employee_data()
        if not merged_df.empty:
            save_csv_to_redis(merged_df, "employee_data_csv")
            await websocket.send_text(merged_df.to_csv(index=False, encoding='utf-8'))

        # Set up change streams
        change_stream = attendance_collection.watch()
        user_stream = users_collection.watch()

        while True:
            change = change_stream.try_next()
            user_change = user_stream.try_next()

            if change is not None or user_change is not None:
                merged_data, merged_df = generate_employee_data()
                if not merged_df.empty:
                    save_csv_to_redis(merged_df, "employee_data_csv")
                    await websocket.send_text(merged_df.to_csv(index=False, encoding='utf-8'))
            await asyncio.sleep(0.1) # Add a small delay to prevent excessive CPU usage

    except Exception as e:
        print(f"WebSocket Error: {e}")
    finally:
        print("WebSocket connection closed")



from lida import Manager, TextGenerationConfig, llm
lida = Manager(text_gen=llm("openai"))
textgen_config = TextGenerationConfig(n=1, temperature=0.5, model="gpt-3.5-turbo", use_cache=True)

@app.get("/summarize/")
async def summarize_redis_data(redis_key: str = "merged_attendance_csv"):
    try:
        df = get_df_from_redis(redis_key)
        if df.empty:
            return JSONResponse(status_code=400, content={"error": "No data found in Redis"})

        summary = lida.summarize(df, summary_method="default", textgen_config=textgen_config)
        goals = lida.goals(summary, n=2, textgen_config=textgen_config)

        return {
            "summary": summary,
            "goals": goals
        }
    except ValueError as ve:
        return JSONResponse(status_code=400, content={"error": str(ve)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})



@app.get("/visualize/")
async def visualize_data(question: str , redis_key: str = "merged_attendance_csv"):
    """
    Generate a visualization based on the user's question and data from Redis.
    """
    try:
        df = get_df_from_redis(redis_key)
        if df.empty:
            return JSONResponse(status_code=400, content={"error": "No data found in Redis"})
        summary = lida.summarize(df, summary_method="default", textgen_config=textgen_config)
        charts = lida.visualize(summary=summary, goal=question, textgen_config=textgen_config)

        if not charts:
            return JSONResponse(status_code=404, content={"error": "No chart could be generated for the given question."})

        chart = charts[0]
        return {
            "question": question,
            "chart_code": chart.code,
            "image_base64": chart.raster
        }
    except ValueError as ve:
        return JSONResponse(status_code=400, content={"error": str(ve)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Visualization failed: {e}"})

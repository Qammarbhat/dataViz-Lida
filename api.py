import datetime
from fastapi import FastAPI, WebSocket
from fastapi.responses import JSONResponse
import pymongo
import pandas as pd
import asyncio
from typing import Dict, Any
from bson import ObjectId  
from dotenv import load_dotenv
import os

from redis_setup import save_csv_to_redis

load_dotenv()
app = FastAPI()

MONGO_URI = os.environ.get("MONGO_URI")
print(MONGO_URI)

DB_NAME = "ems"
ATTENDANCE_COLLECTION = "attendances"
USER_COLLECTION = "users"
OUTPUT_CSV = "attendances_export.csv"




def convert_json_serializable(obj):
    if isinstance(obj, dict):
        return {k: convert_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_json_serializable(v) for v in obj]
    elif isinstance(obj, (pd.Timestamp, datetime.datetime)):
        return obj.isoformat()
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj

def get_merged_attendance_data():
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]

    attendances = list(db[ATTENDANCE_COLLECTION].find())
    users = list(db[USER_COLLECTION].find())

    if not attendances or not users:
        print("One or both collections are empty.")
        return [], None  # Ensure we return a tuple (list of records, DataFrame or None)

    attend_df = pd.DataFrame(attendances)
    users_df = pd.DataFrame(users)

    attend_df['user'] = attend_df['user'].astype(str)
    users_df['_id'] = users_df['_id'].astype(str)

    user_fields = ['_id', 'name', 'position', 'joiningDate', 'linkedInId', 'githubId', 'leaveDate', 'address']
    users_df = users_df[user_fields]

    merged_df = pd.merge(attend_df, users_df, left_on='user', right_on='_id', how='left')
    merged_df = merged_df.drop(["_id_x", "__v", "_id_y"], axis=1)

    merged_records = merged_df.to_dict(orient="records")
    converted_records = [convert_json_serializable(record) for record in merged_records]

    return converted_records, merged_df

# Your existing API endpoint
@app.get("/merged-attendance")
def fetch_data():
    merged_data = get_merged_attendance_data()  # already JSON-ready
    return JSONResponse(content=merged_data)

# New WebSocket endpoint for real-time updates
@app.websocket("/ws/merged-attendance")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    try:
        with db[ATTENDANCE_COLLECTION].watch() as attendance_stream, \
             db[USER_COLLECTION].watch() as user_stream:
            
            while True:
                attendance_change = attendance_stream.try_next()
                user_change = user_stream.try_next()
                
                if attendance_change is not None or user_change is not None:
                    merged_data, merged_df = get_merged_attendance_data()
                    
                    if merged_data:
                        # Convert DataFrame to CSV (as a string)
                        csv_data = merged_df.to_csv(index=False)
                        # Save CSV in Redis instead of a file
                        save_csv_to_redis(merged_df, "merged_attendance_csv")
                        
                        # Send CSV data as a string via WebSocket
                        await websocket.send_text(csv_data)
                
                await asyncio.sleep(0.1)
                
    except Exception as e:
        print(f"WebSocket Error: {e}")
    finally:
        client.close()
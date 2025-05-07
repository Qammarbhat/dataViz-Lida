import datetime
from fastapi import FastAPI, Query, WebSocket
from fastapi.responses import JSONResponse
import pymongo
import pandas as pd
import asyncio
from typing import Dict, Any
from bson import ObjectId  
from dotenv import load_dotenv
import os
from redis_setup import save_csv_to_redis, get_df_from_redis
from lida import Manager, TextGenerationConfig, llm
from export_csv_employee import generate_employee_data

load_dotenv()
app = FastAPI()
# MongoDB connection URI (change if needed)
MONGO_URI = os.environ.get("MONGO_URI")
DATABASE_NAME = "ems"  # Replace with your database name

ATTENDANCE_COLLECTION = "attendances"
USER_COLLECTION = "users"
OUTPUT_CSV = "attendances_export.csv"
client = pymongo.MongoClient(MONGO_URI)


# Your existing API endpoint
# Your existing API endpoint
@app.get("/merged-attendance")
def fetch_data():
    merged_data, merged_df = generate_employee_data()  # Get data from the function
    return JSONResponse(content=merged_data)  #  and return as JSON



# New WebSocket endpoint for real-time updates
@app.websocket("/ws/merged-attendance")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    try:
        attendance_collection = db[ATTENDANCE_COLLECTION]
        users_collection = db[USER_COLLECTION]  # Add this line
        # Send initial data on connection
        merged_data, merged_df = generate_employee_data()
        if not merged_df.empty:
            csv_data = merged_df.to_csv(index=False, encoding='utf-8')
            save_csv_to_redis(merged_df, "merged_attendance_csv")
            await websocket.send_text(csv_data)

        with attendance_collection.watch() as attendance_stream, users_collection.watch() as user_stream: # change here
            while True:
                change = await attendance_stream.__anext__()
                user_change = await user_stream.__anext__() # add user change
                if change or user_change: # change here
                    merged_data, merged_df = generate_employee_data()
                    if not merged_df.empty:
                        csv_data = merged_df.to_csv(index=False, encoding='utf-8')
                        save_csv_to_redis(merged_df, "merged_attendance_csv")
                        await websocket.send_text(csv_data)
    except Exception as e:
        print(f"WebSocket Error: {e}")
    finally:
        print("WebSocket connection closed")
        client.close()


# Initialize LIDA manager
lida = Manager(text_gen=llm("openai"))
textgen_config = TextGenerationConfig(n=1, temperature=0.5, model="gpt-3.5-turbo", use_cache=True)

@app.get("/summarize/")
async def summarize_redis_data():
    try:
        df = get_df_from_redis()

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
async def visualize_data(question: str = Query(...)):
    """
    Generate a visualization based on the user's question and data stored in Redis.
    """
    try:
        df = get_df_from_redis()
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
    
@app.get("/visualize/")
async def visualize_data(question: str = Query(...), redis_key: str = "merged_attendance_csv"):
    """
    Generate a visualization based on the user's question and data stored in Redis.
    """
    try:
        df = get_df_from_redis(redis_key)
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

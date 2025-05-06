from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse
import redis
import os
import pandas as pd
from dotenv import load_dotenv
from io import StringIO 

load_dotenv()
redis_url = os.environ.get("REDIS_URL")
redis_client = redis.StrictRedis.from_url(redis_url, decode_responses=True)

app = FastAPI()

import numpy as np

@app.get("/get-csv/")
async def get_csv_from_redis(
    redis_key: str = Query(default="merged_attendance_csv"), 
    format: str = Query(default="json", enum=["json", "csv"])
):
    csv_data = redis_client.get(redis_key)

    if not csv_data:
        return JSONResponse(status_code=404, content={"error": "Data not found in Redis for the given key"})

    if format == "json":
        try:
            df = pd.read_csv(StringIO(csv_data))

            # ✅ Replace problematic values for JSON compliance
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df = df.fillna("null")

            return df.to_dict(orient="records")
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": f"Failed to parse CSV: {e}"})
    else:
        return PlainTextResponse(csv_data, media_type="text/csv")

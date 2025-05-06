from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, PlainTextResponse
import redis
import os
import pandas as pd
from dotenv import load_dotenv
from io import StringIO 
from lida import Manager, TextGenerationConfig, llm

load_dotenv()
redis_url = os.environ.get("REDIS_URL")
redis_client = redis.StrictRedis.from_url(redis_url, decode_responses=True)

app = FastAPI()

import numpy as np

def get_df_from_redis(redis_client, redis_key: str) -> pd.DataFrame:
    """
    Retrieve and return a pandas DataFrame from Redis-stored CSV string.
    """
    csv_data = redis_client.get(redis_key)
    if not csv_data:
        raise ValueError(f"No data found in Redis under key: {redis_key}")
    
    try:
        df = pd.read_csv(StringIO(csv_data))
        return df
    except Exception as e:
        raise ValueError(f"Error reading CSV from Redis: {e}")

# Initialize LIDA manager
lida = Manager(text_gen=llm("openai"))
textgen_config = TextGenerationConfig(n=1, temperature=0.5, model="gpt-3.5-turbo", use_cache=True)

@app.get("/summarize/")
async def summarize_redis_data(redis_key: str = "merged_attendance_csv"):
    try:
        df = get_df_from_redis(redis_client, redis_key)

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
async def visualize_data(question: str = Query(...), redis_key: str = "merged_attendance_csv"):
    """
    Generate a visualization based on the user's question and data stored in Redis.
    """
    try:
        df = get_df_from_redis(redis_client, redis_key)
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


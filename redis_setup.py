import redis
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()
redis_url = os.environ.get("REDIS_URL")

# r = redis.Redis(
#     host='redis-15296.c263.us-east-1-2.ec2.redns.redis-cloud.com',
#     port=15296,
#     decode_responses=True,
#     username="default",
#     password="*******",
# )

redis_client = redis.StrictRedis.from_url(redis_url, decode_responses=True)

def save_csv_to_redis(df: pd.DataFrame, redis_key: str = "merged_attendance_csv"):
    """
    Save the DataFrame as CSV string in Redis.
    
    Args:
        df (pd.DataFrame): DataFrame to save.
        redis_key (str): Redis key under which to store the CSV.
    """
    try:
        csv_data = df.to_csv(index=False)
        redis_client.set(redis_key, csv_data)
        print(f"CSV saved to Redis under key '{redis_key}'")
    except Exception as e:
        print(f"Error saving CSV to Redis: {e}")
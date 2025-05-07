
from dotenv import load_dotenv
import os
import pandas as pd
from io import StringIO 
import redis
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


def get_df_from_redis() -> pd.DataFrame:
    """
    Retrieve and return a pandas DataFrame from Redis-stored CSV string.
    """
    csv_data = redis_client.get("merged_attendance_csv")
    if not csv_data:
        raise ValueError(f"No data found")
    
    try:
        df = pd.read_csv(StringIO(csv_data))
        return df
    except Exception as e:
        raise ValueError(f"Error reading CSV from Redis: {e}")
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import pandas as pd

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_conn"
# Loading into the RAW schema
TARGET_TABLE = "RAW.WEATHER_HISTORY"
URL = "https://archive-api.open-meteo.com/v1/archive"

# London Coordinates for 2023 history
PARAMS = {
    "latitude": 51.5074,
    "longitude": -0.1278,
    "start_date": "2023-01-01",
    "end_date": "2023-12-31",
    "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max",
    "timezone": "auto"
}

default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="open_meteo_weather_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@once", 
    catchup=False,
    default_args=default_args,
    description="Fetch 1 year of weather data from OpenMeteo and Full Refresh Snowflake",
) as dag:

    @task
    def extract_weather_data():
        """
        Fetches data from Open-Meteo API and returns a list of tuples.
        """
        response = requests.get(URL, params=PARAMS)
        response.raise_for_status()
        data = response.json()
        
        # The API returns data in a 'daily' dictionary with lists
        daily_data = data['daily']
        
        # Convert to DataFrame
        df = pd.DataFrame(daily_data)
        
        # Rename columns to match our Snowflake expectations
        df.rename(columns={
            'time': 'forecast_date',
            'temperature_2m_max': 'max_temp',
            'temperature_2m_min': 'min_temp',
            'precipitation_sum': 'precipitation',
            'wind_speed_10m_max': 'wind_speed'
        }, inplace=True)
        
        # Convert list of records to list of tuples for executemany
        rows = []
        for record in df.itertuples(index=False):
            rows.append((
                record.forecast_date,
                float(record.max_temp) if pd.notna(record.max_temp) else None,
                float(record.min_temp) if pd.notna(record.min_temp) else None,
                float(record.precipitation) if pd.notna(record.precipitation) else None,
                float(record.wind_speed) if pd.notna(record.wind_speed) else None
            ))
            
        return rows

    @task
    def load_to_snowflake(rows):
        """
        Performs a Full Refresh (Truncate + Insert) into Snowflake.
        """
        if not rows:
            print("No data found. Skipping load.")
            return

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        try:
            # 1. Create Table if not exists (DDL)
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                FORECAST_DATE DATE PRIMARY KEY,
                MAX_TEMP FLOAT,
                MIN_TEMP FLOAT,
                PRECIPITATION FLOAT,
                WIND_SPEED FLOAT
            )
            """
            cur.execute(create_sql)
            
            # 2. Transaction for Full Refresh (Idempotency)
            conn.autocommit = False
            cur.execute("BEGIN")
            
            # Truncate for full refresh
            cur.execute(f"TRUNCATE TABLE {TARGET_TABLE}")
            
            # Insert Data
            insert_sql = f"""
            INSERT INTO {TARGET_TABLE} (FORECAST_DATE, MAX_TEMP, MIN_TEMP, PRECIPITATION, WIND_SPEED)
            VALUES (%s, %s, %s, %s, %s)
            """
            cur.executemany(insert_sql, rows)
            
            cur.execute("COMMIT")
            print(f"Successfully loaded {len(rows)} rows into {TARGET_TABLE}")
            
        except Exception as e:
            cur.execute("ROLLBACK")
            print(f"Error occurred: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    # Define Task Dependencies
    weather_data = extract_weather_data()
    load_to_snowflake(weather_data)
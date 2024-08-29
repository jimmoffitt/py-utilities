import requests
import time
from datetime import datetime, timedelta
import schedule
import os
import json
import psycopg
from pathlib import Path
from dotenv import load_dotenv

# Get the directory of the current script
script_dir = Path(__file__).parent 

# Construct the path to .env.local within the script's directory
env_path = script_dir / '.env.local'
load_dotenv(dotenv_path=env_path)

# API and Database configuration
DB_CONNECTION_PARAMS = {
    'dbname': os.getenv("POSTGRES_DATABASE_NAME"),
    'user': os.getenv("POSTGRES_DATABASE_USER"),
    'password': os.getenv("POSTGRES_DATABASE_PASSWORD"),
    'host': os.getenv("POSTGRES_DATABASE_HOST"),
    'port': os.getenv("POSTGRES_DATABASE_PORT")  
}

SOURCE_KEY = os.getenv('TINYBIRD_SOURCE_TOKEN')

DATA_SOURCE_URL = "https://api.tinybird.co/v0/pipes/reportsv2.json"

# Some initial values... 
end_time = datetime.now()
start_time = '2024-08-15 21:54:27'
last_timestamp = None

def fetch_and_post_data():
    global last_timestamp

    try:
        headers_source = {"Authorization": f"Bearer {SOURCE_KEY}", "Content-Type": "application/json"}

        with psycopg.connect(**DB_CONNECTION_PARAMS) as conn:
            with conn.cursor() as cur:

                if not last_timestamp:
                    try:
                        query = "SELECT * FROM weather_reports ORDER BY timestamp DESC LIMIT 1;"
                        cur.execute(query)
                        result = cur.fetchone()
                        if result:
                            value = result[2]
                            start_time = value.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            print("WARNING: No rows found in the database. Using default start time.")
                    except psycopg.Error as e:
                        print(f"ERROR: Database error while fetching last timestamp: {e}")
                        return  # Exit the function on database error

                params = {}
                end_time = datetime.utcnow()
                params['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
                params['start_time'] = last_timestamp if last_timestamp else start_time
                
                try:
                    response = requests.get(DATA_SOURCE_URL, params=params, headers=headers_source, timeout=5)
                    response.raise_for_status()  # Raise an exception for bad status codes
                    data = response.json()['data']
                except requests.exceptions.RequestException as e:
                    print(f"ERROR: API request error: {e}")
                    return  # Exit the function on API error
                except (json.JSONDecodeError, KeyError) as e:
                    print(f"ERROR: Error parsing API response: {e}")
                    return

                if data:
                    last_timestamp = max(entry['timestamp'] for entry in data)

                    try:
                        for report in data:
                            print(f"INFO: Writing report: {report}")
                            cur.execute(
                                """
                                INSERT INTO weather_reports (timestamp, site_name, temp_f, clouds, description, humidity, precip, pressure, wind_dir, wind_speed)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT ("timestamp", site_name) DO UPDATE 
                                SET temp_f = EXCLUDED.temp_f, 
                                    clouds = EXCLUDED.clouds, 
                                    description = EXCLUDED.description, 
                                    humidity = EXCLUDED.humidity, 
                                    precip = EXCLUDED.precip, 
                                    pressure = EXCLUDED.pressure, 
                                    wind_dir = EXCLUDED.wind_dir, 
                                    wind_speed = EXCLUDED.wind_speed;
                                """,
                                (
                                    report['timestamp'],
                                    report['site_name'],
                                    report['temp_f'],
                                    report['clouds'],
                                    report['description'],
                                    report['humidity'],
                                    report['precip'],
                                    report['pressure'],
                                    report['wind_dir'],
                                    report['wind_speed'],
                                ),
                                prepare=False  # Disable prepared statements
                            )
                        conn.commit()
                        print("INFO: Processed all data...")
                    except psycopg.Error as e:
                        print(f"ERROR: Database error while inserting/updating data: {e}")
                        conn.rollback()  # Rollback changes on error

                else:
                    print("INFO: No new data found.")

    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")

# Schedule the task to run every minute
schedule.every(1).minutes.do(fetch_and_post_data)

# Run any pending tasks now
schedule.run_all()

while True:
    schedule.run_pending()
    time.sleep(1)

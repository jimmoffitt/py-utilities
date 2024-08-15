import requests
import time
import schedule
import os
import json
import psycopg
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Get the directory of the current script
script_dir = Path(__file__).parent 

# Construct the path to .env.local within the script's directory
env_path = script_dir / '.env.local'
load_dotenv(dotenv_path=env_path)

# API and Database configuration
DB_CONNECTION_PARAMS = {
    'dbname': os.getenv("DATABASE_NAME"),
    'user': os.getenv("DATABASE_USER"),
    'password': os.getenv("DATABASE_PASSWORD"),
    'host': os.getenv("DATABASE_HOST"),
    'port': os.getenv("DATABASE_PORT")  
}

SOURCE_KEY = os.getenv('TINYBIRD_SOURCE_TOKEN')

DATA_SOURCE_URL = "https://api.tinybird.co/v0/pipes/reportsv2.json"

end_time = datetime.now()
start_time = end_time - timedelta(days=1)
last_timestamp = None

def fetch_and_post_data():
    global last_timestamp

    params = {}
    
    end_time = datetime.now() # Always request data up until now?
    params['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
    
    if last_timestamp: # then start there.
        params['start_time'] = last_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    else: #otherwise, go back a day.     
        params['start_time'] = (end_time - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

    # Include the Source Key in the headers for the data fetch
    headers_source = {"Authorization": f"Bearer {SOURCE_KEY}", "Content-Type": "application/json"}
    
    try:
        response = requests.get(DATA_SOURCE_URL, params=params, headers=headers_source, timeout=5)  # Include headers in the request
        if response.status_code == 200:
            print("API request succeeded.")
        else:
            print(f"API request failed with status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")

    response.raise_for_status()

    data = response.json()['data']
    
    if data: #Is there anything new to send? 
        last_timestamp = max(entry['timestamp'] for entry in data)

        with psycopg.connect(**DB_CONNECTION_PARAMS) as conn:
            with conn.cursor() as cur:
                for report in data:
                    # Assuming your 'reports' table has columns matching the keys in your 'report' dictionary
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
                    )

        conn.commit()  # Commit the changes to the database

    else:
        print("No new data found.")

# Schedule the task to run every minute
schedule.every(1).minutes.do(fetch_and_post_data)

# Run any pending tasks now
schedule.run_all()

while True:
    schedule.run_pending()
    time.sleep(1)

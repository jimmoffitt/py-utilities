import requests
import time
import schedule
import os
import json
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Get the directory of the current script
script_dir = Path(__file__).parent 

# Construct the path to .env.local within the script's directory
env_path = script_dir / '.env.local'
load_dotenv(dotenv_path=env_path)

SOURCE_KEY = os.getenv('TINYBIRD_SOURCE_TOKEN')
TARGET_KEY = os.getenv('TINYBIRD_TARGET_TOKEN')

DATA_SOURCE_URL = "https://api.tinybird.co/v0/pipes/reportsv2.json"
EVENTS_API_URL = "https://api.us-west-2.aws.tinybird.co/v0/events?name=weather_data_from_tb"
#EVENTS_API_URL = "https://api.tinybird.co/v0/events?name=weather_data_v2"

end_time = datetime.now()
start_time = end_time - timedelta(days=1)
last_timestamp = None

def fetch_and_post_data():
    global last_timestamp

    params = {}
    
    end_time = datetime.now() # Always request data up until now?
    params['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
    
    if last_timestamp: # then start there.
        params['start_time'] = last_timestamp
    else: #otherwise, go back a day.     
        #params['start_time'] = (end_time - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        params['start_time'] = '2024-08-13 23:59:39'

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

        # Include the Target Key in the headers for posting data
        headers_target = {"Authorization": f"Bearer {TARGET_KEY}", "Content-Type": "application/json"}

        for report in data:
            print(report)
            payload = json.dumps(report)
            #payload = json.loads(report.replace("'", '"'))
            events_response = requests.post(EVENTS_API_URL, data=payload, headers=headers_target)
            if events_response.status_code == 202:
                print(f"Data point posted successfully! {report}")
            else:
                print(f"Failed to post data point. Status code: {events_response.status_code}")
                print(f"Response: {events_response.text}")

        print("Processed all data")       

    else:
        print("No new data found.")

# Schedule the task to run every minute
schedule.every(1).minutes.do(fetch_and_post_data)

# Run any pending tasks now
schedule.run_all()

while True:
    schedule.run_pending()
    time.sleep(1)

import requests
import time
from datetime import datetime, timedelta
import schedule
import os
import json
from pathlib import Path
from dotenv import load_dotenv


# Get the directory of the current script
script_dir = Path(__file__).parent 

# Construct the path to .env.local within the script's directory
env_path = script_dir / '.env.local'
load_dotenv(dotenv_path=env_path)

SOURCE_KEY = os.getenv('TINYBIRD_SOURCE_TOKEN')
TARGET_KEY = os.getenv('TINYBIRD_TARGET_TOKEN')

DATA_SOURCE_URL = "https://api.tinybird.co/v0/pipes/reportsv2.json"
EVENTS_API_URL = "https://api.us-west-2.aws.tinybird.co/v0/events?name=weather_data_json"
MOST_RECENT_URL = "https://api.us-west-2.aws.tinybird.co/v0/pipes/most_recent.json" 

# Some initial values... 
end_time = datetime.now()
start_time = end_time - timedelta(days=7) 
last_timestamp = None

# First pull data from production live system and then write those updates to 
def fetch_and_post_data():
    global last_timestamp

    # Include the Tinybird token for the data fetch and data posts. 
    headers_source = {"Authorization": f"Bearer {SOURCE_KEY}", "Content-Type": "application/json"}
    headers_target = {"Authorization": f"Bearer {TARGET_KEY}", "Content-Type": "application/json"}

    if not last_timestamp:
        # Try to retrieve most recent time. 
        try:
            response = requests.get(MOST_RECENT_URL, headers=headers_target, timeout=5)  # Include headers in the request
            if response.status_code == 200:
                print("API request succeeded.")
                start_time = response.json()['data'][0]['timestamp']
            else:
                print(f"API request failed with status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"API request error: {e}")

    params = {}
    
    end_time = datetime.utcnow() # Always request data up until now?
    params['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
    
    if last_timestamp: # then start there.
        # Parse the string timestamp into a datetime object
        last_datetime = datetime.fromisoformat(last_timestamp)

        # Add one second
        new_datetime = last_datetime + timedelta(seconds=1)

        # Convert back to ISO format string
        params['start_time'] = new_datetime.isoformat()
    else: #otherwise, go back a day.     
        #params['start_time'] = (end_time - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        params['start_time'] = start_time
    
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

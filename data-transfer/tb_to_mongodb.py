import requests
import time
from datetime import datetime, timedelta
import schedule
import os
import json
from pathlib import Path
from dotenv import load_dotenv

# Import necessary pymongo modules
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Get the directory of the current script
script_dir = Path(__file__).parent 

# Construct the path to .env.local within the script's directory
env_path = script_dir / '.env.local'
load_dotenv(dotenv_path=env_path)

# API and Database configuration

# Replace placeholders with your actual MongoDB Atlas connection details
MONGODB_CONNECTION_STRING = os.getenv("MONGODB_CONNECTION_STRING")
MONGODB_DATABASE_NAME = os.getenv("MONGODB_DATABASE_NAME")
MONGODB_COLLECTION_NAME = os.getenv("MONGODB_COLLECTION_NAME") 

SOURCE_KEY = os.getenv('TINYBIRD_SOURCE_TOKEN')

DATA_SOURCE_URL = "https://api.tinybird.co/v0/pipes/reportsv2.json"

# Initialize MongoDB client
client = MongoClient(MONGODB_CONNECTION_STRING)

try:
    # The ismaster command is cheap and does not require auth.
    client.admin.command('ismaster')
    print("Connected to MongoDB Atlas!")
except ConnectionFailure:
    print("Server not available")
    exit(1)

db = client[MONGODB_DATABASE_NAME]
collection = db[MONGODB_COLLECTION_NAME]

# Some initial values... 
end_time = datetime.now()
start_time = '2024-09-01 00:00:00'  
last_timestamp = None

def fetch_and_post_data():
    global last_timestamp

    # Find the most recent timestamp already in the database.
    try:
        headers_source = {"Authorization": f"Bearer {SOURCE_KEY}", "Content-Type": "application/json"}

        if not last_timestamp:
            # Calculate the timestamp for one month ago
            looking_back = datetime.now() - timedelta(days=1)
            looking_back_str = looking_back.strftime('%Y-%m-%d %H:%M:%S')

            # Find the document with the most recent timestamp
            most_recent_document = collection.find_one(
                filter={"timestamp": {"$gt": looking_back_str}},
                sort=[("timestamp", -1)]  # Sort by timestamp in descending order
            )

            if most_recent_document:
                last_timestamp = most_recent_document['timestamp']
                print(f"Most recent timestamp: {last_timestamp}")
            else:
                print("WARNING: No documents found in the collection. Using default start time.")

        params = {}
        end_time = datetime.utcnow()
        params['end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
        params['start_time'] = last_timestamp if last_timestamp else start_time

        try:
            response = requests.get(DATA_SOURCE_URL, params=params, headers=headers_source, timeout=5)
            response.raise_for_status()
            data = response.json()['data']
        except requests.exceptions.RequestException as e:
            print(f"ERROR: API request error: {e}")
            return
        except (json.JSONDecodeError, KeyError) as e:
            print(f"ERROR: Error parsing API response: {e}")
            return

        if data:
            last_timestamp = max(entry['timestamp'] for entry in data)

            for report in data:
                try:
                    # Insert the document into the MongoDB collection
                    collection.insert_one(report)
                    print(f"SUCCESS: Item with timestamp {report['timestamp']} for {report['site_name']} added to MongoDB.")
                except Exception as e:
                    print(f"ERROR: An unexpected error occurred while inserting document: {e}")
       
        else:
            print("INFO: No new data found.")

        print("All data processed...")

    except Exception as e:
        print(f"ERROR: An unexpected error occurred: {e}")

# Schedule the task to run every minute
schedule.every(1).minutes.do(fetch_and_post_data)

# Run any pending tasks now
schedule.run_all()

while True:
    schedule.run_pending()
    time.sleep(1)
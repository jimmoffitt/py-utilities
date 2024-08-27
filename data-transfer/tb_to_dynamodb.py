import requests
import time
from datetime import datetime, timedelta
import schedule
import os
import json
import boto3
from pathlib import Path
from dotenv import load_dotenv
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal  # Import Decimal class

from botocore.exceptions import ClientError

# Get the directory of the current script
script_dir = Path(__file__).parent 

# Construct the path to .env.local within the script's directory
env_path = script_dir / '.env.local'
load_dotenv(dotenv_path=env_path)

# API and Database configuration
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME")
AWS_REGION = os.getenv("AWS_REGION")

SOURCE_KEY = os.getenv('TINYBIRD_SOURCE_TOKEN')

DATA_SOURCE_URL = "https://api.tinybird.co/v0/pipes/reportsv2.json"

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE_NAME)

# Some initial values... 
end_time = datetime.now()
start_time = '2024-08-20 00:00:00'  # TODO: change this... set to a week ago?
last_timestamp = None

def fetch_and_post_data():
    global last_timestamp

    # Go see what the most recent timestamp already in the database.
    try:
        headers_source = {"Authorization": f"Bearer {SOURCE_KEY}", "Content-Type": "application/json"}

        if not last_timestamp:
            # Calculate the timestamp for one month ago
            looking_back = datetime.now() - timedelta(days=7)
            looking_back_str = looking_back.strftime('%Y-%m-%d %H:%M:%S')

            # Scan the table with a filter to get items from the last month
            response = table.scan(
                FilterExpression=Attr('timestamp').gt(looking_back_str)
            )

            # Find the most recent timestamp
            if response['Items']:
                most_recent_item = max(response['Items'], key=lambda x: x['timestamp'])
                last_timestamp = most_recent_item['timestamp']
                print(f"Most recent timestamp: {last_timestamp}")
            else:
                print("WARNING: No rows found in the database. Using default start time.")

            #items = response.get('Items', [])
            #if items:
            #    last_timestamp = items[0]['timestamp']
            #else:
            #    print("WARNING: No rows found in the database. Using default start time.")

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
                    table.put_item(
                        Item={
                            'timestamp': report['timestamp'],
                            'site_name': report['site_name'],
                            'temp_f': Decimal(str(report['temp_f'])),  # Convert to Decimal
                            'clouds': report['clouds'],
                            'description': report['description'],
                            'humidity': Decimal(str(report['humidity'])),  # Convert to Decimal
                            'precip': Decimal(str(report['precip'])),  # Convert to Decimal
                            'pressure': Decimal(str(report['pressure'])),  # Convert to Decimal
                            'wind_dir': Decimal(str(report['wind_dir'])),  # Convert to Decimal
                            'wind_speed': Decimal(str(report['wind_speed']))  # Convert to Decimal
                        }
                    )
                    print(f"SUCCESS: Item with timestamp {report['timestamp']} for {report['site_name']} added to DynamoDB.")
                except ClientError as e:
                    print(f"ERROR: Failed to insert item with timestamp {report['timestamp']}: {e}")
                except Exception as e:
                    print(f"ERROR: An unexpected error occurred while inserting item: {e}")
       
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

import threading
import requests
from datetime import datetime, timedelta
import argparse 
import time
import os
import random
import psycopg
from psycopg import OperationalError
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env.local
# Get the directory of the current script
script_dir = Path(__file__).parent 

# Construct the path to .env.local within the script's directory
env_path = script_dir / '.env.local'
load_dotenv(dotenv_path=env_path)

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Tinybird and Postgres Stress Tester')
parser.add_argument('--threads', type=int, default=10, help='Number of threads to use')
args = parser.parse_args()

# Use the number of threads from the command-line argument
NUM_THREADS = args.threads

TINYBIRD_KEY = os.getenv("TINYBIRD_KEY")

TINYBIRD_REPORTS_URL = 'https://api.tinybird.co/v0/pipes/reports.json'  
TINYBIRD_CC_ENDPOINT = 'https://api.tinybird.co/v0/pipes/current_conditions.json'
TINYBIRD_CITY_NAME_ENDPOINT = 'https://api.tinybird.co/v0/pipes/api_cities.json'

# API and Database configuration
DB_CONNECTION_PARAMS = {
    'dbname': os.getenv("DATABASE_NAME"),
    'user': os.getenv("DATABASE_USER"),
    'password': os.getenv("DATABASE_PASSWORD"),
    'host': os.getenv("DATABASE_HOST"),
    'port': os.getenv("DATABASE_PORT")  
}



# Global variable to store city names
CITY_NAMES = [] 

# Function to get available city names from Tinybird (called only once)
def fetch_city_names():
    global CITY_NAMES 
    headers = {'Authorization': f'Bearer {TINYBIRD_KEY}'}
    try:
        response = requests.get(TINYBIRD_CITY_NAME_ENDPOINT, headers=headers)
        response.raise_for_status() 
        data = response.json()
        CITY_NAMES = [site['site_name'] for site in data['data']]
        print("City names fetched successfully.") 
    except requests.exceptions.RequestException as e:
        print(f"Error fetching city names: {e}")

# Function to generate random start and end times
def generate_random_times():
    """Generates random start and end times within the specified duration."""
    time_min_str = '2023-05-01 00:00:00'  # Customizable minimum start time

    duration_in_days = random.randint(2,90)

    start_time_min = datetime.strptime(time_min_str, '%Y-%m-%d %H:%M:%S')
    start_time_max = datetime.now() - timedelta(days=duration_in_days)

    time_range = start_time_max - start_time_min
    random_seconds = random.randint(0, int(time_range.total_seconds()))
    start_time = start_time_min + timedelta(seconds=random_seconds)
    
    end_time = start_time + timedelta(days=duration_in_days)

    return start_time, end_time


def call_tinybird_api():
    headers = {'Authorization': f'Bearer {TINYBIRD_KEY}'}

    if not CITY_NAMES:
        print("No city names available. Skipping API call.")
        return

    # Randomly select a city name
    site_name = random.choice(CITY_NAMES)

    # Generate random start and end times
    start_time, end_time = generate_random_times()

    # Prepare /reports request. 
    params = dict(
        city=site_name, 
        start_time=start_time.isoformat(), 
        end_time=end_time.isoformat()
    )

    try:
        response = requests.get(TINYBIRD_REPORTS_URL, params=params, headers=headers, timeout=5)  # Include headers in the request
        if response.status_code == 200:
            print("API request succeeded.")
        else:
            print(f"API request failed with status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")

def call_postgres_db():

    if not CITY_NAMES:
        print("No city names available. Skipping database call.")
        return

    # Randomly select a city name
    site_name = random.choice(CITY_NAMES)

    # Generate random start and end times
    start_time, end_time = generate_random_times()

    try:
        with psycopg.connect(**DB_CONNECTION_PARAMS) as conn:
            with conn.cursor() as cursor:

                sql_query = f"""
                SELECT * FROM weather_reports 
                WHERE site_name = '{site_name}' 
                AND timestamp > '{start_time}' 
                AND timestamp <= '{end_time}' 
                ORDER BY timestamp DESC
                """ 
                
                cursor.execute(sql_query)
                result = cursor.fetchone()
                if result:
                    print("Database request succeeded.")
    except OperationalError as e:
        print(f"Database connection error: {e}")

fetch_city_names()

def worker():
    while True:
        call_tinybird_api()
        call_postgres_db()
        time.sleep(0.01)  # Slight delay to simulate realistic conditions? A way to throttle. 

threads = []
# Creating threads
for _ in range(NUM_THREADS):
    thread = threading.Thread(target=worker)
    thread.start()
    threads.append(thread)

# Wait for all threads to complete
for thread in threads:
    thread.join()

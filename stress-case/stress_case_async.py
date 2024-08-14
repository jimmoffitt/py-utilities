import threading
import asyncio
from datetime import datetime, timedelta
import os
import time
import random
import psycopg
import aiohttp
import argparse
from dotenv import load_dotenv

# Configuration
load_dotenv('.env.local')


#TODO: Pick one config convention. 
DATABASE_URL = os.environ.get("DATABASE_URL")
#  or 
DATABASE_HOST = os.environ.get("DATABASE_HOST")
DATABASE_PORT = os.environ.get("DATABASE_PORT")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
DATABASE_USER = os.environ.get("DATABASE_USER")
DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD")

TINYBIRD_KEY = os.environ.get("TINYBIRD_KEY")

TINYBIRD_REPORTS_ENDPOINT = 'https://api.tinybird.co/v0/pipes/reports.json'
TINYBIRD_CC_ENDPOINT = 'https://api.tinybird.co/v0/pipes/current_conditions.json'
TINYBIRD_CITY_NAME_ENDPOINT = 'https://api.tinybird.co/v0/pipes/api_cities.json'


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

# Functions to construct SQL queries:
def make_reports_query(site_name, start_time, end_time):
    """Constructs a SQL query to fetch weather reports."""
    query = f"SELECT * FROM weather_reports WHERE site_name = '{site_name}' AND timestamp > '{start_time}' AND timestamp <= '{end_time}' ORDER BY timestamp DESC LIMIT 10"   
    return query

# TODO: get query from Pipe.
def make_daily_stats_query(site_name, start_time, end_time):
    """Constructs a SQL query to fetch weather reports."""
    query = f"""
    SELECT
        timestamp::date AS date,  
        site_name AS city,
        AVG(temp_f) AS temp_avg,
        MAX(temp_f) AS temp_max,
        MIN(temp_f) AS temp_min,
        MAX(wind_speed) AS wind_vel_max,
        AVG(humidity) AS humidity_avg,
        MAX(humidity) AS humidity_max,
        MIN(humidity) AS humidity_min,
        MAX(clouds) AS clouds_max,
        MIN(clouds) AS clouds_min
    FROM weather_reports
    WHERE site_name = '{site_name}'
      AND timestamp >= '{start_time}'
      AND timestamp < '{end_time}'
    GROUP BY timestamp::date, city 
    ORDER BY timestamp::date DESC;
    """

    return query

# Asynchronous function to load city names from Tinybird
async def load_city_names():
    """Loads city names from the Tinybird API using aiohttp."""
    data = []
    headers = {'Authorization': f'Bearer {TINYBIRD_KEY}'}

    async with aiohttp.ClientSession() as session:
        async with session.get(TINYBIRD_CITY_NAME_ENDPOINT, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                print("Tinybird responded...")
                #print("Tinybird API response:", data)
            else:
                print(f"Tinybird API Error: {response.status} - {await response.text()}")

    city_names = [site['site_name'] for site in data['data']]

    return city_names

# Asynchronous function to make database requests
async def make_database_request(site_name, start_time, end_time):
    """Makes an asynchronous database request using psycopg."""
   
    async def _make_request():
        try:
            async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
                async with conn.cursor() as cur:
                    sql_query = make_reports_query(site_name, start_time, end_time) 
                    print(f"Making Postgres query: {sql_query}")
                    await cur.execute(sql_query)
                    results = await cur.fetchall()
                    print("Postgres responded...")
                    #print("Postgres response:", results)
                    return results  # Return the results from the database query
        except (psycopg.OperationalError, asyncio.TimeoutError) as e:
            print(f"Database Error: {e}")
        except Exception as e:
            print(f"Unexpected Database Error: {e}")

    # Await the _make_request coroutine and return its result
    return await _make_request() 

# Asynchronous function to make Tinybird requests
async def make_tinybird_request(site_name, start_time, end_time):
    """Makes an asynchronous Tinybird API request using aiohttp."""
    headers = {'Authorization': f'Bearer {TINYBIRD_KEY}'}
  
    # Convert datetime objects to ISO 8601 strings

    # Prepare /reports request. 
    params = dict(
        city=site_name, 
        start_time=start_time.isoformat(), 
        end_time=end_time.isoformat()
    )

    # prepare /reports request.

    print(f"Making Tinybird request: {params} ")

    async with aiohttp.ClientSession() as session:
        async with session.get(TINYBIRD_REPORTS_ENDPOINT, params=params, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                print("Tinybird responded...")
                #print("Tinybird API response:", data)
            else:
                print(f"Tinybird API Error: {response.status} - {await response.text()}")

# -----------------------------------------------------------------------------------------------
# --- Thread Functions ---
def make_database_requests_thread(rps, blocking=False):
    """Thread function for making continuous database requests at a specified RPS."""

    async def _make_requests():
        interval = 1.0 / rps
        while True:
            site_name = random.choice(city_names)
            start_time, end_time = generate_random_times()

            if blocking:
                # Synchronous execution, wait for each request to complete
                await make_database_request(site_name, start_time, end_time) 
            else:
                # Asynchronous execution, fire off requests without waiting
                asyncio.create_task(make_database_request(site_name, start_time, end_time)) 

            await asyncio.sleep(interval) 

    # Create and run the event loop within the thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)  # Set the new loop as the current one
    loop.run_until_complete(_make_requests()) 


async def make_tinybird_requests_thread(rps, blocking=False):
    """Asynchronous thread function for making continuous Tinybird API requests."""
    interval = 1.0 / rps
    while True:
        site_name = random.choice(city_names)
        start_time, end_time = generate_random_times()

        try:
            if blocking:
                await make_tinybird_request(site_name, start_time, end_time)
            else:
                asyncio.create_task(make_tinybird_request(site_name, start_time, end_time))
        except aiohttp.ClientError as e:
            print(f"Tinybird API Error: {e}")

        await asyncio.sleep(interval) 

# --- Main Execution ---

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Make requests to s Postgres database and/or Tinybird API Endpoints.')
    parser.add_argument('--mode', choices=['postgres', 'tinybird', 'both'], default='both', help='Request mode')
    #parser.add_argument('--interval', type=float, default=10.0, help='Interval between requests (seconds)')
    parser.add_argument('--rps', type=int, default=10, help='Requests per second')
    parser.add_argument('--block', action='store_true', help='Block and wait for each request to complete')
    args = parser.parse_args()

    args.mode = 'both'
    args.interval = 1

    city_names = asyncio.run(load_city_names())  

    threads = []

    if args.mode in ['postgres', 'both']:
        print("Starting Postgres requests thread)...")
        db_thread = threading.Thread(target=make_database_requests_thread, args=(args.rps, args.block), daemon=True)
        threads.append(db_thread)
        db_thread.start()

    if args.mode in ['tinybird', 'both']:
        print("Starting Tinybird requests thread)...")
        tinybird_thread = threading.Thread(target=asyncio.run, args=(make_tinybird_requests_thread(args.rps, args.block),))
        threads.append(tinybird_thread)
        tinybird_thread.start()

    for thread in threads:
        thread.join()

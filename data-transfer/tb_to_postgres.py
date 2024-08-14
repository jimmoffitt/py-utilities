import requests
import psycopg
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Tinybird endpoint 
tinybird_endpoint = "https://api.tinybird.co/v0/reports/<your_report_name>"

# Postgres connection details from .env
postgres_host = os.getenv("POSTGRES_HOST")
postgres_port = int(os.getenv("POSTGRES_PORT", 5432))  # Default to 5432 if not set
postgres_database = os.getenv("POSTGRES_DATABASE")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")   


# Table to store the most recent timestamp
timestamp_table_name = "latest_timestamp"

def fetch_and_insert_data(last_timestamp=None):
    params = {}
    if last_timestamp:
        params["from_timestamp"] = last_timestamp  # Adjust parameter name as needed

    response = requests.get(tinybird_endpoint, params=params)
    data = response.json()

    conn = psycopg.connect(
        host=postgres_host,
        port=postgres_port,
        dbname=postgres_database,
        user=postgres_user,
        password=postgres_password
    )
    cur = conn.cursor()

    for row in data:
        columns = ', '.join(row.keys())
        values = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO your_table_name ({columns}) VALUES ({values})"
        cur.execute(insert_query, tuple(row.values()))

        # Update the latest timestamp if this row has a newer one
        if "timestamp" in row and (not last_timestamp or row["timestamp"] > last_timestamp):
            update_timestamp_query = f"UPDATE {timestamp_table_name} SET latest_timestamp = %s"
            cur.execute(update_timestamp_query, (row["timestamp"],))

    conn.commit()
    cur.close()
    conn.close()

# Get the initial latest timestamp or set it to None if the table is empty
conn = psycopg.connect(
    host=postgres_host,
    port=postgres_port,
    dbname=postgres_database,
    user=postgres_user,
    password=postgres_password
)
cur = conn.cursor()
cur.execute(f"SELECT latest_timestamp FROM {timestamp_table_name}")
result = cur.fetchone()
last_timestamp = result[0] if result else None
cur.close()
conn.close()

while True:
    fetch_and_insert_data(last_timestamp)
    time.sleep(60) 
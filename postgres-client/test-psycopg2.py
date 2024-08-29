import psycopg2
import os
import time
from pathlib import Path
from dotenv import load_dotenv

# Get the directory of the current script
script_dir = Path(__file__).parent 

# Construct the path to .env.local within the script's directory
env_path = script_dir / '.env.local'
load_dotenv(dotenv_path=env_path)

# Retrieve connection parameters
host = os.environ.get("POSTGRES_DATABASE_HOST")
port = os.environ.get("POSTGRES_DATABASE_PORT")
database = os.environ.get("POSTGRES_DATABASE_NAME")
user = os.environ.get("POSTGRES_DATABASE_USER")
password = os.environ.get("POSTGRES_DATABASE_PASSWORD")

query = "SELECT * FROM weather_reports WHERE site_name = 'Denver' LIMIT 1000"  

conn = None

print("Connecting to database...")
try:
    conn = psycopg2.connect(
        database=database, 
        user=user,
        password=password,
        host=host,
        port=port 
    )
    print("Connected!")

    print("Creating cursor...")
    cursor = conn.cursor()
    print("Created cursor...")

    # Now you can safely execute your SQL queries here
    # For example:
    postgreSQL_select_Query = query

    start_time = time.time()

    cursor.execute(postgreSQL_select_Query)
    records = cursor.fetchall()

    end_time = time.time()

    # Print the returned rows
    for row in records:
        print(row)

     # Calculate and print execution time in milliseconds
    execution_time = (end_time - start_time) * 1000
    print(f"Query execution time: {execution_time:.2f} milliseconds")

    print("Script completed successfully")

except psycopg2.OperationalError as e:
    print(f"Unable to connect to the database: {e}")
except psycopg2.ProgrammingError as e:
    print(f"Error executing SQL query: {e}")
finally:
    if conn is not None:  # Close the connection if it was successfully established
        cursor.close()
        conn.close()
        print("Database connection closed.")

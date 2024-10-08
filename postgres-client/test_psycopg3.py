import psycopg # latest version 3
import time
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env.local
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

conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

print("Connecting to the database...")

try:
    # Establish a connection to the database
    with psycopg.connect(conn_string) as conn:
        print("Connection established")

        with conn.cursor() as cur:
            print("Cursor created")
            
            # Define a simple query to test
            print(f"Query defined: {query}")

            try:

                start_time = time.time()

                # Execute the query
                print("Executing the query...")
                cur.execute(query)
                print("Query executed")

                # Fetch the result
                print("Fetching the result...")
                result = cur.fetchall()
                print("Result fetched")

                # Print the result
                print("Printing the result...")
                for row in result:
                    print(row)

                end_time = time.time()
                # Calculate and print execution time in milliseconds
                execution_time = (end_time - start_time) * 1000
                print(f"Query execution time: {execution_time:.2f} milliseconds")
                
                print("Script completed successfully")

            except Exception as e:
                print(f"An error occurred while executing the query: {e}")

except Exception as e:
    print(f"An error occurred while connecting to the database: {e}")
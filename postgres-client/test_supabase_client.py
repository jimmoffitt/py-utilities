import os
import time 
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv('.env.local')

# As doc'ed:
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(url, key)


start_time = time.time()

# TODO: City name is case sensitive. 
response = supabase.table("weather_reports").select("*").eq("site_name", "Denver").limit(1000).execute()

print(response)

end_time = time.time()

# Calculate and print execution time in milliseconds
execution_time = (end_time - start_time) * 1000
print(f"Query execution time: {execution_time:.2f} milliseconds")

print("Script completed successfully")




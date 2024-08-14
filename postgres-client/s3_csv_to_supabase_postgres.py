import pandas as pd
from supabase import create_client, Client

# Supabase Connection
url: str = 'your_supabase_url'  # Replace with your Supabase project URL
key: str = 'your_supabase_key'  # Replace with your Supabase project API key
supabase: Client = create_client(url, key)

# CSV Handling (Optimized for Large Files)
chunksize = 10000  # Adjust chunk size based on available memory
csv_file_path = 'your_large_csv_file.csv'
table_name = 'your_table_name'

for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
    try:
        # Data Cleaning/Transformation (Optional)
        #  - Adjust column types
        #  - Handle missing values
        #  - ... any other needed transformations

        # Insert into Supabase
        data = chunk.to_dict(orient='records')  # Convert to list of dictionaries
        response = supabase.table(table_name).insert(data).execute()

        # Check for Errors
        if response.error:
            print(f"Error inserting chunk: {response.error}")
            break  # Handle the error as needed
    except Exception as e:
        print(f"Error processing chunk: {e}")
        break
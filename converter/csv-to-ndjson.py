# python3 csv-to-ndjson.py
# pip3 install csv json

import csv
import json
import os
from pathlib import Path

script_dir = Path(__file__).parent 

# Define the field names and their desired data types
field_types = {
    "clouds": int,
    "humidity": int,
    "precip": float,
    "pressure": float,
    "temp_f": float,
    "wind_speed": float,
    "wind_dir": int
}

# Get all CSV files in the directory
csv_files = [f for f in os.listdir(script_dir) if f.endswith('.csv')]

for csv_file_name in csv_files:
    csvfile = open(script_dir / csv_file_name, 'r')
    jsonfile = open(script_dir / (csv_file_name.replace('.csv', '.ndjson')), 'w')

    reader = csv.DictReader(csvfile)  # No need to pass fieldnames here
    next(reader)  # Skip the header row

    for row in reader:
        # Cast numeric values based on field_types
        for field, data_type in field_types.items():
            if field in row:
                row[field] = data_type(row[field])

        json.dump(row, jsonfile)
        jsonfile.write('\n')

    csvfile.close()
    jsonfile.close()

print("Complete!")
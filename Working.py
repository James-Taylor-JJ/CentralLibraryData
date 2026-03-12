import os
import pandas as pd
import logging
import os

# Read in the json files and create a log file with information of all data that is missing or null
logging.basicConfig(filename='data_quality.log', level=logging.INFO)

json_files = [f for f in os.listdir('.') if f.endswith('.json')]

for file in json_files:
    df = pd.read_json(file)
    missing_data = df.isnull().sum()
    for column, missing_count in missing_data.items():
        if missing_count > 0:
            logging.info(f'{file} - Column: {column} has {missing_count} missing values')
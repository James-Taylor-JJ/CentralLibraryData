import pandas as pd
import glob
import os

# 1. Create a new folder for the cleaned data
output_dir = "cleaned_data"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Define your required columns
required_keys = ['id', 'user_name', 'email']

# Identify all target files
csv_files = glob.glob("*.csv")
json_files = glob.glob("data/*.json")
all_files = csv_files + json_files

dataframes = []

for file in all_files:
    # Load data based on extension
    if file.endswith('.csv'):
        df = pd.read_csv(file)
    else:
        df = pd.read_json(file)
    
    # Ensure required columns exist
    for col in required_keys:
        if col not in df.columns:
            df[col] = None 

    # 2. Report missing values (blanks)
    missing_counts = df.isnull().sum()
    missing_only = missing_counts[missing_counts > 0]
    
    if not missing_only.empty:
        print(f"--- Missing data in {file} ---")
        print(missing_only)
    
    # 3. Fill all blanks with null (pd.NA)
    df = df.fillna(value=pd.NA)
    dataframes.append(df)

# 4. Combine, filter duplicates, and save
if dataframes:
    df_combined = pd.concat(dataframes, ignore_index=True)
    
    # Drop duplicates (keeping the first occurrence)
    df_cleaned = df_combined.drop_duplicates().reset_index(drop=True)
    
    # Save to JSON in the new folder
    output_path = os.path.join(output_dir, "master_cleaned_data.json")
    # 'records' format makes it a clean list of objects
    df_cleaned.to_json(output_path, orient='records', indent=4)
    
    print(f"\nSuccess! Filtered data saved to: {output_path}")
    print(f"Original rows: {len(df_combined)} | Rows after filtering: {len(df_cleaned)}")

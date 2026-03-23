import pandas as pd
import glob
import os
import shutil
import sys
import re
from datetime import datetime

# --- CONFIGURATION ---
MIN_RAW_SIZE_MB = 0.5
MAX_VERSIONS = 5
BASE_DIR = "cleaned_versions"
# RENAME_MAP can cause duplicates (e.g., both 'title' and 'publisher' becoming 'user_name')
RENAME_MAP = {
    "userId": "id", "title_id": "id", "tmdbId": "id", "imdbId": "id",
    "original_title": "user_name", "title": "user_name", "publisher": "user_name"
}
DOMAIN_FIXES = {"gmial.com": "gmail.com", "gmaill.com": "gmail.com", "yaho.com": "yahoo.com", "hotmial.com": "hotmail.com"}
REQUIRED_KEYS = ['id', 'user_name', 'email']
NUMERIC_COLUMNS = ['budget', 'revenue', 'runtime', 'pages', 'issue_count']
DATE_COLUMNS = ['release_date', 'start_date', 'end_date', 'date']
EMAIL_REGEX = r'^[^@]+@[^@]+\.[^@]+$'

def get_readable_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0: return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return "0 B"

# --- STATION 1: SAFETY CHECK (Pre-flight) ---
file_list = sorted(glob.glob("*.csv") + glob.glob("data/*.json"))
if not file_list:
    print("🛑 ERROR: No files found to process.")
    sys.exit()

total_raw_bytes = sum(os.path.getsize(f) for f in file_list)
if (total_raw_bytes / (1024 * 1024)) < MIN_RAW_SIZE_MB:
    print(f"🛑 ERROR: Total data too small ({get_readable_size(total_raw_bytes)}). Aborting to protect history.")
    sys.exit()

# Setup Versioning
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
output_dir = os.path.join(BASE_DIR, f"run_{timestamp}")
os.makedirs(output_dir, exist_ok=True) 
log_path = os.path.join(output_dir, "cleaning_log.txt")

# --- STATION 2: INDIVIDUAL CLEANING (The Loop) ---
all_data = []
email_fix_count = 0
title_fix_count = 0

with open(log_path, "w") as log:
    log.write(f"DATA CLEANING REPORT - {timestamp}\n" + "="*50 + "\n")
    
    for file in file_list:
        try:
            # Use utf-8-sig to handle hidden BOM characters from Excel
            df = pd.read_csv(file, encoding='utf-8-sig') if file.endswith('.csv') else pd.read_json(file)
            
            # 1. Clean headers and remove pandas-generated duplicates (.1, .2)
            df.columns = [re.sub(r'\.\d+$', '', str(c)).strip() for c in df.columns]
            
            # 2. Apply the Rename Map
            df.rename(columns=RENAME_MAP, inplace=True)
            
            # 3. FIX: Handle duplicates created BY the rename (this solves the 'DataFrame' object has no attribute 'str' error)
            df = df.loc[:, ~df.columns.duplicated()].copy()
            
            for col in df.columns:
                series = df[col]
                
                # Double-check: Ensure it's a Series, not a DataFrame
                if isinstance(series, pd.DataFrame):
                    series = series.iloc[:, 0]
                
                # Clean Emails
                if col == 'email':
                    before = series.astype(str)
                    cleaned = before.str.lower().str.strip()
                    for typo, fix in DOMAIN_FIXES.items():
                        cleaned = cleaned.str.replace(typo, fix, regex=False)
                    email_fix_count += (before != cleaned).sum()
                    df[col] = cleaned.apply(lambda x: x if re.match(EMAIL_REGEX, str(x)) else pd.NA)
                
                # Clean Titles (user_name)
                elif col == 'user_name':
                    before = series.astype(str)
                    cleaned = before.str.title().str.strip()
                    title_fix_count += (before != cleaned).sum()
                    df[col] = cleaned.replace(['Nan', 'None', ''], pd.NA)

                # Format Dates & Numbers
                elif col in DATE_COLUMNS:
                    df[col] = pd.to_datetime(series, errors='coerce').dt.strftime('%Y-%m-%d')
                elif col in NUMERIC_COLUMNS:
                    df[col] = pd.to_numeric(series, errors='coerce')

            # Add "extra_" prefix to non-required columns
            df.columns = [c if c in REQUIRED_KEYS else f"extra_{c}" for c in df.columns]
            
            # Discard nearly empty rows
            df_filtered = df.dropna(thresh=2).copy()
            all_data.append(df_filtered.fillna(value=pd.NA))
            log.write(f"SUCCESS: {file} | Kept {len(df_filtered)} rows\n")
            
        except Exception as e:
            log.write(f"ERROR: {file} failed. {e}\n")

    # --- STATION 3: THE BIG MERGE ---
    if all_data:
        df_combined = pd.concat(all_data, ignore_index=True)
        # Final global header deduplication
        df_combined = df_combined.loc[:, ~df_combined.columns.duplicated()]
        
        # --- STATION 4: FINAL POLISHING (Dedupe & Sort) ---
        # Only dedupe by 'id' if 'id' column exists
        if 'id' in df_combined.columns:
            df_final = df_combined.drop_duplicates(subset=['id'], keep='first')
        else:
            df_final = df_combined
            
        # Determine sorting priority based on available columns
        sort_priority = [c for c in ['user_name', 'id'] if c in df_final.columns]
        if sort_priority:
            df_final = df_final.sort_values(by=sort_priority).reset_index(drop=True)
            
        df_final = df_final.reindex(sorted(df_final.columns), axis=1)
        
        # --- STATION 5: STORAGE ---
        output_file = os.path.join(output_dir, f"master_data_{timestamp}.json")
        df_final.to_json(output_file, orient='records', indent=4)
        
        # Dashboard Summary
        print("\n" + "="*30)
        print(f"✅ CLEANING COMPLETE")
        print(f"📧 Emails Corrected: {email_fix_count}")
        print(f"📖 Titles Formatted: {title_fix_count}")
        print(f"📊 Final JSON Size: {get_readable_size(os.path.getsize(output_file))}")
        print("-" * 30)
        print("PREVIEW (FIRST 5 ROWS):")
        print(df_final.head(5).to_json(orient='records', indent=4))
        print("="*30)

# Version Cleanup (Keep only the 5 most recent runs)
if os.path.exists(BASE_DIR):
    existing_runs = sorted([os.path.join(BASE_DIR, d) for d in os.listdir(BASE_DIR) if d.startswith("run_")], key=os.path.getctime)
    if len(existing_runs) > MAX_VERSIONS:
        for old_folder in existing_runs[:-MAX_VERSIONS]:
            shutil.rmtree(old_folder)

# music_pipeline_full.py
import pandas as pd
import re
import os
import json

# -------------------------------
# CONFIG: Output files
# -------------------------------
OUTPUT_CSV = "tcc_ceds_music_cleaned.csv"
OUTPUT_JSON = "tcc_ceds_music_cleaned.json"

# -------------------------------
# COLUMN NUMERIC CONVERSION LIST
# -------------------------------
NUMERIC_COLS = [
    'len', 'dating', 'violence', 'world_life', 'night_time',
    'shake_the_audience', 'family_gospel', 'romantic', 
    'communication', 'obscene', 'music', 'movement_places', 
    'light_visual_perceptions', 'family_spiritual', 'like_girls', 
    'sadness', 'feelings', 'danceability', 'loudness', 
    'acousticness', 'instrumentalness', 'valence', 'energy', 'age'
]

# -------------------------------
# FUNCTION: Clean lyrics text
# -------------------------------
def clean_lyrics(text):
    """Lowercase, remove special characters, and extra spaces in lyrics."""
    if not isinstance(text, str):
        return ''
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# -------------------------------
# FUNCTION: Find input CSV recursively
# -------------------------------
def find_input_csv():
    """Search current directory and all child folders for a CSV starting with 'tcc_ceds_music'."""
    for root, dirs, files in os.walk("."):
        for file in files:
            if file.startswith("tcc_ceds_music") and file.endswith(".csv"):
                full_path = os.path.join(root, file)
                print(f"Found input CSV: {full_path}")
                return full_path
    raise FileNotFoundError("❌ No CSV starting with 'tcc_ceds_music' found in this folder or any subfolder.")

# -------------------------------
# FUNCTION: Music CSV Pipeline
# -------------------------------
def music_pipeline(output_csv: str, output_json: str):
    """Load, clean, save CSV and export JSON."""
    # Find the CSV automatically
    input_csv = find_input_csv()
    
    # 1. Load CSV
    df = pd.read_csv(input_csv)
    
    # 2. Standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(r'[/\s]', '_', regex=True)
    
    # 3. Handle missing data
    df.fillna('', inplace=True)
    
    # 4. Convert numeric columns
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 5. Convert release_date to datetime
    if 'release_date' in df.columns:
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    
    # 6. Clean lyrics
    if 'lyrics' in df.columns:
        df['lyrics'] = df['lyrics'].apply(clean_lyrics)
    
    # 7. Save cleaned CSV
    df.to_csv(output_csv, index=False)
    print(f"✅ Pipeline complete. Cleaned CSV saved to: {output_csv}")
    
    # 8. Export JSON
    df.to_json(output_json, orient='records', lines=True, force_ascii=False)
    print(f"✅ JSON export complete. Saved to: {output_json}")

# -------------------------------
# RUN PIPELINE
# -------------------------------
if __name__ == "__main__":
    music_pipeline(OUTPUT_CSV, OUTPUT_JSON)
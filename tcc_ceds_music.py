# music_pipeline_full.py
import pandas as pd
import re
import os

# --------------------------------
# OUTPUT FILES
# --------------------------------
OUTPUT_CSV = "tcc_ceds_music_cleaned.csv"
OUTPUT_JSON = "tcc_ceds_music_cleaned.json"

# --------------------------------
# NUMERIC COLUMNS
# --------------------------------
NUMERIC_COLS = [
    'len','dating','violence','world_life','night_time',
    'shake_the_audience','family_gospel','romantic',
    'communication','obscene','music','movement_places',
    'light_visual_perceptions','family_spiritual',
    'like_girls','sadness','feelings','danceability',
    'loudness','acousticness','instrumentalness',
    'valence','energy','age'
]

# --------------------------------
# COLUMNS TO REMOVE
# --------------------------------
DROP_COLS = [
    'world_life',
    'shake_the_audience',
    'dating',
    'violence',
    'family_gospel',
    'like_girls',
    'sadness',
    'feelings',
    'valence',
    'energy',
    'topic',
    'danceability'
]

# --------------------------------
# CLEAN LYRICS FUNCTION
# --------------------------------
def clean_lyrics(text):
    """Clean lyrics text."""
    if not isinstance(text, str):
        return ""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# --------------------------------
# FIND INPUT CSV
# --------------------------------
def find_input_csv():
    """Search project folders for tcc_ceds_music CSV."""
    for root, dirs, files in os.walk("."):
        for file in files:
            if file.startswith("tcc_ceds_music") and file.endswith(".csv"):
                path = os.path.join(root, file)
                print(f"Found input file: {path}")
                return path
    raise FileNotFoundError("No tcc_ceds_music CSV found.")

# --------------------------------
# MAIN PIPELINE
# --------------------------------
def music_pipeline():

    # 1️⃣ Locate CSV automatically
    input_csv = find_input_csv()

    # 2️⃣ Load dataset
    df = pd.read_csv(input_csv, low_memory=False)

    print(f"Loaded dataset with {len(df)} rows")

    # 3️⃣ Standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[ /]", "_", regex=True)
    )

    # 4️⃣ Remove duplicate rows
    df.drop_duplicates(inplace=True)

    # 5️⃣ Handle missing values
    df.fillna("", inplace=True)

    # 6️⃣ Convert numeric columns
    for col in NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 7️⃣ Clean lyrics column
    if "lyrics" in df.columns:
        df["lyrics"] = df["lyrics"].apply(clean_lyrics)

    # 8️⃣ Convert release date
    if "release_date" in df.columns:
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

    # 9️⃣ Drop unnecessary columns
    df.drop(columns=DROP_COLS, errors="ignore", inplace=True)

    # 🔟 Save cleaned CSV
    df.to_csv(OUTPUT_CSV, index=False)

    print(f"Clean CSV saved → {OUTPUT_CSV}")

    # 1️⃣1️⃣ Save JSON for LMS
    df.to_json(
        OUTPUT_JSON,
        orient="records",
        indent=2,
        force_ascii=False
    )

    print(f"JSON export saved → {OUTPUT_JSON}")

    # Preview output
    print("\nPreview of cleaned dataset:")
    print(df.head(5))

# --------------------------------
# RUN PIPELINE
# --------------------------------
if __name__ == "__main__":
    music_pipeline()
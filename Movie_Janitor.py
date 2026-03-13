#Here is where I'm writing the code to clean up the movies_metadata.json file by removing homepages
from pathlib import Path
import pandas as pd

class Janitor:
     def __init__(self, data_dir: Path):
         self.data_dir = data_dir

     def clean_metadata(self):
         metadata_file = self.data_dir / "movies_metadata.json"
         try:
             df = pd.read_json(metadata_file)
         except ValueError:
             df = pd.read_json(metadata_file, lines=True)
         except Exception as error:
             print(f"Failed to read JSON ({error})")
             return

         if "homepage" in df.columns:
             df = df.drop(columns=["homepage"])
             df.to_json(metadata_file, orient="records", lines=True)
     def clean_metadata(self):
         metadata_file = self.data_dir / "movies_metadata.json"
         try:
             df = pd.read_json(metadata_file)
         except ValueError:
             df = pd.read_json(metadata_file, lines=True)
         except Exception as error:
             print(f"Failed to read JSON ({error})")
             return

         if "homepage" in df.columns:
             df = df.drop(columns=["homepage"])
             df.to_json(metadata_file, orient="records", lines=True)
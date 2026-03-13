import logging
from pathlib import Path

import pandas as pd


DATA_DIR = Path("LMS-DataStuff")
LOG_FILE = Path("data_quality.log")


logging.basicConfig(
    filename=LOG_FILE,
    filemode="w",
    level=logging.INFO,
    format="%(message)s",
)


def get_json_files(data_dir: Path) -> list[Path]:
    return sorted(path for path in data_dir.rglob("*.json") if path.is_file())


def log_missing_data(json_file: Path) -> None:
    try:
        df = pd.read_json(json_file)
    except ValueError:
        df = pd.read_json(json_file, lines=True)
    except Exception as error:
        logging.info(f"{json_file}: failed to read JSON ({error})")
        return

    missing_data = df.isnull().sum()
    missing_columns = missing_data[missing_data > 0]

    if missing_columns.empty:
        logging.info(f"{json_file}: no missing values found")
        return

    logging.info(f"{json_file}:")
    for column, missing_count in missing_columns.items():
        logging.info(f"  {column}: {missing_count} missing values")


for json_file in get_json_files(DATA_DIR):
    log_missing_data(json_file)



def replace_nan_with_null(json_file: Path) -> None:
    try:
        df = pd.read_json(json_file)
    except ValueError:
        df = pd.read_json(json_file, lines=True)
    except Exception as error:
        logging.info(f"{json_file}: failed to read JSON ({error})")
        return

    df = df.fillna("Null")
    df.to_json(json_file, orient="records", lines=True)

for json_file in get_json_files(DATA_DIR):
    replace_nan_with_null(json_file)
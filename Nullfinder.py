import json
import logging
from pathlib import Path

import pandas as pd


DATA_DIR = Path("LMS-DataStuff")
LOG_FILE = Path("data_quality.log")
MISSING_PLACEHOLDERS = {"", "null", "none", "nan"}


def configure_logger() -> logging.Logger:
    logger = logging.getLogger("data_quality")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def get_json_files(data_dir: Path) -> list[Path]:
    return sorted(path for path in data_dir.rglob("*.json") if path.is_file())


def load_json_as_dataframe(json_file: Path) -> pd.DataFrame:
    text = json_file.read_text(encoding="utf-8")


    try:
        payload = json.loads(text)
        if isinstance(payload, list):
            return pd.json_normalize(payload)
        if isinstance(payload, dict):
            return pd.json_normalize([payload])
    except json.JSONDecodeError:
        pass

    
    try:
        return pd.read_json(json_file, lines=True)
    except ValueError:
        return pd.read_json(json_file)


def missing_counts(df: pd.DataFrame) -> pd.Series:
    normalized = df.copy()
    object_columns = normalized.select_dtypes(include=["object"]).columns
    if len(object_columns) > 0:
        normalized[object_columns] = normalized[object_columns].apply(
            lambda col: col.map(
                lambda value: pd.NA
                if isinstance(value, str) and value.strip().lower() in MISSING_PLACEHOLDERS
                else value
            )
        )
    return normalized.isna().sum()


def log_missing_data(json_file: Path, logger: logging.Logger) -> None:
    try:
        df = load_json_as_dataframe(json_file)
    except Exception as error:
        logger.info(f"{json_file}: failed to read JSON ({error})")
        return

    if df.empty:
        logger.info(f"{json_file}: file parsed but contains no rows")
        return

    missing_data = missing_counts(df)
    missing_columns = missing_data[missing_data > 0]

    if missing_columns.empty:
        logger.info(f"{json_file}: no missing values found")
        return

    logger.info(f"{json_file}:")
    for column, missing_count in missing_columns.items():
        logger.info(f"  {column}: {int(missing_count)} missing values")


def main() -> None:
    logger = configure_logger()
    for json_file in get_json_files(DATA_DIR):
        log_missing_data(json_file, logger)


if __name__ == "__main__":
    main()
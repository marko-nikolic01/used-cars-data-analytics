import os
import time
import shutil
import kagglehub
import pandas as pd
from kagglehub import KaggleDatasetAdapter

DATA_FOLDER = "./data/batch"
DATASET_NAME = "used_cars_data.csv"

STAGING_FILE = os.path.join(DATA_FOLDER, DATASET_NAME + ".staging")
COPY_FILE = os.path.join(DATA_FOLDER, DATASET_NAME + ".staging.copy")
FINAL_FILE = os.path.join(DATA_FOLDER, DATASET_NAME)

SLEEP_SECONDS = 90

def ensure_data_folder():
    os.makedirs(DATA_FOLDER, exist_ok=True)

def batch_dataset_exists() -> bool:
    return os.path.exists(STAGING_FILE)

def download_batch_dataset() -> pd.DataFrame:
    print("Batch dataset not found. Downloading batch dataset from Kaggle...", flush=True)
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "ananaymital/us-used-cars-dataset",
        "used_cars_data.csv"
    )
    df = df.drop(columns=["description"])
    return df

def write_staging_file(df: pd.DataFrame):
    df.to_csv(STAGING_FILE, index=False)
    print(f"Batch dataset written to staging file: {STAGING_FILE}", flush=True)

def copy_to_copy_file():
    shutil.copy2(STAGING_FILE, COPY_FILE)
    print(f"Copied staging batch dataset file to copy batch dataset file: {COPY_FILE}", flush=True)

def rename_copy_to_final():
    os.rename(COPY_FILE, FINAL_FILE)
    print(f"Renamed copy batch dataset file to final batch dataset file: {FINAL_FILE}", flush=True)

def wait_for_ingestion():
    print(f"Sleeping {SLEEP_SECONDS} seconds for NiFi ingestion...", flush=True)
    time.sleep(SLEEP_SECONDS)
    print("Done.", flush=True)

def main():
    ensure_data_folder()

    if not batch_dataset_exists():
        df = download_batch_dataset()
        write_staging_file(df)
    else:
        print(f"Batch dataset already exists: {STAGING_FILE}. Skipping download...", flush=True)

    copy_to_copy_file()
    rename_copy_to_final()
    wait_for_ingestion()

if __name__ == "__main__":
    main()

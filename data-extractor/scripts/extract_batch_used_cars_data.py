import os
import time
import shutil
import kagglehub
from kagglehub import KaggleDatasetAdapter
import pandas as pd

data_folder = "./data/batch"
dataset_name = "used_cars_data.csv"
staging_file = os.path.join(data_folder, dataset_name + ".staging")
copy_file = os.path.join(data_folder, dataset_name + ".staging.copy")
final_file = os.path.join(data_folder, dataset_name)

os.makedirs(data_folder, exist_ok=True)

if not os.path.exists(staging_file):
    print("Batch dataset not found. Downloading batch dataset from Kaggle...", flush=True)
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "ananaymital/us-used-cars-dataset",
        "used_cars_data.csv"
    )
    df = df.drop(columns=["description"])
    df.to_csv(staging_file, index=False)
    print(f"Batch dataset written to staging file: {staging_file}", flush=True)
else:
    print(f"Batch dataset already exists: {staging_file}. Skipping download...", flush=True)

shutil.copy2(staging_file, copy_file)
print(f"Copied staging batch dataset file to copy batch dataset file: {copy_file}", flush=True)

os.rename(copy_file, final_file)
print(f"Renamed copy batch dataset file to final batch dataset file: {final_file}", flush=True)

print("Sleeping 90 seconds for NiFi ingestion...", flush=True)
time.sleep(90)
print("Done.")

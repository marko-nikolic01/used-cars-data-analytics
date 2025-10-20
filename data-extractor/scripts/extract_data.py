import os
import time
import kagglehub
from kagglehub import KaggleDatasetAdapter
import pandas as pd

data_folder = "./data"
dataset_name = "used_cars_data.csv"
staging_file = os.path.join(data_folder, dataset_name + ".staging")
final_file = os.path.join(data_folder, dataset_name)

os.makedirs(data_folder, exist_ok=True)

if not os.path.exists(final_file):
    print("Dataset not found locally. Downloading...", flush=True)
    df = kagglehub.load_dataset(
        KaggleDatasetAdapter.PANDAS,
        "ananaymital/us-used-cars-dataset",
        "used_cars_data.csv"
    )

    df = df.drop(columns=["description"])

    df.to_csv(staging_file, index=False)
    print(f"Dataset written to staging file {staging_file}", flush=True)

    os.rename(staging_file, final_file)
    print(f"Dataset renamed to final file {final_file}", flush=True)

    print("Sleeping 90 seconds for NiFi ingestion...", flush=True)
    time.sleep(90)

else:
    print(f"Dataset already exists at {final_file}", flush=True)
    df = pd.read_csv(final_file)

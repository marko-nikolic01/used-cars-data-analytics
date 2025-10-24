import os
import glob
import json
import random
import time
from datetime import datetime

DATA_FOLDER = "./data/stream"
FINAL_FILE = os.path.join(DATA_FOLDER, "used_cars_data.json")
STAGING_PATTERN = os.path.join(DATA_FOLDER, "*.staging")
SLEEP_SECONDS = 1
MIN_FILES = 1
MAX_FILES = 5

def get_staging_files():
    return glob.glob(STAGING_PATTERN)

def update_listing_date(record):
    record["createdAt"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return record

def adjust_price(record, min_factor=0.95, max_factor=1.05):
    if record.get("retailListing") and "price" in record["retailListing"]:
        factor = random.uniform(min_factor, max_factor)
        record["retailListing"]["price"] = round(record["retailListing"]["price"] * factor)
    return record

def pick_random_records(files):
    records = []
    selected_files = random.sample(files, min(len(files), random.randint(MIN_FILES, MAX_FILES)))
    for file in selected_files:
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if data:
                    record = random.choice(data)
                    record = update_listing_date(record)
                    record = adjust_price(record)
                    records.append(record)
        except Exception as e:
            print(f"Error reading {file}: {e}", flush=True)
    return records

def append_to_final_file(records):
    with open(FINAL_FILE, "a", encoding="utf-8") as f:
        for record in records:
            json_line = json.dumps(record, ensure_ascii=False)
            f.write(json_line + "\n")
    if records:
        print(f"Appended {len(records)} records to {FINAL_FILE}", flush=True)

def main():
    while True:
        staging_files = get_staging_files()
        if not staging_files:
            print("No staging stream dataset files found, waiting...", flush=True)
        else:
            records = pick_random_records(staging_files)
            append_to_final_file(records)
        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()

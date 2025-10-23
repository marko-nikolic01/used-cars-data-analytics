import os
import glob
import uuid
import json
import requests
from typing import List, Dict, Any

DATA_FOLDER = "./data/stream"
DATASET_NAME = "used_cars_data.json"
FINAL_FILE = os.path.join(DATA_FOLDER, DATASET_NAME)

DEFAULT_START_PAGE = 1
DEFAULT_END_PAGE = 1000
DEFAULT_PAGE_SIZE = 100

def ensure_data_folder():
    os.makedirs(DATA_FOLDER, exist_ok=True)

def reset_final_file():
    if os.path.exists(FINAL_FILE):
        os.remove(FINAL_FILE)
    open(FINAL_FILE, "w").close()
    print(f"Created final stream dataset file: {FINAL_FILE}", flush=True)

def find_staging_files() -> List[str]:
    return glob.glob(os.path.join(DATA_FOLDER, "*.staging"))

def get_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        print(f"Invalid int for {name}, using default {default}", flush=True)
        return default

def should_download() -> bool:
    return os.getenv("DOWNLOAD_STREAM_DATA", "N").lower() == "y"

def build_headers() -> Dict[str, str]:
    api_key = os.getenv("AUTO_DEV_API_KEY", "").strip()
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}

def fetch_page(page: int, page_size: int, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    url = f"https://api.auto.dev/listings?page={page}&limit={page_size}"
    print(f"Fetching: {url}", flush=True)
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    json_data = resp.json()
    return json_data.get("data", [])

def write_staging_json(records: List[Dict[str, Any]]) -> str:
    filename = os.path.join(DATA_FOLDER, f"used_cars_data_{uuid.uuid4()}.json.staging")
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(records)} records to {filename}", flush=True)
    return filename

def download_stream_data(start_page: int, end_page: int, page_size: int):
    headers = build_headers()
    for page in range(start_page, end_page + 1):
        try:
            records = fetch_page(page, page_size, headers)
            if not records:
                print(f"No data on page {page}.", flush=True)
                continue
            write_staging_json(records)
        except requests.HTTPError as he:
            print(f"HTTP error fetching page {page}: {he}", flush=True)
        except Exception as e:
            print(f"Error fetching page {page}: {e}", flush=True)

def main():
    ensure_data_folder()

    reset_final_file()

    staging_files = find_staging_files()

    if not staging_files and should_download():
        print("Stream dataset not found. Downloading stream dataset from Auto.dev...", flush=True)
        start_page = get_env_int("DOWNLOAD_STREAM_DATA_START_PAGE", DEFAULT_START_PAGE)
        end_page = get_env_int("DOWNLOAD_STREAM_DATA_END_PAGE", DEFAULT_END_PAGE)
        page_size = get_env_int("DOWNLOAD_STREAM_DATA_PAGE_SIZE", DEFAULT_PAGE_SIZE)
        print(f"DOWNLOAD_STREAM_DATA=Y -> starting download pages {start_page}..{end_page} (size={page_size})", flush=True)
        download_stream_data(start_page, end_page, page_size)
        print("Download completed.", flush=True)
    else:
        print(f"Stream dataset already exists: {staging_files}. Skipping download...", flush=True)

if __name__ == "__main__":
    main()

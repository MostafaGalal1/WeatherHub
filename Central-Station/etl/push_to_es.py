import os
import time
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pyarrow.parquet as pq

# === Configs ===
DATA_DIR = "data"
ES_INDEX = "weather_statuses"
ES_HOST = "http://localhost:9200"
PROCESSED_FILE_LOG = "etl/.processed_files.txt"
SLEEP_SECONDS = 30

# Connect to Elasticsearch
es = Elasticsearch(ES_HOST)

# Load processed files log
def load_processed():
    if os.path.exists(PROCESSED_FILE_LOG):
        with open(PROCESSED_FILE_LOG, "r") as f:
            return set(f.read().splitlines())
    return set()

def save_processed(path):
    with open(PROCESSED_FILE_LOG, "a") as f:
        f.write(path + "\n")

# Watch & index Parquet files
def get_all_parquet_paths(data_dir):
    print(f"🔍 Searching for Parquet files in {os.walk(data_dir)}")
    for root, _, files in os.walk(data_dir):
        print(f"🔍 Scanning {root} for Parquet files...")
        for file in files:
            if file.endswith(".parquet"):
                yield os.path.join(root, file)

def parquet_to_es(parquet_path):
    df = pd.read_parquet(parquet_path)

    actions = [
        {
            "_index": ES_INDEX,
            "_source": row.dropna().to_dict()
        }
        for _, row in df.iterrows()
    ]

    if actions:
        bulk(es, actions)
        print(f"✅ Pushed {len(actions)} records to ES")
    else:
        print("⚠️ No records to push")

# Main loop
def main():
    print("📡 Starting Parquet → Elasticsearch watcher")
    while True:
        processed_files = load_processed()
        new_files = [p for p in get_all_parquet_paths(DATA_DIR) if p not in processed_files]

        if new_files:
            for path in new_files:
                try:
                    parquet_to_es(path)
                    save_processed(path)
                except Exception as e:
                    print(f"❌ Failed to process {path}: {e}")
        else:
            print("🕒 No new files... chilling")

        time.sleep(SLEEP_SECONDS)

if __name__ == "__main__":
    main()

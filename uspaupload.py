import os
import math
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]  # service_role key
TABLE_NAME = "uspa_records"
INPUT_FILE = "uspa_all_records.csv"
BATCH_SIZE = 2000
WORKERS = 2

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# === Load CSV ===
print(f"Reading {INPUT_FILE}...")
df = pd.read_csv(INPUT_FILE)

# Normalize weight_class to kg-only ("60kg/132.2lb" → "60kg", "140+kg/SHW" → "140+kg")
# so it matches the placeholder rows and is consistent in the DB.
df["Weight Class"] = df["Weight Class"].apply(
    lambda wc: str(wc).split("/")[0].strip() if pd.notna(wc) else None
)

# Ensure correct types
df["Kilos"] = pd.to_numeric(df["Kilos"], errors="coerce")
df["Pounds"] = pd.to_numeric(df["Pounds"], errors="coerce")
df["HasRecord"] = df["HasRecord"].fillna(False).astype(bool)

# Rename columns to snake_case to match the DB schema
df = df.rename(columns={
    "Division":     "division",
    "Weight Class": "weight_class",
    "Lift":         "lift",
    "Name":         "name",
    "Kilos":        "kilos",
    "Pounds":       "pounds",
    "Date":         "date",
    "Location":     "location",
    "Event":        "event",
    "Status":       "status",
    "HasRecord":    "has_record",
})

# Serialize through pandas' JSON encoder (handles NaN → null, numpy types → native)
# then immediately parse back to get a list of plain Python dicts with None for nulls.
records = json.loads(df.to_json(orient="records"))
print(f"Loaded {len(records)} rows")

# === Clear existing data ===
print("Clearing existing table data...")
supabase.rpc("truncate_uspa_records").execute()

# === Upload in batches (parallel) ===
batches = [records[i * BATCH_SIZE : (i + 1) * BATCH_SIZE] for i in range(math.ceil(len(records) / BATCH_SIZE))]
total_batches = len(batches)
print(f"Uploading {total_batches} batch(es) of up to {BATCH_SIZE} rows each with {WORKERS} workers...")

completed = 0

def upload_batch(args):
    i, batch = args
    for attempt in range(3):
        try:
            supabase.table(TABLE_NAME).insert(batch).execute()
            return i, len(batch)
        except Exception:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)  # 1s, 2s backoff

with ThreadPoolExecutor(max_workers=WORKERS) as executor:
    futures = [executor.submit(upload_batch, (i + 1, batch)) for i, batch in enumerate(batches)]
    for future in as_completed(futures):
        i, count = future.result()
        completed += count
        print(f"  Batch {i}/{total_batches} done — {completed}/{len(records)} rows uploaded")

print(f"\nDone — {len(records)} rows written to '{TABLE_NAME}'.")

import os
import math
import json
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from postgrest.types import ReturnMethod
from supabase import ClientOptions, create_client
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]  # service_role key
TABLE_NAME = "uspa_records"
INPUT_FILE = Path(__file__).parent / "uspa_all_records.csv"
BATCH_SIZE = int(os.getenv("USPA_UPLOAD_BATCH_SIZE", "1000"))
WORKERS = int(os.getenv("USPA_UPLOAD_WORKERS", "2"))
POSTGREST_TIMEOUT = int(os.getenv("USPA_UPLOAD_TIMEOUT", "60"))

def new_client():
    return create_client(
        SUPABASE_URL,
        SUPABASE_KEY,
        options=ClientOptions(postgrest_client_timeout=POSTGREST_TIMEOUT),
    )


supabase = new_client()
thread_local = threading.local()


def upload_client():
    if not hasattr(thread_local, "client"):
        thread_local.client = new_client()
    return thread_local.client

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
count_after = supabase.table(TABLE_NAME).select("id", count="exact").execute().count
if count_after != 0:
    raise RuntimeError(f"Truncate failed — {count_after} rows still in table.")
print("Table cleared.")

# === Upload in batches (parallel) ===
batches = [records[i * BATCH_SIZE : (i + 1) * BATCH_SIZE] for i in range(math.ceil(len(records) / BATCH_SIZE))]
total_batches = len(batches)
print(f"Uploading {total_batches} batch(es) of up to {BATCH_SIZE} rows each with {WORKERS} workers...")

completed = 0

def upload_batch(args):
    i, batch = args
    client = upload_client()
    for attempt in range(3):
        try:
            print(f"  Batch {i}/{total_batches} starting attempt {attempt + 1} ({len(batch)} rows)", flush=True)
            client.table(TABLE_NAME).insert(batch, returning=ReturnMethod.minimal).execute()
            return i, len(batch)
        except Exception as exc:
            if attempt == 2:
                raise
            # Discard broken connection so next attempt gets a fresh one
            if hasattr(thread_local, "client"):
                del thread_local.client
            wait = 2 ** attempt
            print(f"  Batch {i}/{total_batches} failed attempt {attempt + 1}: {exc}; retrying in {wait}s", flush=True)
            time.sleep(wait)  # 1s, 2s backoff

with ThreadPoolExecutor(max_workers=WORKERS) as executor:
    futures = [executor.submit(upload_batch, (i + 1, batch)) for i, batch in enumerate(batches)]
    for future in as_completed(futures):
        i, count = future.result()
        completed += count
        print(f"  Batch {i}/{total_batches} done — {completed}/{len(records)} rows uploaded")

from datetime import datetime
import subprocess as _sp
date_str = datetime.now().strftime("%B %d, %Y")
(Path(__file__).parent / "last_updated.txt").write_text(date_str)
print(f"\nDone — {len(records)} rows written to '{TABLE_NAME}'.")

# Push last_updated.txt to GitHub so the site reflects the new date
last_updated_file = Path(__file__).parent / "last_updated.txt"
if not last_updated_file.exists() or last_updated_file.read_text().strip() != date_str:
    last_updated_file.write_text(date_str)
    _sp.run(["git", "add", "last_updated.txt"], cwd=str(Path(__file__).parent), check=True)
    _sp.run(["git", "commit", "-m", f"Update last_updated to {date_str}"], cwd=str(Path(__file__).parent), check=True)
    _sp.run(["git", "push"], cwd=str(Path(__file__).parent), check=True)
    print("last_updated.txt pushed to GitHub.")
else:
    print("last_updated.txt already up to date — skipping commit.")

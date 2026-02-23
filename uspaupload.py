import os
import math
import json
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]  # service_role key
TABLE_NAME = "uspa_records"
INPUT_FILE = "uspa_all_records.csv"
BATCH_SIZE = 500

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

# === Upload in batches ===
total_batches = math.ceil(len(records) / BATCH_SIZE)
print(f"Uploading {total_batches} batch(es) of up to {BATCH_SIZE} rows each...")

for i in range(total_batches):
    batch = records[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
    supabase.table(TABLE_NAME).insert(batch).execute()
    print(f"  [{i + 1}/{total_batches}] {len(batch)} rows uploaded")

print(f"\nDone — {len(records)} rows written to '{TABLE_NAME}'.")

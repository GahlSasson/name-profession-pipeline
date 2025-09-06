#!/usr/bin/env python3
# Uploads data/candidates_raw.csv to Airtable in batches of 10 (API limit)
# Reads token from env: AIRTABLE_TOKEN

import argparse
import csv
import os
import time
import json
import sys
import urllib.parse
from typing import List, Dict

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

API_BASE = "https://api.airtable.com/v0"

def auth_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=16))
def post_records(base_id: str, table_id_or_name: str, token: str, batch: List[Dict]) -> Dict:
    table_encoded = urllib.parse.quote(table_id_or_name, safe="")
    url = f"{API_BASE}/{base_id}/{table_encoded}"
    resp = requests.post(url, headers=auth_headers(token), data=json.dumps({"records": batch}))
    if resp.status_code >= 400:
        raise RuntimeError(f"Airtable error {resp.status_code}: {resp.text}")
    return resp.json()

def main():
    ap = argparse.ArgumentParser(description="Upload CSV rows to Airtable")
    ap.add_argument("--csv", required=True, help="Path to CSV (e.g., data/candidates_raw.csv)")
    ap.add_argument("--base", required=True, help="Airtable base ID (e.g., apprub...)")
    ap.add_argument("--table", required=True, help="Airtable table ID (recommended) or name")
    args = ap.parse_args()

    token = os.getenv("AIRTABLE_TOKEN")
    if not token:
        print("Missing AIRTABLE_TOKEN env var.", file=sys.stderr)
        sys.exit(1)

    # Map CSV columns -> Airtable field names (adjust to match your table schema EXACTLY)
    field_map = {
        "full_name": "Full Name",
        "occupation": "Occupation",
        "cluster": "Profession Cluster",
        "lang": "Language",
    }

    # Read CSV
    records = []
    with open(args.csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fields = {}
            for csv_col, value in row.items():
                if csv_col in field_map and value is not None and value != "":
                    fields[field_map[csv_col]] = value
            if fields:
                records.append({"fields": fields})

    if not records:
        print("CSV had no rows; nothing to upload.")
        return 0

    # Batch and upload (Airtable max 10 records/request)
    batch_size = 10
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            post_records(args.base, args.table, token, batch)
            total += len(batch)
        except Exception as e:
            print(f"Batch starting at {i} failed: {e}", file=sys.stderr)
            sys.exit(1)
        time.sleep(0.25)  # gentle rate limiting

    print(f"Uploaded {total} records to Airtable table {args.table} in base {args.base}.")
    return 0

if __name__ == "__main__":
    sys.exit(main())

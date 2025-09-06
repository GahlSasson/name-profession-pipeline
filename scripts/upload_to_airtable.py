#!/usr/bin/env python3
# Uploads data/candidates_raw.csv to Airtable in batches of 10 (API limit)
# Reads token from env: AIRTABLE_TOKEN
#
# This version adapts to your table:
# - If expected fields aren't present, it prints the table's fields (when allowed)
# - It maps only fields that actually exist in the table to avoid 422s
# - Clear errors with guidance

import argparse
import csv
import os
import time
import json
import sys
import urllib.parse
from typing import List, Dict, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

API_BASE = "https://api.airtable.com/v0"
META_BASE = "https://api.airtable.com/v0/meta/bases"

EXPECTED_MAP = {
    "full_name": "Full Name",
    "occupation": "Occupation",
    "cluster": "Profession Cluster",
    "lang": "Language",
}

def auth_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

def auth_headers_noctype(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}"}

def fetch_schema_fields(base_id: str, table_id_or_name: str, token: str) -> Optional[List[str]]:
    """Try to fetch field names for the target table (requires schema.bases:read)."""
    url = f"{META_BASE}/{base_id}/tables"
    r = requests.get(url, headers=auth_headers_noctype(token))
    if r.status_code in (401, 403):
        print("[Uploader] Schema fetch not permitted (no schema.bases:read). Proceeding without it.")
        return None
    if r.status_code >= 400:
        print(f"[Uploader] Schema fetch error {r.status_code}: {r.text}")
        return None
    data = r.json()
    for t in data.get("tables", []):
        if t.get("id") == table_id_or_name or t.get("name") == table_id_or_name:
            return [f["name"] for f in t.get("fields", [])]
    # If not matched, return the first table fields as a hint
    if data.get("tables"):
        print("[Uploader] Could not match table by id or name in schema; showing first table's fields as hint.")
        return [f["name"] for f in data["tables"][0].get("fields", [])]
    return None

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

    # Try to learn the table's fields (best-effort)
    table_fields = fetch_schema_fields(args.base, args.table, token)
    if table_fields:
        print(f"[Uploader] Detected table fields: {table_fields}")

    # Build a CSV->Airtable map that only includes fields present in the table (if known)
    if table_fields:
        field_map = {csv_col: at_name for csv_col, at_name in EXPECTED_MAP.items() if at_name in table_fields}
        missing = [at_name for at_name in EXPECTED_MAP.values() if at_name not in table_fields]
        if missing:
            print(f"[Uploader] The following expected Airtable fields are missing in the table: {missing}")
            print("[Uploader] I will upload only to fields that exist to avoid 422 errors.")
    else:
        field_map = EXPECTED_MAP  # best guess if we can't fetch schema

    if not field_map:
        print("[Uploader] No matching fields between CSV and Airtable table. Create fields or rename them to match:")
        print(f"         Expected any of: {list(EXPECTED_MAP.values())}")
        sys.exit(1)

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

    # Upload in batches of 10
    batch_size = 10
    total = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            post_records(args.base, args.table, token, batch)
            total += len(batch)
            print(f"[Uploader] Uploaded batch {i//batch_size + 1}: {len(batch)} rows")
        except Exception as e:
            print(f"[Uploader] Batch starting at index {i} failed: {e}", file=sys.stderr)
            print("[Uploader] If this is a 422 error, ensure your Airtable table has the following fields (case-sensitive):")
            print(f"           {list(EXPECTED_MAP.values())}")
            sys.exit(1)
        time.sleep(0.25)

    print(f"Uploaded {total} records to Airtable table {args.table} in base {args.base}.")
    return 0

if __name__ == "__main__":
    sys.exit(main())

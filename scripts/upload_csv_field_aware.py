#!/usr/bin/env python3
"""
Uploads data/candidates_raw.csv to Airtable.
- Auto-detects your actual column names (snake_case or spaced).
- Respects single-select options (skips invalid values instead of 422).
- Writes lang to any language-like fields (language, language-origin, language_origin, …).
"""

import os, sys, csv, json, time, urllib.parse, requests

def die(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)

def sanitize_token(tok: str) -> str:
    return tok.strip().replace("\r", "").replace("\n", "").replace("\t", "")

def discover_schema(base_id, table_id_or_name, headers):
    """Return (available_field_names, field_definitions_by_name)."""
    enc_table = urllib.parse.quote(table_id_or_name, safe="")
    # Prefer schema API (needs schema.bases:read). If forbidden, fall back to sampling records.
    r = requests.get(f"https://api.airtable.com/v0/meta/bases/{base_id}/tables",
                     headers=headers, timeout=30)
    fields, defs = [], {}
    if r.status_code == 200:
        data = r.json()
        for t in data.get("tables", []):
            if t.get("id") == table_id_or_name or t.get("name") == table_id_or_name:
                for f in t.get("fields", []):
                    fields.append(f["name"])
                    defs[f["name"]] = f

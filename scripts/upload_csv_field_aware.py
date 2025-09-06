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
    tok2 = tok.strip().replace("\r", "").replace("\n", "").replace("\t", "")
    return tok2

def discover_schema(base_id, table_id_or_name, headers):
    """Return (available_field_names, field_definitions_by_name)."""
    # Try meta schema (needs schema.bases:read). If not allowed, fall back to sampling records.
    enc_table = urllib.parse.quote(table_id_or_name, safe="")
    meta_url = f"https://api.airtable.com/v0/meta/bases/{base_id}/tables"
    r = requests.get(meta_url, headers=headers, timeout=30)
    fields, defs = [], {}
    if r.status_code == 200:
        data = r.json()
        for t in data.get("tables", []):
            if t.get("id") == table_id_or_name or t.get("name") == table_id_or_name:
                for f in t.get("fields", []):
                    fields.append(f["name"])
                    defs[f["name"]] = f
                return fields, defs
    # Fallback: union of field keys in a few records
    list_url = f"https://api.airtable.com/v0/{base_id}/{enc_table}?maxRecords=5"
    r = requests.get(list_url, headers=headers, timeout=30)
    seen = set()
    if r.status_code == 200:
        for rec in r.json().get("records", []):
            seen.update((rec.get("fields") or {}).keys())
    return sorted(seen), {}

def pick_one(avail, candidates):
    # Prefer exact (case-insensitive), then "contains"
    for c in candidates:
        for f in avail:
            if f.lower() == c.lower():
                return f
    for c in candidates:
        cl = c.lower()
        for f in avail:
            if cl in f.lower():
                return f
    return None

def pick_all(avail, candidates):
    out, seen = [], set()
    for c in candidates:
        for f in avail:
            if f.lower() == c.lower() and f not in seen:
                out.append(f); seen.add(f)
    for c in candidates:
        cl = c.lower()
        for f in avail:
            if cl in f.lower() and f not in seen:
                out.append(f); seen.add(f)
    return out

def normalize_select(value, allowed):
    if not value or not allowed:
        return None
    for opt in allowed:
        if value.lower() == opt.lower():
            return opt  # return canonical casing
    return None  # not allowed → skip

def main():
    BASE   = os.getenv("AIRTABLE_BASE_ID")
    TOKEN  = (os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY") or "")
    TABLE  = os.getenv("AIRTABLE_TABLE_ID") or os.getenv("AIRTABLE_TABLE_NAME")
    if not (BASE and TOKEN and TABLE):
        die("Missing AIRTABLE_* envs (need BASE_ID, TOKEN/API_KEY, and TABLE_ID or TABLE_NAME).")

    TOKEN = sanitize_token(TOKEN)
    H_AUTH = {"Authorization": f"Bearer {TOKEN}"}
    H_JSON = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

    enc_table = urllib.parse.quote(TABLE, safe="")
    base_url  = f"https://api.airtable.com/v0/{BASE}/{enc_table}"  # NOTE: no typecast=true

    # 1) Discover fields & schema
    avail, defs = discover_schema(BASE, TABLE, H_AUTH)
    print("[UPLOAD] Available fields:", avail or "(none)")

    # 2) Decide which columns to use in YOUR table
    full_name_field  = pick_one(avail, ["full_name","Full Name","Name","fullname","Full name","title"])
    # cluster may be profession_cluster / professional_cluster / Profession Cluster / cluster
    cluster_field    = pick_one(avail, ["profession_cluster","professional_cluster","Profession Cluster","cluster","Cluster","category","Category"])
    # occupation may be occupation / profession_canonical / professional_canonical / misspelling
    occupation_field = pick_one(avail, ["occupation","profession_canonical","professional_canonical","professional_canonoical","job","role","Role"])
    # language targets (write to all)
    language_fields  = pick_all(avail, ["language","Language","language_origin","language-origin","Language Origin","lang","Lang"])

    print("[UPLOAD] Mapping:", {
        "full_name": full_name_field,
        "cluster": cluster_field,
        "occupation": occupation_field,
        "language_targets": language_fields
    })

    # Allowed options for select cluster (if schema available)
    allowed_cluster = None
    if cluster_field and cluster_field in defs:
        fdef = defs[cluster_field]
        if fdef.get("type") in ("singleSelect","multipleSelects"):
            choices = (fdef.get("options") or {}).get("choices") or []
            allowed_cluster = [c.get("name") for c in choices if "name" in c]
            print(f"[UPLOAD] {cluster_field} allowed options:", allowed_cluster)

    # 3) Build records from CSV
    csv_path = "data/candidates_raw.csv"
    if not os.path.exists(csv_path):
        die("CSV not found at data/candidates_raw.csv")

    records =

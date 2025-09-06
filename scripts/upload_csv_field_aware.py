#!/usr/bin/env python3
"""
Uploads data/candidates_raw.csv to Airtable.
- Auto-detects your actual column names (snake_case or spaced).
- Respects single-select options (skips invalid values instead of 422).
- Writes lang to any language-like fields (language, language-origin, language_origin, …).
"""

import os, sys, csv, json, time, urllib.parse, requests

def die(msg, code=1):
    print(msg, file=sys.stderr); sys.exit(code)

def sanitize_token(tok: str) -> str:
    return tok.strip().replace("\r","").replace("\n","").replace("\t","")

def discover_schema(base_id, table_id_or_name, headers):
    """Return (available_field_names, field_definitions_by_name)."""
    enc_table = urllib.parse.quote(table_id_or_name, safe="")
    # Try schema API (needs schema.bases:read)
    r = requests.get(f"https://api.airtable.com/v0/meta/bases/{base_id}/tables",
                     headers=headers, timeout=30)
    fields, defs = [], {}
    if r.status_code == 200:
        data = r.json()
        for t in data.get("tables", []):
            if t.get("id") == table_id_or_name or t.get("name") == table_id_or_name:
                for f in t.get("fields", []):
                    fields.append(f["name"]); defs[f["name"]] = f
                return fields, defs
    # Fallback: union of field keys seen in a few records
    r = requests.get(f"https://api.airtable.com/v0/{base_id}/{enc_table}?maxRecords=5",
                     headers=headers, timeout=30)
    seen = set()
    if r.status_code == 200:
        for rec in r.json().get("records", []):
            seen.update((rec.get("fields") or {}).keys())
    return sorted(seen), {}

def pick_one(avail, candidates):
    for c in candidates:
        for f in avail:
            if f.lower() == c.lower(): return f
    for c in candidates:
        cl = c.lower()
        for f in avail:
            if cl in f.lower(): return f
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
    if not value or not allowed: return None
    for opt in allowed:
        if value.lower() == opt.lower():
            return opt
    return None

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
    base_url  = f"https://api.airtable.com/v0/{BASE}/{enc_table}"  # no typecast=true

    # 1) Discover fields & schema
    avail, defs = discover_schema(BASE, TABLE, H_AUTH)
    print("[UPLOAD] Available fields:", avail or "(none)")

    # 2) Map CSV -> your real columns
    full_name_field  = pick_one(avail, ["full_name","Full Name","Name","fullname","Full name","title"])
    cluster_field    = pick_one(avail, ["profession_cluster","professional_cluster","Profession Cluster","cluster","Cluster","category","Category"])
    occupation_field = pick_one(avail, ["occupation","profession_canonical","professional_canonical","professional_canonoical","job","role","Role"])
    language_fields  = pick_all(avail, ["language","Language","language_origin","language-origin","Language Origin","lang","Lang"])

    print("[UPLOAD] Mapping:", {
        "full_name": full_name_field,
        "cluster": cluster_field,
        "occupation": occupation_field,
        "language_targets": language_fields
    })

    # Allowed options if cluster is a select
    allowed_cluster = None
    if cluster_field and cluster_field in defs:
        fdef = defs[cluster_field]
        if fdef.get("type") in ("singleSelect","multipleSelects"):
            choices = (fdef.get("options") or {}).get("choices") or []
            allowed_cluster = [c.get("name") for c in choices if "name" in c]
            print(f"[UPLOAD] {cluster_field} allowed options:", allowed_cluster)

    # 3) Read CSV
    csv_path = "data/candidates_raw.csv"
    if not os.path.exists(csv_path):
        die("CSV not found at data/candidates_raw.csv")

    records = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fields = {}
            if full_name_field  and row.get("full_name"):  fields[full_name_field]  = row["full_name"]
            if occupation_field and row.get("occupation"): fields[occupation_field] = row["occupation"]
            if cluster_field and row.get("cluster"):
                if allowed_cluster is None:
                    pass  # no schema → skip to avoid INVALID_MULTIPLE_CHOICE_OPTIONS
                else:
                    norm = normalize_select(row["cluster"], allowed_cluster)
                    if norm is not None:
                        fields[cluster_field] = norm
                    else:
                        print(f"[UPLOAD] Skipping cluster '{row['cluster']}' (not in allowed options).")
            if language_fields and row.get("lang"):
                for lf in language_fields:
                    fields[lf] = row["lang"]
            if not fields and full_name_field:
                fields[full_name_field] = row.get("full_name") or "Record"
            records.append({"fields": fields})

    # 4) Upload in batches of 10
    created = 0
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        r = requests.post(base_url, headers=H_JSON, data=json.dumps({"records": batch}), timeout=60)
        if r.status_code >= 400:
            die(

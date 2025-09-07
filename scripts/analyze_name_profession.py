#!/usr/bin/env python3
"""
Name→Profession analyzer for Airtable.

What it does
------------
- Fetch all records from your Airtable table.
- Auto-detects your schema (snake_case or spaced variants).
- Tokenizes names (first/last + character trigrams).
- Learns token ↔ cluster associations from YOUR data (PMI with Laplace smoothing).
- For each record, scores clusters from its tokens, picks a predicted cluster,
  and writes back: np_cluster_pred, np_cluster_score, np_token_explain.

Requirements
------------
- Repo secrets: AIRTABLE_TOKEN, AIRTABLE_BASE_ID, (AIRTABLE_TABLE_ID or AIRTABLE_TABLE_NAME)
- Python deps: requests (installed in the workflow)

Fields it writes (create these as Single line text/Number/Long text, or the script
falls back to writing JSON into 'notes_qc' / 'Notes' / 'Description' if present):
- np_cluster_pred   (text)
- np_cluster_score  (number)
- np_token_explain  (long text)
"""

import os, sys, time, math, json, re, urllib.parse, requests
from collections import Counter, defaultdict

# ---------- Airtable helpers ----------

def env():
    base  = os.getenv("AIRTABLE_BASE_ID")
    token = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY") or ""
    table = os.getenv("AIRTABLE_TABLE_ID") or os.getenv("AIRTABLE_TABLE_NAME")
    if not (base and token and table):
        sys.exit("Missing AIRTABLE_* envs (BASE_ID, TOKEN/API_KEY, and TABLE_ID or TABLE_NAME).")
    token = token.strip().replace("\r","").replace("\n","").replace("\t","")
    enc_table = urllib.parse.quote(table, safe="")
    base_url  = f"https://api.airtable.com/v0/{base}/{enc_table}"
    headers_auth = {"Authorization": f"Bearer {token}"}
    headers_json = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return base, table, base_url, headers_auth, headers_json

def discover_fields(base, table, headers_auth):
    """Return (available_field_names, defs_by_name or {}). Uses meta if allowed; falls back to observed keys."""
    # Try meta tables (needs schema.bases:read)
    r = requests.get(f"https://api.airtable.com/v0/meta/bases/{base}/tables", headers=headers_auth, timeout=30)
    fields, defs = [], {}
    if r.status_code == 200:
        for t in r.json().get("tables", []):
            if t.get("id") == table or t.get("name") == table:
                for f in t.get("fields", []):
                    fields.append(f["name"])
                    defs[f["name"]] = f
                return fields, defs
    # Fallback: union of fields in first few records
    fields_set = set()
    base_url = f"https://api.airtable.com/v0/{base}/{urllib.parse.quote(table, safe='')}"
    r = requests.get(base_url, headers=headers_auth, params={"maxRecords": 5}, timeout=30)
    if r.status_code == 200:
        for rec in r.json().get("records", []):
            fields_set.update((rec.get("fields") or {}).keys())
    return sorted(fields_set), {}

def pick_one(avail, aliases):
    lower = {a.lower(): a for a in avail}
    # exact (case-insensitive)
    for a in aliases:
        if a.lower() in lower: return lower[a.lower()]
    # contains
    for a in aliases:
        al = a.lower()
        for f in avail:
            if al in f.lower(): return f
    return None

def fetch_all_records(base_url, headers_auth, page_size=100, max_pages=1000):
    recs, offset, pages = [], None, 0
    while True:
        params = {"pageSize": page_size}
        if offset: params["offset"] = offset
        r = requests.get(base_url, headers=headers_auth, params=params, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"Fetch error {r.status_code}: {r.text}")
        data = r.json()
        for rec in data.get("records", []):
            recs.append(rec)
        offset = data.get("offset")
        pages += 1
        if not offset or pages >= max_pages:
            break
    return recs

def patch_record(base_url, headers_json, rec_id, fields):
    r = requests.patch(f"{base_url}/{rec_id}", headers=headers_json, data=json.dumps({"fields": fields}), timeout=60)
    return r

# ---------- Name tokenization & scoring ----------

def clean(s):
    """Keep letters (Latin + diacritics) and spaces; lower-case."""
    if not s: return ""
    return re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ\s]", " ", s).lower().strip()

def char_trigrams(s):
    s = s.replace(" ", "")
    if len(s) < 3: return []
    return [s[i:i+3] for i in range(len(s)-2)]

def name_tokens(full_name, given_name=None, surname=None):
    parts = []
    if full_name:
        parts.extend(clean(full_name).split())
    if given_name:
        parts.extend(clean(given_name).split())
    if surname:
        parts.extend(clean(surname).split())
    toks = set(parts)
    longest = max(parts, key=len) if parts else ""
    toks.update(char_trigrams(longest)[:5])
    toks = {t for t in toks if len(t) >= 2}
    return toks

# ---------- PMI model ----------

def learn_pmi(records, field_map):
    """
    Build PMI(token, cluster) from the data itself.
    field_map: dict with keys: full_name, given_name, surname, cluster, occupation
    """
    token_counts = Counter()
    cluster_counts = Counter()
    joint_counts = defaultdict(Counter)
    N = 0

    for rec in records:
        f = rec.get("fields", {})
        name = f.get(field_map["full_name"]) or ""
        giv

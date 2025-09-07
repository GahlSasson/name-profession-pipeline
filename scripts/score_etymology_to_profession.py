#!/usr/bin/env python3
"""
Score how well name_roots semantically align with the profession.
Writes: etymology_score (0-1), etymology_match (Strong/Medium/Weak/None), etymology_explain.
"""

import os, sys, json, time, urllib.parse, requests

# ---------- Airtable helpers ----------
def _env():
    base  = os.getenv("AIRTABLE_BASE_ID")
    tok   = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY") or ""
    table = os.getenv("AIRTABLE_TABLE_ID") or os.getenv("AIRTABLE_TABLE_NAME")
    if not (base and tok and table):
        sys.exit("Missing AIRTABLE_* envs.")
    tok = tok.strip().replace("\r","").replace("\n","").replace("\t","")
    enc = urllib.parse.quote(table, safe="")
    base_url = f"https://api.airtable.com/v0/{base}/{enc}"
    H_AUTH = {"Authorization": f"Bearer {tok}"}
    H_JSON = {"Authorization": f"Bearer {tok}", "Content-Type":"application/json"}
    return base, table, base_url, H_AUTH, H_JSON

def fetch_all(base_url, H_AUTH, page_size=100):
    recs, off = [], None
    while True:
        p = {"pageSize": page_size}
        if off: p["offset"] = off
        r = requests.get(base_url, headers=H_AUTH, params=p, timeout=60)
        if r.status_code >= 400:
            sys.exit(f"Fetch error {r.status_code}: {r.text}")
        j = r.json()
        recs.extend(j.get("records", []))
        off = j.get("offset")
        if not off: break
    return recs

def patch(base_url, H_JSON, rec_id, fields):
    return requests.patch(f"{base_url}/{rec_id}", headers=H_JSON, data=json.dumps({"fields": fields}), timeout=60)

# ---------- Profession keyword sets (edit/extend freely) ----------
KEYWORDS = {
    "Engineer":      {"build","make","forge","device","engine","machine","metal","smith","carpenter","adze","wood","craft"},
    "Artist":        {"art","paint","color","design","draw","create","sculpt","craft","lace"},
    "Mathematician": {"number","count","measure","logic","calc","think"},
    "Baker":         {"bread","bake","oven","grain","loaf","flour"},
    "Agriculture":   {"field","farm","earth","harvest","plough","grain","vine","vinci"},
    # add more professions as your dataset grows
}

def choose_target(f):
    # Prefer explicit canonical label; else use np_prof_pred if present
    return (f.get("profession_canonical") or f.get("np_prof_pred") or "").strip()

def score_alignment(roots, target):
    kw = KEYWORDS.get(target, set())
    if not roots: return 0.0, set()
    hits = set(r for r in roots if r in kw)
    score = len(hits) / max(1, len(roots))
    return score, hits

def tier(score):
    if score >= 0.66: return "Strong"
    if score >= 0.33: return "Medium"
    if score >  0.0:  return "Weak"
    return "None"

def main():
    base, table, base_url, H_AUTH, H_JSON = _env()
    rows = fetch_all(base_url, H_AUTH)
    updated = 0
    for rec in rows:
        f = rec.get("fields", {}); rid = rec.get("id")
        roots_raw = f.get("name_roots")
        if not roots_raw: continue
        try:
            roots = set(json.loads(roots_raw))
        except Exception:
            continue
        target = choose_target(f)
        if not target: continue
        s, hits = score_alignment(roots, target)
        out = {
            "etymology_score": round(s, 2),
            "etymology_match": tier(s),
            "etymology_explain": f"roots={sorted(list(roots))} → hits={sorted(list(hits))} → target={target}"
        }
        r = patch(base_url, H_JSON, rid, out)
        if r.status_code < 400:
            updated += 1
            time.sleep(0.1)
    print(f"[align] updated={updated}")

if __name__ == "__main__":
    main()

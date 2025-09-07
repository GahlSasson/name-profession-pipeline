#!/usr/bin/env python3
"""
Score how well name_roots semantically align with the profession.

Writes BOTH:
  - name_meaning (Long text): keeps any existing gloss, then appends a rationale line
  - etymology_explain (Long text): same rationale text (dedicated 'why' column)
Also writes:
  - etymology_score (0-1)
  - etymology_match (Strong/Medium/Weak/None)
"""
import os, sys, json, time, urllib.parse, requests

def _env():
    base  = os.getenv("AIRTABLE_BASE_ID")
    tok   = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY") or ""
    table = os.getenv("AIRTABLE_TABLE_ID") or os.getenv("AIRTABLE_TABLE_NAME")
    if not (base and tok and table): sys.exit("Missing AIRTABLE_* envs.")
    tok = tok.strip().replace("\r","").replace("\n","").replace("\t","")
    enc = urllib.parse.quote(table, safe="")
    base_url = f"https://api.airtable.com/v0/{base}/{enc}"
    H_AUTH = {"Authorization": f"Bearer {tok}"}
    H_JSON = {"Authorization": f"Bearer {tok}", "Content-Type":"application/json"}
    return base_url, H_AUTH, H_JSON

def fetch_all(base_url, H_AUTH, page_size=100):
    recs, off = [], None
    while True:
        p = {"pageSize": page_size}
        if off: p["offset"] = off
        r = requests.get(base_url, headers=H_AUTH, params=p, timeout=60)
        if r.status_code >= 400: sys.exit(f"Fetch error {r.status_code}: {r.text}")
        j = r.json(); recs.extend(j.get("records", [])); off = j.get("offset")
        if not off: break
    return recs

def patch(base_url, H_JSON, rid, fields):
    return requests.patch(f"{base_url}/{rid}", headers=H_JSON,
                          data=json.dumps({"fields": fields}), timeout=60)

# profession keywords (expand freely) — case-insensitive at runtime
KEYWORDS = {
    "engineer":      {"build","make","forge","device","engine","machine","metal","smith","carpenter","adze","wood","craft"},
    "artist":        {"art","paint","color","design","draw","create","sculpt","craft","lace"},
    "mathematician": {"number","count","measure","logic","calc","think"},
    "baker":         {"bread","bake","oven","grain","loaf","flour"},
    "agriculture":   {"field","farm","earth","harvest","plough","grain","vine","vinci"},
}

def choose_target(f):
    # prefer your label, else predicted
    t = (f.get("profession_canonical") or f.get("np_prof_pred") or "").strip()
    return t

def score_alignment(roots, target):
    kw = KEYWORDS.get(target.lower(), set())
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
    base_url, H_AUTH, H_JSON = _env()
    rows = fetch_all(base_url, H_AUTH)
    updated = 0

    for rec in rows:
        f = rec.get("fields", {}); rid = rec.get("id")
        roots_raw = f.get("name_roots")
        if not roots_raw:  # enrichment should guarantee this; skip if truly absent
            continue
        try:
            roots = set(json.loads(roots_raw))
        except Exception:
            continue

        target = choose_target(f)
        if not target:  # nothing to align to
            target = "(unknown)"

        s, hits = score_alignment(roots, target)
        tier_txt = tier(s)

        # Existing gloss
        gloss = (f.get("name_meaning") or "").strip()

        # Rationale text (always)
        rationale = (
            f"Rationale: roots={sorted(list(roots))} → hits={sorted(list(hits))} "
            f"→ target={target} → score={s:.2f} ({tier_txt})"
        )

        # Combine into name_meaning
        combined = f"{gloss}\n{rationale}" if gloss else rationale

        out = {
            "name_meaning": combined,       # human-friendly narrative
            "etymology_explain": rationale, # dedicated WHY column
            "etymology_score": round(s, 2),
            "etymology_match": tier_txt,
        }

        r = patch(base_url, H_JSON, rid, out)
        if r.status_code < 400:
            updated += 1
            time.sleep(0.1)

    print(f"[align] updated={updated}")

if __name__ == "__main__":
    main()

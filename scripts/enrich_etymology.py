#!/usr/bin/env python3
"""
Enrich Airtable rows with light etymology:
- name_meaning: short gloss from a tiny built-in lexicon + heuristics
- name_roots: JSON list of normalized roots/tokens
- name_origin: copied from language_origin if present (fallback unknown)
- etymology_source: "lexicon" (or a URL if you extend later)

No external requests; safe to run in Actions.
"""

import os, sys, json, time, re, urllib.parse, requests

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
    return requests.patch(f"{base_url}/{rec_id}", headers=H_JSON, data=json.dumps({"fields":fields}), timeout=60)

# ---------- Etymology mini-lexicon ----------
# NOTE: add to these over time; this is your starting seed.
GIVEN = {
    "nikola": {"meaning": "victory of the people", "roots": ["nike","laos"], "origin": "Greek", "src":"lexicon"},
    "ada":    {"meaning": "noble",                 "roots": ["noble"],       "origin": "Germanic", "src":"lexicon"},
    "leonardo":{"meaning":"lion-strong",           "roots": ["leo","hard"],  "origin": "Germanic+Latin", "src":"lexicon"},
}

SURNAME = {
    "tesla":   {"meaning":"adze; carpenter", "roots":["adze","carpenter"], "origin":"Slavic","src":"lexicon"},
    "smith":   {"meaning":"metalworker",     "roots":["smith","metal","forge"], "origin":"English","src":"lexicon"},
    "miller":  {"meaning":"operates a mill", "roots":["mill","grain"], "origin":"English","src":"lexicon"},
    "baker":   {"meaning":"bakes bread",     "roots":["bake","bread","oven"], "origin":"English","src":"lexicon"},
    "fisher":  {"meaning":"fisher",          "roots":["fish","river"], "origin":"English","src":"lexicon"},
    "carpenter":{"meaning":"woodworker",     "roots":["wood","carpenter"], "origin":"French/Latin","src":"lexicon"},
    "painter": {"meaning":"painter",         "roots":["paint","color"], "origin":"English","src":"lexicon"},
    # treat "da vinci" as locative
    "da vinci":{"meaning":"from Vinci (place)", "roots":["vinci","place"], "origin":"Italian","src":"lexicon"},
}

# simple cleaners
def clean(s): return re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ\s]", " ", s or "").strip().lower()

def derive_from_name(full_name, given_name, surname):
    roots, parts, notes = set(), [], []
    # given name
    gk = clean(given_name or "")
    fk = clean(full_name or "")
    # prefer explicit given_name; else first token of full_name
    given = gk or (fk.split()[0] if fk else "")
    if given in GIVEN:
        d = GIVEN[given]; roots.update(d["roots"]); parts.append(d["meaning"]); notes.append(d["src"])
    # surname
    sk = clean(surname or "")
    if not sk and fk:
        toks = fk.split()
        if len(toks) > 1: sk = " ".join(toks[1:])
    # try exact, then last token
    s_try = sk
    if s_try not in SURNAME and sk:
        s_try = sk.split()[-1]
    if s_try in SURNAME:
        d = SURNAME[s_try]; roots.update(d["roots"]); parts.append(d["meaning"]); notes.append(d["src"])
    # simple suffix heuristics (augment)
    if s_try.endswith("smith"): roots.update(["smith","metal","forge"])
    if s_try.endswith("maker"): roots.update(["make","craft"])
    if s_try.endswith("man"):   roots.update(["man","worker"])
    meaning = "; ".join(parts) if parts else ""
    return list(sorted(roots)), meaning, ", ".join(sorted(set(notes))) or "lexicon"

def main():
    base, table, base_url, H_AUTH, H_JSON = _env()
    rows = fetch_all(base_url, H_AUTH)
    updated = 0
    for rec in rows:
        f = rec.get("fields", {}); rid = rec.get("id")
        full = f.get("full_name") or ""
        given= f.get("given_name") or ""
        sur  = f.get("surname") or ""
        if not (full or given or sur):
            continue
        roots, meaning, src = derive_from_name(full, given, sur)
        origin = f.get("language_origin") or f.get("name_origin") or ""
        out = {}
        if meaning: out["name_meaning"] = meaning
        if roots:   out["name_roots"]   = json.dumps(roots, ensure_ascii=False)
        if origin:  out["name_origin"]  = origin
        out["etymology_source"] = src
        if not out: continue
        r = patch(base_url, H_JSON, rid, out)
        if r.status_code < 400:
            updated += 1
            time.sleep(0.1)
    print(f"[enrich] updated={updated}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Harvest people + professions from Wikidata and upsert into Airtable `Candidates`.

Logging: prints
  - [harvest] resolved: <Label> -> Q...
  - [harvest] fetched N for <Label> (Q...)
  - [harvest] total fetched rows=N
  - [harvest] upserted created=C updated=U

Resilient:
  - Label→QID: case-insensitive exact, then "contains" fallback
  - Accept occupation or subclass of occupation (P31/P279* wd:Q28640)
  - People fetch: ?person wdt:P106/wdt:P279* wd:{qid}
  - Retries on timeouts/429/5xx with backoff; never aborts pipeline if some labels fail
"""

import os, sys, time, json, urllib.parse, requests
from typing import Optional, Dict, List

UA = "name-profession-pipeline/2.3 (GitHub Actions; contact link in repo README)"
SPARQL_URL = "https://query.wikidata.org/sparql"

# Defaults (used when workflow input `professions` is blank)
DEFAULT_PROF_LABELS = [
    "Engineer", "Artist", "Mathematician", "Baker"
]
DEFAULT_LIMIT_PER_PROF = 40

# HTTP / retry tuning
CONNECT_TO = 15
READ_TO    = 120
MAX_RETRIES = 5
BASE_SLEEP  = 1.0
MAX_SLEEP   = 12.0

def getenv_list(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return [s.strip() for s in raw.split(",") if s.strip()]

def getenv_int(name: str, default: int) -> int:
    try:    return int(os.getenv(name, "").strip() or default)
    except: return default

def http_get(url: str, params: Dict, accept_json=True) -> Optional[requests.Response]:
    headers = {"User-Agent": UA, "Accept": "application/sparql-results+json" if accept_json else "*/*"}
    sleep = BASE_SLEEP
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=(CONNECT_TO, READ_TO))
            if r.status_code in (429, 502, 503, 504):
                raise requests.exceptions.HTTPError(f"HTTP {r.status_code}")
            return r
        except (requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectTimeout,
                requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError) as e:
            if attempt == MAX_RETRIES:
                print(f"[harvest] ERROR: GET failed after {attempt} attempts: {e}")
                return None
            time.sleep(min(MAX_SLEEP, sleep)); sleep *= 2
    return None

# ---------- label -> QID ----------
def resolve_label_to_qid(label_en: str) -> Optional[str]:
    # exact (case-insensitive)
    q1 = f"""
    SELECT ?occ ?lab WHERE {{
      ?occ wdt:P31/wdt:P279* wd:Q28640 .
      ?occ rdfs:label ?lab FILTER(LANG(?lab)="en").
      FILTER( LCASE(STR(?lab)) = LCASE("{label_en}") )
    }} LIMIT 1
    """
    r = http_get(SPARQL_URL, {"query": q1})
    if r and r.status_code == 200:
        arr = r.json().get("results", {}).get("bindings", [])
        if arr:
            return arr[0]["occ"]["value"].split("/")[-1]

    # contains fallback (pick shortest label)
    q2 = f"""
    SELECT ?occ ?lab WHERE {{
      ?occ wdt:P31/wdt:P279* wd:Q28640 .
      ?occ rdfs:label ?lab FILTER(LANG(?lab)="en").
      FILTER( CONTAINS(LCASE(STR(?lab)), LCASE("{label_en}")) )
    }} ORDER BY STRLEN(?lab) LIMIT 1
    """
    r = http_get(SPARQL_URL, {"query": q2})
    if r and r.status_code == 200:
        arr = r.json().get("results", {}).get("bindings", [])
        if arr:
            return arr[0]["occ"]["value"].split("/")[-1]
    return None

def resolve_profession_list(labels: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for lab in labels:
        qid = resolve_label_to_qid(lab)
        if qid:
            out[lab] = qid
            print(f"[harvest] resolved: {lab} -> {qid}")
        else:
            print(f"[harvest] WARN: no QID found for label '{lab}' (skipped)")
        time.sleep(0.2)
    return out

# ---------- fetch people (subclass-aware) ----------
def build_people_query(qid: str, limit_val: int) -> str:
    return f"""
    SELECT ?person ?personLabel ?occLabel ?givenNameLabel ?familyNameLabel ?lang
    WHERE {{
      ?person wdt:P106/wdt:P279* wd:{qid} .
      OPTIONAL {{ ?person wdt:P735 ?given . }}
      OPTIONAL {{ ?person wdt:P734 ?family . }}
      BIND(LANG(?personLabel) AS ?lang)
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }} LIMIT {limit_val}
    """

def fetch_people_for_qid(qid: str, limit_init: int) -> List[dict]:
    limit = max(5, limit_init)
    sleep = BASE_SLEEP
    for attempt in range(1, MAX_RETRIES + 1):
        q = build_people_query(qid, limit)
        r = http_get(SPARQL_URL, {"query": q})
        if r and r.status_code == 200:
            binds = r.json().get("results", {}).get("bindings", [])
            rows: List[dict] = []
            for b in binds:
                full  = b.get("personLabel",{}).get("value","").strip()
                given = b.get("givenNameLabel",{}).get("value","").strip()
                fam   = b.get("familyNameLabel",{}).get("value","").strip()
                occ   = b.get("occLabel",{}).get("value","").strip()
                lang  = (b.get("lang",{}).get("value","") or "en").strip()
                if not full:
                    full = " ".join([given, fam]).strip()
                if not full or not occ:
                    continue
                rows.append({
                    "full_name": full,
                    "given_name": given,
                    "surname": fam,
                    "profession_canonical": occ,
                    "language_origin": lang,
                })
            return rows

        # timeout / error: backoff; halve the limit to be kinder
        limit = max(5, limit // 2)
        time.sleep(min(MAX_SLEEP, sleep)); sleep *= 2
    return []

def get_env():
    base  = os.getenv("AIRTABLE_BASE_ID")
    tok   = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY") or ""
    table = os.getenv("AIRTABLE_TABLE_ID") or os.getenv("AIRTABLE_TABLE_NAME")
    if not (base and tok and table):
        sys.exit("[harvest] Missing AIRTABLE_* envs.")
    tok = tok.strip().replace("\r","").replace("\n","").replace("\t","")
    enc = urllib.parse.quote(table, safe="")
    base_url = f"https://api.airtable.com/v0/{base}/{enc}"
    H_AUTH = {"Authorization": f"Bearer {tok}"}
    H_JSON = {"Authorization": f"Bearer {tok}", "Content-Type":"application/json"}
    return base_url, H_AUTH, H_JSON

def upsert(base_url, H_AUTH, H_JSON, rows: List[dict]):
    def find(full_name, occ):
        def esc(s): return s.replace('"','\\"')
        formula = f'AND({{full_name}} = "{esc(full_name)}", {{profession_canonical}} = "{esc(occ)}")'
        r = http_get(base_url, {"filterByFormula": formula, "maxRecords": 1}, accept_json=False)
        if not r or r.status_code != 200: return None
        arr = r.json().get("records") or []
        return arr[0] if arr else None

    created = updated = 0
    seen = set()
    for r in rows:
        key = (r["full_name"].lower(), r["profession_canonical"].lower())
        if key in seen: 
            continue
        seen.add(key)
        full = r["full_name"]; occ = r["profession_canonical"]
        fields = {
            "full_name": full,
            "given_name": r.get("given_name",""),
            "surname": r.get("surname",""),
            "profession_canonical": occ,
            "language_origin": r.get("language_origin","") or "en",
        }
        existing = find(full, occ)
        if existing:
            rid = existing["id"]
            res = requests.patch(f"{base_url}/{rid}", headers=H_JSON, data=json.dumps({"fields":fields}), timeout=60)
            if res.status_code < 400: updated += 1
        else:
            payload = {"records":[{"fields":fields}]}
            res = requests.post(base_url, headers=H_JSON, data=json.dumps(payload), timeout=60)
            if res.status_code < 400: created += 1
        time.sleep(0.12)
    print(f"[harvest] upserted created={created} updated={updated}")

def main():
    labels = getenv_list("WIKIDATA_PROF_LIST", DEFAULT_PROF_LABELS)
    per_prof_limit = getenv_int("WIKIDATA_LIMIT_PER_PROF", DEFAULT_LIMIT_PER_PROF)

    print(f"[harvest] labels={labels}")
    print(f"[harvest] per_prof_limit={per_prof_limit}")

    prof_map = resolve_profession_list(labels)
    if not prof_map:
        print("[harvest] No profession QIDs resolved; nothing to do.")
        return
    print(f"[harvest] professions_resolved={len(prof_map)}")

    all_rows: List[dict] = []
    for lab, qid in prof_map.items():
        rows = fetch_people_for_qid(qid, per_prof_limit)
        print(f"[harvest] fetched {len(rows)} for {lab} ({qid})")
        all_rows.extend(rows)
        time.sleep(0.25)
    print(f"[harvest] total fetched rows={len(all_rows)}")

    if not all_rows:
        print("[harvest] nothing fetched.")
        return
    base_url, H_AUTH, H_JSON = get_env()
    upsert(base_url, H_AUTH, H_JSON, all_rows)

if __name__ == "__main__":
    main()

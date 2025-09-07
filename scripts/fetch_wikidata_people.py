#!/usr/bin/env python3
"""
Harvest people + professions from Wikidata and upsert into Airtable `Candidates`.

Resilient resolver + fetch:
- Label→QID: case-insensitive exact, then "contains" fallback
- Accept occupations OR subclasses of occupation (P31/P279* Q28640)
- People fetch: ?person wdt:P106/wdt:P279* wd:{qid}  (subclass-aware)
- Retries with exponential backoff on timeouts/429/5xx
- Auto-throttles: halves LIMIT on repeated timeouts
- Never aborts pipeline if some labels fail; continues with the rest
"""

import os, sys, time, json, urllib.parse, requests
from typing import Optional, Dict, List

UA = "name-profession-pipeline/2.3 (GitHub Actions; contact link in repo README)"
SPARQL_URL = "https://query.wikidata.org/sparql"

# Defaults (can be overridden by workflow env inputs)
DEFAULT_PROF_LABELS = ["Engineer", "Artist", "Mathematician", "Baker"]
DEFAULT_LIMIT_PER_PROF = 40

# HTTP / retry tuning
CONNECT_TO = 15          # seconds
READ_TO    = 120         # seconds
MAX_RETRIES = 5
BASE_SLEEP  = 1.0        # seconds (first backoff)
MAX_SLEEP   = 12.0       # seconds

def getenv_list(name: str, default: List[str]) -> List[str]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return [s.strip() for s in raw.split(",") if s.strip()]

def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, "").strip() or default)
    except Exception:
        return default

def http_get(url: str, params: Dict, accept_json=True) -> Optional[requests.Response]:
    headers = {
        "User-Agent": UA,
        "Accept": "application/sparql-results+json" if accept_json else "*/*",
    }
    sleep = BASE_SLEEP
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=(CONNECT_TO, READ_TO),   # (connect, read)
            )
            # Retry on common transient statuses
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
            time.sleep(min(MAX_SLEEP, sleep))
            sleep *= 2
    return None

# ---------- label -> QID ----------
def resolve_label_to_qid(label_en: str) -> Optional[str]:
    # 1) case-insensitive exact match
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

    # 2) fallback: "contains" (shortest label first)
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
        time.sleep(0.2)  # be polite
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
                full  = b.get("personLabel",{}).get("value","").strip

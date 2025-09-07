#!/usr/bin/env python3
"""
Harvest people + professions from Wikidata and upsert into Airtable `Candidates`.

Robust label→QID resolver:
- Case-insensitive exact match first
- Then "contains" match as fallback
- Restricts to occupation or subclass-of-occupation (P31/P279* Q28640)
- Continues even if some labels don't resolve

ENV required:
  AIRTABLE_BASE_ID, AIRTABLE_TOKEN (or AIRTABLE_API_KEY),
  and (AIRTABLE_TABLE_ID or AIRTABLE_TABLE_NAME)

Optional ENV:
  WIKIDATA_PROF_LIST       -> comma-separated English labels
  WIKIDATA_LIMIT_PER_PROF  -> per-profession cap (default 40)
"""
import os, sys, time, json, urllib.parse, requests

UA = "name-profession-pipeline/2.1 (GitHub Actions; contact link in repo README)"
SPARQL = "https://query.wikidata.org/sparql"

DEFAULT_PROF_LABELS = [
    # STEM / Medical
    "Engineer","Electrical engineer","Mechanical engineer","Civil engineer","Computer scientist",
    "Programmer","Data scientist","Statistician","Physicist","Chemist","Biologist","Geologist",
    "Meteorologist","Astronomer","Astrophysicist","Oceanographer","Environmental scientist",
    "Mathematician","Doctor","Physician","Surgeon","Psychologist","Psychiatrist","Pharmacist",
    "Dentist","Veterinarian","Nurse",
    # Humanities / Social
    "Historian","Archaeologist","Anthropologist","Sociologist","Economist","Philosopher","Linguist",
    "Teacher","Professor","Lecturer","Librarian","Translator","Interpreter",
    # Law / Public
    "Lawyer","Attorney","Judge","Police officer","Firefighter","Soldier","Diplomat","Civil servant",
    "Politician",
    # Business / Trades
    "Businessperson","Entrepreneur","Manager","Accountant","Salesperson","Marketing professional",
    "Architect","Carpenter","Mason","Plumber","Electrician","Mechanic","Driver","Pilot","Sailor",
    "Farmer","Fisher","Chef","Cook","Baker","Butcher",
    # Arts / Media / Sports
    "Artist","Painter","Sculptor","Illustrator","Designer","Fashion designer","Photographer",
    "Actor","Actress","Film director","Screenwriter","Producer","Journalist","Editor","Writer",
    "Author","Poet","Novelist","Musician","Singer","Composer","Pianist","Violinist","Guitarist",
    "Dancer","Choreographer","Athlete","Footballer","Basketball player","Tennis player",
]

def http_get(url, params=None, headers=None, timeout=60):
    h = {"User-Agent": UA}
    if headers: h.update(headers)
    for i in range(4):
        r = requests.get(url, params=params, headers=h, timeout=timeout)
        if r.status_code in (429, 502, 503, 504):
            time.sleep(1.5 * (i + 1))
            continue
        return r
    return r

# --- label -> QID (case-insensitive) ---
def resolve_label_to_qid(label_en: str) -> str | None:
    # 1) case-insensitive exact match
    q1 = f"""
    SELECT ?occ WHERE {{
      ?occ wdt:P31/wdt:P279* wd:Q28640 .
      ?occ rdfs:label ?lab FILTER(LANG(?lab)="en").
      FILTER( LCASE(STR(?lab)) = LCASE("{label_en}") )
    }} LIMIT 1
    """
    r = http_get(SPARQL, {"query": q1}, headers={"Accept":"application/sparql-results+json"})
    if r.status_code == 200:
        arr = r.json().get("results", {}).get("bindings", [])
        if arr:
            return arr[0]["occ"]["value"].split("/")[-1]

    # 2) fallback: label contains substring (pick the shortest label)
    q2 = f"""
    SELECT ?occ ?lab WHERE {{
      ?occ wdt:P31/wdt:P279* wd:Q28640 .
      ?occ rdfs:label ?lab FILTER(LANG(?lab)="en").
      FILTER( CONTAINS(LCASE(STR(?lab)), LCASE("{label_en}")) )
    }} ORDER BY STRLEN(?lab) LIMIT 1
    """
    r = http_get(SPARQL, {"query": q2}, headers={"Accept":"application/sparql-results+json"})
    if r.status_code == 200:
        arr = r.json().get("results", {}).get("bindings", [])
        if arr:
            return arr[0]["occ"]["value"].split("/")[-1]

    return None

def resolve_profession_list(labels: list[str]) -> dict[str, str]:
    out = {}
    for lab in labels:
        lab = lab.strip()
        if not lab: continue
        qid = resolve_label_to_qid(lab)
        if qid:
            out[lab] = qid
        else:
            print(f"[harvest] WARN: no QID found for label '{lab}' (skipped)")
        time.sleep(0.2)  # be polite to endpoint
    return out

def fetch_people_for_qid(qid: str, per_prof_limit: int) -> list[dict]:
    query = f"""
    SELECT ?person ?personLabel ?occLabel ?givenNameLabel ?familyNameLabel ?lang
    WHERE {{
      ?person wdt:P106 wd:{qid} .
      OPTIONAL {{ ?person wdt:P735 ?given . }}
      OPTIONAL {{ ?person wdt:P734 ?family . }}
      BIND(LANG(?personLabel) AS ?lang)
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }} LIMIT {per_prof_limit}
    """
    r = http_get(SPARQL, {"query": query}, headers={"Accept":"application/sparql-results+json"})
    if r.status_code != 200:
        print(f"[harvest] SPARQL error {r.status_code}: {r.text[:300]}")
        return []
    rows = []
    for b in r.json().get("results", {}).get("bindings", []):
        full  = b.get("personLabel",{}).get("value","").strip()
        given = b.get("givenNameLabel",{}).get("value","").strip()
        fam   = b.get("familyNameLabel",{}).get("value","").strip()
        occ   = b.get("occLabel",{}).get("value","").strip()
        lang  = (b.get("lang",{}).get("value","") or "en").strip()
        if not full: full = " ".join([given,fam]).strip()
        if not full or not occ: continue
        rows.append({
            "full_name": full,
            "given_name": given,
            "surname": fam,
            "profession_canonical": occ,
            "language_origin": lang,
        })
    return rows

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

def upsert(base_url, H_AUTH, H_JSON, rows):
    def find(full_name, occ):
        def esc(s): return s.replace('"','\\"')
        formula = f'AND({{full_name}} = "{esc(full_name)}", {{profession_canonical}} = "{esc(occ)}")'
        r = http_get(base_url, {"filterByFormula": formula, "maxRecords": 1}, headers=H_AUTH)
        if r.status_code!=200: return None
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
    labels_env = os.getenv("WIKIDATA_PROF_LIST","").strip()
    labels = [s.strip() for s in labels_env.split(",") if s.strip()] if labels_env else DEFAULT_PROF_LABELS
    per_prof_limit = int(os.getenv("WIKIDATA_LIMIT_PER_PROF","40"))

    prof_map = resolve_profession_list(labels)
    if not prof_map:
        print("[harvest] No profession QIDs resolved; nothing to do.")
        return  # <— don't fail the pipeline; just skip
    print(f"[harvest] professions_resolved={len(prof_map)}")

    all_rows = []
    for lab, qid in prof_map.items():
        rows = fetch_people_for_qid(qid, per_prof_limit)
        all_rows.extend(rows)
        time.sleep(0.25)
    print(f"[harvest] fetched rows={len(all_rows)}")

    if not all_rows:
        print("[harvest] nothing fetched.")
        return
    base_url, H_AUTH, H_JSON = get_env()
    upsert(base_url, H_AUTH, H_JSON, all_rows)

if __name__ == "__main__":
    main()

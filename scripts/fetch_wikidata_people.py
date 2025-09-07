#!/usr/bin/env python3
"""
Harvest people+professions from Wikidata and upsert into Airtable Candidates.
- Source: Wikidata SPARQL (public endpoint)
- De-dupe key: (full_name + profession_canonical)
- Fields written: full_name, given_name, surname, profession_canonical, language_origin

Secrets reused: AIRTABLE_BASE_ID, AIRTABLE_TOKEN, AIRTABLE_TABLE_ID or AIRTABLE_TABLE_NAME
"""
import os, sys, time, json, urllib.parse, requests

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
UA = "name-profession-pipeline/1.0 (GitHub Actions; contact link in repo README)"

# Select a seed list of professions (add more Q-IDs anytime)
PROF_QIDS = {
    "Engineer":      "Q81096",
    "Artist":        "Q483501",
    "Mathematician": "Q170790",
    "Baker":         "Q80056",
    # "Carpenter":     "Q127843",  # example: uncomment to add
}

SPARQL_TPL = """
SELECT ?person ?personLabel ?givenNameLabel ?familyNameLabel ?occLabel ?lang
WHERE {
  VALUES ?occ { %OCCS% }                # restrict to our occupations
  ?person wdt:P106 ?occ .
  OPTIONAL { ?person wdt:P735 ?given . }
  OPTIONAL { ?person wdt:P734 ?family . }

  # best-effort language hint from person native label
  BIND(IF(BOUND(?person), LANG(?personLabel), "") AS ?lang)

  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
LIMIT %LIMIT%
"""

def qids_braced(ids): return " ".join(f"wd:{qid}" for qid in ids)

def run_sparql(limit=200):
    query = SPARQL_TPL.replace("%OCCS%", qids_braced(PROF_QIDS.values())).replace("%LIMIT%", str(limit))
    r = requests.get(
        WIKIDATA_SPARQL,
        headers={"Accept":"application/sparql-results+json","User-Agent":UA},
        params={"query":query},
        timeout=60
    )
    if r.status_code != 200:
        sys.exit(f"[harvest] SPARQL error {r.status_code}: {r.text[:400]}")
    return r.json().get("results", {}).get("bindings", [])

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
        # filterByFormula to check existing record
        def esc(s): return s.replace('"','\\"')
        formula = f'AND({{full_name}} = "{esc(full_name)}", {{profession_canonical}} = "{esc(occ)}")'
        r = requests.get(base_url, headers=H_AUTH, params={"filterByFormula":formula,"maxRecords":1}, timeout=30)
        if r.status_code!=200: return None
        arr = r.json().get("records") or []
        return arr[0] if arr else None

    created, updated = 0, 0
    for r in rows:
        full = r["full_name"]; occ = r["profession_canonical"]
        existing = find(full, occ)
        fields = {
            "full_name": full,
            "given_name": r.get("given_name",""),
            "surname": r.get("surname",""),
            "profession_canonical": occ,
            "language_origin": r.get("language_origin","")
        }
        if existing:
            rid = existing["id"]
            res = requests.patch(f"{base_url}/{rid}", headers=H_JSON, data=json.dumps({"fields":fields}), timeout=60)
            if res.status_code<400: updated+=1
        else:
            payload = {"records":[{"fields":fields}]}
            res = requests.post(base_url, headers=H_JSON, data=json.dumps(payload), timeout=60)
            if res.status_code<400: created+=1
        time.sleep(0.15)
    print(f"[harvest] upserted created={created} updated={updated}")

def main():
    results = run_sparql(limit=int(os.getenv("WIKIDATA_LIMIT","200")))
    rows=[]
    for b in results:
        label = b.get("personLabel",{}).get("value","").strip()
        given = b.get("givenNameLabel",{}).get("value","").strip()
        fam   = b.get("familyNameLabel",{}).get("value","").strip()
        occ   = b.get("occLabel",{}).get("value","").strip()
        lang  = b.get("lang",{}).get("value","").strip() or "en"
        full  = label or " ".join([given,fam]).strip()
        if not full or not occ: continue
        rows.append({
            "full_name": full,
            "given_name": given,
            "surname": fam,
            "profession_canonical": occ,
            "language_origin": lang
        })
    print(f"[harvest] fetched={len(rows)}")
    if not rows: return
    base_url, H_AUTH, H_JSON = get_env()
    upsert(base_url, H_AUTH, H_JSON, rows)

if __name__ == "__main__":
    main()

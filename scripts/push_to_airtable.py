import os, csv, json, time, requests, sys
API = "https://api.airtable.com/v0"

def batched(it, n=10):
    buf=[]
    for x in it:
        buf.append(x)
        if len(buf)==n: yield buf; buf=[]
    if buf: yield buf

def map_row(r):
    mt = "Exact homonym" if r.get("lexical_match","0")=="1" else ("Phonetic" if float(r.get("phonetic_score","0") or 0)>=80 else "Morphological/Other")
    return {
        "id": r.get("person",""),
        "full_name": r.get("personLabel",""),
        "given_name": "",
        "surname": r.get("surnameLabel",""),
        "language_origin": "",
        "profession_canonical": r.get("occupationLabel",""),
        "profession_cluster": r.get("cluster",""),
        "match_type": mt,
        "match_strength": "Candidate",
        "etymology_note": "",
        "sources_primary": "",
        "sources_etymology": "",
        "notes_qc": "Imported by Discovery job",
        "reviewer_id": "",
        "review_date": "",
        "status": "Candidate"
    }

if __name__=="__main__":
    base  = os.getenv("AIRTABLE_BASE_ID")
    key   = os.getenv("AIRTABLE_API_KEY")
    table = os.getenv("AIRTABLE_TABLE_NAME","Candidates")
    csv_path = sys.argv[sys.argv.index("--csv")+1] if "--csv" in sys.argv else "data/candidates_raw.csv"
    assert base and key, "Missing Airtable secrets"
    headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"}
    url=f"{API}/{base}/{table}"
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows=list(csv.DictReader(f))
    print(f"Uploading {len(rows)} rows…")
    for batch in batched(rows,10):
        payload={"records":[{"fields":map_row(r)} for r in batch],"typecast":True}
        resp=requests.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code>=300: print("Error:",resp.status_code,resp.text); raise SystemExit(1)
        time.sleep(0.4)
    print("Done.")

import os, csv, json, time, requests, sys
API = "https://api.airtable.com/v0"
BATCH = 10

def map_row(r):
    try:
        phon = float(r.get("phonetic_score","0") or 0)
    except Exception:
        phon = 0.0
    mt = "Exact homonym" if str(r.get("lexical_match","0")) == "1" else ("Phonetic" if phon >= 80 else "Morphological/Other")
    person_url = r.get("person","")
    return {
        "id": person_url,
        "full_name": r.get("personLabel",""),
        "given_name": "",
        "surname": r.get("surnameLabel",""),
        "language_origin": "",
        "profession_canonical": r.get("occupationLabel",""),
        "profession_cluster": r.get("cluster",""),
        "match_type": mt,
        "match_strength": "Candidate",
        "etymology_note": "",
        "sources_primary": person_url,
        "sources_etymology": "",
        "notes_qc": "Imported by Discovery job",
        "reviewer_id": "",
        "review_date": "",
        "status": "Candidate"
    }

def fetch_existing_id_map(base, table, key):
    headers = {"Authorization": f"Bearer {key}"}
    url = f"{API}/{base}/{table}?fields[]=id&pageSize=100"
    rec_map = {}
    while True:
        resp = requests.get(url, headers=headers)
        if resp.status_code >= 300:
            print("Fetch error:", resp.status_code, resp.text); break
        data = resp.json()
        for rec in data.get("records", []):
            rec_id = rec["id"]
            val = rec.get("fields", {}).get("id")
            if val:
                rec_map[val] = rec_id
        off = data.get("offset")
        if not off: break
        url = f"{API}/{base}/{table}?fields[]=id&pageSize=100&offset={off}"
        time.sleep(0.2)
    print(f"Existing Airtable ids: {len(rec_map)}")
    return rec_map

def create_records(base, table, key, rows):
    if not rows: return
    headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"}
    url=f"{API}/{base}/{table}"
    for i in range(0, len(rows), BATCH):
        payload={"records":[{"fields":map_row(r)} for r in rows[i:i+BATCH]],"typecast":True}
        resp=requests.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code>=300:
            print("Create error:",resp.status_code,resp.text); raise SystemExit(1)
        time.sleep(0.4)

def update_records(base, table, key, updates):
    if not updates: return
    headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"}
    url=f"{API}/{base}/{table}"
    for i in range(0, len(updates), BATCH):
        payload={"records":[{"id":rid,"fields":fields} for rid,fields in updates[i:i+BATCH]],"typecast":True}
        resp=requests.patch(url, headers=headers, data=json.dumps(payload))
        if resp.status_code>=300:
            print("Update error:",resp.status_code,resp.text); raise SystemExit(1)
        time.sleep(0.4)

if __name__=="__main__":
    base  = os.getenv("AIRTABLE_BASE_ID")
    key   = os.getenv("AIRTABLE_API_KEY")
    table = os.getenv("AIRTABLE_TABLE_NAME","Candidates")
    csv_path = sys.argv[sys.argv.index("--csv")+1] if "--csv" in sys.argv else "data/candidates_raw.csv"
    assert base and key and table, "Missing Airtable secrets"
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows=list(csv.DictReader(f))
    print(f"CSV rows: {len(rows)}")
    id_map = fetch_existing_id_map(base, table, key)
    to_create, to_update = [], []
    for r in rows:
        fields = map_row(r)
        person_id = fields["id"]
        if person_id in id_map:
            to_update.append((id_map[person_id], fields))
        else:
            to_create.append(r)
    print(f"Create: {len(to_create)} | Update: {len(to_update)}")
    create_records(base, table, key, to_create)
    update_records(base, table, key, to_update)
    print("Upsert complete.")

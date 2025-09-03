import os, csv, json, time, requests, sys, re

API = "https://api.airtable.com/v0"
BATCH = 10

def norm(name: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', (name or '').strip().lower())

def fetch_table_fields(base_id: str, table_id_or_name: str, key: str):
    url = f"{API}/meta/bases/{base_id}/tables"
    h = {"Authorization": f"Bearer {key}"}
    r = requests.get(url, headers=h)
    if r.status_code >= 300:
        print("Meta fetch error:", r.status_code, r.text); raise SystemExit(1)
    data = r.json()
    tid_norm = norm(table_id_or_name)
    for t in data.get("tables", []):
        if t.get("id") == table_id_or_name or norm(t.get("name","")) == tid_norm:
            fields = [f.get("name") for f in t.get("fields", []) if f.get("name")]
            return t.get("id"), t.get("name"), fields
    print("Could not find table in meta:", table_id_or_name); raise SystemExit(1)

def build_field_mapping(available_fields):
    avail_by_norm = {norm(f): f for f in available_fields}
    canonical = [
        "id","full_name","given_name","surname","language_origin",
        "profession_canonical","profession_cluster",
        "match_type","match_strength",
        "etymology_note","sources_primary","sources_etymology",
        "notes_qc","reviewer_id","review_date","status"
    ]
    mapping = {}
    hints = {"status": "Status", "match_type": "match-type"}
    for k in canonical:
        k_norm = norm(k)
        if k_norm in avail_by_norm:
            mapping[k] = avail_by_norm[k_norm]; continue
        if k in hints and norm(hints[k]) in avail_by_norm:
            mapping[k] = avail_by_norm[norm(hints[k])]; continue
    print("Field mapping (canonical -> table):")
    for ck, tk in mapping.items():
        print(f"  {ck} -> {tk}")
    skipped = [k for k in canonical if k not in mapping]
    if skipped:
        print("Skipping fields not present in table:", ", ".join(skipped))
    return mapping

def map_row_to_fields(r, mapping):
    try:
        phon = float(r.get("phonetic_score","0") or 0)
    except Exception:
        phon = 0.0
    mt = "Exact homonym" if str(r.get("lexical_match","0")) == "1" else ("Phonetic" if phon >= 80 else "Morphological/Other")
    canonical = {
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
        "sources_primary": r.get("person",""),
        "sources_etymology": "",
        "notes_qc": "Imported by Discovery job",
        "reviewer_id": "",
        "review_date": "",
        "status": "Candidate"
    }
    out={}
    for ck,val in canonical.items():
        tk=mapping.get(ck)
        if tk: out[tk]=val
    return out

def fetch_existing_id_map(base, table, key, id_field_actual):
    headers={"Authorization":f"Bearer {key}"}
    url=f"{API}/{base}/{table}?fields[]={id_field_actual}&pageSize=100"
    rec_map={}
    while True:
        resp=requests.get(url, headers=headers)
        if resp.status_code>=300:
            print("Fetch error:", resp.status_code, resp.text); break
        data=resp.json()
        for rec in data.get("records", []):
            rec_id=rec["id"]
            val=rec.get("fields",{}).get(id_field_actual)
            if val: rec_map[val]=rec_id
        off=data.get("offset")
        if not off: break
        url=f"{API}/{base}/{table}?fields[]={id_field_actual}&pageSize=100&offset={off}"
        time.sleep(0.2)
    print(f"Existing Airtable ids: {len(rec_map)}")
    return rec_map

def create_records(base, table, key, rows):
    if not rows: return
    headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"}
    url=f"{API}/{base}/{table}"
    for i in range(0,len(rows),BATCH):
        batch=rows[i:i+BATCH]
        payload={"records":[{"fields":row} for row in batch], "typecast":True}
        resp=requests.post(url, headers=headers, data=json.dumps(payload))
        if resp.status_code>=300:
            print("Create error:", resp.status_code, resp.text); raise SystemExit(1)
        time.sleep(0.4)

def update_records(base, table, key, updates):
    if not updates: return
    headers={"Authorization":f"Bearer {key}","Content-Type":"application/json"}
    url=f"{API}/{base}/{table}"
    for i in range(0,len(updates),BATCH):
        batch=updates[i:i+BATCH]
        payload={"records":[{"id":rid,"fields":fields} for rid,fields in batch], "typecast":True}
        resp=requests.patch(url, headers=headers, data=json.dumps(payload))
        if resp.status_code>=300:
            print("Update error:", resp.status_code, resp.text); raise SystemExit(1)
        time.sleep(0.4)

if __name__=="__main__":
    base  = os.getenv("AIRTABLE_BASE_ID")
    key   = os.getenv("AIRTABLE_API_KEY")
    table = os.getenv("AIRTABLE_TABLE_NAME","Candidates")
    csv_path = sys.argv[sys.argv.index("--csv")+1] if "--csv" in sys.argv else "data/candidates_raw.csv"
    assert base and key and table, "Missing Airtable secrets"

    table_id, table_name, field_names = fetch_table_fields(base, table, key)
    mapping = build_field_mapping(field_names)
    if "id" not in mapping:
        print("❌ Your table must have a text field named 'id'. Please add it and re-run.")
        raise SystemExit(1)
    id_field_actual = mapping["id"]

    with open(csv_path, newline="", encoding="utf-8") as f:
        rows=list(csv.DictReader(f))
    print(f"CSV rows: {len(rows)}")

    id_map = fetch_existing_id_map(base, table_id, key, id_field_actual)
    to_create, to_update = [], []
    for r in rows:
        fields = map_row_to_fields(r, mapping)
        person_id = fields.get(id_field_actual, "")
        if not person_id: continue
        if person_id in id_map:
            to_update.append((id_map[person_id], fields))
        else:
            to_create.append(fields)

    print(f"Create: {len(to_create)} | Update: {len(to_update)}")
    create_records(base, table_id, key, to_create)
    update_records(base, table_id, key, to_update)
    print("Upsert complete.")

#!/usr/bin/env python3
import csv, os, time, json, urllib.parse, sys
import requests

API_KEY = os.environ.get("AIRTABLE_API_KEY","").strip()
BASE_ID = os.environ.get("AIRTABLE_BASE_ID","").strip()
TABLE   = os.environ.get("AIRTABLE_TABLE_NAME","").strip()  # may be table NAME or table ID

if not (API_KEY and BASE_ID and TABLE):
    print("❌ Missing one of AIRTABLE_API_KEY / AIRTABLE_BASE_ID / AIRTABLE_TABLE_NAME")
    sys.exit(2)

API_ROOT = f"https://api.airtable.com/v0/{BASE_ID}/{urllib.parse.quote(TABLE)}"
HDRS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

# Send only plain text fields to avoid 422 invalid_multiple_choice_options
# (Airtable returns 422 when a select option doesn't exist). 
# We'll add selects back after confirming rows land. 
# Ref: Airtable Web API troubleshooting. 
# https://www.airtable.com/developers/web/api/create-records
SAFE_FIELDS = ["full_name","id","given_name","surname","profession_canonical"]

def _escape_formula_value(s: str) -> str:
    return s.replace("'", "''")

def find_existing_record_id(wk_id: str) -> str | None:
    formula = f"{{id}} = '{_escape_formula_value(wk_id)}'"
    url = f"{API_ROOT}?maxRecords=1&filterByFormula={urllib.parse.quote(formula)}"
    r = requests.get(url, headers=HDRS, timeout=30)
    if r.status_code != 200:
        print(f"[AIRTABLE] FIND failed HTTP {r.status_code}: {r.text}")
        return None
    recs = r.json().get("records", [])
    return recs[0]["id"] if recs else None

def chunk(iterable, size=10):
    buf=[]
    for x in iterable:
        buf.append(x)
        if len(buf)>=size:
            yield buf; buf=[]
    if buf: yield buf

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()

    rows=[]
    with open(args.csv, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            full = (r.get("personLabel","") or "").strip()
            wid  = (r.get("person","") or "").strip()
            if not wid: 
                continue
            surname = (r.get("surnameLabel","") or "").strip()
            occ = (r.get("occupationLabel","") or "").strip()
            given = (full.split()[0] if full else "")
            fields = {
                "full_name": full,
                "id": wid,
                "given_name": given,
                "surname": surname,
                "profession_canonical": occ
            }
            rows.append(fields)

    print(f"[AIRTABLE] Prepared {len(rows)} rows for upsert (text fields only).")

    created=updated=0
    for batch in chunk(rows, size=10):
        updates=[]; creates=[]
        for fields in batch:
            rid = find_existing_record_id(fields["id"])
            if rid: updates.append({"id": rid, "fields": fields})
            else:   creates.append({"fields": fields})
            time.sleep(0.15)

        if creates:
            r = requests.post(API_ROOT, headers=HDRS, data=json.dumps({"records": creates}))
            if r.status_code not in (200,201):
                print(f"[AIRTABLE] CREATE failed HTTP {r.status_code}: {r.text}")
            else:
                n=len(r.json().get("records",[])); created += n
                print(f"[AIRTABLE] Created {n} records.")

        if updates:
            r = requests.patch(API_ROOT, headers=HDRS, data=json.dumps({"records": updates}))
            if r.status_code != 200:
                print(f"[AIRTABLE] UPDATE failed HTTP {r.status_code}: {r.text}")
            else:
                n=len(r.json().get("records",[])); updated += n
                print(f"[AIRTABLE] Updated {n} records.")

        time.sleep(0.3)

    print(f"[AIRTABLE] Upsert complete. Created: {created} | Updated: {updated}")

if __name__ == "__main__":
    main()

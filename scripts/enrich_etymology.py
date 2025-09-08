#!/usr/bin/env python3
"""
Enrich Airtable rows with etymology-ish info.

Always writes:
  - name_roots (JSON list of root tokens)  ← guarantees downstream narrative
Optionally writes (if we infer them):
  - name_meaning (short gloss, written only if empty)
  - name_origin  (from language_origin)
  - etymology_source ("lexicon" or "heuristic")
"""

import os, sys, json, time, re, urllib.parse, requests
import unicodedata

# ---------------- Airtable helpers ----------------
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

# ---------------- helpers ----------------
def strip_diacritics(s: str) -> str:
    # "Jörg" -> "Jorg"; "Müller" -> "Muller"
    return ''.join(c for c in unicodedata.normalize('NFD', s or "") if unicodedata.category(c) != 'Mn')

def clean(s: str) -> str:
    s = strip_diacritics(s).lower()
    return re.sub(r"[^a-z\s\-]", " ", s).strip()

def toks(s: str):
    return [t for t in clean(s).split() if len(t) >= 2]

# ---------- lexicon (expand freely) ----------
# Canonical given-name meanings (lowercase, no diacritics)
GIVEN = {
    "nikola":   {"meaning":"victory of the people","roots":["nike","laos"],"origin":"Greek","src":"lexicon"},
    "ada":      {"meaning":"noble","roots":["noble"],"origin":"Germanic","src":"lexicon"},
    "leonardo": {"meaning":"lion-strong","roots":["leo","hard"],"origin":"Germanic+Latin","src":"lexicon"},

    # ——— Germanic & European set we added ———
    "gustav":   {"meaning":"staff of the Goths","roots":["goth","staff"],"origin":"Germanic","src":"lexicon"},
    "wilhelm":  {"meaning":"will-helmet","roots":["will","helm"],"origin":"Germanic","src":"lexicon"},
    "willi":    {"meaning":"will-helmet","roots":["will","helm"],"origin":"Germanic","src":"lexicon"},
    "willy":    {"meaning":"will-helmet","roots":["will","helm"],"origin":"Germanic","src":"lexicon"},
    "august":   {"meaning":"venerable","roots":["venerable"],"origin":"Latin","src":"lexicon"},
    "rudolf":   {"meaning":"fame-wolf","roots":["fame","wolf"],"origin":"Germanic","src":"lexicon"},
    "paul":     {"meaning":"small; humble","roots":["small","humble"],"origin":"Latin","src":"lexicon"},
    "anton":    {"meaning":"priceless; invaluable","roots":["priceless","invaluable"],"origin":"Latin","src":"lexicon"},
    "albrecht": {"meaning":"noble-bright","roots":["noble","bright"],"origin":"Germanic","src":"lexicon"},
    "georg":    {"meaning":"earth-worker; farmer","roots":["earth","work","farmer"],"origin":"Greek","src":"lexicon"},
    "jorg":     {"meaning":"earth-worker; farmer","roots":["earth","work","farmer"],"origin":"Greek","src":"lexicon"},
    "joerg":    {"meaning":"earth-worker; farmer","roots":["earth","work","farmer"],"origin":"Greek","src":"lexicon"},
    "george":   {"meaning":"earth-worker; farmer","roots":["earth","work","farmer"],"origin":"Greek","src":"lexicon"},
    "emil":     {"meaning":"rival","roots":["rival"],"origin":"Latin","src":"lexicon"},
    "heinrich": {"meaning":"home-ruler","roots":["home","ruler"],"origin":"Germanic","src":"lexicon"},
    "heinz":    {"meaning":"home-ruler","roots":["home","ruler"],"origin":"Germanic","src":"lexicon"},
    "arthur":   {"meaning":"bear-man","roots":["bear","man"],"origin":"Celtic","src":"lexicon"},
    "friedrich":{"meaning":"peace-ruler","roots":["peace","ruler"],"origin":"Germanic","src":"lexicon"},
    "jean":     {"meaning":"God is gracious","roots":["god","grace"],"origin":"Hebrew→French","src":"lexicon"},
    "otto":     {"meaning":"wealth, prosperity","roots":["wealth","prosperity"],"origin":"Germanic","src":"lexicon"},
    "galileo":  {"meaning":"from Galilee (place)","roots":["galilee","place"],"origin":"Hebrew/Latin","src":"lexicon"},
    "michelangelo":{"meaning":"Michael + messenger","roots":["michael","angel"],"origin":"Hebrew/Greek","src":"lexicon"},
}

# surname lexicon (lowercase, no diacritics)
SURNAME = {
    "tesla":   {"meaning":"adze; carpenter","roots":["adze","carpenter"],"origin":"Slavic","src":"lexicon"},
    "smith":   {"meaning":"metalworker","roots":["smith","metal","forge"],"origin":"English","src":"lexicon"},
    "miller":  {"meaning":"operates a mill","roots":["mill","grain"],"origin":"English","src":"lexicon"},
    "baker":   {"meaning":"bakes bread","roots":["bake","bread","oven"],"origin":"English","src":"lexicon"},
    "fisher":  {"meaning":"fisher","roots":["fish","river"],"origin":"English","src":"lexicon"},
    "carpenter":{"meaning":"woodworker","roots":["wood","carpenter"],"origin":"French/Latin","src":"lexicon"},
    "painter": {"meaning":"painter","roots":["paint","color"],"origin":"English","src":"lexicon"},
    "da vinci":{"meaning":"from Vinci (place)","roots":["vinci","place"],"origin":"Italian","src":"lexicon"},
}

# occupational / German variants normalized (all lowercase, no diacritics)
OCC_VAR = {
    "schmidt":"smith","schmitt":"smith","schmid":"smith","schmied":"smith",
    "muller":"miller","mueller":"miller","miller":"miller",
    "backer":"baker","baecker":"baker","backer":"baker","backmann":"baker",
    "fischer":"fisher","schneider":"tailor",
    "zimmermann":"carpenter","zimmerman":"carpenter",
    "bauer":"farmer",
}

LOC_PREFIXES = {"von","van","de","da","di"}

def derive(full, given, sur):
    roots, parts, notes = set(), [], []
    fk = clean(full or "")
    gk = clean(given or "")
    sk = clean(sur or "")

    # pick given token
    gn = gk or (fk.split()[0] if fk else "")

    # 1) GIVEN lexicon
    if gn in GIVEN:
        d = GIVEN[gn]; roots.update(d["roots"]); parts.append(d["meaning"]); notes.append(d["src"])

    # 2) Locatives: von/van/de/da/di + place token
    ftoks = fk.split()
    if ftoks and ftoks[0] in LOC_PREFIXES and len(ftoks) >= 2:
        roots.update({"place", ftoks[-1]})
        parts.append(f"from {ftoks[-1].title()}")
        notes.append("heuristic")

    # 3) Surname lexicon (entire surname or last token of surname phrase)
    s_try = sk or (" ".join(ftoks[1:]) if fk and len(ftoks) > 1 else "")
    s_last = s_try.split()[-1] if s_try else ""
    if s_try in SURNAME:
        d = SURNAME[s_try]; roots.update(d["roots"]); parts.append(d["meaning"]); notes.append(d["src"])
    elif s_last in SURNAME:
        d = SURNAME[s_last]; roots.update(d["roots"]); parts.append(d["meaning"]); notes.append(d["src"])

    # 4) Occupational variants (German forms)
    if sk in OCC_VAR:
        occ = OCC_VAR[sk]; notes.append("heuristic")
        if occ == "miller": roots.update({"mill","grain"}); parts.append("miller")
        elif occ == "baker": roots.update({"bake","bread","oven"}); parts.append("baker")
        elif occ == "fisher": roots.update({"fish","river"}); parts.append("fisher")
        elif occ == "tailor": roots.update({"tailor","cut","cloth"}); parts.append("tailor")
        elif occ == "carpenter": roots.update({"carpenter","wood"}); parts.append("carpenter")
        elif occ == "smith": roots.update({"smith","metal","forge"}); parts.append("smith")
        elif occ == "farmer": roots.update({"farmer","field","earth"}); parts.append("farmer")

    # 5) Germanic compounds in surname (schwarz-kopf → schwarz, kopf)
    comp = re.findall(r"[a-z]+", sk)
    if len(comp) >= 2:
        roots.update(comp); notes.append("compound")

    # 6) FALLBACK roots (so downstream scoring always has tokens)
    roots.update(toks(gn))
    roots.update(toks(sk))

    gloss = "; ".join(parts) if parts else ""
    src = " + ".join(sorted(set(notes))) if notes else "heuristic"
    return list(sorted(roots)), gloss, src

def main():
    base_url, H_AUTH, H_JSON = _env()
    rows = fetch_all(base_url, H_AUTH)
    updated = 0
    for rec in rows:
        f = rec.get("fields", {}); rid = rec.get("id")
        full = f.get("full_name") or ""; given = f.get("given_name") or ""; sur = f.get("surname") or ""
        if not (full or given or sur): continue

        roots, gloss, src = derive(full, given, sur)
        out = {
            "name_roots": json.dumps(roots, ensure_ascii=False),
            "etymology_source": src
        }
        # only set gloss if empty — analyzer will append the narrative later
        if gloss and not f.get("name_meaning"):
            out["name_meaning"] = f"Meaning: {gloss}"

        origin = f.get("language_origin") or f.get("name_origin")
        if origin: out["name_origin"] = origin

        r = patch(base_url, H_JSON, rid, out)
        if r.status_code < 400:
            updated += 1; time.sleep(0.1)
    print(f"[enrich] updated={updated}")

if __name__ == "__main__":
    main()

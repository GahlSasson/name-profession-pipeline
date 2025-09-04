#!/usr/bin/env python3
import argparse, csv, json, re, sys, time
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "queries" / "query_template.sparql"
DICT_DIR = ROOT / "dictionaries"
ENDPOINT = "https://query.wikidata.org/sparql"
UA = "Name-Profession-Pipeline/1.0 (+https://github.com/GahlSasson/name-profession-pipeline)"

def load_json(p: Path):
    return json.loads(p.read_text(encoding="utf-8"))

def safe_prefix_regex(prefixes):
    """
    Build OR-of-prefixes, each as ^<prefix>.* (no \\w; Blazegraph chokes on it).
    Escapes regex metacharacters and double-quotes for SPARQL string.
    """
    alts = []
    for pref in prefixes:
        pref = (pref or "").strip()
        if not pref:
            continue
        alts.append("^" + re.escape(pref) + ".*")
    if not alts:
        return None
    patt = "(" + "|".join(alts) + ")"
    return patt.replace('"', '\\"')

def occ_filter_block(occ: str, mode: str):
    if mode == "strict":
        patt = re.escape(occ.lower()).replace('"', '\\"')
        return f'FILTER ( CONTAINS(LCASE(STR(?occupationLabel)), "{patt}") )'
    else:
        return "# open mode: no occupation filter"

def compile_query(tmpl: str, langs: str, limit: int, sregex: str, occ_block: str):
    return (tmpl
        .replace("{LANGS}", langs)
        .replace("{LIMIT}", str(limit))
        .replace("{SURNAME_FILTER}", f'FILTER ( REGEX(STR(?surnameLabel), "{sregex}", "i") )')
        .replace("{OCCUPATION_FILTER}", occ_block)
    )

def run_sparql(q: str, tries=3, pause=3.0):
    s = SPARQLWrapper(ENDPOINT)
    s.setMethod("POST")
    s.setReturnFormat(JSON)
    s.addCustomHttpHeader("User-Agent", UA)
    for i in range(tries):
        try:
            s.setQuery(q)
            return s.query().convert()
        except Exception as e:
            wait = pause * (i + 1)
            print(f"[WARN] SPARQL attempt {i+1} failed: {e}; retrying in {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
    return {"results": {"bindings": []}}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clusters", required=True, help="Comma- or space-separated cluster names (e.g., Trades)")
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--langs", default="en")
    ap.add_argument("--mode", choices=["open","strict"], default="open")
    ap.add_argument("--outfile", default="data/candidates_raw.csv")
    args = ap.parse_args()

    occ_by_cluster = load_json(DICT_DIR / "occupations_by_cluster.json")
    surname_variants = load_json(DICT_DIR / "surname_variants.json")
    print(f"[INFO] available clusters: {list(occ_by_cluster.keys())}")

    # Parse clusters string -> list
    requested = []
    for token in re.split(r"[,\s]+", args.clusters.strip()):
        if token:
            requested.append(token)
    # Resolve to occupations
    occs = []
    for c in requested:
        occs.extend(occ_by_cluster.get(c, []))
    print(f"[INFO] requested clusters: {requested}")
    print(f"[INFO] resolved occupations ({len(occs)}): {occs[:12]}{'...' if len(occs) > 12 else ''}")

    if not occs:
        print("[ERROR] No occupations found for the requested clusters. Check dictionaries/occupations_by_cluster.json")
        sys.exit(0)  # keep the workflow green but clear message

    tmpl = TEMPLATE.read_text(encoding="utf-8")
    for token in ("{LANGS}", "{LIMIT}", "{OCCUPATION_FILTER}", "{SURNAME_FILTER}"):
        if token not in tmpl:
            print(f"[ERROR] Template missing placeholder {token}.")
            sys.exit(0)

    rows = []
    first_query_logged = False

    for occ in occs:
        prefixes = surname_variants.get(occ, [])
        if not prefixes:
            print(f"[SKIP] No surname prefixes for occ={occ}")
            continue

        sregex = safe_prefix_regex(prefixes)
        if not sregex:
            print(f"[SKIP] Empty regex after cleaning for occ={occ}")
            continue

        occ_block = occ_filter_block(occ, args.mode)
        query = compile_query(tmpl, args.langs, args.limit, sregex, occ_block)

        if not first_query_logged:
            print("----- [DEBUG] First compiled SPARQL query -----")
            print(query)
            print("------------------------------------------------")
            first_query_logged = True

        data = run_sparql(query)
        bindings = data.get("results", {}).get("bindings", [])
        print(f"[OPEN RAW] rows={len(bindings)} for occ={occ}")

        for b in bindings:
            rows.append([
                b.get("person",{}).get("value",""),
                b.get("personLabel",{}).get("value",""),
                b.get("surnameLabel",{}).get("value",""),
                b.get("occupationLabel",{}).get("value",""),
                # cluster tag: first matching requested cluster for this occupation
                next((cl for cl, occs_ in occ_by_cluster.items() if occ in occs_), "")
            ])
        time.sleep(0.25)  # be polite

    outp = Path(args.outfile)
    outp.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        with outp.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["person","personLabel","surnameLabel","occupationLabel","cluster"])
            w.writerows(rows)
        print(f"[WRITE] {len(rows)} -> {outp}")
    else:
        print("[WRITE] No candidates produced; nothing written.")

if __name__ == "__main__":
    main()

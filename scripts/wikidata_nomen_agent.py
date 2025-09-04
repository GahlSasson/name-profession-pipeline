import json, csv, re, time, sys, os
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON

WD_ENDPOINT = "https://query.wikidata.org/sparql"

def load_json(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)

def build_surname_regex(variants):
    """
    Build a SPARQL/Java-regex-safe OR pattern.
    IMPORTANT: Do NOT use \w (causes lexical errors on Blazegraph).
    We match start-of-surname plus any trailing letters: ^Dent.*|^Denta.*
    """
    alts = []
    for v in variants:
        v = v.strip()
        if not v:
            continue
        # escape regex special chars; allow diacritics
        v_esc = re.escape(v)
        alts.append(f"^{v_esc}.*")
    if not alts:
        return None
    return "(" + "|".join(alts) + ")"

def build_occ_filter(occ, mode):
    if mode == "strict":
        # case-insensitive label contains
        return f'BIND(LCASE(STR(?occupationLabel)) AS ?occ_lc) FILTER(CONTAINS(?occ_lc, "{occ.lower()}"))'
    else:
        return "# open mode (no occupation filter)"

def run_query(sparql, q, tries=3):
    for i in range(tries):
        try:
            sparql.setQuery(q)
            sparql.setReturnFormat(JSON)
            return sparql.query().convert()
        except Exception as e:
            # Backoff on 429/504/etc.
            wait = 2 + i * 4
            print(f"[WARN] SPARQL error attempt {i+1}: {e}; sleeping {wait}s", file=sys.stderr)
            time.sleep(wait)
    # On hard failure: return empty result shape
    return {"results":{"bindings":[]}}

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--clusters", nargs="+", default=["Trades"])
    ap.add_argument("--limit", type=int, default=60)
    ap.add_argument("--langs", default="en")
    ap.add_argument("--mode", choices=["open","strict"], default="open")
    ap.add_argument("--outfile", default="data/candidates_raw.csv")
    args = ap.parse_args()

    base = Path(".")
    occ_by_cluster = load_json(base/"dictionaries"/"occupations_by_cluster.json")
    surname_variants = load_json(base/"dictionaries"/"surname_variants.json")

    # Expand clusters → occupations
    occs = []
    for c in args.clusters:
        occs.extend(occ_by_cluster.get(c, []))
    if not occs:
        print(f"[WARN] No occupations for clusters={args.clusters}")

    sparql = SPARQLWrapper(WD_ENDPOINT, agent="Name-Profession-Pipeline/1.0 (GitHub Actions)")
    # Mild etiquette
    sparql.setTimeout(60)

    template = (base/"queries"/"query_template.sparql").read_text(encoding="utf-8")

    rows = []
    for occ in occs:
        variants = surname_variants.get(occ, [])
        if not variants:
            print(f"[OPEN SKIP] No surname patterns for occ={occ}")
            continue

        sregex = build_surname_regex(variants)
        if not sregex:
            print(f"[OPEN SKIP] Empty regex for occ={occ}")
            continue

        occ_block = build_occ_filter(occ, args.mode)
        sn_block  = f'FILTER(REGEX(STR(?surnameLabel), "{sregex}", "i"))'

        q = (template
             .replace("{LANGS}", args.langs)
             .replace("{LIMIT}", str(args.limit))
             .replace("{OCCUPATION_FILTER}", occ_block)
             .replace("{SURNAME_FILTER}", sn_block))

        print(f"[FILTER] occ={occ} mode={args.mode} regex={sregex}")
        data = run_query(sparql, q)
        bindings = data.get("results", {}).get("bindings", [])
        print(f"[OPEN RAW] rows={len(bindings)} for occ={occ}")

        for b in bindings:
            rows.append([
                b.get("person"          ,{}).get("value",""),
                b.get("personLabel"     ,{}).get("value",""),
                b.get("surnameLabel"    ,{}).get("value",""),
                b.get("occupationLabel" ,{}).get("value",""),
                # cluster tag: first cluster containing this occ
                next((cl for cl,v in occ_by_cluster.items() if occ in v), "")
            ])

        # be polite to endpoint
        time.sleep(0.3)

    outp = Path(args.outfile)
    outp.parent.mkdir(parents=True, exist_ok=True)
    if rows:
        with outp.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["person","personLabel","surnameLabel","occupationLabel","cluster"])
            w.writerows(rows)
        print(f"[WRITE] {len(rows)} → {outp}")
    else:
        # Ensure no stale file tricks later steps
        if outp.exists():
            outp.unlink()
        print("[WRITE] No candidates produced; nothing written.")

if __name__ == "__main__":
    main()

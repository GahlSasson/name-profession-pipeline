import argparse, json, time, re, sys, traceback
from pathlib import Path
from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
from rapidfuzz import fuzz
from metaphone import doublemetaphone

TEMPLATE_PATH = Path("queries") / "query_template.sparql"
DICT_SURNAME  = Path("dictionaries") / "surname_variants.json"
DICT_OCC      = Path("dictionaries") / "occupations_by_cluster.json"

def load_template():
    return TEMPLATE_PATH.read_text()

def load_dicts():
    surname_variants = json.loads(DICT_SURNAME.read_text(encoding="utf-8"))
    occ_by_cluster   = json.loads(DICT_OCC.read_text(encoding="utf-8"))
    return surname_variants, occ_by_cluster

def run_sparql(query):
    s = SPARQLWrapper("https://query.wikidata.org/sparql")
    # Polite UA helps avoid 403 rate limits
    try:
        s.addCustomHttpHeader("User-Agent", "name-profession-pipeline/1.0 (GitHub Actions)")
    except Exception:
        pass
    s.setQuery(query)
    s.setReturnFormat(JSON)
    try:
        results = s.query().convert()
        rows = [{k: b[k]["value"] for k in b} for b in results["results"]["bindings"]]
        return pd.DataFrame(rows)
    except Exception as e:
        print("[SPARQL ERROR]", e)
        traceback.print_exc()
        return pd.DataFrame()

def phonetic_score(a: str, b: str) -> int:
    da = doublemetaphone(a)[0] or ""
    db = doublemetaphone(b)[0] or ""
    return fuzz.ratio(da, db)

def build_regex_union(patterns_by_lang: dict, langs=("en","de","fr","es","it","ar","he","tr","hu","fi")) -> str:
    pats = []
    for lang in langs:
        for p in patterns_by_lang.get(lang, []):
            pats.append(f"(?:{p})")
    # include any remaining patterns once
    for arr in patterns_by_lang.values():
        for p in arr:
            v = f"(?:{p})"
            if v not in pats:
                pats.append(v)
    return "|".join(pats) if pats else ".*"

def discover(cluster_names, limit, outfile, langs="en,fr,de,es,it,ar,he,tr,hu,fi"):
    """
    langs may be 'en,fr,de' or 'en|fr|de'; we normalize to commas for SPARQL,
    and pass the list to choose surname patterns by language.
    """
    template = load_template()
    surname_variants, occ_by_cluster = load_dicts()

    # normalize language string
    lang_list = [x.strip() for x in re.split(r"[|,]\s*", langs) if x.strip()]
    langs_for_sparql = ",".join(lang_list)

    print(f"[INFO] clusters={cluster_names} limit={limit} langs={langs_for_sparql}")

    frames = []
    for cluster in cluster_names:
        occs = occ_by_cluster.get(cluster, [])
        if not occs:
            print(f"[WARN] No occupations defined for cluster: {cluster}")
        for occ in occs:
            key = occ.capitalize()  # dict keys like "Dentist", "Baker", "Judge", "Singer"
            if key not in surname_variants:
                print(f"[SKIP] No surname patterns for occupation={occ} (cluster={cluster})")
                continue

            surname_regex = build_regex_union(surname_variants[key], tuple(lang_list))
            occ_regex = re.escape(occ)
            q = template.format(
                OCCUPATION_REGEX=occ_regex,
                SURNAME_REGEX=surname_regex,
                LANGS=langs_for_sparql,
                LIMIT=limit
            )

            print(f"[QUERY] cluster={cluster} occ={occ}")
            df = run_sparql(q)
            print(f"[RESULT] rows={len(df)} for occ={occ}")

            if df.empty:
                time.sleep(0.5)
                continue

            df["cluster"] = cluster
            df["profession_query"] = occ
            df["lexical_match"] = df["surnameLabel"].str.lower().str.contains(occ.lower()).astype(int)
            df["phonetic_score"] = df["surnameLabel"].apply(lambda s: phonetic_score(str(s), key))
            df["combined_score"] = 0.7*df["lexical_match"] + 0.3*(df["phonetic_score"]/100.0)
            frames.append(df)
            time.sleep(1.2)  # be polite to Wikidata

    all_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not all_df.empty:
        Path(outfile).parent.mkdir(parents=True, exist_ok=True)
        all_df.to_csv(outfile, index=False)
        print(f"[WRITE] {len(all_df)} → {outfile}")
    else:
        print("[WRITE] No candidates produced; nothing written.")
    return all_df

if __name__ == "__main__":
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("--clusters", nargs="+", required=True)
        ap.add_argument("--limit", type=int, default=300)
        ap.add_argument("--outfile", type=str, default="data/candidates_raw.csv")
        ap.add_argument("--langs", type=str, default="en,fr,de,es,it,ar,he,tr,hu,fi")
        args = ap.parse_args()
        df = discover(args.clusters, args.limit, args.outfile, args.langs)
        if df is None or df.empty:
            print("No candidates found; expand dictionaries or clusters or surname patterns.")
        else:
            print(f"Wrote {len(df)} candidates → {args.outfile}")
        sys.exit(0)
    except Exception as e:
        print("[FATAL]", e)
        traceback.print_exc()
        # still exit 1 so the Actions job shows failure for investigation
        sys.exit(1)

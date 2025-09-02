import argparse, json, time, re
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
    s.setQuery(query)
    s.setReturnFormat(JSON)
    results = s.query().convert()
    rows = [{k: b[k]["value"] for k in b} for b in results["results"]["bindings"]]
    return pd.DataFrame(rows)

def phonetic_score(a: str, b: str) -> int:
    da = doublemetaphone(a)[0] or ""
    db = doublemetaphone(b)[0] or ""
    return fuzz.ratio(da, db)

def build_regex_union(patterns_by_lang: dict, langs=("en","de","fr","es","it","ar","he","tr","hu","fi")) -> str:
    # include patterns for selected languages; fall back to all keys if missing
    pats = []
    for lang in langs:
        for p in patterns_by_lang.get(lang, []):
            pats.append(f"(?:{p})")
    for _, arr in patterns_by_lang.items():
        for p in arr:
            val = f"(?:{p})"
            if val not in pats:
                pats.append(val)
    return "|".join(pats) if pats else ".*"

def discover(cluster_names, limit, outfile, langs="en,fr,de,es,it,ar,he,tr,hu,fi"):
    """
    langs may be provided as 'en,fr,de' or 'en|fr|de'; we normalize to commas for SPARQL
    and pass a list into the surname pattern picker.
    """
    template = load_template()
    surname_variants, occ_by_cluster = load_dicts()

    # normalize language string
    lang_list = [x.strip() for x in re.split(r"[|,]\s*", langs) if x.strip()]
    langs_for_sparql = ",".join(lang_list)

    frames = []
    for cluster in cluster_names:
        for occ in occ_by_cluster.get(cluster, []):
            key = occ.capitalize()  # dict keys like "Dentist", "Baker"
            if key not in surname_variants:
                continue
            surname_regex = build_regex_union(surname_variants[key], tuple(lang_list))
            occ_regex = re.escape(occ)

            q = template.format(
                OCCUPATION_REGEX=occ_regex,
                SURNAME_REGEX=surname_regex,
                LANGS=langs_for_sparql,
                LIMIT=limit
            )

            # helpful log
            print(f"[DISCOVERY] cluster={cluster} occ={occ} langs={langs_for_sparql}")

            df = run_sparql(q)
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
    return all_df

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--clusters", nargs="+", required=True)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--outfile", type=str, default="data/candidates_raw.csv")
    ap.add_argument("--langs", type=str, default="en,fr,de,es,it,ar,he,tr,hu,fi")
    args = ap.parse_args()
    df = discover(args.clusters, args.limit, args.outfile, args.langs)
    if df is None or df.empty:
        print("No candidates found; expand dictionaries or clusters.")
    else:
        print(f"Wrote {len(df)} candidates → {args.outfile}")

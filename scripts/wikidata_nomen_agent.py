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
    # reverse map: occ(lower) -> Cluster
    occ_to_cluster = {}
    for cluster, occs in occ_by_cluster.items():
        for o in occs:
            occ_to_cluster[o.lower()] = cluster
    return surname_variants, occ_by_cluster, occ_to_cluster

def run_sparql(query):
    s = SPARQLWrapper("https://query.wikidata.org/sparql")
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

def build_regex_union(patterns_by_lang: dict, langs=("en","de","fr","es","it","ar","he","tr","hu","fi","pl","nl")) -> str:
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

# Occupation synonyms to widen lexical match on labels
OCC_SYNONYMS = {
    "singer": ["singer", "vocalist", "opera singer", "pop singer", "cantor"],
    "painter": ["painter", "house painter", "portrait painter"],
    "carpenter": ["carpenter", "joiner", "woodworker"],
    "smith": ["smith", "blacksmith", "silversmith", "goldsmith", "locksmith"],
    "tailor": ["tailor", "dressmaker", "seamstress", "seamster"],
    "shoemaker": ["shoemaker", "cobbler"],
    "vintner": ["vintner", "winemaker"],
    "sailor": ["sailor", "seaman", "seafarer", "mariner"],
    "fisher": ["fisher", "fisherman", "angler"],
    "scribe": ["scribe", "copyist", "scrivener"],
    "mason": ["mason", "stonemason"],
    "cooper": ["cooper", "barrel maker", "barrelmaker"],
    "brewer": ["brewer"],
    "weaver": ["weaver", "textile worker"],
    "baker": ["baker"],
    "butcher": ["butcher"],
    "miller": ["miller"],
    "porter": ["porter"],
    "gardener": ["gardener"],
    "shepherd": ["shepherd", "sheepherder"],
    "farmer": ["farmer"],
    "archer": ["archer"],
    "miner": ["miner"],
    "doctor": ["doctor", "physician"],
    "dentist": ["dentist"],
    "judge": ["judge"]
}

def normalize_langs(langs: str):
    lang_list = [x.strip() for x in re.split(r"[|,]\s*", langs) if x.strip()]
    return lang_list, ",".join(lang_list)

def discover_strict(cluster_names, limit, langs_list, langs_for_sparql):
    """Original strict mode: filter by occupation in SPARQL (often yields few/zero)."""
    template = load_template()
    surname_variants, occ_by_cluster, occ_to_cluster = load_dicts()
    frames = []
    for cluster in cluster_names:
        for occ in occ_by_cluster.get(cluster, []):
            key_cap = occ.capitalize()
            if key_cap not in surname_variants:
                print(f"[SKIP] No surname patterns for strict occ={occ} (cluster={cluster})")
                continue
            surname_regex = build_regex_union(surname_variants[key_cap], tuple(langs_list))
            occ_regex = re.escape(occ)
            q = template.format(
                OCCUPATION_REGEX=occ_regex,
                SURNAME_REGEX=surname_regex,
                LANGS=langs_for_sparql,
                LIMIT=limit
            )
            print(f"[STRICT QUERY] cluster={cluster} occ={occ}")
            df = run_sparql(q)
            print(f"[STRICT RESULT] rows={len(df)} for occ={occ}")
            if df.empty:
                time.sleep(0.5); continue
            df["cluster"] = cluster
            df["profession_query"] = occ
            df["lexical_match"] = df["surnameLabel"].str.lower().str.contains(occ.lower()).astype(int)
            df["phonetic_score"] = df["surnameLabel"].apply(lambda s: phonetic_score(str(s), key_cap))
            df["combined_score"] = 0.7*df["lexical_match"] + 0.3*(df["phonetic_score"]/100.0)
            frames.append(df); time.sleep(1.2)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def discover_open(cluster_names, limit, langs_list, langs_for_sparql):
    """
    Open mode: query by surname only (OCCUPATION_REGEX = '.*') and filter occupations client-side
    using OCC_SYNONYMS. Much higher yield.
    """
    template = load_template()
    surname_variants, occ_by_cluster, occ_to_cluster = load_dicts()

    # Build the set of target occ keys we want for selected clusters
    occ_targets = []
    for occ_lower, cluster in occ_to_cluster.items():
        if cluster in cluster_names:
            occ_targets.append(occ_lower)
    occ_targets = sorted(set(occ_targets))

    frames = []
    for occ_lower in occ_targets:
        key_cap = occ_lower.capitalize()  # e.g., "Singer" (surname key)
        if key_cap not in surname_variants:
            print(f"[OPEN SKIP] No surname patterns for occ={occ_lower} (cluster={occ_to_cluster.get(occ_lower,'?')})")
            continue

        surname_regex = build_regex_union(surname_variants[key_cap], tuple(langs_list))
        # Disable occupation filter in SPARQL by matching anything:
        q = template.format(
            OCCUPATION_REGEX=".*",
            SURNAME_REGEX=surname_regex,
            LANGS=langs_for_sparql,
            LIMIT=limit
        )

        print(f"[OPEN QUERY] occ={occ_lower} cluster={occ_to_cluster.get(occ_lower,'?')}")
        df = run_sparql(q)
        print(f"[OPEN RAW] rows={len(df)} for occ={occ_lower}")

        if df.empty:
            time.sleep(0.5); continue

        # Filter client-side by occupation label containing any synonym
        syns = OCC_SYNONYMS.get(occ_lower, [occ_lower])
        syns_l = [s.lower() for s in syns]
        def occ_hits(label: str):
            lab = (label or "").lower()
            return any(s in lab for s in syns_l)

        df = df[df["occupationLabel"].apply(occ_hits)]
        print(f"[OPEN FILTERED] rows={len(df)} matched synonyms={syns_l}")

        if df.empty:
            time.sleep(0.5); continue

        cluster = occ_to_cluster.get(occ_lower, "Other")
        df["cluster"] = cluster
        df["profession_query"] = occ_lower
        df["lexical_match"] = 1  # we required label match
        df["phonetic_score"] = df["surnameLabel"].apply(lambda s: phonetic_score(str(s), key_cap))
        df["combined_score"] = 0.7*df["lexical_match"] + 0.3*(df["phonetic_score"]/100.0)

        frames.append(df)
        time.sleep(1.2)  # be polite to Wikidata

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def main(clusters, limit, outfile, langs, mode):
    langs_list, langs_for_sparql = normalize_langs(langs)
    print(f"[INFO] mode={mode} clusters={clusters} limit={limit} langs={langs_for_sparql}")
    if mode == "strict":
        df = discover_strict(clusters, limit, langs_list, langs_for_sparql)
    else:
        df = discover_open(clusters, limit, langs_list, langs_for_sparql)

    if df is not None and not df.empty:
        Path(outfile).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(outfile, index=False)
        print(f"[WRITE] {len(df)} → {outfile}")
    else:
        print("[WRITE] No candidates produced; nothing written.")
    return df

if __name__ == "__main__":
    try:
        ap = argparse.ArgumentParser()
        ap.add_argument("--clusters", nargs="+", required=True)
        ap.add_argument("--limit", type=int, default=250)
        ap.add_argument("--outfile", type=str, default="data/candidates_raw.csv")
        ap.add_argument("--langs", type=str, default="en,fr,de,es,it,ar,he,pl,nl,fi,hu,tr")
        ap.add_argument("--mode", choices=["open","strict"], default="open")
        args = ap.parse_args()
        df = main(args.clusters, args.limit, args.outfile, args.langs, args.mode)
        sys.exit(0 if df is not None else 1)
    except Exception as e:
        print("[FATAL]", e)
        traceback.print_exc()
        sys.exit(1)

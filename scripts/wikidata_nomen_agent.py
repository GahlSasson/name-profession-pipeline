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
    return TEMPLATE_PATH.read_text(encoding="utf-8")

def load_dicts():
    surname_variants = json.loads(DICT_SURNAME.read_text(encoding="utf-8"))
    occ_by_cluster   = json.loads(DICT_OCC.read_text(encoding="utf-8"))
    occ_to_cluster = {}
    for cluster, occs in occ_by_cluster.items():
        for o in occs:
            occ_to_cluster[o.lower()] = cluster
    return surname_variants, occ_by_cluster, occ_to_cluster

def user_agent_sparql():
    s = SPARQLWrapper("https://query.wikidata.org/sparql")
    try:
        s.addCustomHttpHeader("User-Agent", "name-profession-pipeline/1.0 (GitHub Actions)")
    except Exception:
        pass
    s.setReturnFormat(JSON)
    return s

def run_sparql_with_retries(query, tries=4):
    backoff = 1.5
    last = None
    for t in range(tries):
        s = user_agent_sparql()
        s.setQuery(query)
        try:
            results = s.query().convert()
            rows = [{k: b[k]["value"] for k in b} for b in results["results"]["bindings"]]
            return pd.DataFrame(rows)
        except Exception as e:
            last = e
            print(f"[SPARQL RETRY {t+1}/{tries}] {e}")
            time.sleep(backoff)
            backoff *= 1.8
    print("[SPARQL GAVE UP]", last)
    return pd.DataFrame()

def phonetic_score(a: str, b: str) -> int:
    da = doublemetaphone(a)[0] or ""
    db = doublemetaphone(b)[0] or ""
    return fuzz.ratio(da, db)

def normalize_langs(langs: str):
    lang_list = [x.strip() for x in re.split(r"[|,]\s*", langs) if x.strip()]
    return lang_list, ",".join(lang_list)

# Treat meta-chars as signal to fall back to a prefix where possible
META_CHARS = re.compile(r"[.^$*+?()\[\]\\{}|]")

def extract_literal_or_prefix(pattern: str):
    p = pattern
    if p.startswith("^"): p = p[1:]
    if p.endswith("$"): p = p[:-1]
    if META_CHARS.search(pattern):
        m = re.match(r"([A-Za-zÀ-ÖØ-öø-ÿ]+)", p)
        if m: return ("prefix", m.group(1).lower())
        return None
    return ("equals", p.lower())

def build_surname_filter_block(patterns_by_lang: dict, langs_list):
    """Return a SPARQL FILTER that uses STRSTARTS / equality (no regex), across given langs."""
    equals, prefixes, seen = set(), set(), set()
    for lang in langs_list:
        for pat in patterns_by_lang.get(lang, []):
            if (lang, pat) in seen: continue
            seen.add((lang, pat))
            kv = extract_literal_or_prefix(pat)
            if not kv: continue
            kind, val = kv
            (equals if kind == "equals" else prefixes).add(val)
    if not equals and not prefixes:
        # fallback: try all languages if selected ones yielded nothing
        for arr in patterns_by_lang.values():
            for pat in arr:
                kv = extract_literal_or_prefix(pat)
                if not kv: continue
                kind, val = kv
                (equals if kind == "equals" else prefixes).add(val)
    conds = [f'LCASE(STR(?surnameLabel)) = "{lit}"' for lit in sorted(equals)]
    conds += [f'STRSTARTS(LCASE(STR(?surnameLabel)), "{pre}")' for pre in sorted(prefixes)]
    if not conds: return ""
    return "FILTER ( " + " || ".join(conds) + " )"

# Light synonym list to keep "open" mode precise post-query
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

def discover_open(cluster_names, limit, langs_list, langs_for_sparql):
    template = load_template()
    surname_variants, occ_by_cluster, occ_to_cluster = load_dicts()
    occ_targets = sorted({o for o, c in occ_to_cluster.items() if c in cluster_names})
    frames = []
    for occ_lower in occ_targets:
        key_cap = occ_lower.capitalize()
        if key_cap not in surname_variants:
            print(f"[OPEN SKIP] No surname patterns for occ={occ_lower} (cluster={occ_to_cluster.get(occ_lower,'?')})")
            continue
        surname_filter = build_surname_filter_block(surname_variants[key_cap], langs_list)
        if not surname_filter:
            print(f"[OPEN SKIP] Built empty surname filter for occ={occ_lower}")
            continue
        q = template.format(
            SURNAME_FILTER=surname_filter,
            OCCUPATION_FILTER="",
            LANGS=langs_for_sparql,
            LIMIT=limit
        )
        print(f"[FILTER] {surname_filter}")
        print(f"[OPEN QUERY] occ={occ_lower} cluster={occ_to_cluster.get(occ_lower,'?')}")
        df = run_sparql_with_retries(q, tries=4)
        print(f"[OPEN RAW] rows={len(df)} for occ={occ_lower}")
        if df.empty:
            time.sleep(1.0); continue
        syns = [s.lower() for s in OCC_SYNONYMS.get(occ_lower, [occ_lower])]
        df = df[df["occupationLabel"].apply(lambda lab: any(s in (lab or "").lower() for s in syns))]
        print(f"[OPEN FILTERED] rows={len(df)} matched synonyms={syns}")
        if df.empty:
            time.sleep(0.8); continue
        cluster = occ_to_cluster.get(occ_lower, "Other")
        df["cluster"] = cluster
        df["profession_query"] = occ_lower
        df["lexical_match"] = 1
        df["phonetic_score"] = df["surnameLabel"].apply(lambda s: phonetic_score(str(s), key_cap))
        df["combined_score"] = 0.7*df["lexical_match"] + 0.3*(df["phonetic_score"]/100.0)
        frames.append(df)
        time.sleep(1.6)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def discover_strict(cluster_names, limit, langs_list, langs_for_sparql):
    template = load_template()
    surname_variants, occ_by_cluster, _ = load_dicts()
    frames = []
    for cluster in cluster_names:
        for occ in occ_by_cluster.get(cluster, []):
            key_cap = occ.capitalize()
            if key_cap not in surname_variants:
                print(f"[STRICT SKIP] No surname patterns for occ={occ} (cluster={cluster})")
                continue
            surname_filter = build_surname_filter_block(surname_variants[key_cap], langs_list)
            if not surname_filter:
                print(f"[STRICT SKIP] Empty surname filter for occ={occ}")
                continue
            # strict uses an occupation label filter (safe lowercased substring)
            occ_filter = f'FILTER ( REGEX(LCASE(STR(?occupationLabel)), "{re.escape(occ)}", "i") )'
            q = template.format(
                SURNAME_FILTER=surname_filter,
                OCCUPATION_FILTER=occ_filter,
                LANGS=langs_for_sparql,
                LIMIT=limit
            )
            print(f"[STRICT QUERY] cluster={cluster} occ={occ}")
            df = run_sparql_with_retries(q, tries=4)
            print(f"[STRICT RESULT] rows={len(df)} for occ={occ}")
            if df.empty:
                time.sleep(1.0); continue
            df["cluster"] = cluster
            df["profession_query"] = occ
            df["lexical_match"] = df["surnameLabel"].str.lower().str.contains(occ.lower()).astype(int)
            df["phonetic_score"] = df["surnameLabel"].apply(lambda s: phonetic_score(str(s), key_cap))
            df["combined_score"] = 0.7*df["lexical_match"] + 0.3*(df["phonetic_score"]/100.0)
            frames.append(df)
            time.sleep(1.6)
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
        ap.add_argument("--limit", type=int, default=120)
        ap.add_argument("--outfile", type=str, default="data/candidates_raw.csv")
        ap.add_argument("--langs", type=str, default="en,de,fr,es,it")
        ap.add_argument("--mode", choices=["open","strict"], default="open")
        args = ap.parse_args()
        df = main(args.clusters, args.limit, args.outfile, args.langs, args.mode)
        sys.exit(0 if df is not None else 1)
    except Exception as e:
        print("[FATAL]", e)
        traceback.print_exc()
        sys.exit(1)

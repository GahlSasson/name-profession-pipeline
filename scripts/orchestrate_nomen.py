#!/usr/bin/env python3
import argparse, os, sys, json, subprocess, shlex, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "queries" / "query_template.sparql"
DICT_OCC = ROOT / "dictionaries" / "occupations_by_cluster.json"
CSV_PATH = ROOT / "data" / "candidates_raw.csv"

def eprint(*a): print(*a, file=sys.stderr)

def check_placeholders():
    missing=[]
    text = TEMPLATE.read_text(encoding="utf-8")
    for t in ("{LANGS}","{LIMIT}","{OCCUPATION_FILTER}","{SURNAME_FILTER}"):
        if t not in text: missing.append(t)
    return missing

def load_json(p: Path): return json.loads(p.read_text(encoding="utf-8"))
def line_count(p: Path):
    try: return sum(1 for _ in p.open(encoding="utf-8"))
    except FileNotFoundError: return 0

def run(cmd):
    print("→", " ".join(shlex.quote(c) for c in cmd))
    return subprocess.run(cmd).returncode

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--clusters", default="Trades")
    ap.add_argument("--limit", type=int, default=40)
    ap.add_argument("--langs", default="en")
    ap.add_argument("--mode", choices=["open","strict"], default="open")
    ap.add_argument("--skip-upload", action="store_true")
    args = ap.parse_args()

    print("== Preflight ==")
    if not TEMPLATE.exists(): eprint("❌ Missing queries/query_template.sparql"); sys.exit(1)
    if not DICT_OCC.exists(): eprint("❌ Missing dictionaries/occupations_by_cluster.json"); sys.exit(1)
    missing = check_placeholders()
    if missing: eprint(f"❌ Template missing placeholders: {missing}"); sys.exit(2)

    occ_by_cluster = load_json(DICT_OCC)
    clusters = [c for c in [x.strip() for x in args.clusters.replace(",", " ").split()] if c]
    not_found = [c for c in clusters if c not in occ_by_cluster]
    if not_found:
        eprint(f"❌ Cluster(s) not in occupations_by_cluster.json: {not_found}")
        sys.exit(3)
    print(f"✓ Clusters OK: {clusters}")

    print("\n== Discovery ==")
    if CSV_PATH.exists():
        try: CSV_PATH.unlink()
        except Exception: pass

    rc = run([sys.executable, str(ROOT / "scripts" / "wikidata_nomen_agent.py"),
              "--clusters", args.clusters,
              "--limit", str(args.limit),
              "--langs", args.langs,
              "--mode", args.mode,
              "--outfile", str(CSV_PATH)])
    if rc != 0: eprint(f"❌ Discovery exited {rc}"); sys.exit(10)

    time.sleep(0.2)
    n = line_count(CSV_PATH)
    print(f"CSV lines: {n}")
    if n <= 1:
        eprint("⚠️  No CSV data produced. Upload skipped. Check [OPEN RAW]/[ERROR] logs in discovery.")
        sys.exit(0)  # green but informative

    if args.skip-upload:
        print("↷ skip-upload set; stopping after discovery."); sys.exit(0)

    print("\n== Upload to Airtable ==")
    missing_env = [k for k in ("AIRTABLE_API_KEY","AIRTABLE_BASE_ID","AIRTABLE_TABLE_NAME") if not os.getenv(k)]
    if missing_env:
        eprint(f"⚠️  Missing env vars {missing_env}; skipping upload.")
        sys.exit(0)

    rc = run([sys.executable, str(ROOT / "scripts" / "push_to_airtable.py"),
              "--csv", str(CSV_PATH)])
    if rc != 0: eprint(f"❌ Upload exited {rc}"); sys.exit(20)

    print("✓ Orchestration complete.")

if __name__ == "__main__":
    main()

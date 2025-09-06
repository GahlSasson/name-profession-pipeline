#!/usr/bin/env python3
# Produces data/candidates_raw.csv
# Replace the MOCK section with your real discovery logic when ready.

import argparse
import csv
import pathlib
import sys
from typing import List, Dict

def parse_args():
    p = argparse.ArgumentParser(description="Run discovery and write CSV")
    p.add_argument("--clusters", required=True, help="Comma-separated clusters")
    p.add_argument("--limit", required=True, type=int, help="Per-occupation result limit")
    p.add_argument("--langs", required=True, help="Comma-separated language codes (e.g., en)")
    p.add_argument("--surname-filter", default="", help="Comma-separated surnames (optional)")
    p.add_argument("--out", required=True, help="Output CSV path (e.g., data/candidates_raw.csv)")
    return p.parse_args()

def ensure_parent(path_str: str) -> pathlib.Path:
    p = pathlib.Path(path_str)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p

def write_csv(path: pathlib.Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def run_discovery(clusters: List[str], limit: int, langs: List[str], surnames: List[str]) -> List[Dict[str, str]]:
    """
    TODO: Replace this MOCK with your real SPARQL/data fetching + normalization.
    Return a list of dicts that match the fieldnames below.
    """
    # --- MOCK START (proves the pipeline) ---
    base_rows = [
        {"full_name": "Ada Lovelace", "occupation": "Mathematician", "cluster": "Arts", "lang": "en"},
        {"full_name": "Nikola Tesla", "occupation": "Engineer", "cluster": "Trades", "lang": "en"},
        {"full_name": "Marie Curie", "occupation": "Physicist", "cluster": "Medicine", "lang": "en"},
        {"full_name": "Leonardo da Vinci", "occupation": "Artist", "cluster": "Arts", "lang": "it"},
        {"full_name": "Katherine Johnson", "occupation": "Mathematician", "cluster": "Science", "lang": "en"},
    ]
    # Filter by clusters/langs/surnames lightly to simulate behavior
    rows = [
        r for r in base_rows
        if (not clusters or r["cluster"] in clusters)
        and (not langs or r["lang"] in langs)
        and (not surnames or any(r["full_name"].split()[-1].lower() == s.lower() for s in surnames))
    ]
    return rows[:max(0, limit)]
    # --- MOCK END ---

def main():
    args = parse_args()

    clusters = [c.strip() for c in args.clusters.split(",") if c.strip()]
    langs = [l.strip() for l in args.langs.split(",") if l.strip()]
    surnames = [s.strip() for s in args.surname_filter.split(",") if s.strip()]

    # Run discovery
    rows = run_discovery(clusters, args.limit, langs, surnames)

    # Define your canonical CSV schema here.
    fieldnames = ["full_name", "occupation", "cluster", "lang"]

    # Ensure path & write file
    out_path = ensure_parent(args.out)
    write_csv(out_path, rows, fieldnames)

    print(f"Wrote {len(rows)} rows to {out_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())

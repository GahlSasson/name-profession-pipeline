#!/usr/bin/env python3
"""
Convert YOUR CSV into the normalized schema the uploader expects:
  full_name, occupation, cluster, lang
It auto-detects common column names in your source file and lets you override
via a simple mapping at the top.

Usage (workflow will call this):
  python scripts/prepare_csv_from_existing.py \
      --src data/source.csv \
      --out data/candidates_raw.csv
"""

import argparse, csv, sys, pathlib

# 1) If your source headers don't match these, edit the OVERRIDES dict.
#    Left side = our target field, right side = EXACT header in your source CSV.
#    If you leave a value empty (""), the script will auto-detect from COMMONS below.
OVERRIDES = {
    "full_name": "",           # e.g., "full_name" or "Full Name" or "given_name + surname" (see COMBINE)
    "occupation": "",          # e.g., "occupation" or "profession_canonical" or "professional_canonical"
    "cluster": "",             # e.g., "profession_cluster" or "professional_cluster"
    "lang": "",                # e.g., "language" or "language_origin" or "language-origin"
}

# 2) If you need to combine columns to build a value, you can declare a COMBINE rule:
#    key = target field, value = list of source fields to join with spaces
#    Example: COMBINE["full_name"] = ["given_name", "surname"]
COMBINE = {
    # "full_name": ["given_name", "surname"],
}

# 3) Common header aliases the script will search if OVERRIDES are blank
COMMONS = {
    "full_name": [
        "full_name", "Full Name", "Name", "fullname", "Full name",
        # two-part names (if COMBINE not used)
        "person", "title"
    ],
    "occupation": [
        "occupation", "profession_canonical", "professional_canonical",
        "professional_canonoical", "job", "role", "Role", "profession"
    ],
    "cluster": [
        "profession_cluster", "professional_cluster", "Profession Cluster",
        "cluster", "Cluster", "category", "Category", "group", "Group"
    ],
    "lang": [
        "language", "Language", "language_origin", "language-origin",
        "Language Origin", "lang", "Lang"
    ],
}

def pick_one(headers, candidates):
    low = {h.lower(): h for h in headers}
    # exact (case-insensitive)
    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]
    # contains
    for c in candidates:
        c_l = c.lower()
        for h in headers:
            if c_l in h.lower():
                return h
    return None

def resolve_field(headers, target):
    """Return the source header to use for a given target."""
    if OVERRIDES.get(target):
        if OVERRIDES[target] not in headers:
            sys.exit(f"[adapter] OVERRIDES for '{target}' refers to '{OVERRIDES[target]}' which wasn't found.")
        return OVERRIDES[target]
    return pick_one(headers, COMMONS[target])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True, help="Path to your input CSV")
    ap.add_argument("--out", required=True, help="Output path data/candidates_raw.csv")
    args = ap.parse_args()

    src = pathlib.Path(args.src)
    if not src.exists():
        sys.exit(f"[adapter] Source CSV not found: {src}")

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    with src.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

        # resolve simple mappings or prepare to combine
        src_full = resolve_field(headers, "full_name") if "full_name" not in COMBINE else None
        src_occ  = resolve_field(headers, "occupation")
        src_clu  = resolve_field(headers, "cluster")
        src_lang = resolve_field(headers, "lang")

        # prepare writer
        with out.open("w", newline="", encoding="utf-8") as g:
            w = csv.DictWriter(g, fieldnames=["full_name", "occupation", "cluster", "lang"])
            w.writeheader()

            for row in reader:
                # full_name
                if "full_name" in COMBINE:
                    parts = [row.get(p, "").strip() for p in COMBINE["full_name"]]
                    full_name = " ".join(p for p in parts if p)
                else:
                    full_name = (row.get(src_full, "") if src_full else "").strip()

                # occupation / cluster / lang
                occupation = (row.get(src_occ, "") if src_occ else "").strip()
                cluster    = (row.get(src_clu, "") if src_clu else "").strip()
                lang       = (row.get(src_lang, "") if src_lang else "").strip()

                w.writerow({
                    "full_name": full_name,
                    "occupation": occupation,
                    "cluster": cluster,
                    "lang": lang
                })

    print(f"[adapter] Wrote normalized CSV -> {out}")

if __name__ == "__main__":
    main()

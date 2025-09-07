#!/usr/bin/env python3
import argparse, csv, pathlib, sys

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--clusters", required=True)
    p.add_argument("--limit", required=True, type=int)
    p.add_argument("--langs", required=True)
    p.add_argument("--out", required=True)
    a = p.parse_args()

    clusters = [c.strip() for c in a.clusters.split(",") if c.strip()]
    langs = [l.strip() for l in a.langs.split(",") if l.strip()]

    rows = [
      {"full_name":"Ada Lovelace","occupation":"Mathematician","cluster":"Science","lang":"en"},
      {"full_name":"Nikola Tesla","occupation":"Engineer","cluster":"Trades","lang":"en"},
      {"full_name":"Leonardo da Vinci","occupation":"Artist","cluster":"Arts","lang":"it"},
    ]
    rows = [r for r in rows if (not clusters or r["cluster"] in clusters) and (not langs or r["lang"] in langs)]
    if not rows:
        rows = [{"full_name":"Test Person","occupation":"Tester","cluster":(clusters[0] if clusters else "Trades"),"lang":(langs[0] if langs else "en")}]

    out = pathlib.Path(a.out); out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["full_name","occupation","cluster","lang"])
        w.writeheader(); w.writerows(rows)
    print(f"[CSV] wrote {len(rows)} rows -> {out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())

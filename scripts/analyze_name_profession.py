#!/usr/bin/env python3
"""
Name→Profession analyzer for Airtable (v2: threshold + lock aware).

What this does
--------------
- Learns token↔cluster associations (PMI) from YOUR table.
- Scores each record by its name tokens.
- Only writes when the score-gap >= THRESHOLD.
- Skips records that are "locked" by a checkbox or text flag.

Config via env (set by workflow inputs):
- THRESHOLD: float, default 0.8      # minimum gap to write
- DRY_RUN:  "true"/"false", default false   # if true, prints what it WOULD write

Writes (create these in Airtable if you want clean columns; otherwise falls back):
- np_cluster_pred   (text)
- np_cluster_score  (number)
- np_token_explain  (long text)
- np_status         (text: e.g., "updated@2025-09-06" or "skipped<threshold>")
- Optional lock field detection: np_lock / Lock / locked (checkbox or text "true")
"""

import os, sys, time, math, json, re, urllib.parse, requests
from collections import Counter, defaultdict
from datetime import datetime

# ---------- helpers ----------

def env():
    base  = os.getenv("AIRTABLE_BASE_ID")
    token = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY") or ""
    table = os.getenv("AIRTABLE_TABLE_ID") or os.getenv("AIRTABLE_TABLE_NAME")
    if not (base and token and table):
        sys.exit("Missing AIRTABLE_* envs (BASE_ID, TOKEN/API_KEY, and TABLE_ID or TABLE_NAME).")
    token = token.strip().replace("\r","").replace("\n","").replace("\t","")
    enc_table = urllib.parse.quote(table, safe="")
    base_url  = f"https://api.airtable.com/v0/{base}/{enc_table}"
    H_AUTH = {"Authorization": f"Bearer {token}"}
    H_JSON = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return base, table, base_url, H_AUTH, H_JSON

def discover_fields(base, table, H_AUTH):
    """Return (available_field_names, defs_by_name or {}). Uses meta if allowed; falls back to observed keys."""
    # Try meta (needs schema.bases:read)
    r = requests.get(f"https://api.airtable.com/v0/meta/bases/{base}/tables", headers=H_AUTH, timeout=30)
    fields, defs = [], {}
    if r.status_code == 200:
        for t in r.json().get("tables", []):
            if t.get("id") == table or t.get("name") == table:
                for f in t.get("fields", []):
                    fields.append(f["name"])
                    defs[f["name"]] = f
                return fields, defs
    # Fallback: union of fields from first few records
    fields_set = set()
    base_url = f"https://api.airtable.com/v0/{base}/{urllib.parse.quote(table, safe='')}"
    r = requests.get(base_url, headers=H_AUTH, params={"maxRecords": 5}, timeout=30)
    if r.status_code == 200:
        for rec in r.json().get("records", []):
            fields_set.update((rec.get("fields") or {}).keys())
    return sorted(fields_set), {}

def pick_one(avail, aliases):
    lower = {a.lower(): a for a in avail}
    for a in aliases:
        if a.lower() in lower: return lower[a.lower()]
    for a in aliases:
        al = a.lower()
        for f in avail:
            if al in f.lower(): return f
    return None

def fetch_all_records(base_url, H_AUTH, page_size=100, max_pages=1000):
    recs, offset, pages = [], None, 0
    while True:
        params = {"pageSize": page_size}
        if offset: params["offset"] = offset
        r = requests.get(base_url, headers=H_AUTH, params=params, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"Fetch error {r.status_code}: {r.text}")
        data = r.json()
        recs.extend(data.get("records", []))
        offset = data.get("offset")
        pages += 1
        if not offset or pages >= max_pages: break
    return recs

def patch_record(base_url, H_JSON, rec_id, fields):
    return requests.patch(f"{base_url}/{rec_id}", headers=H_JSON, data=json.dumps({"fields": fields}), timeout=60)

# ---------- tokenization ----------

def clean(s):
    if not s: return ""
    return re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ\s]", " ", s).lower().strip()

def char_trigrams(s):
    s = s.replace(" ", "")
    if len(s) < 3: return []
    return [s[i:i+3] for i in range(len(s)-2)]

def name_tokens(full_name, given_name=None, surname=None):
    parts = []
    if full_name: parts += clean(full_name).split()
    if given_name: parts += clean(given_name).split()
    if surname: parts += clean(surname).split()
    toks = set(parts)
    longest = max(parts, key=len) if parts else ""
    toks.update(char_trigrams(longest)[:5])
    return {t for t in toks if len(t) >= 2}

# ---------- PMI model ----------

def learn_pmi(records, field_map):
    token_counts = Counter()
    cluster_counts = Counter()
    joint_counts = defaultdict(Counter)
    N = 0
    for rec in records:
        f = rec.get("fields", {})
        name = f.get(field_map["full_name"]) or ""
        given = f.get(field_map["given_name"]) or ""
        sur   = f.get(field_map["surname"]) or ""
        cluster = f.get(field_map["cluster"]) or ""
        if not cluster: continue
        toks = name_tokens(name, given, sur)
        if not toks: continue
        N += 1
        cluster_counts[cluster] += 1
        for t in toks:
            token_counts[t] += 1
            joint_counts[cluster][t] += 1
    if N == 0: return {}, {}, {}, 0
    alpha = 1.0
    clusters = list(cluster_counts.keys())
    tokens = list(token_counts.keys())
    p_cluster = {c: (cluster_counts[c]+alpha)/(N+alpha*len(clusters)) for c in clusters}
    p_token   = {t: (token_counts[t]+alpha)/(N+alpha*len(tokens))  for t in tokens}
    pmi = defaultdict(dict)
    denom_const = (N + alpha * len(tokens) * len(clusters))
    for c in clusters:
        pc = p_cluster[c]
        for t in tokens:
            num = (joint_counts[c][t] + alpha) / denom_const
            pmi[c][t] = math.log(num / (pc * p_token[t]), 2)
    return pmi, p_cluster, p_token, N

def score_record(tokens, pmi, clusters):
    scores  = {c: 0.0 for c in clusters}
    contrib = {c: []   for c in clusters}
    for t in tokens:
        for c in clusters:
            v = pmi.get(c, {}).get(t, 0.0)
            if v:
                scores[c] += v
                contrib[c].append((t, v))
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    best_c, best_s = ranked[0]
    second_s = ranked[1][1] if len(ranked) > 1 else 0.0
    gap = best_s - second_s
    top = sorted(contrib[best_c], key=lambda kv: kv[1], reverse=True)[:5]
    return best_c, gap, top

# ---------- Main ----------

def truthy(val):
    if isinstance(val, bool): return val
    if isinstance(val, (int, float)): return val != 0
    s = str(val).strip().lower()
    return s in {"1","true","yes","y","checked"}

def main():
    # Inputs
    THRESHOLD = float(os.getenv("THRESHOLD", "0.8"))
    DRY_RUN   = os.getenv("DRY_RUN", "false").strip().lower() in {"1","true","yes"}

    base, table, base_url, H_AUTH, H_JSON = env()
    avail, defs = discover_fields(base, table, H_AUTH)

    # Map important fields (flexible names)
    full_name_f  = pick_one(avail, ["full_name","Full Name","Name","fullname","Full name","title"])
    given_name_f = pick_one(avail, ["given_name","first_name","First Name","Given Name"])
    surname_f    = pick_one(avail, ["surname","last_name","Last Name","Family Name"])
    cluster_f    = pick_one(avail, ["profession_cluster","professional_cluster","Profession Cluster","cluster","Cluster"])
    # results
    pred_f       = pick_one(avail, ["np_cluster_pred","np_cluster","np_pred"])
    score_f      = pick_one(avail, ["np_cluster_score","np_score","np_gap"])
    explain_f    = pick_one(avail, ["np_token_explain","np_explain","notes_qc","Notes","Description"])
    status_f     = pick_one(avail, ["np_status","status_np","NP Status"])
    # lock
    lock_f       = pick_one(avail, ["np_lock","Lock","locked","freeze","frozen"])

    print(f"[cfg] THRESHOLD={THRESHOLD}, DRY_RUN={DRY_RUN}")
    print(f"[map] name={full_name_f}/{given_name_f}/{surname_f}, cluster={cluster_f}")
    print(f"[map] out pred={pred_f}, score={score_f}, explain={explain_f}, status={status_f}, lock={lock_f}")

    # Fetch
    records = fetch_all_records(base_url, H_AUTH)
    print(f"[fetch] {len(records)} records")

    # Learn PMI from labeled rows
    pmi, p_cluster, p_token, N = learn_pmi(records, {
        "full_name":  full_name_f or "",
        "given_name": given_name_f or "",
        "surname":    surname_f or "",
        "cluster":    cluster_f or "",
        "occupation": "",  # not used in v2
    })
    if N == 0 or not pmi:
        sys.exit("[learn] Not enough labeled rows (name + cluster) to learn.")

    clusters = list(p_cluster.keys())
    print(f"[learn] PMI over {N} examples; clusters={clusters}")

    # Score & write
    dt = datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ")
    would, updated, locked, weak = 0, 0, 0, 0

    for rec in records:
        f = rec.get("fields", {})
        rec_id = rec.get("id")

        # skip locked
        if lock_f and lock_f in f and truthy(f.get(lock_f)):
            locked += 1
            continue

        toks = name_tokens(
            f.get(full_name_f, "") if full_name_f else "",
            f.get(given_name_f, "") if given_name_f else "",
            f.get(surname_f, "") if surname_f else "",
        )
        if not toks:
            continue

        best_c, gap, top = score_record(toks, pmi, clusters)

        if gap < THRESHOLD:
            weak += 1
            # optionally mark status
            if not DRY_RUN and status_f:
                patch_record(base_url, H_JSON, rec_id, {status_f: f"skipped<threshold:{THRESHOLD} @ {dt}"})
            continue

        # Prepare fields
        expl = "; ".join([f"{t}:{v:.2f}" for t, v in top]) or "no strong tokens"
        out = {}
        if pred_f:    out[pred_f] = best_c
        if score_f:   out[score_f] = round(gap, 3)
        if explain_f: out[explain_f] = f"best={best_c}, gap={gap:.3f}, top={expl}"
        if status_f:  out[status_f] = f"updated@{dt}"

        if DRY_RUN:
            would += 1
            continue

        r = patch_record(base_url, H_JSON, rec_id, out)
        if r.status_code >= 400:
            print(f"[patch] fail {r.status_code} rec={rec_id}: {r.text}")
        else:
            updated += 1
            time.sleep(0.1)

    print(f"[done] updated={updated} would={would} locked={locked} weak(<{THRESHOLD})={weak}")

if __name__ == "__main__":
    main()

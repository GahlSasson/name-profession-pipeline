#!/usr/bin/env python3
"""
Name→Profession analyzer for Airtable.

What it does
------------
- Fetch all records from your Airtable table.
- Auto-detects your schema (snake_case or spaced variants).
- Tokenizes names (first/last + character trigrams).
- Learns token ↔ cluster associations from YOUR data (PMI with Laplace smoothing).
- For each record, scores clusters from its tokens, picks a predicted cluster,
  and writes back: np_cluster_pred, np_cluster_score, np_token_explain.

Requirements
------------
- Repo secrets: AIRTABLE_TOKEN, AIRTABLE_BASE_ID, (AIRTABLE_TABLE_ID or AIRTABLE_TABLE_NAME)
- Python deps: requests (installed in the workflow)

Fields it writes (create these as Single line text/Number/Long text, or the script
falls back to writing JSON into 'notes_qc' / 'Notes' / 'Description' if present):
- np_cluster_pred   (text)
- np_cluster_score  (number)
- np_token_explain  (long text)
"""

import os, sys, time, math, json, re, urllib.parse, requests
from collections import Counter, defaultdict

# ---------- Airtable helpers ----------

def env():
    base  = os.getenv("AIRTABLE_BASE_ID")
    token = os.getenv("AIRTABLE_TOKEN") or os.getenv("AIRTABLE_API_KEY") or ""
    table = os.getenv("AIRTABLE_TABLE_ID") or os.getenv("AIRTABLE_TABLE_NAME")
    if not (base and token and table):
        sys.exit("Missing AIRTABLE_* envs (BASE_ID, TOKEN/API_KEY, and TABLE_ID or TABLE_NAME).")
    token = token.strip().replace("\r","").replace("\n","").replace("\t","")
    enc_table = urllib.parse.quote(table, safe="")
    base_url  = f"https://api.airtable.com/v0/{base}/{enc_table}"
    headers_auth = {"Authorization": f"Bearer {token}"}
    headers_json = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return base, table, base_url, headers_auth, headers_json

def discover_fields(base, table, headers_auth):
    """Return (available_field_names, defs_by_name or {}). Uses meta if allowed; falls back to observed keys."""
    # Try meta tables (needs schema.bases:read)
    r = requests.get(f"https://api.airtable.com/v0/meta/bases/{base}/tables", headers=headers_auth, timeout=30)
    fields, defs = [], {}
    if r.status_code == 200:
        for t in r.json().get("tables", []):
            if t.get("id") == table or t.get("name") == table:
                for f in t.get("fields", []):
                    fields.append(f["name"])
                    defs[f["name"]] = f
                return fields, defs
    # Fallback: union of fields in first few records
    fields_set = set()
    base_url = f"https://api.airtable.com/v0/{base}/{urllib.parse.quote(table, safe='')}"
    r = requests.get(base_url, headers=headers_auth, params={"maxRecords": 5}, timeout=30)
    if r.status_code == 200:
        for rec in r.json().get("records", []):
            fields_set.update((rec.get("fields") or {}).keys())
    return sorted(fields_set), {}

def pick_one(avail, aliases):
    lower = {a.lower(): a for a in avail}
    # exact (case-insensitive)
    for a in aliases:
        if a.lower() in lower: return lower[a.lower()]
    # contains
    for a in aliases:
        al = a.lower()
        for f in avail:
            if al in f.lower(): return f
    return None

def fetch_all_records(base_url, headers_auth, page_size=100, max_pages=1000):
    recs, offset, pages = [], None, 0
    while True:
        params = {"pageSize": page_size}
        if offset: params["offset"] = offset
        r = requests.get(base_url, headers=headers_auth, params=params, timeout=60)
        if r.status_code >= 400:
            raise RuntimeError(f"Fetch error {r.status_code}: {r.text}")
        data = r.json()
        for rec in data.get("records", []):
            recs.append(rec)
        offset = data.get("offset")
        pages += 1
        if not offset or pages >= max_pages:
            break
    return recs

def patch_record(base_url, headers_json, rec_id, fields):
    r = requests.patch(f"{base_url}/{rec_id}", headers=headers_json, data=json.dumps({"fields": fields}), timeout=60)
    return r

# ---------- Name tokenization & scoring ----------

def clean(s):
    """Keep letters (Latin + diacritics) and spaces; lower-case."""
    if not s: return ""
    return re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ\s]", " ", s).lower().strip()

def char_trigrams(s):
    s = s.replace(" ", "")
    if len(s) < 3: return []
    return [s[i:i+3] for i in range(len(s)-2)]

def name_tokens(full_name, given_name=None, surname=None):
    parts = []
    if full_name:
        parts.extend(clean(full_name).split())
    if given_name:
        parts.extend(clean(given_name).split())
    if surname:
        parts.extend(clean(surname).split())
    toks = set(parts)
    longest = max(parts, key=len) if parts else ""
    toks.update(char_trigrams(longest)[:5])
    toks = {t for t in toks if len(t) >= 2}
    return toks

# ---------- PMI model ----------

def learn_pmi(records, field_map):
    """
    Build PMI(token, cluster) from the data itself.
    field_map: dict with keys: full_name, given_name, surname, cluster, occupation
    """
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
        if not name and not (given or sur):
            continue
        if not cluster:
            continue
        toks = name_tokens(name, given, sur)
        if not toks: continue
        N += 1
        cluster_counts[cluster] += 1
        for t in toks:
            token_counts[t] += 1
            joint_counts[cluster][t] += 1

    if N == 0:
        return {}, {}, {}, 0

    # Laplace smoothing
    alpha = 1.0
    clusters = list(cluster_counts.keys())
    tokens = list(token_counts.keys())

    p_cluster = {c: (cluster_counts[c] + alpha) / (N + alpha * len(clusters)) for c in clusters}
    p_token   = {t: (token_counts[t]   + alpha) / (N + alpha * len(tokens))  for t in tokens}

    pmi = defaultdict(dict)
    for c in clusters:
        for t in tokens:
            num   = (joint_counts[c][t] + alpha) / (N + alpha * len(tokens) * len(clusters))
            denom = p_cluster[c] * p_token[t]
            pmi[c][t] = math.log(num / denom, 2)

    return pmi, p_cluster, p_token, N

def score_record(tokens, pmi, clusters):
    """Sum PMI over tokens for each cluster; return best cluster, score gap, top contributing tokens."""
    scores  = {c: 0.0 for c in clusters}
    contrib = {c: []   for c in clusters}
    for t in tokens:
        for c in clusters:
            val = pmi.get(c, {}).get(t, 0.0)
            if val != 0.0:
                scores[c] += val
                contrib[c].append((t, val))
    ranked   = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    best_c   = ranked[0][0]
    best_s   = ranked[0][1]
    second_s = ranked[1][1] if len(ranked) > 1 else 0.0
    gap      = best_s - second_s
    top_tokens = sorted(contrib[best_c], key=lambda kv: kv[1], reverse=True)[:5]
    return best_c, gap, top_tokens, scores

# ---------- Main pipeline ----------

def main():
    base, table, base_url, H_AUTH, H_JSON = env()

    # Discover fields in the table
    avail, defs = discover_fields(base, table, H_AUTH)
    # Map to your column names
    full_name_f  = pick_one(avail, ["full_name", "Full Name", "Name", "fullname", "Full name", "title"])
    given_name_f = pick_one(avail, ["given_name", "first_name", "First Name", "Given Name"])
    surname_f    = pick_one(avail, ["surname", "last_name", "Last Name", "Family Name"])
    cluster_f    = pick_one(avail, ["profession_cluster", "professional_cluster", "Profession Cluster", "cluster", "Cluster"])
    occupation_f = pick_one(avail, ["occupation","profession_canonical","professional_canonical","professional_canonoical","job","role","Role"])

    # Result field targets
    np_pred_f    = pick_one(avail, ["np_cluster_pred", "np_cluster", "np_pred"])
    np_score_f   = pick_one(avail, ["np_cluster_score", "np_score", "np_gap"])
    np_explain_f = pick_one(avail, ["np_token_explain", "np_explain", "notes_qc", "Notes", "Description"])

    # Fetch data
    print("[analyzer] Fetching records…")
    records = fetch_all_records(base_url, H_AUTH, page_size=100)
    print(f"[analyzer] Loaded {len(records)} records")

    # Learn PMI from labeled data (rows that have cluster & name info)
    field_map = {
        "full_name":  full_name_f or "",
        "given_name": given_name_f or "",
        "surname":    surname_f or "",
        "cluster":    cluster_f or "",
        "occupation": occupation_f or "",
    }
    pmi, p_cluster, p_token, N = learn_pmi(records, field_map)
    if N == 0 or not pmi:
        sys.exit("[analyzer] Not enough data with both Name and Cluster to learn signals.")

    clusters = list(p_cluster.keys())
    print(f"[analyzer] Learned PMI over {N} examples; clusters={clusters}")

    # Score each record and write back
    updated = 0
    for rec in records:
        f = rec.get("fields", {})
        rec_id = rec.get("id")
        name   = f.get(full_name_f) if full_name_f else ""
        tokens = name_tokens(name, f.get(given_name_f, ""), f.get(surname_f, "")) if (full_name_f or given_name_f or surname_f) else set()
        if not tokens:
            continue

        best_c, gap, top_tokens, _scores = score_record(tokens, pmi, clusters)
        expl = "; ".join([f"{tok}:{val:.2f}" for tok, val in top_tokens]) or "no strong tokens"

        out_fields = {}
        if np_pred_f:    out_fields[np_pred_f] = best_c
        if np_score_f:   out_fields[np_score_f] = round(gap, 3)
        if np_explain_f: out_fields[np_explain_f] = f"best={best_c}, gap={gap:.3f}, top={expl}"

        if not (np_pred_f and np_score_f and np_explain_f):
            fallback = pick_one(avail, ["notes_qc","Description","Notes"])
            if fallback:
                out_fields[fallback] = json.dumps({"best": best_c, "gap": gap, "top": top_tokens}, ensure_ascii=False)

        if not out_fields:
            continue

        r = patch_record(base_url, H_JSON, rec_id, out_fields)
        if r.status_code >= 400:
            print(f"[analyzer] Patch failed ({r.status_code}) for {rec_id}: {r.text}")
        else:
            updated += 1
            time.sleep(0.15)

    print(f"[analyzer] Updated {updated} records with predictions/scores.")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Name→Profession/Cluster analyzer for Airtable (auto-label + threshold + lock).

- Learns PMI(token, LABEL) where LABEL is:
    * profession_cluster / professional_cluster  (if present), else
    * profession_canonical                      (fallback)
- Scores each record from its name tokens.
- Only writes when the score-gap >= THRESHOLD.
- Skips records marked "locked" (np_lock / Lock / locked).

Writes (depending on which label is used):
  If CLUSTER label is used:
    - np_cluster_pred (text)
    - np_cluster_score (number)
  If PROFESSION label is used:
    - np_prof_pred (text)
    - np_prof_score (number)

Common:
  - np_token_explain (long text)
  - np_status        (text)

Env (set by workflow inputs):
  - THRESHOLD  (float, default 0.8)
  - DRY_RUN    ("true"/"false", default false)
"""

import os, sys, time, math, json, re, urllib.parse, requests
from collections import Counter, defaultdict
from datetime import datetime

# ---------- Airtable helpers ----------

def _env():
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
    r = requests.get(f"https://api.airtable.com/v0/meta/bases/{base}/tables", headers=H_AUTH, timeout=30)
    fields, defs = [], {}
    if r.status_code == 200:
        for t in r.json().get("tables", []):
            if t.get("id") == table or t.get("name") == table:
                for f in t.get("fields", []):
                    fields.append(f["name"])
                    defs[f["name"]] = f
                return fields, defs
    # Fallback: union of keys seen in a few records
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

def fetch_all(base_url, H_AUTH, page_size=100, max_pages=1000):
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

def patch(base_url, H_JSON, rec_id, fields):
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

# ---------- PMI ----------

def learn_pmi(records, nm_f, gn_f, sn_f, label_f):
    token_counts = Counter()
    label_counts = Counter()
    joint_counts = defaultdict(Counter)
    N = 0
    for rec in records:
        f = rec.get("fields", {})
        label = f.get(label_f) or ""
        if not label: continue
        toks = name_tokens(f.get(nm_f, ""), f.get(gn_f, ""), f.get(sn_f, ""))
        if not toks: continue
        N += 1
        label_counts[label] += 1
        for t in toks:
            token_counts[t] += 1
            joint_counts[label][t] += 1
    if N == 0: return {}, {}, 0
    alpha = 1.0
    labels = list(label_counts.keys())
    tokens = list(token_counts.keys())
    p_label = {c: (label_counts[c]+alpha)/(N+alpha*len(labels)) for c in labels}
    p_token = {t: (token_counts[t]+alpha)/(N+alpha*len(tokens)) for t in tokens}
    denom = (N + alpha*len(tokens)*len(labels))
    pmi = defaultdict(dict)
    for c in labels:
        pc = p_label[c]
        for t in tokens:
            num = (joint_counts[c][t] + alpha) / denom
            pmi[c][t] = math.log(num / (pc * p_token[t]), 2)
    return pmi, labels, N

def score(tokens, pmi, labels):
    scores  = {c: 0.0 for c in labels}
    contrib = {c: []   for c in labels}
    for t in tokens:
        for c in labels:
            v = pmi.get(c, {}).get(t, 0.0)
            if v:
                scores[c] += v
                contrib[c].append((t, v))
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    best_c, best_s = ranked[0]
    second_s = ranked[1][1] if len(ranked)>1 else 0.0
    gap = best_s - second_s
    top = sorted(contrib[best_c], key=lambda kv: kv[1], reverse=True)[:5]
    return best_c, gap, top

def truthy(val):
    if isinstance(val, bool): return val
    if isinstance(val, (int, float)): return val != 0
    s = str(val).strip().lower()
    return s in {"1","true","yes","y","checked"}

# ---------- main ----------

def main():
    THRESHOLD = float(os.getenv("THRESHOLD", "0.8"))
    DRY_RUN   = os.getenv("DRY_RUN","false").strip().lower() in {"1","true","yes"}

    base, table, base_url, H_AUTH, H_JSON = _env()
    avail, defs = discover_fields(base, table, H_AUTH)

    # Input fields (flexible names)
    nm_f  = pick_one(avail, ["full_name","Full Name","Name","fullname","Full name","title"])
    gn_f  = pick_one(avail, ["given_name","first_name","First Name","Given Name"])
    sn_f  = pick_one(avail, ["surname","last_name","Last Name","Family Name"])
    cl_f  = pick_one(avail, ["profession_cluster","professional_cluster","Profession Cluster","cluster","Cluster"])
    pr_f  = pick_one(avail, ["profession_canonical","professional_canonical","professional_canonoical","occupation","Occupation","Job","Role"])

    # Decide label to learn
    label_f   = cl_f if cl_f else pr_f
    label_kind= "cluster" if cl_f else "profession"
    if not label_f:
        sys.exit("[analyzer] No label field found (cluster or profession_canonical). Add one and retry.")

    # Output fields based on label kind
    if label_kind == "cluster":
        pred_f  = pick_one(avail, ["np_cluster_pred","np_cluster","np_pred"])
        score_f = pick_one(avail, ["np_cluster_score","np_score","np_gap"])
    else:
        pred_f  = pick_one(avail, ["np_prof_pred","np_profession_pred","np_prof"])
        score_f = pick_one(avail, ["np_prof_score","np_score","np_gap_prof"])

    explain_f = pick_one(avail, ["np_token_explain","np_explain","notes_qc","Notes","Description"])
    status_f  = pick_one(avail, ["np_status","status_np","NP Status"])
    lock_f    = pick_one(avail, ["np_lock","Lock","locked","freeze","frozen"])

    print(f"[cfg] THRESHOLD={THRESHOLD} DRY_RUN={DRY_RUN}")
    print(f"[map] label_kind={label_kind} label_field={label_f}")
    print(f"[map] name={nm_f}/{gn_f}/{sn_f}  pred={pred_f} score={score_f} explain={explain_f} status={status_f} lock={lock_f}")

    # Data
    rows = fetch_all(base_url, H_AUTH)
    print(f"[fetch] {len(rows)} records")

    # Learn
    pmi, labels, N = learn_pmi(rows, nm_f or "", gn_f or "", sn_f or "", label_f)
    if N == 0:
        sys.exit("[learn] Not enough rows with both name and label to learn.")
    print(f"[learn] PMI over {N} examples; labels={labels}")

    # Score + write
    dt = datetime.utcnow().strftime("%Y-%m-%dT%H:%MZ")
    updated = would = locked = weak = 0

    for rec in rows:
        f = rec.get("fields", {})
        rec_id = rec.get("id")

        if lock_f and lock_f in f and truthy(f.get(lock_f)):
            locked += 1; continue

        toks = name_tokens(
            f.get(nm_f, "") if nm_f else "",
            f.get(gn_f, "") if gn_f else "",
            f.get(sn_f, "") if sn_f else "",
        )
        if not toks: continue

        best, gap, top = score(toks, pmi, labels)

        if gap < THRESHOLD:
            weak += 1
            if not DRY_RUN and status_f:
                patch(base_url, H_JSON, rec_id, {status_f: f"skipped<threshold:{THRESHOLD} @ {dt}"})
            continue

        expl = "; ".join([f"{t}:{v:.2f}" for t,v in top]) or "no strong tokens"
        out = {}
        if pred_f:  out[pred_f]  = best
        if score_f: out[score_f] = round(gap, 3)
        if explain_f: out[explain_f] = f"best={best}, gap={gap:.3f}, top={expl}"
        if status_f:  out[status_f]  = f"updated@{dt}"

        if DRY_RUN:
            would += 1
            continue

        r = patch(base_url, H_JSON, rec_id, out)
        if r.status_code >= 400:
            print(f"[patch] fail {r.status_code} rec={rec_id}: {r.text}")
        else:
            updated += 1
            time.sleep(0.1)

    print(f"[done] label_kind={label_kind} updated={updated} would={would} locked={locked} weak(<{THRESHOLD})={weak}")

if __name__ == "__main__":
    main()

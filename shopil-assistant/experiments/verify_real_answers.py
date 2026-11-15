"""Manual-verification step for the REAL-model run: re-check every approved answer from the audit log
against INDEPENDENT queries (direct DuckDB / SQLite SQL, not the retrieval layer).

Reads <evidence_dir>/real_logs/audit.jsonl and <evidence_dir>/real_stream.db (written by partCD_real.py).
Why separate: the answers are stored output of real model calls, so verification can be repeated without new calls.
"""
import json
import re
import sqlite3
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from shopil import config  # noqa: E402

D = Path(sys.argv[1]).resolve()
audit = [json.loads(l) for l in open(D / "real_logs" / "audit.jsonl", encoding="utf-8")]
snaps = {a["snapshot_id"]: a for a in audit if a["event"] == "snapshot"}
finals = {a["snapshot_id"]: a for a in audit if a["event"] == "final"}
drafts = {}
for a in audit:
    if a["event"] == "draft":
        drafts.setdefault(a["snapshot_id"], []).append(a)

wh = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
st = sqlite3.connect(str(D / "real_stream.db"))
q = lambda con, sql: con.execute(sql).fetchone()
paid_n, paid_rev = q(wh, "select count(*), cast(sum(amount) as double) from marts.fct_orders where product_id=5 and status='paid'")
rv_n, rv_avg, rv_neg = q(wh, "select count(*), round(avg(rating),2), count(*) filter (where rating<=2) from staging.stg_reviews where product_id=5")
acc_n, acc_c = q(st, "select count(*), sum(json_extract(payload_json,'$.amount_cents')) from seen_events where kind='order' and disposition='accepted' and json_extract(payload_json,'$.product_id')=5")
late_n, late_c = q(st, "select count(*), sum(json_extract(payload_json,'$.amount_cents')) from seen_events where kind='order' and disposition='late' and json_extract(payload_json,'$.product_id')=5")
p3 = q(wh, "select count(*), cast(sum(amount) as double) from marts.fct_orders where product_id=3 and status='paid'")
print(f"INDEPENDENT: p5 batch paid={paid_n} rev={paid_rev} | reviews={rv_n} avg={rv_avg} neg={rv_neg} | stream accepted={acc_n}/{acc_c} late={late_n}/{late_c}")


def norm(t):  # 8,925 -> 8925 ; 8,925.00 -> 8925.00 ; keep dots
    return re.sub(r"(?<=\d),(?=\d{3})", "", t)


def number_in(text, value):
    """value (int/float) appears in text as a number, tolerating thousands separators and trailing .00/.0"""
    t = norm(text)
    forms = {f"{value:.2f}", f"{value:g}", f"{value:.1f}", str(int(value)) if float(value).is_integer() else f"{value}"}
    return any(re.search(rf"(?<![\d.]){re.escape(f)}(?![\d])", t) for f in forms)


results = []
def rec(name, ok, detail=""):
    results.append(dict(name=name, passed=bool(ok), detail=detail)); print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


for sid, s in snaps.items():
    f = finals[sid]
    question = s["question"]
    approved = f.get("approved_answer")
    print(f"\n--- {sid} | {question} | status={f['status']} attempts={f['attempts']}")
    if f["status"] != "approved":
        rec(f"{question[:30]}... approved", False); continue
    if "הרווח" in question:   # R3: missing metric
        rec("R3 states profit is missing", any(w in approved for w in ("אינו קיים", "אין", "חסר")))
        rec("R3 contains no invented profit number", not re.search(r"רווח[^\n.]{0,30}\d", norm(approved)))
        rec("R3 reported available product-3 revenue is correct", number_in(approved, p3[1]) and str(p3[0]) in approved, f"expected {p3}")
        continue
    if "שליליות" in question:  # R2
        rec("R2 negative reviews == 8", number_in(approved, rv_neg), f"expected {rv_neg}")
        continue
    # what the question ASKED for must be present and correct
    asked = {"batch revenue": paid_rev, "avg rating": rv_avg, "stream accepted orders": acc_n,
             "stream accepted agorot": acc_c, "stream late orders": late_n}
    if "מספר הביקורות" in question:
        asked["review count"] = rv_n
    for k, v in asked.items():
        rec(f"{question[:22]}... {k} = {v}", number_in(approved, v))
    # whatever extra count the answer volunteers must also be right
    stated = {int(n) for n in re.findall(r"(\d+)\s*הזמנות ששולמו", norm(approved))}
    rec(f"{question[:22]}... any 'paid orders' count the answer states equals {paid_n}", stated <= {paid_n}, f"stated={sorted(stated)}")
    if "הכנסות" in question and "לבטח" not in question:
        rec(f"{question[:22]}... no combined batch+stream total", not number_in(approved, paid_rev + acc_c / 100))
        rec(f"{question[:22]}... no hallucinated 18,925", "18925" not in norm(approved))

# the rejected injected draft really contained errors that the critic caught
for sid, ds in drafts.items():
    inj = [d for d in ds if d["draft_source"] == "injected_test_draft"]
    if inj:
        ev = [a for a in audit if a["event"] == "evaluation" and a["snapshot_id"] == sid]
        rec("injected flawed draft rejected by the real critic", ev and ev[0]["is_acceptable"] is False)
        rec("critic feedback names the correct batch revenue (8,925) and avg rating (3.52)",
            "8,925" in ev[0]["feedback"] or "8925" in norm(ev[0]["feedback"]) and "3.52" in ev[0]["feedback"], ev[0]["feedback"][:100].replace("\n", " "))
        # same evidence for all attempts
        shas = {a["evidence_sha256"] for a in audit if a["snapshot_id"] == sid and a.get("evidence_sha256")}
        rec("all attempts of that question used one evidence package", len(shas) == 1, str(shas))

failed = [r for r in results if not r["passed"]]
print(f"\nTOTAL (verification of REAL-model answers): {len(results) - len(failed)}/{len(results)} passed")
json.dump(results, open(D / "real_verification.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
sys.exit(1 if failed else 0)

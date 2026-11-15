"""Part C/D experiments with REAL Actor and Critic models (OpenRouter free models). Not mocks.

R1 existing fact  R2 follow-up (history)  R3 missing data  R4 flawed draft -> real rejection -> fix -> re-approval
Every approved answer is then verified against INDEPENDENT queries (direct DuckDB / SQLite SQL, not the
retrieval layer). Run: .venv\\Scripts\\python.exe experiments\\partCD_real.py <evidence_dir>
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
OUT = Path(sys.argv[1]).resolve(); OUT.mkdir(parents=True, exist_ok=True)

from shopil import config  # noqa: E402
config.LOG_DIR = OUT / "real_logs"                     # real-run logs kept with the evidence
import functools  # noqa: E402

import duckdb  # noqa: E402
from shopil import actor_critic as ac, live_ingest  # noqa: E402
from shopil.llm import LLM  # noqa: E402
from shopil.retrieval import build_evidence  # noqa: E402

DB = OUT / "real_stream.db"
if DB.exists():
    for p in OUT.glob("real_stream.db*"):
        p.unlink()
live_ingest.preload(DB)                                # deterministic 300-order fixture (windows left open)
build = functools.partial(build_evidence, stream_db=DB)
actor, critic = LLM("actor"), LLM("critic")
transcript, checks = [], []


def say(m=""):
    print(m); transcript.append(m)


def run(label, question, history, **kw):
    say(f"\n{'=' * 78}\n## {label} (REAL actor + REAL critic)\nשאלה: {question}")
    r = ac.answer_question(question, history, actor, critic, evidence_builder=build, **kw)
    say(f"status={r.status} attempts={len(r.attempts)} snapshot={r.snapshot_id} evidence_sha={r.evidence_sha} mock_roles={r.mock_roles}")
    for a in r.attempts:
        say(f"--- attempt {a.n} [{a.draft_source}] critic.is_acceptable={a.is_acceptable}\nDRAFT:\n{a.draft}\nCRITIC FEEDBACK: {a.feedback}")
    say(f"--- DISPLAYED TO USER:\n{r.display}")
    return r


def nums(text):
    return set(re.sub(r"(?<=\d),(?=\d{3})", "", text).replace("₪", " ").split())


def has(text, *needles):
    flat = re.sub(r"(?<=\d),(?=\d{3})", "", text)
    return {n: (n in flat) for n in needles}


def check(name, ok, detail=""):
    checks.append(dict(name=name, passed=bool(ok), detail=detail))
    say(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


# ---------------- independent expected values (direct SQL, NOT the retrieval layer)
wh = duckdb.connect(str(config.WAREHOUSE_PATH), read_only=True)
exp5 = wh.execute("select count(*), round(sum(amount),2) from marts.fct_orders where product_id=5 and status='paid'").fetchone()
rev5 = wh.execute("select count(*), round(avg(rating),2), count(*) filter (where rating<=2) from staging.stg_reviews where product_id=5").fetchone()
st = sqlite3.connect(str(DB))
s_acc = st.execute("select count(*), sum(json_extract(payload_json,'$.amount_cents')) from seen_events where kind='order' and disposition='accepted' and json_extract(payload_json,'$.product_id')=5").fetchone()
s_late = st.execute("select count(*), sum(json_extract(payload_json,'$.amount_cents')) from seen_events where kind='order' and disposition='late' and json_extract(payload_json,'$.product_id')=5").fetchone()
cp = st.execute("select checkpoint from stream_meta").fetchone()[0]
say(f"INDEPENDENT EXPECTED (product 5): batch paid_orders={exp5[0]} revenue={exp5[1]} | reviews={rev5[0]} avg={rev5[1]} negative={rev5[2]} | "
    f"stream accepted={s_acc[0]} orders/{s_acc[1]} agorot, late={s_late[0]} orders/{s_late[1]} agorot, checkpoint={cp}")

hist: list[dict] = []
# R1 -------------------------------------------------------------------
r1 = run("R1 existing fact", "מה ההכנסות, מספר הביקורות והדירוג הממוצע של מוצר 5? ומה מצב הזרם עבורו?", hist)
check("R1 approved with real models", r1.status == "approved" and not r1.mock_roles)
if r1.status == "approved":
    hist.append(ac.history_entry(r1))
    h = has(r1.answer, f"{exp5[1]:g}", str(rev5[0]), f"{rev5[1]:g}", str(s_acc[0]), str(s_acc[1]), str(s_late[0]))
    check("R1 manual verification: answer contains the independently computed numbers", all(h.values()), json.dumps(h, ensure_ascii=False))
    check("R1 batch and stream are not added together (no combined total of 8925 + 8211)", "17136" not in re.sub(r",", "", r1.answer) and "9,736" not in r1.answer)

# R2 -------------------------------------------------------------------
r2 = run("R2 follow-up question (uses chat history)", "ומה המצב של ביקורות שליליות עבורו?", hist)
check("R2 follow-up resolved to product 5 from history", r2.product_ids == [5], f"route product_ids={r2.product_ids}")
check("R2 approved with real models", r2.status == "approved" and not r2.mock_roles)
if r2.status == "approved":
    hist.append(ac.history_entry(r2))
    check("R2 manual verification: 8 negative reviews (<=2) of 42", has(r2.answer, str(rev5[2]), str(rev5[0])) and all(has(r2.answer, str(rev5[2]), str(rev5[0])).values()))

# R3 -------------------------------------------------------------------
r3 = run("R3 missing data", "מה הרווח הגולמי של מוצר 3?", hist)
check("R3 approved with real models", r3.status == "approved" and not r3.mock_roles)
if r3.status == "approved":
    check("R3 answer says the metric is missing (no invented profit figure)",
          any(w in r3.answer for w in ("אין", "לא קיים", "חסר", "אינו קיים", "לא זמין")) and not re.search(r"רווח[^.\n]{0,40}\d", r3.answer.replace(",", "")),
          r3.answer[:140].replace("\n", " "))

# R4 -------------------------------------------------------------------
flawed = ("הכנסות מוצר 5 ב-Batch הן 18,925 ש\"ח, ובזרם התקבלו 8,211 ש\"ח. סך ההכנסות המשולב הוא 27,136 ש\"ח. "
          "הדירוג הממוצע הוא 0 כי אין ביקורות. החלון הנוכחי כבר סגור.")
r4 = run("R4 injected flawed draft -> real rejection -> real fix -> re-evaluation",
         "מה ההכנסות והדירוג של מוצר 5 ב-Batch ובזרם?", [], injected_first_draft=flawed)
check("R4 first (injected, flawed) draft was REJECTED by the real critic with concrete feedback",
      len(r4.attempts) >= 2 and r4.attempts[0].draft_source == "injected_test_draft" and r4.attempts[0].is_acceptable is False
      and len(r4.attempts[0].feedback or "") > 20, (r4.attempts[0].feedback or "")[:160])
check("R4 real actor produced a corrected version and the real critic approved it", r4.status == "approved" and len(r4.attempts) >= 2
      and r4.attempts[-1].draft_source == "actor_model" and not r4.mock_roles)
if r4.status == "approved":
    h = has(r4.answer, f"{exp5[1]:g}", f"{rev5[1]:g}")
    check("R4 manual verification: the approved answer has the correct batch revenue and avg rating (not 18,925 / 0 / combined 27,136)",
          all(h.values()) and "18925" not in r4.answer.replace(",", "") and "27136" not in r4.answer.replace(",", ""), json.dumps(h))

failed = [c for c in checks if not c["passed"]]
say(f"\nTOTAL (REAL models): {len(checks) - len(failed)}/{len(checks)} checks passed")
calls = [json.loads(l) for l in open(config.LOG_DIR / "llm_calls.jsonl", encoding="utf-8")]
say(f"LLM calls in this run: {len(calls)} (mock={sum(c['mock'] for c in calls)}, failed={sum(not c['ok'] for c in calls)})")
(OUT / "real_transcript.md").write_text("\n".join(transcript), encoding="utf-8")
json.dump(checks, open(OUT / "real_checks.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
sys.exit(1 if failed else 0)

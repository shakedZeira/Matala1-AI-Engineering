"""Part C/D experiments that use MOCK models (scripted Actor/Critic). All are labeled MOCK in the output.

They test the control flow, not model quality:
  M1  snapshot stays fixed while a valid unique event arrives mid-answer; the NEXT question sees it
        (a) event into an OPEN window  (b) event that advances the watermark and CLOSES windows  (c) review alert
  M2  three rejections -> stop with "no approved answer"; rejected drafts only in the audit log
  M3  invalid Critic output -> documented error state; model error -> documented error state
  M4  approved answers enter chat history, rejected/error ones do not
Run: .venv\\Scripts\\python.exe experiments\\partCD_mock.py <evidence_dir>
"""
from __future__ import annotations

import functools
import hashlib
import json
import re
import shutil
import sys
import tempfile
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
OUT = Path(sys.argv[1]).resolve()
OUT.mkdir(parents=True, exist_ok=True)

from shopil import config  # noqa: E402
WORK = Path(tempfile.mkdtemp(prefix="mock-", dir=OUT))
config.LOG_DIR = WORK / "logs"                       # isolated logs for this experiment
import shopil.actor_critic as ac  # noqa: E402
import shopil.llm as llm  # noqa: E402
from shopil.llm import LLMError, MockLLM  # noqa: E402
from shopil.retrieval import build_evidence, read_stream_state  # noqa: E402
from streaming_lab.engine import StreamEngine  # noqa: E402
from streaming_lab.events import Event, T0, generate_order_fixture, teaching_fixture  # noqa: E402

results = []
LOG = open(OUT / "partCD_mock_log.txt", "w", encoding="utf-8")


def say(m=""):
    print(m); LOG.write(m + "\n"); LOG.flush()


def check(name, ok, detail=""):
    results.append((name, bool(ok), detail))
    say(f"[{'PASS' if ok else 'FAIL'}] [MOCK] {name} {detail}")


CFG = dict(window_seconds=60, grace_seconds=10, out_of_order_seconds=0, alert_threshold=3)
GOOD = json.dumps({"is_acceptable": True, "feedback": "מאושר (מעריך מדומה)"}, ensure_ascii=False)
BAD = lambda i: json.dumps({"is_acceptable": False, "feedback": f"דחייה מדומה מס' {i}: תקנו את המספרים לפי החבילה"}, ensure_ascii=False)


def extract_blob_sha(messages) -> str:
    text = messages[-1]["content"]
    line = next(l for l in text.split("\n") if l.startswith("{") and l.endswith("}") and len(l) > 500)
    return hashlib.sha256(line.encode("utf-8")).hexdigest()[:16]


def recording(responses, seen):
    """Mock model that records the sha256 of the evidence JSON it was given on every call."""
    i = {"n": 0}
    def f(messages):
        seen.append(extract_blob_sha(messages))
        r = responses[min(i["n"], len(responses) - 1)]; i["n"] += 1
        return r(messages) if callable(r) else r
    return f


def prep_stream(tag: str, deliveries) -> Path:
    p = WORK / f"{tag}.db"
    with StreamEngine(p, **CFG) as e:
        for d in deliveries:
            e.ingest(d.event, offset=d.offset)
    return p


def builder(db):
    return functools.partial(build_evidence, stream_db=db)


# ------------------------------------------------------------------ M1 update during question
def m1_cases():
    say("\n=== M1. A valid, unique event arrives while the answer is being produced")
    data = generate_order_fixture(300)[:120]
    for label, kind in (("a_open_window", "open"), ("b_closes_windows", "close"), ("c_review_alert", "alert")):
        db = prep_stream(f"m1_{label}", data if kind != "alert" else [])
        if kind == "alert":   # reviews only: two negatives for product 5 already, the 3rd arrives mid-answer
            with StreamEngine(db, **CFG) as e:
                for i, (sec, r) in enumerate([(5, 1), (12, 2)]):
                    e.ingest(Event(f"r{i}", "review", T0 + timedelta(seconds=sec), 5, rating=r, text="לא טוב"), offset=i)
        before = read_stream_state(db)
        seen_sha: list[str] = []
        injected = {"done": False}

        def on_stage(stage):
            if stage == "snapshot_created" and not injected["done"]:      # event lands AFTER the snapshot was frozen
                injected["done"] = True
                with StreamEngine(db, **CFG) as e:
                    nxt = e.next_offset
                    if kind == "open":
                        wm = before["max_accepted_event_time"]
                        t = Event("arrives-mid-answer", "order", __import__("datetime").datetime.fromisoformat(wm), 5, amount_cents=7000, order_id=99001)
                    elif kind == "close":
                        t = Event("arrives-mid-answer", "order", __import__("datetime").datetime.fromisoformat(before["watermark"]) + timedelta(seconds=200), 5, amount_cents=7000, order_id=99001)
                    else:
                        t = Event("arrives-mid-answer", "review", T0 + timedelta(seconds=18), 5, rating=1, text="הסוללה נגמרת")
                    r = e.ingest(t, offset=nxt)
                    injected["status"] = r["status"]

        actor = MockLLM("actor", recording(["תשובה מדומה לפי החבילה"], seen_sha), "mock-actor")
        critic = MockLLM("critic", recording([GOOD], seen_sha), "mock-critic")
        res1 = ac.answer_question("מה מצב הזרם עבור מוצר 5?", [], actor, critic, evidence_builder=builder(db), on_stage=on_stage)
        mid = read_stream_state(db)
        res2 = ac.answer_question("ומה מצב הזרם עכשיו?", [ac.history_entry(res1)], MockLLM("actor", recording(["x"], seen_sha), "mock-actor"),
                                  MockLLM("critic", recording([GOOD], seen_sha), "mock-critic"), evidence_builder=builder(db))
        e1 = json.load(open(config.LOG_DIR / "evidence" / f"{res1.snapshot_id}.json", encoding="utf-8"))
        e2 = json.load(open(config.LOG_DIR / "evidence" / f"{res2.snapshot_id}.json", encoding="utf-8"))
        check(f"M1{label[0]} injected event was accepted as valid+unique after the snapshot", injected.get("status") == "accepted", injected.get("status", ""))
        check(f"M1{label[0]} Actor and Critic (draft + evaluation) saw ONE identical evidence package",
              len(set(seen_sha[:2])) == 1 and seen_sha[0] == res1.evidence_sha, f"sha={res1.evidence_sha}")
        check(f"M1{label[0]} first answer's snapshot is still the OLD state (checkpoint {before['checkpoint']})",
              e1["snapshot"]["stream_checkpoint"] == before["checkpoint"] and mid["checkpoint"] == before["checkpoint"] + 1)
        check(f"M1{label[0]} the NEXT question gets a NEW snapshot with the newer checkpoint",
              e2["snapshot"]["stream_checkpoint"] == mid["checkpoint"] and res2.snapshot_id != res1.snapshot_id
              and res2.evidence_sha != res1.evidence_sha)
        if kind == "open":
            ow1 = e1["stream"]["open_order_windows"]; ow2 = e2["stream"]["open_order_windows"]
            check("M1a open-window counter increased by exactly 1 order / 7000 agorot in the new snapshot",
                  ow2[-1]["orders"] == ow1[-1]["orders"] + 1 and ow2[-1]["revenue_cents"] == ow1[-1]["revenue_cents"] + 7000
                  and ow2[-1]["window_start"] == ow1[-1]["window_start"],
                  f"{ow1[-1]['orders']}->{ow2[-1]['orders']} orders, window {ow2[-1]['window_start']} still open")
        elif kind == "close":
            check("M1b the new snapshot shows windows closed by the advanced watermark (policy: watermark >= end + grace)",
                  len(e2["stream"]["closed_order_windows"]) > len(e1["stream"]["closed_order_windows"])
                  and e2["stream"]["watermark"] > e1["stream"]["watermark"],
                  f"closed {len(e1['stream']['closed_order_windows'])}->{len(e2['stream']['closed_order_windows'])}; "
                  f"watermark {e1['stream']['watermark']} -> {e2['stream']['watermark']}")
        else:
            check("M1c the new snapshot contains the alert; the old snapshot did not",
                  len(e1["stream"]["alerts"]) == 0 and len(e2["stream"]["alerts"]) == 1 and e2["stream"]["alerts"][0]["product_id"] == 5)


# ------------------------------------------------------------------ M2/M3/M4 control flow
def m2_to_m4():
    say("\n=== M2. Three rejections -> stop")
    db = prep_stream("m2", teaching_fixture())
    seen: list[str] = []
    actor = MockLLM("actor", recording([lambda m: f"טיוטה מדומה #{len(seen)}"], seen), "mock-actor")
    critic = MockLLM("critic", recording([BAD(1), BAD(2), BAD(3)], seen), "mock-critic")
    hist: list[dict] = []
    r = ac.answer_question("מה ההכנסה בזרם?", hist, actor, critic, evidence_builder=builder(db))
    audit = [json.loads(l) for l in open(config.LOG_DIR / "audit.jsonl", encoding="utf-8") if r.snapshot_id in l]
    check("M2 exactly 3 attempts then status rejected_max_attempts", r.status == "rejected_max_attempts" and len(r.attempts) == 3)
    check("M2 the chat shows the 'no approved answer' message and no rejected draft",
          ac.NO_ANSWER_MESSAGE in r.display and all(a.draft not in r.display for a in r.attempts) and r.answer == "")
    check("M2 all 3 rejected drafts + feedback are in the separate audit log",
          sum(1 for a in audit if a["event"] == "draft") == 3 and sum(1 for a in audit if a["event"] == "evaluation" and not a["is_acceptable"]) == 3)
    check("M2 the same evidence sha was used for every attempt (3 drafts + 3 evaluations = 6 calls)",
          len(seen) == 6 and len(set(seen)) == 1 and seen[0] == r.evidence_sha, f"calls={len(seen)}")

    say("\n=== M3. Invalid evaluator output / model failure -> documented error state")
    bad_outputs = {"not-json": "אני חושב שהתשובה בסדר", "wrong-type": '{"is_acceptable": "yes", "feedback": "ok"}',
                   "missing-field": '{"is_acceptable": true}'}
    for name, out in bad_outputs.items():
        r = ac.answer_question("שאלה", [], MockLLM("actor", ["טיוטה"], "mock-actor"), MockLLM("critic", [out], "mock-critic"), evidence_builder=builder(db))
        check(f"M3 critic output '{name}' -> status error_invalid_evaluation, no answer shown as approved",
              r.status == "error_invalid_evaluation" and r.answer == "" and "שגיאה" in r.display, r.error)

    class Boom:
        mock = True
        def complete(self, *a, **k): raise LLMError("simulated outage")
    r = ac.answer_question("שאלה", [], Boom(), MockLLM("critic", [GOOD], "mock-critic"), evidence_builder=builder(db))
    check("M3 actor model failure -> status error_llm", r.status == "error_llm" and r.answer == "", r.error)
    r = ac.answer_question("שאלה", [], MockLLM("actor", ["טיוטה"], "mock-actor"), Boom(), evidence_builder=builder(db))
    check("M3 critic model failure -> status error_llm", r.status == "error_llm" and r.answer == "", r.error)

    say("\n=== M4. Rejection then fix within the budget (mock)")
    r = ac.answer_question("שאלה", [], MockLLM("actor", ["טיוטה שגויה", "טיוטה מתוקנת"], "mock-actor"),
                           MockLLM("critic", [BAD(1), GOOD], "mock-critic"), evidence_builder=builder(db))
    check("M4 second attempt approved; final answer is the corrected draft", r.status == "approved" and r.answer == "טיוטה מתוקנת" and len(r.attempts) == 2)
    check("M4 the chat shows the approved answer with source footer incl. snapshot id and checkpoint",
          "snap-" in r.display and "checkpoint=" in r.display and "טיוטה שגויה" not in r.display)


m1_cases()
m2_to_m4()
failed = [x for x in results if not x[1]]
say(f"\nTOTAL (MOCK): {len(results) - len(failed)}/{len(results)} checks passed")
json.dump([dict(name=n, passed=p, detail=d, mock=True) for n, p, d in results], open(OUT / "partCD_mock_checks.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
shutil.copy(config.LOG_DIR / "audit.jsonl", OUT / "partCD_mock_audit.jsonl")
sys.exit(1 if failed else 0)

"""Actor (generator) + Critic (evaluator) loop with a bounded number of attempts.

Flow per user question:
  1. build ONE evidence package (frozen snapshot) -> same JSON string for Actor, Critic and every retry
  2. Actor drafts an answer; Critic returns a Pydantic `Evaluation`
  3. rejected -> Actor gets (draft, feedback, SAME evidence) and writes a new version, max 3 attempts total
  4. approved -> shown + saved to chat history; rejected drafts live only in logs/audit.jsonl
  5. 3 rejections -> "no approved answer" message; LLM error / invalid Critic output -> documented error state
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from pydantic import BaseModel, StrictBool, StrictStr, ValidationError

from . import config
from .llm import LLMError
from .retrieval import build_evidence

ACTOR_PROMPT = (config.PROMPT_DIR / "actor_he.txt").read_text(encoding="utf-8")
CRITIC_PROMPT = (config.PROMPT_DIR / "critic_he.txt").read_text(encoding="utf-8")

NO_ANSWER_MESSAGE = "לא התקבלה תשובה מאושרת. הטיוטות נדחו על ידי המעריך שלוש פעמים. נסו לנסח את השאלה מחדש או לשאול על נתון ספציפי."


class Evaluation(BaseModel):
    """Critic output contract (assignment, part C)."""
    is_acceptable: StrictBool          # approve / reject by the criteria
    feedback: StrictStr                # reasoning; on rejection, a concrete instruction for the fix


@dataclass
class Attempt:
    n: int
    draft: str
    draft_source: str                   # "actor_model" | "injected_test_draft"
    is_acceptable: bool | None = None
    feedback: str | None = None


@dataclass
class Result:
    status: str                         # approved | rejected_max_attempts | error_llm | error_invalid_evaluation
    question: str
    snapshot_id: str
    evidence_sha: str
    attempts: list[Attempt] = field(default_factory=list)
    answer: str = ""                    # approved text only
    display: str = ""                   # what the chat shows (answer + source footer, or the no-answer message)
    product_ids: list[int] = field(default_factory=list)
    error: str = ""
    mock_roles: list[str] = field(default_factory=list)


def audit(event: str, snapshot_id: str, **data: Any) -> None:
    """Separate audit log: every draft, decision, feedback and the final state. Not shown in the chat."""
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    row = {"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"), "snapshot_id": snapshot_id, "event": event, **data}
    with open(config.LOG_DIR / "audit.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _history_text(history: list[dict], limit: int = 4) -> str:
    if not history:
        return "(אין היסטוריה)"
    return "\n".join(f"שאלה: {h['question']}\nתשובה מאושרת (נכונה למועד {h.get('snapshot_id', '?')}): {h['answer']}"
                     for h in history[-limit:])


def parse_evaluation(text: str) -> Evaluation:
    """Strict parse. Raises ValueError if the Critic's output is not a valid Evaluation."""
    t = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.M).strip()
    m = re.search(r"\{.*\}", t, flags=re.S)
    if not m:
        raise ValueError("no JSON object in critic output")
    try:
        return Evaluation.model_validate_json(m.group(0))
    except ValidationError as exc:
        raise ValueError(f"critic output is not a valid Evaluation: {exc.errors()[0]['msg']}") from exc


def footer(pkg: dict) -> str:
    s = pkg["snapshot"]
    stream = pkg.get("stream", {})
    parts = [f"צילום מצב: {s['snapshot_id']} ({s['taken_at_utc']} UTC)",
             f"נקודת המשך בזרם: checkpoint={s['stream_checkpoint']}, watermark={s['stream_watermark']}"]
    if "batch" in pkg:
        parts.append("Batch: DuckDB (נטען מראש)")
    if stream:
        parts.append("זרם: SQLite")
    return "—\n📌 " + " · ".join(parts)


def answer_question(question: str, history: list[dict], actor, critic, *,
                    max_attempts: int = config.MAX_ATTEMPTS,
                    evidence_builder: Callable[..., dict] = build_evidence,
                    injected_first_draft: str | None = None,
                    on_stage: Callable[[str], None] | None = None) -> Result:
    stage = on_stage or (lambda _name: None)
    ev = evidence_builder(question, history)
    pkg, blob, sha = ev["package"], ev["json"], ev["sha256"]
    sid = pkg["snapshot"]["snapshot_id"]
    res = Result(status="", question=question, snapshot_id=sid, evidence_sha=sha, product_ids=pkg["route"]["product_ids"],
                 mock_roles=[r for r, m in (("actor", actor), ("critic", critic)) if getattr(m, "mock", False)])
    (config.LOG_DIR / "evidence").mkdir(parents=True, exist_ok=True)
    (config.LOG_DIR / "evidence" / f"{sid}.json").write_text(json.dumps(pkg, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    audit("snapshot", sid, question=question, evidence_sha256=sha, stream_checkpoint=pkg["snapshot"]["stream_checkpoint"],
          stream_watermark=pkg["snapshot"]["stream_watermark"], route=pkg["route"], mock_roles=res.mock_roles)
    stage("snapshot_created")                      # tests use this hook to let new events arrive mid-answer

    draft = feedback = None
    for n in range(1, max_attempts + 1):
        # ---- Actor
        if n == 1 and injected_first_draft is not None:
            draft, src = injected_first_draft, "injected_test_draft"
        else:
            user = (f"היסטוריית שיחה (תשובות מאושרות בלבד):\n{_history_text(history)}\n\n"
                    f"חבילת ראיות (JSON, צילום מצב {sid}):\n{blob}\n\nשאלת המשתמש: {question}\n")
            if feedback is not None:
                user += (f"\nהטיוטה הקודמת שנדחתה:\n{draft}\n\nמשוב המעריך:\n{feedback}\n\n"
                         "כתוב גרסה חדשה שמתקנת את הבעיות, על סמך אותה חבילת ראיות בדיוק.")
            try:
                draft = actor.complete([{"role": "system", "content": ACTOR_PROMPT}, {"role": "user", "content": user}])
            except LLMError as exc:
                res.status, res.error = "error_llm", f"actor: {exc}"
                break
            src = "actor_model"
        att = Attempt(n=n, draft=draft, draft_source=src)
        res.attempts.append(att)
        audit("draft", sid, attempt=n, draft_source=src, draft=draft, evidence_sha256=sha)
        stage(f"draft_{n}")
        # ---- Critic
        cuser = (f"שאלת המשתמש: {question}\n\nחבילת ראיות (JSON, צילום מצב {sid}):\n{blob}\n\n"
                 f"התשובה המועמדת:\n{draft}\n")
        try:
            raw = critic.complete([{"role": "system", "content": CRITIC_PROMPT}, {"role": "user", "content": cuser}])
        except LLMError as exc:
            res.status, res.error = "error_llm", f"critic: {exc}"
            break
        try:
            ev_out = parse_evaluation(raw)
        except ValueError as exc:
            res.status, res.error = "error_invalid_evaluation", str(exc)
            audit("evaluation_invalid", sid, attempt=n, raw_output=raw[:600], error=str(exc))
            break
        att.is_acceptable, att.feedback = ev_out.is_acceptable, ev_out.feedback
        audit("evaluation", sid, attempt=n, is_acceptable=ev_out.is_acceptable, feedback=ev_out.feedback, evidence_sha256=sha)
        stage(f"evaluation_{n}")
        if ev_out.is_acceptable:
            res.status, res.answer = "approved", draft
            res.display = draft.rstrip() + "\n\n" + footer(pkg)
            break
        feedback = ev_out.feedback
    else:
        res.status = "rejected_max_attempts"

    if res.status == "rejected_max_attempts":
        res.display = NO_ANSWER_MESSAGE + "\n\n" + footer(pkg)
    elif res.status.startswith("error"):
        res.display = f"אירעה שגיאה בטיפול בשאלה ({res.status}). לא נוצרה תשובה. פרטים נשמרו ביומן הבדיקה.\n\n" + footer(pkg)
    audit("final", sid, status=res.status, attempts=len(res.attempts), error=res.error,
          approved_answer=res.answer if res.status == "approved" else None)
    return res


def history_entry(res: Result) -> dict:
    return {"question": res.question, "answer": res.answer, "product_ids": res.product_ids, "snapshot_id": res.snapshot_id}

"""Gradio chat: ShopIL business assistant (Actor + Critic over a frozen evidence snapshot)."""
from __future__ import annotations

import os

import gradio as gr

from . import config, live_ingest, retrieval
from .actor_critic import answer_question, history_entry
from .llm import LLM, LLMError

EXAMPLES = [
    "מה ההכנסות והדירוג הממוצע של מוצר 5?",
    "ומה מצב הזרם עבורו עכשיו?",
    "על מה לקוחות מתלוננים במוצר 5?",
    "מה הרווח של מוצר 3?",
    "כמה הזמנות איחרו בזרם ומה מצב החלונות?",
]


def live_panel_md(ingestor: live_ingest.LiveIngestor | None) -> str:
    st = retrieval.read_stream_state(config.STREAM_DB_PATH)
    if not st.get("available"):
        return "### קליטה חיה\nמסד הזרם עדיין לא קיים."
    p = ingestor.progress if ingestor else {}
    t, s = st["totals"], st["stats"]
    run = "🟢 פעילה" if p.get("running") else "⚪ לא פעילה"
    return (
        f"### קליטה חיה בזמן השיחה {run}\n"
        f"| מדד | ערך |\n|---|---|\n"
        f"| נוצרו / עובדו (ריצה נוכחית) | {p.get('produced', '-')} / {p.get('consumed', '-')} |\n"
        f"| עומק תור / המתנות יצרן (תור מוגבל) | {p.get('queue_depth', '-')} / {p.get('backpressure_waits', '-')} |\n"
        f"| checkpoint (offset אחרון שנשמר) | **{st['checkpoint']}** |\n"
        f"| watermark | {st['watermark']} |\n"
        f"| הזמנות שהתקבלו לחלונות | {t['orders_accepted']} |\n"
        f"| הכנסה שהתקבלה (אגורות) | {t['revenue_cents_accepted']:,} (₪{t['revenue_ils_accepted']:,.2f}) |\n"
        f"| הזמנות מאוחרות / כפולות / פסולות | {s['late']} / {s['duplicates']} / {s['invalid']} |\n"
        f"| חלונות פתוחים / סגורים | {len(st['open_order_windows'])} / {len(st['closed_order_windows'])} |\n"
        f"| התראות ביקורות | {len(st['alerts'])} |\n"
        + (f"\n⚠️ שגיאת קליטה: {ingestor.error}" if ingestor and ingestor.error else "")
    )


def build_app(ingestor: live_ingest.LiveIngestor | None = None, actor=None, critic=None) -> gr.Blocks:
    actor = actor or LLM("actor")
    critic = critic or LLM("critic")

    def respond(message: str, chat: list, hist: list):
        message = (message or "").strip()
        if not message:
            yield chat, hist, ""
            return
        chat = chat + [{"role": "user", "content": message},
                       {"role": "assistant", "content": "⏳ מכין צילום מצב, יוצר תשובה ומעריך אותה..."}]
        yield chat, hist, ""
        res = answer_question(message, hist, actor, critic)
        chat[-1] = {"role": "assistant", "content": res.display}
        if res.status == "approved":
            hist = hist + [history_entry(res)]          # only approved answers enter the history
        yield chat, hist, ""

    with gr.Blocks(title="ShopIL – עוזר עסקי") as demo:
        gr.Markdown("# ShopIL – עוזר עסקי\nתשובות מבוססות על נתוני Batch (DuckDB) ועל זרם חי (SQLite), עם יוצר ומעריך.")
        with gr.Row():
            with gr.Column(scale=3):
                chatbot = gr.Chatbot(label="שיחה", height=int(os.getenv("CHAT_HEIGHT", "520")), rtl=True)
                box = gr.Textbox(label="שאלה", placeholder="שאלו על הכנסות, דירוגים, ביקורות או מצב הזרם…", rtl=True)
                gr.Examples(EXAMPLES, inputs=box, label="דוגמאות")
            with gr.Column(scale=2):
                panel = gr.Markdown(live_panel_md(ingestor))
        hist = gr.State([])
        box.submit(respond, [box, chatbot, hist], [chatbot, hist, box])
        timer = gr.Timer(1.0)
        timer.tick(lambda: live_panel_md(ingestor), None, panel)
    return demo

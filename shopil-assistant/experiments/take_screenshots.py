"""Real screenshots of the running app (Edge, driven by Playwright). Uses the real Actor/Critic models.

1. start run_app.py (live ingestion + Gradio) on a free port with a FRESH preloaded stream DB
2. screenshot the live-ingestion panel twice, a few seconds apart (counters must have advanced)
3. ask a question in the chat, wait for the approved answer, screenshot answer + source footer
Run: .venv\\Scripts\\python.exe experiments\\take_screenshots.py <evidence_dir>
"""
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(sys.argv[1]).resolve(); OUT.mkdir(parents=True, exist_ok=True)
PORT = 7861
QUESTION = "מה ההכנסות והדירוג הממוצע של מוצר 5, ומה מצב הזרם עבורו עכשיו?"

import os
env = {**os.environ, "CHAT_HEIGHT": "1000", "PYTHONUTF8": "1"}
proc = subprocess.Popen([str(ROOT / ".venv" / "Scripts" / "python.exe"), str(ROOT / "run_app.py"), "--port", str(PORT), "--fresh-stream"],
                        cwd=ROOT, env=env, stdout=open(OUT / "app_stdout.txt", "w", encoding="utf-8"), stderr=subprocess.STDOUT)
try:
    for _ in range(90):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{PORT}", timeout=2); break
        except Exception:
            time.sleep(1)
    else:
        raise SystemExit("app did not start")
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True)
        page = browser.new_page(viewport={"width": 1500, "height": 1500})
        page.goto(f"http://127.0.0.1:{PORT}")
        page.wait_for_selector("text=קליטה חיה", timeout=30000)
        time.sleep(3)
        page.screenshot(path=str(OUT / "screenshot_1_live_ingestion_t0.png"), clip={"x": 860, "y": 95, "width": 490, "height": 470})
        time.sleep(8)
        page.screenshot(path=str(OUT / "screenshot_2_live_ingestion_t8s.png"), clip={"x": 860, "y": 95, "width": 490, "height": 470})
        box = page.get_by_label("שאלה")
        box.fill(QUESTION); box.press("Enter")
        page.wait_for_selector("text=צילום מצב:", timeout=240000)   # footer = approved (or no-answer) result is displayed
        time.sleep(2)
        page.screenshot(path=str(OUT / "screenshot_3_answer_with_source.png"), clip={"x": 120, "y": 0, "width": 1260, "height": 1420})
        answer = page.locator(".message").last.inner_text()
        (OUT / "screenshot_answer_text.txt").write_text(answer, encoding="utf-8")
        browser.close()
    print("screenshots saved to", OUT)
finally:
    proc.terminate()

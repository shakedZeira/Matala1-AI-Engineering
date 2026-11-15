"""Paths and settings. Secrets come from environment variables / .env only (never from code)."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent          # shopil-assistant/
PROJECT = ROOT.parent                                    # פרויקט/
load_dotenv(ROOT / ".env")

WAREHOUSE_PATH = Path(os.getenv("WAREHOUSE_PATH", PROJECT / "etl-course-demo" / "warehouse.duckdb"))
STREAM_DB_PATH = Path(os.getenv("STREAM_DB_PATH", ROOT / "data" / "stream" / "live.db"))
LOG_DIR = Path(os.getenv("LOG_DIR", ROOT / "logs"))
PROMPT_DIR = ROOT / "prompts"

LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://openrouter.ai/api/v1")
LLM_MODEL = os.getenv("LLM_MODEL", "nvidia/nemotron-3-ultra-550b-a55b:free")
LLM_MODEL_FAST = os.getenv("LLM_MODEL_FAST", "nvidia/nemotron-3.5-lightning:free")

MAX_ATTEMPTS = 3   # assignment: at most three attempts including the first

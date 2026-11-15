"""Background stream ingestion that keeps running while the chat is used.

Uses the course StreamEngine + run_live (real asyncio producer, bounded Queue, SQLite state,
one transaction per event). The engine is single-writer: only this thread writes the stream DB;
the retrieval layer only READS it in its own snapshot transaction.
"""
from __future__ import annotations

import asyncio
import threading
from pathlib import Path

from streaming_lab.engine import StreamEngine
from streaming_lab.live import run_live

from . import config

STREAM_CONFIG = dict(window_seconds=60, grace_seconds=10, out_of_order_seconds=0, alert_threshold=3)


class LiveIngestor:
    def __init__(self, db_path: Path | str | None = None, *, interval_s: float = 0.3, queue_size: int = 8):
        self.db_path = Path(db_path or config.STREAM_DB_PATH)
        self.interval_s, self.queue_size = interval_s, queue_size
        self.thread: threading.Thread | None = None
        self.progress: dict = {"produced": 0, "consumed": 0, "queue_depth": 0, "backpressure_waits": 0, "running": False}
        self.error: str | None = None

    def _on_snapshot(self, snap: dict) -> None:
        q = snap["queue"]
        self.progress.update(produced=q["produced"], consumed=q["consumed"], queue_depth=q["depth"],
                             backpressure_waits=q["backpressure_waits"])

    def _run(self) -> None:
        try:
            self.progress["running"] = True
            with StreamEngine(self.db_path, **STREAM_CONFIG) as engine:
                # a long admission window; the thread is a daemon so it ends with the app
                asyncio.run(run_live(engine, duration_s=24 * 3600, interval_s=self.interval_s,
                                     queue_maxsize=self.queue_size, on_snapshot=self._on_snapshot))
        except Exception as exc:  # surfaced in the UI instead of dying silently
            self.error = f"{type(exc).__name__}: {exc}"
        finally:
            self.progress["running"] = False

    def start(self) -> "LiveIngestor":
        if self.thread and self.thread.is_alive():
            return self
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.thread = threading.Thread(target=self._run, name="shopil-live-ingest", daemon=True)
        self.thread.start()
        return self


def preload(db_path: Path | str | None = None, orders: int = 300) -> Path:
    """Fill a fresh stream DB with the course's deterministic 300-order fixture (no finalize: windows stay open)."""
    from streaming_lab.events import generate_order_fixture, generate_review_fixture
    path = Path(db_path or config.STREAM_DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    with StreamEngine(path, **STREAM_CONFIG) as engine:
        for d in generate_order_fixture(orders):
            engine.ingest(d.event, offset=d.offset)
    return path

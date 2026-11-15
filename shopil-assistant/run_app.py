"""Start the live stream ingestion and the Gradio chat.

    .venv\\Scripts\\python.exe run_app.py [--port 7860] [--no-ingest] [--fresh-stream]

Prerequisite (Part A): the batch warehouse must already be loaded (etl-course-demo/warehouse.duckdb).
"""
import argparse
import sys

from shopil import config, live_ingest
from shopil.app import build_app

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, default=7860)
ap.add_argument("--no-ingest", action="store_true", help="do not start the background stream producer")
ap.add_argument("--fresh-stream", action="store_true", help="start from a freshly preloaded stream DB")
args = ap.parse_args()

if not config.WAREHOUSE_PATH.is_file():
    sys.exit(f"Batch warehouse not found: {config.WAREHOUSE_PATH}\nRun the Part A load first (see README).")
if args.fresh_stream and config.STREAM_DB_PATH.exists():
    for p in config.STREAM_DB_PATH.parent.glob(config.STREAM_DB_PATH.name + "*"):
        p.unlink()
if not config.STREAM_DB_PATH.exists():
    live_ingest.preload()
ingestor = None if args.no_ingest else live_ingest.LiveIngestor().start()
build_app(ingestor).launch(server_name="127.0.0.1", server_port=args.port)

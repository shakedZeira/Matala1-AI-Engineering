"""Retrieval layer: defined queries over the batch marts (DuckDB) and the stream state (SQLite).

Design rules (from the assignment):
  * All numbers are computed here in SQL/Python. The LLMs only phrase and explain them.
  * One evidence package per user question = one frozen snapshot. The Actor, the Critic and every
    retry receive the SAME serialized package (sha256 is logged to prove it).
  * Batch (paid orders, ILS) and stream (order events, agorot, no payment status) are reported in
    SEPARATE sections. A combined figure is never produced; `source_comparison` documents why.
  * Missing data is explicit: avg_rating is null (not 0) for a product without reviews; unsupported
    metrics are listed in `data_gaps`.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from . import config

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _iso(ms: int | None) -> str | None:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")


# --------------------------------------------------------------------------- defined batch queries
BATCH_QUERIES = {
    "mart_all": ("marts.mart_product_reviews",
                 "select product_id, product_name, category, review_count, avg_rating, negative_reviews, "
                 "paid_orders, cast(revenue as double) as revenue_ils from marts.mart_product_reviews order by product_id"),
    "batch_totals": ("marts.fct_orders",
                     "select count(*) as paid_orders, cast(sum(amount) as double) as revenue_ils, "
                     "min(order_date) as first_order_date, max(order_date) as last_order_date "
                     "from marts.fct_orders where status = 'paid'"),
    "review_totals": ("staging.stg_reviews",
                      "select count(*) as clean_reviews, round(avg(rating), 2) as avg_rating, "
                      "count(*) filter (where rating <= 2) as negative_reviews, "
                      "min(created_at) as first_review_at, max(created_at) as last_review_at from staging.stg_reviews"),
    "product_reviews": ("staging.stg_reviews",
                        "select review_id, product_id, rating, review_text, created_at from staging.stg_reviews "
                        "where product_id = ? order by created_at desc limit ?"),
    "product_negative_reviews": ("staging.stg_reviews",
                                 "select review_id, product_id, rating, review_text, created_at from staging.stg_reviews "
                                 "where product_id = ? and rating <= 2 order by created_at desc limit ?"),
    "batch_order_ids": ("marts.fct_orders", "select min(order_id) as min_id, max(order_id) as max_id from marts.fct_orders"),
    "batch_product_ids": ("staging.stg_products", "select product_id from staging.stg_products order by 1"),
}


def _batch_rows(con: duckdb.DuckDBPyConnection, key: str, params: list | None = None) -> list[dict]:
    cur = con.execute(BATCH_QUERIES[key][1], params or [])
    cols = [d[0] for d in cur.description]
    out = []
    for row in cur.fetchall():
        rec = {}
        for c, v in zip(cols, row):
            rec[c] = v.isoformat() if hasattr(v, "isoformat") else v
        out.append(rec)
    return out


# --------------------------------------------------------------------------- stream snapshot
def read_stream_state(db_path: Path | str) -> dict[str, Any]:
    """One consistent read of the stream DB: checkpoint, watermark, windows and side outputs.

    A single deferred read transaction on a WAL database gives snapshot isolation, so the
    checkpoint and the state come from the same commit even while the writer keeps ingesting.
    """
    path = Path(db_path)
    if not path.is_file():
        return {"available": False, "reason": f"stream database not found: {path.name}"}
    con = sqlite3.connect(str(path), timeout=10, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA query_only=ON")
    try:
        con.execute("BEGIN")
        meta = con.execute("select * from stream_meta where singleton=1").fetchone()
        cfg = json.loads(meta["config_json"])
        window_ms = cfg["window_seconds"] * 1000
        stats = {r["name"]: r["value"] for r in con.execute("select name, value from counters")}

        def rows(sql: str, args: tuple = ()) -> list[dict]:
            return [dict(r) for r in con.execute(sql, args)]

        def with_window(r: dict) -> dict:
            r["window_start"] = _iso(r["window_start_ms"])
            r["window_end"] = _iso(r["window_start_ms"] + window_ms)
            return r

        open_orders = [with_window(r) for r in rows("select * from order_state order by window_start_ms")]
        closed_orders = [with_window({k: v for k, v in r.items() if k != "window_end_ms"})
                         for r in rows("select * from realtime_revenue order by window_start_ms")]
        open_reviews = [with_window(r) for r in rows("select * from review_state order by window_start_ms, product_id")]
        closed_reviews = [with_window({k: v for k, v in r.items() if k != "window_end_ms"})
                          for r in rows("select * from review_windows order by window_start_ms, product_id")]
        alerts = [with_window(r) for r in rows("select * from review_alerts order by window_start_ms, product_id")]
        late = rows("select event_id, source_offset, kind, event_time_ms, window_start_ms, watermark_ms, amount_cents "
                    "from late_events order by source_offset")
        for r in late:
            r["event_time"] = _iso(r.pop("event_time_ms"))
            r["window_start"] = _iso(r.pop("window_start_ms"))
            r["watermark_at_arrival"] = _iso(r.pop("watermark_ms"))
        dead = rows("select source_offset, payload_json, reason from dead_letters order by source_offset")
        per_product = rows(
            "select cast(json_extract(payload_json,'$.product_id') as integer) as product_id, disposition, "
            "count(*) as orders, sum(cast(json_extract(payload_json,'$.amount_cents') as integer)) as revenue_cents "
            "from seen_events where kind='order' and disposition in ('accepted','late') group by 1, 2 order by 1, 2")
        stream_ids = rows("select distinct cast(json_extract(payload_json,'$.order_id') as integer) as order_id "
                          "from seen_events where kind='order' and json_extract(payload_json,'$.order_id') is not null")
        time_range = rows("select min(json_extract(payload_json,'$.event_time')) as first_event_time, "
                          "max(json_extract(payload_json,'$.event_time')) as last_event_time from seen_events")[0]
        con.execute("COMMIT")
    finally:
        con.close()

    t_orders = sum(r["orders"] for r in open_orders + closed_orders)
    t_cents = sum(r["revenue_cents"] for r in open_orders + closed_orders)
    return {
        "available": True,
        "source": "SQLite stream state (StreamEngine)",
        "units": {"money": "agorot (1/100 ILS), integer", "time": "UTC ISO-8601"},
        "checkpoint": meta["checkpoint"],
        "next_offset": meta["checkpoint"] + 1,
        "watermark": _iso(meta["watermark_ms"]),
        "max_accepted_event_time": _iso(meta["max_event_time_ms"]),
        "finalized": bool(meta["finalized"]),
        "config": cfg,
        "window_rule": "window [start, end) closes when watermark >= window_end + grace; wall clock never advances watermark",
        "stats": stats,
        "open_order_windows": open_orders,
        "closed_order_windows": closed_orders,
        "open_review_windows": open_reviews,
        "closed_review_windows": closed_reviews,
        "alerts": alerts,
        "late_events_count": len(late),
        "late_events": late[:25],
        "late_orders_cents": sum(r["amount_cents"] or 0 for r in late if r["kind"] == "order"),
        "dead_letters_count": len(dead),
        "dead_letters_sample": [{"offset": r["source_offset"], "reason": r["reason"]} for r in dead[:5]],
        "totals": {"orders_accepted": t_orders, "revenue_cents_accepted": t_cents,
                   "revenue_ils_accepted": t_cents / 100,
                   "open_windows_included_in_totals": len(open_orders)},
        "by_product_orders": per_product,
        "event_time_range": time_range,
        "_order_ids": [r["order_id"] for r in stream_ids],
    }


# --------------------------------------------------------------------------- question routing
UNSUPPORTED = {
    "רווח": "profit", "רווחיות": "profit", "עלות": "cost", "מרווח": "margin", "מרג'ין": "margin", "margin": "margin",
    "profit": "profit", "cost": "cost", "גיל": "customer age", "מגדר": "gender", "עיר": "city", "אזור": "region",
    "משלוח": "shipping", "מלאי": "inventory", "מבצע": "promotions", "החזרות": "returns analysis",
}
PRODUCT_RE = re.compile(r"(?:מוצר|product|#|מס['׳]?|מספר)\s*(\d+)", re.I)


def route(question: str, history: list[dict], products: list[dict]) -> dict[str, Any]:
    """Deterministic routing so retrieval is auditable: which queries feed this question?"""
    q = question.lower()
    ids = [int(m) for m in PRODUCT_RE.findall(question)]
    for p in products:
        if p["product_name"] and p["product_name"].lower() in q:
            ids.append(p["product_id"])
    carried = False
    if not ids and history:                       # follow-up question: inherit the last product in context
        last = history[-1].get("product_ids") or []
        if last:
            ids, carried = list(last), True
    wants_stream = any(w in q for w in ("זרם", "עכשיו", "כרגע", "בזמן אמת", "חלון", "חלונות", "איחור", "מאוחר",
                                        "התראה", "התראות", "watermark", "checkpoint", "live", "stream", "קליטה", "אחרון"))
    wants_reviews = any(w in q for w in ("ביקורת", "ביקורות", "דירוג", "תלונ", "rating", "review", "שלילי", "שליליות"))
    wants_revenue = any(w in q for w in ("הכנס", "מכירות", "הזמנות", "revenue", "שולם", "שקל", "אגורות", "sales"))
    wants_texts = any(w in q for w in ("תלונ", "מה אומרים", "מתלוננ", "טקסט", "ציטוט", "complain"))
    wants_batch = wants_reviews or wants_revenue or not wants_stream
    gaps = sorted({v for k, v in UNSUPPORTED.items() if k in q})
    return {"product_ids": sorted(set(ids)), "carried_from_history": carried, "wants_stream": wants_stream,
            "wants_batch": wants_batch, "wants_reviews": wants_reviews, "wants_revenue": wants_revenue,
            "wants_review_texts": wants_texts, "unsupported_metrics": gaps}


# --------------------------------------------------------------------------- evidence package
def build_evidence(question: str, history: list[dict] | None = None, *,
                   warehouse: Path | str | None = None, stream_db: Path | str | None = None) -> dict[str, Any]:
    """Build ONE frozen evidence package. Returns {"package": dict, "json": str, "sha256": str}."""
    history = history or []
    warehouse = Path(warehouse or config.WAREHOUSE_PATH)
    stream_db = Path(stream_db or config.STREAM_DB_PATH)
    taken = datetime.now(timezone.utc)

    # stream first: its checkpoint is the resume point this snapshot is tied to
    stream = read_stream_state(stream_db)

    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        mart = _batch_rows(con, "mart_all")
        plan = route(question, history, mart)
        tables_used: list[str] = []
        batch: dict[str, Any] = {
            "source": "DuckDB warehouse (dlt load + dbt marts)", "refreshed": "loaded once before the conversation",
            "units": {"money": "ILS (shekels)", "rating": "1-5 stars", "dates": "calendar dates / timestamps as loaded"},
            "revenue_definition": "paid orders only (status = 'paid'); amount already includes quantity",
            "negative_review_definition": "rating <= 2",
            "null_semantics": "avg_rating is null (missing, NOT zero) for a product with no reviews; review_count 0 means none",
        }
        if plan["wants_batch"]:
            totals = _batch_rows(con, "batch_totals")[0]
            rev_tot = _batch_rows(con, "review_totals")[0]
            batch["totals"] = {**totals, **rev_tot}
            tables_used += ["marts.fct_orders", "staging.stg_reviews"]
            if plan["product_ids"]:
                batch["products"] = [p for p in mart if p["product_id"] in plan["product_ids"]]
                missing = [i for i in plan["product_ids"] if i not in {p["product_id"] for p in mart}]
                if missing:
                    batch["unknown_product_ids"] = missing
            else:
                batch["products"] = mart
            tables_used.append("marts.mart_product_reviews")
            if plan["wants_review_texts"] and plan["product_ids"]:
                key = "product_negative_reviews" if "תלונ" in question or "שלילי" in question else "product_reviews"
                batch["review_texts"] = {
                    str(pid): _batch_rows(con, key, [pid, 8]) for pid in plan["product_ids"]
                    if pid in {p["product_id"] for p in mart}}
                batch["review_texts_query"] = BATCH_QUERIES[key][1]
        batch_ids = _batch_rows(con, "batch_product_ids")
        order_id_range = _batch_rows(con, "batch_order_ids")[0]
        batch_totals_for_cmp = _batch_rows(con, "batch_totals")[0]
    finally:
        con.close()

    pkg: dict[str, Any] = {"question": question, "route": plan}
    if plan["wants_batch"]:
        pkg["batch"] = batch
    if plan["wants_stream"] or not plan["wants_batch"]:
        s = {k: v for k, v in stream.items() if not k.startswith("_")}
        if plan["product_ids"] and stream.get("available"):
            s["by_product_orders"] = [r for r in stream["by_product_orders"] if r["product_id"] in plan["product_ids"]]
            s["open_review_windows"] = [r for r in stream["open_review_windows"] if r["product_id"] in plan["product_ids"]]
            s["closed_review_windows"] = [r for r in stream["closed_review_windows"] if r["product_id"] in plan["product_ids"]]
            s["alerts"] = [r for r in stream["alerts"] if r["product_id"] in plan["product_ids"]]
        else:
            s["by_product_orders"] = stream.get("by_product_orders", [])
        s["scope_notes"] = [
            "open_order_windows / closed_order_windows aggregate ALL products together (the engine does not split order windows "
            "by product). They must NOT be presented as figures of a single product.",
            "by_product_orders is the per-product view: disposition 'accepted' includes orders that sit in still-open windows; "
            "'late' orders were rejected from windows and are shown separately.",
            "review windows (open_review_windows / closed_review_windows) ARE per product.",
        ]
        pkg["stream"] = s
        tables_used += ["stream:order_state", "stream:realtime_revenue", "stream:review_state",
                        "stream:late_events", "stream:seen_events", "stream:stream_meta"]
    # documentation of why batch and stream are never added together
    if stream.get("available"):
        stream_pids = {r["product_id"] for r in stream["by_product_orders"]}
        batch_pids = {r["product_id"] for r in batch_ids}
        stream_oids = set(stream["_order_ids"])
        lo, hi = order_id_range["min_id"], order_id_range["max_id"]
        overlap_orders = sorted(o for o in stream_oids if lo <= o <= hi)
        pkg["source_comparison"] = {
            "combined_total_allowed": False,
            "reasons": [
                "batch revenue = paid orders in ILS; the stream contract has no payment-status field and money is in agorot",
                f"product ids: stream uses {sorted(stream_pids)} which are {'all' if stream_pids <= batch_pids else 'not all'} "
                f"present in the batch product table {sorted(batch_pids)} (ids are matchable)",
                f"order_id overlap between sources: {len(overlap_orders)} (batch range {lo}-{hi}); "
                + ("no shared orders found" if not overlap_orders else f"shared: {overlap_orders[:10]}"),
                f"batch order dates {batch_totals_for_cmp['first_order_date']}..{batch_totals_for_cmp['last_order_date']}; "
                f"stream event time {stream['event_time_range']['first_event_time']}..{stream['event_time_range']['last_event_time']}",
            ],
            "instruction": "report batch and stream metrics separately; do not add them",
        }
    gaps = plan["unsupported_metrics"]
    pkg["data_gaps"] = {
        "unsupported_metrics_requested": gaps,
        "available_metrics": ["revenue (paid orders, ILS)", "paid orders count", "review count", "average rating",
                              "negative reviews (<=2)", "review texts", "stream order counts/revenue (agorot)",
                              "stream windows, late events, alerts, checkpoint, watermark"],
        "note": ("the requested metric(s) " + ", ".join(gaps) + " are NOT in the data") if gaps else "",
    }
    snap_id = f"snap-{taken.strftime('%Y%m%dT%H%M%S')}-cp{stream.get('checkpoint', 'NA')}-{uuid.uuid4().hex[:6]}"
    pkg["snapshot"] = {
        "snapshot_id": snap_id, "taken_at_utc": taken.isoformat(timespec="seconds"),
        "stream_checkpoint": stream.get("checkpoint"), "stream_watermark": stream.get("watermark"),
        "tables_used": sorted(set(tables_used)),
        "freshness_rule": "values in this package are current as of taken_at_utc; they override older numbers from chat history",
    }
    blob = json.dumps(pkg, ensure_ascii=False, sort_keys=True, default=str)
    return {"package": pkg, "json": blob, "sha256": hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]}

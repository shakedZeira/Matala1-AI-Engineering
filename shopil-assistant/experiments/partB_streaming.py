"""Part B experiments: continuous ingestion, record vs micro-batch, windows/lateness, replay.

Run with the streaming-course-demo virtualenv:
    streaming-course-demo/.venv/Scripts/python.exe partB_streaming.py <evidence_dir>

Everything uses the course StreamEngine (streaming_lab) unchanged. Each experiment uses a
fresh SQLite DB. An independent pure-Python oracle re-implements the documented time policy
so engine results are compared with a second calculation, not only with themselves.
"""
from __future__ import annotations

import asyncio
import csv
import json
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path

from streaming_lab.engine import InjectedCrash, OffsetGapError, StreamEngine
from streaming_lab.events import (Delivery, Event, T0, batch_truth, epoch_ms, generate_order_fixture,
                                  generate_review_fixture, teaching_fixture, window_start_ms)
from streaming_lab.live import run_live
from streaming_lab.microbatch import run_microbatch
from streaming_lab.reporting import canonical_results, money

OUT = Path(sys.argv[1]).resolve()
OUT.mkdir(parents=True, exist_ok=True)
WORK = Path(tempfile.mkdtemp(prefix="partB-", dir=OUT))  # fresh, never reused
RESULTS: list[tuple[str, bool, str]] = []
LOG = open(OUT / "partB_log.txt", "w", encoding="utf-8")


def say(msg: str = "") -> None:
    print(msg)
    LOG.write(msg + "\n")
    LOG.flush()


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, bool(ok), detail))
    say(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


def new_db(tag: str) -> Path:
    return WORK / f"{tag}.db"


def cfg(grace: int = 10, window: int = 60, ooo: int = 0, alert: int = 3) -> dict:
    return dict(window_seconds=window, grace_seconds=grace, out_of_order_seconds=ooo, alert_threshold=alert)


def write_csv(name: str, rows: list[dict]) -> None:
    if not rows:
        return
    with open(OUT / name, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


# ------------------------------------------------------------------ independent oracle
def oracle(deliveries, *, window=60, grace=10, ooo=0):
    """Re-implementation of the assignment's policy (no engine code used except Event parsing).

    watermark = max(prev, max_seen_accepted_unique_time - ooo); window closes when
    watermark >= window_end + grace; an event is late if its window is already closed
    by the watermark that existed BEFORE it arrived. Duplicates are by event_id.
    """
    seen, wm, mx = set(), None, None
    acc = dict(orders=0, cents=0); late = dict(orders=0, cents=0)
    dup = inv = 0
    late_ids = []
    for d in deliveries:
        try:
            ev = d.event if isinstance(d.event, Event) else Event.from_dict(d.event)
            ms = epoch_ms(ev.event_time)
        except Exception:
            inv += 1
            continue
        if ev.event_id in seen:
            dup += 1
            continue
        seen.add(ev.event_id)
        end = window_start_ms(ms, window) + window * 1000
        if wm is not None and wm >= end + grace * 1000:
            if ev.kind == "order":
                late["orders"] += 1; late["cents"] += ev.amount_cents
            late_ids.append(ev.event_id)
            continue
        mx = ms if mx is None else max(mx, ms)
        wm = mx - ooo * 1000 if wm is None else max(wm, mx - ooo * 1000)
        if ev.kind == "order":
            acc["orders"] += 1; acc["cents"] += ev.amount_cents
    return dict(accepted_orders=acc["orders"], accepted_cents=acc["cents"], late_orders=late["orders"],
                late_cents=late["cents"], duplicates=dup, invalid=inv, late_ids=late_ids, watermark_ms=wm)


def direct_run(deliveries, tag, finalize=True, **c):
    with StreamEngine(new_db(tag), **cfg(**c)) as e:
        for d in deliveries:
            e.ingest(d.event, offset=d.offset)
        return e.finalize_demo() if finalize else e.snapshot()


# ============================================================ B1: continuous live ingestion
async def b1_live():
    say("\n=== B1. Continuous producer + bounded asyncio.Queue + SQLite state (run_live)")
    progress: list[dict] = []
    t0 = time.perf_counter()
    duration = 4.0

    def on_snap(s):
        q = s["queue"]
        progress.append(dict(t=round(time.perf_counter() - t0, 2), produced=q["produced"], consumed=q["consumed"],
                             queue_depth=q["depth"], backpressure_waits=q["backpressure_waits"],
                             orders=s["totals"]["orders"], revenue_ils=s["totals"]["revenue_cents"] / 100,
                             reviews=s["totals"]["reviews"], watermark=s["watermark"],
                             open_windows=len(s["open_windows"]), closed_windows=len(s["revenue_windows"]),
                             checkpoint=s["checkpoint"]))

    with StreamEngine(new_db("b1_live"), **cfg()) as e:
        res = await run_live(e, duration_s=duration, interval_s=0.02, processing_delay_s=0.03, queue_maxsize=8,
                             on_snapshot=on_snap)
    # print ~12 evenly spaced progress lines (the full log goes to CSV)
    step = max(1, len(progress) // 12)
    say(f"{'t(s)':>5} {'produced':>8} {'consumed':>8} {'queue':>5} {'bp-waits':>8} {'orders':>6} {'revenue':>10} {'closed_win':>10}")
    for p in progress[::step]:
        say(f"{p['t']:>5} {p['produced']:>8} {p['consumed']:>8} {p['queue_depth']:>5} {p['backpressure_waits']:>8} "
            f"{p['orders']:>6} {p['revenue_ils']:>10.2f} {p['closed_windows']:>10}")
    write_csv("B1_live_progress.csv", progress)
    final = res["snapshot"]
    say(f"final: produced={res['produced']} consumed={res['consumed']} max_queue_depth={res['max_queue_depth']}/"
        f"{res['queue_maxsize']} backpressure_waits={res['backpressure_waits']} elapsed={res['elapsed_s']}s "
        f"orders={final['totals']['orders']} revenue={money(final['totals']['revenue_cents'])} "
        f"open_windows={len(final['open_windows'])} (not finalized: {not final['finalized']})")
    check("B1 produced == consumed after graceful drain", res["produced"] == res["consumed"])
    check("B1 queue never exceeded its bound", res["max_queue_depth"] <= res["queue_maxsize"],
          f"max={res['max_queue_depth']} bound={res['queue_maxsize']}")
    check("B1 backpressure observed (producer had to wait on full queue)", res["backpressure_waits"] > 0)
    mid = [p for p in progress if p["t"] < duration * 0.75]
    check("B1 processing progressed while producer still running (no waiting for all input)",
          len(mid) > 5 and mid[-1]["consumed"] > 0 and mid[-1]["consumed"] < res["consumed"],
          f"consumed {mid[-1]['consumed']} at t={mid[-1]['t']}s vs {res['consumed']} at end")
    check("B1 live run left windows open (timer expiry is not end-of-input)", not final["finalized"])
    check("B1 checkpoint == last consumed offset", final["checkpoint"] == res["consumed"] - 1)
    return res


# ======================================================== B2: record vs micro-batch + flush reasons
async def b2_modes():
    say("\n=== B2. Same input, same order, fresh DBs: run_live (per record) vs run_microbatch vs direct")
    data = generate_order_fixture(300)  # 317 deliveries incl. 17 retries
    truth = batch_truth(data)
    base = direct_run(data, "b2_direct")
    with StreamEngine(new_db("b2_live"), **cfg()) as e:
        r_live = await run_live(e, data, duration_s=None, interval_s=0.0, queue_maxsize=8, finalize=True)
    with StreamEngine(new_db("b2_mb"), **cfg()) as e:
        r_mb = await run_microbatch(e, data, duration_s=None, interval_s=0.0, max_batch_size=7, max_wait_s=0.05,
                                    queue_maxsize=8, finalize=True)
    c_base, c_live, c_mb = (canonical_results(x) for x in (base, r_live["snapshot"], r_mb["snapshot"]))
    check("B2 canonical_results: run_live == direct ingest", c_live == c_base)
    check("B2 canonical_results: run_microbatch == run_live", c_mb == c_live)
    say(f"run_live      produced={r_live['produced']} consumed={r_live['consumed']}")
    say(f"run_microbatch produced={r_mb['produced']} consumed={r_mb['consumed']} batches={r_mb['batches']} "
        f"flush_counts={r_mb['flush_counts']}")
    say(f"batch_sizes={r_mb['batch_sizes']}")
    check("B2 everything ingested was processed (clean end)", r_live["produced"] == r_live["consumed"] == len(data)
          and r_mb["produced"] == r_mb["consumed"] == len(data))
    check("B2 sum(batch_sizes) == events processed", sum(r_mb["batch_sizes"]) == r_mb["consumed"],
          f"{sum(r_mb['batch_sizes'])} == {r_mb['consumed']}")
    t = base["totals"]
    check("B2 reconciliation: accepted + late == batch truth (cents)",
          t["revenue_cents"] + t["late_order_cents"] == truth["revenue_cents"] == 6124170,
          f"{t['revenue_cents']} + {t['late_order_cents']} = {truth['revenue_cents']}")
    write_csv("B2_mode_comparison.csv", [
        dict(mode="direct", produced=len(data), consumed=len(data), batches="", accepted=base["stats"]["accepted"],
             late=base["stats"]["late"], duplicates=base["stats"]["duplicates"], revenue_cents=t["revenue_cents"],
             late_cents=t["late_order_cents"]),
        dict(mode="run_live", produced=r_live["produced"], consumed=r_live["consumed"], batches="",
             accepted=r_live["snapshot"]["stats"]["accepted"], late=r_live["snapshot"]["stats"]["late"],
             duplicates=r_live["snapshot"]["stats"]["duplicates"],
             revenue_cents=r_live["snapshot"]["totals"]["revenue_cents"],
             late_cents=r_live["snapshot"]["totals"]["late_order_cents"]),
        dict(mode="run_microbatch(7, 0.05s)", produced=r_mb["produced"], consumed=r_mb["consumed"],
             batches=r_mb["batches"], accepted=r_mb["snapshot"]["stats"]["accepted"],
             late=r_mb["snapshot"]["stats"]["late"], duplicates=r_mb["snapshot"]["stats"]["duplicates"],
             revenue_cents=r_mb["snapshot"]["totals"]["revenue_cents"],
             late_cents=r_mb["snapshot"]["totals"]["late_order_cents"]),
    ])
    write_csv("B2_microbatch_trace.csv", r_mb["batch_details"])


async def b3_flush_reasons():
    say("\n=== B3. Micro-batch flush reasons: max_size, max_wait, final_drain")
    data = generate_order_fixture(300)[:23]  # 23 records: 4 full batches of 5 + partial 3
    rows = []

    async def case(tag, deliveries, **kw):
        with StreamEngine(new_db(tag), **cfg()) as e:
            r = await run_microbatch(e, deliveries, duration_s=None, **kw)
        rows.append(dict(case=tag, max_batch_size=kw["max_batch_size"], max_wait_s=kw["max_wait_s"],
                         interval_s=kw["interval_s"], events=r["consumed"], batches=r["batches"],
                         batch_sizes=" ".join(map(str, r["batch_sizes"])),
                         flush_reasons=" ".join(d["flush_reason"] for d in r["batch_details"]),
                         sum_sizes=sum(r["batch_sizes"]), collection_waits=" ".join(f"{w:.2f}" for w in r["batch_wait_s"]),
                         final_partial=r["final_partial_batch_size"]))
        say(f"{tag}: sizes={r['batch_sizes']} reasons={[d['flush_reason'] for d in r['batch_details']]} "
            f"waits={[round(w, 2) for w in r['batch_wait_s']]}")
        return r

    # (a) fast source, long timer: batches fill up -> max_size; the 3 leftover records -> final_drain
    r_a = await case("a_max_size", data, max_batch_size=5, max_wait_s=5.0, interval_s=0.0, queue_maxsize=8)
    check("B3a max_size releases full batches, leftover released by final_drain",
          r_a["flush_counts"]["max_size"] == 4 and r_a["flush_counts"]["final_drain"] == 1
          and r_a["batch_sizes"] == [5, 5, 5, 5, 3], str(r_a["flush_counts"]))
    # (b) slow source relative to the timer: the timer fires first -> max_wait with partial batches
    r_b = await case("b_max_wait", data[:9], max_batch_size=50, max_wait_s=0.12, interval_s=0.05, queue_maxsize=8)
    check("B3b max_wait releases partial batches when the timer fires first",
          r_b["flush_counts"]["max_wait"] >= 2 and max(r_b["batch_sizes"]) < 50, str(r_b["flush_counts"]))
    # (c) partial batch released by TIMEOUT although no further event arrives for a long time
    r_c = await case("c_partial_no_next_event", data[:3], max_batch_size=5, max_wait_s=0.2, interval_s=1.0, queue_maxsize=8)
    d0 = r_c["batch_details"][0]
    check("B3c partial batch (1 of 5) released by max_wait before the next event arrived (arrival gap 1.0s)",
          d0["flush_reason"] == "max_wait" and d0["size"] == 1 and 0.15 <= d0["collection_wait_s"] < 0.6,
          f"size={d0['size']} reason={d0['flush_reason']} waited={d0['collection_wait_s']}s")
    check("B3c remaining records still drained cleanly, sum(sizes)==events",
          sum(r_c["batch_sizes"]) == r_c["consumed"] == 3)
    for r in (r_a, r_b, r_c):
        assert sum(r["batch_sizes"]) == r["consumed"]
    check("B3 sum(batch_sizes) == events processed in all three cases", True)
    check("B3 final_drain does not close business windows (finalize not requested)",
          not r_a["snapshot"]["finalized"] and r_a["snapshot"]["stats"]["revenue_windows_emitted"]
          == len(r_a["snapshot"]["revenue_windows"]))
    write_csv("B3_flush_reasons.csv", rows)
    write_csv("B3_trace_a.csv", r_a["batch_details"])
    write_csv("B3_trace_b.csv", r_b["batch_details"])
    write_csv("B3_trace_c.csv", r_c["batch_details"])


# ============================================================ B4: windows, lateness, hand calculation
def b4_windows():
    say("\n=== B4. Teaching fixture (17 deliveries): windows 60s, out-of-order 0, grace 10")
    data = teaching_fixture()
    with StreamEngine(new_db("b4_teach"), **cfg()) as e:
        log = []
        for d in data:
            r = e.ingest(d.event, offset=d.offset)
            ev = d.event if isinstance(d.event, dict) else d.event.to_dict()
            log.append(dict(offset=d.offset, event_id=ev.get("event_id"), kind=ev.get("kind"),
                            t_plus_s=(epoch_ms(ev["event_time"]) - epoch_ms(T0)) // 1000 if "event_time" in ev else "",
                            cents=ev.get("amount_cents", ""), decision=r["status"],
                            watermark=r.get("watermark_ms") and r["watermark_ms"], note=r.get("reason", "")))
        snap = e.finalize_demo()
        # watermark after each step (read from late/accepted results is partial) -> recompute for the trace file
    write_csv("B4_teaching_trace.csv", log)
    t, st, truth = snap["totals"], snap["stats"], batch_truth(data)
    say(f"stats: {st}")
    say(f"accepted revenue={money(t['revenue_cents'])}  late={money(t['late_order_cents'])}  "
        f"batch truth={money(truth['revenue_cents'])}")
    check("B4 accepted revenue == 122.00 ILS (assignment reference)", t["revenue_cents"] == 12200)
    check("B4 late revenue == 13.00 ILS (assignment reference)", t["late_order_cents"] == 1300)
    check("B4 full calculation on valid unique orders == 135.00 ILS", truth["revenue_cents"] == 13500
          and t["revenue_cents"] + t["late_order_cents"] == 13500, "122.00 + 13.00 = 135.00")
    check("B4 counters: 7 accepted orders, 2 late orders, 2 duplicates, 1 invalid",
          (st["accepted_orders"], st["late"], st["duplicates"], st["invalid"]) == (7, 2, 2, 1),
          f"{(st['accepted_orders'], st['late'], st['duplicates'], st['invalid'])}")
    # hand calculation of window [0,60): order-a 1000 + order-c 1500 = 2500 cents, accepted at watermark 65/65
    w0 = next(w for w in snap["revenue_windows"] if w["window_start"].startswith("2026-11-01T10:00:00"))
    check("B4 hand-computed window [10:00:00,10:01:00) = order-a 1000 + order-c 1500 = 2500 cents, 2 orders",
          (w0["orders"], w0["revenue_cents"]) == (2, 2500), f"engine: {w0['orders']} orders {w0['revenue_cents']} cents; "
          f"closed at watermark {w0['emitted_at_watermark_ms']}")
    # boundary case: an order at exactly T0+60s belongs to window [60,120) (start inclusive), and here it is late
    late_ids = {x["event_id"]: x for x in snap["late_events"]}
    bd = late_ids.get("order-boundary")
    check("B4 boundary case: order at exactly 10:01:00.000 is in window [10:01:00,10:02:00) (start inclusive) "
          "and arrives after that window closed -> late_events", bd is not None and bd["window_start"].startswith(
              "2026-11-01T10:01:00") and bd["amount_cents"] == 800, str(bd and (bd["window_start"], bd["watermark"])))
    check("B4 late_events contains order-late (500) and order-boundary (800)",
          set(late_ids) == {"order-late", "order-boundary"} and sum(x["amount_cents"] for x in late_ids.values()) == 1300)
    wm_vals = [r["watermark"] for r in log if isinstance(r["watermark"], int)]
    check("B4 watermark never moves backwards", wm_vals == sorted(wm_vals))
    check("B4 invalid payload kept in dead_letters with offset, input and reason",
          len(snap["dead_letters"]) == 1 and snap["dead_letters"][0]["source_offset"] == 11
          and "bad-money" in json.dumps(snap["dead_letters"][0]["payload"]) and snap["dead_letters"][0]["reason"],
          snap["dead_letters"][0]["reason"] if snap["dead_letters"] else "")
    check("B4 invalid + duplicate + late events all advanced the checkpoint (checkpoint == last offset)",
          snap["checkpoint"] == 16)
    # alert: product 5, three negatives -> exactly one alert; the 4th negative increases the counter only
    al = snap["alerts"]
    win0_reviews = [w for w in snap["review_windows"] if w["product_id"] == 5 and w["window_start"].startswith("2026-11-01T10:00:00")]
    check("B4 product 5: exactly one alert (at the 3rd negative review)", len(al) == 1 and al[0]["product_id"] == 5
          and al[0]["trigger_event_id"] == "review-3", str(al and (al[0]["trigger_event_id"], al[0]["negative_reviews"])))
    check("B4 4th negative review raises the window counter to 4 but creates no second alert",
          win0_reviews and win0_reviews[0]["negative_reviews"] == 4 and len(al) == 1)
    # oracle reconciliation
    o = oracle(data, grace=10)
    check("B4 independent oracle == engine (accepted/late/dup/invalid)",
          (o["accepted_cents"], o["late_cents"], o["duplicates"], o["invalid"], o["accepted_orders"], o["late_orders"])
          == (t["revenue_cents"], t["late_order_cents"], st["duplicates"], st["invalid"], st["accepted_orders"], 2))
    # reconciliation report: stream accepted+late vs batch calc over the same valid unique orders
    rep = [dict(metric="orders accepted into windows", stream=st["accepted_orders"], batch=""),
           dict(metric="orders late", stream=st["late"], batch=""),
           dict(metric="revenue accepted (ILS)", stream=t["revenue_cents"] / 100, batch=""),
           dict(metric="revenue late (ILS)", stream=t["late_order_cents"] / 100, batch=""),
           dict(metric="accepted + late (ILS)", stream=(t["revenue_cents"] + t["late_order_cents"]) / 100,
                batch=truth["revenue_cents"] / 100),
           dict(metric="valid unique orders (count)", stream=st["accepted_orders"] + 2, batch=truth["orders"])]
    write_csv("B4_reconciliation_report.csv", rep)
    write_csv("B4_revenue_windows.csv", [dict(window=w["window_start"], orders=w["orders"], cents=w["revenue_cents"],
                                              closure=w["closure_reason"], closed_at_wm=w["emitted_at_watermark"])
                                         for w in snap["revenue_windows"]])
    write_csv("B4_late_events.csv", [dict(event_id=x["event_id"], offset=x["source_offset"], window=x["window_start"],
                                          cents=x["amount_cents"], watermark=x["watermark"], reason=x["reason"])
                                     for x in snap["late_events"]])
    write_csv("B4_dead_letters.csv", [dict(offset=x["source_offset"], payload=json.dumps(x["payload"], ensure_ascii=False),
                                           reason=x["reason"]) for x in snap["dead_letters"]])
    # a bigger set of invalid inputs: each lands in dead_letters, none stops the stream
    bad = [{"event_id": "x1", "kind": "order", "event_time": "2026-11-01T10:00:00", "product_id": 1, "amount_cents": 100},
           {"event_id": "x2", "kind": "order", "event_time": "2026-11-01T10:00:00Z", "product_id": 1, "amount_cents": "5"},
           {"event_id": "x3", "kind": "review", "event_time": "2026-11-01T10:00:00Z", "product_id": 1, "rating": 7, "text": "a"},
           {"kind": "order", "event_time": "2026-11-01T10:00:00Z", "product_id": 1, "amount_cents": 1},
           "not-an-object"]
    with StreamEngine(new_db("b4_invalid"), **cfg()) as e:
        res = [e.ingest(b, offset=i) for i, b in enumerate(bad)]
        s2 = e.snapshot()
    check("B4 five kinds of invalid input -> five dead_letters, zero accepted, checkpoint advanced",
          [r["status"] for r in res] == ["invalid"] * 5 and len(s2["dead_letters"]) == 5 and s2["stats"]["accepted"] == 0
          and s2["checkpoint"] == 4, "; ".join(x["reason"] for x in s2["dead_letters"]))
    write_csv("B4_invalid_inputs.csv", [dict(offset=x["source_offset"], payload=json.dumps(x["payload"], ensure_ascii=False),
                                             reason=x["reason"]) for x in s2["dead_letters"]])


# ================================================================ B5: grace policy comparison
def b5_grace():
    say("\n=== B5. Grace policy comparison, same 300-order input, same order, fresh DBs")
    data = generate_order_fixture(300)
    rows = []
    for g in (0, 10, 30, 120):
        snap_open = direct_run(data, f"b5_g{g}_open", finalize=False, grace=g)
        snap = direct_run(data, f"b5_g{g}", grace=g)
        t, st = snap["totals"], snap["stats"]
        o = oracle(data, grace=g)
        closed_before_end = len(snap_open["revenue_windows"])
        first_close = min((w["emitted_at_watermark"] for w in snap["revenue_windows"] if w["closure_reason"] == "watermark"),
                          default=None)
        rows.append(dict(grace_s=g, accepted_orders=st["accepted_orders"], late_orders=t["late_orders"],
                         accepted_ils=t["revenue_cents"] / 100, late_ils=t["late_order_cents"] / 100,
                         windows_closed_by_watermark_before_end=closed_before_end,
                         total_ils=(t["revenue_cents"] + t["late_order_cents"]) / 100,
                         oracle_match=(o["accepted_cents"], o["late_cents"], o["accepted_orders"], o["late_orders"])
                         == (t["revenue_cents"], t["late_order_cents"], st["accepted_orders"], t["late_orders"])))
        check(f"B5 grace={g}: accepted+late == 300 orders and 6,124,170 cents",
              st["accepted_orders"] + t["late_orders"] == 300 and t["revenue_cents"] + t["late_order_cents"] == 6124170,
              f"{st['accepted_orders']}+{t['late_orders']} orders; {t['revenue_cents']}+{t['late_order_cents']} cents")
        check(f"B5 grace={g}: independent oracle matches engine", rows[-1]["oracle_match"])
    exp = {0: (279, 21, 56916.90), 10: (286, 14, 57954.30), 120: (300, 0, 61241.70)}
    for g, (a, l, ils) in exp.items():
        r = next(x for x in rows if x["grace_s"] == g)
        check(f"B5 grace={g}: matches course reference ({a} accepted, {l} late, {ils:,.2f} ILS)",
              (r["accepted_orders"], r["late_orders"], r["accepted_ils"]) == (a, l, ils))
    late_counts = [r["late_orders"] for r in rows]
    check("B5 more grace -> never more late events (monotone)", late_counts == sorted(late_counts, reverse=True), str(late_counts))
    write_csv("B5_grace_comparison.csv", rows)
    for r in rows:
        say(f"grace={r['grace_s']:>3}s accepted={r['accepted_orders']:>3} late={r['late_orders']:>3} "
            f"accepted={r['accepted_ils']:>10,.2f} late={r['late_ils']:>9,.2f} closed-before-end={r['windows_closed_by_watermark_before_end']}")


# ================================================== B6: duplicates, replay, crash, reopen
def b6_replay():
    say("\n=== B6. Duplicates, replay of saved offsets, restart, crash before/after commit")
    data = teaching_fixture()
    db = new_db("b6_replay")
    with StreamEngine(db, **cfg()) as e:
        for d in data[:12]:
            e.ingest(d.event, offset=d.offset)
        before = canonical_results(e.snapshot())
        rev_before = before["totals"]["revenue_cents"]
        # same event_id at a NEW offset: duplicate, revenue unchanged, checkpoint advances
        a = data[0].event
        r = e.ingest(a, offset=e.next_offset)
        mid = canonical_results(e.snapshot())
        check("B6 same event_id at a new offset is a duplicate and does not increase revenue",
              r["status"] == "duplicate" and mid["totals"]["revenue_cents"] == rev_before
              and mid["stats"]["duplicates"] == before["stats"]["duplicates"] + 1 and mid["checkpoint"] == before["checkpoint"] + 1)
        # replay of an already-saved offset: state unchanged
        snap_a = canonical_results(e.snapshot())
        r2 = e.ingest(data[3].event, offset=3)
        check("B6 replay of an already-saved offset returns 'replayed' and changes nothing",
              r2["status"] == "replayed" and canonical_results(e.snapshot()) == snap_a)
        try:
            e.ingest(data[5].event, offset=e.next_offset + 5)
            gap = False
        except OffsetGapError:
            gap = True
        check("B6 an offset gap is rejected instead of silently losing records", gap)
    with StreamEngine(db, **cfg()) as e2:  # reopen the same file
        check("B6 after reopening the engine the checkpoint and state are exactly as saved",
              canonical_results(e2.snapshot()) == snap_a)
        for d in data[:6]:
            r3 = e2.ingest(d.event, offset=d.offset)
            assert r3["status"] == "replayed"
        check("B6 replaying old offsets after reopening does not change the durable state",
              canonical_results(e2.snapshot()) == snap_a)
        # finish the stream from the saved checkpoint
        for d in data[e2.next_offset - 1:]:
            pass
    # crash tests: before/after commit must converge to the failure-free business result
    baseline = None
    outputs = {}
    for stage in ("baseline", "before_commit", "after_commit"):
        p = new_db(f"b6_crash_{stage}")
        crash_offset = 8
        crashed = None
        with StreamEngine(p, **cfg()) as e:
            try:
                for d in data:
                    e.ingest(d.event, offset=d.offset, crash_at=stage if stage != "baseline" and d.offset == crash_offset else None)
            except InjectedCrash as exc:
                crashed = str(exc)
        with StreamEngine(p, **cfg()) as e:  # a NEW engine object = process restart
            cp = e.checkpoint
            for d in data:                      # replay the whole source from the start
                e.ingest(d.event, offset=d.offset)
            outputs[stage] = (canonical_results(e.finalize_demo()), cp, crashed)
    base = outputs["baseline"][0]
    check("B6 crash BEFORE commit: restart resumes from checkpoint 7 and reaches the same business result",
          outputs["before_commit"][1] == 7 and outputs["before_commit"][0] == base, outputs["before_commit"][2] or "")
    check("B6 crash AFTER commit: restart resumes from checkpoint 8 (offset 8 not re-applied) and same result",
          outputs["after_commit"][1] == 8 and outputs["after_commit"][0] == base, outputs["after_commit"][2] or "")
    write_csv("B6_crash_recovery.csv", [dict(stage=s, crashed=o[2], checkpoint_at_restart=o[1],
                                             revenue_cents=o[0]["totals"]["revenue_cents"], late=o[0]["stats"]["late"],
                                             equals_baseline=o[0] == base) for s, o in outputs.items()])
    # watermark does not move on late events; late event does not advance the max time
    with StreamEngine(new_db("b6_wm"), **cfg()) as e:
        for d in data[:9]:
            e.ingest(d.event, offset=d.offset)
        wm1 = e.snapshot()["watermark_ms"]
        r = e.ingest(data[9].event, offset=9)   # order-late
        wm2 = e.snapshot()["watermark_ms"]
    check("B6 a late event does not advance or regress the watermark", r["status"] == "late" and wm1 == wm2)


def main():
    asyncio.run(b1_live())
    asyncio.run(b2_modes())
    asyncio.run(b3_flush_reasons())
    b4_windows()
    b5_grace()
    b6_replay()
    failed = [r for r in RESULTS if not r[1]]
    say(f"\nTOTAL: {len(RESULTS) - len(failed)}/{len(RESULTS)} checks passed")
    json.dump([dict(name=n, passed=ok, detail=d) for n, ok, d in RESULTS], open(OUT / "partB_checks.json", "w", encoding="utf-8"),
              ensure_ascii=False, indent=1)
    LOG.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

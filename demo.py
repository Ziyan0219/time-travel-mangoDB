"""
Demo: Stock Price Time Travel Pipeline
========================================
Run with:
  python demo.py            → full demo (all sections)
  python demo.py --verbose  → show every pipeline step in detail
  python demo.py --persist  → save data to stocks.duckdb (survives restart)
"""

import sys
import os
from datetime import datetime, timedelta
from pipeline import StockPipeline, TimeTravelResult
from src.mongo_simulator import ValidationError

# ─── CLI flags ────────────────────────────────────────────────────────────────
VERBOSE = "--verbose" in sys.argv
PERSIST = "--persist" in sys.argv

# ─── ANSI colors ──────────────────────────────────────────────────────────────
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
RESET  = "\033[0m"

def h1(text):
    print(f"\n{BOLD}{BLUE}{'═'*64}{RESET}")
    print(f"{BOLD}{BLUE}  {text}{RESET}")
    print(f"{BOLD}{BLUE}{'═'*64}{RESET}")

def h2(text):
    pad = max(0, 54 - len(text))
    print(f"\n{BOLD}{CYAN}  ── {text} {'─' * pad}{RESET}")

def ok(label, text):  print(f"  {GREEN}✓{RESET}  {BOLD}{label}{RESET}  {text}")
def warn(text):       print(f"  {YELLOW}⚠{RESET}  {text}")
def err(text):        print(f"  {RED}✗{RESET}  {text}")
def q(text):          print(f"\n  {BOLD}❓ Q:{RESET} {text}")
def a(text):          print(f"  {GREEN}{BOLD}✅ A:{RESET} {text}")

# ─── Time helpers ─────────────────────────────────────────────────────────────
# Base: Monday 2026-03-16 09:30 AM (market open)
BASE = datetime(2026, 3, 16, 9, 30, 0)
DAY  = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}

def t(day: int, hour: int, minute: int = 0) -> datetime:
    return BASE + timedelta(days=day, hours=hour - 9, minutes=minute - 30)

# ─── Price change schedule ────────────────────────────────────────────────────
# (day, ticker, price, volume, hour, minute)
PRICE_CHANGES = [
    # Monday
    (0, "AAPL",  170.50, 1_200_000, 9,  30),
    (0, "GOOGL", 155.20,   890_000, 9,  30),
    (0, "MSFT",  410.30,   750_000, 9,  30),
    (0, "AAPL",  171.80, 1_100_000, 10, 15),
    (0, "GOOGL", 156.40,   810_000, 11,  0),
    (0, "AAPL",  169.90,   980_000, 14,  0),
    (0, "AAPL",  170.10,   870_000, 15, 30),
    # Wednesday
    (2, "AAPL",  172.40, 1_350_000, 9,  30),
    (2, "GOOGL", 157.80,   920_000, 9,  35),
    (2, "MSFT",  412.50,   680_000, 10,  0),
    (2, "AAPL",  173.20, 1_250_000, 11, 30),
    (2, "AAPL",  172.80,   990_000, 14, 45),
    # Friday
    (4, "AAPL",  174.50, 1_400_000, 9,  30),
    (4, "AAPL",  175.10, 1_300_000, 9,  36),   # ← THE price we want
    (4, "GOOGL", 159.00, 1_050_000, 9,  38),
    (4, "AAPL",  174.80, 1_150_000, 9,  45),
    (4, "MSFT",  415.60,   800_000, 10, 30),
    (4, "AAPL",  176.30, 1_600_000, 14,  0),
    (4, "GOOGL", 160.10,   930_000, 14, 30),
    (4, "AAPL",  175.90, 1_100_000, 15, 30),
    (4, "MSFT",  416.20,   720_000, 15, 45),
]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_demo():
    h1("📈 Stock Price Time Travel Pipeline — Full Demo")

    # ─── Persistence setup ────────────────────────────────────────────────
    if PERSIST and os.path.exists("stocks.duckdb"):
        print(f"\n  {YELLOW}[--persist]{RESET} Found existing stocks.duckdb — loading prior session data.")
    elif PERSIST:
        print(f"\n  {YELLOW}[--persist]{RESET} No existing DB found — starting fresh, will save to stocks.duckdb.")

    pipeline = StockPipeline(verbose=VERBOSE, persist=PERSIST)

    if VERBOSE:
        print(f"\n  {DIM}Verbose mode ON — every byte through the pipeline will be shown.{RESET}")

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 1 — Feed price changes
    # ─────────────────────────────────────────────────────────────────────
    h1("PHASE 1 — Simulating a Week of Price Changes")

    already_loaded = pipeline.stats["total_rows"] > 0
    if already_loaded:
        warn(f"DB already has {pipeline.stats['total_rows']} rows from a previous session.")
        warn("Skipping re-insertion of historical data (persistence working!)")
    else:
        print(f"  Feeding {len(PRICE_CHANGES)} updates through the pipeline...\n")
        for day, ticker, price, volume, hour, minute in PRICE_CHANGES:
            ts = t(day, hour, minute)
            if not VERBOSE:
                print(f"  {DIM}{DAY[day]} {ts.strftime('%H:%M')}{RESET}  "
                      f"{BOLD}{ticker}{RESET}  ${price:.2f}  vol={volume:>10,}")
            pipeline.record_price_change(ticker, price, volume, ts, _quiet=not VERBOSE)

        stats = pipeline.stats
        print(f"\n  {GREEN}Done!{RESET} {BOLD}{stats['total_rows']} rows{RESET} written "
              f"across {stats['unique_tickers']} tickers.")

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 2 — MongoDB vs BigQuery contrast
    # ─────────────────────────────────────────────────────────────────────
    h1("PHASE 2 — MongoDB vs BigQuery: The Fundamental Difference")

    h2("MongoDB (in-place) — current state only")
    print(f"  After {len(PRICE_CHANGES)} price changes, MongoDB still has exactly 3 documents:\n")
    for ticker in ["AAPL", "GOOGL", "MSFT"]:
        doc = pipeline.current_price_from_mongo(ticker)
        print(f"    {BOLD}{ticker}{RESET}  price=${doc['price']:.2f}  "
              f"last_updated={doc['last_updated'].strftime('%a %H:%M')}")
    warn("No history in MongoDB — all past prices are gone forever.")

    h2("BigQuery (append-only) — complete history")
    history = pipeline.bigquery.get_price_history("AAPL")
    print(f"  AAPL has {len(history)} rows in BigQuery:\n")
    for ticker, price, volume, event_time in history:
        print(f"    {DIM}{DAY[event_time.weekday()]} {event_time.strftime('%m/%d %H:%M')}{RESET}  "
              f"→  ${price:.2f}  vol={volume:>10,}")
    ok(f"{len(history)} AAPL price points", "preserved — zero data loss.")

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 3 — Time Travel Queries
    # ─────────────────────────────────────────────────────────────────────
    h1("PHASE 3 — Time Travel Queries")

    h2("Core scenario (the original design goal)")
    q("It's Sunday. What was AAPL's price on Friday at 9:36 AM?")
    r = pipeline.time_travel("AAPL", t(4, 9, 36))
    a(f"${r.price:.2f}  (event recorded at {r.event_time.strftime('%a %H:%M:%S')})")
    print(f"\n  {DIM}SQL:  WHERE ticker='AAPL' AND event_time <= '2026-03-20 09:36:00'"
          f"\n        ORDER BY event_time DESC  LIMIT 1{RESET}")

    h2("Query between updates — no event at exact minute")
    q("What was AAPL's price on Wednesday at 1:00 PM?")
    r = pipeline.time_travel("AAPL", t(2, 13, 0))
    a(f"${r.price:.2f}  (last update was {r.event_time.strftime('%a %H:%M')} — unchanged since)")

    h2("Full market snapshot at a moment")
    q("What were ALL prices on Friday at 9:36 AM?")
    for ticker, price, volume, event_time in pipeline.snapshot_all(t(4, 9, 36)):
        a(f"{ticker}: ${price:.2f}  (last change: {event_time.strftime('%a %m/%d %H:%M')})")

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 4 — Edge Cases
    # ─────────────────────────────────────────────────────────────────────
    h1("PHASE 4 — Edge Case Handling")

    # ── Edge case 1: Unknown ticker ────────────────────────────────────────
    h2("Edge Case 1: Query for a ticker that never existed")
    q("What was NVDA's price on Friday at 10:00?")
    r = pipeline.time_travel("NVDA", t(4, 10, 0))
    if r.status == TimeTravelResult.UNKNOWN:
        warn(f"NVDA has never been seen in this pipeline. Status: {r.status}")
    print(f"  {DIM}→ Caller gets status='{r.status}' instead of a crash or wrong result.{RESET}")

    # ── Edge case 2: Query before any data exists ──────────────────────────
    h2("Edge Case 2: Query before market ever opened")
    q("What was AAPL's price on Sunday (before the week started)?")
    r = pipeline.time_travel("AAPL", t(-1, 20, 0))   # Sunday before Monday
    if r.status == TimeTravelResult.NO_DATA:
        warn(f"AAPL exists but has no event before that time. Status: {r.status}")
    print(f"  {DIM}→ Distinct from UNKNOWN: the ticker exists, but has no data that early.{RESET}")

    # ── Edge case 3: Duplicate event ──────────────────────────────────────
    h2("Edge Case 3: Exact duplicate event (Pub/Sub at-least-once delivery)")
    print("  Sending the same AAPL $175.10 @ Fri 09:36 event a second time...")
    skipped_before = pipeline.stats["duplicates_skipped"]
    pipeline.record_price_change("AAPL", 175.10, 1_300_000, t(4, 9, 36), _quiet=True)
    skipped_after = pipeline.stats["duplicates_skipped"]
    if skipped_after > skipped_before:
        ok("Duplicate detected", f"skipped insertion. Total rows unchanged: "
                                  f"{pipeline.stats['total_rows']}")
    print(f"  {DIM}→ Idempotency: same event arriving twice doesn't corrupt the history.{RESET}")

    # ── Edge case 4: Late-arriving / out-of-order event ───────────────────
    h2("Edge Case 4: Late-arriving event (out-of-order insertion)")
    print("  Inserting a Wednesday 12:00 MSFT price that 'arrived late' after Friday data...")
    pipeline.record_price_change("MSFT", 413.00, 700_000, t(2, 12, 0), _quiet=True)
    r_before = pipeline.time_travel("MSFT", t(2, 11, 59))
    r_after  = pipeline.time_travel("MSFT", t(2, 12, 0))
    ok("Time travel still works:", f"Wed 11:59 → ${r_before.price:.2f}  |  Wed 12:00 → ${r_after.price:.2f}")
    print(f"  {DIM}→ Because time travel uses event_time (not ingestion_time), "
          f"late arrivals slot into the correct position automatically.{RESET}")

    # ── Edge case 5: Schema validation ────────────────────────────────────
    h2("Edge Case 5: Invalid data — schema validation")
    invalid_cases = [
        ("AAPL",  -50.0, 1000,   t(4, 10, 0), "negative price"),
        ("AAPL",    0.0, 1000,   t(4, 10, 0), "zero price"),
        ("",     150.0,  1000,   t(4, 10, 0), "empty ticker"),
        ("AAPL", 150.0,    -1,   t(4, 10, 0), "negative volume"),
    ]
    for ticker, price, volume, ts, reason in invalid_cases:
        try:
            pipeline.record_price_change(ticker, price, volume, ts)
            err(f"should have rejected: {reason}")
        except ValidationError as e:
            ok(f"Rejected ({reason}):", str(e))

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 5 — Persistence demo
    # ─────────────────────────────────────────────────────────────────────
    h1("PHASE 5 — Persistence")

    if PERSIST:
        ok("Data persisted to", "stocks.duckdb")
        print(f"  Run  {BOLD}python demo.py --persist{RESET}  again to see the pipeline "
              f"reload all {pipeline.stats['total_rows']} rows without re-processing.")
    else:
        warn("Running in in-memory mode — data will be lost when this process exits.")
        print(f"  Run  {BOLD}python demo.py --persist{RESET}  to enable persistent storage.")

    # ─────────────────────────────────────────────────────────────────────
    # PHASE 6 — Observability
    # ─────────────────────────────────────────────────────────────────────
    h1("PHASE 6 — Observability / Health Report")

    report = pipeline.health_report()
    lag    = report["lag_ms"]

    h2("Processing Lag  (Pub/Sub publish → Cloud Function write)")
    print(f"  avg={lag['avg']:.3f} ms   max={lag['max']:.3f} ms   "
          f"min={lag['min']:.3f} ms   p95={lag['p95']:.3f} ms")
    print(f"  {DIM}In production: spikes here mean Cloud Functions are overwhelmed.{RESET}")

    h2("Event Counts per Ticker")
    for ticker, count in sorted(report["events_per_ticker"].items()):
        print(f"  {BOLD}{ticker}{RESET}: {count} events")

    h2("Late / Out-of-Order Events")
    if report["late_events"] == 0:
        ok("Late events:", "0  — all events arrived in order")
    else:
        warn(f"{report['late_events']} late event(s) detected — "
             f"time travel still correct because we use event_time, not ingestion_time")

    h2("Data Freshness  (latest event_time per ticker)")
    for ticker, ts_str in sorted(report["data_freshness"].items()):
        print(f"  {BOLD}{ticker}{RESET}: last seen at {ts_str}")
    print(f"  {DIM}In production: staleness here during market hours = upstream problem.{RESET}")

    # ─────────────────────────────────────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────────────────────────────────────
    h1("Summary")
    s = pipeline.stats
    print(f"""
  {BOLD}MongoDB:{RESET}       {len(pipeline.mongo.find_all())} documents — in-place — current state only
  {BOLD}BigQuery:{RESET}      {s['total_rows']} rows — append-only — full history
  {BOLD}Processed:{RESET}     {s['events_processed']} events this session
  {BOLD}Duplicates:{RESET}    {s['duplicates_skipped']} skipped (idempotency)
  {BOLD}Late events:{RESET}   {report['late_events']} (out-of-order arrivals)
  {BOLD}Avg lag:{RESET}       {lag['avg']:.3f} ms

  {BOLD}Time travel SQL:{RESET}
    WHERE ticker = ? AND event_time <= <target>
    ORDER BY event_time DESC  LIMIT 1

  {BOLD}Features:{RESET}
    ✓ Persistence         (--persist flag, DuckDB file, restart recovery)
    ✓ Verbose pipeline    (--verbose flag, shows every stage)
    ✓ Edge case handling  (unknown ticker, no-data, duplicates, out-of-order, validation)
    ✓ Observability       (lag, late events, data freshness, events per ticker)
    """)


if __name__ == "__main__":
    run_demo()

"""
query.py — Time Travel Query Interface
========================================
Query any historical price from the pipeline's DuckDB event log.

USAGE:
  python query.py                              # interactive REPL
  python query.py AAPL "2026-03-20 09:36"     # single lookup
  python query.py --snapshot "2026-03-20 09:36"  # all tickers at a moment
  python query.py --history AAPL              # full price history for one ticker
  python query.py --health                    # show pipeline health metrics
"""

import os
import sys
from datetime import datetime
from pipeline import StockPipeline, TimeTravelResult, DEFAULT_DB, DEFAULT_SNAPSHOT
from src.bigquery_simulator import BigQuerySimulator

# ── ANSI ──────────────────────────────────────────────────────────────────────
G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; C = "\033[96m"
D = "\033[2m"; RESET = "\033[0m"; BOLD = "\033[1m"

HELP = f"""
  {BOLD}Commands:{RESET}
    {C}<TICKER> <YYYY-MM-DD HH:MM>{RESET}        time travel query
    {C}snapshot <YYYY-MM-DD HH:MM>{RESET}         all tickers at a moment
    {C}history  <TICKER>{RESET}                   full price history
    {C}health{RESET}                              pipeline metrics
    {C}tickers{RESET}                             list all known tickers
    {C}exit{RESET}                                quit

  {BOLD}Examples:{RESET}
    AAPL 2026-03-20 09:36
    snapshot 2026-03-20 09:36
    history GOOGL
"""


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def parse_time(s: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse '{s}'. Use: YYYY-MM-DD HH:MM")


def print_result(result: TimeTravelResult):
    if result.found:
        lag = (result.as_of_time - result.event_time).total_seconds()
        lag_str = f"{lag/3600:.1f}h before query" if lag > 60 else "exact match"
        print(f"\n  {G}✓{RESET}  {BOLD}{result.ticker}{RESET}"
              f"  @  {result.as_of_time.strftime('%Y-%m-%d %H:%M')}"
              f"  →  {BOLD}${result.price:.2f}{RESET}"
              f"  {D}(recorded {result.event_time.strftime('%Y-%m-%d %H:%M')} — {lag_str}){RESET}\n")
    elif result.status == TimeTravelResult.NO_DATA:
        print(f"\n  {Y}⚠{RESET}  {result.ticker} exists but has no data before "
              f"{result.as_of_time.strftime('%Y-%m-%d %H:%M')}\n")
    else:
        print(f"\n  {R}✗{RESET}  '{result.ticker}' not found in pipeline.\n")


def cmd_snapshot(pipeline, time_str):
    as_of = parse_time(time_str)
    rows  = pipeline.snapshot_all(as_of)
    if not rows:
        print(f"  {Y}No data before {as_of}{RESET}")
        return
    print(f"\n  {BOLD}Snapshot @ {as_of.strftime('%Y-%m-%d %H:%M')}{RESET}\n")
    for ticker, price, volume, event_time in sorted(rows):
        print(f"  {BOLD}{ticker:8s}{RESET}  ${price:>10.2f}"
              f"  {D}last change: {event_time.strftime('%Y-%m-%d %H:%M')}{RESET}")
    print()


def cmd_history(pipeline, ticker):
    rows = pipeline.bigquery.get_price_history(ticker.upper())
    if not rows:
        print(f"  {Y}No history for {ticker.upper()}{RESET}")
        return
    print(f"\n  {BOLD}Price history: {ticker.upper()}{RESET}  ({len(rows)} events)\n")
    for _, price, volume, event_time in rows:
        print(f"  {D}{event_time.strftime('%Y-%m-%d %H:%M')}{RESET}"
              f"  →  ${price:.2f}"
              f"  {D}vol: {volume:>12,}{RESET}")
    print()


def cmd_health(pipeline):
    report = pipeline.health_report()
    lag    = report["lag_ms"]
    stats  = pipeline.stats

    print(f"\n  {BOLD}Pipeline Health{RESET}\n")
    print(f"  Total rows in log   : {stats['total_rows']}")
    print(f"  Unique tickers      : {stats['unique_tickers']}")
    print(f"  Events processed    : {report['events_processed']}")
    print(f"  Duplicates skipped  : {report['duplicates_skipped']}")
    print(f"  Late events         : {report['late_events']}")
    print(f"\n  {BOLD}Processing Lag{RESET}  (Pub/Sub → BigQuery)")
    print(f"  avg={lag['avg']:.3f} ms   max={lag['max']:.3f} ms"
          f"   min={lag['min']:.3f} ms   p95={lag['p95']:.3f} ms")
    print(f"\n  {BOLD}Data Freshness{RESET}")
    for ticker, ts_str in sorted(report["data_freshness"].items()):
        print(f"  {BOLD}{ticker:8s}{RESET}  last seen: {ts_str}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def _open_pipeline():
    """
    Try to open the pipeline against the live DuckDB file.
    If DuckDB is locked (connect.py is running), fall back to the Parquet
    snapshot that connect.py exports after every write — so queries still
    work in real time, just reading the snapshot that's at most one event old.
    """
    try:
        return StockPipeline(persist=True, verbose=False, read_only=True)
    except Exception:
        pass   # DuckDB file locked — fall through to Parquet fallback

    if os.path.exists(DEFAULT_SNAPSHOT):
        print(f"  {Y}Note:{RESET} DuckDB is locked by connect.py — "
              f"reading from Parquet snapshot (data is current to last write).{RESET}\n")
        # Build a minimal pipeline-like object backed by the Parquet snapshot
        bq = BigQuerySimulator.from_snapshot(DEFAULT_SNAPSHOT)
        pipeline = StockPipeline.__new__(StockPipeline)
        pipeline.verbose   = False
        pipeline.persist   = True
        pipeline.read_only = True
        pipeline.mongo     = None
        pipeline.topic     = None
        pipeline.sub       = None
        pipeline.bigquery  = bq
        from src.metrics import PipelineMetrics
        pipeline.metrics   = PipelineMetrics()
        pipeline._events_processed   = 0
        pipeline._skipped_duplicates = 0
        return pipeline

    print(f"\n  {R}Error:{RESET} DuckDB file is locked and no Parquet snapshot exists yet.")
    print(f"  Wait a moment for connect.py to write its first event, then retry.\n")
    sys.exit(1)


def main():
    pipeline = _open_pipeline()
    stats    = pipeline.bigquery.get_stats()

    print(f"\n  {BOLD}Time Travel Query{RESET}"
          f"  {D}({stats['total_rows']} rows · "
          f"{stats['unique_tickers']} tickers in stocks.duckdb){RESET}")

    if stats["total_rows"] == 0:
        print(f"\n  {Y}No data yet.{RESET}  Run  {BOLD}python connect.py{RESET}"
              "  first to start capturing price changes.\n")
        sys.exit(0)

    known = pipeline.bigquery.get_known_tickers()
    print(f"  Tickers: {', '.join(sorted(known))}\n")

    # ── Single-shot CLI args ──────────────────────────────────────────────────
    args = sys.argv[1:]

    if len(args) == 2 and not args[0].startswith("--"):
        # python query.py AAPL "2026-03-20 09:36"
        try:
            print_result(pipeline.time_travel(args[0].upper(), parse_time(args[1])))
        except ValueError as e:
            print(f"  {R}{e}{RESET}")
        return

    if args[:1] == ["--snapshot"] and len(args) == 2:
        try:
            cmd_snapshot(pipeline, args[1])
        except ValueError as e:
            print(f"  {R}{e}{RESET}")
        return

    if args[:1] == ["--history"] and len(args) == 2:
        cmd_history(pipeline, args[1])
        return

    if args == ["--health"]:
        cmd_health(pipeline)
        return

    # ── Interactive REPL ──────────────────────────────────────────────────────
    print(HELP)
    while True:
        try:
            line = input("  query> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line:
            continue

        parts = line.split(None, 1)
        cmd   = parts[0].lower()

        if cmd in ("exit", "quit"):
            break

        # Release the old DuckDB connection before opening a new one.
        # Without this, the previous read_only connection holds the file lock
        # and blocks connect.py from opening the same file for writing.
        pipeline.bigquery.close()
        pipeline = _open_pipeline()

        if cmd == "health":
            cmd_health(pipeline)
            continue

        if cmd == "tickers":
            print(f"\n  {', '.join(sorted(pipeline.bigquery.get_known_tickers()))}\n")
            continue

        if cmd == "snapshot":
            if len(parts) < 2:
                print("  Usage: snapshot YYYY-MM-DD HH:MM")
                continue
            try:
                cmd_snapshot(pipeline, parts[1])
            except ValueError as e:
                print(f"  {R}{e}{RESET}")
            continue

        if cmd == "history":
            if len(parts) < 2:
                print("  Usage: history TICKER")
                continue
            cmd_history(pipeline, parts[1])
            continue

        # Default: time travel query  (TICKER YYYY-MM-DD HH:MM)
        if len(parts) < 2:
            print("  Usage: TICKER YYYY-MM-DD HH:MM")
            continue
        try:
            ticker = parts[0].upper()
            as_of  = parse_time(parts[1])
            print_result(pipeline.time_travel(ticker, as_of))
        except ValueError as e:
            print(f"  {R}{e}{RESET}")


if __name__ == "__main__":
    main()

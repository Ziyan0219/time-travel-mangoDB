"""
connect.py — Connect Real MongoDB to the Time Travel Pipeline
=============================================================
This script opens a Change Stream on your MongoDB collection and
routes every price update through the full pipeline into DuckDB.

SETUP (10 minutes):
  1. pip install -r requirements.txt
  2. cp .env.example .env  →  fill in MONGODB_URI + field names
  3. python connect.py

REQUIREMENTS:
  - MongoDB must run as a replica set (or use MongoDB Atlas).
    Change Streams are not available on standalone instances.
  - The collection must already exist with at least one document.

USAGE:
  Terminal 1:  python connect.py          ← listens for changes
  Terminal 2:  python query.py            ← queries the history

HOW IT WORKS:
  Real MongoDB emits a Change Stream event for every write.
  This script is the "Change Stream listener" that was simulated
  by drain_change_stream() in our demo — here it's the real thing:
  a blocking cursor that yields events as MongoDB produces them.
"""

import os
import sys
import time
import signal
from datetime import datetime, timezone

# ── Load .env ─────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass   # dotenv optional; fall back to real env vars

MONGODB_URI     = os.getenv("MONGODB_URI",    "mongodb://localhost:27017")
DB_NAME         = os.getenv("DB_NAME",        "stocks")
COLLECTION_NAME = os.getenv("COLLECTION",     "prices")
TICKER_FIELD    = os.getenv("TICKER_FIELD",   "ticker")
PRICE_FIELD     = os.getenv("PRICE_FIELD",    "price")
VOLUME_FIELD    = os.getenv("VOLUME_FIELD",   "volume")

# ── Dependency checks ─────────────────────────────────────────────────────────
try:
    from pymongo import MongoClient
    from pymongo.errors import PyMongoError, OperationFailure
except ImportError:
    print("\n  Error: pymongo not installed.")
    print("  Fix:   pip install -r requirements.txt\n")
    sys.exit(1)

from pipeline import StockPipeline
from src.mongo_simulator import ValidationError

# ── ANSI ──────────────────────────────────────────────────────────────────────
G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"; D = "\033[2m"; RESET = "\033[0m"; BOLD = "\033[1m"


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def extract_event(change: dict):
    """
    Pull (ticker, price, volume, timestamp) out of a raw MongoDB Change Event.
    Returns None if the event is missing required fields or is an irrelevant op type.

    operationType values we care about: insert, update, replace
    We ignore: delete, drop, rename, invalidate
    """
    op = change.get("operationType")
    if op not in ("insert", "update", "replace"):
        return None

    # fullDocument is populated when full_document="updateLookup" is set on the stream.
    # On insert/replace it's always present; on update it requires the lookup option.
    doc = change.get("fullDocument") or {}

    ticker = doc.get(TICKER_FIELD)
    price  = doc.get(PRICE_FIELD)

    if not ticker or price is None:
        return None   # document doesn't match expected schema

    volume = int(doc.get(VOLUME_FIELD, 0))

    # clusterTime: MongoDB's logical clock, a bson.Timestamp(seconds, increment).
    # Converting to Python datetime — this is the "event_time" in our pipeline.
    cluster_ts = change.get("clusterTime")
    if cluster_ts:
        timestamp = datetime.fromtimestamp(cluster_ts.time)  # local time, matches query input
    else:
        timestamp = datetime.utcnow()

    return str(ticker).upper(), float(price), volume, timestamp


def check_replica_set(client):
    """
    Change Streams require a replica set. Detect this early and give a clear error.
    Returns True if OK, prints a fix and returns False if not.
    """
    try:
        info = client.admin.command("isMaster")
        if "setName" in info or info.get("isreplicaset"):
            return True
        # Atlas clusters always have setName
        if "msg" in info and "isdbgrid" in info["msg"]:
            return True   # sharded cluster — also fine
        print(f"\n  {Y}Warning:{RESET} MongoDB does not appear to be running as a replica set.")
        print("  Change Streams require a replica set or Atlas cluster.")
        print("\n  To enable a local replica set:")
        print(f"  {D}  1. Stop mongod")
        print(f"  2. Add 'replication:\\n    replSetName: rs0' to mongod.conf")
        print(f"  3. Start mongod, then run:  mongosh --eval 'rs.initiate()'{RESET}")
        print()
        return False
    except Exception:
        return True   # can't check — proceed and let watch() fail naturally


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run():
    uri_display = MONGODB_URI if len(MONGODB_URI) < 50 else MONGODB_URI[:47] + "..."
    print(f"""
{BOLD}╔══════════════════════════════════════════════════════════════╗
║   MongoDB → Time Travel Pipeline                             ║
╠══════════════════════════════════════════════════════════════╣{RESET}
  MongoDB URI  :  {uri_display}
  Database     :  {DB_NAME}
  Collection   :  {COLLECTION_NAME}
  Ticker field :  {TICKER_FIELD}
  Price field  :  {PRICE_FIELD}
  Volume field :  {VOLUME_FIELD}  {D}(optional){RESET}
{BOLD}╚══════════════════════════════════════════════════════════════╝{RESET}
""")

    # ── Connect ───────────────────────────────────────────────────────────────
    print("  Connecting to MongoDB...", end="", flush=True)
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
        print(f"  {G}✓ Connected{RESET}")
    except Exception as e:
        print(f"  {R}✗ Failed{RESET}")
        print(f"\n  Error: {e}")
        print("\n  Check that MONGODB_URI in .env is correct and MongoDB is running.")
        sys.exit(1)

    check_replica_set(client)

    # ── Pipeline (persistent — survives restarts) ─────────────────────────────
    pipeline = StockPipeline(verbose=True, persist=True)

    collection = client[DB_NAME][COLLECTION_NAME]
    events_seen = 0
    start_time  = time.time()

    print(f"\n  {G}Watching {DB_NAME}.{COLLECTION_NAME} for changes...{RESET}")
    print(f"  {D}In another terminal run:  python query.py{RESET}")
    print("  Press Ctrl+C to stop.\n")

    # ── Change Stream loop ────────────────────────────────────────────────────
    #
    # collection.watch() opens a cursor backed by MongoDB's oplog.
    # This is the REAL version of drain_change_stream() in our simulator:
    #   - Blocking: each iteration waits until the next event arrives
    #   - Resumable: if interrupted, re-open with resume_after=last_token
    #   - full_document="updateLookup": fetch the complete document on update
    #     (without this, update events only contain the changed fields)
    #
    resume_token = None

    while True:   # outer loop: reconnect on transient errors
        try:
            watch_kwargs = dict(full_document="updateLookup")
            if resume_token:
                watch_kwargs["resume_after"] = resume_token
                print(f"  {Y}Resuming from last checkpoint...{RESET}")

            with collection.watch(**watch_kwargs) as stream:
                for change in stream:
                    resume_token = stream.resume_token  # save for reconnect

                    result = extract_event(change)
                    if result is None:
                        continue

                    ticker, price, volume, timestamp = result
                    events_seen += 1

                    try:
                        pipeline.record_price_change(ticker, price, volume, timestamp)
                    except ValidationError as e:
                        print(f"  {Y}[Validation]{RESET} Skipped: {e}")

        except KeyboardInterrupt:
            elapsed = time.time() - start_time
            report  = pipeline.health_report()
            lag     = report["lag_ms"]
            print(f"\n\n  {BOLD}Session Summary{RESET}")
            print(f"  ─────────────────────────────────────")
            print(f"  Runtime        : {elapsed:.0f}s")
            print(f"  Events seen    : {events_seen}")
            print(f"  Duplicates     : {pipeline.stats['duplicates_skipped']}")
            print(f"  Late events    : {report['late_events']}")
            print(f"  Avg lag        : {lag['avg']:.3f} ms")
            print(f"  ─────────────────────────────────────")
            print(f"  Data saved to stocks.duckdb")
            print(f"  Run  {BOLD}python query.py{RESET}  to query it.\n")
            break

        except OperationFailure as e:
            # Change Streams not supported (standalone instance)
            print(f"\n  {R}✗ Change Stream error:{RESET} {e}")
            print("  MongoDB must be a replica set or Atlas cluster.")
            sys.exit(1)

        except PyMongoError as e:
            # Transient network error — wait and reconnect using resume_token
            print(f"\n  {Y}Connection lost ({e}). Reconnecting in 5s...{RESET}")
            time.sleep(5)
            continue


if __name__ == "__main__":
    run()

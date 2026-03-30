"""
seed.py — Insert test stock prices into MongoDB Atlas
=======================================================
Covers a broad set of edge cases so you can verify every query scenario.

Run this while connect.py is running in Terminal 1.
The script prints a timestamped log — keep it open alongside query.py
so you know exactly which times to use in your queries.

Usage:  python seed.py
"""

import time
import sys
import os
from datetime import datetime

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

MONGODB_URI     = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME         = os.getenv("DB_NAME",     "stocks")
COLLECTION_NAME = os.getenv("COLLECTION",  "prices")

try:
    from pymongo import MongoClient
except ImportError:
    print("Run:  pip install -r requirements.txt")
    sys.exit(1)

G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"
RESET = "\033[0m"; BOLD = "\033[1m"; D = "\033[2m"

# ── Script ────────────────────────────────────────────────────────────────────
# (ticker, price, volume, note)
# Designed to exercise every interesting query scenario.
SCRIPT = [
    # ① Opening prices — all 3 tickers appear for the first time
    ("AAPL",  170.50, 1_200_000, "AAPL opens"),
    ("GOOGL", 155.20,   890_000, "GOOGL opens"),
    ("MSFT",  410.30,   750_000, "MSFT opens"),

    # ② AAPL ticks up — time-travel between ① and ② should see $170.50
    ("AAPL",  171.80, 1_100_000, "AAPL ↑"),

    # ③ TSLA enters mid-session — before this point TSLA is UNKNOWN
    ("TSLA",  185.00, 2_100_000, "TSLA enters (new ticker)"),

    # ④ AAPL drops — price reversal, tests non-monotonic history
    ("AAPL",  169.90,   980_000, "AAPL ↓ reversal"),

    # ⑤ GOOGL ticks up
    ("GOOGL", 156.40,   810_000, "GOOGL ↑"),

    # ⑥ AAPL big jump — snapshot here should show AAPL at $175.10
    ("AAPL",  175.10, 1_300_000, "AAPL ↑↑ big jump"),

    # ⑦ MSFT ticks up
    ("MSFT",  415.60,   800_000, "MSFT ↑"),

    # ⑧ AAPL slight drop after peak — tests that peak is still reachable via time travel
    ("AAPL",  174.80, 1_150_000, "AAPL ↓ slight drop"),

    # ⑨ TSLA big move — tests second update on a "late" ticker
    ("TSLA",  192.50, 2_500_000, "TSLA ↑↑ big move (+7.50)"),

    # ⑩ NVDA enters very late — only one data point, tests single-event ticker
    ("NVDA",  890.00,   650_000, "NVDA enters (single event)"),
]


def run():
    client     = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    collection = client[DB_NAME][COLLECTION_NAME]

    print(f"""
  {BOLD}Seeding {DB_NAME}.{COLLECTION_NAME}{RESET}
  {D}Each write is timestamped — note the times for use in query.py{RESET}

  {'─'*62}
  {'#':>3}  {'Ticker':<8}  {'Price':>8}  {'Note':<28}  Time written
  {'─'*62}""")

    log = []   # collect (step, ticker, price, ts) for summary at end

    for i, (ticker, price, volume, note) in enumerate(SCRIPT, 1):
        collection.update_one(
            {"ticker": ticker},
            {"$set": {"ticker": ticker, "price": price, "volume": volume}},
            upsert=True,
        )
        ts = datetime.now()
        log.append((i, ticker, price, ts))
        print(f"  {G}#{i:>2}{RESET}  {BOLD}{ticker:<8}{RESET}"
              f"  ${price:>8.2f}  {D}{note:<28}{RESET}"
              f"  {ts.strftime('%H:%M:%S')}")
        time.sleep(1.5)

    print(f"\n  {'─'*62}")
    print(f"\n  {G}✓  Done seeding {len(SCRIPT)} events across "
          f"{len(set(t for t,*_ in SCRIPT))} tickers.{RESET}")

    # ── Print ready-to-use query cheat sheet ──────────────────────────────────
    first_ts = log[0][3]
    last_ts  = log[-1][3]
    mid_ts   = log[5][3]   # just after AAPL's reversal (#6)
    tsla_enter = log[4][3] # right after TSLA first appears (#5)

    print(f"""
  {BOLD}━━ Query cheat sheet (copy-paste into query.py) ━━{RESET}

  Replace DATE with today: {BOLD}{first_ts.strftime('%Y-%m-%d')}{RESET}

  {Y}# Time travel — AAPL between open and first tick{RESET}
  AAPL DATE {log[0][3].strftime('%H:%M')}   → expect $170.50  (opening price)

  {Y}# Time travel — AAPL after reversal, before big jump{RESET}
  AAPL DATE {log[5][3].strftime('%H:%M')}   → expect $169.90  (reversal price)

  {Y}# Time travel — AAPL at its peak{RESET}
  AAPL DATE {log[7][3].strftime('%H:%M')}   → expect $175.10  (big jump)

  {Y}# TSLA before it existed  (UNKNOWN){RESET}
  TSLA DATE {log[0][3].strftime('%H:%M')}   → expect: ticker not found

  {Y}# TSLA just after it appeared{RESET}
  TSLA DATE {log[4][3].strftime('%H:%M')}   → expect $185.00

  {Y}# NVDA — only one data point{RESET}
  NVDA DATE {last_ts.strftime('%H:%M')}    → expect $890.00

  {Y}# Full snapshot — all tickers at peak AAPL moment{RESET}
  snapshot DATE {log[7][3].strftime('%H:%M')}

  {Y}# Full history — all AAPL price changes{RESET}
  history AAPL
""")

    client.close()


if __name__ == "__main__":
    run()

#!/usr/bin/env python3
"""
Compare what is in your database with what the simulator says should be there.

Checks:
  missing    - valid messages that never made it into the DB          (data loss)
  duplicated - the same msg_id stored more than once                  (not idempotent)
  rejected   - messages that should have been rejected but were stored (validation hole)
  unknown    - msg_ids in the DB the simulator never produced         (e.g. leftovers from an older run)

Your table needs a column holding the message's msg_id. By default this runs
`SELECT msg_id::text FROM telemetry`. Override it with --query if your schema differs.

Usage:
  python tools/verify.py --dsn postgresql://iot:iot@localhost:5432/iot
  python tools/verify.py --query "SELECT msg_id::text FROM readings_raw"

Tip: TRUNCATE your table before each simulator run, or you'll see "unknown" ids.
Wait a few seconds after the simulator ends so your workers can drain their queues.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter


def load_ids(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line)["msg_id"] for line in f if line.strip()]


def compare(truth_ids: list[str], rejected_ids: list[str], db_ids: list[str]) -> dict:
    truth, rejected = set(truth_ids), set(rejected_ids)
    counts = Counter(db_ids)
    stored = set(counts)
    return {
        "expected": len(truth),
        "stored_rows": len(db_ids),
        "missing": sorted(truth - stored),
        "duplicated": sorted(k for k, v in counts.items() if v > 1),
        "rejected_but_stored": sorted(rejected & stored),
        "unknown": sorted(stored - truth - rejected),
    }


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dsn", default=os.getenv("DATABASE_URL", "postgresql://iot:iot@localhost:5432/iot"))
    p.add_argument("--query", default="SELECT msg_id::text FROM telemetry")
    p.add_argument("--out", default="sim_out", help="simulator output dir")
    a = p.parse_args()

    import psycopg

    with psycopg.connect(a.dsn) as conn:
        db_ids = [r[0] for r in conn.execute(a.query).fetchall()]

    r = compare(load_ids(f"{a.out}/truth.jsonl"), load_ids(f"{a.out}/rejected.jsonl"), db_ids)

    print(f"Expected valid messages : {r['expected']}")
    print(f"Rows in DB              : {r['stored_rows']}")
    ok = True
    for key, label in [("missing", "MISSING (data loss)"), ("duplicated", "DUPLICATED msg_ids"),
                       ("rejected_but_stored", "SHOULD HAVE BEEN REJECTED"), ("unknown", "UNKNOWN msg_ids")]:
        items = r[key]
        mark = "OK  " if not items else "FAIL"
        if items and key != "unknown":
            ok = False
        print(f"[{mark}] {label}: {len(items)}" + (f"   e.g. {items[:3]}" if items else ""))
    print("\nPASS" if ok else "\nFAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

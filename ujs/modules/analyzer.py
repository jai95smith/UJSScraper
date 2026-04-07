#!/usr/bin/env python3
"""Slow background analyzer — processes queue + stale dockets at a safe rate."""

import time, traceback
from datetime import datetime

from ujs import db
from ujs.modules.ingest import deep_analyze_docket

DEFAULT_DELAY = 10  # seconds between each analysis


def run(delay=DEFAULT_DELAY):
    """Run forever: process queue items first, then stale dockets."""
    print(f"[analyzer] Starting slow analyzer (delay={delay}s)")

    while True:
        try:
            # 1. Process on-demand queue (priority)
            with db.connect() as conn:
                job = db.claim_ingest_job(conn)

            if job:
                job_id, docket_number = job
                print(f"[analyzer] Queue: {docket_number}")
                try:
                    deep_analyze_docket(docket_number)
                    with db.connect() as conn:
                        db.complete_ingest_job(conn, job_id)
                    print(f"[analyzer] Done: {docket_number}")
                except Exception as e:
                    err = str(e)
                    print(f"[analyzer] Error: {docket_number}: {err}")
                    with db.connect() as conn:
                        db.complete_ingest_job(conn, job_id, error=err)
                    if "429" in err:
                        print("[analyzer] Rate limited, pausing 5 min...")
                        time.sleep(300)
                        continue
                time.sleep(delay)
                continue

            # 2. Refresh stale dockets (one at a time)
            with db.connect() as conn:
                stale = db.get_stale_dockets(conn, active_hours=24, closed_days=7, limit=1)

            if stale:
                dn = stale[0]["docket_number"]
                print(f"[analyzer] Refresh: {dn}")
                try:
                    deep_analyze_docket(dn)
                except Exception as e:
                    err = str(e)
                    print(f"[analyzer] Refresh error {dn}: {err}")
                    if "429" in err:
                        print("[analyzer] Rate limited, pausing 5 min...")
                        time.sleep(300)
                        continue
                time.sleep(delay)
                continue

            # 3. Nothing to do — wait and check again
            time.sleep(30)

        except Exception as e:
            print(f"[analyzer] Unexpected error: {e}")
            traceback.print_exc()
            time.sleep(60)


def main():
    import argparse
    p = argparse.ArgumentParser(description="Slow background docket analyzer")
    p.add_argument("--delay", type=int, default=DEFAULT_DELAY,
                   help=f"Seconds between each analysis (default: {DEFAULT_DELAY})")
    args = p.parse_args()
    run(delay=args.delay)


if __name__ == "__main__":
    main()

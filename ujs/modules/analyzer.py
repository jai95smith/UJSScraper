#!/usr/bin/env python3
"""Slow background analyzer — processes queue + stale dockets at a safe rate."""

import time, traceback
from datetime import datetime

from ujs import db
from ujs.modules.ingest import deep_analyze_docket

DEFAULT_DELAY = 5  # seconds between each analysis (~12/min)
_failed_dockets = set()  # skip dockets that keep failing


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

            # 2. Analyze unanalyzed cases
            with db.connect() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT c.docket_number FROM cases c
                    LEFT JOIN analyses a ON c.docket_number = a.docket_number AND a.doc_type = 'docket'
                    WHERE a.id IS NULL
                    ORDER BY c.created_at DESC LIMIT 1
                """)
                row = cur.fetchone()

            if row:
                dn = row[0]
                if dn in _failed_dockets:
                    # Skip known failures — mark as analyzed with empty to unblock queue
                    with db.connect() as conn:
                        db.store_analysis(conn, dn, {"error": "analysis_failed"}, "docket")
                    continue
                print(f"[analyzer] Analyze: {dn}")
                _start = time.time()
                try:
                    deep_analyze_docket(dn)
                    _dur = int((time.time() - _start) * 1000)
                    print(f"[analyzer] Done: {dn} ({_dur}ms)")
                    db.log_event("analyzer", "analyzed", docket_number=dn, duration_ms=_dur)
                except Exception as e:
                    err = str(e)
                    _dur = int((time.time() - _start) * 1000)
                    print(f"[analyzer] Error: {dn}: {err}")
                    db.log_event("analyzer", "error", docket_number=dn, detail=err, duration_ms=_dur, success=False)
                    _failed_dockets.add(dn)
                    if "429" in err:
                        print("[analyzer] Rate limited, pausing 5 min...")
                        time.sleep(300)
                        continue
                time.sleep(delay)
                continue

            # 3. Refresh stale dockets (one at a time)
            with db.connect() as conn:
                stale = db.get_stale_dockets(conn, active_hours=24, closed_days=7, limit=1)

            if stale:
                dn = stale[0]["docket_number"]
                print(f"[analyzer] Refresh: {dn}")
                _start = time.time()
                try:
                    deep_analyze_docket(dn)
                    _dur = int((time.time() - _start) * 1000)
                    db.log_event("analyzer", "refreshed", docket_number=dn, duration_ms=_dur)
                except Exception as e:
                    err = str(e)
                    _dur = int((time.time() - _start) * 1000)
                    print(f"[analyzer] Refresh error {dn}: {err}")
                    db.log_event("analyzer", "refresh_error", docket_number=dn, detail=err, duration_ms=_dur, success=False)
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

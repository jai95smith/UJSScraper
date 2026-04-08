#!/usr/bin/env python3
"""Slow background analyzer — processes queue + stale dockets at a safe rate."""

import os, time, traceback, random
from datetime import datetime

import psycopg2
from ujs import db
from ujs.modules.ingest import deep_analyze_docket

DEFAULT_DELAY = 3  # seconds between each analysis (~20/min)
_failed_dockets = set()  # skip dockets that keep failing


def run(delay=DEFAULT_DELAY):
    """Run forever: process queue items first, then stale dockets."""
    worker_id = os.getpid()
    print(f"[analyzer:{worker_id}] Starting (delay={delay}s)")

    while True:
        try:
            # 1. Process on-demand queue (priority)
            with db.connect() as conn:
                job = db.claim_ingest_job(conn)

            if job:
                job_id, docket_number = job
                print(f"[analyzer:{worker_id}] Queue: {docket_number}")
                try:
                    deep_analyze_docket(docket_number)
                    with db.connect() as conn:
                        db.complete_ingest_job(conn, job_id)
                    print(f"[analyzer:{worker_id}] Done: {docket_number}")
                except Exception as e:
                    err = str(e)
                    print(f"[analyzer:{worker_id}] Error: {docket_number}: {err}")
                    with db.connect() as conn:
                        db.complete_ingest_job(conn, job_id, error=err)
                    if "429" in err:
                        print("[analyzer:{worker_id}] Rate limited, pausing 5 min...")
                        time.sleep(300)
                        continue
                time.sleep(delay)
                continue

            # 2. Analyze unanalyzed cases — claim with advisory lock held through analysis
            lock_conn = psycopg2.connect(db.get_db_url())
            dn = None
            try:
                cur = lock_conn.cursor()
                cur.execute("""
                    SELECT c.docket_number FROM cases c
                    LEFT JOIN analyses a ON c.docket_number = a.docket_number AND a.doc_type = 'docket'
                    WHERE a.id IS NULL
                    ORDER BY
                        CASE WHEN EXISTS (SELECT 1 FROM events e WHERE e.docket_number = c.docket_number) THEN 0 ELSE 1 END,
                        CASE WHEN c.status ILIKE '%%active%%' THEN 0 ELSE 1 END,
                        c.created_at DESC
                    LIMIT 20
                """)
                for (candidate,) in cur.fetchall():
                    if candidate in _failed_dockets:
                        continue
                    cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (candidate,))
                    if cur.fetchone()[0]:
                        dn = candidate
                        break

                if dn:
                    if dn in _failed_dockets:
                        with db.connect() as conn:
                            db.store_analysis(conn, dn, {"error": "analysis_failed"}, "docket")
                    else:
                        print(f"[analyzer:{worker_id}] Analyze: {dn}")
                        _start = time.time()
                        try:
                            deep_analyze_docket(dn)
                            _dur = int((time.time() - _start) * 1000)
                            print(f"[analyzer:{worker_id}] Done: {dn} ({_dur}ms)")
                            db.log_event("analyzer", "analyzed", docket_number=dn, duration_ms=_dur)
                        except Exception as e:
                            err = str(e)
                            _dur = int((time.time() - _start) * 1000)
                            print(f"[analyzer:{worker_id}] Error: {dn}: {err}")
                            db.log_event("analyzer", "error", docket_number=dn, detail=err, duration_ms=_dur, success=False)
                            _failed_dockets.add(dn)
                            if "429" in err:
                                print(f"[analyzer:{worker_id}] Rate limited, pausing 5 min...")
                                time.sleep(300)
            finally:
                lock_conn.close()

            if dn:
                time.sleep(delay)
                continue

            # 3. Refresh stale dockets (one at a time)
            with db.connect() as conn:
                stale = db.get_stale_dockets(conn, active_hours=24, closed_days=7, limit=1)

            if stale:
                dn = stale[0]["docket_number"]
                print(f"[analyzer:{worker_id}] Refresh: {dn}")
                _start = time.time()
                try:
                    deep_analyze_docket(dn)
                    _dur = int((time.time() - _start) * 1000)
                    db.log_event("analyzer", "refreshed", docket_number=dn, duration_ms=_dur)
                except Exception as e:
                    err = str(e)
                    _dur = int((time.time() - _start) * 1000)
                    print(f"[analyzer:{worker_id}] Refresh error {dn}: {err}")
                    db.log_event("analyzer", "refresh_error", docket_number=dn, detail=err, duration_ms=_dur, success=False)
                    if "429" in err:
                        print("[analyzer:{worker_id}] Rate limited, pausing 5 min...")
                        time.sleep(300)
                        continue
                time.sleep(delay)
                continue

            # 3. Nothing to do — wait and check again
            time.sleep(30)

        except Exception as e:
            print(f"[analyzer:{worker_id}] Unexpected error: {e}")
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

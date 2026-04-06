#!/usr/bin/env python3
"""Ingest pipeline — scrapes UJS, stores in DB, processes queue."""

import tempfile, time, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from ujs import db
from ujs.core import search_by_date, search_by_calendar
from ujs.modules.docket_pdf import analyze_docket


def ingest_filings(county=None, docket_type=None, lookback_days=1):
    """Scrape recent filings and upsert into DB."""
    today = datetime.now()
    start = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    print(f"[filings] Scraping {start} to {end} | county={county} type={docket_type}")
    results = search_by_date(start, end, county=county, docket_type=docket_type)

    with db.connect() as conn:
        total, new = db.upsert_cases(conn, results)
        # Log the scrape
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO scrape_log (scrape_type, county, docket_type, date_range,
                                    cases_found, cases_new, completed_at)
            VALUES ('filings', %s, %s, %s, %s, %s, NOW())
        """, (county, docket_type, f"{start}/{end}", total, new))

    print(f"[filings] {total} found, {new} new")
    return total, new


def ingest_events(county=None, docket_type=None, lookahead_days=7):
    """Scrape upcoming calendar events and store."""
    today = datetime.now()
    start = today.strftime("%Y-%m-%d")
    end = (today + timedelta(days=lookahead_days)).strftime("%Y-%m-%d")

    print(f"[events] Scraping {start} to {end} | county={county} type={docket_type}")
    results = search_by_calendar(start, end, county=county, docket_type=docket_type)

    with db.connect() as conn:
        total, new = db.upsert_events(conn, results)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO scrape_log (scrape_type, county, docket_type, date_range,
                                    cases_found, cases_new, completed_at)
            VALUES ('events', %s, %s, %s, %s, %s, NOW())
        """, (county, docket_type, f"{start}/{end}", total, new))

    print(f"[events] {total} found, {new} new")
    return total, new


def deep_analyze_docket(docket_number):
    """Download PDF, run Gemini, store analysis + change detection."""
    with tempfile.TemporaryDirectory() as d:
        analysis = analyze_docket(docket_number, out_dir=d)

    with db.connect() as conn:
        changes = db.detect_and_store_changes(conn, docket_number, analysis)

    if changes:
        for c in changes:
            print(f"  CHANGE {docket_number}: {c['field']}: {c.get('old')} -> {c.get('new')}")
    return changes


def process_queue(batch_size=10, workers=3):
    """Process pending items in the ingest queue with parallel workers."""
    # Claim all jobs first
    jobs = []
    for _ in range(batch_size):
        with db.connect() as conn:
            job = db.claim_ingest_job(conn)
        if not job:
            break
        jobs.append(job)

    if not jobs:
        return 0

    print(f"[queue] Processing {len(jobs)} jobs with {workers} workers")

    def _process_job(job):
        job_id, docket_number = job
        try:
            deep_analyze_docket(docket_number)
            with db.connect() as conn:
                db.complete_ingest_job(conn, job_id)
            return True
        except Exception as e:
            print(f"[queue] Error {docket_number}: {e}")
            with db.connect() as conn:
                db.complete_ingest_job(conn, job_id, error=str(e))
            return False

    processed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_process_job, job): job for job in jobs}
        for future in as_completed(futures):
            if future.result():
                processed += 1

    return processed


def refresh_stale(active_hours=24, closed_days=7, batch_size=20, workers=3):
    """Re-analyze stale dockets to detect changes, with parallel workers."""
    with db.connect() as conn:
        stale = db.get_stale_dockets(conn, active_hours, closed_days, limit=batch_size)

    if not stale:
        print("[refresh] No stale dockets")
        return 0

    print(f"[refresh] {len(stale)} stale dockets to re-analyze ({workers} workers)")

    def _refresh_one(row):
        dn = row["docket_number"]
        try:
            changes = deep_analyze_docket(dn)
            if changes:
                print(f"[refresh] {dn}: {len(changes)} change(s)")
            return True
        except Exception as e:
            print(f"[refresh] Error {dn}: {e}")
            return False

    refreshed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_refresh_one, row): row for row in stale}
        for future in as_completed(futures):
            if future.result():
                refreshed += 1

    return refreshed


def batch_analyze_unanalyzed(limit=50, workers=3):
    """Find cases in DB that don't have a Gemini analysis yet, analyze them."""
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT c.docket_number FROM cases c
            LEFT JOIN analyses a ON c.docket_number = a.docket_number AND a.doc_type = 'docket'
            WHERE a.id IS NULL
            ORDER BY c.created_at DESC
            LIMIT %s
        """, (limit,))
        dockets = [r[0] for r in cur.fetchall()]

    if not dockets:
        print("[analyze] No unanalyzed dockets")
        return 0

    print(f"[analyze] {len(dockets)} dockets to analyze ({workers} workers)")

    def _analyze_one(dn):
        try:
            deep_analyze_docket(dn)
            return True
        except Exception as e:
            print(f"[analyze] Error {dn}: {e}")
            return False

    analyzed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_analyze_one, dn): dn for dn in dockets}
        for future in as_completed(futures):
            if future.result():
                analyzed += 1
            print(f"[analyze] {analyzed}/{len(dockets)} done", end="\r")

    print(f"\n[analyze] Completed {analyzed}/{len(dockets)}")
    return analyzed


def run_cycle(county=None, docket_type=None, lookback_days=1,
              lookahead_days=7, analyze_batch=10, refresh_batch=20, workers=3):
    """Full ingest cycle: filings → events → queue → stale refresh."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*60}")
    print(f"[{ts}] Ingest cycle starting")
    print(f"{'='*60}")

    # 1. New filings
    try:
        ingest_filings(county, docket_type, lookback_days)
    except Exception as e:
        print(f"[filings] Error: {e}")

    # 2. Upcoming events
    try:
        ingest_events(county, docket_type, lookahead_days)
    except Exception as e:
        print(f"[events] Error: {e}")

    # 3. Process ingest queue (on-demand requests)
    try:
        queued = process_queue(analyze_batch, workers=workers)
        print(f"[queue] Processed {queued} jobs")
    except Exception as e:
        print(f"[queue] Error: {e}")

    # 4. Refresh stale records
    try:
        refreshed = refresh_stale(batch_size=refresh_batch, workers=workers)
        print(f"[refresh] Refreshed {refreshed} dockets")
    except Exception as e:
        print(f"[refresh] Error: {e}")

    print(f"[{ts}] Cycle complete\n")


def run_loop(interval_minutes=60, **kwargs):
    """Run ingest cycles in a loop."""
    print(f"Ingest loop starting | interval={interval_minutes}m | {kwargs}")
    while True:
        try:
            run_cycle(**kwargs)
        except Exception as e:
            print(f"Cycle error: {e}")
            traceback.print_exc()
        time.sleep(interval_minutes * 60)


def main():
    import argparse
    p = argparse.ArgumentParser(description="UJS Ingest Pipeline")
    p.add_argument("--county", default="Lehigh")
    p.add_argument("--type", dest="docket_type", default="Criminal")
    p.add_argument("--lookback", type=int, default=1, help="Days back for filings")
    p.add_argument("--lookahead", type=int, default=7, help="Days ahead for events")
    p.add_argument("--interval", type=int, default=60, help="Minutes between cycles")
    p.add_argument("--workers", type=int, default=3, help="Parallel workers for Gemini (default: 3)")
    p.add_argument("--once", action="store_true", help="Run one cycle and exit")
    p.add_argument("--queue-only", action="store_true", help="Only process ingest queue")
    p.add_argument("--refresh-only", action="store_true", help="Only refresh stale dockets")
    p.add_argument("--analyze-new", type=int, metavar="N", help="Batch analyze N unanalyzed dockets")
    args = p.parse_args()

    kwargs = dict(county=args.county, docket_type=args.docket_type,
                  lookback_days=args.lookback, lookahead_days=args.lookahead,
                  workers=args.workers)

    if args.analyze_new:
        batch_analyze_unanalyzed(limit=args.analyze_new, workers=args.workers)
    elif args.queue_only:
        process_queue(workers=args.workers)
    elif args.refresh_only:
        refresh_stale(workers=args.workers)
    elif args.once:
        run_cycle(**kwargs)
    else:
        run_loop(interval_minutes=args.interval, **kwargs)


if __name__ == "__main__":
    main()

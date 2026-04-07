#!/usr/bin/env python3
"""Ingest pipeline — scrapes UJS, stores in DB, processes queue."""

import tempfile, time, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from ujs import db
from ujs.core import search_by_date, search_by_calendar, get_session, _post_search
from ujs.modules.docket_pdf import analyze_docket


def ingest_filings(county=None, docket_type=None, lookback_days=1):
    """Scrape recent filings and upsert into DB.
    For lookback > 7 days, chunks into weekly batches to stay under UJS result caps."""
    today = datetime.now()
    total_found = 0
    total_new = 0

    # Chunk into 7-day windows to avoid UJS 1000-result cap
    chunk_days = min(lookback_days, 7)
    chunks = max(1, lookback_days // chunk_days)

    for i in range(chunks):
        chunk_end = today - timedelta(days=i * chunk_days)
        chunk_start = chunk_end - timedelta(days=chunk_days)
        start = chunk_start.strftime("%Y-%m-%d")
        end = chunk_end.strftime("%Y-%m-%d")

        print(f"[filings] Scraping {start} to {end} | county={county} type={docket_type}")
        try:
            results = search_by_date(start, end, county=county, docket_type=docket_type)
        except Exception as e:
            print(f"[filings] Error on chunk {start}-{end}: {e}")
            if "429" in str(e):
                break
            continue

        with db.connect() as conn:
            total, new = db.upsert_cases(conn, results)
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO scrape_log (scrape_type, county, docket_type, date_range,
                                        cases_found, cases_new, completed_at)
                VALUES ('filings', %s, %s, %s, %s, %s, NOW())
            """, (county, docket_type, f"{start}/{end}", total, new))

        total_found += total
        total_new += new
        print(f"[filings] {total} found, {new} new")

        if chunks > 1:
            time.sleep(5)  # gentle delay between chunks

    return total_found, total_new


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
    from ujs.core import search_by_docket

    # Ensure case exists in DB first
    with db.connect() as conn:
        if not db.get_case(conn, docket_number):
            results = search_by_docket(docket_number)
            if results:
                db.upsert_cases(conn, results)
            else:
                raise ValueError(f"Docket not found on UJS: {docket_number}")

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


def refresh_stale(active_hours=24, closed_days=7, batch_size=10, delay=8):
    """Re-analyze stale dockets sequentially with delay to avoid rate limiting."""
    with db.connect() as conn:
        stale = db.get_stale_dockets(conn, active_hours, closed_days, limit=batch_size)

    if not stale:
        print("[refresh] No stale dockets")
        return 0

    print(f"[refresh] {len(stale)} stale dockets to re-analyze ({delay}s delay)")
    refreshed = 0
    for row in stale:
        dn = row["docket_number"]
        try:
            changes = deep_analyze_docket(dn)
            refreshed += 1
            if changes:
                print(f"[refresh] {dn}: {len(changes)} change(s)")
        except Exception as e:
            if "429" in str(e):
                print(f"[refresh] Rate limited, stopping refresh")
                break
            print(f"[refresh] Error {dn}: {e}")
        time.sleep(delay)

    return refreshed


def ingest_appellate(lookback_days=1):
    """Scrape recent appellate filings from all three PA appellate courts."""
    today = datetime.now()
    start = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    total_new = 0

    for court in ["Supreme", "Superior", "Commonwealth"]:
        try:
            session, token = get_session()
            results = _post_search(session, token, SearchBy="AppellateCourtName",
                                   FiledStartDate=start, FiledEndDate=end,
                                   AppellateCourtName=court)
            with db.connect() as conn:
                total, new = db.upsert_cases(conn, results)
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO scrape_log (scrape_type, county, docket_type, date_range,
                                            cases_found, cases_new, completed_at)
                    VALUES ('appellate', %s, NULL, %s, %s, %s, NOW())
                """, (court, f"{start}/{end}", total, new))
            print(f"[appellate] {court}: {total} found, {new} new")
            total_new += new
        except Exception as e:
            print(f"[appellate] {court} error: {e}")

    return total_new


def batch_analyze_unanalyzed(limit=50, workers=1, delay=8):
    """Find cases in DB that don't have a Gemini analysis yet, analyze them.
    delay: seconds between each request to avoid UJS rate limiting."""
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

    print(f"[analyze] {len(dockets)} dockets to analyze ({workers} workers, {delay}s delay)")

    analyzed = 0
    errors = 0
    for i, dn in enumerate(dockets):
        try:
            deep_analyze_docket(dn)
            analyzed += 1
        except Exception as e:
            errors += 1
            err_str = str(e)
            if "429" in err_str:
                print(f"\n[analyze] Rate limited at {i}/{len(dockets)}, pausing 60s...")
                time.sleep(60)
            else:
                print(f"[analyze] Error {dn}: {e}")
        print(f"[analyze] {analyzed}/{len(dockets)} done ({errors} errors)", end="\r")
        time.sleep(delay)

    print(f"\n[analyze] Completed {analyzed}/{len(dockets)} ({errors} errors)")
    return analyzed


def run_cycle(counties=None, docket_type=None, lookback_days=1,
              lookahead_days=7, analyze_batch=20, refresh_batch=20,
              auto_analyze=False, workers=3):
    """Full ingest cycle: filings → events → auto-analyze → queue → stale refresh."""
    counties = counties or ["Lehigh"]
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*60}")
    print(f"[{ts}] Ingest cycle | counties={counties} type={docket_type}")
    print(f"{'='*60}")

    total_new = 0

    # 1. New filings per county
    for county in counties:
        try:
            _, new = ingest_filings(county, docket_type, lookback_days)
            total_new += new
        except Exception as e:
            print(f"[filings] Error ({county}): {e}")

    # 2. Upcoming events per county
    for county in counties:
        try:
            ingest_events(county, docket_type, lookahead_days)
        except Exception as e:
            print(f"[events] Error ({county}): {e}")

    # 3. Appellate courts (statewide)
    try:
        app_new = ingest_appellate(lookback_days)
        total_new += app_new
    except Exception as e:
        print(f"[appellate] Error: {e}")

    # 4. Auto-analyze new unanalyzed dockets
    if auto_analyze and total_new > 0:
        try:
            analyzed = batch_analyze_unanalyzed(limit=analyze_batch, workers=workers)
            print(f"[auto-analyze] Analyzed {analyzed} new dockets")
        except Exception as e:
            print(f"[auto-analyze] Error: {e}")

    # 4. Process ingest queue (on-demand requests)
    try:
        queued = process_queue(analyze_batch, workers=workers)
        print(f"[queue] Processed {queued} jobs")
    except Exception as e:
        print(f"[queue] Error: {e}")

    # 5. Refresh stale records
    try:
        refreshed = refresh_stale(batch_size=refresh_batch)
        print(f"[refresh] Refreshed {refreshed} dockets")
    except Exception as e:
        print(f"[refresh] Error: {e}")

    # 6. Retry failed jobs (up to 3 attempts)
    try:
        with db.connect() as conn:
            retried = db.retry_failed_jobs(conn)
        if retried:
            print(f"[retry] Re-queued {retried} failed jobs")
    except Exception as e:
        print(f"[retry] Error: {e}")

    # 7. Cleanup old data
    try:
        with db.connect() as conn:
            q_del, c_del = db.cleanup_old_data(conn)
        if q_del or c_del:
            print(f"[cleanup] Deleted {q_del} old queue entries, {c_del} old change logs")
    except Exception as e:
        print(f"[cleanup] Error: {e}")

    print(f"[{ts}] Cycle complete | {total_new} new cases\n")


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
    p.add_argument("--counties", default="Lehigh,Northampton",
                   help="Comma-separated county names (default: Lehigh,Northampton)")
    p.add_argument("--type", dest="docket_type", default=None,
                   help="Docket type filter: Criminal, Civil, Traffic, etc. (default: all)")
    p.add_argument("--lookback", type=int, default=1, help="Days back for filings")
    p.add_argument("--lookahead", type=int, default=7, help="Days ahead for events")
    p.add_argument("--interval", type=int, default=60, help="Minutes between cycles")
    p.add_argument("--workers", type=int, default=3, help="Parallel workers for Gemini (default: 3)")
    p.add_argument("--auto-analyze", action="store_true",
                   help="Auto-analyze new dockets with Gemini after ingest")
    p.add_argument("--once", action="store_true", help="Run one cycle and exit")
    p.add_argument("--queue-only", action="store_true", help="Only process ingest queue")
    p.add_argument("--refresh-only", action="store_true", help="Only refresh stale dockets")
    p.add_argument("--analyze-new", type=int, metavar="N", help="Batch analyze N unanalyzed dockets")
    args = p.parse_args()

    counties = [c.strip() for c in args.counties.split(",")]

    if args.analyze_new:
        batch_analyze_unanalyzed(limit=args.analyze_new, workers=args.workers)
    elif args.queue_only:
        process_queue(workers=args.workers)
    elif args.refresh_only:
        refresh_stale()
    elif args.once:
        run_cycle(counties=counties, docket_type=args.docket_type,
                  lookback_days=args.lookback, lookahead_days=args.lookahead,
                  auto_analyze=args.auto_analyze, workers=args.workers)
    else:
        run_loop(interval_minutes=args.interval, counties=counties,
                 docket_type=args.docket_type, lookback_days=args.lookback,
                 lookahead_days=args.lookahead, auto_analyze=args.auto_analyze,
                 workers=args.workers)


if __name__ == "__main__":
    main()

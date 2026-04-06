"""REST API for PA UJS court data — DB-first with async ingest."""

import threading, time, traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from typing import Optional
import tempfile, os

from ujs import db
from ujs.modules.docket_pdf import fetch_docket_pdf, extract_text, analyze_summary


# -------------------------------------------------------------------
# Background queue worker — processes ingest jobs automatically
# -------------------------------------------------------------------

_worker_running = False


def _queue_worker():
    """Background thread that polls the ingest queue every 10s."""
    global _worker_running
    _worker_running = True
    while _worker_running:
        try:
            with db.connect() as conn:
                job = db.claim_ingest_job(conn)
            if job:
                job_id, docket_number = job
                print(f"[worker] Processing {docket_number}")
                try:
                    from ujs.modules.ingest import deep_analyze_docket
                    deep_analyze_docket(docket_number)
                    with db.connect() as conn:
                        db.complete_ingest_job(conn, job_id)
                    print(f"[worker] Done {docket_number}")
                except Exception as e:
                    print(f"[worker] Error {docket_number}: {e}")
                    with db.connect() as conn:
                        db.complete_ingest_job(conn, job_id, error=str(e))
            else:
                time.sleep(10)
        except Exception as e:
            print(f"[worker] Connection error: {e}")
            time.sleep(30)


@asynccontextmanager
async def lifespan(app):
    # Start background worker on API startup
    t = threading.Thread(target=_queue_worker, daemon=True)
    t.start()
    print("[worker] Queue worker started")
    yield
    _worker_running = False


app = FastAPI(
    title="PA UJS Court Search API",
    description="Programmatic access to Pennsylvania Unified Judicial System court records. "
                "Data served from database — updated hourly via ingest pipeline.",
    version="2.0.0",
    lifespan=lifespan,
)


# -------------------------------------------------------------------
# Search endpoints (DB-first)
# -------------------------------------------------------------------

@app.get("/search/cases")
def api_search_cases(
    name: Optional[str] = None,
    county: Optional[str] = None,
    status: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
    filed_after: Optional[str] = Query(None, description="MM/DD/YYYY"),
    filed_before: Optional[str] = Query(None, description="MM/DD/YYYY"),
    limit: int = Query(100, le=500),
):
    """Search indexed cases from database."""
    with db.connect() as conn:
        results = db.search_cases(
            conn, county=county, status=status, docket_type=docket_type,
            filed_after=filed_after, filed_before=filed_before,
            name=name, limit=limit,
        )
        return [dict(r) for r in results]


@app.get("/search/events")
def api_search_events(
    county: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
    days: int = Query(7, description="Days ahead"),
):
    """Get upcoming calendar events from database."""
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        clauses = ["e.event_date >= %s"]
        params = [datetime.now().strftime("%m/%d/%Y")]
        if county:
            clauses.append("c.county ILIKE %s")
            params.append(county)
        if docket_type:
            dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}
            code = dtype_map.get(docket_type.lower(), "")
            if code:
                clauses.append("c.docket_number LIKE %s")
                params.append(f"%{code}%")
        where = " AND ".join(clauses)
        cur.execute(f"""
            SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date
            FROM events e JOIN cases c ON e.docket_number = c.docket_number
            WHERE {where}
            ORDER BY e.event_date ASC LIMIT 200
        """, params)
        return [dict(r) for r in cur.fetchall()]


# -------------------------------------------------------------------
# Docket endpoints (DB-first, auto-queue on miss)
# -------------------------------------------------------------------

@app.get("/docket/{docket_number}")
def api_docket_info(docket_number: str):
    """Get case info. Returns 202 + queues ingest if not yet indexed."""
    with db.connect() as conn:
        case = db.get_case(conn, docket_number)
        if case:
            return dict(case)

        # Not in DB — auto-queue
        queue_id, status = db.queue_ingest(conn, docket_number, priority=5)
        return JSONResponse(status_code=202, content={
            "status": "queuing",
            "docket_number": docket_number,
            "queue_id": queue_id,
            "message": "Not yet indexed. Queued for scraping — retry in ~15s.",
        })


@app.get("/docket/{docket_number}/analyze")
def api_docket_analyze(docket_number: str):
    """Get Gemini-parsed analysis. Returns 202 if not yet analyzed."""
    with db.connect() as conn:
        analysis = db.get_analysis(conn, docket_number, "docket")
        if analysis:
            return analysis

        # Check if case exists but not yet analyzed
        case = db.get_case(conn, docket_number)
        if case:
            queue_id, status = db.queue_ingest(conn, docket_number, priority=5)
            return JSONResponse(status_code=202, content={
                "status": "queuing",
                "docket_number": docket_number,
                "message": "Case indexed but not yet analyzed. Queued — retry in ~15s.",
            })

        # Not in DB at all
        queue_id, status = db.queue_ingest(conn, docket_number, priority=5)
        return JSONResponse(status_code=202, content={
            "status": "queuing",
            "docket_number": docket_number,
            "message": "Not yet indexed. Queued for scraping — retry in ~15s.",
        })


@app.get("/docket/{docket_number}/summary")
def api_docket_summary(docket_number: str):
    """Get court summary (cross-case person history). Returns 202 if not cached."""
    with db.connect() as conn:
        analysis = db.get_analysis(conn, docket_number, "summary")
        if analysis:
            return analysis

    # Not cached — scrape live for summaries (they're per-person, not per-docket)
    with tempfile.TemporaryDirectory() as tmpdir:
        result = analyze_summary(docket_number, out_dir=tmpdir)
        clean = {k: v for k, v in result.items() if k != "pdf_path"}

    with db.connect() as conn:
        db.store_analysis(conn, docket_number, clean, "summary")

    return clean


@app.get("/docket/{docket_number}/text")
def api_docket_text(docket_number: str):
    """Download docket PDF and return raw extracted text."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pdf_path = fetch_docket_pdf(docket_number, out_dir=tmpdir)
        text = extract_text(pdf_path)
        return {"docket_number": docket_number, "text": text}


@app.get("/docket/{docket_number}/pdf")
def api_docket_pdf(docket_number: str, doc: str = Query("docket", description="'docket' or 'summary'")):
    """Download and serve a PDF directly."""
    tmpdir = tempfile.mkdtemp()
    pdf_path = fetch_docket_pdf(docket_number, out_dir=tmpdir, doc_type=doc)
    return FileResponse(pdf_path, media_type="application/pdf",
                        filename=os.path.basename(pdf_path))


@app.get("/docket/{docket_number}/changes")
def api_docket_changes(docket_number: str):
    """Get change history for a docket."""
    with db.connect() as conn:
        changes = db.get_changes(conn, docket_number=docket_number)
        return [dict(c) for c in changes]


# -------------------------------------------------------------------
# Ingest / queue
# -------------------------------------------------------------------

@app.get("/ingest/{docket_number}/status")
def api_ingest_status(docket_number: str):
    """Check ingest status for a docket."""
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        cur.execute("""
            SELECT id, status, requested_at, started_at, completed_at, error
            FROM ingest_queue WHERE docket_number = %s
            ORDER BY requested_at DESC LIMIT 1
        """, (docket_number,))
        job = cur.fetchone()
        if not job:
            case = db.get_case(conn, docket_number)
            if case:
                return {"status": "indexed", "last_scraped": case["last_scraped"].isoformat() if case["last_scraped"] else None}
            return {"status": "unknown", "docket_number": docket_number}
        return dict(job)


# -------------------------------------------------------------------
# Changes feed
# -------------------------------------------------------------------

@app.get("/changes")
def api_changes(
    since: Optional[str] = Query(None, description="ISO datetime"),
    limit: int = Query(50, le=200),
):
    """Get recent changes across all dockets."""
    since_dt = datetime.fromisoformat(since) if since else None
    with db.connect() as conn:
        changes = db.get_changes(conn, since=since_dt, limit=limit)
        return [dict(c) for c in changes]


# -------------------------------------------------------------------
# Stats / health
# -------------------------------------------------------------------

@app.get("/stats")
def api_stats():
    """Database statistics and health."""
    with db.connect() as conn:
        return db.get_stats(conn)


@app.get("/health")
def health():
    try:
        with db.connect() as conn:
            stats = db.get_stats(conn)
        return {"status": "ok", "db": "connected", "cases_indexed": stats["cases"]}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "detail": str(e)})

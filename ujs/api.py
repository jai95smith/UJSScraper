"""REST API for PA UJS court data — DB-first with async ingest."""

import threading, time
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Query, Header, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List
import tempfile, os

from ujs import db
from ujs.modules.docket_pdf import fetch_docket_pdf, extract_text, analyze_summary


# -------------------------------------------------------------------
# Background queue worker
# -------------------------------------------------------------------

_worker_running = False


def _queue_worker():
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
    t = threading.Thread(target=_queue_worker, daemon=True)
    t.start()
    print("[worker] Queue worker started")
    yield
    global _worker_running
    _worker_running = False


app = FastAPI(
    title="PA UJS Court Search API",
    description="Programmatic access to Pennsylvania Unified Judicial System court records. "
                "Data served from database — updated hourly via ingest pipeline.",
    version="2.0.0",
    lifespan=lifespan,
)


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    import traceback
    traceback.print_exc()
    return JSONResponse(status_code=500, content= {"error": str(exc)})


# -------------------------------------------------------------------
# Auth helper
# -------------------------------------------------------------------

def _get_key(x_api_key: Optional[str] = None):
    """Validate API key if provided; allow unauthenticated for now."""
    if not x_api_key:
        return "public"
    with db.connect() as conn:
        valid = db.validate_api_key(conn, x_api_key)
        if not valid:
            raise HTTPException(401, "Invalid or rate-limited API key")
    return x_api_key


# -------------------------------------------------------------------
# Case search
# -------------------------------------------------------------------

# -------------------------------------------------------------------
# Quick access — filings & hearings
# -------------------------------------------------------------------

@app.get("/filings/today", tags=["Quick Access"])
def filings_today(
    county: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
    limit: int = Query(200, le=500),
):
    """Cases filed today."""
    today = datetime.now().strftime("%m/%d/%Y")
    with db.connect() as conn:
        return [dict(r) for r in db.search_cases(
            conn, county=county, docket_type=docket_type,
            filed_after=today, filed_before=today, limit=limit)]


@app.get("/filings/recent", tags=["Quick Access"])
def filings_recent(
    days: int = Query(7, description="Days back"),
    county: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
    limit: int = Query(200, le=500),
):
    """Cases filed in the last N days."""
    from datetime import timedelta
    start = (datetime.now() - timedelta(days=days)).strftime("%m/%d/%Y")
    today = datetime.now().strftime("%m/%d/%Y")
    with db.connect() as conn:
        return [dict(r) for r in db.search_cases(
            conn, county=county, docket_type=docket_type,
            filed_after=start, filed_before=today, limit=limit)]


@app.get("/hearings/today", tags=["Quick Access"])
def hearings_today(
    county: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
):
    """Court hearings/events scheduled for today."""
    today = datetime.now().strftime("%m/%d/%Y")
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        clauses = ["e.event_date LIKE %s"]
        params = [f"{today}%"]
        if county:
            clauses.append("c.county ILIKE %s")
            params.append(county)
        if docket_type:
            dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}
            code = dtype_map.get(docket_type.lower(), "")
            if code:
                clauses.append("c.docket_number LIKE %s")
                params.append(f"%{code}%")
        cur.execute(f"""
            SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date
            FROM events e JOIN cases c ON e.docket_number = c.docket_number
            WHERE {' AND '.join(clauses)}
            ORDER BY e.event_date ASC
        """, params)
        return [dict(r) for r in cur.fetchall()]


@app.get("/hearings/upcoming", tags=["Quick Access"])
def hearings_upcoming(
    days: int = Query(7, description="Days ahead"),
    county: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
    event_type: Optional[str] = Query(None, description="e.g. 'Preliminary Hearing', 'Trial'"),
    limit: int = Query(200, le=500),
):
    """Court hearings/events in the next N days."""
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        clauses = []
        params = []
        if county:
            clauses.append("c.county ILIKE %s")
            params.append(county)
        if docket_type:
            dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}
            code = dtype_map.get(docket_type.lower(), "")
            if code:
                clauses.append("c.docket_number LIKE %s")
                params.append(f"%{code}%")
        if event_type:
            clauses.append("e.event_type ILIKE %s")
            params.append(f"%{event_type}%")
        where = " AND ".join(clauses) if clauses else "TRUE"
        params.append(limit)
        cur.execute(f"""
            SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date
            FROM events e JOIN cases c ON e.docket_number = c.docket_number
            WHERE {where}
            ORDER BY e.event_date ASC LIMIT %s
        """, params)
        return [dict(r) for r in cur.fetchall()]


# -------------------------------------------------------------------
# Search endpoints
# -------------------------------------------------------------------

@app.get("/search/cases", tags=["Search"])
def search_cases(
    name: Optional[str] = None,
    county: Optional[str] = None,
    status: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
    filed_after: Optional[str] = Query(None, description="MM/DD/YYYY"),
    filed_before: Optional[str] = Query(None, description="MM/DD/YYYY"),
    limit: int = Query(100, le=500),
):
    """Search indexed cases."""
    with db.connect() as conn:
        results = db.search_cases(conn, county=county, status=status,
                                  docket_type=docket_type, filed_after=filed_after,
                                  filed_before=filed_before, name=name, limit=limit)
        return [dict(r) for r in results]


@app.get("/search/judge", tags=["Search"])
def search_judge(
    name: str,
    county: Optional[str] = None,
    limit: int = Query(100, le=500),
):
    """Find cases by judge name."""
    with db.connect() as conn:
        return [dict(r) for r in db.search_by_judge(conn, name, county=county, limit=limit)]


@app.get("/search/attorney", tags=["Search"])
def search_attorney(
    name: str,
    role: Optional[str] = Query(None, description="e.g. 'Public Defender', 'District Attorney'"),
    county: Optional[str] = None,
    limit: int = Query(100, le=500),
):
    """Find cases by attorney name."""
    with db.connect() as conn:
        return [dict(r) for r in db.search_by_attorney(conn, name, role=role,
                                                        county=county, limit=limit)]


@app.get("/search/charges", tags=["Search"])
def search_charges(
    statute: Optional[str] = Query(None, description="e.g. 3929"),
    description: Optional[str] = Query(None, description="e.g. 'Retail Theft'"),
    disposition: Optional[str] = Query(None, description="e.g. 'Guilty'"),
    county: Optional[str] = None,
    limit: int = Query(100, le=500),
):
    """Find cases by charge statute, description, or disposition."""
    with db.connect() as conn:
        return [dict(r) for r in db.search_by_charge(conn, statute=statute,
                                                      description=description,
                                                      disposition=disposition,
                                                      county=county, limit=limit)]


@app.get("/search/events", tags=["Search"])
def search_events(
    county: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
    days: int = Query(7, description="Days ahead"),
):
    """Get upcoming calendar events."""
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        clauses = []
        params = []
        if county:
            clauses.append("c.county ILIKE %s")
            params.append(county)
        if docket_type:
            dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}
            code = dtype_map.get(docket_type.lower(), "")
            if code:
                clauses.append("c.docket_number LIKE %s")
                params.append(f"%{code}%")
        where = " AND ".join(clauses) if clauses else "TRUE"
        cur.execute(f"""
            SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date
            FROM events e JOIN cases c ON e.docket_number = c.docket_number
            WHERE {where}
            ORDER BY e.event_date ASC LIMIT 200
        """, params)
        return [dict(r) for r in cur.fetchall()]


# -------------------------------------------------------------------
# Docket endpoints
# -------------------------------------------------------------------

@app.get("/docket/{docket_number}", tags=["Docket"])
def docket_info(docket_number: str):
    """Get case info. Auto-queues ingest if not yet indexed."""
    try:
        with db.connect() as conn:
            case = db.get_case(conn, docket_number)
            if case:
                return dict(case)
            queue_id, status = db.queue_ingest(conn, docket_number, priority=5)
            return JSONResponse(status_code=202, content= {"status": "queuing", "docket_number": docket_number,
                                      "message": "Not yet indexed. Queued — retry in ~15s."})
    except Exception as e:
        return JSONResponse(status_code=500, content= {"error": str(e), "docket_number": docket_number})


@app.get("/docket/{docket_number}/analyze", tags=["Docket"])
def docket_analyze(docket_number: str):
    """Get Gemini-parsed analysis. Auto-queues if missing."""
    with db.connect() as conn:
        analysis = db.get_analysis(conn, docket_number, "docket")
        if analysis:
            return analysis
        db.queue_ingest(conn, docket_number, priority=5)
        case = db.get_case(conn, docket_number)
        msg = "Case indexed but not yet analyzed." if case else "Not yet indexed."
        return JSONResponse(status_code=202, content= {"status": "queuing", "docket_number": docket_number,
                                  "message": f"{msg} Queued — retry in ~15s."})


@app.get("/docket/{docket_number}/summary", tags=["Docket"])
def docket_summary(docket_number: str):
    """Get court summary (cross-case person history). Cached by defendant name to avoid duplicates."""
    with db.connect() as conn:
        # Check if this docket already has a summary
        analysis = db.get_analysis(conn, docket_number, "summary")
        if analysis:
            return analysis

        # Check if another docket for the same person already has a summary
        cur = conn.cursor()
        cur.execute("""
            SELECT a.analysis FROM participants p1
            JOIN participants p2 ON p1.name = p2.name AND p1.role = p2.role
            JOIN analyses a ON a.docket_number = p2.docket_number AND a.doc_type = 'summary'
            WHERE p1.docket_number = %s
            LIMIT 1
        """, (docket_number,))
        row = cur.fetchone()
        if row:
            # Cache it under this docket too
            db.store_analysis(conn, docket_number, row[0], "summary")
            return row[0]

    # Not cached anywhere — fetch fresh
    with tempfile.TemporaryDirectory() as tmpdir:
        result = analyze_summary(docket_number, out_dir=tmpdir)
        clean = {k: v for k, v in result.items() if k != "pdf_path"}
    with db.connect() as conn:
        db.store_analysis(conn, docket_number, clean, "summary")
    return clean


@app.get("/docket/{docket_number}/charges", tags=["Docket"])
def docket_charges(docket_number: str):
    """Get charges for a specific docket."""
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        cur.execute("SELECT * FROM charges WHERE docket_number = %s ORDER BY seq", (docket_number,))
        return [dict(r) for r in cur.fetchall()]


@app.get("/docket/{docket_number}/sentences", tags=["Docket"])
def docket_sentences(docket_number: str):
    """Get sentences for a specific docket."""
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        cur.execute("SELECT * FROM sentences WHERE docket_number = %s", (docket_number,))
        return [dict(r) for r in cur.fetchall()]


@app.get("/docket/{docket_number}/attorneys", tags=["Docket"])
def docket_attorneys(docket_number: str):
    """Get attorneys for a specific docket."""
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        cur.execute("SELECT * FROM attorneys WHERE docket_number = %s", (docket_number,))
        return [dict(r) for r in cur.fetchall()]


@app.get("/docket/{docket_number}/bail", tags=["Docket"])
def docket_bail(docket_number: str):
    """Get bail info for a specific docket."""
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        cur.execute("SELECT * FROM bail WHERE docket_number = %s", (docket_number,))
        row = cur.fetchone()
        return dict(row) if row else {}


@app.get("/docket/{docket_number}/entries", tags=["Docket"])
def docket_entries(docket_number: str):
    """Get docket entry timeline."""
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=__import__("psycopg2").extras.RealDictCursor)
        cur.execute("SELECT * FROM docket_entries WHERE docket_number = %s ORDER BY entry_date",
                    (docket_number,))
        return [dict(r) for r in cur.fetchall()]


@app.get("/docket/{docket_number}/changes", tags=["Docket"])
def docket_changes(docket_number: str):
    """Get change history for a docket."""
    with db.connect() as conn:
        return [dict(c) for c in db.get_changes(conn, docket_number=docket_number)]


@app.get("/docket/{docket_number}/text", tags=["Docket"])
def docket_text(docket_number: str):
    """Download docket PDF and return raw extracted text."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pdf_path = fetch_docket_pdf(docket_number, out_dir=tmpdir)
        text = extract_text(pdf_path)
        return {"docket_number": docket_number, "text": text}


@app.get("/docket/{docket_number}/pdf", tags=["Docket"])
def docket_pdf(docket_number: str, doc: str = Query("docket", description="'docket' or 'summary'")):
    """Download and serve a PDF directly."""
    tmpdir = tempfile.mkdtemp()
    pdf_path = fetch_docket_pdf(docket_number, out_dir=tmpdir, doc_type=doc)
    return FileResponse(pdf_path, media_type="application/pdf",
                        filename=os.path.basename(pdf_path))


# -------------------------------------------------------------------
# Watchlist
# -------------------------------------------------------------------

class WatchlistAdd(BaseModel):
    docket_number: str
    label: Optional[str] = None


@app.get("/watchlist", tags=["Watchlist"])
def get_watchlist(x_api_key: str = Header(...)):
    """Get your watched cases with current status."""
    key = _get_key(x_api_key)
    with db.connect() as conn:
        return [dict(r) for r in db.get_watchlist(conn, key)]


@app.post("/watchlist", tags=["Watchlist"])
def add_watchlist(body: WatchlistAdd, x_api_key: str = Header(...)):
    """Add a docket to your watchlist. Auto-queues for ingest if not indexed."""
    key = _get_key(x_api_key)
    with db.connect() as conn:
        wid = db.add_to_watchlist(conn, key, body.docket_number, body.label)
        return {"id": wid, "docket_number": body.docket_number, "status": "watching"}


@app.delete("/watchlist/{docket_number}", tags=["Watchlist"])
def remove_watchlist(docket_number: str, x_api_key: str = Header(...)):
    """Remove a docket from your watchlist."""
    key = _get_key(x_api_key)
    with db.connect() as conn:
        removed = db.remove_from_watchlist(conn, key, docket_number)
        if not removed:
            raise HTTPException(404, "Not in watchlist")
        return {"status": "removed"}


@app.get("/watchlist/changes", tags=["Watchlist"])
def watchlist_changes(
    since: Optional[str] = Query(None, description="ISO datetime"),
    x_api_key: str = Header(...),
):
    """Get recent changes across all your watched dockets."""
    key = _get_key(x_api_key)
    since_dt = datetime.fromisoformat(since) if since else None
    with db.connect() as conn:
        return [dict(c) for c in db.get_watchlist_changes(conn, key, since=since_dt)]


# -------------------------------------------------------------------
# Webhooks
# -------------------------------------------------------------------

class WebhookCreate(BaseModel):
    url: str
    events: Optional[List[str]] = None
    county: Optional[str] = None
    docket_type: Optional[str] = None


@app.get("/webhooks", tags=["Webhooks"])
def list_webhooks(x_api_key: str = Header(...)):
    """List your webhooks."""
    key = _get_key(x_api_key)
    with db.connect() as conn:
        return [dict(w) for w in db.get_webhooks(conn, key)]


@app.post("/webhooks", tags=["Webhooks"])
def create_webhook(body: WebhookCreate, x_api_key: str = Header(...)):
    """Register a webhook for change/filing/event notifications."""
    key = _get_key(x_api_key)
    with db.connect() as conn:
        wid = db.create_webhook(conn, key, body.url, events=body.events,
                                county=body.county, docket_type=body.docket_type)
        return {"id": wid, "url": body.url, "status": "active"}


@app.delete("/webhooks/{webhook_id}", tags=["Webhooks"])
def delete_webhook(webhook_id: int, x_api_key: str = Header(...)):
    """Delete a webhook."""
    key = _get_key(x_api_key)
    with db.connect() as conn:
        removed = db.delete_webhook(conn, key, webhook_id)
        if not removed:
            raise HTTPException(404, "Webhook not found")
        return {"status": "deleted"}


# -------------------------------------------------------------------
# Changes feed
# -------------------------------------------------------------------

@app.get("/changes", tags=["Changes"])
def changes_feed(
    since: Optional[str] = Query(None, description="ISO datetime"),
    docket_number: Optional[str] = None,
    limit: int = Query(50, le=200),
):
    """Get recent changes across all dockets."""
    since_dt = datetime.fromisoformat(since) if since else None
    with db.connect() as conn:
        return [dict(c) for c in db.get_changes(conn, docket_number=docket_number,
                                                 since=since_dt, limit=limit)]


# -------------------------------------------------------------------
# Analytics / Stats
# -------------------------------------------------------------------

@app.get("/stats", tags=["Analytics"])
def stats():
    """Database statistics and health."""
    with db.connect() as conn:
        return db.get_stats(conn)


@app.get("/stats/filings", tags=["Analytics"])
def filing_stats(
    county: Optional[str] = None,
    days: int = Query(30, description="Number of days to show"),
):
    """Filing counts by date, broken down by case type."""
    with db.connect() as conn:
        return [dict(r) for r in db.get_filing_stats(conn, county=county, days=days)]


@app.get("/stats/counties", tags=["Analytics"])
def county_stats():
    """Case counts by county."""
    with db.connect() as conn:
        return [dict(r) for r in db.get_county_stats(conn)]


@app.get("/stats/charges", tags=["Analytics"])
def charge_stats(
    county: Optional[str] = None,
    limit: int = Query(25, le=100),
):
    """Most common charges with disposition breakdown."""
    with db.connect() as conn:
        return [dict(r) for r in db.get_charge_stats(conn, county=county, limit=limit)]


@app.get("/stats/judges", tags=["Analytics"])
def judge_stats(
    county: Optional[str] = None,
    limit: int = Query(25, le=100),
):
    """Case counts by judge."""
    with db.connect() as conn:
        return [dict(r) for r in db.get_judge_stats(conn, county=county, limit=limit)]


# -------------------------------------------------------------------
# Ingest
# -------------------------------------------------------------------

@app.get("/ingest/{docket_number}/status", tags=["Ingest"])
def ingest_status(docket_number: str):
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
                return {"status": "indexed",
                        "last_scraped": case["last_scraped"].isoformat() if case["last_scraped"] else None}
            return {"status": "unknown", "docket_number": docket_number}
        return dict(job)


# -------------------------------------------------------------------
# API key management
# -------------------------------------------------------------------

@app.post("/keys", tags=["API Keys"])
def create_key(name: str, email: Optional[str] = None):
    """Generate a new API key."""
    with db.connect() as conn:
        key = db.create_api_key(conn, name, email)
        return {"key": key, "name": name, "daily_limit": 1000}


# -------------------------------------------------------------------
# Natural language chat
# -------------------------------------------------------------------

class AskRequest(BaseModel):
    question: str

@app.post("/ask", tags=["Chat"])
def ask_question(body: AskRequest):
    """Ask a natural language question about court records."""
    from ujs.chat import ask
    try:
        answer = ask(body.question)
        return {"question": body.question, "answer": answer}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/ask", tags=["Chat"])
def ask_question_get(q: str = Query(..., description="Your question")):
    """Ask a question via GET (browser-friendly)."""
    from ujs.chat import ask
    try:
        answer = ask(q)
        return {"question": q, "answer": answer}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/ask/stream", tags=["Chat"])
async def ask_stream(q: str = Query(..., description="Your question")):
    """Ask a question with streaming response — text appears as it's generated."""
    from starlette.responses import StreamingResponse
    from ujs.chat import ask_stream as _ask_stream

    return StreamingResponse(_ask_stream(q), media_type="text/plain")


# -------------------------------------------------------------------
# Health
# -------------------------------------------------------------------

@app.get("/health", tags=["Health"])
def health():
    try:
        with db.connect() as conn:
            s = db.get_stats(conn)
        return {"status": "ok", "db": "connected", "cases_indexed": s["cases"]}
    except Exception as e:
        return JSONResponse(status_code=503, content= {"status": "error", "detail": str(e)})

"""Docket endpoints — per-case data."""

import tempfile, os
from fastapi import APIRouter, Query
from fastapi.responses import FileResponse, JSONResponse
from typing import Optional

from ujs import db
from ujs.modules.docket_pdf import fetch_docket_pdf, extract_text, analyze_summary

router = APIRouter(prefix="/docket", tags=["Docket"])


@router.get("/{docket_number}")
def info(docket_number: str):
    try:
        with db.connect() as conn:
            case = db.get_case(conn, docket_number)
            if case:
                return dict(case)
            queue_id, status = db.queue_ingest(conn, docket_number, priority=5)
            return JSONResponse(status_code=202, content={"status": "queuing", "docket_number": docket_number,
                                                          "message": "Not yet indexed. Queued — retry in ~15s."})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "docket_number": docket_number})


@router.get("/{docket_number}/analyze")
def analyze(docket_number: str):
    with db.connect() as conn:
        analysis = db.get_analysis(conn, docket_number, "docket")
        if analysis:
            return analysis
        db.queue_ingest(conn, docket_number, priority=5)
        case = db.get_case(conn, docket_number)
        msg = "Case indexed but not yet analyzed." if case else "Not yet indexed."
        return JSONResponse(status_code=202, content={"status": "queuing", "docket_number": docket_number,
                                                      "message": f"{msg} Queued — retry in ~15s."})


@router.get("/{docket_number}/summary")
def summary(docket_number: str):
    import psycopg2.extras
    with db.connect() as conn:
        analysis = db.get_analysis(conn, docket_number, "summary")
        if analysis:
            return analysis
        cur = conn.cursor()
        cur.execute("""
            SELECT a.analysis FROM participants p1
            JOIN participants p2 ON p1.name = p2.name AND p1.role = p2.role
            JOIN analyses a ON a.docket_number = p2.docket_number AND a.doc_type = 'summary'
            WHERE p1.docket_number = %s LIMIT 1
        """, (docket_number,))
        row = cur.fetchone()
        if row:
            db.store_analysis(conn, docket_number, row[0], "summary")
            return row[0]
    with tempfile.TemporaryDirectory() as tmpdir:
        result = analyze_summary(docket_number, out_dir=tmpdir)
        clean = {k: v for k, v in result.items() if k != "pdf_path"}
    with db.connect() as conn:
        db.store_analysis(conn, docket_number, clean, "summary")
    return clean


@router.get("/{docket_number}/charges")
def charges(docket_number: str):
    import psycopg2.extras
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM charges WHERE docket_number = %s ORDER BY seq", (docket_number,))
        return [dict(r) for r in cur.fetchall()]


@router.get("/{docket_number}/sentences")
def sentences(docket_number: str):
    import psycopg2.extras
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM sentences WHERE docket_number = %s", (docket_number,))
        return [dict(r) for r in cur.fetchall()]


@router.get("/{docket_number}/attorneys")
def attorneys(docket_number: str):
    import psycopg2.extras
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM attorneys WHERE docket_number = %s", (docket_number,))
        return [dict(r) for r in cur.fetchall()]


@router.get("/{docket_number}/bail")
def bail(docket_number: str):
    import psycopg2.extras
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM bail WHERE docket_number = %s", (docket_number,))
        row = cur.fetchone()
        return dict(row) if row else {}


@router.get("/{docket_number}/entries")
def entries(docket_number: str):
    import psycopg2.extras
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT * FROM docket_entries WHERE docket_number = %s ORDER BY entry_date", (docket_number,))
        return [dict(r) for r in cur.fetchall()]


@router.get("/{docket_number}/changes")
def changes(docket_number: str):
    with db.connect() as conn:
        return [dict(c) for c in db.get_changes(conn, docket_number=docket_number)]


@router.get("/{docket_number}/text")
def text(docket_number: str):
    with tempfile.TemporaryDirectory() as tmpdir:
        pdf_path = fetch_docket_pdf(docket_number, out_dir=tmpdir)
        return {"docket_number": docket_number, "text": extract_text(pdf_path)}


@router.get("/{docket_number}/pdf")
def pdf(docket_number: str, doc: str = Query("docket")):
    tmpdir = tempfile.mkdtemp()
    pdf_path = fetch_docket_pdf(docket_number, out_dir=tmpdir, doc_type=doc)
    return FileResponse(pdf_path, media_type="application/pdf", filename=os.path.basename(pdf_path))

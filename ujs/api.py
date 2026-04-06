"""REST API for PA UJS court data."""

from datetime import datetime, timedelta
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from typing import Optional
import tempfile, os

from ujs.core import (
    search_by_name, search_by_docket, search_by_otn,
    search_by_date, search_by_calendar, download_pdf,
)
from ujs.modules.docket_pdf import analyze_docket, fetch_docket_pdf, extract_text

app = FastAPI(
    title="PA UJS Court Search API",
    description="Programmatic access to Pennsylvania Unified Judicial System court records",
    version="1.0.0",
)


# -------------------------------------------------------------------
# Search endpoints
# -------------------------------------------------------------------

@app.get("/search/name")
def api_search_name(
    last: str,
    first: Optional[str] = None,
    dob: Optional[str] = Query(None, description="MM/DD/YYYY"),
    county: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
):
    return search_by_name(last, first=first, dob=dob, county=county, docket_type=docket_type)


@app.get("/search/docket")
def api_search_docket(number: str = Query(..., description="e.g. CP-39-CR-0001378-1989")):
    return search_by_docket(number)


@app.get("/search/otn")
def api_search_otn(otn: str):
    return search_by_otn(otn)


@app.get("/search/filings")
def api_search_filings(
    days: int = Query(1, description="Days back to search"),
    start: Optional[str] = Query(None, description="YYYY-MM-DD start date"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD end date"),
    county: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
):
    today = datetime.now()
    s = start or (today - timedelta(days=days)).strftime("%Y-%m-%d")
    e = end or today.strftime("%Y-%m-%d")
    return search_by_date(s, e, county=county, docket_type=docket_type)


@app.get("/search/calendar")
def api_search_calendar(
    days: int = Query(7, description="Days ahead to search"),
    start: Optional[str] = Query(None, description="YYYY-MM-DD start date"),
    end: Optional[str] = Query(None, description="YYYY-MM-DD end date"),
    county: Optional[str] = None,
    docket_type: Optional[str] = Query(None, alias="type"),
):
    today = datetime.now()
    s = start or today.strftime("%Y-%m-%d")
    e = end or (today + timedelta(days=days)).strftime("%Y-%m-%d")
    return search_by_calendar(s, e, county=county, docket_type=docket_type)


# -------------------------------------------------------------------
# Docket endpoints
# -------------------------------------------------------------------

@app.get("/docket/{docket_number}")
def api_docket_info(docket_number: str):
    """Get case info for a docket number."""
    results = search_by_docket(docket_number)
    if not results:
        return {"error": "Not found", "docket_number": docket_number}
    return results[0]


@app.get("/docket/{docket_number}/analyze")
def api_docket_analyze(docket_number: str):
    """Download docket PDF and extract charges, dispositions, bail."""
    with tempfile.TemporaryDirectory() as tmpdir:
        result = analyze_docket(docket_number, out_dir=tmpdir)
        return {
            "docket_number": result["docket_number"],
            "charges": result["charges"],
            "dispositions": result["dispositions"],
            "bail": result["bail"],
        }


@app.get("/docket/{docket_number}/text")
def api_docket_text(docket_number: str):
    """Download docket PDF and return raw extracted text."""
    with tempfile.TemporaryDirectory() as tmpdir:
        pdf_path = fetch_docket_pdf(docket_number, out_dir=tmpdir)
        text = extract_text(pdf_path)
        return {"docket_number": docket_number, "text": text}


@app.get("/docket/{docket_number}/pdf")
def api_docket_pdf(docket_number: str):
    """Download and serve the docket sheet PDF directly."""
    tmpdir = tempfile.mkdtemp()
    pdf_path = fetch_docket_pdf(docket_number, out_dir=tmpdir)
    return FileResponse(pdf_path, media_type="application/pdf",
                        filename=os.path.basename(pdf_path))


# -------------------------------------------------------------------
# Health
# -------------------------------------------------------------------

@app.get("/health")
def health():
    return {"status": "ok", "portal": "https://ujsportal.pacourts.us"}

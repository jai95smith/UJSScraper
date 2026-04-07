"""Search + Quick Access routes."""

from datetime import datetime, timedelta
from fastapi import APIRouter, Query
from typing import Optional

from ujs import db

router = APIRouter()


# --- Quick Access ---

@router.get("/filings/today", tags=["Quick Access"])
def filings_today(county: Optional[str] = None, docket_type: Optional[str] = Query(None, alias="type"), limit: int = Query(200, le=500)):
    today = datetime.now().strftime("%m/%d/%Y")
    with db.connect() as conn:
        return [dict(r) for r in db.search_cases(conn, county=county, docket_type=docket_type, filed_after=today, filed_before=today, limit=limit)]


@router.get("/filings/recent", tags=["Quick Access"])
def filings_recent(days: int = Query(7), county: Optional[str] = None, docket_type: Optional[str] = Query(None, alias="type"), limit: int = Query(200, le=500)):
    start = (datetime.now() - timedelta(days=days)).strftime("%m/%d/%Y")
    today = datetime.now().strftime("%m/%d/%Y")
    with db.connect() as conn:
        return [dict(r) for r in db.search_cases(conn, county=county, docket_type=docket_type, filed_after=start, filed_before=today, limit=limit)]


@router.get("/hearings/today", tags=["Quick Access"])
def hearings_today(county: Optional[str] = None, docket_type: Optional[str] = Query(None, alias="type")):
    import psycopg2.extras
    today = datetime.now().strftime("%m/%d/%Y")
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        clauses, params = ["e.event_date LIKE %s"], [f"{today}%"]
        if county: clauses.append("c.county ILIKE %s"); params.append(county)
        if docket_type:
            code = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}.get(docket_type.lower(), "")
            if code: clauses.append("c.docket_number LIKE %s"); params.append(f"%{code}%")
        cur.execute(f"SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date FROM events e JOIN cases c ON e.docket_number = c.docket_number WHERE {' AND '.join(clauses)} ORDER BY e.event_date ASC", params)
        return [dict(r) for r in cur.fetchall()]


@router.get("/hearings/upcoming", tags=["Quick Access"])
def hearings_upcoming(days: int = Query(7), county: Optional[str] = None, docket_type: Optional[str] = Query(None, alias="type"), event_type: Optional[str] = None, limit: int = Query(200, le=500)):
    import psycopg2.extras
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        clauses, params = [], []
        if county: clauses.append("c.county ILIKE %s"); params.append(county)
        if docket_type:
            code = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}.get(docket_type.lower(), "")
            if code: clauses.append("c.docket_number LIKE %s"); params.append(f"%{code}%")
        if event_type: clauses.append("e.event_type ILIKE %s"); params.append(f"%{event_type}%")
        where = " AND ".join(clauses) if clauses else "TRUE"
        params.append(limit)
        cur.execute(f"SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date FROM events e JOIN cases c ON e.docket_number = c.docket_number WHERE {where} ORDER BY e.event_date ASC LIMIT %s", params)
        return [dict(r) for r in cur.fetchall()]


# --- Search ---

@router.get("/search/cases", tags=["Search"])
def search_cases(name: Optional[str] = None, county: Optional[str] = None, status: Optional[str] = None,
                 docket_type: Optional[str] = Query(None, alias="type"), filed_after: Optional[str] = None,
                 filed_before: Optional[str] = None, limit: int = Query(100, le=500)):
    with db.connect() as conn:
        return [dict(r) for r in db.search_cases(conn, county=county, status=status, docket_type=docket_type,
                                                  filed_after=filed_after, filed_before=filed_before, name=name, limit=limit)]


@router.get("/search/judge", tags=["Search"])
def search_judge(name: str, county: Optional[str] = None, limit: int = Query(100, le=500)):
    with db.connect() as conn:
        return [dict(r) for r in db.search_by_judge(conn, name, county=county, limit=limit)]


@router.get("/search/attorney", tags=["Search"])
def search_attorney(name: str, role: Optional[str] = None, county: Optional[str] = None, limit: int = Query(100, le=500)):
    with db.connect() as conn:
        return [dict(r) for r in db.search_by_attorney(conn, name, role=role, county=county, limit=limit)]


@router.get("/search/charges", tags=["Search"])
def search_charges(statute: Optional[str] = None, description: Optional[str] = None,
                   disposition: Optional[str] = None, county: Optional[str] = None, limit: int = Query(100, le=500)):
    with db.connect() as conn:
        return [dict(r) for r in db.search_by_charge(conn, statute=statute, description=description,
                                                      disposition=disposition, county=county, limit=limit)]


@router.get("/search/events", tags=["Search"])
def search_events(county: Optional[str] = None, docket_type: Optional[str] = Query(None, alias="type"), days: int = Query(7)):
    import psycopg2.extras
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        clauses, params = [], []
        if county: clauses.append("c.county ILIKE %s"); params.append(county)
        if docket_type:
            code = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}.get(docket_type.lower(), "")
            if code: clauses.append("c.docket_number LIKE %s"); params.append(f"%{code}%")
        where = " AND ".join(clauses) if clauses else "TRUE"
        cur.execute(f"SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date FROM events e JOIN cases c ON e.docket_number = c.docket_number WHERE {where} ORDER BY e.event_date ASC LIMIT 200", params)
        return [dict(r) for r in cur.fetchall()]

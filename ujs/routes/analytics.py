"""Analytics + Stats + Changes routes."""

from datetime import datetime
from fastapi import APIRouter, Query
from typing import Optional

from ujs import db

router = APIRouter()


@router.get("/stats", tags=["Analytics"])
def stats():
    with db.connect() as conn:
        return db.get_stats(conn)


@router.get("/stats/filings", tags=["Analytics"])
def filing_stats(county: Optional[str] = None, days: int = Query(30)):
    with db.connect() as conn:
        return [dict(r) for r in db.get_filing_stats(conn, county=county, days=days)]


@router.get("/stats/daily", tags=["Analytics"])
def daily_stats(county: Optional[str] = None, days: int = Query(30, le=180)):
    import psycopg2.extras
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        params = []
        county_clause = ""
        if county: county_clause = "AND county ILIKE %s"; params.append(county)
        params.append(days)
        cur.execute(f"""
            SELECT filing_date, COUNT(*) as total,
                SUM(CASE WHEN docket_number LIKE '%%-CR-%%' THEN 1 ELSE 0 END) as criminal,
                SUM(CASE WHEN docket_number LIKE '%%-TR-%%' THEN 1 ELSE 0 END) as traffic,
                SUM(CASE WHEN docket_number LIKE '%%-CV-%%' THEN 1 ELSE 0 END) as civil,
                SUM(CASE WHEN docket_number LIKE '%%-NT-%%' THEN 1 ELSE 0 END) as non_traffic,
                SUM(CASE WHEN docket_number LIKE '%%-LT-%%' THEN 1 ELSE 0 END) as landlord_tenant
            FROM cases WHERE filing_date != '' {county_clause}
            AND TO_DATE(filing_date, 'MM/DD/YYYY') >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY filing_date ORDER BY TO_DATE(filing_date, 'MM/DD/YYYY') DESC
        """, params)
        return [dict(r) for r in cur.fetchall()]


@router.get("/stats/counties", tags=["Analytics"])
def county_stats():
    with db.connect() as conn:
        return [dict(r) for r in db.get_county_stats(conn)]


@router.get("/stats/charges", tags=["Analytics"])
def charge_stats(county: Optional[str] = None, limit: int = Query(25, le=100)):
    with db.connect() as conn:
        return [dict(r) for r in db.get_charge_stats(conn, county=county, limit=limit)]


@router.get("/stats/judges", tags=["Analytics"])
def judge_stats(county: Optional[str] = None, limit: int = Query(25, le=100)):
    with db.connect() as conn:
        return [dict(r) for r in db.get_judge_stats(conn, county=county, limit=limit)]


@router.get("/changes", tags=["Changes"])
def changes_feed(since: Optional[str] = None, docket_number: Optional[str] = None, limit: int = Query(50, le=200)):
    since_dt = datetime.fromisoformat(since) if since else None
    with db.connect() as conn:
        return [dict(c) for c in db.get_changes(conn, docket_number=docket_number, since=since_dt, limit=limit)]

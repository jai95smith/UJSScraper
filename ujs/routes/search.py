"""Search + Quick Access routes."""

import random
from datetime import datetime, timedelta
from fastapi import APIRouter, Query
from typing import Optional

from ujs import db

router = APIRouter()


# --- Suggestions & Autocomplete ---

_suggestions_cache = {"data": [], "expires": 0}


@router.get("/suggestions", tags=["Search"])
def get_suggestions():
    """Dynamic query suggestions based on real data. Cached 1 hour."""
    import time
    now = time.time()
    if now < _suggestions_cache["expires"] and _suggestions_cache["data"]:
        return random.sample(_suggestions_cache["data"], min(6, len(_suggestions_cache["data"])))

    suggestions = []
    try:
        with db.connect() as conn:
            cur = db._dict_cur(conn)
            today = datetime.now().strftime("%m/%d/%Y")
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%m/%d/%Y")

            # Count today's hearings
            cur.execute("SELECT COUNT(*) as c FROM events WHERE event_date LIKE %s", (today + "%",))
            today_count = cur.fetchone()["c"]
            if today_count:
                suggestions.append(f"Show today's {today_count} hearings")

            # Count tomorrow's hearings
            cur.execute("SELECT COUNT(*) as c FROM events WHERE event_date LIKE %s", (tomorrow + "%",))
            tmrw_count = cur.fetchone()["c"]
            if tmrw_count:
                suggestions.append(f"What hearings are scheduled tomorrow?")

            # Recent notable filing
            cur.execute("""
                SELECT c.caption, c.county FROM cases c
                WHERE c.docket_number LIKE '%%-CR-%%'
                AND c.filing_date IS NOT NULL AND c.filing_date != ''
                ORDER BY TO_DATE(c.filing_date, 'MM/DD/YYYY') DESC LIMIT 5
            """)
            recent = cur.fetchall()
            if recent:
                r = random.choice(recent)
                name = r["caption"].split(" v. ")[-1] if " v. " in r["caption"] else r["caption"]
                suggestions.append(f"What is {name} charged with?")

            # Random judge
            cur.execute("SELECT DISTINCT analysis->>'judge' as j FROM analyses WHERE analysis->>'judge' IS NOT NULL AND analysis->>'judge' != '' ORDER BY RANDOM() LIMIT 1")
            judge = cur.fetchone()
            if judge and judge["j"]:
                suggestions.append(f"Show cases for Judge {judge['j'].split(',')[0]}")

            # Always include these — use a random active county for variety
            import random
            _counties = db.get_active_county_names()
            _sample = random.choice(_counties) if _counties else "your county"
            suggestions.extend([
                f"Criminal filings this week in {_sample}",
                "Average bail for DUI cases",
                f"Busiest defense attorneys in {_sample}",
                "How long do theft cases take to resolve?",
                "Sentencing patterns for assault charges",
                "Search docket entries for motion to suppress",
            ])

            _suggestions_cache["data"] = suggestions
            _suggestions_cache["expires"] = now + 3600
    except Exception:
        suggestions = [
            "What hearings are scheduled tomorrow?",
            "Criminal cases filed this week",
            "Average bail for DUI cases",
            "Busiest defense attorneys",
            "How long do theft cases take?",
            "Search for motion to suppress filings",
        ]

    return random.sample(suggestions, min(6, len(suggestions)))


@router.get("/autocomplete", tags=["Search"])
def autocomplete(q: str = Query(..., min_length=2, max_length=100)):
    """Autocomplete for participant names and docket numbers."""
    results = []
    with db.connect() as conn:
        cur = db._dict_cur(conn)
        q_clean = q.strip()

        # Docket number prefix
        if any(q_clean.upper().startswith(p) for p in ["CP-", "MJ-", "MD-"]):
            cur.execute("""
                SELECT docket_number, caption, county FROM cases
                WHERE docket_number ILIKE %s
                ORDER BY docket_number LIMIT 8
            """, (q_clean + "%",))
            for r in cur.fetchall():
                results.append({"type": "docket", "value": r["docket_number"], "label": f"{r['docket_number']} — {r['caption']}"})
        else:
            # Name search — match each word independently, split on spaces and hyphens
            import re
            words = [w for w in re.split(r'[\s\-]+', q_clean) if w]
            word_clauses = " AND ".join(["p.name ILIKE %s"] * len(words))
            word_params = [f"%{w}%" for w in words]
            cur.execute(f"""
                SELECT DISTINCT p.name, COUNT(DISTINCT p.docket_number) as cases
                FROM participants p
                WHERE {word_clauses}
                GROUP BY p.name
                ORDER BY cases DESC
                LIMIT 8
            """, word_params)
            for r in cur.fetchall():
                cases = r["cases"]
                results.append({"type": "name", "value": r["name"], "label": f"{r['name']} ({cases} case{'s' if cases != 1 else ''})"})

    return results


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
        cur.execute(f"SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date FROM events e JOIN cases c ON e.docket_number = c.docket_number WHERE {' AND '.join(clauses)} ORDER BY TO_DATE(SUBSTRING(e.event_date FROM 1 FOR 10), 'MM/DD/YYYY') ASC, e.event_date ASC", params)
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
        cur.execute(f"SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date FROM events e JOIN cases c ON e.docket_number = c.docket_number WHERE {where} ORDER BY TO_DATE(SUBSTRING(e.event_date FROM 1 FOR 10), 'MM/DD/YYYY') ASC, e.event_date ASC LIMIT %s", params)
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
        cur.execute(f"SELECT e.*, c.caption, c.status as case_status, c.county, c.filing_date FROM events e JOIN cases c ON e.docket_number = c.docket_number WHERE {where} ORDER BY TO_DATE(SUBSTRING(e.event_date FROM 1 FOR 10), 'MM/DD/YYYY') ASC, e.event_date ASC LIMIT 200", params)
        return [dict(r) for r in cur.fetchall()]

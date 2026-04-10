"""Rap sheet — full person court history."""

import tempfile
from fastapi import APIRouter, Query
from typing import Optional

from ujs import db

router = APIRouter(tags=["Rap Sheet"])


def _parse_date(d):
    try:
        parts = d.split("/")
        return f"{parts[2]}{parts[0]}{parts[1]}"
    except Exception:
        return "0"


@router.get("/rapsheet/{name}")
def rapsheet(name: str, county: Optional[str] = None):
    """Get a person's full court history."""
    import psycopg2.extras

    # Step 1: Search DB
    with db.connect() as conn:
        cases = db.search_cases(conn, name=name, limit=50)
        if not cases:
            fuzzy = db.fuzzy_name_search(conn, name, limit=5)
            if fuzzy and fuzzy[0]["match_score"] >= 0.4:
                cases = db.search_cases(conn, name=fuzzy[0]["name"], limit=50)

    # Step 2: Live search if not in DB
    if not cases:
        from ujs.core import search_by_name
        parts = name.strip().split()
        search_attempts = []
        if len(parts) >= 2:
            search_attempts.append((parts[-1], parts[0]))
            search_attempts.append((parts[0], parts[-1]))
        search_attempts.append((name, None))

        search_counties = db.get_active_county_names()
        if county and county not in search_counties:
            search_counties.append(county)

        for sc in search_counties:
            for last, first in search_attempts:
                try:
                    results = search_by_name(last, first=first, county=sc)
                    if results:
                        with db.connect() as conn:
                            db.upsert_cases(conn, results)
                        break
                except Exception:
                    pass

        # Analyze unanalyzed cases
        from ujs.modules.docket_pdf import analyze_docket as _analyze
        with db.connect() as conn:
            cases = db.search_cases(conn, name=name, limit=50)
        if cases:
            unanalyzed = []
            with db.connect() as conn:
                cur = conn.cursor()
                for case in cases:
                    cur.execute("SELECT id FROM analyses WHERE docket_number = %s AND doc_type = 'docket'", (case["docket_number"],))
                    if not cur.fetchone():
                        unanalyzed.append(case)
            unanalyzed.sort(key=lambda c: _parse_date(c.get("filing_date", "")), reverse=True)
            for case in unanalyzed[:10]:
                try:
                    with tempfile.TemporaryDirectory() as d:
                        analysis = _analyze(case["docket_number"], out_dir=d)
                    with db.connect() as conn:
                        db.detect_and_store_changes(conn, case["docket_number"], analysis)
                except Exception:
                    pass

        with db.connect() as conn:
            cases = db.search_cases(conn, name=name, limit=50)

    if not cases:
        county_list = ", ".join(db.get_active_county_names()) or "any"
        return {"name": name, "cases": [], "message": f"No cases found in {county_list}, or statewide courts"}

    # Step 2b: Analyze unanalyzed cases already in DB
    from ujs.modules.docket_pdf import analyze_docket as _analyze
    unanalyzed = []
    with db.connect() as conn:
        cur = conn.cursor()
        for case in cases:
            cur.execute("SELECT id FROM analyses WHERE docket_number = %s AND doc_type = 'docket'", (case["docket_number"],))
            if not cur.fetchone():
                unanalyzed.append(case)
    unanalyzed.sort(key=lambda c: _parse_date(c.get("filing_date", "")), reverse=True)
    for case in unanalyzed[:10]:
        try:
            with tempfile.TemporaryDirectory() as d:
                analysis = _analyze(case["docket_number"], out_dir=d)
            with db.connect() as conn:
                db.detect_and_store_changes(conn, case["docket_number"], analysis)
        except Exception:
            pass

    # Step 3: Build profile
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        person = {"name": name, "aliases": set(), "dob": None, "addresses": set()}
        case_details = []

        for case in cases:
            dn = case["docket_number"]
            entry = {"docket_number": dn, "caption": case["caption"], "status": case["status"],
                     "county": case["county"], "filing_date": case["filing_date"], "court_type": case["court_type"]}

            cur.execute("SELECT name, dob FROM participants WHERE docket_number = %s", (dn,))
            for p in cur.fetchall():
                person["aliases"].add(p["name"])
                if p["dob"]: person["dob"] = p["dob"]

            analysis = db.get_analysis(conn, dn, "docket")
            if analysis:
                entry.update({k: analysis.get(k) for k in ["charges", "sentences", "bail", "judge", "attorneys"]})
                entry["analyzed"] = True
                addr = (analysis.get("defendant") or {}).get("address")
                if addr: person["addresses"].add(addr)
            else:
                entry["analyzed"] = False

            cur.execute("SELECT event_type, event_date, event_status FROM events WHERE docket_number = %s ORDER BY TO_DATE(SUBSTRING(event_date FROM 1 FOR 10), 'MM/DD/YYYY') ASC, event_date ASC", (dn,))
            events = cur.fetchall()
            if events: entry["upcoming_events"] = [dict(e) for e in events]

            case_details.append(entry)

        total = len(case_details)
        return {
            "person": {
                "name": person["aliases"].pop() if person["aliases"] else name,
                "aliases": list(person["aliases"]),
                "dob": person["dob"],
                "addresses": list(person["addresses"]),
            },
            "summary": {
                "total_cases": total,
                "active_cases": sum(1 for c in case_details if "active" in c["status"].lower()),
                "closed_cases": sum(1 for c in case_details if "closed" in c["status"].lower()),
                "criminal_cases": sum(1 for c in case_details if "-CR-" in c["docket_number"]),
                "analyzed": sum(1 for c in case_details if c.get("analyzed")),
            },
            "cases": case_details,
        }

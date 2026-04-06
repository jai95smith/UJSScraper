"""MCP server for PA UJS court data — lets Claude answer natural language court questions."""

import json, os
from typing import Optional

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("PA Court Records",
              instructions="You have access to Pennsylvania court records for Lehigh and "
                           "Northampton counties. Use these tools to look up cases, check "
                           "hearing schedules, find attorney/judge info, and answer questions "
                           "about court proceedings. Always cite docket numbers in your answers.")


def _db():
    """Lazy import to avoid circular deps."""
    from ujs import db
    return db


# ---------------------------------------------------------------------------
# Case lookup
# ---------------------------------------------------------------------------

@mcp.tool()
def lookup_docket(docket_number: str) -> str:
    """Look up a specific court case by docket number.
    Examples: CP-39-CR-0000142-2025, MJ-31107-CR-0000122-2026, 138 MD 2026"""
    db = _db()
    with db.connect() as conn:
        case = db.get_case(conn, docket_number)
        if not case:
            return f"No case found for docket number: {docket_number}"
        return json.dumps(dict(case), indent=2, default=str)


@mcp.tool()
def get_case_analysis(docket_number: str) -> str:
    """Get detailed Gemini-parsed analysis of a case including charges, sentences,
    bail, attorneys, and full docket entry timeline."""
    db = _db()
    with db.connect() as conn:
        analysis = db.get_analysis(conn, docket_number, "docket")
        if not analysis:
            # Check if case exists at all
            case = db.get_case(conn, docket_number)
            if case:
                return f"Case {docket_number} is indexed but not yet analyzed. The analysis will be available shortly."
            return f"No case found for: {docket_number}. Try searching by name instead."
        return json.dumps(analysis, indent=2, default=str)


@mcp.tool()
def get_court_summary(docket_number: str) -> str:
    """Get a person's full court history across all counties by looking up their
    court summary via any of their docket numbers."""
    db = _db()
    with db.connect() as conn:
        analysis = db.get_analysis(conn, docket_number, "summary")
        if analysis:
            return json.dumps(analysis, indent=2, default=str)
    return f"No court summary cached for {docket_number}. Use the API endpoint /docket/{docket_number}/summary to fetch it."


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@mcp.tool()
def search_cases(
    name: Optional[str] = None,
    county: Optional[str] = None,
    case_status: Optional[str] = None,
    case_type: Optional[str] = None,
    filed_after: Optional[str] = None,
    filed_before: Optional[str] = None,
    limit: int = 20,
) -> str:
    """Search court cases by participant name, county, status, type, or filing date.
    county: Lehigh, Northampton, etc.
    case_type: Criminal, Civil, Traffic
    case_status: Active, Closed
    filed_after/filed_before: MM/DD/YYYY"""
    db = _db()
    with db.connect() as conn:
        results = db.search_cases(conn, county=county, status=case_status,
                                  docket_type=case_type, filed_after=filed_after,
                                  filed_before=filed_before, name=name, limit=limit)
        if not results:
            return "No cases found matching those criteria."
        cases = [dict(r) for r in results]
        summary = f"Found {len(cases)} case(s):\n\n"
        for c in cases:
            summary += f"- **{c['docket_number']}** | {c['caption']} | {c['status']} | Filed: {c['filing_date']} | {c['county']}\n"
        return summary


@mcp.tool()
def search_by_judge_name(judge_name: str, county: Optional[str] = None) -> str:
    """Find cases assigned to a specific judge."""
    db = _db()
    with db.connect() as conn:
        results = db.search_by_judge(conn, judge_name, county=county, limit=20)
        if not results:
            return f"No cases found for judge matching '{judge_name}'"
        summary = f"Found {len(results)} case(s) for judge '{judge_name}':\n\n"
        for r in results:
            summary += f"- **{r['docket_number']}** | {r['caption']} | {r['status']} | {r['county']}\n"
        return summary


@mcp.tool()
def search_by_attorney_name(attorney_name: str, role: Optional[str] = None, county: Optional[str] = None) -> str:
    """Find cases involving a specific attorney.
    role: 'Public Defender', 'District Attorney', 'Private', etc."""
    db = _db()
    with db.connect() as conn:
        results = db.search_by_attorney(conn, attorney_name, role=role, county=county, limit=20)
        if not results:
            return f"No cases found for attorney matching '{attorney_name}'"
        summary = f"Found {len(results)} case(s) for attorney '{attorney_name}':\n\n"
        for r in results:
            summary += f"- **{r['docket_number']}** | {r['caption']} | {r['name']} ({r['role']}) | {r['county']}\n"
        return summary


@mcp.tool()
def search_by_charge_type(
    statute: Optional[str] = None,
    description: Optional[str] = None,
    disposition: Optional[str] = None,
    county: Optional[str] = None,
) -> str:
    """Search cases by charge type, statute number, or disposition.
    statute: e.g. '3929' for retail theft
    description: e.g. 'DUI', 'Assault', 'Retail Theft'
    disposition: e.g. 'Guilty', 'Dismissed'"""
    db = _db()
    with db.connect() as conn:
        results = db.search_by_charge(conn, statute=statute, description=description,
                                       disposition=disposition, county=county, limit=20)
        if not results:
            return "No cases found matching those charge criteria."
        summary = f"Found {len(results)} charge(s):\n\n"
        for r in results:
            summary += f"- **{r['docket_number']}** | {r['description']} ({r['grade']}) | Disp: {r['disposition']} | {r['county']}\n"
        return summary


# ---------------------------------------------------------------------------
# Calendar / hearings
# ---------------------------------------------------------------------------

@mcp.tool()
def get_todays_hearings(county: Optional[str] = None, case_type: Optional[str] = None) -> str:
    """Get all court hearings scheduled for today.
    case_type: Criminal, Civil, Traffic"""
    db = _db()
    from datetime import datetime
    today = datetime.now().strftime("%m/%d/%Y")
    with db.connect() as conn:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        clauses = ["e.event_date LIKE %s"]
        params = [f"{today}%"]
        if county:
            clauses.append("c.county ILIKE %s")
            params.append(county)
        if case_type:
            dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}
            code = dtype_map.get(case_type.lower(), "")
            if code:
                clauses.append("c.docket_number LIKE %s")
                params.append(f"%{code}%")
        cur.execute(f"""
            SELECT e.*, c.caption, c.county FROM events e
            JOIN cases c ON e.docket_number = c.docket_number
            WHERE {' AND '.join(clauses)} ORDER BY e.event_date ASC
        """, params)
        results = cur.fetchall()
        if not results:
            return f"No hearings scheduled for today ({today})"
        summary = f"{len(results)} hearing(s) today ({today}):\n\n"
        for r in results:
            summary += f"- **{r['event_date']}** | {r['event_type']} | {r['caption']} | {r['docket_number']} | {r['county']}\n"
        return summary


@mcp.tool()
def get_upcoming_hearings(
    days: int = 7,
    county: Optional[str] = None,
    case_type: Optional[str] = None,
    event_type: Optional[str] = None,
) -> str:
    """Get upcoming court hearings/events in the next N days.
    event_type: 'Preliminary Hearing', 'Trial', 'Arraignment', 'Sentencing', etc."""
    db = _db()
    with db.connect() as conn:
        import psycopg2.extras
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        clauses = []
        params = []
        if county:
            clauses.append("c.county ILIKE %s")
            params.append(county)
        if case_type:
            dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}
            code = dtype_map.get(case_type.lower(), "")
            if code:
                clauses.append("c.docket_number LIKE %s")
                params.append(f"%{code}%")
        if event_type:
            clauses.append("e.event_type ILIKE %s")
            params.append(f"%{event_type}%")
        where = " AND ".join(clauses) if clauses else "TRUE"
        params.append(min(days * 50, 200))
        cur.execute(f"""
            SELECT e.*, c.caption, c.county FROM events e
            JOIN cases c ON e.docket_number = c.docket_number
            WHERE {where} ORDER BY e.event_date ASC LIMIT %s
        """, params)
        results = cur.fetchall()
        if not results:
            return "No upcoming hearings found matching those criteria."
        summary = f"{len(results)} upcoming hearing(s):\n\n"
        for r in results:
            summary += f"- **{r['event_date']}** | {r['event_type']} | {r['caption']} | {r['docket_number']} | {r['county']}\n"
        return summary


# ---------------------------------------------------------------------------
# Stats / analytics
# ---------------------------------------------------------------------------

@mcp.tool()
def get_filing_statistics(county: Optional[str] = None, days: int = 30) -> str:
    """Get filing counts and trends by date, broken down by case type (criminal, traffic, civil)."""
    db = _db()
    with db.connect() as conn:
        results = db.get_filing_stats(conn, county=county, days=days)
        if not results:
            return "No filing statistics available."
        summary = f"Filing stats (last {days} days):\n\n"
        summary += "| Date | Total | Criminal | Traffic | Civil |\n|---|---|---|---|---|\n"
        for r in results:
            summary += f"| {r['filing_date']} | {r['count']} | {r['criminal']} | {r['traffic']} | {r['civil']} |\n"
        return summary


@mcp.tool()
def get_charge_statistics(county: Optional[str] = None) -> str:
    """Get the most common charges filed, with guilty/dismissed breakdown."""
    db = _db()
    with db.connect() as conn:
        results = db.get_charge_stats(conn, county=county, limit=15)
        if not results:
            return "No charge statistics available. Cases may not be analyzed yet."
        summary = "Most common charges:\n\n"
        summary += "| Charge | Grade | Count | Guilty | Dismissed |\n|---|---|---|---|---|\n"
        for r in results:
            summary += f"| {r['description']} | {r['grade']} | {r['count']} | {r['guilty']} | {r['dismissed']} |\n"
        return summary


@mcp.tool()
def get_database_stats() -> str:
    """Get current database statistics — how many cases, events, analyses are indexed."""
    db = _db()
    with db.connect() as conn:
        stats = db.get_stats(conn)
        return json.dumps(stats, indent=2, default=str)


@mcp.tool()
def get_case_changes(docket_number: Optional[str] = None, limit: int = 20) -> str:
    """Get recent changes/updates to cases. Can filter by specific docket number
    to see what changed on a case over time."""
    db = _db()
    with db.connect() as conn:
        changes = db.get_changes(conn, docket_number=docket_number, limit=limit)
        if not changes:
            return "No changes recorded." if not docket_number else f"No changes recorded for {docket_number}."
        summary = f"{len(changes)} recent change(s):\n\n"
        for c in changes:
            summary += f"- **{c['docket_number']}** | {c['field']}: {c['old_value']} → {c['new_value']} | {c['detected_at']}\n"
        return summary


if __name__ == "__main__":
    import sys
    if "--http" in sys.argv:
        port = 8200
        for i, arg in enumerate(sys.argv):
            if arg == "--port" and i + 1 < len(sys.argv):
                port = int(sys.argv[i + 1])
        print(f"MCP server starting on http://0.0.0.0:{port}/mcp")
        mcp.run(transport="streamable-http", host="0.0.0.0", port=port)
    else:
        mcp.run()

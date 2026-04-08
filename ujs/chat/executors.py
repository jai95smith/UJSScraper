"""Tool executors — each function handles one tool call against the DB."""

import json
import psycopg2.extras
from datetime import datetime

from ujs import db


def execute_tool(name, inputs):
    """Route a tool call to the appropriate executor."""
    with db.connect() as conn:
        handler = HANDLERS.get(name)
        if handler:
            return handler(conn, inputs)
    return f"Unknown tool: {name}"


# ---------------------------------------------------------------------------
# Lookup tools
# ---------------------------------------------------------------------------

def _lookup_docket(conn, inputs):
    from ujs.chat.docket_parser import normalize_docket
    raw = inputs["docket_number"]
    normalized, confidence = normalize_docket(raw)

    # Try normalized first
    case = db.get_case(conn, normalized)
    if not case and normalized != raw:
        case = db.get_case(conn, raw)  # fallback to raw
    if not case:
        return f"No case found for: {raw}" + (f" (tried: {normalized})" if normalized != raw else "")
    return json.dumps(dict(case), default=str)


def _get_case_analysis(conn, inputs):
    analysis = db.get_analysis(conn, inputs["docket_number"], "docket")
    if not analysis:
        case = db.get_case(conn, inputs["docket_number"])
        if case:
            return f"Case exists but not yet analyzed. Basic info: {json.dumps(dict(case), default=str)}"
        return f"No case found for: {inputs['docket_number']}"
    return json.dumps(analysis, default=str)


def _get_person_history(conn, inputs):
    cases = db.search_cases(conn, name=inputs["name"], county=inputs.get("county"), limit=20)
    if not cases:
        return f"No cases found for {inputs['name']}"
    history = []
    for case in cases:
        dn = case["docket_number"]
        entry = {"docket_number": dn, "caption": case["caption"], "status": case["status"],
                 "county": case["county"], "filing_date": case["filing_date"]}
        analysis = db.get_analysis(conn, dn, "docket")
        if analysis:
            entry.update({k: analysis.get(k) for k in ["charges", "sentences", "bail", "judge"]})
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT event_type, event_date, event_status FROM events WHERE docket_number = %s", (dn,))
        events = cur.fetchall()
        if events:
            entry["events"] = [dict(e) for e in events]
        history.append(entry)
    return json.dumps(history, default=str)


def _get_docket_events(conn, inputs):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT event_type, event_status, event_date, event_location FROM events WHERE docket_number = %s ORDER BY event_date ASC", (inputs["docket_number"],))
    results = cur.fetchall()
    if not results:
        return f"No upcoming events for {inputs['docket_number']}"
    return json.dumps([dict(r) for r in results], default=str)


# ---------------------------------------------------------------------------
# Search tools
# ---------------------------------------------------------------------------

def _search_cases(conn, inputs):
    results = db.search_cases(conn, name=inputs.get("name"), county=inputs.get("county"),
                              status=inputs.get("case_status"), docket_type=inputs.get("case_type"),
                              filed_after=inputs.get("filed_after"), filed_before=inputs.get("filed_before"), limit=20)
    return json.dumps([dict(r) for r in results], default=str) if results else "No cases found."


def _fuzzy_name_search(conn, inputs):
    results = db.fuzzy_name_search(conn, inputs["name"], limit=10)
    return json.dumps([dict(r) for r in results], default=str) if results else f"No close matches found for: {inputs['name']}"


def _search_by_judge(conn, inputs):
    results = db.search_by_judge(conn, inputs["judge_name"], county=inputs.get("county"), limit=20)
    return json.dumps([dict(r) for r in results], default=str) if results else f"No cases found for judge: {inputs['judge_name']}"


def _search_by_attorney(conn, inputs):
    results = db.search_by_attorney(conn, inputs["attorney_name"], role=inputs.get("role"), county=inputs.get("county"), limit=20)
    return json.dumps([dict(r) for r in results], default=str) if results else f"No cases found for attorney: {inputs['attorney_name']}"


def _search_by_charge(conn, inputs):
    results = db.search_by_charge(conn, statute=inputs.get("statute"), description=inputs.get("description"),
                                   disposition=inputs.get("disposition"), county=inputs.get("county"), limit=20)
    return json.dumps([dict(r) for r in results], default=str) if results else "No charges found."


# ---------------------------------------------------------------------------
# Hearing tools
# ---------------------------------------------------------------------------

def _get_todays_hearings(conn, inputs):
    today = datetime.now().strftime("%m/%d/%Y")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    clauses, params = ["e.event_date LIKE %s"], [f"{today}%"]
    if inputs.get("county"):
        clauses.append("c.county ILIKE %s"); params.append(inputs["county"])
    if inputs.get("case_type"):
        code = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}.get(inputs["case_type"].lower(), "")
        if code: clauses.append("c.docket_number LIKE %s"); params.append(f"%{code}%")
    cur.execute(f"SELECT e.*, c.caption, c.county FROM events e JOIN cases c ON e.docket_number = c.docket_number WHERE {' AND '.join(clauses)} ORDER BY e.event_date ASC", params)
    results = cur.fetchall()
    return json.dumps([dict(r) for r in results], default=str) if results else f"No hearings today ({today})"


def _get_upcoming_hearings(conn, inputs):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    clauses, params = [], []
    if inputs.get("target_date"):
        clauses.append("e.event_date LIKE %s"); params.append(f"{inputs['target_date']}%")
    if inputs.get("county"):
        clauses.append("c.county ILIKE %s"); params.append(inputs["county"])
    if inputs.get("case_type"):
        code = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}.get(inputs["case_type"].lower(), "")
        if code: clauses.append("c.docket_number LIKE %s"); params.append(f"%{code}%")
    if inputs.get("event_type"):
        clauses.append("e.event_type ILIKE %s"); params.append(f"%{inputs['event_type']}%")
    where = " AND ".join(clauses) if clauses else "TRUE"
    params.append(200)
    cur.execute(f"SELECT e.*, c.caption, c.county FROM events e JOIN cases c ON e.docket_number = c.docket_number WHERE {where} ORDER BY e.event_date ASC LIMIT %s", params)
    results = cur.fetchall()
    return json.dumps([dict(r) for r in results], default=str) if results else f"No hearings found for {inputs.get('target_date', 'the specified period')}"


# ---------------------------------------------------------------------------
# Live search
# ---------------------------------------------------------------------------

def _live_search_ujs(conn, inputs):
    from ujs.core import search_by_name
    from ujs.modules.docket_pdf import analyze_docket
    import tempfile

    last, first = inputs["last_name"], inputs.get("first_name")
    user_county = inputs.get("county")

    # UJS requires county for name search — always search both LV counties + user's county
    search_counties = ["Lehigh", "Northampton"]
    if user_county and user_county not in search_counties:
        search_counties.append(user_county)

    results = []
    for county in search_counties:
        try:
            r = search_by_name(last, first=first, county=county)
            if r:
                results.extend(r)
        except Exception:
            pass

    # Try hyphenated parts if still nothing
    if not results and "-" in last:
        for part in last.split("-"):
            for county in search_counties:
                try:
                    r = search_by_name(part.strip(), first=first, county=county)
                    if r: results.extend(r)
                except Exception:
                    pass
            if results: break

    if not results:
        return f"No cases found on UJS for {first or ''} {last} (searched: {', '.join(search_counties)})"

    # Deduplicate by docket number
    seen = set()
    unique = []
    for r in results:
        dn = r.get("docket_number")
        if dn and dn not in seen:
            seen.add(dn)
            unique.append(r)
    results = unique

    with db.connect() as conn_store:
        db.upsert_cases(conn_store, results)

    def _parse_date(d):
        try:
            parts = d.split("/")
            return f"{parts[2]}{parts[0]}{parts[1]}"
        except Exception:
            return "0"

    results.sort(key=lambda r: _parse_date(r.get("filing_date", "")), reverse=True)

    analyzed = []
    for r in results:
        if r.get("status", "").lower() == "closed":
            continue
        try:
            with tempfile.TemporaryDirectory() as d:
                analysis = analyze_docket(r["docket_number"], out_dir=d)
            with db.connect() as conn2:
                db.detect_and_store_changes(conn2, r["docket_number"], analysis)
            analyzed.append({"docket_number": r["docket_number"], "analysis": analysis})
        except Exception as e:
            print(f"[live_search] Analyze error {r['docket_number']}: {e}")

    if not analyzed:
        try:
            with tempfile.TemporaryDirectory() as d:
                analysis = analyze_docket(results[0]["docket_number"], out_dir=d)
            with db.connect() as conn2:
                db.detect_and_store_changes(conn2, results[0]["docket_number"], analysis)
            analyzed.append({"docket_number": results[0]["docket_number"], "analysis": analysis})
        except Exception as e:
            print(f"[live_search] Fallback analyze error: {e}")

    return json.dumps({
        "analyzed_cases": analyzed,
        "all_cases": [{"docket_number": r["docket_number"], "caption": r["caption"],
                       "status": r["status"], "county": r["county"], "filing_date": r["filing_date"]}
                      for r in results[:15]],
    }, default=str)


# ---------------------------------------------------------------------------
# Stats tools
# ---------------------------------------------------------------------------

def _get_stats_query(conn, inputs):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    stat = inputs["stat_type"]
    county = inputs.get("county")
    cp = [county] if county else []
    cc = "AND c.county ILIKE %s" if county else ""

    if stat == "case_counts":
        cur.execute(f"""
            SELECT county, COUNT(*) as total,
                SUM(CASE WHEN docket_number LIKE '%%-CR-%%' THEN 1 ELSE 0 END) as criminal,
                SUM(CASE WHEN docket_number LIKE '%%-TR-%%' THEN 1 ELSE 0 END) as traffic,
                SUM(CASE WHEN docket_number LIKE '%%-CV-%%' THEN 1 ELSE 0 END) as civil,
                SUM(CASE WHEN docket_number LIKE '%%-NT-%%' THEN 1 ELSE 0 END) as non_traffic,
                SUM(CASE WHEN docket_number LIKE '%%-LT-%%' THEN 1 ELSE 0 END) as landlord_tenant,
                ROUND(SUM(CASE WHEN docket_number LIKE '%%-CR-%%' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as criminal_pct
            FROM cases c WHERE county != '' {cc.replace('AND c.', 'AND ')}
            GROUP BY county ORDER BY total DESC
        """, cp)
    elif stat == "bail_stats":
        amt = "REPLACE(REPLACE(b.amount, '$', ''), ',', '')::numeric"
        cur.execute(f"""
            SELECT b.bail_type, COUNT(*) as count, MIN({amt}) as min_amount,
                MAX({amt}) as max_amount, ROUND(AVG({amt}), 2) as avg_amount
            FROM bail b JOIN cases c ON b.docket_number = c.docket_number
            WHERE b.amount IS NOT NULL AND b.amount != '' {"AND c.county ILIKE %s" if county else ""}
            GROUP BY b.bail_type ORDER BY count DESC
        """, cp)
        by_type = [dict(r) for r in cur.fetchall()]
        cur.execute(f"""
            SELECT COUNT(*) as total_with_bail, ROUND(AVG({amt}), 2) as overall_avg
            FROM bail b JOIN cases c ON b.docket_number = c.docket_number
            WHERE b.amount IS NOT NULL AND b.amount != '' AND {amt} > 0
            {"AND c.county ILIKE %s" if county else ""}
        """, cp)
        return json.dumps({"by_type": by_type, "overall": dict(cur.fetchone())}, default=str)
    elif stat == "charge_breakdown":
        cur.execute(f"""
            SELECT ch.description, ch.grade, COUNT(*) as count,
                SUM(CASE WHEN ch.disposition ILIKE '%%guilty%%' THEN 1 ELSE 0 END) as guilty,
                SUM(CASE WHEN ch.disposition ILIKE '%%dismissed%%' OR ch.disposition ILIKE '%%quashed%%' THEN 1 ELSE 0 END) as dismissed,
                ROUND(SUM(CASE WHEN ch.disposition ILIKE '%%guilty%%' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as guilty_rate
            FROM charges ch JOIN cases c ON ch.docket_number = c.docket_number
            WHERE ch.description != '' {cc}
            GROUP BY ch.description, ch.grade ORDER BY count DESC LIMIT 20
        """, cp)
    elif stat == "filing_trend":
        days = inputs.get("days", 30)
        cur.execute(f"""
            SELECT filing_date, COUNT(*) as total,
                SUM(CASE WHEN docket_number LIKE '%%-CR-%%' THEN 1 ELSE 0 END) as criminal,
                SUM(CASE WHEN docket_number LIKE '%%-TR-%%' THEN 1 ELSE 0 END) as traffic
            FROM cases c WHERE filing_date != ''
            AND TO_DATE(filing_date, 'MM/DD/YYYY') >= CURRENT_DATE - INTERVAL '{days} days'
            {cc.replace('AND c.', 'AND ')}
            GROUP BY filing_date ORDER BY TO_DATE(filing_date, 'MM/DD/YYYY') DESC
        """, cp)
    elif stat == "hearing_counts":
        cur.execute(f"""
            SELECT e.event_type, COUNT(*) as count,
                SUM(CASE WHEN e.event_status = 'Scheduled' THEN 1 ELSE 0 END) as scheduled
            FROM events e JOIN cases c ON e.docket_number = c.docket_number
            WHERE TRUE {cc} GROUP BY e.event_type ORDER BY count DESC LIMIT 15
        """, cp)
    elif stat == "repeat_offenders":
        cur.execute(f"""
            SELECT p.name, COUNT(DISTINCT p.docket_number) as case_count,
                STRING_AGG(DISTINCT c.county, ', ') as counties
            FROM participants p JOIN cases c ON p.docket_number = c.docket_number
            WHERE TRUE {cc} GROUP BY p.name HAVING COUNT(DISTINCT p.docket_number) >= 5
            ORDER BY case_count DESC LIMIT 15
        """, cp)
    elif stat == "judge_performance":
        cur.execute(f"""
            SELECT a.analysis->>'judge' as judge, COUNT(DISTINCT ch.docket_number) as total_cases,
                COUNT(*) as total_charges,
                SUM(CASE WHEN ch.disposition ILIKE '%%guilty%%' THEN 1 ELSE 0 END) as guilty,
                SUM(CASE WHEN ch.disposition ILIKE '%%dismissed%%' OR ch.disposition ILIKE '%%quashed%%' THEN 1 ELSE 0 END) as dismissed,
                ROUND(SUM(CASE WHEN ch.disposition ILIKE '%%dismissed%%' OR ch.disposition ILIKE '%%quashed%%' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as dismissal_rate
            FROM charges ch JOIN analyses a ON ch.docket_number = a.docket_number
            WHERE a.analysis->>'judge' IS NOT NULL AND a.analysis->>'judge' != ''
            {"AND EXISTS (SELECT 1 FROM cases c WHERE c.docket_number = ch.docket_number AND c.county ILIKE %s)" if county else ""}
            GROUP BY a.analysis->>'judge' HAVING COUNT(*) >= 3
            ORDER BY COUNT(DISTINCT ch.docket_number) DESC LIMIT 15
        """, cp)
    else:
        return "Unknown stat type"
    return json.dumps([dict(r) for r in cur.fetchall()], default=str)


def _run_custom_query(conn, inputs):
    sql = inputs["sql"].strip()
    sql_upper = sql.upper()
    blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
               "CREATE", "GRANT", "REVOKE", "COPY", "EXECUTE", "SET ", "COMMIT", "ROLLBACK", "BEGIN", ";"]
    for word in blocked:
        if word in sql_upper:
            return f"Blocked: query contains '{word}'. Only SELECT queries allowed."
    if not sql_upper.startswith("SELECT"):
        return "Blocked: query must start with SELECT."
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SET statement_timeout = '5s'")
        cur.execute(sql)
        rows = cur.fetchall()
        cur.execute("RESET statement_timeout")
        return json.dumps([dict(r) for r in rows[:100]], default=str)
    except Exception as e:
        return f"Query error: {str(e)[:300]}. Fix the SQL and try again."


def _get_analysis_coverage(conn, inputs):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    clauses, params = ["TRUE"], []
    if inputs.get("county"):
        clauses.append("c.county ILIKE %s"); params.append(inputs["county"])
    if inputs.get("case_type"):
        code = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}.get(inputs["case_type"].lower(), "")
        if code: clauses.append("c.docket_number LIKE %s"); params.append(f"%{code}%")
    where = " AND ".join(clauses)
    cur.execute(f"SELECT COUNT(*) as total FROM cases c WHERE {where}", params)
    total = cur.fetchone()["total"]
    cur.execute(f"SELECT COUNT(*) as analyzed FROM cases c JOIN analyses a ON c.docket_number = a.docket_number WHERE {where}", params)
    analyzed = cur.fetchone()["analyzed"]
    pct = round(analyzed / total * 100, 1) if total > 0 else 0
    return json.dumps({"total_cases": total, "analyzed_cases": analyzed, "coverage_pct": pct,
                       "note": f"{analyzed} of {total} cases ({pct}%) have full charge/bail/attorney data."})


def _render_chart(conn, inputs):
    chart_json = json.dumps({"type": inputs["type"], "title": inputs["title"],
                             "labels": inputs["labels"], "datasets": inputs["datasets"]})
    return f"CHART_RENDERED. Include this exact block in your response:\n```chart\n{chart_json}\n```"


def _get_system_logs(conn, inputs):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    clauses = [f"created_at >= NOW() - INTERVAL '{inputs.get('hours', 24)} hours'"]
    params = []
    if inputs.get("component") and inputs["component"] != "all":
        clauses.append("component = %s")
        params.append(inputs["component"])
    if inputs.get("errors_only"):
        clauses.append("success = FALSE")
    params.append(inputs.get("limit", 50))
    cur.execute(f"""
        SELECT component, event, docket_number, detail, duration_ms, success, created_at
        FROM system_log WHERE {' AND '.join(clauses)}
        ORDER BY created_at DESC LIMIT %s
    """, params)
    rows = [dict(r) for r in cur.fetchall()]

    # Also get summary
    cur.execute(f"""
        SELECT component, success, COUNT(*) FROM system_log
        WHERE created_at >= NOW() - INTERVAL '{inputs.get('hours', 24)} hours'
        GROUP BY component, success ORDER BY component
    """)
    summary = [dict(r) for r in cur.fetchall()]
    return json.dumps({"logs": rows, "summary": summary}, default=str)


def _get_analyzer_throughput(conn, inputs):
    hours = inputs.get("hours", 24)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT date_trunc('hour', parsed_at) as hour, COUNT(*) as count
        FROM analyses
        WHERE parsed_at >= NOW() - INTERVAL '%s hours'
        GROUP BY date_trunc('hour', parsed_at)
        ORDER BY hour
    """, (hours,))
    hourly = [dict(r) for r in cur.fetchall()]

    cur.execute("SELECT COUNT(*) as total FROM analyses")
    total = cur.fetchone()["total"]
    cur.execute("SELECT COUNT(*) as total FROM cases")
    total_cases = cur.fetchone()["total"]

    return json.dumps({
        "hourly_breakdown": hourly,
        "total_analyzed": total,
        "total_cases": total_cases,
        "remaining": total_cases - total,
        "coverage_pct": round(total / total_cases * 100, 1) if total_cases > 0 else 0,
    }, default=str)


def _get_data_source(conn, inputs):
    dn = inputs["docket_number"]
    case = db.get_case(conn, dn)
    if not case:
        return json.dumps({"docket_number": dn, "source": "not_indexed",
                           "note": "Case not in database. Data would need to come from a live UJS search."})

    analysis = db.get_analysis(conn, dn, "docket")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT COUNT(*) as cnt FROM events WHERE docket_number = %s", (dn,))
    event_count = cur.fetchone()["cnt"]

    parsed_at = None
    if analysis and "error" not in analysis:
        source = "fully_analyzed"
        available = ["case info", "charges", "bail", "sentences", "attorneys", "judge", "docket entries"]
        note = "Full Gemini-parsed data from docket sheet PDF. All fields available."
        cur.execute("SELECT parsed_at FROM analyses WHERE docket_number = %s AND doc_type = 'docket'", (dn,))
        row = cur.fetchone()
        if row:
            parsed_at = row["parsed_at"]
    else:
        source = "metadata_only"
        available = ["case info", "status", "county", "filing date", "participant name"]
        note = "Basic info from UJS search results. No charges, bail, attorney, or judge data — case not yet analyzed by Gemini."

    if event_count > 0:
        available.append(f"{event_count} upcoming events")

    return json.dumps({
        "docket_number": dn,
        "source": source,
        "available_data": available,
        "has_events": event_count > 0,
        "last_scraped": case["last_scraped"].isoformat() if case.get("last_scraped") else None,
        "last_analyzed": parsed_at.isoformat() if parsed_at else None,
        "note": note,
    }, default=str)


def _get_case_changes(conn, inputs):
    changes = db.get_changes(conn, docket_number=inputs.get("docket_number"), limit=20)
    return json.dumps([dict(c) for c in changes], default=str) if changes else "No changes recorded."


def _get_filing_stats(conn, inputs):
    results = db.get_filing_stats(conn, county=inputs.get("county"), days=inputs.get("days", 30))
    return json.dumps([dict(r) for r in results], default=str) if results else "No filing stats available."


def _get_charge_stats(conn, inputs):
    results = db.get_charge_stats(conn, county=inputs.get("county"))
    return json.dumps([dict(r) for r in results], default=str) if results else "No charge stats available."


# ---------------------------------------------------------------------------
# News search (Gemini grounded)
# ---------------------------------------------------------------------------

def _news_search(conn, inputs):
    """Search for news using Gemini with Google Search grounding."""
    from google import genai
    from google.genai import types

    query = inputs.get("query", "")
    if not query:
        return "No search query provided."

    try:
        client = genai.Client()
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=f"Find recent local news articles about: {query}. "
                     f"Return ONLY factual summaries of what news outlets reported. "
                     f"Include the source name and date for each article. "
                     f"If no relevant news is found, say 'No relevant news coverage found.'",
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())],
            ),
        )
        text = response.text if response.text else "No results returned."

        # Extract grounding sources if available
        sources = []
        if hasattr(response, "candidates") and response.candidates:
            candidate = response.candidates[0]
            grounding = getattr(candidate, "grounding_metadata", None)
            if grounding:
                chunks = getattr(grounding, "grounding_chunks", []) or []
                for chunk in chunks:
                    web = getattr(chunk, "web", None)
                    if web:
                        sources.append({"title": getattr(web, "title", ""), "uri": getattr(web, "uri", "")})

        result = {"summary": text}
        if sources:
            result["sources"] = sources[:5]
        return json.dumps(result)

    except Exception as e:
        return f"News search error: {str(e)[:200]}"


# ---------------------------------------------------------------------------
# Handler registry
# ---------------------------------------------------------------------------

HANDLERS = {
    "lookup_docket": _lookup_docket,
    "get_case_analysis": _get_case_analysis,
    "get_person_history": _get_person_history,
    "get_docket_events": _get_docket_events,
    "search_cases": _search_cases,
    "fuzzy_name_search": _fuzzy_name_search,
    "search_by_judge": _search_by_judge,
    "search_by_attorney": _search_by_attorney,
    "search_by_charge": _search_by_charge,
    "get_todays_hearings": _get_todays_hearings,
    "get_upcoming_hearings": _get_upcoming_hearings,
    "live_search_ujs": _live_search_ujs,
    "get_stats_query": _get_stats_query,
    "run_custom_query": _run_custom_query,
    "get_analysis_coverage": _get_analysis_coverage,
    "render_chart": _render_chart,
    "get_analyzer_throughput": _get_analyzer_throughput,
    "get_system_logs": _get_system_logs,
    "get_data_source": _get_data_source,
    "get_case_changes": _get_case_changes,
    "get_filing_stats": _get_filing_stats,
    "get_charge_stats": _get_charge_stats,
    "news_search": _news_search,
}

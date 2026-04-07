"""Natural language chat endpoint — Claude + DB tools for court questions."""

import json, os
from typing import Optional

import anthropic

from ujs import db

_SYSTEM_PROMPT_TEMPLATE = """You are a PA court records assistant for Lehigh and Northampton counties.
You answer questions about court cases, hearings, charges, attorneys, and judges using the provided tools.
Always cite docket numbers. Be concise and factual. If data isn't available, say so clearly.
Dates are in MM/DD/YYYY format. Never make up case information.
Today's date is {today}.

IMPORTANT — When answering about a specific case, ALWAYS also call get_docket_events to check
for upcoming hearings/events. Include any scheduled events in your answer.

Name search strategy:
- Names in court records are stored as "Last, First Middle" (e.g. "Murphy, Kelli Anne")
- If search_cases returns 0 results, use fuzzy_name_search which handles misspellings
- If multiple people share the same name, list ALL of them with their DOB and docket numbers
  so the user can clarify which person they mean. Do not guess.
- When the user provides a DOB or other detail, use it to narrow to the right person.
- If a person has MULTIPLE cases, use get_person_history to get ALL cases with details in
  one call. Do NOT call get_case_analysis + get_docket_events separately for each case.
- If search_cases AND fuzzy_name_search both return nothing, use live_search_ujs as a last
  resort — it searches the PA court portal directly and adds results to the database.
- For hyphenated last names like "Janko-Hudson", pass the FULL hyphenated name as last_name.
  Do NOT split on hyphens. "Janko-Hudson" is one last name, not two.
"""


def _get_system_prompt():
    from datetime import datetime
    return _SYSTEM_PROMPT_TEMPLATE.format(today=datetime.now().strftime("%m/%d/%Y"))

# Tool definitions matching our DB functions
TOOLS = [
    {
        "name": "lookup_docket",
        "description": "Look up a court case by docket number (e.g. CP-39-CR-0000142-2025)",
        "input_schema": {
            "type": "object",
            "properties": {"docket_number": {"type": "string"}},
            "required": ["docket_number"],
        },
    },
    {
        "name": "get_case_analysis",
        "description": "Get full parsed analysis of a case: charges, sentences, bail, attorneys, docket entries",
        "input_schema": {
            "type": "object",
            "properties": {"docket_number": {"type": "string"}},
            "required": ["docket_number"],
        },
    },
    {
        "name": "get_person_history",
        "description": "Get ALL cases, charges, and events for a person across all their dockets. Use this instead of calling get_case_analysis multiple times when someone has several cases.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Person name"},
                "county": {"type": "string"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "get_docket_events",
        "description": "Get upcoming court events (hearings, trials, arraignments) for a specific docket number. Always call this after looking up a case to include scheduling info.",
        "input_schema": {
            "type": "object",
            "properties": {"docket_number": {"type": "string"}},
            "required": ["docket_number"],
        },
    },
    {
        "name": "search_cases",
        "description": "Search cases by participant name, county, status, type, or filing date",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Participant name"},
                "county": {"type": "string", "description": "Lehigh, Northampton, etc."},
                "case_status": {"type": "string", "description": "Active, Closed"},
                "case_type": {"type": "string", "description": "Criminal, Civil, Traffic"},
                "filed_after": {"type": "string", "description": "MM/DD/YYYY"},
                "filed_before": {"type": "string", "description": "MM/DD/YYYY"},
            },
        },
    },
    {
        "name": "search_by_judge",
        "description": "Find cases assigned to a specific judge",
        "input_schema": {
            "type": "object",
            "properties": {
                "judge_name": {"type": "string"},
                "county": {"type": "string"},
            },
            "required": ["judge_name"],
        },
    },
    {
        "name": "search_by_attorney",
        "description": "Find cases involving a specific attorney",
        "input_schema": {
            "type": "object",
            "properties": {
                "attorney_name": {"type": "string"},
                "role": {"type": "string", "description": "Public Defender, District Attorney, etc."},
                "county": {"type": "string"},
            },
            "required": ["attorney_name"],
        },
    },
    {
        "name": "search_by_charge",
        "description": "Search cases by charge statute, description, or disposition",
        "input_schema": {
            "type": "object",
            "properties": {
                "statute": {"type": "string", "description": "e.g. 3929"},
                "description": {"type": "string", "description": "e.g. DUI, Retail Theft, Assault"},
                "disposition": {"type": "string", "description": "e.g. Guilty, Dismissed"},
                "county": {"type": "string"},
            },
        },
    },
    {
        "name": "get_todays_hearings",
        "description": "Get all court hearings scheduled for today",
        "input_schema": {
            "type": "object",
            "properties": {
                "county": {"type": "string"},
                "case_type": {"type": "string", "description": "Criminal, Civil, Traffic"},
            },
        },
    },
    {
        "name": "get_upcoming_hearings",
        "description": "Get court hearings/events. Use target_date for a specific day (MM/DD/YYYY), or days for a range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "target_date": {"type": "string", "description": "Specific date MM/DD/YYYY (e.g. 04/07/2026)"},
                "days": {"type": "integer", "default": 7, "description": "Days ahead if no target_date"},
                "county": {"type": "string"},
                "case_type": {"type": "string"},
                "event_type": {"type": "string", "description": "Preliminary Hearing, Trial, Arraignment, Sentencing"},
            },
        },
    },
    {
        "name": "fuzzy_name_search",
        "description": "Fuzzy search for person names — finds close matches even with misspellings. Use this when exact search returns 0 results.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "The name to search (any format)"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "live_search_ujs",
        "description": "Search the UJS portal directly (live scrape) when a person is NOT found in the local database. Use this as a LAST RESORT after search_cases and fuzzy_name_search both fail. Slower (~15s) but searches all PA courts and analyzes the most recent case.",
        "input_schema": {
            "type": "object",
            "properties": {
                "last_name": {"type": "string", "description": "Last name — keep hyphenated names whole (e.g. Janko-Hudson)"},
                "first_name": {"type": "string", "description": "First name (optional)"},
                "county": {"type": "string", "description": "County (optional)"},
            },
            "required": ["last_name"],
        },
    },
    {
        "name": "get_stats_query",
        "description": "Get computed statistics from the database. Use this for any question involving counts, averages, percentages, trends, or comparisons. Available stat types: case_counts, bail_stats, charge_breakdown, filing_trend, hearing_counts, repeat_offenders",
        "input_schema": {
            "type": "object",
            "properties": {
                "stat_type": {"type": "string", "enum": ["case_counts", "bail_stats", "charge_breakdown", "filing_trend", "hearing_counts", "repeat_offenders"]},
                "county": {"type": "string"},
                "case_type": {"type": "string", "description": "Criminal, Traffic, Civil"},
                "days": {"type": "integer", "default": 30},
            },
            "required": ["stat_type"],
        },
    },
    {
        "name": "run_custom_query",
        "description": "Run a custom read-only SQL query against the court database for questions the other stats tools can't answer. Tables: cases (docket_number, court_type, caption, status, filing_date, county), participants (docket_number, name, dob), charges (docket_number, seq, statute, description, grade, disposition, disposition_date), bail (docket_number, bail_type, amount, status), sentences (docket_number, charge, sentence_type, duration, sentence_date), attorneys (docket_number, name, role), events (docket_number, event_type, event_status, event_date), docket_entries (docket_number, entry_date, description, filer). Write SELECT queries only.",
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SELECT query only — no INSERT/UPDATE/DELETE/DROP/ALTER"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "get_case_changes",
        "description": "Get recent changes/updates to a specific case or all cases",
        "input_schema": {
            "type": "object",
            "properties": {
                "docket_number": {"type": "string"},
            },
        },
    },
    {
        "name": "get_filing_stats",
        "description": "Get filing counts and trends by date and case type",
        "input_schema": {
            "type": "object",
            "properties": {
                "county": {"type": "string"},
                "days": {"type": "integer", "default": 30},
            },
        },
    },
    {
        "name": "get_charge_stats",
        "description": "Get the most common charges with guilty/dismissed rates",
        "input_schema": {
            "type": "object",
            "properties": {"county": {"type": "string"}},
        },
    },
]


def _execute_tool(name, inputs):
    """Execute a tool call against the DB."""
    import psycopg2.extras
    from datetime import datetime

    with db.connect() as conn:
        if name == "lookup_docket":
            case = db.get_case(conn, inputs["docket_number"])
            if not case:
                return f"No case found for: {inputs['docket_number']}"
            return json.dumps(dict(case), default=str)

        elif name == "get_case_analysis":
            analysis = db.get_analysis(conn, inputs["docket_number"], "docket")
            if not analysis:
                case = db.get_case(conn, inputs["docket_number"])
                if case:
                    return f"Case exists but not yet analyzed. Basic info: {json.dumps(dict(case), default=str)}"
                return f"No case found for: {inputs['docket_number']}"
            return json.dumps(analysis, default=str)

        elif name == "search_cases":
            results = db.search_cases(
                conn, name=inputs.get("name"), county=inputs.get("county"),
                status=inputs.get("case_status"), docket_type=inputs.get("case_type"),
                filed_after=inputs.get("filed_after"), filed_before=inputs.get("filed_before"),
                limit=20,
            )
            if not results:
                return "No cases found."
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "get_person_history":
            # Find all cases for this person
            cases = db.search_cases(conn, name=inputs["name"],
                                     county=inputs.get("county"), limit=20)
            if not cases:
                return f"No cases found for {inputs['name']}"
            history = []
            for case in cases:
                dn = case["docket_number"]
                entry = {
                    "docket_number": dn,
                    "caption": case["caption"],
                    "status": case["status"],
                    "county": case["county"],
                    "filing_date": case["filing_date"],
                }
                # Get analysis if available
                analysis = db.get_analysis(conn, dn, "docket")
                if analysis:
                    entry["charges"] = analysis.get("charges", [])
                    entry["sentences"] = analysis.get("sentences", [])
                    entry["bail"] = analysis.get("bail", {})
                    entry["judge"] = analysis.get("judge")
                # Get events
                cur3 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur3.execute("SELECT event_type, event_date, event_status FROM events WHERE docket_number = %s", (dn,))
                events = cur3.fetchall()
                if events:
                    entry["events"] = [dict(e) for e in events]
                history.append(entry)
            return json.dumps(history, default=str)

        elif name == "get_docket_events":
            import psycopg2.extras as extras
            cur2 = conn.cursor(cursor_factory=extras.RealDictCursor)
            cur2.execute("""
                SELECT event_type, event_status, event_date, event_location
                FROM events WHERE docket_number = %s ORDER BY event_date ASC
            """, (inputs["docket_number"],))
            results = cur2.fetchall()
            if not results:
                return f"No upcoming events for {inputs['docket_number']}"
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "fuzzy_name_search":
            results = db.fuzzy_name_search(conn, inputs["name"], limit=10)
            if not results:
                return f"No close matches found for: {inputs['name']}"
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "search_by_judge":
            results = db.search_by_judge(conn, inputs["judge_name"],
                                         county=inputs.get("county"), limit=20)
            if not results:
                return f"No cases found for judge: {inputs['judge_name']}"
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "search_by_attorney":
            results = db.search_by_attorney(conn, inputs["attorney_name"],
                                             role=inputs.get("role"),
                                             county=inputs.get("county"), limit=20)
            if not results:
                return f"No cases found for attorney: {inputs['attorney_name']}"
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "search_by_charge":
            results = db.search_by_charge(conn, statute=inputs.get("statute"),
                                           description=inputs.get("description"),
                                           disposition=inputs.get("disposition"),
                                           county=inputs.get("county"), limit=20)
            if not results:
                return "No charges found."
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "get_todays_hearings":
            today = datetime.now().strftime("%m/%d/%Y")
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            clauses = ["e.event_date LIKE %s"]
            params = [f"{today}%"]
            if inputs.get("county"):
                clauses.append("c.county ILIKE %s")
                params.append(inputs["county"])
            if inputs.get("case_type"):
                dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}
                code = dtype_map.get(inputs["case_type"].lower(), "")
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
                return f"No hearings today ({today})"
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "get_upcoming_hearings":
            cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            clauses = []
            params = []
            # Filter by specific date or date range
            if inputs.get("target_date"):
                clauses.append("e.event_date LIKE %s")
                params.append(f"{inputs['target_date']}%")
            else:
                # No specific date — get next N days from today
                from datetime import datetime, timedelta
                today = datetime.now()
                days = inputs.get("days", 7)
                for d in range(days):
                    pass  # just use all events, sorted by date
            if inputs.get("county"):
                clauses.append("c.county ILIKE %s")
                params.append(inputs["county"])
            if inputs.get("case_type"):
                dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-"}
                code = dtype_map.get(inputs["case_type"].lower(), "")
                if code:
                    clauses.append("c.docket_number LIKE %s")
                    params.append(f"%{code}%")
            if inputs.get("event_type"):
                clauses.append("e.event_type ILIKE %s")
                params.append(f"%{inputs['event_type']}%")
            where = " AND ".join(clauses) if clauses else "TRUE"
            params.append(200)
            cur2.execute(f"""
                SELECT e.*, c.caption, c.county FROM events e
                JOIN cases c ON e.docket_number = c.docket_number
                WHERE {where} ORDER BY e.event_date ASC LIMIT %s
            """, params)
            results = cur2.fetchall()
            if not results:
                date_str = inputs.get('target_date', 'the specified period')
                return f"No hearings found for {date_str}"
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "live_search_ujs":
            from ujs.core import search_by_name
            from ujs.modules.docket_pdf import analyze_docket
            import tempfile
            last = inputs["last_name"]
            first = inputs.get("first_name")
            # Try full hyphenated name first, then parts
            results = search_by_name(last, first=first, county=inputs.get("county"))
            if not results and "-" in last:
                # Try each part of hyphenated name
                for part in last.split("-"):
                    results = search_by_name(part.strip(), first=first, county=inputs.get("county"))
                    if results:
                        break
            if not results:
                return f"No cases found on UJS for {inputs.get('first_name', '')} {inputs['last_name']}"
            # Store discovered cases in DB — commit immediately so analyses can reference them
            with db.connect() as conn_store:
                db.upsert_cases(conn_store, results)
            # Sort by filing date — parse MM/DD/YYYY to sortable format
            def _parse_date(d):
                try:
                    parts = d.split("/")
                    return f"{parts[2]}{parts[0]}{parts[1]}"
                except Exception:
                    return "0"
            results.sort(key=lambda r: _parse_date(r.get("filing_date", "")), reverse=True)
            # Analyze active cases (up to 3) so we have real data
            analyzed = []
            for r in results:
                if len(analyzed) >= 3:
                    break
                if r.get("status", "").lower() in ("active", ""):
                    try:
                        with tempfile.TemporaryDirectory() as d:
                            analysis = analyze_docket(r["docket_number"], out_dir=d)
                        # Use separate connection for storing to avoid nesting issues
                        with db.connect() as conn2:
                            db.detect_and_store_changes(conn2, r["docket_number"], analysis)
                        analyzed.append({"docket_number": r["docket_number"], "analysis": analysis})
                    except Exception as e:
                        print(f"[live_search] Analyze error {r['docket_number']}: {e}")
            # If no active cases analyzed, try the most recent one
            if not analyzed:
                try:
                    with tempfile.TemporaryDirectory() as d:
                        analysis = analyze_docket(results[0]["docket_number"], out_dir=d)
                    with db.connect() as conn2:
                        db.detect_and_store_changes(conn2, results[0]["docket_number"], analysis)
                    analyzed.append({"docket_number": results[0]["docket_number"], "analysis": analysis})
                except Exception as e:
                    print(f"[live_search] Fallback analyze error: {e}")
            output = {
                "analyzed_cases": analyzed,
                "all_cases": [{"docket_number": r["docket_number"],
                               "caption": r["caption"],
                               "status": r["status"],
                               "county": r["county"],
                               "filing_date": r["filing_date"]}
                              for r in results[:15]],
            }
            return json.dumps(output, default=str)

        elif name == "get_stats_query":
            import psycopg2.extras as extras
            cur2 = conn.cursor(cursor_factory=extras.RealDictCursor)
            stat = inputs["stat_type"]
            county = inputs.get("county")
            county_clause = "AND c.county ILIKE %s" if county else ""
            county_params = [county] if county else []

            if stat == "case_counts":
                cur2.execute(f"""
                    SELECT county,
                        COUNT(*) as total,
                        SUM(CASE WHEN docket_number LIKE '%%-CR-%%' THEN 1 ELSE 0 END) as criminal,
                        SUM(CASE WHEN docket_number LIKE '%%-TR-%%' THEN 1 ELSE 0 END) as traffic,
                        SUM(CASE WHEN docket_number LIKE '%%-CV-%%' THEN 1 ELSE 0 END) as civil,
                        SUM(CASE WHEN docket_number LIKE '%%-NT-%%' THEN 1 ELSE 0 END) as non_traffic,
                        SUM(CASE WHEN docket_number LIKE '%%-LT-%%' THEN 1 ELSE 0 END) as landlord_tenant,
                        SUM(CASE WHEN status ILIKE '%%active%%' THEN 1 ELSE 0 END) as active,
                        SUM(CASE WHEN status ILIKE '%%closed%%' THEN 1 ELSE 0 END) as closed,
                        ROUND(SUM(CASE WHEN docket_number LIKE '%%-CR-%%' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as criminal_pct,
                        ROUND(SUM(CASE WHEN docket_number LIKE '%%-TR-%%' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as traffic_pct
                    FROM cases c WHERE county != '' {county_clause.replace('AND c.', 'AND ')}
                    GROUP BY county ORDER BY total DESC
                """, county_params)
                return json.dumps([dict(r) for r in cur2.fetchall()], default=str)

            elif stat == "bail_stats":
                amt = "REPLACE(REPLACE(b.amount, '$', ''), ',', '')::numeric"
                cur2.execute(f"""
                    SELECT b.bail_type, COUNT(*) as count,
                        MIN({amt}) as min_amount,
                        MAX({amt}) as max_amount,
                        ROUND(AVG({amt}), 2) as avg_amount
                    FROM bail b JOIN cases c ON b.docket_number = c.docket_number
                    WHERE b.amount IS NOT NULL AND b.amount != '' {"AND c.county ILIKE %s" if county else ""}
                    GROUP BY b.bail_type ORDER BY count DESC
                """, county_params)
                by_type = [dict(r) for r in cur2.fetchall()]
                cur2.execute(f"""
                    SELECT COUNT(*) as total_with_bail,
                        ROUND(AVG({amt}), 2) as overall_avg,
                        MIN({amt}) as overall_min,
                        MAX({amt}) as overall_max
                    FROM bail b JOIN cases c ON b.docket_number = c.docket_number
                    WHERE b.amount IS NOT NULL AND b.amount != '' AND {amt} > 0
                    {"AND c.county ILIKE %s" if county else ""}
                """, county_params)
                overall = dict(cur2.fetchone())
                return json.dumps({"by_type": by_type, "overall": overall}, default=str)

            elif stat == "charge_breakdown":
                cur2.execute(f"""
                    SELECT ch.description, ch.grade, COUNT(*) as count,
                        SUM(CASE WHEN ch.disposition ILIKE '%%guilty%%' THEN 1 ELSE 0 END) as guilty,
                        SUM(CASE WHEN ch.disposition ILIKE '%%dismissed%%' OR ch.disposition ILIKE '%%quashed%%' THEN 1 ELSE 0 END) as dismissed,
                        SUM(CASE WHEN ch.disposition IS NULL OR ch.disposition = '' THEN 1 ELSE 0 END) as pending,
                        ROUND(SUM(CASE WHEN ch.disposition ILIKE '%%guilty%%' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*), 0) * 100, 1) as guilty_rate
                    FROM charges ch JOIN cases c ON ch.docket_number = c.docket_number
                    WHERE ch.description != '' {county_clause}
                    GROUP BY ch.description, ch.grade ORDER BY count DESC LIMIT 20
                """, county_params)
                return json.dumps([dict(r) for r in cur2.fetchall()], default=str)

            elif stat == "filing_trend":
                days = inputs.get("days", 30)
                cur2.execute(f"""
                    SELECT filing_date, COUNT(*) as total,
                        SUM(CASE WHEN docket_number LIKE '%%-CR-%%' THEN 1 ELSE 0 END) as criminal,
                        SUM(CASE WHEN docket_number LIKE '%%-TR-%%' THEN 1 ELSE 0 END) as traffic
                    FROM cases c WHERE filing_date != '' {county_clause.replace('AND c.', 'AND ')}
                    GROUP BY filing_date ORDER BY filing_date DESC LIMIT %s
                """, county_params + [days])
                return json.dumps([dict(r) for r in cur2.fetchall()], default=str)

            elif stat == "hearing_counts":
                cur2.execute(f"""
                    SELECT e.event_type, COUNT(*) as count,
                        SUM(CASE WHEN e.event_status = 'Scheduled' THEN 1 ELSE 0 END) as scheduled,
                        SUM(CASE WHEN e.event_status = 'Continued' THEN 1 ELSE 0 END) as continued,
                        SUM(CASE WHEN e.event_status = 'Cancelled' THEN 1 ELSE 0 END) as cancelled
                    FROM events e JOIN cases c ON e.docket_number = c.docket_number
                    WHERE TRUE {county_clause}
                    GROUP BY e.event_type ORDER BY count DESC LIMIT 15
                """, county_params)
                return json.dumps([dict(r) for r in cur2.fetchall()], default=str)

            elif stat == "repeat_offenders":
                cur2.execute(f"""
                    SELECT p.name, COUNT(DISTINCT p.docket_number) as case_count,
                        STRING_AGG(DISTINCT c.county, ', ') as counties,
                        SUM(CASE WHEN c.status ILIKE '%%active%%' THEN 1 ELSE 0 END) as active_cases
                    FROM participants p JOIN cases c ON p.docket_number = c.docket_number
                    WHERE TRUE {county_clause}
                    GROUP BY p.name HAVING COUNT(DISTINCT p.docket_number) >= 5
                    ORDER BY case_count DESC LIMIT 15
                """, county_params)
                return json.dumps([dict(r) for r in cur2.fetchall()], default=str)

            return "Unknown stat type"

        elif name == "run_custom_query":
            sql = inputs["sql"].strip()
            # Safety checks
            sql_upper = sql.upper()
            blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
                       "CREATE", "GRANT", "REVOKE", "COPY", "EXECUTE", "SET ", "COMMIT",
                       "ROLLBACK", "BEGIN", ";"]
            for word in blocked:
                if word in sql_upper:
                    return f"Blocked: query contains '{word}'. Only SELECT queries allowed."
            if not sql_upper.startswith("SELECT"):
                return "Blocked: query must start with SELECT."
            # Allowed tables only
            allowed_tables = {"cases", "participants", "charges", "bail", "sentences",
                              "attorneys", "events", "docket_entries", "analyses", "change_log"}
            # Run with statement_timeout and read-only transaction
            try:
                import psycopg2.extras as extras
                cur2 = conn.cursor(cursor_factory=extras.RealDictCursor)
                cur2.execute("SET statement_timeout = '5s'")
                cur2.execute(sql)
                rows = cur2.fetchall()
                cur2.execute("RESET statement_timeout")
                if len(rows) > 100:
                    rows = rows[:100]
                return json.dumps([dict(r) for r in rows], default=str)
            except Exception as e:
                return f"Query error: {str(e)[:200]}"

        elif name == "get_case_changes":
            changes = db.get_changes(conn, docket_number=inputs.get("docket_number"), limit=20)
            if not changes:
                return "No changes recorded."
            return json.dumps([dict(c) for c in changes], default=str)

        elif name == "get_filing_stats":
            results = db.get_filing_stats(conn, county=inputs.get("county"),
                                           days=inputs.get("days", 30))
            if not results:
                return "No filing stats available."
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "get_charge_stats":
            results = db.get_charge_stats(conn, county=inputs.get("county"))
            if not results:
                return "No charge stats available."
            return json.dumps([dict(r) for r in results], default=str)

    return f"Unknown tool: {name}"


def _clean_question(question: str) -> str:
    """Fix spelling/grammar in court questions. Does NOT change person names."""
    try:
        from google import genai
        key = os.environ.get("GEMINI_API_KEY")
        if not key:
            return question
        client = genai.Client(api_key=key)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=(
                "Fix spelling and grammar in this question about Pennsylvania court cases. "
                "Fix legal terms (arraignment, preliminary hearing, disposition, etc.) "
                "and PA county names (Lehigh, Northampton, Bucks, etc.). "
                "DO NOT change person names — leave them exactly as the user typed them. "
                "Return ONLY the corrected question, nothing else.\n\n"
                f"Question: {question}"
            ),
            config={"temperature": 0},
        )
        cleaned = response.text.strip()
        if cleaned:
            return cleaned
    except Exception:
        pass
    return question


def _run_chat(client, model, question, max_rounds=10):
    """Run a tool-use chat loop with a given model. Returns (answer, tool_calls_made)."""
    import time as _t
    start_time = _t.time()
    messages = [{"role": "user", "content": question}]
    tool_calls = 0

    for _ in range(20):  # safety cap
        if _t.time() - start_time > 120:  # 2 min timeout
            return "Request timed out. Try a more specific question.", tool_calls
        response = client.messages.create(
            model=model, max_tokens=1024,
            system=_get_system_prompt(), tools=TOOLS, messages=messages,
        )

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_calls += 1
                    result = _execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            messages.append({"role": "user", "content": tool_results})
        else:
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text, tool_calls
            return "", tool_calls

    return "", tool_calls


def ask(question: str, api_key: Optional[str] = None) -> str:
    """Send a natural language question, get an answer using Claude + DB tools.
    Tries Haiku first (fast). Falls back to Sonnet if Haiku doesn't use tools."""
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("Set ANTHROPIC_API_KEY env var")

    client = anthropic.Anthropic(api_key=key)

    answer, _ = _run_chat(client, "claude-sonnet-4-20250514", question, max_rounds=10)

    return answer or "I couldn't find enough data to answer that question. Try being more specific or providing a docket number."


def ask_stream(question: str, api_key: Optional[str] = None):
    """Generator that yields answer chunks as Claude streams them.
    Sends status updates during tool calls so the user sees activity immediately."""
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        yield "Error: ANTHROPIC_API_KEY not set"
        return

    client = anthropic.Anthropic(api_key=key)
    messages = [{"role": "user", "content": question}]

    yield "Searching court records"

    for round_num in range(20):
        response = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=1024,
            system=_get_system_prompt(), tools=TOOLS, messages=messages,
        )

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_name = block.name.replace("_", " ")
                    yield f"..{tool_name}"
                    result = _execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            messages.append({"role": "user", "content": tool_results})
        else:
            yield "\n\n"
            with client.messages.stream(
                model="claude-sonnet-4-20250514", max_tokens=1024,
                system=_get_system_prompt(), tools=TOOLS, messages=messages,
            ) as stream:
                for text in stream.text_stream:
                    yield text
            return

    yield "\n\nCould not resolve answer."

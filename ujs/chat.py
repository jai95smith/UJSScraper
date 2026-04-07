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
- If search_cases AND fuzzy_name_search both return nothing, use live_search_ujs as a last
  resort — it searches the PA court portal directly and adds results to the database.
- For hyphenated last names like "Janko-Hudson", search the last part as the last name.
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
        "description": "Get upcoming court hearings/events in the next N days",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "default": 7},
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
        "description": "Search the UJS portal directly (live scrape) when a person is NOT found in the local database. Use this as a LAST RESORT after search_cases and fuzzy_name_search both fail. Slower (~5s) but searches all PA courts.",
        "input_schema": {
            "type": "object",
            "properties": {
                "last_name": {"type": "string", "description": "Last name (required)"},
                "first_name": {"type": "string", "description": "First name (optional)"},
                "county": {"type": "string", "description": "County (optional)"},
            },
            "required": ["last_name"],
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
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            clauses = []
            params = []
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
            params.append(inputs.get("days", 7) * 50)
            cur.execute(f"""
                SELECT e.*, c.caption, c.county FROM events e
                JOIN cases c ON e.docket_number = c.docket_number
                WHERE {where} ORDER BY e.event_date ASC LIMIT %s
            """, params)
            results = cur.fetchall()
            if not results:
                return "No upcoming hearings found."
            return json.dumps([dict(r) for r in results], default=str)

        elif name == "live_search_ujs":
            from ujs.core import search_by_name
            results = search_by_name(
                inputs["last_name"],
                first=inputs.get("first_name"),
                county=inputs.get("county"),
            )
            if not results:
                return f"No cases found on UJS for {inputs.get('first_name', '')} {inputs['last_name']}"
            # Store discovered cases in DB for future queries
            db.upsert_cases(conn, results)
            return json.dumps([dict(r) for r in results[:20]], default=str)

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


def _run_chat(client, model, question, max_rounds=8):
    """Run a tool-use chat loop with a given model. Returns (answer, tool_calls_made)."""
    messages = [{"role": "user", "content": question}]
    tool_calls = 0

    for _ in range(max_rounds):
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

    # Try Haiku first — fast and cheap
    answer, tool_calls = _run_chat(client, "claude-haiku-4-5-20251001", question, max_rounds=6)

    # If Haiku gave up or returned empty, escalate to Sonnet
    if not answer or tool_calls == 0:
        answer, _ = _run_chat(client, "claude-sonnet-4-20250514", question, max_rounds=8)

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

    for round_num in range(6):
        response = client.messages.create(
            model="claude-haiku-4-5-20251001", max_tokens=1024,
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
                model="claude-haiku-4-5-20251001", max_tokens=1024,
                system=_get_system_prompt(), tools=TOOLS, messages=messages,
            ) as stream:
                for text in stream.text_stream:
                    yield text
            return

    yield "\n\nCould not resolve answer."

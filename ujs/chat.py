"""Natural language chat endpoint — Claude + DB tools for court questions."""

import json, os
from typing import Optional

import anthropic

from ujs import db

SYSTEM_PROMPT = """You are a PA court records assistant for Lehigh and Northampton counties.
You answer questions about court cases, hearings, charges, attorneys, and judges using the provided tools.
Always cite docket numbers. Be concise and factual. If data isn't available, say so clearly.
Dates are in MM/DD/YYYY format. Never make up case information."""

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
    """Use Gemini Flash with Google Search grounding to fix spelling and verify names."""
    try:
        from google import genai
        from google.genai.types import Tool, GoogleSearch
        key = os.environ.get("GEMINI_API_KEY")
        if not key:
            return question
        client = genai.Client(api_key=key)
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=(
                "Fix any spelling, grammar, or typos in this question about "
                "Pennsylvania court cases. Use Google Search to verify the correct "
                "spelling of person names, places, and legal terms. "
                "Return ONLY the corrected question, nothing else.\n\n"
                f"Question: {question}"
            ),
            config={
                "temperature": 0,
                "tools": [Tool(google_search=GoogleSearch())],
            },
        )
        cleaned = response.text.strip()
        if cleaned:
            return cleaned
    except Exception:
        pass
    return question


def ask(question: str, api_key: Optional[str] = None) -> str:
    """Send a natural language question, get an answer using Claude + DB tools."""
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("Set ANTHROPIC_API_KEY env var")

    # Clean up the question first
    cleaned = _clean_question(question)

    client = anthropic.Anthropic(api_key=key)
    messages = [{"role": "user", "content": cleaned}]

    # Tool use loop — Claude may call multiple tools
    for _ in range(5):  # max 5 tool rounds
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=messages,
        )

        # Check if Claude wants to use tools
        if response.stop_reason == "tool_use":
            # Add assistant response
            messages.append({"role": "assistant", "content": response.content})

            # Execute each tool call
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    result = _execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })

            messages.append({"role": "user", "content": tool_results})
        else:
            # Final text response
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text
            return ""

    return "Could not resolve answer after multiple tool calls."

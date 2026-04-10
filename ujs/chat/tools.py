"""Tool definitions for the court records assistant."""

import os

# --- News search provider: "claude" (built-in web_search) or "gemini" (grounded search) ---
NEWS_SEARCH_PROVIDER = os.environ.get("NEWS_SEARCH_PROVIDER", "claude")

# Anthropic server-side web search — executed by the API, not by us.
_CLAUDE_WEB_SEARCH = {"type": "web_search_20250305", "name": "web_search"}

# Gemini-grounded search — client-side tool, executed by us.
_GEMINI_NEWS_SEARCH = {
    "name": "news_search",
    "description": "Search the web for news coverage about a person using Google Search. Use this AFTER retrieving court data when the query is about a specific named person. Returns relevant news articles with sources.",
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "Search query, e.g. 'Jason Krasley Lehigh County PA official oppression'"},
        },
        "required": ["query"],
    },
}


# --- Render table tool ---
RENDER_TABLE_TOOL = {
    "name": "render_table",
    "description": "Render a table in the chat UI. Use this instead of markdown tables for charges, cases, hearings, or any tabular data. Include a text summary alongside.",
    "input_schema": {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Table title"},
            "headers": {"type": "array", "items": {"type": "string"}, "description": "Column headers"},
            "rows": {"type": "array", "items": {"type": "array", "items": {"type": "string"}}, "description": "Array of rows, each row is array of cell values"},
        },
        "required": ["headers", "rows"],
    },
}

_GENERATE_QUERIES = {
    "name": "generate_news_queries",
    "description": "Generate targeted web search queries for a person based on their case data. Call this BEFORE using web_search so you get better, more specific queries. Pass the person's name, county, and a brief summary of their charges/case info.",
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Full name of the person"},
            "county": {"type": "string", "description": "County (e.g. Lehigh)"},
            "case_summary": {"type": "string", "description": "Brief summary: charges, dates, co-defendants, any notable details from court records"},
        },
        "required": ["name", "case_summary"],
    },
}


def get_news_tools():
    """Return news search tools based on provider config."""
    if NEWS_SEARCH_PROVIDER == "gemini":
        return [_GENERATE_QUERIES, _GEMINI_NEWS_SEARCH]
    # Claude mode: Claude generates queries itself and runs web_search in parallel
    return [_CLAUDE_WEB_SEARCH]

TOOLS = [
    {
        "name": "lookup_docket",
        "description": "Look up a court case by docket number. Returns case info + full analysis (charges, sentences, bail, attorneys, judge, docket entries) if available. Includes _source field indicating data completeness.",
        "input_schema": {
            "type": "object",
            "properties": {"docket_number": {"type": "string"}},
            "required": ["docket_number"],
        },
    },
    {
        "name": "get_person_history",
        "description": "Get ALL cases, charges, and events for a person across all their dockets. Use this instead of calling get_case_analysis multiple times.",
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
        "name": "search_cases",
        "description": "Search cases by participant name, county, status, type, or filing date",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Participant name"},
                "county": {"type": "string"},
                "case_status": {"type": "string", "description": "Active, Closed"},
                "case_type": {"type": "string", "description": "Criminal, Civil, Traffic"},
                "filed_after": {"type": "string", "description": "MM/DD/YYYY"},
                "filed_before": {"type": "string", "description": "MM/DD/YYYY"},
            },
        },
    },
    {
        "name": "search_by_judge",
        "description": "Find cases assigned to a specific judge. Returns cases + charge/disposition breakdown showing the judge's track record.",
        "input_schema": {
            "type": "object",
            "properties": {"judge_name": {"type": "string"}, "county": {"type": "string"}},
            "required": ["judge_name"],
        },
    },
    {
        "name": "search_by_attorney",
        "description": "Find cases involving a specific attorney. Returns cases + disposition breakdown (win/loss rate).",
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
        "description": "Search cases by charge. Uses semantic matching — pass plain English (e.g. 'kiddie porn', 'beating someone up'). Returns enriched data: defendant, DOB, judge, bail, sentence, key docket entries. Pass disposition='guilty' to filter convictions.",
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
        "name": "get_upcoming_hearings",
        "description": "Get court hearings/events with defendant name + lead charge. Use target_date for a specific day (MM/DD/YYYY), or days for a range.",
        "input_schema": {
            "type": "object",
            "properties": {
                "target_date": {"type": "string", "description": "Specific date MM/DD/YYYY"},
                "days": {"type": "integer", "default": 7},
                "county": {"type": "string"},
                "case_type": {"type": "string"},
                "event_type": {"type": "string", "description": "Preliminary Hearing, Trial, Arraignment, Sentencing"},
            },
        },
    },
    {
        "name": "fuzzy_name_search",
        "description": "Fuzzy search for person names — finds close matches even with misspellings.",
        "input_schema": {
            "type": "object",
            "properties": {"name": {"type": "string"}},
            "required": ["name"],
        },
    },
    {
        "name": "live_search_ujs",
        "description": "Search the UJS portal directly (live scrape). LAST RESORT after search_cases and fuzzy_name_search both fail. Searches all indexed counties. Provide first_name whenever possible — searches are more accurate with both names.",
        "input_schema": {
            "type": "object",
            "properties": {
                "last_name": {"type": "string", "description": "Last name — keep hyphenated names whole (e.g. Janko-Hudson)"},
                "first_name": {"type": "string", "description": "First name — always provide if known, improves accuracy"},
                "county": {"type": "string", "description": "Additional county to search beyond the default indexed counties"},
            },
            "required": ["last_name"],
        },
    },
    {
        "name": "search_docket_entries",
        "description": "Search court docket entries (filings, motions, pleas, orders) across all cases. Full-text search on descriptions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "search_text": {"type": "string", "description": "Text to search for (e.g. 'motion to suppress', 'guilty plea', 'probation violation')"},
                "county": {"type": "string"},
                "after_date": {"type": "string", "description": "Only entries after this date (MM/DD/YYYY)"},
            },
            "required": ["search_text"],
        },
    },
    {
        "name": "bail_analytics",
        "description": "Get bail statistics: average amounts, common types, by charge or county. Use for questions about bail patterns.",
        "input_schema": {
            "type": "object",
            "properties": {
                "charge_description": {"type": "string", "description": "Filter by charge (e.g. DUI, Theft, Assault)"},
                "county": {"type": "string"},
                "group_by": {"type": "string", "enum": ["charge", "county", "judge"], "description": "How to group results"},
            },
        },
    },
    {
        "name": "case_duration",
        "description": "Calculate how long cases take from filing to disposition. Filter by charge type, county, or judge.",
        "input_schema": {
            "type": "object",
            "properties": {
                "charge_description": {"type": "string", "description": "Filter by charge type (e.g. DUI, Theft)"},
                "county": {"type": "string"},
                "judge": {"type": "string"},
            },
        },
    },
    {
        "name": "run_custom_query",
        "description": """Run a custom SQL query. SELECT only. ONLY use for pure counting/aggregation — other tools return richer data. Last resort for questions no other tool handles.

FULL SCHEMA (all TEXT columns unless noted):
- cases: docket_number (PK), court_type, caption, status ('Active','Closed'), filing_date ('MM/DD/YYYY'), county ('Lehigh','Northampton'), state ('PA'), court_office
- participants: docket_number, name ('Last, First Middle'), dob ('MM/DD/YYYY')
- charges: docket_number, seq (int), statute ('18 § 2701'), description ('Simple Assault'), grade ('M1','F3','S'), disposition ('Guilty Plea','Dismissed','Proceed to Court'), disposition_date, offense_date
- bail: docket_number, bail_type ('Monetary','ROR','Unsecured'), amount ('$10,000.00' text), status ('Set','Posted')
- sentences: docket_number, charge, sentence_type ('Probation','Confinement','Fine'), duration ('1 year'), sentence_date
- attorneys: docket_number, name, role ('Public Defender','Defense','District Attorney')
- events: docket_number, event_type ('Preliminary Hearing','Trial','Sentencing'), event_status ('Scheduled','Continued','Completed'), event_date ('MM/DD/YYYY')
- docket_entries: docket_number, entry_date, description ('Motion to Suppress Filed'), filer
- analyses: docket_number, analysis (JSONB — keys: judge, defendant, case_caption, case_status)

KEY PATTERNS:
- Judge: a.analysis->>'judge' FROM analyses a
- Date filter: TO_DATE(filing_date, 'MM/DD/YYYY') >= CURRENT_DATE - INTERVAL '30 days'
- Bail math: REPLACE(REPLACE(amount, '$', ''), ',', '')::numeric (filter: amount ~ '^\\$?[0-9]')
- Case type from docket: LIKE '%%-CR-%%' (criminal), '%%-TR-%%' (traffic), '%%-CV-%%' (civil)
- Charge search: description ILIKE '%%assault%%' (use %% not %)
- Conviction: disposition ILIKE '%%guilty%%'
- Join charges to judge: charges ch JOIN analyses a ON ch.docket_number = a.docket_number
- Always LIMIT results (max 100 rows returned)""",
        "input_schema": {
            "type": "object",
            "properties": {"sql": {"type": "string", "description": "SELECT query only"}},
            "required": ["sql"],
        },
    },
    {
        "name": "get_analysis_coverage",
        "description": "Get how many cases have been fully analyzed vs total. Call this for charge/bail/attorney/judge questions.",
        "input_schema": {
            "type": "object",
            "properties": {"county": {"type": "string"}, "case_type": {"type": "string"}},
        },
    },
    RENDER_TABLE_TOOL,
    {
        "name": "render_chart",
        "description": "Render a chart in the chat UI. Use for comparisons, trends, or distributions. Include a text summary alongside.",
        "input_schema": {
            "type": "object",
            "properties": {
                "type": {"type": "string", "enum": ["bar", "line", "pie", "doughnut"]},
                "title": {"type": "string"},
                "labels": {"type": "array", "items": {"type": "string"}},
                "datasets": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string"},
                            "data": {"type": "array", "items": {"type": "number"}},
                        },
                    },
                },
            },
            "required": ["type", "title", "labels", "datasets"],
        },
    },
    # get_system_logs, get_analyzer_throughput, get_data_source removed — admin-only, accessible via run_custom_query if needed
    {
        "name": "get_case_changes",
        "description": "Get recent changes/updates to a specific case or all cases",
        "input_schema": {
            "type": "object",
            "properties": {"docket_number": {"type": "string"}},
        },
    },
]

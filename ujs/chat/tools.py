"""Tool definitions for the court records assistant."""

import os

# --- News search provider: "claude" (built-in web_search) or "gemini" (grounded search) ---
NEWS_SEARCH_PROVIDER = os.environ.get("NEWS_SEARCH_PROVIDER", "claude")

# Anthropic server-side web search — executed by the API, not by us.
_CLAUDE_WEB_SEARCH = {"type": "web_search_20250305", "name": "web_search", "max_uses": 3}

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
        "name": "get_docket_events",
        "description": "Get upcoming court events for a specific docket number. Always call this after looking up a case.",
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
        "description": "Find cases assigned to a specific judge",
        "input_schema": {
            "type": "object",
            "properties": {"judge_name": {"type": "string"}, "county": {"type": "string"}},
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
        "description": "Search the UJS portal directly (live scrape). LAST RESORT after search_cases and fuzzy_name_search both fail. Always searches both Lehigh and Northampton counties. Provide first_name whenever possible — searches are more accurate with both names.",
        "input_schema": {
            "type": "object",
            "properties": {
                "last_name": {"type": "string", "description": "Last name — keep hyphenated names whole (e.g. Janko-Hudson)"},
                "first_name": {"type": "string", "description": "First name — always provide if known, improves accuracy"},
                "county": {"type": "string", "description": "Additional county to search beyond Lehigh/Northampton"},
            },
            "required": ["last_name"],
        },
    },
    {
        "name": "get_stats_query",
        "description": "Get computed statistics. Types: case_counts, bail_stats, charge_breakdown, filing_trend, hearing_counts, repeat_offenders, judge_performance",
        "input_schema": {
            "type": "object",
            "properties": {
                "stat_type": {"type": "string", "enum": ["case_counts", "bail_stats", "charge_breakdown", "filing_trend", "hearing_counts", "repeat_offenders", "judge_performance"]},
                "county": {"type": "string"},
                "case_type": {"type": "string"},
                "days": {"type": "integer", "default": 30},
            },
            "required": ["stat_type"],
        },
    },
    {
        "name": "run_custom_query",
        "description": """Run a custom read-only SQL query. SELECT only. Schema:
- cases: docket_number (PK), court_type, caption, status, filing_date, county
- participants: docket_number, name, dob
- charges: docket_number, seq, statute, description, grade, disposition, disposition_date, offense_date
- bail: docket_number, bail_type, amount, status
- sentences: docket_number, charge, sentence_type, duration, sentence_date
- attorneys: docket_number, name, role
- events: docket_number, event_type, event_status, event_date
- docket_entries: docket_number, entry_date, description, filer
- analyses: docket_number, analysis (JSONB with keys: judge, defendant, case_caption, case_status)

Key patterns:
- Get judge: a.analysis->>'judge' FROM analyses a
- Date compare: TO_DATE(filing_date, 'MM/DD/YYYY') >= CURRENT_DATE - INTERVAL '30 days'
- Bail math: REPLACE(REPLACE(amount, '$', ''), ',', '')::numeric
- Case type: docket_number LIKE '%-CR-%' (criminal), '%-TR-%' (traffic), '%-CV-%' (civil)
- Join charges to judge: charges ch JOIN analyses a ON ch.docket_number = a.docket_number
- ILIKE for fuzzy text match, %% for wildcards inside queries""",
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
    {
        "name": "get_system_logs",
        "description": "Get system operation logs — scraper runs, analyzer events, errors. Use for debugging or system health questions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "component": {"type": "string", "description": "Filter: scraper, analyzer, or all"},
                "errors_only": {"type": "boolean", "default": False},
                "hours": {"type": "integer", "default": 24},
                "limit": {"type": "integer", "default": 50},
            },
        },
    },
    {
        "name": "get_analyzer_throughput",
        "description": "Get how many dockets were analyzed per hour over a time period. Use this for questions about analysis rate, throughput, or system activity.",
        "input_schema": {
            "type": "object",
            "properties": {
                "hours": {"type": "integer", "default": 24, "description": "Hours to look back"},
            },
        },
    },
    {
        "name": "get_data_source",
        "description": "Check where data comes from for a docket — metadata only, fully analyzed, or not indexed. Call this before answering about a specific case so you can tell the user the data source and completeness.",
        "input_schema": {
            "type": "object",
            "properties": {"docket_number": {"type": "string"}},
            "required": ["docket_number"],
        },
    },
    {
        "name": "get_case_changes",
        "description": "Get recent changes/updates to a specific case or all cases",
        "input_schema": {
            "type": "object",
            "properties": {"docket_number": {"type": "string"}},
        },
    },
    {
        "name": "get_filing_stats",
        "description": "Get filing counts and trends by date and case type",
        "input_schema": {
            "type": "object",
            "properties": {"county": {"type": "string"}, "days": {"type": "integer", "default": 30}},
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

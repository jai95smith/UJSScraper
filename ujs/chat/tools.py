"""Tool definitions for the court records assistant."""

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

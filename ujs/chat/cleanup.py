"""Gemini Flash structured output — all classification and cleanup calls."""

import json, os


def _gemini_json(prompt, schema, retries=2, timeout=10):
    """Call Gemini Flash with forced JSON schema. Validates response matches schema.
    Returns parsed dict or None on failure. Retries on parse/validation errors.
    Timeout in seconds per attempt."""
    from google import genai
    from google.genai import types

    client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"))
    required = schema.get("required", [])
    props = schema.get("properties", {})

    for attempt in range(retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_json_schema=schema,
                    thinking_config=types.ThinkingConfig(thinking_budget=0),
                    http_options=types.HttpOptions(timeout=timeout * 1000),  # ms
                ),
            )
            result = json.loads(response.text)

            # Validate required fields exist and have correct types
            valid = True
            for field in required:
                if field not in result:
                    valid = False
                    break
                expected_type = props.get(field, {}).get("type")
                val = result[field]
                if expected_type == "boolean" and not isinstance(val, bool):
                    valid = False
                elif expected_type == "string" and not isinstance(val, str):
                    valid = False
                elif expected_type == "array" and not isinstance(val, list):
                    valid = False

            if valid:
                return result
            print(f"[gemini_json] Validation failed attempt {attempt + 1}: {result}")

        except (json.JSONDecodeError, Exception) as e:
            print(f"[gemini_json] Error attempt {attempt + 1}: {e}")

    return None


# ---------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------

_NEWS_SCHEMA = {
    "type": "object",
    "properties": {
        "has_news": {"type": "boolean", "description": "True if relevant news was found"},
        "summary": {"type": "string", "description": "1-2 paragraph factual summary of news coverage. No preamble, no speculation. Just the facts — who, what, when, where."},
    },
    "required": ["has_news", "summary"],
}

_PERSON_CLASSIFY_SCHEMA = {
    "type": "object",
    "properties": {
        "is_person_query": {"type": "boolean", "description": "True if asking about a specific named individual. False for bulk queries, stats, docket lookups, generic searches."},
        "name": {"type": "string", "description": "Person's full name (if is_person_query is true, otherwise empty)"},
        "county": {"type": "string", "description": "County (e.g. Lehigh) if known, otherwise empty"},
        "charges": {"type": "string", "description": "Key charges in plain English, comma separated (if available)"},
        "details": {"type": "string", "description": "Any notable details (employer, co-defendants, role)"},
    },
    "required": ["is_person_query", "name"],
}


# ---------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------

def classify_and_extract(question, court_answer):
    """Classify if person query AND extract context in one Gemini call.
    Returns (is_person, context_string) or (False, None) on failure."""
    # Quick reject: court answer found nothing
    al = court_answer.lower()
    if any(p in al for p in ["no case", "not found", "no result", "couldn't find"]):
        return False, None

    result = _gemini_json(
        f"Analyze this court records question and answer.\n"
        f"1. Is it about a specific named person (first+last name)? "
        f"Answer false for docket lookups, bulk queries, stats, generic searches.\n"
        f"2. If yes, extract their name, county, key charges, and notable details.\n\n"
        f"Question: {question}\n\nCourt answer:\n{court_answer[:1500]}",
        _PERSON_CLASSIFY_SCHEMA,
    )
    if result is not None:
        is_person = result["is_person_query"]
        if is_person and result.get("name"):
            parts = [f"Person: {result['name']}"]
            if result.get("county"):
                parts.append(f"Location: {result['county']} County, PA")
            if result.get("charges"):
                parts.append(f"Charges: {result['charges']}")
            if result.get("details"):
                parts.append(f"Details: {result['details']}")
            return True, "\n".join(parts)
        return is_person, None

    # Fallback: regex check
    import re
    is_person = bool(re.search(r'[A-Z][a-z]+[\s,]+[A-Z][a-z]+', question.strip()))
    return is_person, f"Question: {question}\n\nCourt records answer:\n{court_answer[:500]}" if is_person else None


def is_person_query(question, court_answer):
    """Legacy wrapper — returns bool only."""
    is_person, _ = classify_and_extract(question, court_answer)
    return is_person


def structure_news(raw_text):
    """Clean raw Claude news output into structured text.
    Returns clean summary string or None if no news."""
    result = _gemini_json(
        f"Extract the factual news summary from this text. Remove any preamble, "
        f"headers, narration ('I searched...', 'Let me...'), and speculation. "
        f"Keep only the factual reporting — who, what, when, where.\n\n{raw_text}",
        _NEWS_SCHEMA,
    )
    if result is not None:
        if result["has_news"] and result["summary"].strip():
            return result["summary"].strip()
        return None

    # Fallback
    return _fallback_cleanup(raw_text)


def _fallback_cleanup(raw_text):
    """Regex fallback if Gemini is unavailable."""
    clean = raw_text.strip()
    for prefix in ["## News Coverage\n", "**News Coverage**\n", "### News Coverage\n"]:
        if clean.startswith(prefix):
            clean = clean[len(prefix):].strip()
    lines = clean.split("\n")
    while lines and any(lines[0].lower().startswith(p) for p in [
        "i'll ", "i will ", "let me ", "now ", "here ", "searching", "based on"
    ]):
        lines.pop(0)
    clean = "\n".join(lines).strip()
    return clean if clean else None

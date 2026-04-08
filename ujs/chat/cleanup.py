"""Gemini Flash structured output — all classification and cleanup calls."""

import json


def _gemini_json(prompt, schema, retries=2, timeout=10):
    """Call Gemini Flash with forced JSON schema. Validates response matches schema.
    Returns parsed dict or None on failure. Retries on parse/validation errors.
    Timeout in seconds per attempt."""
    from google import genai
    from google.genai import types

    client = genai.Client()
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

_PERSON_CHECK_SCHEMA = {
    "type": "object",
    "properties": {
        "is_person_query": {"type": "boolean", "description": "True if asking about a specific named individual. False for bulk queries, stats, docket lookups, generic searches."},
    },
    "required": ["is_person_query"],
}


# ---------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------

def is_person_query(question, court_answer):
    """Classify whether this question is about a specific named person.
    Returns True if news search should run."""
    # Quick reject: court answer found nothing
    al = court_answer.lower()
    if any(p in al for p in ["no case", "not found", "no result", "couldn't find"]):
        return False

    result = _gemini_json(
        f"Does this question ask about a specific named person? "
        f"Answer true ONLY if the question itself contains a person's name "
        f"(first and last name, or last name comma first name). "
        f"Answer false for: docket number lookups, bulk queries, stats, "
        f"generic searches, system questions.\n\n"
        f"Question: {question}",
        _PERSON_CHECK_SCHEMA,
    )
    if result is not None:
        return result["is_person_query"]

    # Fallback: capitalized two-word name
    import re
    return bool(re.search(r'[A-Z][a-z]+[\s,]+[A-Z][a-z]+', question.strip()))


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

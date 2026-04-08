"""Gemini-powered output cleanup for structured, preamble-free responses."""

import json

_NEWS_SCHEMA = {
    "type": "object",
    "properties": {
        "has_news": {"type": "boolean", "description": "True if relevant news was found"},
        "summary": {"type": "string", "description": "1-2 paragraph factual summary of news coverage. No preamble, no speculation. Just facts."},
    },
    "required": ["has_news", "summary"],
}


def structure_news(raw_text):
    """Use Gemini Flash to clean raw Claude news output into structured text.
    Returns clean summary string or None if no news."""
    try:
        from google import genai
        from google.genai import types

        client = genai.Client()
        response = client.models.generate_content(
            model="gemini-2.5-flash",
            contents=f"Extract the factual news summary from this text. Remove any preamble, "
                     f"headers, narration ('I searched...', 'Let me...'), and speculation. "
                     f"Keep only the factual reporting — who, what, when, where.\n\n{raw_text}",
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_json_schema=_NEWS_SCHEMA,
                thinking_config=types.ThinkingConfig(thinking_budget=0),
            ),
        )
        result = json.loads(response.text)
        if result.get("has_news") and result.get("summary", "").strip():
            return result["summary"].strip()
    except Exception as e:
        print(f"[structure_news] Gemini error: {e}")
        return _fallback_cleanup(raw_text)
    return None


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

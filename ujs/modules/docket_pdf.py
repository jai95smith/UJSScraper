#!/usr/bin/env python3
"""Docket PDF downloader and text extractor."""

import json, os, re

from google import genai

from ujs.core import search_by_docket, search_by_name, download_pdf

GEMINI_SCHEMA = {
    "type": "object",
    "properties": {
        "docket_number": {"type": "string"},
        "case_caption": {"type": "string"},
        "court": {"type": "string"},
        "county": {"type": "string"},
        "case_status": {"type": "string"},
        "filing_date": {"type": "string"},
        "judge": {"type": "string"},
        "defendant": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "dob": {"type": "string"},
                "address": {"type": "string"},
            },
        },
        "charges": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "seq": {"type": "integer"},
                    "statute": {"type": "string"},
                    "description": {"type": "string"},
                    "grade": {"type": "string"},
                    "offense_date": {"type": "string"},
                    "otn": {"type": "string"},
                    "disposition": {"type": "string"},
                    "disposition_date": {"type": "string"},
                },
            },
        },
        "bail": {
            "type": "object",
            "properties": {
                "type": {"type": "string"},
                "amount": {"type": "string"},
                "status": {"type": "string"},
                "posting_date": {"type": "string"},
            },
        },
        "sentences": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "charge": {"type": "string"},
                    "sentence_type": {"type": "string"},
                    "duration": {"type": "string"},
                    "conditions": {"type": "string"},
                    "sentence_date": {"type": "string"},
                },
            },
        },
        "attorneys": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "role": {"type": "string"},
                },
            },
        },
        "docket_entries": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "date": {"type": "string"},
                    "description": {"type": "string"},
                    "filer": {"type": "string"},
                },
            },
        },
    },
}

GEMINI_PROMPT = """Extract structured data from this PA court docket sheet. Copy values exactly as they appear in the document — do not rephrase, reformat, summarize, or abbreviate any field values. Every string value must be a verbatim copy from the source text.

FORMAT RULES:
- ALL dates: MM/DD/YYYY (e.g. 01/08/2025). Never YYYY-MM-DD.
- ALL currency: include $ and commas (e.g. $7,500.00).
- Statute numbers: exact as printed (e.g. "18 § 3929 §§ A1"), no added whitespace.
- Sentence durations: copy verbatim from the document (e.g. "Min of 3.00 Months Max of 23.00 Months 29.00 Days").
- Do not add words, reorder, or paraphrase any values."""

SUMMARY_SCHEMA = {
    "type": "object",
    "properties": {
        "person": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "dob": {"type": "string"},
                "sex": {"type": "string"},
                "address": {"type": "string"},
                "eyes": {"type": "string"},
                "hair": {"type": "string"},
                "race": {"type": "string"},
                "aliases": {"type": "array", "items": {"type": "string"}},
            },
        },
        "cases": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "docket_number": {"type": "string"},
                    "county": {"type": "string"},
                    "status": {"type": "string"},
                    "otn": {"type": "string"},
                    "arrest_date": {"type": "string"},
                    "disposition_date": {"type": "string"},
                    "disposition_judge": {"type": "string"},
                    "defense_attorney": {"type": "string"},
                    "charges": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "seq": {"type": "integer"},
                                "statute": {"type": "string"},
                                "grade": {"type": "string"},
                                "description": {"type": "string"},
                                "disposition": {"type": "string"},
                                "sentence_date": {"type": "string"},
                                "sentence_type": {"type": "string"},
                                "sentence_duration": {"type": "string"},
                            },
                        },
                    },
                },
            },
        },
    },
}

SUMMARY_PROMPT = """Extract structured data from this PA Court Summary report. This is a multi-case summary for one person across potentially multiple counties. Extract the person's info and EVERY case listed with all charges, dispositions, and sentences. Copy values exactly as they appear in the document — do not rephrase, reformat, summarize, or abbreviate any field values.

FORMAT RULES:
- ALL dates: MM/DD/YYYY (e.g. 01/08/2025). Never YYYY-MM-DD.
- Statute numbers: exact as printed (e.g. "18 § 3929 §§ A1"), no added whitespace.
- Sentence durations: copy verbatim (e.g. "Min: 1 Year(s) Max: 3 Year(s)").
- Do not add words, reorder, or paraphrase any values.
- Include every case even if it has minimal info."""


def fetch_docket_pdf(docket_number, out_dir=".", doc_type="docket"):
    """Search for a docket, download its PDF, return the file path.
    doc_type: 'docket' for docket sheet, 'summary' for court summary.
    """
    results = search_by_docket(docket_number)
    if not results:
        raise ValueError(f"No results for docket: {docket_number}")

    r = results[0]
    url_key = "court_summary_url" if doc_type == "summary" else "docket_sheet_url"
    url = r.get(url_key)
    if not url:
        raise ValueError(f"No {doc_type} URL for: {docket_number}")

    os.makedirs(out_dir, exist_ok=True)
    fn = os.path.join(out_dir, f"{docket_number.replace('-','_')}_{doc_type}.pdf")
    download_pdf(url, fn)
    return fn


def extract_text(pdf_path):
    """Extract text from a docket sheet PDF. Returns full text string."""
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            return "\n".join(page.extract_text() or "" for page in pdf.pages)
    except ImportError:
        import subprocess
        result = subprocess.run(["pdftotext", "-layout", pdf_path, "-"],
                                capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"pdftotext failed: {result.stderr}")
        return result.stdout


def parse_charges(text):
    """Extract charges from docket sheet text."""
    charges = []
    in_charges = False
    for line in text.splitlines():
        if "CHARGES" in line.upper() or "Seq No" in line:
            in_charges = True
            continue
        if in_charges:
            if line.strip() == "" or "DISPOSITION" in line.upper() or "BAIL" in line.upper():
                in_charges = False
                continue
            charges.append(line.strip())
    return charges


def parse_dispositions(text):
    """Extract disposition info from docket sheet text."""
    dispositions = []
    in_disp = False
    for line in text.splitlines():
        if "DISPOSITION" in line.upper() and "SENTENCING" not in line.upper():
            in_disp = True
            continue
        if in_disp:
            if line.strip() == "" or "SENTENCE" in line.upper() or "COMMONWEALTH" in line.upper():
                in_disp = False
                continue
            dispositions.append(line.strip())
    return dispositions


def parse_bail(text):
    """Extract bail information from docket sheet text."""
    bail_info = []
    in_bail = False
    for line in text.splitlines():
        if "BAIL" in line.upper() and "ACTION" in line.upper():
            in_bail = True
            continue
        if in_bail:
            if line.strip() == "" or "CHARGES" in line.upper():
                in_bail = False
                continue
            bail_info.append(line.strip())
    return bail_info


def _gemini_extract(text, prompt, schema, api_key=None, docket_number=None):
    """Send text to Gemini Flash with a given schema. Tracks token costs."""
    key = api_key or os.environ.get("GEMINI_API_KEY")
    if not key:
        raise RuntimeError("Set GEMINI_API_KEY env var or pass api_key")

    client = genai.Client(api_key=key)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt + "\n\n" + text,
        config={
            "temperature": 0,
            "response_mime_type": "application/json",
            "response_schema": schema,
        },
    )
    result = json.loads(response.text)
    _clean_result(result)

    # Track token usage and cost
    try:
        usage = response.usage_metadata
        if usage:
            _log_cost(
                docket_number=docket_number,
                model="gemini-2.5-flash",
                input_tokens=getattr(usage, 'prompt_token_count', 0) or 0,
                output_tokens=getattr(usage, 'candidates_token_count', 0) or 0,
                thinking_tokens=getattr(usage, 'thoughts_token_count', 0) or 0,
                operation="analyze",
            )
    except Exception:
        pass

    return result


# Gemini 2.5 Flash pricing (per 1M tokens, standard tier)
_PRICING = {
    "input": 0.30 / 1_000_000,
    "output": 2.50 / 1_000_000,   # includes thinking tokens
    "thinking": 2.50 / 1_000_000,  # same rate as output
}


def _log_cost(docket_number, model, input_tokens, output_tokens, thinking_tokens, operation):
    """Log API cost to database."""
    cost = (input_tokens * _PRICING["input"] +
            output_tokens * _PRICING["output"] +
            thinking_tokens * _PRICING["thinking"])
    try:
        from ujs import db
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO api_costs (docket_number, model, input_tokens, output_tokens, thinking_tokens, cost_usd, operation)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (docket_number, model, input_tokens, output_tokens, thinking_tokens, cost, operation))
    except Exception:
        pass


def _clean_result(obj):
    """Post-process Gemini output: fix nulls, deduplicate arrays."""
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            if v == "null" or v == "None":
                obj[k] = None
            elif isinstance(v, list):
                # Deduplicate list of dicts by converting to tuples
                if v and isinstance(v[0], dict):
                    seen = set()
                    deduped = []
                    for item in v:
                        key = tuple(sorted(item.items()))
                        if key not in seen:
                            seen.add(key)
                            deduped.append(item)
                    obj[k] = deduped
                _clean_result(obj[k])
            elif isinstance(v, dict):
                _clean_result(v)
    elif isinstance(obj, list):
        for item in obj:
            _clean_result(item)


def parse_with_gemini(text, api_key=None, docket_number=None):
    """Parse a docket sheet with Gemini."""
    return _gemini_extract(text, GEMINI_PROMPT, GEMINI_SCHEMA, api_key, docket_number=docket_number)


def parse_summary_with_gemini(text, api_key=None):
    """Parse a court summary with Gemini."""
    return _gemini_extract(text, SUMMARY_PROMPT, SUMMARY_SCHEMA, api_key)


def analyze_docket(docket_number, out_dir=".", use_gemini=True, api_key=None):
    """Full pipeline: fetch docket sheet PDF, extract text, parse."""
    pdf_path = fetch_docket_pdf(docket_number, out_dir, doc_type="docket")
    text = extract_text(pdf_path)

    if use_gemini:
        try:
            parsed = parse_with_gemini(text, api_key=api_key, docket_number=docket_number)
            parsed["pdf_path"] = pdf_path
            return parsed
        except Exception as e:
            print(f"Gemini parsing failed ({e}), falling back to regex")

    return {
        "docket_number": docket_number,
        "pdf_path": pdf_path,
        "charges": parse_charges(text),
        "dispositions": parse_dispositions(text),
        "bail": parse_bail(text),
        "full_text": text,
    }


def analyze_summary(docket_number, out_dir=".", api_key=None):
    """Fetch court summary PDF and parse with Gemini. Returns full case history."""
    pdf_path = fetch_docket_pdf(docket_number, out_dir, doc_type="summary")
    text = extract_text(pdf_path)
    parsed = parse_summary_with_gemini(text, api_key=api_key)
    parsed["pdf_path"] = pdf_path
    return parsed


def main():
    import argparse, json
    p = argparse.ArgumentParser(description="UJS Docket PDF Analyzer")
    p.add_argument("docket", help="Docket number to analyze")
    p.add_argument("--out-dir", default="./pdfs", help="Output directory for PDFs")
    p.add_argument("--text-only", action="store_true", help="Just print extracted text")
    p.add_argument("--json", action="store_true", dest="as_json", help="Output JSON")
    p.add_argument("--summary", action="store_true", help="Analyze court summary instead of docket sheet")
    p.add_argument("--no-ai", action="store_true", help="Skip Gemini, use regex parsing only")
    args = p.parse_args()

    if args.text_only:
        doc = "summary" if args.summary else "docket"
        pdf_path = fetch_docket_pdf(args.docket, args.out_dir, doc_type=doc)
        print(extract_text(pdf_path))
    elif args.summary:
        result = analyze_summary(args.docket, args.out_dir)
        out = {k: v for k, v in result.items() if k != "pdf_path"}
        print(json.dumps(out, indent=2))
    else:
        result = analyze_docket(args.docket, args.out_dir, use_gemini=not args.no_ai)
        out = {k: v for k, v in result.items() if k not in ("full_text", "pdf_path")}
        print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

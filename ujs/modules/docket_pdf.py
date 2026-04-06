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

GEMINI_PROMPT = """Extract all structured data from this PA court docket sheet.
Return every field you can find. For charges, include the disposition if present.
For docket entries, include every entry with date and description.
Be precise with statute numbers, dates, and names. Omit fields you cannot find.

IMPORTANT FORMAT RULES:
- ALL dates MUST be MM/DD/YYYY format (e.g. 01/08/2025, 12/19/2014). Never use YYYY-MM-DD.
- ALL currency amounts MUST include $ and commas (e.g. $7,500.00, $15,000.00).
- Statute numbers should be clean (e.g. "18 § 3929 §§ A1"), no extra whitespace or newlines."""


def fetch_docket_pdf(docket_number, out_dir="."):
    """Search for a docket, download its PDF, return the file path."""
    results = search_by_docket(docket_number)
    if not results:
        raise ValueError(f"No results for docket: {docket_number}")

    r = results[0]
    url = r.get("docket_sheet_url")
    if not url:
        raise ValueError(f"No docket sheet URL for: {docket_number}")

    os.makedirs(out_dir, exist_ok=True)
    fn = os.path.join(out_dir, f"{docket_number.replace('-','_')}_docket.pdf")
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


def parse_with_gemini(text, api_key=None):
    """Send docket text to Gemini Flash for structured extraction."""
    key = api_key or os.environ.get("GEMINI_API_KEY")
    if not key:
        raise RuntimeError("Set GEMINI_API_KEY env var or pass api_key")

    client = genai.Client(api_key=key)
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=GEMINI_PROMPT + "\n\n" + text,
        config={
            "response_mime_type": "application/json",
            "response_schema": GEMINI_SCHEMA,
        },
    )
    return json.loads(response.text)


def analyze_docket(docket_number, out_dir=".", use_gemini=True, api_key=None):
    """Full pipeline: fetch PDF, extract text, parse with Gemini or regex."""
    pdf_path = fetch_docket_pdf(docket_number, out_dir)
    text = extract_text(pdf_path)

    if use_gemini:
        try:
            parsed = parse_with_gemini(text, api_key=api_key)
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


def main():
    import argparse, json
    p = argparse.ArgumentParser(description="UJS Docket PDF Analyzer")
    p.add_argument("docket", help="Docket number to analyze")
    p.add_argument("--out-dir", default="./pdfs", help="Output directory for PDFs")
    p.add_argument("--text-only", action="store_true", help="Just print extracted text")
    p.add_argument("--json", action="store_true", dest="as_json", help="Output JSON")
    p.add_argument("--no-ai", action="store_true", help="Skip Gemini, use regex parsing only")
    args = p.parse_args()

    if args.text_only:
        pdf_path = fetch_docket_pdf(args.docket, args.out_dir)
        print(extract_text(pdf_path))
    else:
        result = analyze_docket(args.docket, args.out_dir, use_gemini=not args.no_ai)
        out = {k: v for k, v in result.items() if k not in ("full_text", "pdf_path")}
        if args.as_json:
            print(json.dumps(out, indent=2))
        else:
            print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()

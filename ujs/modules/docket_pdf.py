#!/usr/bin/env python3
"""Docket PDF downloader and text extractor."""

import os, re

from ujs.core import search_by_docket, search_by_name, download_pdf


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


def analyze_docket(docket_number, out_dir="."):
    """Full pipeline: fetch PDF, extract text, parse key sections."""
    pdf_path = fetch_docket_pdf(docket_number, out_dir)
    text = extract_text(pdf_path)
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
    args = p.parse_args()

    if args.text_only:
        pdf_path = fetch_docket_pdf(args.docket, args.out_dir)
        print(extract_text(pdf_path))
    else:
        result = analyze_docket(args.docket, args.out_dir)
        if args.as_json:
            out = {k: v for k, v in result.items() if k != "full_text"}
            print(json.dumps(out, indent=2))
        else:
            print(f"Docket: {result['docket_number']}")
            print(f"PDF:    {result['pdf_path']}")
            print(f"\n--- CHARGES ({len(result['charges'])}) ---")
            for c in result["charges"]:
                print(f"  {c}")
            print(f"\n--- DISPOSITIONS ({len(result['dispositions'])}) ---")
            for d in result["dispositions"]:
                print(f"  {d}")
            print(f"\n--- BAIL ({len(result['bail'])}) ---")
            for b in result["bail"]:
                print(f"  {b}")


if __name__ == "__main__":
    main()

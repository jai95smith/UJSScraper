#!/usr/bin/env python3
"""PA UJS Portal Case Search — lightweight CLI scraper."""

import argparse, re, sys
from html import unescape
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://ujsportal.pacourts.us"
SEARCH_URL = f"{BASE}/CaseSearch"

FIELDS = [
    "docket_number", "court_type", "caption", "status", "filing_date",
    "participant", "dob", "county", "court_office", "otn", "complaint", "incident",
]


def get_session():
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    r = s.get(SEARCH_URL)
    r.raise_for_status()
    m = re.search(r'name="__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"', r.text)
    if not m:
        sys.exit("Could not find CSRF token")
    return s, m.group(1)


def search(session, token, **kwargs):
    data = {"__RequestVerificationToken": token}
    data.update(kwargs)
    r = session.post(SEARCH_URL, data=data)
    r.raise_for_status()
    return parse_results(r.text)


def parse_results(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tbody tr")
    results = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 13:
            continue
        # first two cells are hidden sort indices
        vals = [unescape(c.get_text(strip=True)) for c in cells[2:14]]
        rec = dict(zip(FIELDS, vals))
        # extract PDF links
        for a in row.find_all("a", href=True):
            href = a["href"]
            if "DocketSheet" in href:
                rec["docket_sheet_url"] = urljoin(BASE, unescape(href))
            elif "CourtSummary" in href:
                rec["court_summary_url"] = urljoin(BASE, unescape(href))
        results.append(rec)
    return results


def download_pdf(session, url, filename):
    r = session.get(url)
    r.raise_for_status()
    with open(filename, "wb") as f:
        f.write(r.content)
    print(f"Saved: {filename} ({len(r.content)} bytes)")


def print_results(results):
    if not results:
        print("No results found.")
        return
    print(f"\n{'='*80}")
    print(f"Found {len(results)} result(s)")
    print(f"{'='*80}\n")
    for i, r in enumerate(results, 1):
        print(f"[{i}] {r['docket_number']}  |  {r['caption']}")
        print(f"    Status: {r['status']}  |  Filed: {r['filing_date']}  |  County: {r['county']}")
        print(f"    Participant: {r['participant']}  |  DOB: {r['dob']}")
        if r.get("otn"):
            print(f"    OTN: {r['otn']}  |  Complaint: {r.get('complaint','')}")
        print()


def main():
    p = argparse.ArgumentParser(description="Search PA UJS Portal")
    p.add_argument("--last", help="Last name")
    p.add_argument("--first", help="First name")
    p.add_argument("--docket", help="Docket number (e.g. CP-39-CR-0001378-1989)")
    p.add_argument("--county", help="County name (e.g. Lehigh)")
    p.add_argument("--type", help="Docket type: Criminal, Civil, etc.", dest="docket_type")
    p.add_argument("--dob", help="Date of birth (MM/DD/YYYY)")
    p.add_argument("--otn", help="OTN number")
    p.add_argument("--download", type=int, metavar="N", help="Download docket sheet PDF for result N")
    p.add_argument("--json", action="store_true", help="Output JSON")
    args = p.parse_args()

    if not any([args.last, args.docket, args.otn]):
        p.error("Provide at least --last, --docket, or --otn")

    print("Connecting to UJS Portal...")
    session, token = get_session()

    params = {}
    if args.docket:
        params["SearchBy"] = "DocketNumber"
        params["DocketNumber"] = args.docket
    elif args.otn:
        params["SearchBy"] = "OTN"
        params["OTN"] = args.otn
    else:
        params["SearchBy"] = "ParticipantName"
        params["ParticipantLastName"] = args.last
        if args.first:
            params["ParticipantFirstName"] = args.first
        if args.dob:
            params["ParticipantDateOfBirth"] = args.dob

    if args.county:
        params["County"] = args.county
    if args.docket_type:
        params["DocketType"] = args.docket_type

    results = search(session, token, **params)

    if args.json:
        import json
        print(json.dumps(results, indent=2))
    else:
        print_results(results)

    if args.download:
        idx = args.download - 1
        if 0 <= idx < len(results):
            r = results[idx]
            url = r.get("docket_sheet_url")
            if url:
                fn = f"{r['docket_number'].replace('-','_')}_docket.pdf"
                download_pdf(session, url, fn)
            else:
                print("No docket sheet URL for this result.")
        else:
            print(f"Invalid result number. Choose 1-{len(results)}")


if __name__ == "__main__":
    main()

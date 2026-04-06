#!/usr/bin/env python3
"""PA UJS Portal Case Search — lightweight CLI scraper."""

import argparse, re, sys
from datetime import datetime, timedelta
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


def search_by_date(start_date, end_date, county=None, docket_type=None):
    """Date-filed search requires Playwright (results are JS-rendered)."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(SEARCH_URL)
        page.wait_for_load_state("networkidle")

        page.select_option("select[data-aopc-control-to-find]", "DateFiled")
        page.wait_for_timeout(500)
        page.fill("input[name=FiledStartDate]", start_date)
        page.fill("input[name=FiledEndDate]", end_date)

        if county or docket_type:
            page.check("input[name=AdvanceSearch]")
            page.wait_for_timeout(1000)
            for sel in [("County", county), ("Docket Type", docket_type)]:
                if sel[1]:
                    try:
                        page.select_option(f'select[title="{sel[0]}"]:visible', sel[1], timeout=3000)
                    except Exception:
                        pass  # filter client-side instead

        page.click('button:has-text("Search")')
        page.wait_for_timeout(8000)

        html = page.content()
        browser.close()

    results = parse_results(html)
    if county:
        results = [r for r in results if r["county"].lower() == county.lower()]
    if docket_type:
        dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-",
                     "non-traffic": "-NT-", "landlord/tenant": "-LT-"}
        code = dtype_map.get(docket_type.lower(), "")
        if code:
            results = [r for r in results if code in r["docket_number"]]
    return results


def parse_results(html):
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tbody tr")
    results = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 13:
            continue
        vals = [unescape(c.get_text(strip=True)) for c in cells[2:14]]
        rec = dict(zip(FIELDS, vals))
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
    p.add_argument("--recent", nargs="?", const="today", metavar="DAYS_OR_DATE",
                   help="Search by filing date. 'today' (default), a number of days back, or MM/DD/YYYY start date")
    p.add_argument("--end-date", help="End date for --recent range (default: today)", dest="end_date")
    p.add_argument("--download", type=int, metavar="N", help="Download docket sheet PDF for result N")
    p.add_argument("--json", action="store_true", help="Output JSON")
    args = p.parse_args()

    if not any([args.last, args.docket, args.otn, args.recent]):
        p.error("Provide at least --last, --docket, --otn, or --recent")

    if args.recent:
        today = datetime.now()
        if args.recent == "today":
            start = today
        elif args.recent.isdigit():
            start = today - timedelta(days=int(args.recent))
        else:
            start = datetime.strptime(args.recent, "%m/%d/%Y")
        end = datetime.strptime(args.end_date, "%m/%d/%Y") if args.end_date else today

        print(f"Searching filings {start.strftime('%m/%d/%Y')} – {end.strftime('%m/%d/%Y')} (headless browser)...")
        results = search_by_date(
            start.strftime("%Y-%m-%d"),
            end.strftime("%Y-%m-%d"),
            county=args.county,
            docket_type=args.docket_type,
        )
    else:
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
                session_dl, _ = get_session()
                download_pdf(session_dl, url, fn)
            else:
                print("No docket sheet URL for this result.")
        else:
            print(f"Invalid result number. Choose 1-{len(results)}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""CLI interface for UJS scraper toolkit."""

import argparse, json, sys
from datetime import datetime, timedelta

from ujs.core import (
    search_by_name, search_by_docket, search_by_otn,
    search_by_date, search_by_calendar, download_pdf, get_session,
)


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
        if r.get("event_date"):
            print(f"    Event: {r['event_type']}  |  {r['event_status']}  |  {r['event_date']}")
            if r.get("event_location"):
                print(f"    Location: {r['event_location']}")
        print()


def _parse_date_arg(val, direction="back"):
    """Parse a date arg: 'today', integer days, or MM/DD/YYYY."""
    today = datetime.now()
    if val == "today":
        return today
    if val.isdigit():
        delta = timedelta(days=int(val))
        return (today - delta) if direction == "back" else (today + delta)
    return datetime.strptime(val, "%m/%d/%Y")


def main():
    p = argparse.ArgumentParser(description="PA UJS Portal Search Tool")
    p.add_argument("--last", help="Last name")
    p.add_argument("--first", help="First name")
    p.add_argument("--docket", help="Docket number")
    p.add_argument("--county", help="County name (e.g. Lehigh)")
    p.add_argument("--type", help="Docket type: Criminal, Civil, Traffic, etc.", dest="docket_type")
    p.add_argument("--dob", help="Date of birth (MM/DD/YYYY)")
    p.add_argument("--otn", help="OTN number")
    p.add_argument("--recent", nargs="?", const="today", metavar="DAYS_OR_DATE",
                   help="Filing date search: 'today', N days back, or MM/DD/YYYY")
    p.add_argument("--calendar", nargs="?", const="today", metavar="DAYS_OR_DATE",
                   help="Calendar event search: 'today', N days ahead, or MM/DD/YYYY")
    p.add_argument("--end-date", help="End date for range (default: today / +7d)", dest="end_date")
    p.add_argument("--download", type=int, metavar="N", help="Download docket sheet PDF for result N")
    p.add_argument("--json", action="store_true", help="Output JSON")
    args = p.parse_args()

    if not any([args.last, args.docket, args.otn, args.recent, args.calendar]):
        p.error("Provide at least --last, --docket, --otn, --recent, or --calendar")

    results = []

    if args.calendar:
        start = _parse_date_arg(args.calendar, direction="forward")
        if args.calendar.isdigit():
            end_default = datetime.now() + timedelta(days=int(args.calendar))
        else:
            end_default = start + timedelta(days=7)
        end = datetime.strptime(args.end_date, "%m/%d/%Y") if args.end_date else end_default
        print(f"Calendar events {start:%m/%d/%Y} – {end:%m/%d/%Y} (headless browser)...")
        results = search_by_calendar(f"{start:%Y-%m-%d}", f"{end:%Y-%m-%d}",
                                     county=args.county, docket_type=args.docket_type)

    elif args.recent:
        start = _parse_date_arg(args.recent, direction="back")
        end = datetime.strptime(args.end_date, "%m/%d/%Y") if args.end_date else datetime.now()
        print(f"Filings {start:%m/%d/%Y} – {end:%m/%d/%Y} (headless browser)...")
        results = search_by_date(f"{start:%Y-%m-%d}", f"{end:%Y-%m-%d}",
                                 county=args.county, docket_type=args.docket_type)

    elif args.docket:
        print("Searching by docket number...")
        results = search_by_docket(args.docket)

    elif args.otn:
        print("Searching by OTN...")
        results = search_by_otn(args.otn)

    else:
        print("Searching by name...")
        results = search_by_name(args.last, first=args.first, dob=args.dob,
                                 county=args.county, docket_type=args.docket_type)

    if args.json:
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
                nbytes = download_pdf(url, fn)
                print(f"Saved: {fn} ({nbytes} bytes)")
            else:
                print("No docket sheet URL for this result.")
        else:
            print(f"Invalid result number. Choose 1-{len(results)}")


if __name__ == "__main__":
    main()

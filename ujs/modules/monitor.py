#!/usr/bin/env python3
"""Hourly monitor — detect new filings and upcoming calendar events."""

import json, os, time
from datetime import datetime, timedelta

from ujs.core import search_by_date, search_by_calendar

DEFAULT_STATE_FILE = os.path.expanduser("~/.ujs_monitor_state.json")


def load_state(path=DEFAULT_STATE_FILE):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {"seen_dockets": [], "seen_events": [], "last_run": None}


def save_state(state, path=DEFAULT_STATE_FILE):
    with open(path, "w") as f:
        json.dump(state, f, indent=2)


def check_new_filings(county=None, docket_type=None, lookback_days=1):
    """Return cases filed since last check."""
    today = datetime.now()
    start = today - timedelta(days=lookback_days)
    return search_by_date(
        start.strftime("%Y-%m-%d"),
        today.strftime("%Y-%m-%d"),
        county=county,
        docket_type=docket_type,
    )


def check_upcoming_events(county=None, docket_type=None, lookahead_days=3):
    """Return calendar events in the next N days."""
    today = datetime.now()
    end = today + timedelta(days=lookahead_days)
    return search_by_calendar(
        today.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
        county=county,
        docket_type=docket_type,
    )


def run_monitor(county=None, docket_type=None, state_file=DEFAULT_STATE_FILE,
                lookback_days=1, lookahead_days=3, on_new_filing=None, on_new_event=None):
    """Single monitor pass. Returns (new_filings, new_events)."""
    state = load_state(state_file)
    seen_dockets = set(state["seen_dockets"])
    seen_events = set(state["seen_events"])

    # Check filings
    filings = check_new_filings(county, docket_type, lookback_days)
    new_filings = [f for f in filings if f["docket_number"] not in seen_dockets]

    # Check events
    events = check_upcoming_events(county, docket_type, lookahead_days)
    new_events = []
    for e in events:
        key = f"{e['docket_number']}|{e.get('event_date','')}"
        if key not in seen_events:
            new_events.append(e)
            seen_events.add(key)

    # Update state
    for f in new_filings:
        seen_dockets.add(f["docket_number"])
    state["seen_dockets"] = list(seen_dockets)
    state["seen_events"] = list(seen_events)
    state["last_run"] = datetime.now().isoformat()
    save_state(state, state_file)

    # Callbacks
    if on_new_filing:
        for f in new_filings:
            on_new_filing(f)
    if on_new_event:
        for e in new_events:
            on_new_event(e)

    return new_filings, new_events


def run_loop(interval_minutes=60, **kwargs):
    """Run monitor in a loop. Ctrl+C to stop."""
    print(f"UJS Monitor running every {interval_minutes}m | county={kwargs.get('county')} type={kwargs.get('docket_type')}")
    while True:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            new_f, new_e = run_monitor(**kwargs)
            print(f"[{ts}] {len(new_f)} new filings, {len(new_e)} new events")
            for f in new_f:
                print(f"  NEW FILING: {f['docket_number']}  |  {f['caption']}  |  {f['county']}")
            for e in new_e:
                print(f"  NEW EVENT:  {e['docket_number']}  |  {e['caption']}  |  {e.get('event_type','')} {e.get('event_date','')}")
        except Exception as ex:
            print(f"[{ts}] ERROR: {ex}")
        time.sleep(interval_minutes * 60)


def main():
    import argparse
    p = argparse.ArgumentParser(description="UJS Monitor — track new filings & events")
    p.add_argument("--county", default="Lehigh")
    p.add_argument("--type", dest="docket_type", default="Criminal")
    p.add_argument("--interval", type=int, default=60, help="Minutes between checks (default: 60)")
    p.add_argument("--lookback", type=int, default=1, help="Days back for filings (default: 1)")
    p.add_argument("--lookahead", type=int, default=3, help="Days ahead for events (default: 3)")
    p.add_argument("--once", action="store_true", help="Run once and exit")
    p.add_argument("--reset", action="store_true", help="Clear seen state and start fresh")
    args = p.parse_args()

    if args.reset:
        save_state({"seen_dockets": [], "seen_events": [], "last_run": None})
        print("State reset.")

    kwargs = dict(county=args.county, docket_type=args.docket_type,
                  lookback_days=args.lookback, lookahead_days=args.lookahead)

    if args.once:
        new_f, new_e = run_monitor(**kwargs)
        print(f"{len(new_f)} new filings, {len(new_e)} new events")
        for f in new_f:
            print(f"  FILING: {f['docket_number']}  |  {f['caption']}")
        for e in new_e:
            print(f"  EVENT:  {e['docket_number']}  |  {e['caption']}  |  {e.get('event_type','')} {e.get('event_date','')}")
    else:
        run_loop(interval_minutes=args.interval, **kwargs)


if __name__ == "__main__":
    main()

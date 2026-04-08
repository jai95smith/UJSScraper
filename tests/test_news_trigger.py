#!/usr/bin/env python3
"""Tests for news search trigger logic (_is_person_query).

Uses Gemini Flash to classify queries — requires GEMINI_API_KEY.
Run: DATABASE_URL=... GEMINI_API_KEY=... python -m tests.test_news_trigger

Or on droplet:
  cd /opt/ujs && export $(grep -v '^#' .env | grep -v '^$' | xargs) && .venv/bin/python -m tests.test_news_trigger
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

PASS = 0
FAIL = 0


def test(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} — {detail}")


from ujs.chat.jobs import _is_person_query

# Fake court answers
PERSON_ANSWER = """Jason Michael Krasley has 3 active criminal cases in Lehigh County.
Docket CP-39-CR-0001517-2025: Charges include Rape (F1), Official Oppression (M2).
Bail: $100,000 posted. Next hearing: 04/08/2026."""

BULK_ANSWER = """Today's hearings in Lehigh County:
- CP-39-CR-0001517-2025: Evidentiary Hearing at 9:00 AM
- CP-39-CR-0002001-2025: Preliminary Hearing at 10:00 AM
15 total hearings scheduled."""

NO_RESULTS = "No cases found for that name. Try a different spelling."
STATS_ANSWER = "Lehigh County has 4,521 cases indexed. 67% are criminal cases."

# ---------------------------------------------------------------
print("\n" + "=" * 60)
print("News Trigger — Should Search")
print("=" * 60)

test("First Last",
     _is_person_query("tell me about Jason Krasley", PERSON_ANSWER))

test("Last, First Middle",
     _is_person_query("Krasley, Jason Michael hearing today", PERSON_ANSWER))

test("lowercase name",
     _is_person_query("tell me about jason krasley", PERSON_ANSWER))

test("name with question",
     _is_person_query("What cases does John Smith have?", PERSON_ANSWER))

test("rapsheet request",
     _is_person_query("show rapsheet for Maria Garcia", PERSON_ANSWER))

test("name + county",
     _is_person_query("jason krasley lehigh county", PERSON_ANSWER))

# ---------------------------------------------------------------
print("\n" + "=" * 60)
print("News Trigger — Should NOT Search")
print("=" * 60)

test("today's hearings",
     not _is_person_query("hearings today", BULK_ANSWER))

test("hearings that happened",
     not _is_person_query("Hearings that already happened today", BULK_ANSWER))

test("how many cases",
     not _is_person_query("how many criminal cases in Lehigh?", STATS_ANSWER))

test("filing stats",
     not _is_person_query("show filing stats for this month", STATS_ANSWER))

test("upcoming hearings",
     not _is_person_query("what hearings are scheduled tomorrow?", BULK_ANSWER))

test("docket number only",
     not _is_person_query("look up CP-39-CR-0001517-2025", PERSON_ANSWER))

test("generic charge search",
     not _is_person_query("search for DUI cases in Northampton", BULK_ANSWER))

test("system question",
     not _is_person_query("how many cases are indexed?", STATS_ANSWER))

test("no results",
     not _is_person_query("tell me about Xyz Qwerty", NO_RESULTS))

test("coverage question",
     not _is_person_query("what is the analysis coverage?", STATS_ANSWER))

# ---------------------------------------------------------------
print(f"\n{'=' * 60}")
print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
print(f"{'=' * 60}\n")
sys.exit(0 if FAIL == 0 else 1)

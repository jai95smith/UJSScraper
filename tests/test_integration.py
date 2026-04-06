#!/usr/bin/env python3
"""Integration test suite for UJS scraper + DB + API.

Run: DATABASE_URL=... GEMINI_API_KEY=... python -m tests.test_integration
"""

import json, os, sys, tempfile, time, hashlib
from datetime import datetime

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from ujs import db
from ujs.core import search_by_docket, search_by_name
from ujs.modules.docket_pdf import analyze_docket, analyze_summary

TEST_DOCKET = "CP-39-CR-0000142-2025"
TEST_DOCKET_MJ = "MJ-31303-TR-0000496-2026"
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


def run_tests():
    global PASS, FAIL

    print("\n" + "=" * 60)
    print("UJS Integration Tests")
    print("=" * 60)

    # ------------------------------------------------------------------
    print("\n--- 1. Core scraper ---")
    # ------------------------------------------------------------------
    results = search_by_docket(TEST_DOCKET)
    test("search_by_docket returns results", len(results) > 0)
    test("result has required keys",
         all(k in results[0] for k in ["docket_number", "caption", "status", "county"]),
         f"keys: {list(results[0].keys())}")
    test("docket_sheet_url is encoded",
         "%20" in results[0].get("docket_sheet_url", "") or " " not in results[0].get("docket_sheet_url", ""))

    name_results = search_by_name("Smith", first="John", county="Lehigh", docket_type="Criminal")
    test("search_by_name returns results", len(name_results) > 0)
    test("all name results have same keys",
         len(set(tuple(sorted(r.keys())) for r in name_results)) == 1)

    # ------------------------------------------------------------------
    print("\n--- 2. Gemini analysis ---")
    # ------------------------------------------------------------------
    with tempfile.TemporaryDirectory() as d:
        analysis = analyze_docket(TEST_DOCKET, d)

    test("analysis has defendant", "defendant" in analysis)
    test("analysis has charges", "charges" in analysis and len(analysis["charges"]) > 0)
    test("analysis has docket_entries", "docket_entries" in analysis)
    test("no string nulls",
         not _find_string_nulls(analysis),
         f"found: {_find_string_nulls(analysis)}")
    test("dates are MM/DD/YYYY",
         not _find_bad_dates(analysis),
         f"found: {_find_bad_dates(analysis)}")

    # Consistency — run twice, compare
    with tempfile.TemporaryDirectory() as d:
        analysis2 = analyze_docket(TEST_DOCKET, d)
    a1_clean = {k: v for k, v in analysis.items() if k not in ("pdf_path", "full_text")}
    a2_clean = {k: v for k, v in analysis2.items() if k not in ("pdf_path", "full_text")}
    h1 = hashlib.md5(json.dumps(a1_clean, sort_keys=True).encode()).hexdigest()
    h2 = hashlib.md5(json.dumps(a2_clean, sort_keys=True).encode()).hexdigest()
    test("Gemini output consistent across runs", h1 == h2,
         f"hash1={h1} hash2={h2}")

    # ------------------------------------------------------------------
    print("\n--- 3. DB upsert + retrieval ---")
    # ------------------------------------------------------------------
    # Clean test data first
    with db.connect() as conn:
        cur = conn.cursor()
        for tbl in ["change_log", "docket_entries", "attorneys", "sentences",
                     "bail", "charges", "events", "participants", "analyses",
                     "ingest_queue", "scrape_log"]:
            cur.execute(f"DELETE FROM {tbl}")
        cur.execute("DELETE FROM cases")

    # Upsert from search results
    with db.connect() as conn:
        total, new = db.upsert_cases(conn, results)
    test("upsert_cases inserts", new == 1)

    # Upsert again — should not be new
    with db.connect() as conn:
        total, new = db.upsert_cases(conn, results)
    test("upsert_cases deduplicates", new == 0)

    # Retrieve
    with db.connect() as conn:
        case = db.get_case(conn, TEST_DOCKET)
    test("get_case returns data", case is not None)
    test("get_case has correct docket", case["docket_number"] == TEST_DOCKET)
    test("last_scraped is set", case["last_scraped"] is not None)

    # Search
    with db.connect() as conn:
        found = db.search_cases(conn, county="Lehigh")
    test("search_cases by county", len(found) > 0)

    # ------------------------------------------------------------------
    print("\n--- 4. Change detection ---")
    # ------------------------------------------------------------------
    # Initial store
    with db.connect() as conn:
        changes = db.detect_and_store_changes(conn, TEST_DOCKET, a1_clean)
    test("initial ingest detected", any(c["field"] == "initial_ingest" for c in changes))

    # Same data — no changes
    with db.connect() as conn:
        changes = db.detect_and_store_changes(conn, TEST_DOCKET, a1_clean)
    test("identical data = no changes", len(changes) == 0)

    # Simulated status change
    modified = dict(a1_clean)
    modified["case_status"] = "FAKE_ACTIVE_STATUS"
    with db.connect() as conn:
        changes = db.detect_and_store_changes(conn, TEST_DOCKET, modified)
    test("status change detected",
         any(c["field"] == "case_status" for c in changes),
         f"changes: {changes}")

    # Simulated new entries
    modified2 = dict(a1_clean)
    modified2["docket_entries"] = a1_clean.get("docket_entries", []) + [
        {"date": "12/31/2099", "description": "Test entry", "filer": "Test"}
    ]
    with db.connect() as conn:
        changes = db.detect_and_store_changes(conn, TEST_DOCKET, modified2)
    test("new docket entry detected",
         any("entries" in c["field"] for c in changes),
         f"changes: {changes}")

    # Change log persisted
    with db.connect() as conn:
        logs = db.get_changes(conn, docket_number=TEST_DOCKET)
    test("change_log has entries", len(logs) >= 2)

    # ------------------------------------------------------------------
    print("\n--- 5. Ingest queue lifecycle ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        qid, status = db.queue_ingest(conn, "TEST-DOCKET-999")
    test("queue_ingest returns id", qid is not None)
    test("queue status is pending", status == "pending")

    # Duplicate queue — should return same
    with db.connect() as conn:
        qid2, status2 = db.queue_ingest(conn, "TEST-DOCKET-999")
    test("duplicate queue returns existing", qid2 == qid)

    # Claim job
    with db.connect() as conn:
        job = db.claim_ingest_job(conn)
    test("claim_ingest_job returns job", job is not None)
    test("claimed job matches", job[1] == "TEST-DOCKET-999")

    # Complete job
    with db.connect() as conn:
        db.complete_ingest_job(conn, job[0])

    # No more jobs
    with db.connect() as conn:
        empty = db.claim_ingest_job(conn)
    test("no more pending jobs", empty is None)

    # ------------------------------------------------------------------
    print("\n--- 6. Staleness detection ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        # Set last_scraped to 48h ago for our test case
        cur = conn.cursor()
        cur.execute("""
            UPDATE cases SET last_scraped = NOW() - INTERVAL '48 hours', status = 'Active'
            WHERE docket_number = %s
        """, (TEST_DOCKET,))

    with db.connect() as conn:
        stale = db.get_stale_dockets(conn, active_hours=24)
    test("stale active case detected", any(s["docket_number"] == TEST_DOCKET for s in stale))

    # ------------------------------------------------------------------
    print("\n--- 7. Stats ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        stats = db.get_stats(conn)
    test("stats has cases count", "cases" in stats)
    test("stats has last_scrape", "last_scrape" in stats)
    test("cases count > 0", stats["cases"] > 0)

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    print("\n--- Cleanup ---")
    with db.connect() as conn:
        cur = conn.cursor()
        for tbl in ["change_log", "docket_entries", "attorneys", "sentences",
                     "bail", "charges", "events", "participants", "analyses",
                     "ingest_queue", "scrape_log"]:
            cur.execute(f"DELETE FROM {tbl}")
        cur.execute("DELETE FROM cases")
    print("  Test data cleaned")

    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'='*60}\n")
    return FAIL == 0


def _find_string_nulls(obj, path=""):
    found = []
    if obj == "null" or obj == "None":
        found.append(path)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            found.extend(_find_string_nulls(v, f"{path}.{k}"))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            found.extend(_find_string_nulls(v, f"{path}[{i}]"))
    return found


def _find_bad_dates(obj, path=""):
    import re
    found = []
    if isinstance(obj, str) and re.match(r"\d{4}-\d{2}-\d{2}", obj):
        found.append(f"{path}={obj}")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            found.extend(_find_bad_dates(v, f"{path}.{k}"))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            found.extend(_find_bad_dates(v, f"{path}[{i}]"))
    return found


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

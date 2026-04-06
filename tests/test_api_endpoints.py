#!/usr/bin/env python3
"""API endpoint tests — verifies every endpoint returns correct shape.
Runs against live API on localhost:8100. Does NOT modify prod data.

Run: DATABASE_URL=... python -m tests.test_api_endpoints
"""

import json, os, sys, requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

BASE = "http://localhost:8100"
PASS = 0
FAIL = 0

# Dynamically find a known docket with analysis
KNOWN_DOCKET = None


def _find_known_docket():
    global KNOWN_DOCKET
    from ujs import db
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT a.docket_number FROM analyses a LIMIT 1")
        row = cur.fetchone()
        if row:
            KNOWN_DOCKET = row[0]
    if not KNOWN_DOCKET:
        print("WARNING: No analyzed dockets in DB. Some tests will be skipped.")
    else:
        print(f"Using known docket: {KNOWN_DOCKET}")


def test(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} — {detail}")


def get(path, expected_status=200):
    r = requests.get(f"{BASE}{path}")
    return r


def run_tests():
    global PASS, FAIL

    # Verify API is running
    try:
        r = requests.get(f"{BASE}/health", timeout=3)
    except Exception:
        print("ERROR: API not running on localhost:8100. Start it first.")
        sys.exit(1)

    _find_known_docket()

    print("\n" + "=" * 60)
    print("API Endpoint Tests")
    print("=" * 60)

    # ------------------------------------------------------------------
    print("\n--- Health & Stats ---")
    # ------------------------------------------------------------------
    r = get("/health")
    test("/health returns 200", r.status_code == 200)
    d = r.json()
    test("/health has status", d.get("status") == "ok")
    test("/health has cases_indexed", "cases_indexed" in d)

    r = get("/stats")
    test("/stats returns 200", r.status_code == 200)
    d = r.json()
    test("/stats has cases", "cases" in d)
    test("/stats has events", "events" in d)
    test("/stats has last_scrape", "last_scrape" in d)

    # ------------------------------------------------------------------
    print("\n--- Quick Access ---")
    # ------------------------------------------------------------------
    r = get("/filings/today")
    test("/filings/today returns 200", r.status_code == 200)
    test("/filings/today returns list", isinstance(r.json(), list))

    r = get("/filings/recent?days=7&county=Lehigh")
    test("/filings/recent returns 200", r.status_code == 200)
    d = r.json()
    test("/filings/recent returns list", isinstance(d, list))
    if d:
        test("/filings/recent has docket_number", "docket_number" in d[0])
        test("/filings/recent has county", "county" in d[0])

    r = get("/hearings/today?county=Lehigh")
    test("/hearings/today returns 200", r.status_code == 200)
    test("/hearings/today returns list", isinstance(r.json(), list))

    r = get("/hearings/upcoming?county=Lehigh&days=7")
    test("/hearings/upcoming returns 200", r.status_code == 200)
    d = r.json()
    test("/hearings/upcoming returns list", isinstance(d, list))
    if d:
        test("/hearings/upcoming has event fields",
             "event_type" in d[0] and "event_date" in d[0] and "caption" in d[0])
    else:
        print("  SKIP  hearings/upcoming fields (no events)")

    r = get("/hearings/upcoming?county=Lehigh&event_type=Preliminary")
    test("/hearings/upcoming with event_type filter", r.status_code == 200)

    # ------------------------------------------------------------------
    print("\n--- Search ---")
    # ------------------------------------------------------------------
    r = get("/search/cases?county=Lehigh&limit=5")
    test("/search/cases returns 200", r.status_code == 200)
    d = r.json()
    test("/search/cases returns list", isinstance(d, list))
    test("/search/cases respects limit", len(d) <= 5)

    # Search by name — use "Comm" which is in most captions
    r = get("/search/cases?name=Comm&limit=3")
    test("/search/cases by name (caption)", r.status_code == 200)
    d = r.json()
    test("/search/cases finds results by name", len(d) > 0, f"got {len(d)}")

    r = get("/search/cases?county=Lehigh&type=Criminal&limit=3")
    test("/search/cases type filter", r.status_code == 200)
    d = r.json()
    if d:
        test("criminal filter returns CR dockets",
             all("-CR-" in c["docket_number"] for c in d),
             f"dockets: {[c['docket_number'] for c in d]}")

    r = get("/search/events?county=Lehigh")
    test("/search/events returns 200", r.status_code == 200)

    r = get("/search/judge?name=Test")
    test("/search/judge returns 200", r.status_code == 200)
    test("/search/judge returns list", isinstance(r.json(), list))

    r = get("/search/attorney?name=Test")
    test("/search/attorney returns 200", r.status_code == 200)

    r = get("/search/charges?description=Theft")
    test("/search/charges returns 200", r.status_code == 200)

    # ------------------------------------------------------------------
    print("\n--- Docket endpoints ---")
    # ------------------------------------------------------------------
    if KNOWN_DOCKET:
        r = get(f"/docket/{KNOWN_DOCKET}")
        test("/docket/{n} returns 200 for known docket", r.status_code == 200)
        d = r.json()
        test("/docket/{n} has docket_number", "docket_number" in d)

        r = get(f"/docket/{KNOWN_DOCKET}/analyze")
        test("/docket/{n}/analyze returns 200", r.status_code == 200)
        d = r.json()
        test("/docket/{n}/analyze has charges or case_caption",
             "case_caption" in d or "charges" in d, f"keys: {list(d.keys())[:5]}")

        r = get(f"/docket/{KNOWN_DOCKET}/changes")
        test("/docket/{n}/changes returns 200", r.status_code == 200)
        test("/docket/{n}/changes returns list", isinstance(r.json(), list))

        r = get(f"/docket/{KNOWN_DOCKET}/charges")
        test("/docket/{n}/charges returns 200", r.status_code == 200)
        test("/docket/{n}/charges returns list", isinstance(r.json(), list))

        r = get(f"/docket/{KNOWN_DOCKET}/sentences")
        test("/docket/{n}/sentences returns 200", r.status_code == 200)

        r = get(f"/docket/{KNOWN_DOCKET}/attorneys")
        test("/docket/{n}/attorneys returns 200", r.status_code == 200)

        r = get(f"/docket/{KNOWN_DOCKET}/bail")
        test("/docket/{n}/bail returns 200", r.status_code == 200)

        r = get(f"/docket/{KNOWN_DOCKET}/entries")
        test("/docket/{n}/entries returns 200", r.status_code == 200)
        d = r.json()
        if d:
            test("/docket/{n}/entries has date", "entry_date" in d[0])
            test("/docket/{n}/entries has description", "description" in d[0])
    else:
        print("  SKIP  docket endpoints (no analyzed dockets)")

    # Unknown docket — should queue
    r = get("/docket/FAKE-NONEXISTENT-999")
    test("/docket unknown returns 202", r.status_code == 202)
    d = r.json()
    test("/docket unknown has queuing status", d.get("status") == "queuing")

    r = get("/docket/FAKE-NONEXISTENT-999/analyze")
    test("/docket/analyze unknown returns 202", r.status_code == 202)

    # ------------------------------------------------------------------
    print("\n--- Analytics ---")
    # ------------------------------------------------------------------
    r = get("/stats/filings?county=Lehigh")
    test("/stats/filings returns 200", r.status_code == 200)
    test("/stats/filings returns list", isinstance(r.json(), list))

    r = get("/stats/counties")
    test("/stats/counties returns 200", r.status_code == 200)
    d = r.json()
    test("/stats/counties has Lehigh", any(c["county"] == "Lehigh" for c in d))
    test("/stats/counties has Northampton", any(c["county"] == "Northampton" for c in d))

    r = get("/stats/charges")
    test("/stats/charges returns 200", r.status_code == 200)

    r = get("/stats/judges")
    test("/stats/judges returns 200", r.status_code == 200)

    # ------------------------------------------------------------------
    print("\n--- Changes feed ---")
    # ------------------------------------------------------------------
    r = get("/changes?limit=5")
    test("/changes returns 200", r.status_code == 200)
    test("/changes returns list", isinstance(r.json(), list))

    # ------------------------------------------------------------------
    print("\n--- Ingest status ---")
    # ------------------------------------------------------------------
    r = get(f"/ingest/{KNOWN_DOCKET}/status")
    test("/ingest/{n}/status returns 200", r.status_code == 200)
    d = r.json()
    test("/ingest/{n}/status has status field", "status" in d)

    # ------------------------------------------------------------------
    print("\n--- API key creation ---")
    # ------------------------------------------------------------------
    r = requests.post(f"{BASE}/keys?name=test-endpoint-suite")
    test("POST /keys returns 200", r.status_code == 200)
    d = r.json()
    test("POST /keys returns key", "key" in d and d["key"].startswith("ujs_"))
    api_key = d.get("key", "")

    # ------------------------------------------------------------------
    print("\n--- Chat /ask ---")
    # ------------------------------------------------------------------
    r = get("/ask?q=How%20many%20cases%20are%20indexed")
    test("GET /ask returns 200", r.status_code == 200)
    d = r.json()
    test("GET /ask has answer", "answer" in d)
    test("GET /ask answer is non-empty", len(d.get("answer", "")) > 10)

    # ------------------------------------------------------------------
    print("\n--- Swagger docs ---")
    # ------------------------------------------------------------------
    r = get("/docs")
    test("/docs returns 200", r.status_code == 200)

    r = get("/openapi.json")
    test("/openapi.json returns 200", r.status_code == 200)
    d = r.json()
    paths = list(d.get("paths", {}).keys())
    test("OpenAPI has 20+ endpoints", len(paths) >= 20, f"got {len(paths)}")

    # Cleanup: remove test API key
    from ujs import db
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM api_keys WHERE name = 'test-endpoint-suite'")
        cur.execute("DELETE FROM ingest_queue WHERE docket_number = 'FAKE-NONEXISTENT-999'")
        cur.execute("DELETE FROM cases WHERE docket_number = 'FAKE-NONEXISTENT-999'")

    print(f"\n{'='*60}")
    print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'='*60}\n")
    return FAIL == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

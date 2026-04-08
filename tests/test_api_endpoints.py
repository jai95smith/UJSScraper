#!/usr/bin/env python3
"""API endpoint tests — verifies every endpoint returns correct shape.
Runs against live API. Requires AUTH_SIGNING_KEY env var for chat endpoints.

Run on droplet: export $(grep -v "^#" .env | xargs) && python3 -m tests.test_api_endpoints
"""

import json, os, sys, requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

BASE = os.environ.get("API_BASE", "http://localhost:8100")
PASS = 0
FAIL = 0

# Dynamically find a known docket with analysis
KNOWN_DOCKET = None
AUTH_HEADERS = {}


def _setup():
    global KNOWN_DOCKET, AUTH_HEADERS
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

    # Auth token for chat endpoints
    if os.environ.get("AUTH_SIGNING_KEY"):
        from ujs.auth import create_user_token
        token = create_user_token("test-api-user", "apitest@test.com", "API Test")
        AUTH_HEADERS = {"Authorization": f"Bearer {token}"}
        print("Auth token generated for chat tests")
    else:
        print("WARNING: AUTH_SIGNING_KEY not set. Chat endpoint tests will be skipped.")


def test(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} — {detail}")


def get(path, headers=None):
    return requests.get(f"{BASE}{path}", headers=headers)


def run_tests():
    global PASS, FAIL

    # Verify API is running
    try:
        r = requests.get(f"{BASE}/health", timeout=3)
    except Exception:
        print(f"ERROR: API not running on {BASE}. Start it first.")
        sys.exit(1)

    _setup()

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

    # ------------------------------------------------------------------
    print("\n--- Search ---")
    # ------------------------------------------------------------------
    r = get("/search/cases?county=Lehigh&limit=5")
    test("/search/cases returns 200", r.status_code == 200)
    d = r.json()
    test("/search/cases returns list", isinstance(d, list))
    test("/search/cases respects limit", len(d) <= 5)

    r = get("/search/cases?name=Comm&limit=3")
    test("/search/cases by name", r.status_code == 200)
    d = r.json()
    test("/search/cases finds results", len(d) > 0, f"got {len(d)}")

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

    r = get("/search/attorney?name=Test")
    test("/search/attorney returns 200", r.status_code == 200)

    r = get("/search/charges?description=Theft")
    test("/search/charges returns 200", r.status_code == 200)

    # ------------------------------------------------------------------
    print("\n--- Docket endpoints ---")
    # ------------------------------------------------------------------
    if KNOWN_DOCKET:
        r = get(f"/docket/{KNOWN_DOCKET}")
        test("/docket/{n} returns 200", r.status_code == 200)
        d = r.json()
        test("/docket/{n} has docket_number", "docket_number" in d)

        r = get(f"/docket/{KNOWN_DOCKET}/analyze")
        test("/docket/{n}/analyze returns 200", r.status_code == 200)
        d = r.json()
        test("/docket/{n}/analyze has charges or caption",
             "case_caption" in d or "charges" in d, f"keys: {list(d.keys())[:5]}")

        r = get(f"/docket/{KNOWN_DOCKET}/changes")
        test("/docket/{n}/changes returns 200", r.status_code == 200)
        test("/docket/{n}/changes returns list", isinstance(r.json(), list))

        r = get(f"/docket/{KNOWN_DOCKET}/charges")
        test("/docket/{n}/charges returns 200", r.status_code == 200)

        r = get(f"/docket/{KNOWN_DOCKET}/sentences")
        test("/docket/{n}/sentences returns 200", r.status_code == 200)

        r = get(f"/docket/{KNOWN_DOCKET}/attorneys")
        test("/docket/{n}/attorneys returns 200", r.status_code == 200)

        r = get(f"/docket/{KNOWN_DOCKET}/bail")
        test("/docket/{n}/bail returns 200", r.status_code == 200)

        r = get(f"/docket/{KNOWN_DOCKET}/entries")
        test("/docket/{n}/entries returns 200", r.status_code == 200)
    else:
        print("  SKIP  docket endpoints (no analyzed dockets)")

    # ------------------------------------------------------------------
    print("\n--- Analytics ---")
    # ------------------------------------------------------------------
    r = get("/stats/filings?county=Lehigh")
    test("/stats/filings returns 200", r.status_code == 200)

    r = get("/stats/counties")
    test("/stats/counties returns 200", r.status_code == 200)
    d = r.json()
    test("/stats/counties has Lehigh", any(c["county"] == "Lehigh" for c in d))

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
    if KNOWN_DOCKET:
        r = get(f"/ingest/{KNOWN_DOCKET}/status")
        test("/ingest/{n}/status returns 200", r.status_code == 200)
        d = r.json()
        test("/ingest/{n}/status has status field", "status" in d)

    # ------------------------------------------------------------------
    print("\n--- Chat endpoints (auth required) ---")
    # ------------------------------------------------------------------
    if AUTH_HEADERS:
        # List conversations (empty for test user)
        r = requests.get(f"{BASE}/conversations", headers=AUTH_HEADERS)
        test("GET /conversations returns 200", r.status_code == 200)
        test("GET /conversations returns list", isinstance(r.json(), list))

        # Create conversation via /ask
        r = requests.post(f"{BASE}/ask",
                          json={"question": "How many cases are indexed?"},
                          headers=AUTH_HEADERS)
        test("POST /ask returns 200", r.status_code == 200)
        d = r.json()
        test("POST /ask has job_id", "job_id" in d)
        test("POST /ask has conversation_id", "conversation_id" in d)
        test("POST /ask status is running", d.get("status") == "running")
        cid = d.get("conversation_id")
        job_id = d.get("job_id")

        # Poll job
        if job_id:
            r = requests.get(f"{BASE}/ask/job/{job_id}?cid={cid}", headers=AUTH_HEADERS)
            test("GET /ask/job/{id} returns 200", r.status_code == 200)
            d = r.json()
            test("job has status field", "status" in d)
            test("job has response field", "response" in d)

        # Get conversation
        if cid:
            r = requests.get(f"{BASE}/conversations/{cid}", headers=AUTH_HEADERS)
            test("GET /conversations/{id} returns 200", r.status_code == 200)

            # Get conversation job
            r = requests.get(f"{BASE}/conversations/{cid}/job", headers=AUTH_HEADERS)
            test("GET /conversations/{id}/job returns 200", r.status_code == 200)

            # Delete conversation
            r = requests.delete(f"{BASE}/conversations/{cid}", headers=AUTH_HEADERS)
            test("DELETE /conversations/{id} returns 200", r.status_code == 200)
            test("DELETE returns deleted status", r.json().get("status") == "deleted")

            # Verify deleted
            r = requests.get(f"{BASE}/conversations/{cid}", headers=AUTH_HEADERS)
            test("deleted conversation returns 404", r.status_code == 404)

        # Auth: other user can't access
        from ujs.auth import create_user_token
        other_token = create_user_token("other-user-id", "other@test.com")
        other_headers = {"Authorization": f"Bearer {other_token}"}
        if cid:
            r = requests.get(f"{BASE}/conversations/{cid}", headers=other_headers)
            test("other user can't access conversation", r.status_code == 404)
    else:
        print("  SKIP  chat endpoints (no AUTH_SIGNING_KEY)")

    # Cleanup
    try:
        from ujs import db
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM chat_jobs WHERE conversation_id IN (SELECT id FROM conversations WHERE user_id = 'test-api-user')")
            cur.execute("DELETE FROM conversations WHERE user_id = 'test-api-user'")
            cur.execute("DELETE FROM ingest_queue WHERE docket_number = 'FAKE-NONEXISTENT-999'")
            cur.execute("DELETE FROM cases WHERE docket_number = 'FAKE-NONEXISTENT-999'")
    except Exception:
        pass

    print(f"\n{'='*60}")
    print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'='*60}\n")
    return FAIL == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

#!/usr/bin/env python3
"""Watch system tests — API endpoints, auth, limits, preferences, cross-user isolation.

Run on droplet: export $(grep -v "^#" .env | xargs) && python3 -m tests.test_watches
"""

import os, sys, requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

BASE = os.environ.get("API_BASE", "http://localhost:8100")
PASS = 0
FAIL = 0
AUTH_HEADERS = {}
OTHER_HEADERS = {}
TEST_DOCKET = "CP-39-CR-0000142-2025"


def _setup():
    global AUTH_HEADERS, OTHER_HEADERS
    if not os.environ.get("AUTH_SIGNING_KEY"):
        print("ERROR: AUTH_SIGNING_KEY not set.")
        sys.exit(1)
    from ujs.auth import create_user_token
    AUTH_HEADERS = {"Authorization": f"Bearer {create_user_token('test-watch-user', 'watch@test.com', 'Watch Test')}"}
    OTHER_HEADERS = {"Authorization": f"Bearer {create_user_token('other-watch-user', 'other@test.com', 'Other')}"}


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
    _setup()

    print("\n" + "=" * 60)
    print("Watch System Tests")
    print("=" * 60)

    # ------------------------------------------------------------------
    print("\n--- Auth required ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{BASE}/watches")
    test("GET /watches requires auth", r.status_code == 401)

    r = requests.post(f"{BASE}/watches", json={"docket_number": TEST_DOCKET})
    test("POST /watches requires auth", r.status_code == 401)

    r = requests.delete(f"{BASE}/watches/{TEST_DOCKET}")
    test("DELETE /watches requires auth", r.status_code == 401)

    r = requests.get(f"{BASE}/preferences")
    test("GET /preferences requires auth", r.status_code == 401)

    r = requests.put(f"{BASE}/preferences", json={"email_alerts": False})
    test("PUT /preferences requires auth", r.status_code == 401)

    # ------------------------------------------------------------------
    print("\n--- Watch CRUD ---")
    # ------------------------------------------------------------------
    # Clean slate
    requests.delete(f"{BASE}/watches/{TEST_DOCKET}", headers=AUTH_HEADERS)

    # Add watch
    r = requests.post(f"{BASE}/watches",
                      json={"docket_number": TEST_DOCKET, "label": "Test watch"},
                      headers=AUTH_HEADERS)
    test("POST /watches returns 200", r.status_code == 200, f"got {r.status_code}")
    d = r.json()
    test("watch has id", "id" in d)
    test("watch status is watching", d.get("status") == "watching")

    # List watches
    r = requests.get(f"{BASE}/watches", headers=AUTH_HEADERS)
    test("GET /watches returns 200", r.status_code == 200)
    watches = r.json()
    test("watches list has our docket", any(w["docket_number"] == TEST_DOCKET for w in watches))
    if watches:
        w = [w for w in watches if w["docket_number"] == TEST_DOCKET][0]
        test("watch has caption field", "caption" in w)
        test("watch has pending_changes", "pending_changes" in w)

    # Check status
    r = requests.get(f"{BASE}/watches/{TEST_DOCKET}/status", headers=AUTH_HEADERS)
    test("status shows watching=true", r.json().get("watching") is True)

    # Duplicate watch (upsert)
    r = requests.post(f"{BASE}/watches",
                      json={"docket_number": TEST_DOCKET, "label": "Updated label"},
                      headers=AUTH_HEADERS)
    test("duplicate watch upserts (200)", r.status_code == 200)

    # ------------------------------------------------------------------
    print("\n--- Cross-user isolation ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{BASE}/watches", headers=OTHER_HEADERS)
    test("other user sees empty list", len(r.json()) == 0)

    r = requests.get(f"{BASE}/watches/{TEST_DOCKET}/status", headers=OTHER_HEADERS)
    test("other user not watching", r.json().get("watching") is False)

    r = requests.delete(f"{BASE}/watches/{TEST_DOCKET}", headers=OTHER_HEADERS)
    test("other user can't delete our watch", r.status_code == 404)

    # Verify ours still exists
    r = requests.get(f"{BASE}/watches/{TEST_DOCKET}/status", headers=AUTH_HEADERS)
    test("our watch still exists", r.json().get("watching") is True)

    # ------------------------------------------------------------------
    print("\n--- Preferences ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{BASE}/preferences", headers=AUTH_HEADERS)
    test("GET /preferences returns 200", r.status_code == 200)
    prefs = r.json()
    test("prefs has email_alerts", "email_alerts" in prefs)
    test("prefs has weekly_digest", "weekly_digest" in prefs)
    test("prefs does NOT expose unsubscribe_token", "unsubscribe_token" not in prefs)

    # Update
    r = requests.put(f"{BASE}/preferences",
                     json={"email_alerts": False, "weekly_digest": True},
                     headers=AUTH_HEADERS)
    test("PUT /preferences returns 200", r.status_code == 200)

    r = requests.get(f"{BASE}/preferences", headers=AUTH_HEADERS)
    prefs = r.json()
    test("email_alerts updated to false", prefs.get("email_alerts") is False)
    test("weekly_digest updated to true", prefs.get("weekly_digest") is True)

    # Reset
    requests.put(f"{BASE}/preferences", json={"email_alerts": True, "weekly_digest": False}, headers=AUTH_HEADERS)

    # ------------------------------------------------------------------
    print("\n--- Remove watch ---")
    # ------------------------------------------------------------------
    r = requests.delete(f"{BASE}/watches/{TEST_DOCKET}", headers=AUTH_HEADERS)
    test("DELETE /watches returns 200", r.status_code == 200)
    test("delete status is removed", r.json().get("status") == "removed")

    r = requests.get(f"{BASE}/watches/{TEST_DOCKET}/status", headers=AUTH_HEADERS)
    test("watch removed", r.json().get("watching") is False)

    # Delete again → 404
    r = requests.delete(f"{BASE}/watches/{TEST_DOCKET}", headers=AUTH_HEADERS)
    test("double delete returns 404", r.status_code == 404)

    # ------------------------------------------------------------------
    print("\n--- Input validation ---")
    # ------------------------------------------------------------------
    r = requests.post(f"{BASE}/watches", json={"docket_number": ""}, headers=AUTH_HEADERS)
    test("empty docket rejected", r.status_code == 422, f"got {r.status_code}")

    r = requests.post(f"{BASE}/watches",
                      json={"docket_number": TEST_DOCKET, "notify_frequency": "invalid"},
                      headers=AUTH_HEADERS)
    test("invalid frequency rejected", r.status_code == 422, f"got {r.status_code}")

    # Cleanup
    try:
        from ujs import db
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM user_watches WHERE user_id IN ('test-watch-user', 'other-watch-user')")
            cur.execute("DELETE FROM user_preferences WHERE user_id IN ('test-watch-user', 'other-watch-user')")
        print("\nCleaned up test data")
    except Exception as e:
        print(f"\nCleanup warning: {e}")

    print(f"\n{'='*60}")
    print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'='*60}\n")
    return FAIL == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

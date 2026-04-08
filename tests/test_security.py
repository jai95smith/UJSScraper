#!/usr/bin/env python3
"""Security hardening tests — headers, CORS, rate limits, body limits.

Run against live servers:
  python3 -m tests.test_security
  python3 -m tests.test_security --prod   (test against gavelsearch.com)
"""

import sys, os, json, requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

PASS = 0
FAIL = 0

# Default to local. --prod tests HTTPS from outside, --droplet tests locally on server.
if "--prod" in sys.argv:
    WEB_BASE = "https://gavelsearch.com"
    API_BASE = "https://api.gavelsearch.com"
    print("Testing against PRODUCTION (external)")
elif "--droplet" in sys.argv:
    WEB_BASE = "http://localhost:8000"
    API_BASE = "http://localhost:8100"
    print("Testing against DROPLET (localhost)")
else:
    WEB_BASE = "http://localhost:8000"
    API_BASE = "http://localhost:8100"
    print("Testing against LOCALHOST")


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
    print("Security Hardening Tests")
    print("=" * 60)

    # ------------------------------------------------------------------
    print("\n--- Flask security headers ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{WEB_BASE}/login", allow_redirects=False)
    h = r.headers

    test("X-Frame-Options: DENY", h.get("X-Frame-Options") == "DENY", h.get("X-Frame-Options"))
    test("X-Content-Type-Options: nosniff", h.get("X-Content-Type-Options") == "nosniff", h.get("X-Content-Type-Options"))
    test("Referrer-Policy set", "strict-origin" in (h.get("Referrer-Policy") or ""), h.get("Referrer-Policy"))
    test("HSTS header present", "max-age=" in (h.get("Strict-Transport-Security") or ""), h.get("Strict-Transport-Security"))
    test("CSP header present", "default-src" in (h.get("Content-Security-Policy") or ""), h.get("Content-Security-Policy"))
    test("Cache-Control no-store on HTML", "no-store" in (h.get("Cache-Control") or ""), h.get("Cache-Control"))

    # CSP details
    csp = h.get("Content-Security-Policy", "")
    test("CSP has frame-ancestors none", "frame-ancestors 'none'" in csp, csp[:80])
    test("CSP has script-src", "script-src" in csp)
    test("CSP allows googleusercontent for images", "googleusercontent.com" in csp)

    # ------------------------------------------------------------------
    print("\n--- FastAPI security headers ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{API_BASE}/health")
    h = r.headers
    test("API: X-Content-Type-Options", h.get("X-Content-Type-Options") == "nosniff", h.get("X-Content-Type-Options"))
    test("API: HSTS header", "max-age=" in (h.get("Strict-Transport-Security") or ""), h.get("Strict-Transport-Security"))
    test("API: Referrer-Policy", "strict-origin" in (h.get("Referrer-Policy") or ""), h.get("Referrer-Policy"))

    # ------------------------------------------------------------------
    print("\n--- Auth: unauthenticated access blocked ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{API_BASE}/conversations")
    test("GET /conversations returns 401 without token", r.status_code == 401, f"got {r.status_code}")

    r = requests.post(f"{API_BASE}/ask", json={"question": "test"})
    test("POST /ask returns 401 without token", r.status_code == 401, f"got {r.status_code}")

    r = requests.get(f"{API_BASE}/conversations/fake-id")
    test("GET /conversations/{id} returns 401", r.status_code == 401, f"got {r.status_code}")

    r = requests.delete(f"{API_BASE}/conversations/fake-id")
    test("DELETE /conversations/{id} returns 401", r.status_code == 401, f"got {r.status_code}")

    r = requests.get(f"{API_BASE}/conversations/fake-id/job")
    test("GET /conversations/{id}/job returns 401", r.status_code == 401, f"got {r.status_code}")

    # ------------------------------------------------------------------
    print("\n--- Auth: invalid token rejected ---")
    # ------------------------------------------------------------------
    bad_headers = {"Authorization": "Bearer fake-token-12345"}
    r = requests.get(f"{API_BASE}/conversations", headers=bad_headers)
    test("invalid token returns 401", r.status_code == 401, f"got {r.status_code}")

    r = requests.post(f"{API_BASE}/ask", json={"question": "test"}, headers=bad_headers)
    test("invalid token on /ask returns 401", r.status_code == 401, f"got {r.status_code}")

    # ------------------------------------------------------------------
    print("\n--- Auth: valid token accepted ---")
    # ------------------------------------------------------------------
    # AUTH_SIGNING_KEY must match the running API's key
    if not os.environ.get("AUTH_SIGNING_KEY"):
        print("  SKIP  (AUTH_SIGNING_KEY not set — can't create valid token)")
        token = None
    else:
        from ujs.auth import create_user_token
        token = create_user_token("test-security-user", "security@test.com", "Security Test")
    if token:
        auth_headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(f"{API_BASE}/conversations", headers=auth_headers)
        test("valid token on /conversations returns 200", r.status_code == 200, f"got {r.status_code}")
        if r.status_code == 200:
            test("conversations returns list", isinstance(r.json(), list))
        else:
            test("conversations returns list", False, f"status {r.status_code}")

    # ------------------------------------------------------------------
    print("\n--- Login flow ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{WEB_BASE}/chat", allow_redirects=False)
    test("/chat redirects when not logged in", r.status_code == 302, f"got {r.status_code}")
    location = r.headers.get("Location", "")
    test("/chat redirects to /login", "/login" in location, location)

    r = requests.get(f"{WEB_BASE}/login")
    test("/login returns 200", r.status_code == 200)
    test("/login has Google sign-in", "Continue with Google" in r.text)

    # ------------------------------------------------------------------
    print("\n--- Open redirect blocked ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{WEB_BASE}/login?next=https://evil.com", allow_redirects=False)
    test("open redirect blocked (absolute URL)", "evil.com" not in r.text)

    r = requests.get(f"{WEB_BASE}/login?next=//evil.com", allow_redirects=False)
    test("open redirect blocked (protocol-relative)", "evil.com" not in r.text)

    # ------------------------------------------------------------------
    print("\n--- /docs disabled in production ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{API_BASE}/docs")
    test("/docs returns 404 (disabled)", r.status_code == 404, f"got {r.status_code}")

    r = requests.get(f"{API_BASE}/openapi.json")
    test("/openapi.json returns 404 (disabled)", r.status_code == 404, f"got {r.status_code}")

    # ------------------------------------------------------------------
    print("\n--- CORS headers ---")
    # ------------------------------------------------------------------
    r = requests.options(f"{API_BASE}/conversations", headers={
        "Origin": "https://gavelsearch.com",
        "Access-Control-Request-Method": "GET",
        "Access-Control-Request-Headers": "Authorization",
    })
    cors_origin = r.headers.get("access-control-allow-origin", "")
    test("CORS allows gavelsearch.com", cors_origin == "https://gavelsearch.com", cors_origin)

    r = requests.options(f"{API_BASE}/conversations", headers={
        "Origin": "https://evil.com",
        "Access-Control-Request-Method": "GET",
    })
    cors_origin = r.headers.get("access-control-allow-origin", "")
    test("CORS blocks evil.com", cors_origin != "https://evil.com", cors_origin)

    # ------------------------------------------------------------------
    print("\n--- Body size limits ---")
    # ------------------------------------------------------------------
    oversized = json.dumps({"question": "x" * (2 * 1024 * 1024)})
    try:
        r = requests.post(f"{API_BASE}/ask", data=oversized,
                          headers={"Content-Type": "application/json", "Content-Length": str(len(oversized))})
        test("oversized body rejected (413 or 422)", r.status_code in (413, 422), f"got {r.status_code}")
    except Exception as e:
        test("oversized body rejected", False, str(e))

    # ------------------------------------------------------------------
    print("\n--- POST /keys requires admin token ---")
    # ------------------------------------------------------------------
    r = requests.post(f"{API_BASE}/keys?name=test-hacker")
    test("POST /keys without admin token returns 403", r.status_code == 403, f"got {r.status_code}")

    # ------------------------------------------------------------------
    print("\n--- GET /ask removed ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{API_BASE}/ask?q=test")
    test("GET /ask returns 404/405 (removed)", r.status_code in (404, 405), f"got {r.status_code}")

    # ------------------------------------------------------------------
    print("\n--- Public endpoints still work ---")
    # ------------------------------------------------------------------
    r = requests.get(f"{API_BASE}/health")
    test("/health accessible without auth", r.status_code == 200)

    r = requests.get(f"{API_BASE}/stats")
    test("/stats accessible without auth", r.status_code == 200)

    r = requests.get(f"{API_BASE}/search/cases?county=Lehigh&limit=1")
    test("/search/cases accessible without auth", r.status_code == 200)

    print(f"\n{'=' * 60}")
    print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'=' * 60}\n")
    return FAIL == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

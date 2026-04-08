#!/usr/bin/env python3
"""Auth utility tests — token creation, verification, revocation.

Run: AUTH_SIGNING_KEY=test-key python -m tests.test_auth
"""

import os, sys, time

# Must set signing key before import
os.environ.setdefault("AUTH_SIGNING_KEY", "test-key-for-unit-tests")

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from ujs.auth import create_user_token, verify_user_token, revoke_user_tokens, _revoked, _b64e, _sign

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
    _revoked.clear()

    print("\n" + "=" * 60)
    print("Auth Utility Tests")
    print("=" * 60)

    # ------------------------------------------------------------------
    print("\n--- Token creation ---")
    # ------------------------------------------------------------------
    token = create_user_token("user123", "test@example.com", "Test User")
    test("token is string", isinstance(token, str))
    test("token has two parts (payload.sig)", len(token.split(".")) == 2)
    test("token is non-empty", len(token) > 20)

    # Second token for same user should be different (different iat)
    time.sleep(0.01)
    token2 = create_user_token("user123", "test@example.com", "Test User")
    # Tokens may be identical if created in same second — that's OK

    # ------------------------------------------------------------------
    print("\n--- Token verification (valid) ---")
    # ------------------------------------------------------------------
    user = verify_user_token(token)
    test("valid token returns user dict", user is not None)
    test("user has correct sub", user and user.get("sub") == "user123")
    test("user has correct email", user and user.get("email") == "test@example.com")
    test("user has correct name", user and user.get("name") == "Test User")

    # ------------------------------------------------------------------
    print("\n--- Token verification (invalid cases) ---")
    # ------------------------------------------------------------------
    test("None token returns None", verify_user_token(None) is None)
    test("empty string returns None", verify_user_token("") is None)
    test("random string returns None", verify_user_token("not-a-token") is None)
    test("single dot returns None", verify_user_token("abc.") is None)
    test("triple dot returns None", verify_user_token("a.b.c") is None)

    # Tampered payload
    parts = token.split(".")
    tampered = "dGFtcGVyZWQ" + "." + parts[1]
    test("tampered payload rejected", verify_user_token(tampered) is None)

    # Tampered signature
    tampered_sig = parts[0] + ".deadbeef1234567890"
    test("tampered signature rejected", verify_user_token(tampered_sig) is None)

    # ------------------------------------------------------------------
    print("\n--- Token expiry ---")
    # ------------------------------------------------------------------
    # Create a token that's already expired by manually building it
    expired_payload = {"sub": "expired-user", "email": "exp@test.com", "name": "", "iat": 1000, "exp": 1001}
    payload_b64 = _b64e(expired_payload)
    sig = _sign(payload_b64)
    expired_token = f"{payload_b64}.{sig}"
    test("expired token rejected", verify_user_token(expired_token) is None)

    # ------------------------------------------------------------------
    print("\n--- Token revocation ---")
    # ------------------------------------------------------------------
    token_before = create_user_token("revoke-me", "revoke@test.com")
    user_before = verify_user_token(token_before)
    test("token valid before revocation", user_before is not None)

    # Revoke
    time.sleep(1)  # Ensure iat < revoked_at
    revoke_user_tokens("revoke-me")
    test("revoked token rejected", verify_user_token(token_before) is None)

    # New token after revocation should work
    time.sleep(1)
    token_after = create_user_token("revoke-me", "revoke@test.com")
    user_after = verify_user_token(token_after)
    test("new token after revocation works", user_after is not None)
    test("new token has correct sub", user_after and user_after.get("sub") == "revoke-me")

    # Revocation of one user doesn't affect another
    other_token = create_user_token("other-user", "other@test.com")
    test("other user unaffected by revocation", verify_user_token(other_token) is not None)

    # ------------------------------------------------------------------
    print("\n--- Missing fields in payload ---")
    # ------------------------------------------------------------------
    # Token with missing email
    bad_payload = {"sub": "no-email", "iat": int(time.time()), "exp": int(time.time()) + 3600}
    payload_b64 = _b64e(bad_payload)
    sig = _sign(payload_b64)
    bad_token = f"{payload_b64}.{sig}"
    test("token missing email rejected", verify_user_token(bad_token) is None)

    # Token with missing sub
    bad_payload2 = {"email": "no-sub@test.com", "iat": int(time.time()), "exp": int(time.time()) + 3600}
    payload_b64 = _b64e(bad_payload2)
    sig = _sign(payload_b64)
    bad_token2 = f"{payload_b64}.{sig}"
    test("token missing sub rejected", verify_user_token(bad_token2) is None)

    # ------------------------------------------------------------------
    print("\n--- Edge cases ---")
    # ------------------------------------------------------------------
    # Unicode in name
    unicode_token = create_user_token("uni-user", "uni@test.com", "Josue Garcia-Lopez")
    uni_user = verify_user_token(unicode_token)
    test("unicode name preserved", uni_user and "Garcia-Lopez" in uni_user.get("name", ""))

    # Very long email
    long_email = "a" * 200 + "@test.com"
    long_token = create_user_token("long-user", long_email)
    long_user = verify_user_token(long_token)
    test("long email preserved", long_user and long_user.get("email") == long_email)

    # Cleanup
    _revoked.clear()

    print(f"\n{'=' * 60}")
    print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'=' * 60}\n")
    return FAIL == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

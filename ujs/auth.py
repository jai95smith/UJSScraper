"""Shared auth utilities — JWT token creation/verification for user identity."""

import os, time, hmac, hashlib, json, base64, logging

logger = logging.getLogger("ujs.auth")

# Token blocklist: {user_sub: revoked_at_timestamp}
# Tokens issued before revoked_at are rejected.
_revoked = {}


def _get_signing_key():
    key = os.environ.get("AUTH_SIGNING_KEY")
    if not key:
        raise RuntimeError("AUTH_SIGNING_KEY env var is required")
    return key


def _b64e(data):
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")


def _b64d(s):
    s += "=" * (4 - len(s) % 4)
    return json.loads(base64.urlsafe_b64decode(s))


def _sign(payload_b64):
    return hmac.new(_get_signing_key().encode(), payload_b64.encode(), hashlib.sha256).hexdigest()


def create_user_token(user_id, email, name=""):
    """Create a signed token encoding user identity. Expires in 7 days."""
    payload = {
        "sub": user_id, "email": email, "name": name,
        "iat": int(time.time()),
        "exp": int(time.time()) + 7 * 86400,
    }
    payload_b64 = _b64e(payload)
    sig = _sign(payload_b64)
    return f"{payload_b64}.{sig}"


def revoke_user_tokens(user_sub):
    """Revoke all tokens for a user (e.g. on logout). Tokens issued before now are rejected."""
    _revoked[user_sub] = time.time()


def verify_user_token(token):
    """Verify token signature and expiry. Returns {sub, email, name} or None."""
    if not token:
        return None
    try:
        parts = token.split(".")
        if len(parts) != 2:
            logger.warning("Auth failure: malformed token")
            return None
        payload_b64, sig = parts
        if not hmac.compare_digest(_sign(payload_b64), sig):
            logger.warning("Auth failure: invalid signature")
            return None
        payload = _b64d(payload_b64)
        if payload.get("exp", 0) < time.time():
            logger.info("Auth failure: expired token for %s", payload.get("email", "unknown"))
            return None
        # Require essential fields
        sub = payload.get("sub")
        email = payload.get("email")
        if not sub or not email:
            logger.warning("Auth failure: missing sub or email in token")
            return None
        # Check revocation
        iat = payload.get("iat", 0)
        if sub in _revoked and iat <= _revoked[sub]:
            logger.info("Auth failure: revoked token for %s", email)
            return None
        return {"sub": sub, "email": email, "name": payload.get("name", "")}
    except Exception:
        logger.warning("Auth failure: token decode error")
        return None


def get_user_from_request(request):
    """Extract user from FastAPI/Starlette request Authorization header. Returns dict or None."""
    auth = request.headers.get("authorization", "")
    if auth.startswith("Bearer "):
        return verify_user_token(auth[7:])
    return None

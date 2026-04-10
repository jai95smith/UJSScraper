"""Shared auth utilities — JWT token creation/verification for user identity."""

import os, time, hmac, hashlib, json, base64, logging

logger = logging.getLogger("ujs.auth")

# Token blocklist: persisted in Redis, in-memory fallback
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
    ts = time.time()
    _revoked[user_sub] = ts
    try:
        from ujs.cache import _get_redis
        r = _get_redis()
        if r:
            r.set(f"ujs:revoked:{user_sub}", str(ts), ex=8 * 86400)
    except Exception:
        pass


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
        # Check revocation (in-memory first, then Redis)
        iat = payload.get("iat", 0)
        revoked_at = _revoked.get(sub)
        if not revoked_at:
            try:
                from ujs.cache import _get_redis
                r = _get_redis()
                if r:
                    val = r.get(f"ujs:revoked:{sub}")
                    if val:
                        revoked_at = float(val)
                        _revoked[sub] = revoked_at
            except Exception:
                pass
        if revoked_at and iat <= revoked_at:
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

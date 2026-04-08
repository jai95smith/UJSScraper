"""Watch routes — user-authenticated docket monitoring and preferences."""

import re, time
from collections import defaultdict
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional

from ujs import db
from ujs.auth import get_user_from_request

router = APIRouter(tags=["Watches"])

# Rate limiting: user_id -> list of timestamps
_watch_rates = defaultdict(list)
_WATCH_RATE_LIMIT = 5  # watches per minute
_PREFS_RATE_LIMIT = 10  # preference updates per minute
_WATCH_RATE_WINDOW = 60

# Valid PA docket format
_DOCKET_RE = re.compile(r'^[A-Z]{2}-\d{2,5}-[A-Z]{2}-\d{5,7}-\d{4}$')


def _require_user(request: Request):
    user = get_user_from_request(request)
    if not user:
        return None
    return user


def _check_rate(key, limit):
    now = time.time()
    _watch_rates[key] = [t for t in _watch_rates[key] if now - t < _WATCH_RATE_WINDOW]
    if len(_watch_rates[key]) >= limit:
        return True
    _watch_rates[key].append(now)
    return False


class WatchRequest(BaseModel):
    docket_number: str = Field(..., min_length=1, max_length=50)
    label: Optional[str] = Field(None, max_length=100)
    notify_frequency: Optional[str] = Field('daily', pattern='^(immediate|daily|none)$')


class PreferencesUpdate(BaseModel):
    email_alerts: Optional[bool] = None
    weekly_digest: Optional[bool] = None
    notify_frequency: Optional[str] = Field(None, pattern='^(immediate|daily|none)$')


# --- Watches ---

@router.post("/watches")
def add_watch(body: WatchRequest, request: Request):
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    if not _DOCKET_RE.match(body.docket_number):
        return JSONResponse(status_code=400, content={"error": "Invalid docket number format"})
    if _check_rate(f"watch:{user['sub']}", _WATCH_RATE_LIMIT):
        return JSONResponse(status_code=429, content={"error": "Too many watch requests. Try again in a minute."})
    with db.connect() as conn:
        wid = db.add_user_watch(conn, user["sub"], user["email"], body.docket_number,
                                label=body.label, notify_frequency=body.notify_frequency or 'daily')
        if wid is None:
            return JSONResponse(status_code=400, content={"error": "Watch limit reached (25 max)"})
        return {"id": wid, "docket_number": body.docket_number, "status": "watching"}


@router.get("/watches")
def list_watches(request: Request):
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    with db.connect() as conn:
        watches = db.get_user_watches(conn, user["sub"])
        return watches


@router.delete("/watches/{docket_number:path}")
def remove_watch(docket_number: str, request: Request):
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    with db.connect() as conn:
        removed = db.remove_user_watch(conn, user["sub"], docket_number)
        if not removed:
            return JSONResponse(status_code=404, content={"error": "Watch not found"})
        return {"status": "removed"}


@router.get("/watches/{docket_number:path}/status")
def watch_status(docket_number: str, request: Request):
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    with db.connect() as conn:
        watching = db.is_watching(conn, user["sub"], docket_number)
        return {"docket_number": docket_number, "watching": watching}


# --- Preferences ---

@router.get("/preferences")
def get_preferences(request: Request):
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    with db.connect() as conn:
        prefs = db.get_or_create_preferences(conn, user["sub"])
        # Don't expose unsubscribe_token to client
        prefs.pop("unsubscribe_token", None)
        return prefs


@router.put("/preferences")
def update_preferences(body: PreferencesUpdate, request: Request):
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    if _check_rate(f"prefs:{user['sub']}", _PREFS_RATE_LIMIT):
        return JSONResponse(status_code=429, content={"error": "Too many updates. Try again in a minute."})
    updates = {k: v for k, v in body.dict().items() if v is not None}
    if not updates:
        return {"status": "no changes"}
    with db.connect() as conn:
        db.get_or_create_preferences(conn, user["sub"])  # ensure row exists
        db.update_preferences(conn, user["sub"], **updates)
        return {"status": "updated"}

"""Admin routes — Watchlist, Webhooks, API Keys, Ingest, Health."""

from fastapi import APIRouter, Query, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List

from ujs import db

router = APIRouter()


def _get_key(x_api_key=None):
    if not x_api_key: return "public"
    with db.connect() as conn:
        valid = db.validate_api_key(conn, x_api_key)
        if not valid: raise HTTPException(401, "Invalid or rate-limited API key")
    return x_api_key


# --- Watchlist ---

class WatchlistAdd(BaseModel):
    docket_number: str
    label: Optional[str] = None


@router.get("/watchlist", tags=["Watchlist"])
def get_watchlist(x_api_key: str = Header(...)):
    key = _get_key(x_api_key)
    with db.connect() as conn:
        return [dict(r) for r in db.get_watchlist(conn, key)]


@router.post("/watchlist", tags=["Watchlist"])
def add_watchlist(body: WatchlistAdd, x_api_key: str = Header(...)):
    key = _get_key(x_api_key)
    with db.connect() as conn:
        wid = db.add_to_watchlist(conn, key, body.docket_number, body.label)
        return {"id": wid, "docket_number": body.docket_number, "status": "watching"}


@router.delete("/watchlist/{docket_number}", tags=["Watchlist"])
def remove_watchlist(docket_number: str, x_api_key: str = Header(...)):
    key = _get_key(x_api_key)
    with db.connect() as conn:
        if not db.remove_from_watchlist(conn, key, docket_number):
            raise HTTPException(404, "Not in watchlist")
        return {"status": "removed"}


@router.get("/watchlist/changes", tags=["Watchlist"])
def watchlist_changes(since: Optional[str] = None, x_api_key: str = Header(...)):
    from datetime import datetime
    key = _get_key(x_api_key)
    since_dt = datetime.fromisoformat(since) if since else None
    with db.connect() as conn:
        return [dict(c) for c in db.get_watchlist_changes(conn, key, since=since_dt)]


# --- Webhooks ---

class WebhookCreate(BaseModel):
    url: str
    events: Optional[List[str]] = None
    county: Optional[str] = None
    docket_type: Optional[str] = None


@router.get("/webhooks", tags=["Webhooks"])
def list_webhooks(x_api_key: str = Header(...)):
    key = _get_key(x_api_key)
    with db.connect() as conn:
        return [dict(w) for w in db.get_webhooks(conn, key)]


@router.post("/webhooks", tags=["Webhooks"])
def create_webhook(body: WebhookCreate, x_api_key: str = Header(...)):
    key = _get_key(x_api_key)
    with db.connect() as conn:
        wid = db.create_webhook(conn, key, body.url, events=body.events,
                                county=body.county, docket_type=body.docket_type)
        return {"id": wid, "url": body.url, "status": "active"}


@router.delete("/webhooks/{webhook_id}", tags=["Webhooks"])
def delete_webhook(webhook_id: int, x_api_key: str = Header(...)):
    key = _get_key(x_api_key)
    with db.connect() as conn:
        if not db.delete_webhook(conn, key, webhook_id):
            raise HTTPException(404, "Webhook not found")
        return {"status": "deleted"}


# --- Ingest ---

@router.get("/ingest/{docket_number}/status", tags=["Ingest"])
def ingest_status(docket_number: str):
    import psycopg2.extras
    with db.connect() as conn:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT id, status, requested_at, started_at, completed_at, error FROM ingest_queue WHERE docket_number = %s ORDER BY requested_at DESC LIMIT 1", (docket_number,))
        job = cur.fetchone()
        if not job:
            case = db.get_case(conn, docket_number)
            if case:
                return {"status": "indexed", "last_scraped": case["last_scraped"].isoformat() if case["last_scraped"] else None}
            return {"status": "unknown", "docket_number": docket_number}
        return dict(job)


# --- API Keys ---

@router.post("/keys", tags=["API Keys"])
def create_key(name: str, email: Optional[str] = None, admin_token: str = Header(None, alias="x-admin-token")):
    import os, hmac
    expected = os.environ.get("ADMIN_TOKEN", "")
    if not expected or not hmac.compare_digest(admin_token or "", expected):
        raise HTTPException(status_code=403, detail="Admin token required.")
    with db.connect() as conn:
        key = db.create_api_key(conn, name, email)
        return {"key": key, "name": name, "daily_limit": 1000}


# --- Health ---

@router.get("/health", tags=["Health"])
def health():
    try:
        with db.connect() as conn:
            s = db.get_stats(conn)
        return {"status": "ok", "db": "connected", "cases_indexed": s["cases"]}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "detail": str(e)})

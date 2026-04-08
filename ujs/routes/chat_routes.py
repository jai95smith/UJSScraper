"""Chat routes — server-side conversations with job-based responses."""

import json, time, uuid
from collections import defaultdict
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List

from ujs import db
from ujs.auth import get_user_from_request

router = APIRouter(tags=["Chat"])

# Rate limiting: key -> list of timestamps
_rate_limits = defaultdict(list)
_RATE_LIMIT_IP = 10  # requests per minute per IP
_RATE_LIMIT_USER = 20  # requests per minute per user (more generous)
_RATE_WINDOW = 60  # seconds


def _check_rate(key, limit):
    """Check if key has exceeded limit in the window."""
    now = time.time()
    _rate_limits[key] = [t for t in _rate_limits[key] if now - t < _RATE_WINDOW]
    if len(_rate_limits[key]) >= limit:
        return True
    _rate_limits[key].append(now)
    return False


def _check_rate_limit(request: Request, user=None):
    """Returns True if rate limited. Checks both IP and user."""
    ip = request.client.host if request.client else "unknown"
    if _check_rate(f"ip:{ip}", _RATE_LIMIT_IP):
        return True
    if user and _check_rate(f"user:{user['sub']}", _RATE_LIMIT_USER):
        return True
    return False


def _require_user(request: Request):
    """Extract user from auth header. Returns user dict or raises 401."""
    user = get_user_from_request(request)
    if not user:
        return None
    return user


class AskRequest(BaseModel):
    question: str = Field(..., max_length=2000)
    conversation_id: Optional[str] = None


def _nanoid():
    """Generate a cryptographically random 16-char ID."""
    return uuid.uuid4().hex[:16]


# --- Conversations ---

@router.post("/conversations")
def create_conversation(request: Request):
    """Create a new empty conversation."""
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    cid = _nanoid()
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO conversations (id, user_id, user_email, user_name) VALUES (%s, %s, %s, %s)",
                    (cid, user["sub"], user["email"], user.get("name", "")))
    return {"id": cid}


@router.get("/conversations")
def list_conversations(request: Request, limit: int = Query(30, le=100)):
    """List recent conversations for the authenticated user."""
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    with db.connect() as conn:
        cur = db._dict_cur(conn)
        cur.execute("SELECT id, title, created_at, updated_at FROM conversations WHERE user_id = %s ORDER BY updated_at DESC LIMIT %s",
                    (user["sub"], limit))
        return [dict(r) for r in cur.fetchall()]


@router.get("/conversations/{cid}")
def get_conversation(cid: str, request: Request):
    """Get full conversation with messages. Must belong to user."""
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    with db.connect() as conn:
        cur = db._dict_cur(conn)
        cur.execute("SELECT * FROM conversations WHERE id = %s AND user_id = %s", (cid, user["sub"]))
        row = cur.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Conversation not found"})
        return dict(row)


@router.delete("/conversations/{cid}")
def delete_conversation(cid: str, request: Request):
    """Delete a conversation. Must belong to user."""
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM chat_jobs WHERE conversation_id = %s AND conversation_id IN (SELECT id FROM conversations WHERE user_id = %s)",
                    (cid, user["sub"]))
        cur.execute("DELETE FROM conversations WHERE id = %s AND user_id = %s", (cid, user["sub"]))
    return {"status": "deleted"}


@router.get("/conversations/{cid}/job")
def get_conversation_job(cid: str, request: Request):
    """Get the latest job for a conversation (for resume on page reload)."""
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    # Verify conversation ownership
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM conversations WHERE id = %s AND user_id = %s", (cid, user["sub"]))
        if not cur.fetchone():
            return JSONResponse(status_code=404, content={"error": "Conversation not found"})
        cur = db._dict_cur(conn)
        cur.execute("""
            SELECT id as job_id, status, error FROM chat_jobs
            WHERE conversation_id = %s ORDER BY created_at DESC LIMIT 1
        """, (cid,))
        row = cur.fetchone()
        if not row:
            return {"job_id": None, "status": "none"}
        return dict(row)


# --- Ask (creates job, appends to conversation) ---

@router.post("/ask")
def ask(body: AskRequest, request: Request):
    """Submit a question. Creates conversation if needed, starts background job."""
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    if _check_rate_limit(request, user):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded. Try again in a minute."})
    from ujs.chat.jobs import create_job

    cid = body.conversation_id

    # Create conversation if none provided
    if not cid:
        cid = _nanoid()
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO conversations (id, title, user_id, user_email, user_name) VALUES (%s, %s, %s, %s, %s)",
                        (cid, body.question[:60], user["sub"], user["email"], user.get("name", "")))
    else:
        # Verify ownership of existing conversation
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM conversations WHERE id = %s AND user_id = %s", (cid, user["sub"]))
            if not cur.fetchone():
                return JSONResponse(status_code=403, content={"error": "Access denied"})

    # Get existing messages for context
    history = []
    with db.connect() as conn:
        cur = db._dict_cur(conn)
        cur.execute("SELECT messages FROM conversations WHERE id = %s", (cid,))
        row = cur.fetchone()
        if row and row["messages"]:
            history = row["messages"]

    # Append user message to conversation
    history.append({"role": "user", "content": body.question})
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE conversations SET messages = %s, updated_at = NOW() WHERE id = %s",
                    (json.dumps(history), cid))

    # Set title from first message
    if len(history) == 1:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE conversations SET title = %s WHERE id = %s AND title = ''",
                        (body.question[:60], cid))

    # Create background job
    job_id = create_job(body.question, history=history[-10:], conversation_id=cid)

    return {"job_id": job_id, "conversation_id": cid, "status": "running"}


# --- Job polling ---

@router.get("/ask/job/{job_id}")
def job_status(job_id: str, after: int = Query(0), cid: str = Query(None), request: Request = None):
    """Poll for job progress. Verifies user owns the conversation."""
    user = _require_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentication required"})
    from ujs.chat.jobs import get_job
    job = get_job(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})
    # Verify user owns the conversation this job belongs to
    conv_id = job.get("conversation_id")
    if conv_id:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM conversations WHERE id = %s AND user_id = %s", (conv_id, user["sub"]))
            if not cur.fetchone():
                return JSONResponse(status_code=403, content={"error": "Access denied"})

    response = job.get("response", "")
    return {
        "job_id": job_id,
        "conversation_id": job.get("conversation_id"),
        "status": job["status"],
        "tools": job.get("tools_log", []),
        "response": response[after:],
        "total_length": len(response),
        "error": job.get("error"),
    }

"""Chat routes — server-side conversations with job-based responses."""

import json, time, uuid
from collections import defaultdict
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import Optional, List

from ujs import db

router = APIRouter(tags=["Chat"])

# Rate limiting: IP -> list of timestamps
_rate_limits = defaultdict(list)
_RATE_LIMIT = 10  # requests per minute
_RATE_WINDOW = 60  # seconds


def _check_rate_limit(request: Request):
    """Returns True if rate limited."""
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    timestamps = _rate_limits[ip]
    # Prune old entries
    _rate_limits[ip] = [t for t in timestamps if now - t < _RATE_WINDOW]
    if len(_rate_limits[ip]) >= _RATE_LIMIT:
        return True
    _rate_limits[ip].append(now)
    return False


class AskRequest(BaseModel):
    question: str = Field(..., max_length=2000)
    conversation_id: Optional[str] = None


def _nanoid():
    """Generate a cryptographically random 16-char ID."""
    return uuid.uuid4().hex[:16]


# --- Conversations ---

@router.post("/conversations")
def create_conversation():
    """Create a new empty conversation."""
    cid = _nanoid()
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO conversations (id) VALUES (%s)", (cid,))
    return {"id": cid}


@router.get("/conversations")
def list_conversations(limit: int = Query(30, le=100)):
    """List recent conversations."""
    with db.connect() as conn:
        cur = db._dict_cur(conn)
        cur.execute("SELECT id, title, created_at, updated_at FROM conversations ORDER BY updated_at DESC LIMIT %s", (limit,))
        return [dict(r) for r in cur.fetchall()]


@router.get("/conversations/{cid}")
def get_conversation(cid: str):
    """Get full conversation with messages."""
    with db.connect() as conn:
        cur = db._dict_cur(conn)
        cur.execute("SELECT * FROM conversations WHERE id = %s", (cid,))
        row = cur.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "Conversation not found"})
        return dict(row)


@router.delete("/conversations/{cid}")
def delete_conversation(cid: str):
    """Delete a conversation."""
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM chat_jobs WHERE conversation_id = %s", (cid,))
        cur.execute("DELETE FROM conversations WHERE id = %s", (cid,))
    return {"status": "deleted"}


@router.get("/conversations/{cid}/job")
def get_conversation_job(cid: str):
    """Get the latest job for a conversation (for resume on page reload)."""
    with db.connect() as conn:
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
    if _check_rate_limit(request):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded. Try again in a minute."})
    from ujs.chat.jobs import create_job

    cid = body.conversation_id

    # Create conversation if none provided
    if not cid:
        cid = _nanoid()
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO conversations (id, title) VALUES (%s, %s)", (cid, body.question[:60]))

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


@router.get("/ask")
def ask_get(q: str = Query(..., max_length=2000), request: Request = None):
    """Submit via GET."""
    return ask(AskRequest(question=q), request)


# --- Job polling ---

@router.get("/ask/job/{job_id}")
def job_status(job_id: str, after: int = Query(0)):
    """Poll for job progress."""
    from ujs.chat.jobs import get_job
    job = get_job(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})

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

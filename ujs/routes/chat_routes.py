"""Chat routes — server-side conversations with job-based responses."""

import json, uuid
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List

from ujs import db

router = APIRouter(tags=["Chat"])


class AskRequest(BaseModel):
    question: str
    conversation_id: Optional[str] = None


def _nanoid():
    return str(uuid.uuid4())[:8]


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


# --- Ask (creates job, appends to conversation) ---

@router.post("/ask")
def ask(body: AskRequest):
    """Submit a question. Creates conversation if needed, starts background job."""
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
def ask_get(q: str = Query(...)):
    """Submit via GET."""
    return ask(AskRequest(question=q))


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

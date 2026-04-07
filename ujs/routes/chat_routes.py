"""Chat routes — job-based with polling."""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter(tags=["Chat"])


class AskRequest(BaseModel):
    question: str
    history: Optional[List[dict]] = None


@router.post("/ask")
def ask_post(body: AskRequest):
    """Submit a question — returns job_id. Poll /ask/job/{id} for response."""
    from ujs.chat.jobs import create_job
    job_id = create_job(body.question, history=body.history)
    return {"job_id": job_id, "status": "running"}


@router.get("/ask")
def ask_get(q: str = Query(...)):
    """Submit via GET — returns job_id."""
    from ujs.chat.jobs import create_job
    job_id = create_job(q)
    return {"job_id": job_id, "status": "running"}


@router.get("/ask/job/{job_id}")
def ask_job_status(job_id: str, after: int = Query(0, description="Return response text after this character position")):
    """Poll for job progress. Pass after=len(seen) to get only new text."""
    from ujs.chat.jobs import get_job
    job = get_job(job_id)
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})

    response = job.get("response", "")
    return {
        "job_id": job_id,
        "status": job["status"],
        "tools": job.get("tools_log", []),
        "response": response[after:],
        "total_length": len(response),
        "error": job.get("error"),
    }

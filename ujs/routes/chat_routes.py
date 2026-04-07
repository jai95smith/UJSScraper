"""Chat + Ask routes."""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter(tags=["Chat"])


class AskRequest(BaseModel):
    question: str


class AskStreamRequest(BaseModel):
    question: str
    history: Optional[List[dict]] = None


def _sse_wrap(gen):
    for chunk in gen:
        yield f"data: {chunk.replace(chr(10), '\\n')}\n\n"
    yield "data: [DONE]\n\n"


@router.post("/ask")
def ask_post(body: AskRequest):
    from ujs.chat import ask
    try:
        return {"question": body.question, "answer": ask(body.question)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/ask")
def ask_get(q: str = Query(...)):
    from ujs.chat import ask
    try:
        return {"question": q, "answer": ask(q)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.get("/ask/stream")
async def ask_stream_get(q: str = Query(...)):
    from ujs.chat import ask_stream
    return StreamingResponse(_sse_wrap(ask_stream(q)), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.post("/ask/stream")
async def ask_stream_post(body: AskStreamRequest):
    from ujs.chat import ask_stream
    return StreamingResponse(_sse_wrap(ask_stream(body.question, history=body.history)),
                             media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

"""PA UJS Court Search API — DB-first with async ingest."""

import threading, time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ujs import db
from ujs.routes import search, docket, analytics, chat_routes, rapsheet, admin, watch_routes


# -------------------------------------------------------------------
# Background queue worker
# -------------------------------------------------------------------

_worker_running = False


def _queue_worker():
    global _worker_running
    _worker_running = True
    while _worker_running:
        try:
            with db.connect() as conn:
                job = db.claim_ingest_job(conn)
            if job:
                job_id, docket_number = job
                print(f"[worker] Processing {docket_number}")
                try:
                    from ujs.modules.ingest import deep_analyze_docket
                    deep_analyze_docket(docket_number)
                    with db.connect() as conn:
                        db.complete_ingest_job(conn, job_id)
                    print(f"[worker] Done {docket_number}")
                except Exception as e:
                    print(f"[worker] Error {docket_number}: {e}")
                    with db.connect() as conn:
                        db.complete_ingest_job(conn, job_id, error=str(e))
            else:
                time.sleep(10)
        except Exception as e:
            print(f"[worker] Connection error: {e}")
            time.sleep(30)


@asynccontextmanager
async def lifespan(app):
    t = threading.Thread(target=_queue_worker, daemon=True)
    t.start()
    print("[worker] Queue worker started")
    yield
    global _worker_running
    _worker_running = False


# -------------------------------------------------------------------
# App
# -------------------------------------------------------------------

import os as _os
_is_prod = _os.environ.get('FLASK_ENV') != 'development'

app = FastAPI(
    title="PA UJS Court Search API",
    description="Programmatic access to Pennsylvania Unified Judicial System court records.",
    version="2.0.0",
    lifespan=lifespan,
    docs_url=None if _is_prod else "/docs",
    redoc_url=None if _is_prod else "/redoc",
    openapi_url=None if _is_prod else "/openapi.json",
)

_ALLOWED_ORIGINS = [
    "https://gavelsearch.com",
    "https://www.gavelsearch.com",
    "http://localhost:8000",  # local Flask dev
    "http://localhost:3000",  # local dev
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
    max_age=3600,
)


from starlette.middleware.base import BaseHTTPMiddleware

_MAX_BODY = 1 * 1024 * 1024  # 1MB

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Reject oversized bodies
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > _MAX_BODY:
            return JSONResponse(status_code=413, content={"error": "Request too large"})
        response = await call_next(request)
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
        return response

app.add_middleware(SecurityHeadersMiddleware)


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    import traceback, logging
    logging.error(f"Unhandled exception on {request.url.path}: {exc}")
    traceback.print_exc()  # Server-side only
    return JSONResponse(status_code=500, content={"error": "An internal error occurred."})


# Mount routers
app.include_router(search.router)
app.include_router(docket.router)
app.include_router(analytics.router)
app.include_router(chat_routes.router)
app.include_router(rapsheet.router)
app.include_router(admin.router)
app.include_router(watch_routes.router)

"""PA UJS Court Search API — DB-first with async ingest."""

import threading, time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ujs import db
from ujs.routes import search, docket, analytics, chat_routes, rapsheet, admin


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

app = FastAPI(
    title="PA UJS Court Search API",
    description="Programmatic access to Pennsylvania Unified Judicial System court records.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    import traceback
    traceback.print_exc()
    return JSONResponse(status_code=500, content={"error": str(exc)})


# Mount routers
app.include_router(search.router)
app.include_router(docket.router)
app.include_router(analytics.router)
app.include_router(chat_routes.router)
app.include_router(rapsheet.router)
app.include_router(admin.router)

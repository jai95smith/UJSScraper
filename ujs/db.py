"""Database layer for UJS court data."""

import hashlib, json, os, logging
from contextlib import contextmanager
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
import psycopg2.pool

logger = logging.getLogger("ujs.db")

def get_db_url():
    return os.environ["DATABASE_URL"]


# Connection pool — reuses connections instead of opening/closing each time.
# min=2 idle connections, max=20 concurrent connections.
_pool = None


def _get_pool():
    global _pool
    if _pool is None or _pool.closed:
        _pool = psycopg2.pool.ThreadedConnectionPool(2, 20, get_db_url())
        logger.info("DB connection pool created (2-20 connections)")
    return _pool


@contextmanager
def connect():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def _hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()


def _dict_cur(conn):
    """Shorthand for a RealDictCursor."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def log_event(component, event, docket_number=None, detail=None, duration_ms=None, success=True):
    """Log a system event for tracking/debugging."""
    try:
        with connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO system_log (component, event, docket_number, detail, duration_ms, success)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (component, event, docket_number, str(detail)[:500] if detail else None, duration_ms, success))
    except Exception:
        pass  # logging should never crash the app


def _case_type_code(docket_type):
    """Convert case type name to docket number pattern."""
    return {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-",
            "non-traffic": "-NT-", "landlord/tenant": "-LT-"}.get((docket_type or "").lower(), "")


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def upsert_case(conn, case):
    """Upsert a case from search results."""
    # Ensure all expected keys exist with defaults
    defaults = {
        "docket_number": "", "court_type": "", "caption": "", "status": "",
        "filing_date": "", "participant": "", "dob": "", "county": "",
        "state": "PA", "court_office": "", "otn": "", "complaint": "", "incident": "",
        "docket_sheet_url": None, "court_summary_url": None,
    }
    case = {**defaults, **case}
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cases (docket_number, court_type, caption, status, filing_date,
                           county, court_office, otn, complaint, incident,
                           docket_sheet_url, court_summary_url, last_scraped, updated_at)
        VALUES (%(docket_number)s, %(court_type)s, %(caption)s, %(status)s, %(filing_date)s,
                %(county)s, %(court_office)s, %(otn)s, %(complaint)s, %(incident)s,
                %(docket_sheet_url)s, %(court_summary_url)s, NOW(), NOW())
        ON CONFLICT (docket_number) DO UPDATE SET
            court_type = EXCLUDED.court_type,
            caption = EXCLUDED.caption,
            status = EXCLUDED.status,
            filing_date = EXCLUDED.filing_date,
            county = EXCLUDED.county,
            court_office = EXCLUDED.court_office,
            otn = EXCLUDED.otn,
            complaint = EXCLUDED.complaint,
            incident = EXCLUDED.incident,
            docket_sheet_url = EXCLUDED.docket_sheet_url,
            court_summary_url = EXCLUDED.court_summary_url,
            last_scraped = NOW(),
            updated_at = NOW()
        RETURNING (xmax = 0) AS is_new
    """, case)
    row = cur.fetchone()
    is_new = row[0] if row else False

    # Store participant from search results if present
    participant = " ".join(case.get("participant", "").split())
    # For appellate cases without participant, extract from caption
    if not participant and case.get("court_type") == "Appellate":
        caption = case.get("caption", "")
        if " v. " in caption:
            parts = caption.split(" v. ", 1)
            for part in parts:
                name = part.strip().split(",")[0].strip() if "," in part else part.strip()
                if name and len(name) > 2:
                    cur.execute("""
                        INSERT INTO participants (docket_number, name, role)
                        VALUES (%s, %s, 'party')
                        ON CONFLICT (docket_number, name, role) DO NOTHING
                    """, (case["docket_number"], name))
    if participant:
        cur.execute("""
            INSERT INTO participants (docket_number, name, dob, role)
            VALUES (%s, %s, %s, 'defendant')
            ON CONFLICT (docket_number, name, role) DO UPDATE SET dob = EXCLUDED.dob
        """, (case["docket_number"], participant, case.get("dob", "") or None))

    return is_new


def upsert_cases(conn, cases):
    """Upsert multiple cases. Returns (total, new_count)."""
    new_count = 0
    for case in cases:
        if upsert_case(conn, case):
            new_count += 1
    return len(cases), new_count


def get_case(conn, docket_number):
    """Get a case by docket number."""
    cur = _dict_cur(conn)
    cur.execute("SELECT * FROM cases WHERE docket_number = %s", (docket_number,))
    return cur.fetchone()


def search_cases(conn, county=None, status=None, docket_type=None,
                 filed_after=None, filed_before=None, name=None, limit=100):
    """Search cases in the database."""
    cur = _dict_cur(conn)
    clauses = []
    params = []

    if county:
        clauses.append("c.county ILIKE %s")
        params.append(county)
    if status:
        clauses.append("c.status ILIKE %s")
        params.append(f"%{status}%")
    if docket_type:
        code = _case_type_code(docket_type)
        if code:
            clauses.append("c.docket_number LIKE %s")
            params.append(f"%{code}%")
    if filed_after:
        clauses.append("TO_DATE(c.filing_date, 'MM/DD/YYYY') >= TO_DATE(%s, 'MM/DD/YYYY')")
        params.append(filed_after)
    if filed_before:
        clauses.append("TO_DATE(c.filing_date, 'MM/DD/YYYY') <= TO_DATE(%s, 'MM/DD/YYYY')")
        params.append(filed_before)
    if name:
        # Handle "First Last" → also search "Last, First" and each word separately
        name_parts = name.strip().split()
        if len(name_parts) >= 2:
            # "Kelli Murphy" → search for "%Kelli%Murphy%" AND "%Murphy%Kelli%"
            forward = f"%{'%'.join(name_parts)}%"
            reverse = f"%{name_parts[-1]}%{name_parts[0]}%"
            clauses.append("""(
                c.caption ILIKE %s OR c.caption ILIKE %s
                OR EXISTS (
                    SELECT 1 FROM participants p
                    WHERE p.docket_number = c.docket_number
                    AND (p.name ILIKE %s OR p.name ILIKE %s)
                )
            )""")
            params.extend([forward, reverse, forward, reverse])
        else:
            clauses.append("""(
                c.caption ILIKE %s
                OR EXISTS (
                    SELECT 1 FROM participants p
                    WHERE p.docket_number = c.docket_number AND p.name ILIKE %s
                )
            )""")
            params.extend([f"%{name}%", f"%{name}%"])

    where = " AND ".join(clauses) if clauses else "TRUE"
    params.append(limit)

    cur.execute(f"""
        SELECT c.* FROM cases c
        WHERE {where}
        ORDER BY TO_DATE(c.filing_date, 'MM/DD/YYYY') DESC
        LIMIT %s
    """, params)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Analysis (Gemini-parsed data)
# ---------------------------------------------------------------------------

def store_analysis(conn, docket_number, analysis, doc_type="docket"):
    """Store Gemini-parsed analysis JSON."""
    # Strip transient fields before storing
    analysis = {k: v for k, v in analysis.items() if k not in ("pdf_path", "full_text")}
    h = _hash(analysis)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO analyses (docket_number, doc_type, analysis, data_hash, parsed_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (docket_number, doc_type) DO UPDATE SET
            analysis = EXCLUDED.analysis,
            data_hash = EXCLUDED.data_hash,
            parsed_at = NOW()
        RETURNING (xmax = 0) AS is_new
    """, (docket_number, doc_type, json.dumps(analysis), h))
    return cur.fetchone()[0]


def get_analysis(conn, docket_number, doc_type="docket"):
    """Get cached Gemini analysis."""
    cur = _dict_cur(conn)
    cur.execute("""
        SELECT analysis, data_hash, parsed_at FROM analyses
        WHERE docket_number = %s AND doc_type = %s
    """, (docket_number, doc_type))
    row = cur.fetchone()
    if row:
        return row["analysis"]
    return None


# ---------------------------------------------------------------------------
# Structured data upserts (from Gemini analysis)
# ---------------------------------------------------------------------------

def store_parsed_data(conn, docket_number, analysis):
    """Break down a Gemini analysis and store in normalized tables.
    Clears and re-inserts mutable data (sentences, entries) to avoid stale rows."""
    cur = conn.cursor()

    # Participant/defendant
    defendant = analysis.get("defendant") or analysis.get("person", {})
    if defendant and defendant.get("name"):
        cur.execute("""
            INSERT INTO participants (docket_number, name, dob, address, role)
            VALUES (%s, %s, %s, %s, 'defendant')
            ON CONFLICT (docket_number, name, role) DO UPDATE SET
                dob = EXCLUDED.dob, address = EXCLUDED.address
        """, (docket_number, defendant.get("name"), defendant.get("dob"),
              defendant.get("address")))

    # Charges — upsert by seq
    for charge in analysis.get("charges", []):
        cur.execute("""
            INSERT INTO charges (docket_number, seq, statute, description, grade,
                                 offense_date, otn, disposition, disposition_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (docket_number, seq) DO UPDATE SET
                statute = EXCLUDED.statute, description = EXCLUDED.description,
                grade = EXCLUDED.grade, disposition = EXCLUDED.disposition,
                disposition_date = EXCLUDED.disposition_date
        """, (docket_number, charge.get("seq"), charge.get("statute"),
              charge.get("description"), charge.get("grade"),
              charge.get("offense_date"), charge.get("otn"),
              charge.get("disposition"), charge.get("disposition_date")))

    # Bail — upsert single row per docket
    bail = analysis.get("bail", {})
    if bail and bail.get("amount"):
        cur.execute("""
            INSERT INTO bail (docket_number, bail_type, amount, status, posting_date)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (docket_number) DO UPDATE SET
                bail_type = EXCLUDED.bail_type, amount = EXCLUDED.amount,
                status = EXCLUDED.status, posting_date = EXCLUDED.posting_date
        """, (docket_number, bail.get("type"), bail.get("amount"),
              bail.get("status"), bail.get("posting_date")))

    # Sentences — clear and re-insert, skip duplicates
    cur.execute("DELETE FROM sentences WHERE docket_number = %s", (docket_number,))
    seen_sents = set()
    for sent in analysis.get("sentences", []):
        key = (sent.get("charge"), sent.get("sentence_type"), sent.get("duration"), sent.get("sentence_date"))
        if key in seen_sents:
            continue
        seen_sents.add(key)
        cur.execute("""
            INSERT INTO sentences (docket_number, charge, sentence_type, duration,
                                   conditions, sentence_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (docket_number, sent.get("charge"), sent.get("sentence_type"),
              sent.get("duration"), sent.get("conditions"), sent.get("sentence_date")))

    # Attorneys — upsert by name+role
    for att in analysis.get("attorneys", []):
        if att.get("name"):
            cur.execute("""
                INSERT INTO attorneys (docket_number, name, role)
                VALUES (%s, %s, %s)
                ON CONFLICT (docket_number, name, role) DO NOTHING
            """, (docket_number, att.get("name"), att.get("role")))

    # Docket entries — clear and re-insert, skip duplicates within same analysis
    cur.execute("DELETE FROM docket_entries WHERE docket_number = %s", (docket_number,))
    seen_entries = set()
    for entry in analysis.get("docket_entries", []):
        key = (entry.get("date"), entry.get("description"))
        if key in seen_entries:
            continue
        seen_entries.add(key)
        cur.execute("""
            INSERT INTO docket_entries (docket_number, entry_date, description, filer)
            VALUES (%s, %s, %s, %s)
        """, (docket_number, entry.get("date"), entry.get("description"),
              entry.get("filer")))


# ---------------------------------------------------------------------------
# Calendar events
# ---------------------------------------------------------------------------

def upsert_events(conn, events):
    """Store calendar events from search results."""
    cur = conn.cursor()
    new_count = 0
    for e in events:
        # Ensure the case exists first
        upsert_case(conn, e)
        cur.execute("""
            INSERT INTO events (docket_number, event_type, event_status, event_date, event_location)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (docket_number, event_type, event_date) DO UPDATE SET
                event_status = EXCLUDED.event_status,
                event_location = EXCLUDED.event_location
            RETURNING (xmax = 0) AS is_new
        """, (e.get("docket_number"), e.get("event_type"), e.get("event_status"),
              e.get("event_date"), e.get("event_location")))
        row = cur.fetchone()
        if row and row[0]:
            new_count += 1
    return len(events), new_count


# ---------------------------------------------------------------------------
# Ingest queue
# ---------------------------------------------------------------------------

def queue_ingest(conn, docket_number, priority=0):
    """Add a docket to the ingest queue. Returns queue entry ID."""
    cur = conn.cursor()
    # Check if already queued/processing
    cur.execute("""
        SELECT id, status FROM ingest_queue
        WHERE docket_number = %s AND status IN ('pending', 'processing')
        ORDER BY requested_at DESC LIMIT 1
    """, (docket_number,))
    existing = cur.fetchone()
    if existing:
        return existing[0], existing[1]

    cur.execute("""
        INSERT INTO ingest_queue (docket_number, priority)
        VALUES (%s, %s) RETURNING id
    """, (docket_number, priority))
    return cur.fetchone()[0], "pending"


def retry_failed_jobs(conn, max_attempts=3):
    """Re-queue failed jobs that haven't exceeded max attempts. Returns count re-queued."""
    cur = conn.cursor()
    cur.execute("""
        WITH failed AS (
            SELECT docket_number, COUNT(*) as attempts
            FROM ingest_queue
            WHERE status = 'failed'
            GROUP BY docket_number
            HAVING COUNT(*) < %s
        )
        INSERT INTO ingest_queue (docket_number, priority)
        SELECT f.docket_number, 1 FROM failed f
        WHERE NOT EXISTS (
            SELECT 1 FROM ingest_queue q
            WHERE q.docket_number = f.docket_number AND q.status IN ('pending', 'processing')
        )
        RETURNING id
    """, (max_attempts,))
    rows = cur.fetchall()
    return len(rows)


def claim_ingest_job(conn):
    """Claim the next job to process. Checks queue first (on-demand requests),
    then falls back to the next unanalyzed case (most recent first)."""
    cur = conn.cursor()
    # 1. Check explicit queue (on-demand requests, watchlist adds)
    cur.execute("""
        UPDATE ingest_queue SET status = 'processing', started_at = NOW()
        WHERE id = (
            SELECT id FROM ingest_queue
            WHERE status = 'pending'
            ORDER BY priority DESC, requested_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, docket_number
    """)
    row = cur.fetchone()
    if row:
        return row

    # 2. Refill queue from unanalyzed cases if running low
    cur.execute("SELECT COUNT(*) FROM ingest_queue WHERE status = 'pending'")
    pending = cur.fetchone()[0]
    if pending < 10:
        cur.execute("""
            INSERT INTO ingest_queue (docket_number, priority)
            SELECT c.docket_number, 2
            FROM cases c
            LEFT JOIN analyses a ON c.docket_number = a.docket_number AND a.doc_type = 'docket'
            WHERE a.id IS NULL AND c.filing_date IS NOT NULL AND c.filing_date != ''
            AND c.docket_number NOT IN (SELECT docket_number FROM ingest_queue)
            ORDER BY TO_DATE(c.filing_date, 'MM/DD/YYYY') DESC
            LIMIT 20
        """)
        if cur.rowcount > 0:
            # Now claim one from the queue (SKIP LOCKED handles parallelism)
            cur.execute("""
                UPDATE ingest_queue SET status = 'processing', started_at = NOW()
                WHERE id = (
                    SELECT id FROM ingest_queue
                    WHERE status = 'pending'
                    ORDER BY priority DESC, requested_at ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING id, docket_number
            """)
            return cur.fetchone()

    return None


def complete_ingest_job(conn, job_id, error=None):
    """Mark an ingest job as completed or failed. Skips if job_id=0 (auto-picked)."""
    if not job_id:
        return
    cur = conn.cursor()
    status = "failed" if error else "completed"
    cur.execute("""
        UPDATE ingest_queue SET status = %s, completed_at = NOW(), error = %s
        WHERE id = %s
    """, (status, error, job_id))


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------

def detect_and_store_changes(conn, docket_number, new_analysis, doc_type="docket"):
    """Compare new analysis against stored version, log diffs, update if changed.
    Returns list of changes or empty list if unchanged."""
    # Strip transient fields for comparison
    new_analysis = {k: v for k, v in new_analysis.items() if k not in ("pdf_path", "full_text")}
    old = get_analysis(conn, docket_number, doc_type)
    if not old:
        store_analysis(conn, docket_number, new_analysis, doc_type)
        store_parsed_data(conn, docket_number, new_analysis)
        return [{"field": "initial_ingest", "old": None, "new": "created"}]

    old_hash = _hash(old)
    new_hash = _hash(new_analysis)

    if old_hash == new_hash:
        # Update last_scraped timestamp even if data unchanged
        cur = conn.cursor()
        cur.execute("UPDATE cases SET last_scraped = NOW() WHERE docket_number = %s",
                    (docket_number,))
        return []

    # Data changed — find what's different
    changes = _diff_analysis(old, new_analysis)

    # Log changes (skip null→value — those are first-time field captures, not real changes)
    changes = [c for c in changes if c["old"] is not None]
    cur = conn.cursor()
    for change in changes:
        cur.execute("""
            INSERT INTO change_log (docket_number, field, old_value, new_value)
            VALUES (%s, %s, %s, %s)
        """, (docket_number, change["field"],
              str(change["old"])[:500] if change["old"] else None,
              str(change["new"])[:500] if change["new"] else None))

    # Update stored data
    store_analysis(conn, docket_number, new_analysis, doc_type)
    store_parsed_data(conn, docket_number, new_analysis)

    return changes


def _normalize_val(v):
    """Normalize values for comparison: treat None, 'None', 'null', '' as equivalent."""
    if v is None or v == "None" or v == "null" or v == "":
        return None
    return v


def _diff_analysis(old, new):
    """Compare two analysis dicts, return list of changes."""
    changes = []
    track_fields = ["case_status", "judge", "filing_date"]

    for field in track_fields:
        ov = _normalize_val(old.get(field))
        nv = _normalize_val(new.get(field))
        if ov != nv:
            changes.append({"field": field, "old": ov, "new": nv})

    # Compare charges
    old_charges = {c.get("seq"): c for c in old.get("charges", [])}
    new_charges = {c.get("seq"): c for c in new.get("charges", [])}
    # New/removed charges
    old_seqs = set(old_charges.keys())
    new_seqs = set(new_charges.keys())
    for seq in new_seqs - old_seqs:
        changes.append({"field": f"charge_{seq}_added", "old": None,
                        "new": new_charges[seq].get("description")})
    for seq in old_seqs - new_seqs:
        changes.append({"field": f"charge_{seq}_removed",
                        "old": old_charges[seq].get("description"), "new": None})
    # Changed disposition or grade on existing charges
    for seq in old_seqs & new_seqs:
        oc, nc = old_charges[seq], new_charges[seq]
        if nc.get("disposition") != oc.get("disposition"):
            changes.append({"field": f"charge_{seq}_disposition",
                            "old": oc.get("disposition"), "new": nc.get("disposition")})
        if nc.get("grade") != oc.get("grade"):
            changes.append({"field": f"charge_{seq}_grade",
                            "old": oc.get("grade"), "new": nc.get("grade")})

    # Compare bail
    ob = old.get("bail", {})
    nb = new.get("bail", {})
    for bf in ["amount", "status", "type"]:
        if ob.get(bf) != nb.get(bf):
            changes.append({"field": f"bail_{bf}", "old": ob.get(bf), "new": nb.get(bf)})

    # Sentence changes
    old_sents = old.get("sentences", [])
    new_sents = new.get("sentences", [])
    if len(new_sents) != len(old_sents):
        changes.append({"field": "sentences_changed",
                        "old": str(len(old_sents)), "new": str(len(new_sents))})
    elif old_sents != new_sents:
        changes.append({"field": "sentences_modified",
                        "old": json.dumps(old_sents)[:200], "new": json.dumps(new_sents)[:200]})

    # New docket entries added
    old_entry_count = len(old.get("docket_entries", []))
    new_entry_count = len(new.get("docket_entries", []))
    if new_entry_count > old_entry_count:
        changes.append({
            "field": "docket_entries_added",
            "old": str(old_entry_count),
            "new": str(new_entry_count),
        })

    # Attorney changes
    old_attys = set((a.get("name", ""), a.get("role", "")) for a in old.get("attorneys", []))
    new_attys = set((a.get("name", ""), a.get("role", "")) for a in new.get("attorneys", []))
    added = new_attys - old_attys
    removed = old_attys - new_attys
    if added or removed:
        changes.append({
            "field": "attorneys_changed",
            "old": ", ".join(f"{n} ({r})" for n, r in removed) or None,
            "new": ", ".join(f"{n} ({r})" for n, r in added) or None,
        })

    # Catch-all: if hashes differ but nothing specific found
    if not changes:
        changes.append({"field": "data_changed", "old": "see analysis", "new": "updated"})

    return changes


def get_changes(conn, docket_number=None, since=None, limit=100):
    """Get change log entries."""
    cur = _dict_cur(conn)
    clauses = []
    params = []
    if docket_number:
        clauses.append("docket_number = %s")
        params.append(docket_number)
    if since:
        clauses.append("detected_at >= %s")
        params.append(since)
    where = " AND ".join(clauses) if clauses else "TRUE"
    params.append(limit)
    cur.execute(f"""
        SELECT * FROM change_log WHERE {where}
        ORDER BY detected_at DESC LIMIT %s
    """, params)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Staleness
# ---------------------------------------------------------------------------

def cleanup_old_data(conn, queue_days=7, changelog_days=90, chat_days=90):
    """Delete old completed/failed queue entries, change_log, and chat_jobs."""
    cur = conn.cursor()
    cur.execute("DELETE FROM ingest_queue WHERE status IN ('completed', 'failed') AND completed_at < NOW() - INTERVAL '%s days'", (queue_days,))
    queue_deleted = cur.rowcount
    cur.execute("DELETE FROM change_log WHERE detected_at < NOW() - INTERVAL '%s days'", (changelog_days,))
    changelog_deleted = cur.rowcount
    cur.execute("DELETE FROM chat_jobs WHERE status IN ('completed', 'error') AND created_at < NOW() - INTERVAL '%s days'", (chat_days,))
    chat_deleted = cur.rowcount
    return queue_deleted, changelog_deleted


def get_stale_dockets(conn, active_hours=24, closed_days=7, limit=50):
    """Get dockets that need re-scraping based on case status."""
    cur = _dict_cur(conn)
    cur.execute("""
        SELECT docket_number, status, last_scraped FROM cases
        WHERE
            (status ILIKE '%%active%%' AND last_scraped < NOW() - INTERVAL '%s hours')
            OR
            (status NOT ILIKE '%%active%%' AND last_scraped < NOW() - INTERVAL '%s days')
            OR last_scraped IS NULL
        ORDER BY last_scraped ASC NULLS FIRST
        LIMIT %s
    """, (active_hours, closed_days, limit))
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Watchlist
# ---------------------------------------------------------------------------

def add_to_watchlist(conn, api_key, docket_number, label=None):
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO watchlist (api_key, docket_number, label)
        VALUES (%s, %s, %s)
        ON CONFLICT (api_key, docket_number) DO UPDATE SET label = EXCLUDED.label
        RETURNING id
    """, (api_key, docket_number, label))
    # Auto-queue for ingest if not in DB
    if not get_case(conn, docket_number):
        queue_ingest(conn, docket_number, priority=5)
    return cur.fetchone()[0]


def remove_from_watchlist(conn, api_key, docket_number):
    cur = conn.cursor()
    cur.execute("DELETE FROM watchlist WHERE api_key = %s AND docket_number = %s",
                (api_key, docket_number))
    return cur.rowcount > 0


def get_watchlist(conn, api_key):
    cur = _dict_cur(conn)
    cur.execute("""
        SELECT w.docket_number, w.label, w.created_at,
               c.caption, c.status, c.county, c.filing_date, c.last_scraped
        FROM watchlist w
        LEFT JOIN cases c ON w.docket_number = c.docket_number
        WHERE w.api_key = %s
        ORDER BY w.created_at DESC
    """, (api_key,))
    return cur.fetchall()


def get_watchlist_changes(conn, api_key, since=None):
    """Get changes for all watched dockets."""
    cur = _dict_cur(conn)
    params = [api_key]
    since_clause = ""
    if since:
        since_clause = "AND cl.detected_at >= %s"
        params.append(since)
    cur.execute(f"""
        SELECT cl.*, c.caption FROM change_log cl
        JOIN watchlist w ON cl.docket_number = w.docket_number
        LEFT JOIN cases c ON cl.docket_number = c.docket_number
        WHERE w.api_key = %s {since_clause}
        ORDER BY cl.detected_at DESC LIMIT 200
    """, params)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Webhooks
# ---------------------------------------------------------------------------

def create_webhook(conn, api_key, url, events=None, county=None, docket_type=None):
    cur = conn.cursor()
    events = events or ["change", "new_filing", "new_event"]
    cur.execute("""
        INSERT INTO webhooks (api_key, url, events, county, docket_type)
        VALUES (%s, %s, %s, %s, %s) RETURNING id
    """, (api_key, url, events, county, docket_type))
    return cur.fetchone()[0]


def get_webhooks(conn, api_key):
    cur = _dict_cur(conn)
    cur.execute("SELECT * FROM webhooks WHERE api_key = %s ORDER BY created_at DESC", (api_key,))
    return cur.fetchall()


def delete_webhook(conn, api_key, webhook_id):
    cur = conn.cursor()
    cur.execute("DELETE FROM webhooks WHERE id = %s AND api_key = %s", (webhook_id, api_key))
    return cur.rowcount > 0


def get_active_webhooks(conn, event_type=None, county=None):
    cur = _dict_cur(conn)
    clauses = ["active = TRUE"]
    params = []
    if event_type:
        clauses.append("%s = ANY(events)")
        params.append(event_type)
    if county:
        clauses.append("(county IS NULL OR county ILIKE %s)")
        params.append(county)
    cur.execute(f"""
        SELECT * FROM webhooks WHERE {' AND '.join(clauses)}
    """, params)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# API keys
# ---------------------------------------------------------------------------

def create_api_key(conn, name, email=None):
    import secrets
    key = f"ujs_{secrets.token_urlsafe(32)}"
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO api_keys (key, name, email) VALUES (%s, %s, %s) RETURNING key
    """, (key, name, email))
    return cur.fetchone()[0]


def validate_api_key(conn, key):
    cur = _dict_cur(conn)
    cur.execute("SELECT * FROM api_keys WHERE key = %s", (key,))
    row = cur.fetchone()
    if not row:
        return None
    if row["requests_today"] >= row["daily_limit"]:
        return None
    cur = conn.cursor()
    cur.execute("""
        UPDATE api_keys SET requests_today = requests_today + 1, last_used = NOW()
        WHERE key = %s
    """, (key,))
    return row


# ---------------------------------------------------------------------------
# Search: judges, attorneys, charges
# ---------------------------------------------------------------------------

def fuzzy_name_search(conn, name, limit=10):
    """Fuzzy search for participant names using trigram similarity."""
    cur = _dict_cur(conn)
    cur.execute("""
        SELECT p.name, p.docket_number, p.dob,
               c.caption, c.status, c.county, c.filing_date,
               similarity(p.name, %s) as match_score
        FROM participants p
        JOIN cases c ON p.docket_number = c.docket_number
        WHERE similarity(p.name, %s) > 0.15
        ORDER BY similarity(p.name, %s) DESC
        LIMIT %s
    """, (name, name, name, limit))
    return cur.fetchall()


def search_by_judge(conn, judge_name, county=None, limit=100):
    cur = _dict_cur(conn)
    parts = judge_name.strip().split()
    pattern = "%" + "%".join(parts) + "%" if len(parts) >= 2 else f"%{judge_name}%"
    params = [pattern]
    county_clause = ""
    if county:
        county_clause = "AND c.county ILIKE %s"
        params.append(county)
    params.append(limit)
    cur.execute(f"""
        SELECT a.analysis->>'judge' as judge, c.docket_number, c.caption, c.status,
               c.county, c.filing_date
        FROM analyses a JOIN cases c ON a.docket_number = c.docket_number
        WHERE a.analysis->>'judge' ILIKE %s {county_clause}
        ORDER BY TO_DATE(c.filing_date, 'MM/DD/YYYY') DESC LIMIT %s
    """, params)
    return cur.fetchall()


def search_by_attorney(conn, attorney_name, role=None, county=None, limit=100):
    cur = _dict_cur(conn)
    # Split name parts for flexible matching ("Michael Murphy" → "%Michael%Murphy%")
    parts = attorney_name.strip().split()
    if len(parts) >= 2:
        pattern = "%" + "%".join(parts) + "%"
    else:
        pattern = f"%{attorney_name}%"
    params = [pattern]
    clauses = ["a.name ILIKE %s"]
    if role:
        clauses.append("a.role ILIKE %s")
        params.append(f"%{role}%")
    if county:
        clauses.append("c.county ILIKE %s")
        params.append(county)
    params.append(limit)
    cur.execute(f"""
        SELECT a.name, a.role, c.docket_number, c.caption, c.status, c.county, c.filing_date
        FROM attorneys a JOIN cases c ON a.docket_number = c.docket_number
        WHERE {' AND '.join(clauses)}
        ORDER BY TO_DATE(c.filing_date, 'MM/DD/YYYY') DESC LIMIT %s
    """, params)
    return cur.fetchall()


def search_by_charge(conn, statute=None, description=None, county=None,
                     disposition=None, limit=100):
    cur = _dict_cur(conn)
    clauses = []
    params = []
    if statute:
        clauses.append("ch.statute ILIKE %s")
        params.append(f"%{statute}%")
    if description:
        clauses.append("ch.description ILIKE %s")
        params.append(f"%{description}%")
    if disposition:
        clauses.append("ch.disposition ILIKE %s")
        params.append(f"%{disposition}%")
    if county:
        clauses.append("c.county ILIKE %s")
        params.append(county)
    if not clauses:
        return []
    params.append(limit)
    cur.execute(f"""
        SELECT ch.*, c.caption, c.status, c.county, c.filing_date
        FROM charges ch JOIN cases c ON ch.docket_number = c.docket_number
        WHERE {' AND '.join(clauses)}
        ORDER BY TO_DATE(c.filing_date, 'MM/DD/YYYY') DESC LIMIT %s
    """, params)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Aggregation / stats
# ---------------------------------------------------------------------------

def get_filing_stats(conn, county=None, period="daily", days=30):
    cur = _dict_cur(conn)
    county_clause = ""
    params = []
    if county:
        county_clause = "WHERE county ILIKE %s"
        params.append(county)
    # Group by filing_date (already MM/DD/YYYY strings)
    where_parts = ["filing_date != ''", f"TO_DATE(filing_date, 'MM/DD/YYYY') >= CURRENT_DATE - INTERVAL '{days} days'"]
    if county:
        where_parts.append("county ILIKE %s")
    cur.execute(f"""
        SELECT filing_date, COUNT(*) as count,
               SUM(CASE WHEN docket_number LIKE '%%-CR-%%' THEN 1 ELSE 0 END) as criminal,
               SUM(CASE WHEN docket_number LIKE '%%-TR-%%' THEN 1 ELSE 0 END) as traffic,
               SUM(CASE WHEN docket_number LIKE '%%-CV-%%' THEN 1 ELSE 0 END) as civil
        FROM cases WHERE {' AND '.join(where_parts)}
        GROUP BY filing_date
        ORDER BY TO_DATE(filing_date, 'MM/DD/YYYY') DESC
        LIMIT %s
    """, params + [days])
    return cur.fetchall()


def get_county_stats(conn):
    cur = _dict_cur(conn)
    cur.execute("""
        SELECT county, COUNT(*) as total_cases,
               SUM(CASE WHEN status ILIKE '%%active%%' THEN 1 ELSE 0 END) as active,
               SUM(CASE WHEN status ILIKE '%%closed%%' THEN 1 ELSE 0 END) as closed,
               SUM(CASE WHEN docket_number LIKE '%%-CR-%%' THEN 1 ELSE 0 END) as criminal,
               SUM(CASE WHEN docket_number LIKE '%%-TR-%%' THEN 1 ELSE 0 END) as traffic
        FROM cases WHERE county != ''
        GROUP BY county ORDER BY total_cases DESC
    """)
    return cur.fetchall()


import time as _time


def embed_new_charges():
    """Embed any charge descriptions not yet in charge_embeddings. Call periodically."""
    import os
    try:
        from google import genai
        from google.genai import types
        client = genai.Client(api_key=os.environ.get("GEMINI_API_KEY"))
    except Exception:
        return 0
    with connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ch.description FROM charges ch
            WHERE ch.description IS NOT NULL AND ch.description != ''
            AND ch.description NOT IN (SELECT description FROM charge_embeddings)
        """)
        new_charges = [r[0] for r in cur.fetchall()]
    if not new_charges:
        return 0
    # Batch embed
    for i in range(0, len(new_charges), 100):
        batch = new_charges[i:i+100]
        result = client.models.embed_content(
            model="gemini-embedding-001", contents=batch,
            config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT", output_dimensionality=768)
        )
        with connect() as conn:
            cur = conn.cursor()
            for desc, emb in zip(batch, result.embeddings):
                cur.execute(
                    "INSERT INTO charge_embeddings (description, embedding) VALUES (%s, %s) ON CONFLICT (description) DO NOTHING",
                    (desc, str(emb.values))
                )
    return len(new_charges)


_active_counties_cache = {"data": [], "expires": 0}


def get_active_counties():
    """Counties with cases in DB. Cached 1 hour. Returns [{"county": ..., "state": ..., "case_count": ...}]."""
    now = _time.time()
    if now < _active_counties_cache["expires"] and _active_counties_cache["data"]:
        return _active_counties_cache["data"]
    with connect() as conn:
        cur = _dict_cur(conn)
        cur.execute("""
            SELECT county, COALESCE(state, 'PA') as state, COUNT(*) as case_count
            FROM cases
            WHERE county IS NOT NULL AND county != ''
            GROUP BY county, state
            ORDER BY case_count DESC
        """)
        result = [dict(r) for r in cur.fetchall()]
    _active_counties_cache["data"] = result
    _active_counties_cache["expires"] = now + 3600
    return result


def get_active_county_names():
    """Just the county name strings."""
    return [c["county"] for c in get_active_counties()]


def get_charge_stats(conn, county=None, limit=25):
    cur = _dict_cur(conn)
    clauses = []
    params = []
    if county:
        clauses.append("c.county ILIKE %s")
        params.append(county)
    where = "WHERE " + " AND ".join(clauses) if clauses else ""
    params.append(limit)
    cur.execute(f"""
        SELECT ch.description, ch.grade, COUNT(*) as count,
               SUM(CASE WHEN ch.disposition ILIKE '%%guilty%%' THEN 1 ELSE 0 END) as guilty,
               SUM(CASE WHEN ch.disposition ILIKE '%%dismissed%%' OR ch.disposition ILIKE '%%quashed%%' THEN 1 ELSE 0 END) as dismissed
        FROM charges ch JOIN cases c ON ch.docket_number = c.docket_number
        {where}
        GROUP BY ch.description, ch.grade
        ORDER BY count DESC LIMIT %s
    """, params)
    return cur.fetchall()


def get_judge_stats(conn, county=None, limit=25):
    cur = _dict_cur(conn)
    params = []
    county_clause = ""
    if county:
        county_clause = "AND c.county ILIKE %s"
        params.append(county)
    params.append(limit)
    cur.execute(f"""
        SELECT a.analysis->>'judge' as judge, COUNT(*) as total_cases,
               SUM(CASE WHEN c.status ILIKE '%%active%%' THEN 1 ELSE 0 END) as active,
               SUM(CASE WHEN c.status ILIKE '%%closed%%' THEN 1 ELSE 0 END) as closed
        FROM analyses a JOIN cases c ON a.docket_number = c.docket_number
        WHERE a.analysis->>'judge' IS NOT NULL AND a.analysis->>'judge' != ''
        {county_clause}
        GROUP BY a.analysis->>'judge'
        ORDER BY total_cases DESC LIMIT %s
    """, params)
    return cur.fetchall()


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def get_stats(conn):
    """Get database statistics."""
    cur = _dict_cur(conn)
    stats = {}
    for table in ["cases", "participants", "charges", "events", "analyses", "ingest_queue"]:
        cur.execute(f"SELECT COUNT(*) as count FROM {table}")
        stats[table] = cur.fetchone()["count"]
    cur.execute("SELECT COUNT(*) FROM ingest_queue WHERE status = 'pending'")
    stats["pending_ingests"] = cur.fetchone()["count"]
    cur.execute("SELECT MAX(last_scraped) FROM cases")
    row = cur.fetchone()
    stats["last_scrape"] = row["max"].isoformat() if row["max"] else None
    cur.execute("SELECT MIN(last_scraped) FROM cases WHERE status ILIKE '%%active%%'")
    row = cur.fetchone()
    stats["oldest_active_scrape"] = row["min"].isoformat() if row["min"] else None
    return stats


# ---------------------------------------------------------------------------
# User Watches (per-user docket monitoring)
# ---------------------------------------------------------------------------

_MAX_WATCHES_PER_USER = 25


def add_user_watch(conn, user_id, user_email, docket_number, label=None, notify_frequency='daily'):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM user_watches WHERE user_id = %s", (user_id,))
    if cur.fetchone()[0] >= _MAX_WATCHES_PER_USER:
        return None  # limit reached
    cur.execute("""
        INSERT INTO user_watches (user_id, user_email, docket_number, label, notify_frequency)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id, docket_number) DO UPDATE SET label = EXCLUDED.label, notify_frequency = EXCLUDED.notify_frequency
        RETURNING id
    """, (user_id, user_email, docket_number, label, notify_frequency))
    if not get_case(conn, docket_number):
        queue_ingest(conn, docket_number, priority=5)
    return cur.fetchone()[0]


def remove_user_watch(conn, user_id, docket_number):
    cur = conn.cursor()
    cur.execute("DELETE FROM user_watches WHERE user_id = %s AND docket_number = %s", (user_id, docket_number))
    return cur.rowcount > 0


def get_user_watches(conn, user_id):
    cur = _dict_cur(conn)
    cur.execute("""
        SELECT w.id, w.docket_number, w.label, w.notify_frequency, w.created_at, w.last_notified_at,
               c.caption, c.status, c.county, c.filing_date, c.last_scraped,
               (SELECT COUNT(*) FROM change_log cl WHERE cl.docket_number = w.docket_number
                AND cl.detected_at > COALESCE(w.last_notified_at, w.created_at)
                AND cl.field != 'initial_ingest'
                AND NOT (cl.old_value IS NULL AND cl.field IN ('bail_status','bail_amount','bail_type','data_changed'))) AS pending_changes
        FROM user_watches w
        LEFT JOIN cases c ON w.docket_number = c.docket_number
        WHERE w.user_id = %s
        ORDER BY w.created_at DESC
    """, (user_id,))
    return [dict(r) for r in cur.fetchall()]


def is_watching(conn, user_id, docket_number):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM user_watches WHERE user_id = %s AND docket_number = %s", (user_id, docket_number))
    return cur.fetchone() is not None


def get_pending_notifications(conn, frequency='daily'):
    """Get users with unsent changes on watched dockets."""
    cur = _dict_cur(conn)
    cur.execute("""
        SELECT w.user_id, w.user_email, w.docket_number, w.label, w.notify_frequency,
               c.caption, c.county,
               cl.change_type, cl.field_name, cl.old_value, cl.new_value, cl.detected_at,
               p.unsubscribe_token
        FROM user_watches w
        JOIN change_log cl ON cl.docket_number = w.docket_number
            AND cl.detected_at > COALESCE(w.last_notified_at, w.created_at)
            AND cl.field != 'initial_ingest'
            AND NOT (cl.old_value IS NULL AND cl.field IN ('bail_status','bail_amount','bail_type','data_changed'))
        LEFT JOIN cases c ON w.docket_number = c.docket_number
        LEFT JOIN user_preferences p ON w.user_id = p.user_id
        WHERE w.notify_frequency = %s
          AND (p.email_alerts IS NULL OR p.email_alerts = TRUE)
        ORDER BY w.user_id, w.docket_number, cl.detected_at
    """, (frequency,))
    return [dict(r) for r in cur.fetchall()]


def mark_notified(conn, user_id, docket_numbers):
    """Mark watches as notified for a list of docket numbers."""
    cur = conn.cursor()
    for dn in docket_numbers:
        cur.execute("UPDATE user_watches SET last_notified_at = NOW() WHERE user_id = %s AND docket_number = %s",
                    (user_id, dn))


# ---------------------------------------------------------------------------
# User Preferences
# ---------------------------------------------------------------------------

def get_or_create_preferences(conn, user_id):
    cur = _dict_cur(conn)
    cur.execute("SELECT * FROM user_preferences WHERE user_id = %s", (user_id,))
    row = cur.fetchone()
    if row:
        return dict(row)
    cur = conn.cursor()
    cur.execute("INSERT INTO user_preferences (user_id) VALUES (%s) ON CONFLICT DO NOTHING", (user_id,))
    cur = _dict_cur(conn)
    cur.execute("SELECT * FROM user_preferences WHERE user_id = %s", (user_id,))
    return dict(cur.fetchone())


def update_preferences(conn, user_id, **kwargs):
    allowed = {'email_alerts', 'weekly_digest', 'notify_frequency'}
    sets, params = [], []
    for k, v in kwargs.items():
        if k in allowed:
            sets.append(f"{k} = %s")
            params.append(v)
    if not sets:
        return
    params.append(user_id)
    cur = conn.cursor()
    cur.execute(f"UPDATE user_preferences SET {', '.join(sets)}, updated_at = NOW() WHERE user_id = %s", params)


def get_preferences_by_token(conn, token):
    cur = _dict_cur(conn)
    cur.execute("SELECT * FROM user_preferences WHERE unsubscribe_token = %s", (token,))
    row = cur.fetchone()
    return dict(row) if row else None

"""Database layer for UJS court data."""

import hashlib, json, os
from contextlib import contextmanager
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras


def get_db_url():
    return os.environ["DATABASE_URL"]


@contextmanager
def connect():
    conn = psycopg2.connect(get_db_url())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def upsert_case(conn, case):
    """Upsert a case from search results."""
    # Ensure all expected keys exist with defaults
    defaults = {
        "docket_number": "", "court_type": "", "caption": "", "status": "",
        "filing_date": "", "participant": "", "dob": "", "county": "",
        "court_office": "", "otn": "", "complaint": "", "incident": "",
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
    return row[0] if row else False


def upsert_cases(conn, cases):
    """Upsert multiple cases. Returns (total, new_count)."""
    new_count = 0
    for case in cases:
        if upsert_case(conn, case):
            new_count += 1
    return len(cases), new_count


def get_case(conn, docket_number):
    """Get a case by docket number."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM cases WHERE docket_number = %s", (docket_number,))
    return cur.fetchone()


def search_cases(conn, county=None, status=None, docket_type=None,
                 filed_after=None, filed_before=None, name=None, limit=100):
    """Search cases in the database."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    clauses = []
    params = []

    if county:
        clauses.append("c.county ILIKE %s")
        params.append(county)
    if status:
        clauses.append("c.status ILIKE %s")
        params.append(f"%{status}%")
    if docket_type:
        dtype_map = {"criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-",
                     "non-traffic": "-NT-"}
        code = dtype_map.get(docket_type.lower(), "")
        if code:
            clauses.append("c.docket_number LIKE %s")
            params.append(f"%{code}%")
    if filed_after:
        clauses.append("c.filing_date >= %s")
        params.append(filed_after)
    if filed_before:
        clauses.append("c.filing_date <= %s")
        params.append(filed_before)
    if name:
        clauses.append("""EXISTS (
            SELECT 1 FROM participants p
            WHERE p.docket_number = c.docket_number AND p.name ILIKE %s
        )""")
        params.append(f"%{name}%")

    where = " AND ".join(clauses) if clauses else "TRUE"
    params.append(limit)

    cur.execute(f"""
        SELECT c.* FROM cases c
        WHERE {where}
        ORDER BY c.filing_date DESC
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
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
    """Break down a Gemini analysis and store in normalized tables."""
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

    # Charges
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

    # Bail
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

    # Sentences
    for sent in analysis.get("sentences", []):
        cur.execute("""
            INSERT INTO sentences (docket_number, charge, sentence_type, duration,
                                   conditions, sentence_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (docket_number, sent.get("charge"), sent.get("sentence_type"),
              sent.get("duration"), sent.get("conditions"), sent.get("sentence_date")))

    # Attorneys
    for att in analysis.get("attorneys", []):
        if att.get("name"):
            cur.execute("""
                INSERT INTO attorneys (docket_number, name, role)
                VALUES (%s, %s, %s)
                ON CONFLICT (docket_number, name, role) DO NOTHING
            """, (docket_number, att.get("name"), att.get("role")))

    # Docket entries
    for entry in analysis.get("docket_entries", []):
        cur.execute("""
            INSERT INTO docket_entries (docket_number, entry_date, description, filer)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (docket_number, entry_date, description) DO NOTHING
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


def claim_ingest_job(conn):
    """Claim the next pending ingest job. Returns (id, docket_number) or None."""
    cur = conn.cursor()
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


def complete_ingest_job(conn, job_id, error=None):
    """Mark an ingest job as completed or failed."""
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

    # Log changes
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


def _diff_analysis(old, new):
    """Compare two analysis dicts, return list of changes."""
    changes = []
    track_fields = ["case_status", "judge", "filing_date"]

    for field in track_fields:
        ov = old.get(field)
        nv = new.get(field)
        if ov != nv:
            changes.append({"field": field, "old": ov, "new": nv})

    # Compare charge dispositions
    old_charges = {c.get("seq"): c for c in old.get("charges", [])}
    for nc in new.get("charges", []):
        seq = nc.get("seq")
        oc = old_charges.get(seq, {})
        if nc.get("disposition") != oc.get("disposition"):
            changes.append({
                "field": f"charge_{seq}_disposition",
                "old": oc.get("disposition"),
                "new": nc.get("disposition"),
            })

    # Compare bail
    ob = old.get("bail", {})
    nb = new.get("bail", {})
    for bf in ["amount", "status", "type"]:
        if ob.get(bf) != nb.get(bf):
            changes.append({"field": f"bail_{bf}", "old": ob.get(bf), "new": nb.get(bf)})

    # New sentences added
    old_sent_count = len(old.get("sentences", []))
    new_sent_count = len(new.get("sentences", []))
    if new_sent_count > old_sent_count:
        changes.append({
            "field": "sentences_added",
            "old": str(old_sent_count),
            "new": str(new_sent_count),
        })

    # New docket entries added
    old_entry_count = len(old.get("docket_entries", []))
    new_entry_count = len(new.get("docket_entries", []))
    if new_entry_count > old_entry_count:
        changes.append({
            "field": "docket_entries_added",
            "old": str(old_entry_count),
            "new": str(new_entry_count),
        })

    # Catch-all: if hashes differ but nothing specific found
    if not changes:
        changes.append({"field": "data_changed", "old": "see analysis", "new": "updated"})

    return changes


def get_changes(conn, docket_number=None, since=None, limit=100):
    """Get change log entries."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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

def get_stale_dockets(conn, active_hours=24, closed_days=7, limit=50):
    """Get dockets that need re-scraping based on case status."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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
# Stats
# ---------------------------------------------------------------------------

def get_stats(conn):
    """Get database statistics."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
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

#!/usr/bin/env python3
"""Isolated data integrity tests — verifies upsert/update/append behavior.
Uses TEST- prefix docket numbers, cleans up after itself.

Run: DATABASE_URL=... python -m tests.test_data_integrity
"""

import json, os, sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from ujs import db

PREFIX = "TEST-INTEGRITY"
PASS = 0
FAIL = 0


def test(name, condition, detail=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} — {detail}")


def cleanup(conn):
    cur = conn.cursor()
    for tbl in ["change_log", "docket_entries", "attorneys", "sentences",
                "bail", "charges", "events", "participants", "analyses",
                "ingest_queue"]:
        cur.execute(f"DELETE FROM {tbl} WHERE docket_number LIKE %s", (f"{PREFIX}%",))
    cur.execute("DELETE FROM cases WHERE docket_number LIKE %s", (f"{PREFIX}%",))


def make_case(suffix, **overrides):
    base = {
        "docket_number": f"{PREFIX}-{suffix}",
        "court_type": "Common Pleas", "caption": f"Test v. Case {suffix}",
        "status": "Active", "filing_date": "04/01/2026", "county": "Lehigh",
        "court_office": "CP-31", "otn": "", "complaint": "", "incident": "",
        "docket_sheet_url": None, "court_summary_url": None,
    }
    base.update(overrides)
    return base


def make_analysis(suffix, charges=1, sentences=1, entries=3, attorneys=2):
    return {
        "docket_number": f"{PREFIX}-{suffix}",
        "case_caption": f"Test v. Case {suffix}",
        "case_status": "Active",
        "judge": "Test Judge",
        "defendant": {"name": "Test Defendant", "dob": "01/01/1990", "address": "123 Test St"},
        "charges": [
            {"seq": i + 1, "statute": f"18 § {3929 + i}", "description": f"Charge {i+1}",
             "grade": "F3", "offense_date": "01/01/2026", "otn": f"T{i}",
             "disposition": None, "disposition_date": None}
            for i in range(charges)
        ],
        "bail": {"type": "Monetary", "amount": "$10,000.00", "status": "Set", "posting_date": "01/01/2026"},
        "sentences": [
            {"charge": f"Charge {i+1}", "sentence_type": "Confinement",
             "duration": f"{i+1} months", "conditions": "", "sentence_date": "03/01/2026"}
            for i in range(sentences)
        ],
        "attorneys": [
            {"name": f"Attorney {i+1}", "role": "Defense" if i == 0 else "Prosecution"}
            for i in range(attorneys)
        ],
        "docket_entries": [
            {"date": f"0{i+1}/01/2026", "description": f"Entry {i+1}", "filer": "Court"}
            for i in range(entries)
        ],
    }


def run_tests():
    global PASS, FAIL

    print("\n" + "=" * 60)
    print("Data Integrity Tests (isolated)")
    print("=" * 60)

    with db.connect() as conn:
        cleanup(conn)

    # ------------------------------------------------------------------
    print("\n--- 1. Case upsert: insert ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        case = make_case("001")
        is_new = db.upsert_case(conn, case)
        test("insert returns is_new=True", is_new is True)

        row = db.get_case(conn, f"{PREFIX}-001")
        test("case exists after insert", row is not None)
        test("caption correct", row["caption"] == "Test v. Case 001")
        test("status is Active", row["status"] == "Active")
        test("last_scraped set", row["last_scraped"] is not None)

    # ------------------------------------------------------------------
    print("\n--- 2. Case upsert: update ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        case = make_case("001", status="Closed", caption="Updated Caption")
        is_new = db.upsert_case(conn, case)
        test("update returns is_new=False", is_new is False)

        row = db.get_case(conn, f"{PREFIX}-001")
        test("status updated to Closed", row["status"] == "Closed")
        test("caption updated", row["caption"] == "Updated Caption")
        test("county preserved", row["county"] == "Lehigh")

    # ------------------------------------------------------------------
    print("\n--- 3. Case upsert: no duplicate rows ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        for _ in range(5):
            db.upsert_case(conn, make_case("001"))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM cases WHERE docket_number = %s", (f"{PREFIX}-001",))
        test("still exactly 1 row after 5 upserts", cur.fetchone()[0] == 1)

    # ------------------------------------------------------------------
    print("\n--- 4. Analysis store + retrieve ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("001")
        db.store_analysis(conn, f"{PREFIX}-001", analysis)
        retrieved = db.get_analysis(conn, f"{PREFIX}-001", "docket")
        test("analysis retrieved", retrieved is not None)
        test("charges preserved", len(retrieved.get("charges", [])) == 1)
        test("judge preserved", retrieved.get("judge") == "Test Judge")

    # ------------------------------------------------------------------
    print("\n--- 5. Parsed data: sentences don't duplicate ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("001", sentences=2)
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sentences WHERE docket_number = %s", (f"{PREFIX}-001",))
        count1 = cur.fetchone()[0]
        test("2 sentences after first store", count1 == 2)

        # Re-store same analysis — should NOT duplicate
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)
        cur.execute("SELECT COUNT(*) FROM sentences WHERE docket_number = %s", (f"{PREFIX}-001",))
        count2 = cur.fetchone()[0]
        test("still 2 sentences after re-store (no duplication)", count2 == 2, f"got {count2}")

    # ------------------------------------------------------------------
    print("\n--- 6. Parsed data: sentences update when changed ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("001", sentences=3)
        analysis["sentences"][2]["duration"] = "99 months"
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sentences WHERE docket_number = %s", (f"{PREFIX}-001",))
        test("3 sentences after adding one", cur.fetchone()[0] == 3)

        cur.execute("SELECT duration FROM sentences WHERE docket_number = %s AND charge = 'Charge 3'",
                    (f"{PREFIX}-001",))
        row = cur.fetchone()
        test("new sentence has correct duration", row and row[0] == "99 months", f"got {row}")

    # ------------------------------------------------------------------
    print("\n--- 7. Parsed data: sentences removed when count decreases ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("001", sentences=1)
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sentences WHERE docket_number = %s", (f"{PREFIX}-001",))
        test("1 sentence after reducing from 3", cur.fetchone()[0] == 1, f"got {cur.fetchone()}")

    # ------------------------------------------------------------------
    print("\n--- 8. Docket entries: don't duplicate on re-store ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("001", entries=5)
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM docket_entries WHERE docket_number = %s", (f"{PREFIX}-001",))
        count1 = cur.fetchone()[0]
        test("5 entries after first store", count1 == 5)

        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)
        cur.execute("SELECT COUNT(*) FROM docket_entries WHERE docket_number = %s", (f"{PREFIX}-001",))
        count2 = cur.fetchone()[0]
        test("still 5 entries after re-store", count2 == 5, f"got {count2}")

    # ------------------------------------------------------------------
    print("\n--- 9. Docket entries: new entries appended ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("001", entries=7)
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM docket_entries WHERE docket_number = %s", (f"{PREFIX}-001",))
        test("7 entries after adding 2 more", cur.fetchone()[0] == 7)

    # ------------------------------------------------------------------
    print("\n--- 10. Charges: disposition update ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("001", charges=2)
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)

        cur = conn.cursor()
        cur.execute("SELECT disposition FROM charges WHERE docket_number = %s AND seq = 1",
                    (f"{PREFIX}-001",))
        test("charge 1 disposition is NULL initially", cur.fetchone()[0] is None)

        # Update disposition
        analysis["charges"][0]["disposition"] = "Guilty Plea"
        analysis["charges"][0]["disposition_date"] = "03/15/2026"
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)

        cur.execute("SELECT disposition, disposition_date FROM charges WHERE docket_number = %s AND seq = 1",
                    (f"{PREFIX}-001",))
        row = cur.fetchone()
        test("charge 1 disposition updated to Guilty Plea", row[0] == "Guilty Plea")
        test("charge 1 disposition_date set", row[1] == "03/15/2026")

        # Verify no duplicate charges
        cur.execute("SELECT COUNT(*) FROM charges WHERE docket_number = %s", (f"{PREFIX}-001",))
        test("still exactly 2 charges", cur.fetchone()[0] == 2)

    # ------------------------------------------------------------------
    print("\n--- 11. Bail: update amount ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("001")
        analysis["bail"]["amount"] = "$25,000.00"
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)

        cur = conn.cursor()
        cur.execute("SELECT amount FROM bail WHERE docket_number = %s", (f"{PREFIX}-001",))
        test("bail updated to $25,000", cur.fetchone()[0] == "$25,000.00")

        cur.execute("SELECT COUNT(*) FROM bail WHERE docket_number = %s", (f"{PREFIX}-001",))
        test("still exactly 1 bail row", cur.fetchone()[0] == 1)

    # ------------------------------------------------------------------
    print("\n--- 12. Attorneys: no duplicates on re-store ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("001", attorneys=3)
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)
        db.store_parsed_data(conn, f"{PREFIX}-001", analysis)

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM attorneys WHERE docket_number = %s", (f"{PREFIX}-001",))
        test("3 attorneys, no dupes after 2 stores", cur.fetchone()[0] == 3)

    # ------------------------------------------------------------------
    print("\n--- 13. Events: upsert + status update ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        event_case = make_case("002")
        event_case["event_type"] = "Preliminary Hearing"
        event_case["event_status"] = "Scheduled"
        event_case["event_date"] = "04/10/2026 10:00 AM"
        event_case["event_location"] = "Courtroom 2A"

        total, new = db.upsert_events(conn, [event_case])
        test("event inserted", new == 1)

        # Update status
        event_case["event_status"] = "Continued"
        total, new = db.upsert_events(conn, [event_case])
        test("event re-upsert is not new", new == 0)

        cur = conn.cursor()
        cur.execute("SELECT event_status FROM events WHERE docket_number = %s", (f"{PREFIX}-002",))
        test("event status updated to Continued", cur.fetchone()[0] == "Continued")

        cur.execute("SELECT COUNT(*) FROM events WHERE docket_number = %s", (f"{PREFIX}-002",))
        test("still 1 event row", cur.fetchone()[0] == 1)

    # ------------------------------------------------------------------
    print("\n--- 14. Change detection: initial ingest ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        analysis = make_analysis("003")
        db.upsert_case(conn, make_case("003"))
        changes = db.detect_and_store_changes(conn, f"{PREFIX}-003", analysis)
        test("initial ingest detected", any(c["field"] == "initial_ingest" for c in changes))

    # ------------------------------------------------------------------
    print("\n--- 15. Change detection: no change ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        changes = db.detect_and_store_changes(conn, f"{PREFIX}-003", analysis)
        test("identical data = no changes", len(changes) == 0)

    # ------------------------------------------------------------------
    print("\n--- 16. Change detection: status change ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        modified = json.loads(json.dumps(analysis))
        modified["case_status"] = "Closed"
        changes = db.detect_and_store_changes(conn, f"{PREFIX}-003", modified)
        test("status change detected", any(c["field"] == "case_status" for c in changes))
        status_change = [c for c in changes if c["field"] == "case_status"][0]
        test("old status is Active", status_change["old"] == "Active")
        test("new status is Closed", status_change["new"] == "Closed")

    # ------------------------------------------------------------------
    print("\n--- 17. Change detection: new disposition ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        modified = json.loads(json.dumps(analysis))
        modified["charges"][0]["disposition"] = "Guilty"
        changes = db.detect_and_store_changes(conn, f"{PREFIX}-003", modified)
        test("disposition change detected",
             any("disposition" in c["field"] for c in changes),
             f"changes: {changes}")

    # ------------------------------------------------------------------
    print("\n--- 18. Change detection: new entries added ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        modified = json.loads(json.dumps(analysis))
        modified["docket_entries"].append({"date": "04/05/2026", "description": "New Motion", "filer": "DA"})
        changes = db.detect_and_store_changes(conn, f"{PREFIX}-003", modified)
        test("new entry detected",
             any("entries" in c["field"] for c in changes),
             f"changes: {changes}")

    # ------------------------------------------------------------------
    print("\n--- 19. Change detection: bail change ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        modified = json.loads(json.dumps(analysis))
        modified["bail"]["amount"] = "$50,000.00"
        changes = db.detect_and_store_changes(conn, f"{PREFIX}-003", modified)
        test("bail amount change detected",
             any("bail" in c["field"] for c in changes),
             f"changes: {changes}")

    # ------------------------------------------------------------------
    print("\n--- 20. Change log persistence ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        logs = db.get_changes(conn, docket_number=f"{PREFIX}-003")
        test("change_log has entries", len(logs) >= 3, f"got {len(logs)}")
        fields = [l["field"] for l in logs]
        test("status change in log", "case_status" in fields)

    # ------------------------------------------------------------------
    print("\n--- 21. Queue: prevents double-queue ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        id1, s1 = db.queue_ingest(conn, f"{PREFIX}-Q1")
        id2, s2 = db.queue_ingest(conn, f"{PREFIX}-Q1")
        test("double queue returns same id", id1 == id2)

    # ------------------------------------------------------------------
    print("\n--- 22. Queue: completed jobs allow re-queue ---")
    # ------------------------------------------------------------------
    with db.connect() as conn:
        job = db.claim_ingest_job(conn)
        if job:
            db.complete_ingest_job(conn, job[0])
        id3, s3 = db.queue_ingest(conn, f"{PREFIX}-Q1")
        test("re-queue after completion gets new id", id3 != id1, f"id1={id1} id3={id3}")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    print("\n--- Cleanup ---")
    with db.connect() as conn:
        cleanup(conn)
    print("  Test data cleaned")

    print(f"\n{'='*60}")
    print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'='*60}\n")
    return FAIL == 0


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)

#!/usr/bin/env python3
"""End-to-end tests for the two-pass job flow.

Tests against live API on the droplet. Requires DATABASE_URL + ANTHROPIC_API_KEY + GEMINI_API_KEY.

Run on droplet:
  cd /opt/ujs && export $(grep -v '^#' .env | grep -v '^$' | xargs) && .venv/bin/python -m tests.test_job_flow
"""

import json, os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

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


def run_job(question, timeout=90):
    """Create and poll a job until completion. Returns job dict."""
    from ujs.chat.jobs import create_job, get_job
    job_id = create_job(question)
    chunks = []  # track response growth
    for _ in range(timeout):
        time.sleep(1)
        j = get_job(job_id)
        if j:
            resp_len = len(j.get("response", ""))
            if not chunks or resp_len != chunks[-1]:
                chunks.append(resp_len)
            if j["status"] in ("completed", "error"):
                j["_chunks"] = chunks
                return j
    return None


# ---------------------------------------------------------------
print("\n" + "=" * 60)
print("Job Flow — Person Query (should get court + news)")
print("=" * 60)

j = run_job("tell me about Krasley, Jason Michael hearing today")
test("job completed", j and j["status"] == "completed")
if j:
    resp = j.get("response", "")
    tools = j.get("tools_log", [])
    chunks = j.get("_chunks", [])

    test("used search_cases", "search_cases" in tools)
    test("used get_person_history", "get_person_history" in tools)
    test("has court data (docket number)", "CP-39-CR-" in resp)
    test("has news section", "News Coverage" in resp)
    test("no loading indicator left", "Searching for news coverage..." not in resp)
    test("no status crumbs in content",
         "..web search" not in resp.split("\n\n", 1)[-1][:50] if "\n\n" in resp else True)
    test("response streamed incrementally", len(chunks) >= 3,
         f"only {len(chunks)} size changes")

    # Check conversation was saved
    from ujs import db
    conv_id = j.get("conversation_id")
    if conv_id:
        with db.connect() as conn:
            cur = db._dict_cur(conn)
            cur.execute("SELECT messages FROM conversations WHERE id = %s", (conv_id,))
            c = cur.fetchone()
            if c:
                msgs = json.loads(c["messages"]) if isinstance(c["messages"], str) else c["messages"]
                asst = [m for m in msgs if m["role"] == "assistant"]
                test("conversation saved", len(asst) > 0)
                if asst:
                    saved = asst[-1]["content"]
                    test("saved has court data", "CP-39-CR-" in saved)
                    test("saved has news", "News Coverage" in saved)
                    test("saved has no status crumbs", ".." not in saved[:20])


# ---------------------------------------------------------------
print("\n" + "=" * 60)
print("Job Flow — Bulk Query (should NOT get news)")
print("=" * 60)

j2 = run_job("what hearings are scheduled today?")
test("bulk job completed", j2 and j2["status"] == "completed")
if j2:
    resp = j2.get("response", "")
    test("bulk has no news section", "News Coverage" not in resp)
    test("bulk has no loading indicator", "Searching for news" not in resp)


# ---------------------------------------------------------------
print("\n" + "=" * 60)
print("Job Flow — Docket Lookup (should NOT get news)")
print("=" * 60)

j3 = run_job("look up CP-39-CR-0001517-2025")
test("docket job completed", j3 and j3["status"] == "completed")
if j3:
    resp = j3.get("response", "")
    test("docket has no news section", "News Coverage" not in resp)


# ---------------------------------------------------------------
print("\n" + "=" * 60)
print("Job Flow — News Cache")
print("=" * 60)

start = time.time()
j4 = run_job("tell me about Krasley, Jason Michael")
t1 = time.time() - start

start = time.time()
j5 = run_job("tell me about Krasley, Jason Michael")
t2 = time.time() - start

test("first query completed", j4 and j4["status"] == "completed")
test("second query completed", j5 and j5["status"] == "completed")
if j4 and j5:
    test("second query faster (cache hit)", t2 < t1 * 0.8,
         f"first: {t1:.1f}s, second: {t2:.1f}s")
    test("both have news", "News Coverage" in j4.get("response", "") and "News Coverage" in j5.get("response", ""))


# ---------------------------------------------------------------
# Cleanup test conversations (those with no user_id from direct create_job calls)
try:
    from ujs import db
    with db.connect() as conn:
        cur = conn.cursor()
        for jj in [j, j2, j3, j4, j5]:
            if jj and jj.get("conversation_id"):
                cur.execute("DELETE FROM chat_jobs WHERE conversation_id = %s", (jj["conversation_id"],))
                cur.execute("DELETE FROM conversations WHERE id = %s", (jj["conversation_id"],))
        print("Cleaned up test conversations")
except Exception as e:
    print(f"Cleanup warning: {e}")

print(f"\n{'=' * 60}")
print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
print(f"{'=' * 60}\n")
sys.exit(0 if FAIL == 0 else 1)

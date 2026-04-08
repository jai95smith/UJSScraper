"""Background chat job runner — generates responses server-side."""

import json, os, re, time, threading, uuid

import anthropic

from ujs import db
from ujs.chat.prompts import get_court_prompt, get_news_prompt
from ujs.chat.tools import TOOLS, get_news_tools
from ujs.chat.executors import execute_tool


def create_job(question, history=None, conversation_id=None):
    """Create a chat job and start processing in background. Returns job_id."""
    job_id = str(uuid.uuid4())[:12]
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO chat_jobs (id, question, history, conversation_id) VALUES (%s, %s, %s, %s)",
            (job_id, question, json.dumps(history or []), conversation_id)
        )
    thread = threading.Thread(target=_run_job, args=(job_id, question, history, conversation_id), daemon=True)
    thread.start()
    return job_id


def get_job(job_id):
    """Get current job state."""
    with db.connect() as conn:
        cur = db._dict_cur(conn)
        cur.execute("SELECT * FROM chat_jobs WHERE id = %s", (job_id,))
        row = cur.fetchone()
        if row:
            return dict(row)
    return None


def _update_job(job_id, **kwargs):
    """Update job fields."""
    with db.connect() as conn:
        cur = conn.cursor()
        sets = []
        params = []
        for k, v in kwargs.items():
            if k == "append_response":
                sets.append("response = response || %s")
                params.append(v)
            elif k == "replace_in_response":
                old, new = v
                sets.append("response = REPLACE(response, %s, %s)")
                params.extend([old, new])
            elif k == "append_tool":
                sets.append("tools_log = array_append(tools_log, %s)")
                params.append(v)
            else:
                sets.append(f"{k} = %s")
                params.append(v)
        params.append(job_id)
        cur.execute(f"UPDATE chat_jobs SET {', '.join(sets)} WHERE id = %s", params)


def _save_to_conversation(conversation_id, response_text):
    """Append assistant response to conversation messages."""
    if not conversation_id:
        return
    try:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT messages FROM conversations WHERE id = %s", (conversation_id,))
            row = cur.fetchone()
            if row:
                msgs = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or [])
                msgs.append({"role": "assistant", "content": response_text})
                cur.execute("UPDATE conversations SET messages = %s, updated_at = NOW() WHERE id = %s",
                            (json.dumps(msgs), conversation_id))
    except Exception:
        pass


def _is_person_query(question, court_answer):
    """Check if this query is about a specific named person (worth searching news)."""
    # If the court answer mentions a specific person's cases, it's a person query
    if any(kw in court_answer.lower() for kw in ["docket", "charges", "case", "hearing", "bail"]):
        # Check it's not a bulk query
        if not any(kw in question.lower() for kw in ["how many", "stats", "filing", "trend", "coverage"]):
            return True
    return False


def _extract_person_context(question, court_answer):
    """Extract person name and case summary for news search."""
    # Take first 500 chars of court answer as context
    return f"Question: {question}\n\nCourt records answer:\n{court_answer[:500]}"


def _run_tool_loop(client, system, tools, messages, job_id, timeout_at, silent=False):
    """Run a tool-use loop until end_turn or timeout. Returns final text.
    If silent=True, don't write status/tool names to the job response."""
    for round_num in range(20):
        if time.time() > timeout_at:
            return None

        response = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=2048,
            system=system, tools=tools, messages=messages,
        )

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "server_tool_use":
                    if not silent:
                        _update_job(job_id, append_tool="web_search")
                elif block.type == "tool_use":
                    if not silent:
                        tool_name = block.name.replace("_", " ")
                        _update_job(job_id, append_response=f"..{tool_name}", append_tool=block.name)
                    result = execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            if tool_results:
                messages.append({"role": "user", "content": tool_results})
        else:
            text_parts = [b.text for b in response.content if hasattr(b, "text")]
            return "".join(text_parts) if text_parts else ""

    return None


def _run_job(job_id, question, history, conversation_id=None):
    """Run the chat job — two passes: court data, then news."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        _update_job(job_id, status="error", error="ANTHROPIC_API_KEY not set")
        return

    client = anthropic.Anthropic(api_key=key)
    start = time.time()
    timeout_at = start + 120

    # Build messages
    messages = []
    if history:
        for h in history:
            if h.get("role") in ("user", "assistant") and h.get("content"):
                messages.append({"role": h["role"], "content": h["content"]})
    if not messages or messages[-1].get("content") != question:
        messages.append({"role": "user", "content": question})

    _update_job(job_id, append_response="Searching court records")

    try:
        # ---------------------------------------------------------------
        # Pass 1: Court data (no web search tools)
        # ---------------------------------------------------------------
        court_answer = _run_tool_loop(
            client, get_court_prompt(), TOOLS, list(messages),
            job_id, timeout_at,
        )

        if court_answer is None:
            _update_job(job_id, append_response="\n\nRequest timed out.", status="completed", completed_at="NOW()")
            return

        _update_job(job_id, append_response="\n\n" + court_answer)

        # ---------------------------------------------------------------
        # Pass 2: News search (only for person queries, separate call)
        # ---------------------------------------------------------------
        if _is_person_query(question, court_answer) and time.time() < timeout_at - 15:
            _update_job(job_id, append_response="\n\n---\n\n*Searching for news coverage...*")

            news_messages = [{
                "role": "user",
                "content": _extract_person_context(question, court_answer),
            }]

            news_text = _run_tool_loop(
                client, get_news_prompt(), get_news_tools(), news_messages,
                job_id, timeout_at, silent=True,
            )

            news_loading = "\n\n---\n\n*Searching for news coverage...*"
            if news_text and "NO_NEWS_FOUND" not in news_text:
                # Strip any duplicate header Claude may have added
                clean_news = news_text.strip()
                for prefix in ["## News Coverage\n", "**News Coverage**\n", "### News Coverage\n"]:
                    if clean_news.startswith(prefix):
                        clean_news = clean_news[len(prefix):].strip()
                news_final = "\n\n---\n\n**News Coverage**\n\n" + clean_news
                _update_job(job_id, replace_in_response=(news_loading, news_final))
            else:
                # No news — remove the loading indicator
                _update_job(job_id, replace_in_response=(news_loading, ""))

        # ---------------------------------------------------------------
        # Done
        # ---------------------------------------------------------------
        duration = int((time.time() - start) * 1000)
        _update_job(job_id, status="completed", completed_at="NOW()")

        # Save to conversation — use clean court_answer + news, not raw job response
        save_text = court_answer or ""
        job = get_job(job_id)
        raw = job.get("response", "")
        # Extract news section if present
        news_idx = raw.find("\n\n---\n\n**News Coverage**")
        if news_idx >= 0:
            save_text += raw[news_idx:]
        _save_to_conversation(conversation_id, save_text)

        # Log
        try:
            with db.connect() as conn:
                cur = conn.cursor()
                job = get_job(job_id)
                cur.execute(
                    "INSERT INTO query_log (question, tools_used, response_length, duration_ms) VALUES (%s, %s, %s, %s)",
                    (question[:500], job.get("tools_log", []), len(job.get("response", "")), duration)
                )
        except Exception:
            pass

    except Exception as e:
        _update_job(job_id, status="error", error=str(e)[:500], completed_at="NOW()")

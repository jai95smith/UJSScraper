"""Background chat job runner — generates responses server-side."""

import json, os, time, threading, uuid

import anthropic

from ujs import db
from ujs.chat.prompts import get_system_prompt
from ujs.chat.tools import TOOLS, get_news_tool
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


def _run_job(job_id, question, history, conversation_id=None):
    """Run the chat job — tool calls + streaming response."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        _update_job(job_id, status="error", error="ANTHROPIC_API_KEY not set")
        return

    client = anthropic.Anthropic(api_key=key)
    start = time.time()

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
        for round_num in range(20):
            if time.time() - start > 120:
                _update_job(job_id, append_response="\n\nRequest timed out.", status="completed", completed_at="NOW()")
                return

            response = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=2048,
                system=get_system_prompt(), tools=TOOLS + [get_news_tool()], messages=messages,
            )

            if response.stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": response.content})
                tool_results = []
                for block in response.content:
                    if block.type == "server_tool_use":
                        _update_job(job_id, append_response="..web search", append_tool="web_search")
                    elif block.type == "tool_use":
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
                # Log server-side tools (web_search) that ran in this final turn
                for block in response.content:
                    if block.type == "server_tool_use":
                        _update_job(job_id, append_response="..web search", append_tool="web_search")

                # Extract text from the response we already have
                text_parts = [b.text for b in response.content if hasattr(b, "text")]
                if text_parts:
                    _update_job(job_id, append_response="\n\n" + "".join(text_parts))
                else:
                    _update_job(job_id, append_response="\n\nNo response generated.")

                duration = int((time.time() - start) * 1000)
                _update_job(job_id, status="completed", completed_at="NOW()")

                # Save response to conversation
                job = get_job(job_id)
                response_text = job.get("response", "")
                idx = response_text.find("\n\n")
                if idx >= 0:
                    response_text = response_text[idx + 2:]
                _save_to_conversation(conversation_id, response_text)

                # Log to query_log
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
                return

        _update_job(job_id, append_response="\n\nCould not resolve answer.", status="completed", completed_at="NOW()")

    except Exception as e:
        _update_job(job_id, status="error", error=str(e)[:500], completed_at="NOW()")

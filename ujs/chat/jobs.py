"""Background chat job runner — generates responses server-side."""

import json, os, re, time, threading, uuid

import anthropic

from ujs import db
from ujs.chat.prompts import get_court_prompt, get_news_prompt
from ujs.chat.tools import TOOLS, get_news_tools
from ujs.chat.executors import execute_tool
from ujs.chat.cleanup import structure_news, classify_and_extract

# In-memory news cache: {key: (structured_text, timestamp)}
_news_cache = {}
_NEWS_CACHE_TTL = 86400  # 24 hours
_NEWS_CACHE_MAX = 500  # max entries


def _cache_set(key, value):
    """Set cache entry, evicting oldest if over max size."""
    _news_cache[key] = (value, time.time())
    if len(_news_cache) > _NEWS_CACHE_MAX:
        # Evict oldest entries
        sorted_keys = sorted(_news_cache, key=lambda k: _news_cache[k][1])
        for k in sorted_keys[:len(_news_cache) - _NEWS_CACHE_MAX]:
            del _news_cache[k]


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


_ALLOWED_JOB_COLUMNS = {"status", "error", "completed_at"}


def _update_job(job_id, **kwargs):
    """Update job fields. Column names are whitelisted to prevent SQL injection."""
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
            elif k in _ALLOWED_JOB_COLUMNS:
                if v == "NOW()":
                    sets.append(f"{k} = NOW()")
                else:
                    sets.append(f"{k} = %s")
                    params.append(v)
            else:
                raise ValueError(f"Disallowed column in _update_job: {k}")
        params.append(job_id)
        cur.execute(f"UPDATE chat_jobs SET {', '.join(sets)} WHERE id = %s", params)


def _save_to_conversation(conversation_id, response_text):
    """Append assistant response to conversation messages. Uses row lock to prevent lost updates."""
    if not conversation_id:
        return
    try:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("SELECT messages FROM conversations WHERE id = %s FOR UPDATE", (conversation_id,))
            row = cur.fetchone()
            if row:
                msgs = json.loads(row[0]) if isinstance(row[0], str) else (row[0] or [])
                msgs.append({"role": "assistant", "content": response_text})
                cur.execute("UPDATE conversations SET messages = %s, updated_at = NOW() WHERE id = %s",
                            (json.dumps(msgs), conversation_id))
    except Exception:
        pass





def _run_tool_loop(client, system, tools, messages, job_id, timeout_at, silent=False, stream=False):
    """Run a tool-use loop until end_turn or timeout. Returns final text.
    If silent=True, don't write status/tool names to the job response.
    If stream=True, stream the final text response to DB in chunks (buffers fenced blocks)."""
    for round_num in range(20):
        if time.time() > timeout_at:
            return None

        if stream and not silent:
            # Use streaming API — handles both tool_use and end_turn in one call
            result = _streamed_turn(client, system, tools, messages, job_id)
            if result is None:
                continue  # tool_use turn — loop continues
            return result  # end_turn — final text
        else:
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
                            _update_job(job_id, append_tool=block.name)
                        result = execute_tool(block.name, block.input)
                        # Handle large table injection — write directly to DB response
                        clean_result = result
                        if "<!--TABLE_INJECT:" in result:
                            start = result.index("<!--TABLE_INJECT:") + 17
                            end = result.index(":TABLE_INJECT-->")
                            table_json = result[start:end]
                            if not silent:
                                _update_job(job_id, append_response=f"\n\n```table\n{table_json}\n```\n\n")
                            clean_result = result[:result.index("<!--TABLE_INJECT:")]
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": clean_result,
                        })
                if tool_results:
                    messages.append({"role": "user", "content": tool_results})
            else:
                text_parts = [b.text for b in response.content if hasattr(b, "text")]
                return "".join(text_parts) if text_parts else ""

    return None


def _streamed_turn(client, system, tools, messages, job_id):
    """Single streaming turn. Returns final text on end_turn, None on tool_use (caller loops).
    Buffers fenced blocks (```...```) so partial JSON never hits the DB.
    Properly tracks tool IDs and handles server_tool_use blocks."""
    full_text = ""
    buffer = ""
    fence_depth = 0  # Tracks nested ``` pairs
    backtick_trail = ""  # Tracks partial ``` across chunk boundaries
    first_text = True
    last_flush = time.time()

    # Tool tracking: {block_id: {"name": str, "json_str": str}}
    tool_blocks = {}
    current_block_id = None
    current_block_type = None
    has_tool_use = False

    def _flush(force=False):
        nonlocal buffer, first_text, last_flush
        if not buffer:
            return
        if fence_depth > 0 and not force:
            return  # Hold fenced content
        if first_text:
            _update_job(job_id, append_response="\n\n" + buffer)
            first_text = False
        else:
            _update_job(job_id, append_response=buffer)
        buffer = ""
        last_flush = time.time()

    def _track_fences(text):
        """Track ``` fence markers, handling partial sequences across chunks."""
        nonlocal fence_depth, backtick_trail
        check = backtick_trail + text
        backtick_trail = ""
        i = 0
        while i < len(check):
            if check[i] == '`':
                # Count consecutive backticks
                start = i
                while i < len(check) and check[i] == '`':
                    i += 1
                count = i - start
                if count >= 3:
                    if fence_depth > 0:
                        fence_depth -= 1
                    else:
                        fence_depth += 1
                elif i == len(check):
                    # Partial backticks at end of chunk — carry over
                    backtick_trail = check[start:]
            else:
                i += 1

    try:
        with client.messages.stream(
            model="claude-sonnet-4-20250514", max_tokens=2048,
            system=system, tools=tools, messages=messages,
        ) as stream:
            for event in stream:
                if not hasattr(event, 'type'):
                    continue

                if event.type == 'content_block_start':
                    block = event.content_block
                    btype = getattr(block, 'type', None)
                    current_block_id = getattr(block, 'id', None)
                    current_block_type = btype

                    if btype == 'tool_use':
                        has_tool_use = True
                        name = block.name
                        _update_job(job_id, append_tool=name)
                        tool_blocks[block.id] = {"name": name, "json_str": ""}
                    elif btype == 'server_tool_use':
                        has_tool_use = True
                        _update_job(job_id, append_tool="web_search")

                elif event.type == 'content_block_delta':
                    delta = event.delta
                    dtype = getattr(delta, 'type', None)

                    if dtype == 'input_json_delta' and current_block_id in tool_blocks:
                        tool_blocks[current_block_id]["json_str"] += delta.partial_json
                    elif dtype == 'text_delta':
                        text = delta.text
                        full_text += text
                        buffer += text
                        _track_fences(text)
                        now = time.time()
                        if fence_depth == 0 and (len(buffer) >= 500 or now - last_flush >= 0.5):
                            _flush()
                            last_flush = now

                elif event.type == 'content_block_stop':
                    current_block_id = None
                    current_block_type = None

        # Flush remaining buffer
        _flush(force=True)

    except Exception as e:
        _flush(force=True)
        if not full_text and not tool_blocks:
            return f"Streaming error: {str(e)[:200]}"

    # If tool_use turn, execute tools and update message history
    client_tools = {bid: tb for bid, tb in tool_blocks.items()
                    if tb["name"] != "web_search"}
    if has_tool_use and client_tools:
        assistant_content = []
        if full_text:
            assistant_content.append({"type": "text", "text": full_text})
        for bid, tb in tool_blocks.items():
            try:
                inp = json.loads(tb["json_str"]) if tb["json_str"] else {}
            except json.JSONDecodeError:
                inp = {}
            assistant_content.append({"type": "tool_use", "id": bid, "name": tb["name"], "input": inp})

        messages.append({"role": "assistant", "content": assistant_content})
        tool_results = []
        for bid, tb in client_tools.items():
            try:
                inp = json.loads(tb["json_str"]) if tb["json_str"] else {}
            except json.JSONDecodeError:
                inp = {}
            result = execute_tool(tb["name"], inp)
            # Handle large table injection
            clean_result = result
            if "<!--TABLE_INJECT:" in result:
                start = result.index("<!--TABLE_INJECT:") + 17
                end = result.index(":TABLE_INJECT-->")
                table_json = result[start:end]
                _update_job(job_id, append_response=f"\n\n```table\n{table_json}\n```\n\n")
                clean_result = result[:result.index("<!--TABLE_INJECT:")]
            tool_results.append({"type": "tool_result", "tool_use_id": bid, "content": clean_result})
        if tool_results:
            messages.append({"role": "user", "content": tool_results})
        return None  # tool_use turn — caller should loop

    # server_tool_use only (web_search) — response includes results already
    if has_tool_use and not client_tools:
        return full_text  # Text from the server-tool turn

    return full_text


def _run_job(job_id, question, history, conversation_id=None):
    """Run the chat job — two sequential passes: court data, then news.
    Job stays 'running' until both passes complete. Frontend sees court
    data as it arrives via polling, then news appends before completion."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        _update_job(job_id, status="error", error="ANTHROPIC_API_KEY not set")
        return

    client = anthropic.Anthropic(api_key=key)
    start = time.time()
    timeout_at = start + 180  # 3 minutes — large tables need multiple API rounds

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
            job_id, timeout_at, stream=True,
        )

        if court_answer is None:
            _update_job(job_id, append_response="\n\nRequest timed out.", status="completed", completed_at="NOW()")
            return
        # Court answer already streamed to DB by _stream_final_response

        # ---------------------------------------------------------------
        # Pass 2: News search (non-blocking — user sees court data immediately)
        # ---------------------------------------------------------------
        # Classify + extract in one Gemini call (~500ms)
        is_person, context = classify_and_extract(question, court_answer)

        if is_person and context and time.time() < timeout_at - 15:
            # Check cache first
            context_first_line = context.split("\n")[0].lower().strip()
            cache_key = context_first_line + ":" + " ".join(sorted(question.lower().split()))
            cached = _news_cache.get(cache_key)

            if cached and (time.time() - cached[1]) < _NEWS_CACHE_TTL:
                news_section = "\n\n---\n\n**News Coverage**\n\n" + cached[0]
                _update_job(job_id, append_response=news_section)
                _update_job(job_id, status="completed", completed_at="NOW()")
                _save_to_conversation(conversation_id, court_answer + news_section)
            else:
                # Mark court data as complete so user can read it + interact
                _update_job(job_id, append_response="\n\n---\n\n*Searching for news coverage...*")
                _update_job(job_id, status="completed", completed_at="NOW()")
                _save_to_conversation(conversation_id, court_answer)

                # Run news search in background thread — appends to response when done
                def _news_worker():
                    try:
                        news_messages = [{"role": "user", "content": context}]
                        news_text = _run_tool_loop(
                            client, get_news_prompt(), get_news_tools(), news_messages,
                            job_id, timeout_at, silent=True,
                        )
                        news_loading = "\n\n---\n\n*Searching for news coverage...*"
                        if news_text and "NO_NEWS_FOUND" not in news_text:
                            structured = structure_news(news_text)
                            if structured:
                                _cache_set(cache_key, structured)
                                news_section = "\n\n---\n\n**News Coverage**\n\n" + structured
                                _update_job(job_id, replace_in_response=(news_loading, news_section))
                                _save_to_conversation(conversation_id, court_answer + news_section)
                            else:
                                _update_job(job_id, replace_in_response=(news_loading, ""))
                        else:
                            _update_job(job_id, replace_in_response=(news_loading, ""))
                    except Exception as e:
                        print(f"[news_worker] Error: {e}")
                        _update_job(job_id, replace_in_response=(
                            "\n\n---\n\n*Searching for news coverage...*", ""))

                threading.Thread(target=_news_worker, daemon=True).start()
        else:
            # No news needed — just complete
            _update_job(job_id, status="completed", completed_at="NOW()")
            _save_to_conversation(conversation_id, court_answer)

        # Log
        duration = int((time.time() - start) * 1000)
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

"""Background chat job runner — generates responses server-side."""

import hashlib, json, os, re, time, threading, uuid

import anthropic

from ujs import db
from ujs.chat.prompts import get_court_prompt, get_news_prompt
from ujs.chat.tools import TOOLS, get_news_tools
from ujs.chat.executors import execute_tool
from ujs.chat.cleanup import structure_news, classify_and_extract
from ujs import cache as rcache

# Pricing: Claude Sonnet 4
_PRICE_INPUT = 3.0 / 1_000_000
_PRICE_OUTPUT = 15.0 / 1_000_000
_settings_cache = {"data": {}, "expires": 0}


def _get_setting(key, default="0"):
    now = time.time()
    if now >= _settings_cache["expires"]:
        try:
            with db.connect() as conn:
                cur = conn.cursor()
                cur.execute("SELECT key, value FROM app_settings")
                _settings_cache["data"] = {r[0]: r[1] for r in cur.fetchall()}
                _settings_cache["expires"] = now + 300
        except Exception:
            pass
    return _settings_cache["data"].get(key, default)


def get_user_usage(user_id):
    limit = float(_get_setting("user_spend_limit", "5.0"))
    window_raw = _get_setting("user_spend_window_hours", "0")
    try:
        window = max(0, int(float(window_raw)))
    except (ValueError, TypeError):
        window = 0
    with db.connect() as conn:
        cur = conn.cursor()
        if window > 0:
            cur.execute("SELECT COALESCE(SUM(cost_usd), 0) FROM chat_jobs WHERE user_id = %s AND created_at > NOW() - make_interval(hours => %s)", (user_id, window))
        else:
            cur.execute("SELECT COALESCE(SUM(cost_usd), 0) FROM chat_jobs WHERE user_id = %s", (user_id,))
        spent = float(cur.fetchone()[0])
    return {"spent": round(spent, 4), "limit": limit, "remaining": round(max(0, limit - spent), 4)}


_ADMIN_BYPASS_EMAILS = {"jai95smith@gmail.com", "jsmith@lehighdaily.com"}


_ESTIMATED_COST_PER_QUERY = 0.10  # Pessimistic estimate reserved at job creation


def check_user_limit(user_id, email=None):
    if email and email in _ADMIN_BYPASS_EMAILS:
        return False
    usage = get_user_usage(user_id)
    return usage["remaining"] <= 0


def create_job(question, history=None, conversation_id=None, user_id=None):
    """Create a chat job and start processing in background. Returns job_id.
    Reserves a pessimistic cost estimate upfront to prevent race conditions."""
    job_id = str(uuid.uuid4()).replace('-', '')[:16]
    with db.connect() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO chat_jobs (id, question, history, conversation_id, user_id, cost_usd) VALUES (%s, %s, %s, %s, %s, %s)",
            (job_id, question, json.dumps(history or []), conversation_id, user_id, _ESTIMATED_COST_PER_QUERY)
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


def _process_tool_result(result, job_id, silent=False):
    """Process a tool result — detect _table auto-inject and TABLE_INJECT markers.
    Returns the cleaned result text for Claude (without table data)."""
    if not result or not isinstance(result, str):
        return result or ""

    # Auto-inject from tools: {"_summary": "...", "_table": {...}} or {"_summary": "...", "_chart": "..."}
    try:
        parsed = json.loads(result)
        if isinstance(parsed, dict):
            if "_table" in parsed:
                table_json = json.dumps(parsed["_table"])
                if not silent:
                    _update_job(job_id, append_response=f"\n\n```table\n{table_json}\n```\n\n")
                return parsed.get("_summary", "Table rendered.")
            if "_chart" in parsed:
                if not silent:
                    _update_job(job_id, append_response=f"\n\n```chart\n{parsed['_chart']}\n```\n\n")
                return parsed.get("_summary", "Chart rendered.")
    except (json.JSONDecodeError, TypeError):
        pass

    return result




def _run_tool_loop(client, system, tools, messages, job_id, timeout_at, silent=False, stream=False, usage_acc=None):
    """Run a tool-use loop until end_turn or timeout. Returns final text.
    If silent=True, don't write status/tool names to the job response.
    If stream=True, stream the final text response to DB in chunks (buffers fenced blocks)."""
    for round_num in range(10):
        if time.time() > timeout_at:
            return None

        if stream and not silent:
            # Use streaming API — handles both tool_use and end_turn in one call
            result = _streamed_turn(client, system, tools, messages, job_id, usage_acc=usage_acc)
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
                        clean_result = _process_tool_result(result, job_id, silent=silent)
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


def _streamed_turn(client, system, tools, messages, job_id, usage_acc=None):
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
        # Clean any leaked tool XML
        clean = re.sub(r'<invoke\b[^>]*>.*?</invoke>', '', buffer, flags=re.DOTALL)
        clean = re.sub(r'<invoke\b[^>]*>.*', '', clean, flags=re.DOTALL)
        clean = re.sub(r'<function_calls>.*?</function_calls>', '', clean, flags=re.DOTALL)
        clean = re.sub(r'<function_calls>.*', '', clean, flags=re.DOTALL)
        clean = re.sub(r'</invoke>|</function_calls>', '', clean)
        if not clean.strip():
            buffer = ""
            return
        if first_text:
            _update_job(job_id, append_response="\n\n" + clean)
            first_text = False
        else:
            _update_job(job_id, append_response=clean)
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
        ) as stream_ctx:
            for event in stream_ctx:
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
        # Capture usage (mutates usage_acc in place — no return type change)
        if usage_acc is not None:
            try:
                final = stream_ctx.get_final_message()
                if hasattr(final, 'usage'):
                    usage_acc["input"] += final.usage.input_tokens
                    usage_acc["output"] += final.usage.output_tokens
            except Exception:
                pass

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
            clean_result = _process_tool_result(result, job_id)
            tool_results.append({"type": "tool_result", "tool_use_id": bid, "content": clean_result})
        if tool_results:
            messages.append({"role": "user", "content": tool_results})
        return None  # tool_use turn — caller should loop

    # server_tool_use only (web_search) — response includes results already
    if has_tool_use and not client_tools:
        return full_text  # Text from the server-tool turn

    return full_text


def _save_job_cost(job_id, usage):
    inp = max(0, usage.get("input", 0))
    out = max(0, usage.get("output", 0))
    cost = inp * _PRICE_INPUT + out * _PRICE_OUTPUT
    try:
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("UPDATE chat_jobs SET input_tokens = %s, output_tokens = %s, cost_usd = %s WHERE id = %s",
                        (inp, out, round(cost, 6), job_id))
    except Exception as e:
        print(f"[cost] Failed to save cost for {job_id}: {e}")


def _run_job(job_id, question, history, conversation_id=None):
    """Run the chat job — two sequential passes: court data, then news.
    Job stays 'running' until both passes complete. Frontend sees court
    data as it arrives via polling, then news appends before completion."""
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        _update_job(job_id, status="error", error="ANTHROPIC_API_KEY not set")
        return

    # Check response cache (skip for follow-up questions in a conversation)
    is_first_message = not history or len(history) <= 1
    if is_first_message:
        cached = rcache.get_cached_response(question)
        if cached:
            _update_job(job_id, append_response=f"\n\n{cached}", status="completed", completed_at="NOW()")
            _save_to_conversation(conversation_id, cached)
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
        total_usage = {"input": 0, "output": 0}
        court_answer = _run_tool_loop(
            client, get_court_prompt(), TOOLS, list(messages),
            job_id, timeout_at, stream=True, usage_acc=total_usage,
        )

        if court_answer is None:
            _save_job_cost(job_id, total_usage)
            _update_job(job_id, append_response="\n\nRequest timed out.", status="completed", completed_at="NOW()")
            return

        # Get the full response from DB (includes auto-injected tables + streamed text)
        job_data = get_job(job_id)
        full_response = job_data.get("response", "") if job_data else ""
        # Strip the status prefix for saving
        idx = full_response.find("\n\n")
        save_text = full_response[idx + 2:] if idx >= 0 else full_response
        # Clean any leaked tool XML from response
        save_text = re.sub(r'<invoke\b[^>]*>.*?</invoke>', '', save_text, flags=re.DOTALL)
        save_text = re.sub(r'<function_calls>.*?</function_calls>', '', save_text, flags=re.DOTALL)
        save_text = re.sub(r'<invoke\b[^>]*>.*', '', save_text, flags=re.DOTALL)
        save_text = re.sub(r'<function_calls>.*', '', save_text, flags=re.DOTALL)
        save_text = re.sub(r'</invoke>|</function_calls>', '', save_text).strip()

        # ---------------------------------------------------------------
        # Pass 2: News search — only on first message in conversation
        # ---------------------------------------------------------------
        news_done = False
        if is_first_message:
            is_person, context = classify_and_extract(question, court_answer)

            if is_person and context and time.time() < timeout_at - 15:
                news_done = True
                # Cache key on person name only (not question phrasing)
                context_first_line = context.split("\n")[0].lower().strip()
                cache_key = hashlib.md5(context_first_line.encode()).hexdigest()[:16]
                cached = rcache.get_cached_news(cache_key)

                if cached:
                    news_section = "\n\n---\n\n**News Coverage**\n\n" + cached
                    _update_job(job_id, append_response=news_section)
                    _update_job(job_id, status="completed", completed_at="NOW()")
                    _save_to_conversation(conversation_id, save_text + news_section)
                else:
                    # Mark court data as complete so user can read it + interact
                    _update_job(job_id, append_response="\n\n---\n\n*Searching for news coverage...*")
                    _update_job(job_id, status="completed", completed_at="NOW()")
                    _save_to_conversation(conversation_id, save_text)

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
                                    rcache.set_cached_news(cache_key, structured)
                                    news_section = "\n\n---\n\n**News Coverage**\n\n" + structured
                                    _update_job(job_id, replace_in_response=(news_loading, news_section))
                                    _save_to_conversation(conversation_id, save_text + news_section)
                                else:
                                    _update_job(job_id, replace_in_response=(news_loading, ""))
                            else:
                                _update_job(job_id, replace_in_response=(news_loading, ""))
                        except Exception as e:
                            print(f"[news_worker] Error: {e}")
                            _update_job(job_id, replace_in_response=(
                                "\n\n---\n\n*Searching for news coverage...*", ""))

                    threading.Thread(target=_news_worker, daemon=True).start()

        if not news_done:
            _update_job(job_id, status="completed", completed_at="NOW()")
            _save_to_conversation(conversation_id, save_text)

        # Cache response for identical future queries
        if is_first_message and save_text:
            rcache.set_cached_response(question, save_text)

        # Save cost
        _save_job_cost(job_id, total_usage)

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
        _save_job_cost(job_id, total_usage)
        _update_job(job_id, status="error", error=str(e)[:500], completed_at="NOW()")

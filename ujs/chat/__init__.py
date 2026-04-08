"""Natural language chat — Claude + DB tools for court questions."""

import os, time
from typing import Optional

import anthropic

from ujs.chat.prompts import get_system_prompt
from ujs.chat.tools import TOOLS, WEB_SEARCH_TOOL
from ujs.chat.executors import execute_tool


def _log_query(question, tools_used, response_length, duration_ms, error=None):
    """Log query to database for debugging/tracking."""
    try:
        from ujs import db
        with db.connect() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO query_log (question, tools_used, response_length, duration_ms, error)
                VALUES (%s, %s, %s, %s, %s)
            """, (question[:500], tools_used, response_length, duration_ms, error))
    except Exception:
        pass


def _run_chat(client, model, question, max_rounds=10):
    """Run a tool-use chat loop. Returns (answer, tool_calls_made)."""
    start = time.time()
    messages = [{"role": "user", "content": question}]
    tool_calls = 0

    for _ in range(20):
        if time.time() - start > 120:
            return "Request timed out. Try a more specific question.", tool_calls

        try:
            response = client.messages.create(
                model=model, max_tokens=1024,
                system=get_system_prompt(), tools=TOOLS + [WEB_SEARCH_TOOL], messages=messages,
            )
        except Exception as e:
            return f"API error: {str(e)[:200]}", tool_calls

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tool_calls += 1
                    result = execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            if tool_results:
                messages.append({"role": "user", "content": tool_results})
        else:
            for block in response.content:
                if hasattr(block, "text"):
                    return block.text, tool_calls
            return "", tool_calls

    return "", tool_calls


def ask(question: str, api_key: Optional[str] = None) -> str:
    """Send a question, get an answer using Claude + DB tools."""
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("Set ANTHROPIC_API_KEY env var")

    start = time.time()
    client = anthropic.Anthropic(api_key=key)
    tools_used = []
    answer, tool_count = _run_chat(client, "claude-sonnet-4-20250514", question)
    duration = int((time.time() - start) * 1000)
    _log_query(question, [], len(answer or ""), duration)

    return answer or "I couldn't find enough data to answer that question. Try being more specific or providing a docket number."


def ask_stream(question: str, api_key: Optional[str] = None, history: Optional[list] = None):
    """Generator that yields answer chunks as Claude streams them."""
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        yield "Error: ANTHROPIC_API_KEY not set"
        return

    start = time.time()
    client = anthropic.Anthropic(api_key=key)
    tools_used = []

    messages = []
    if history:
        for h in history[:-1]:
            if h.get("role") in ("user", "assistant") and h.get("content"):
                messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": question})

    yield "Searching court records"

    for round_num in range(20):
        if time.time() - start > 90:
            yield "\n\nRequest timed out."
            _log_query(question, tools_used, 0, int((time.time() - start) * 1000), "timeout")
            return

        try:
            response = client.messages.create(
                model="claude-sonnet-4-20250514", max_tokens=1024,
                system=get_system_prompt(), tools=TOOLS + [WEB_SEARCH_TOOL], messages=messages,
            )
        except Exception as e:
            yield f"\n\nAPI error: {str(e)[:200]}"
            _log_query(question, tools_used, 0, int((time.time() - start) * 1000), str(e)[:300])
            return

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "server_tool_use":
                    tools_used.append("web_search")
                    yield "..web search"
                elif block.type == "tool_use":
                    tools_used.append(block.name)
                    yield f"..{block.name.replace('_', ' ')}"
                    result = execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            if tool_results:
                messages.append({"role": "user", "content": tool_results})
        else:
            yield "\n\n"
            full_text = ""
            try:
                with client.messages.stream(
                    model="claude-sonnet-4-20250514", max_tokens=1024,
                    system=get_system_prompt(), tools=TOOLS + [WEB_SEARCH_TOOL], messages=messages,
                ) as stream:
                    for text in stream.text_stream:
                        full_text += text
                        yield text
            except Exception as e:
                yield f"\n\nStreaming error: {str(e)[:200]}"
                _log_query(question, tools_used, len(full_text), int((time.time() - start) * 1000), str(e)[:300])
                return

            _log_query(question, tools_used, len(full_text), int((time.time() - start) * 1000))
            return

    yield "\n\nCould not resolve answer."
    _log_query(question, tools_used, 0, int((time.time() - start) * 1000), "max_rounds")

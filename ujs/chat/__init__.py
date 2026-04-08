"""Natural language chat — Claude + DB tools for court questions."""

import os, time
from typing import Optional

import anthropic

from ujs.chat.prompts import get_court_prompt, get_news_prompt
from ujs.chat.tools import TOOLS, get_news_tools
from ujs.chat.executors import execute_tool
from ujs.chat.cleanup import structure_news, is_person_query


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


def _run_tool_loop(client, model, system, tools, messages, timeout=120):
    """Run a tool-use loop. Returns (answer_text, tools_used)."""
    start = time.time()
    tools_used = []

    for _ in range(20):
        if time.time() - start > timeout:
            return "Request timed out.", tools_used

        try:
            response = client.messages.create(
                model=model, max_tokens=2048,
                system=system, tools=tools, messages=messages,
            )
        except Exception as e:
            return f"API error: {str(e)[:200]}", tools_used

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tools_used.append(block.name)
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
                if block.type == "server_tool_use":
                    tools_used.append("web_search")
            text_parts = [b.text for b in response.content if hasattr(b, "text")]
            return "".join(text_parts), tools_used

    return "", tools_used


def ask(question: str, api_key: Optional[str] = None) -> str:
    """Send a question, get an answer using Claude + DB tools. Two-pass: court data then news."""
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        raise RuntimeError("Set ANTHROPIC_API_KEY env var")

    start = time.time()
    client = anthropic.Anthropic(api_key=key)

    # Pass 1: Court data
    messages = [{"role": "user", "content": question}]
    court_answer, court_tools = _run_tool_loop(
        client, "claude-sonnet-4-20250514", get_court_prompt(), TOOLS, messages
    )

    if not court_answer:
        return "I couldn't find enough data to answer that question."

    # Pass 2: News (only for person queries)
    full_answer = court_answer
    if is_person_query(question, court_answer):
        context = f"Question: {question}\n\nCourt records answer:\n{court_answer[:500]}"
        news_answer, news_tools = _run_tool_loop(
            client, "claude-sonnet-4-20250514", get_news_prompt(),
            get_news_tools(), [{"role": "user", "content": context}],
            timeout=30,
        )
        if news_answer and "NO_NEWS_FOUND" not in news_answer:
            clean = structure_news(news_answer)
            if clean:
                full_answer += "\n\n---\n\n**News Coverage**\n\n" + clean

    duration = int((time.time() - start) * 1000)
    _log_query(question, court_tools + ["news_search"], len(full_answer), duration)
    return full_answer

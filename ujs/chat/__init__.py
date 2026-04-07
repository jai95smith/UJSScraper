"""Natural language chat — Claude + DB tools for court questions."""

import os, time
from typing import Optional

import anthropic

from ujs.chat.prompts import get_system_prompt
from ujs.chat.tools import TOOLS
from ujs.chat.executors import execute_tool


def _run_chat(client, model, question, max_rounds=10):
    """Run a tool-use chat loop. Returns (answer, tool_calls_made)."""
    start = time.time()
    messages = [{"role": "user", "content": question}]
    tool_calls = 0

    for _ in range(20):
        if time.time() - start > 120:
            return "Request timed out. Try a more specific question.", tool_calls

        response = client.messages.create(
            model=model, max_tokens=1024,
            system=get_system_prompt(), tools=TOOLS, messages=messages,
        )

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

    client = anthropic.Anthropic(api_key=key)
    answer, _ = _run_chat(client, "claude-sonnet-4-20250514", question)
    return answer or "I couldn't find enough data to answer that question. Try being more specific or providing a docket number."


def ask_stream(question: str, api_key: Optional[str] = None, history: Optional[list] = None):
    """Generator that yields answer chunks as Claude streams them."""
    key = api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        yield "Error: ANTHROPIC_API_KEY not set"
        return

    client = anthropic.Anthropic(api_key=key)

    messages = []
    if history:
        for h in history[:-1]:
            if h.get("role") in ("user", "assistant") and h.get("content"):
                messages.append({"role": h["role"], "content": h["content"]})
    messages.append({"role": "user", "content": question})

    yield "Searching court records"

    for _ in range(20):
        response = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=1024,
            system=get_system_prompt(), tools=TOOLS, messages=messages,
        )

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    yield f"..{block.name.replace('_', ' ')}"
                    result = execute_tool(block.name, block.input)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
            messages.append({"role": "user", "content": tool_results})
        else:
            yield "\n\n"
            with client.messages.stream(
                model="claude-sonnet-4-20250514", max_tokens=1024,
                system=get_system_prompt(), tools=TOOLS, messages=messages,
            ) as stream:
                for text in stream.text_stream:
                    yield text
            return

    yield "\n\nCould not resolve answer."

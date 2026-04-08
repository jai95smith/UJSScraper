#!/usr/bin/env python3
"""Tests for web_search tool integration.

Verifies:
1. web_search tool is included in API tool list
2. System prompt contains web search guidance
3. Claude calls web_search for serious/notable cases (via live API)
4. Claude does NOT call web_search for routine queries (via live API)

Live tests hit the Anthropic API — they cost money and take time.
Run selectively:
    python -m tests.test_web_search              # structural tests only (free)
    python -m tests.test_web_search --live        # include live API tests
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


# -------------------------------------------------------------------------
# Structural tests — no API calls
# -------------------------------------------------------------------------

def test_web_search_tool_definition():
    """Both provider tools are defined correctly."""
    from ujs.chat.tools import _CLAUDE_WEB_SEARCH, _GEMINI_NEWS_SEARCH
    test("Claude tool has correct type", _CLAUDE_WEB_SEARCH.get("type") == "web_search_20250305")
    test("Claude tool has no max_uses cap", "max_uses" not in _CLAUDE_WEB_SEARCH)
    test("Gemini tool has name", _GEMINI_NEWS_SEARCH.get("name") == "news_search")
    test("Gemini tool has input_schema", "input_schema" in _GEMINI_NEWS_SEARCH)


def test_provider_flag():
    """get_news_tools returns the right tools based on NEWS_SEARCH_PROVIDER."""
    from ujs.chat.tools import get_news_tools, _CLAUDE_WEB_SEARCH, _GEMINI_NEWS_SEARCH, _GENERATE_QUERIES
    import ujs.chat.tools as tools_mod

    original = tools_mod.NEWS_SEARCH_PROVIDER
    try:
        tools_mod.NEWS_SEARCH_PROVIDER = "claude"
        tools = get_news_tools()
        test("claude provider skips query generator (Claude does it)", _GENERATE_QUERIES not in tools)
        test("claude provider includes web_search", _CLAUDE_WEB_SEARCH in tools)
        tools_mod.NEWS_SEARCH_PROVIDER = "gemini"
        tools = get_news_tools()
        test("gemini provider includes query generator", _GENERATE_QUERIES in tools)
        test("gemini provider includes news_search", _GEMINI_NEWS_SEARCH in tools)
    finally:
        tools_mod.NEWS_SEARCH_PROVIDER = original


def test_tools_list_includes_news_tools():
    """TOOLS + get_news_tools() produces a valid tools array."""
    from ujs.chat.tools import TOOLS, get_news_tools
    combined = TOOLS + get_news_tools()
    names = [t.get("name") for t in combined if "name" in t]
    test("custom tools still present", "lookup_docket" in names and "get_person_history" in names)
    # generate_news_queries only in gemini mode
    has_gen = "generate_news_queries" in names
    from ujs.chat.tools import NEWS_SEARCH_PROVIDER
    if NEWS_SEARCH_PROVIDER == "gemini":
        test("generate_news_queries included (gemini mode)", has_gen)
    else:
        test("generate_news_queries excluded (claude mode)", not has_gen)
    has_search = "web_search" in names or "news_search" in names or \
                 any(t.get("type") == "web_search_20250305" for t in combined)
    test("search tool is included", has_search)


def test_system_prompt_has_web_search_rules():
    """System prompt contains web search guidance covering the key behavioral rules."""
    from ujs.chat.prompts import get_system_prompt
    prompt = get_system_prompt().lower()

    # Core: web search is mentioned at all
    test("prompt references web search", "web_search" in prompt or "web search" in prompt)

    # Must have trigger criteria (when to search)
    test("prompt defines when to search", "when to search" in prompt or "trigger" in prompt)

    # Must have exclusion criteria (when not to search)
    test("prompt defines when NOT to search", "when not to search" in prompt or "do not search" in prompt)

    # Must say what to do when nothing is found
    test("prompt handles no-results gracefully",
         "nothing" in prompt or "don't mention" in prompt or "don't mention" in prompt)

    # Must handle no-results gracefully (don't tell user you searched and found nothing)
    test("prompt handles empty results silently",
         ("do not mention" in prompt or "don't mention" in prompt) and "search" in prompt)

    # Must not speculate
    test("prompt forbids speculation",
         "speculate" in prompt or "never speculate" in prompt)


def test_executor_handlers():
    """web_search (server-side) not in HANDLERS, but news_search (gemini) is."""
    from ujs.chat.executors import HANDLERS
    test("web_search not in HANDLERS (server-handled)", "web_search" not in HANDLERS)
    test("news_search in HANDLERS (gemini provider)", "news_search" in HANDLERS)
    test("generate_news_queries in HANDLERS", "generate_news_queries" in HANDLERS)


# -------------------------------------------------------------------------
# Live API tests — sends real questions to Claude
# -------------------------------------------------------------------------

def _ask_and_get_tools(question, max_rounds=10):
    """Send question through chat, return (answer, tools_used).

    Tracks which tools Claude calls by intercepting the tool loop.
    """
    import anthropic
    from ujs.chat.prompts import get_system_prompt
    from ujs.chat.tools import TOOLS, WEB_SEARCH_TOOL
    from ujs.chat.executors import execute_tool

    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": question}]
    tools_used = []

    for _ in range(max_rounds):
        response = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=1024,
            system=get_system_prompt(),
            tools=TOOLS + [WEB_SEARCH_TOOL],
            messages=messages,
        )

        if response.stop_reason == "tool_use":
            messages.append({"role": "assistant", "content": response.content})
            tool_results = []
            for block in response.content:
                if block.type == "server_tool_use":
                    tools_used.append("web_search")
                elif block.type == "tool_use":
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
            text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    text += block.text
            return text, tools_used

    return "", tools_used


def test_live_should_search():
    """Claude SHOULD use web_search for notable cases."""
    print("\n  --- Should trigger web_search ---")

    # Scenario: user asks about a person with serious charges
    q = "Tell me about Jason Krasley in Lehigh County — what are his charges and any background?"
    print(f"  Asking: {q[:80]}...")
    answer, tools = _ask_and_get_tools(q)
    test("Krasley query uses web_search",
         "web_search" in tools,
         f"tools used: {tools}")

    # Scenario: user explicitly asks for news context
    q = "What cases does John Smith have in Lehigh County? Any news coverage?"
    print(f"  Asking: {q[:80]}...")
    answer, tools = _ask_and_get_tools(q)
    test("explicit news request uses web_search",
         "web_search" in tools,
         f"tools used: {tools}")


def test_live_should_not_search():
    """Claude should NOT use web_search for routine queries."""
    print("\n  --- Should NOT trigger web_search ---")

    # Scenario: routine stats query
    q = "How many criminal cases were filed in Lehigh County this month?"
    print(f"  Asking: {q[:80]}...")
    answer, tools = _ask_and_get_tools(q)
    test("stats query skips web_search",
         "web_search" not in tools,
         f"tools used: {tools}")

    # Scenario: today's hearings (bulk)
    q = "What hearings are scheduled for today?"
    print(f"  Asking: {q[:80]}...")
    answer, tools = _ask_and_get_tools(q)
    test("hearings query skips web_search",
         "web_search" not in tools,
         f"tools used: {tools}")

    # Scenario: docket lookup with no person context
    q = "Look up docket CP-39-CR-0000142-2025"
    print(f"  Asking: {q[:80]}...")
    answer, tools = _ask_and_get_tools(q)
    test("docket lookup skips web_search",
         "web_search" not in tools,
         f"tools used: {tools}")


def test_live_answer_quality():
    """When web_search is used, verify the answer follows formatting rules."""
    print("\n  --- Answer quality checks ---")

    q = "Tell me about Jason Krasley in Lehigh County. Include any news reports."
    print(f"  Asking: {q[:80]}...")
    answer, tools = _ask_and_get_tools(q)

    if "web_search" in tools:
        # If web search was used and found results, check formatting
        if "News Coverage" in answer or "news" in answer.lower():
            test("news section uses proper format",
                 "**News Coverage:**" in answer or "News Coverage" in answer,
                 f"answer snippet: {answer[:200]}")
        else:
            test("no news found — correctly omitted", True)
    else:
        print("  SKIP  web_search not called — can't check answer format")


# -------------------------------------------------------------------------
# Runner
# -------------------------------------------------------------------------

def run_structural():
    print("\n" + "=" * 60)
    print("Web Search — Structural Tests")
    print("=" * 60)
    test_web_search_tool_definition()
    test_provider_flag()
    test_tools_list_includes_news_tools()
    test_system_prompt_has_web_search_rules()
    test_executor_handlers()


def run_live():
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("\nSKIP: Live tests require ANTHROPIC_API_KEY")
        return
    print("\n" + "=" * 60)
    print("Web Search — Live API Tests (costs ~$0.10)")
    print("=" * 60)
    test_live_should_not_search()
    test_live_should_search()
    test_live_answer_quality()


if __name__ == "__main__":
    run_structural()
    if "--live" in sys.argv:
        run_live()

    print(f"\n{'=' * 60}")
    print(f"Results: {PASS} passed, {FAIL} failed, {PASS + FAIL} total")
    print(f"{'=' * 60}\n")
    sys.exit(0 if FAIL == 0 else 1)

"""Allow running as `python -m ujs <command>`."""

import sys


COMMANDS = {
    "search": "ujs.cli",
    "monitor": "ujs.modules.monitor",
    "docket": "ujs.modules.docket_pdf",
    "ingest": "ujs.modules.ingest",
    "notify": "ujs.modules.notify",
    "api": None,  # handled separately
    "mcp": None,  # handled separately
}

USAGE = """Usage: python -m ujs <command> [args]

Commands:
  search    Search by name, docket, OTN, date, or calendar events
  monitor   Hourly monitor for new filings & events (file-based)
  docket    Download and analyze docket sheet PDFs
  ingest    DB ingest pipeline — scrape, analyze, store, refresh
  notify    Send docket watch email notifications
  api       Start the REST API server

Examples:
  python -m ujs search --last Smith --county Lehigh --type Criminal
  python -m ujs search --calendar 3 --county Lehigh
  python -m ujs docket CP-39-CR-0000142-2025
  python -m ujs ingest --county Lehigh --type Criminal --once
  python -m ujs ingest --queue-only
  python -m ujs ingest --refresh-only
  python -m ujs api --port 8100
  python -m ujs mcp                    # stdio (Claude Code local)
  python -m ujs mcp --http --port 8200 # HTTP (remote/prod)
"""


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(USAGE)
        sys.exit(0)

    cmd = sys.argv[1]
    if cmd not in COMMANDS:
        print(f"Unknown command: {cmd}\n")
        print(USAGE)
        sys.exit(1)

    if cmd == "api":
        import argparse
        p = argparse.ArgumentParser(description="UJS REST API Server")
        p.add_argument("--host", default="0.0.0.0")
        p.add_argument("--port", type=int, default=8100)
        p.add_argument("--reload", action="store_true")
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        args = p.parse_args()
        import uvicorn
        uvicorn.run("ujs.api:app", host=args.host, port=args.port, reload=args.reload)
        return

    if cmd == "mcp":
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        from ujs.mcp_server import mcp as mcp_app
        if "--http" in sys.argv:
            port = 8200
            for i, arg in enumerate(sys.argv):
                if arg == "--port" and i + 1 < len(sys.argv):
                    port = int(sys.argv[i + 1])
            print(f"MCP server starting on http://0.0.0.0:{port}/mcp")
            mcp_app.run(transport="streamable-http", host="0.0.0.0", port=port)
        else:
            mcp_app.run()
        return

    # Remove the command name so argparse in submodules sees the right args
    sys.argv = [f"ujs {cmd}"] + sys.argv[2:]

    import importlib
    mod = importlib.import_module(COMMANDS[cmd])
    mod.main()


if __name__ == "__main__":
    main()

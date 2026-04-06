"""Allow running as `python -m ujs <command>`."""

import sys


COMMANDS = {
    "search": "ujs.cli",
    "monitor": "ujs.modules.monitor",
    "docket": "ujs.modules.docket_pdf",
}

USAGE = """Usage: python -m ujs <command> [args]

Commands:
  search    Search by name, docket, OTN, date, or calendar events
  monitor   Hourly monitor for new filings & events
  docket    Download and analyze docket sheet PDFs

Examples:
  python -m ujs search --last Smith --county Lehigh --type Criminal
  python -m ujs search --calendar 3 --county Lehigh
  python -m ujs monitor --county Lehigh --type Criminal --once
  python -m ujs monitor --county Lehigh --interval 60
  python -m ujs docket CP-39-CR-0000142-2025
  python -m ujs docket CP-39-CR-0000142-2025 --json
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

    # Remove the command name so argparse in submodules sees the right args
    sys.argv = [f"ujs {cmd}"] + sys.argv[2:]

    import importlib
    mod = importlib.import_module(COMMANDS[cmd])
    mod.main()


if __name__ == "__main__":
    main()

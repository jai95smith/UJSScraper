"""Docket number parser — normalizes any format to standard PA docket format."""

import re


# Standard formats:
# CP-39-CR-0001234-2025  (Common Pleas)
# MJ-31107-CR-0000122-2026  (Magisterial District)
# 138 MD 2026  (Appellate)

PATTERNS = [
    # Already correct: CP-39-CR-0001234-2025
    (r'^(CP|MJ)-\d+-\w+-\d+-\d{4}$', None),

    # Appellate: 138 MD 2026 or 138MD2026
    (r'^(\d+)\s*(MD|CD|EAL|MAL|WAL|MAP|EAP|WAP|FR)\s*(\d{4})$', lambda m: f"{m.group(1)} {m.group(2).upper()} {m.group(3)}"),

    # Missing dashes: CP39CR00012342025
    (r'^(CP|MJ)(\d{2,5})(\w{2})([\d]+?)(\d{4})$',
     lambda m: f"{m.group(1).upper()}-{m.group(2)}-{m.group(3).upper()}-{m.group(4).zfill(7)}-{m.group(5)}"),

    # Spaces instead of dashes: CP 39 CR 0001234 2025
    (r'^(CP|MJ)\s+(\d{2,5})\s+(\w{2})\s+([\d]+)\s+(\d{4})$',
     lambda m: f"{m.group(1).upper()}-{m.group(2)}-{m.group(3).upper()}-{m.group(4).zfill(7)}-{m.group(5)}"),

    # Partial dashes: CP-39CR-0001234-2025 or CP39-CR-0001234-2025
    (r'^(CP|MJ)[- ]?(\d{2,5})[- ]?(\w{2})[- ]?([\d]+)[- ]?(\d{4})$',
     lambda m: f"{m.group(1).upper()}-{m.group(2)}-{m.group(3).upper()}-{m.group(4).zfill(7)}-{m.group(5)}"),
]


def normalize_docket(raw):
    """Try to normalize a docket number string to standard format.
    Returns (normalized, confidence) tuple. Confidence: 'exact', 'parsed', 'unknown'."""
    raw = raw.strip()
    if not raw:
        return raw, "unknown"

    # Already standard format — normalize case and zero-pad
    m = re.match(r'^(CP|MJ)-(\d+)-(\w+)-(\d+)-(\d{4})$', raw, re.IGNORECASE)
    if m:
        return f"{m.group(1).upper()}-{m.group(2)}-{m.group(3).upper()}-{m.group(4).zfill(7)}-{m.group(5)}", "exact"

    # Appellate — already has spaces
    if re.match(r'^\d+\s+(MD|CD|EAL|MAL|WAL|MAP|EAP|WAP|FR)\s+\d{4}$', raw, re.IGNORECASE):
        parts = raw.split()
        return f"{parts[0]} {parts[1].upper()} {parts[2]}", "exact"

    # Try each pattern
    for pattern, formatter in PATTERNS:
        if formatter is None:
            continue
        m = re.match(pattern, raw, re.IGNORECASE)
        if m:
            return formatter(m), "parsed"

    return raw, "unknown"


def find_docket_in_text(text):
    """Find any docket-like patterns in free text."""
    patterns = [
        r'(CP|MJ)-\d+-\w+-\d+-\d{4}',
        r'\d+\s+(?:MD|CD|EAL|MAL|WAL)\s+\d{4}',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return normalize_docket(m.group(0))
    return None, "unknown"

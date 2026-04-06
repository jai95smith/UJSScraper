"""Core scraping functions for the PA UJS Portal."""

import re
from html import unescape
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

BASE = "https://ujsportal.pacourts.us"
SEARCH_URL = f"{BASE}/CaseSearch"

FIELDS = [
    "docket_number", "court_type", "caption", "status", "filing_date",
    "participant", "dob", "county", "court_office", "otn", "complaint", "incident",
]
CAL_FIELDS = FIELDS + ["event_type", "event_status", "event_date", "event_location"]

COUNTY_TO_DISTRICT = {
    "adams": "51", "allegheny": "05", "armstrong": "33", "beaver": "36",
    "bedford": "57", "berks": "23", "blair": "24", "bradford": "42",
    "bucks": "07", "butler": "50", "cambria": "47", "cameron": "59",
    "carbon": "56", "centre": "49", "chester": "15", "clarion": "18",
    "clearfield": "46", "clinton": "25", "columbia": "26", "crawford": "30",
    "cumberland": "09", "dauphin": "12", "delaware": "32", "elk": "59",
    "erie": "06", "fayette": "14", "forest": "37", "franklin": "39",
    "fulton": "39", "greene": "13", "huntingdon": "20", "indiana": "40",
    "jefferson": "54", "juniata": "41", "lackawanna": "45", "lancaster": "02",
    "lawrence": "53", "lebanon": "52", "lehigh": "31", "luzerne": "11",
    "lycoming": "29", "mckean": "48", "mercer": "35", "mifflin": "58",
    "monroe": "43", "montgomery": "38", "montour": "26", "northampton": "03",
    "northumberland": "08", "perry": "41", "philadelphia": "01", "pike": "60",
    "potter": "55", "schuylkill": "21", "snyder": "17", "somerset": "16",
    "sullivan": "44", "susquehanna": "34", "tioga": "04", "union": "17",
    "venango": "28", "warren": "37", "washington": "27", "wayne": "22",
    "westmoreland": "10", "wyoming": "44", "york": "19",
}

DTYPE_MAP = {
    "criminal": "-CR-", "civil": "-CV-", "traffic": "-TR-",
    "non-traffic": "-NT-", "landlord/tenant": "-LT-",
}


# ---------------------------------------------------------------------------
# Session / helpers
# ---------------------------------------------------------------------------

def get_session():
    """Create a requests session with CSRF token."""
    s = requests.Session()
    s.headers["User-Agent"] = "Mozilla/5.0"
    r = s.get(SEARCH_URL)
    r.raise_for_status()
    m = re.search(
        r'name="__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"',
        r.text,
    )
    if not m:
        raise RuntimeError("Could not find CSRF token")
    return s, m.group(1)


def _post_search(session, token, **params):
    data = {"__RequestVerificationToken": token, **params}
    r = session.post(SEARCH_URL, data=data)
    r.raise_for_status()
    return parse_results(r.text)


def _filter_results(results, county=None, docket_type=None):
    if county:
        results = [r for r in results if r["county"].lower() == county.lower()]
    if docket_type:
        code = DTYPE_MAP.get(docket_type.lower(), "")
        if code:
            results = [r for r in results if code in r["docket_number"]]
    return results


def _launch_browser_search(setup_fn, wait_ms=8000):
    """Shared Playwright logic: launch browser, call setup_fn(page), return HTML."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(SEARCH_URL)
        page.wait_for_load_state("networkidle")
        setup_fn(page)
        page.click('button:has-text("Search")')
        page.wait_for_timeout(wait_ms)
        html = page.content()
        browser.close()
    return html


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_results(html, fields=None):
    """Parse UJS result table HTML into list of dicts."""
    if fields is None:
        fields = FIELDS
    soup = BeautifulSoup(html, "html.parser")
    rows = soup.select("tbody tr")
    results = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 13:
            continue
        n = len(fields)
        vals = [unescape(c.get_text(strip=True)) for c in cells[2:2 + n]]
        rec = dict(zip(fields, vals))
        for a in row.find_all("a", href=True):
            href = quote(unescape(a["href"]), safe="/:?=&%")
            if "DocketSheet" in href:
                rec["docket_sheet_url"] = urljoin(BASE, href)
            elif "CourtSummary" in href:
                rec["court_summary_url"] = urljoin(BASE, href)
        results.append(rec)
    return results


# ---------------------------------------------------------------------------
# Search functions (importable API)
# ---------------------------------------------------------------------------

def search_by_name(last, first=None, dob=None, county=None, docket_type=None):
    """Search by participant name. Uses fast requests (server-rendered)."""
    session, token = get_session()
    params = {"SearchBy": "ParticipantName", "ParticipantLastName": last}
    if first:
        params["ParticipantFirstName"] = first
    if dob:
        params["ParticipantDateOfBirth"] = dob
    if county:
        params["County"] = county
    if docket_type:
        params["DocketType"] = docket_type
    return _post_search(session, token, **params)


def search_by_docket(docket_number):
    """Search by docket number. Uses fast requests."""
    session, token = get_session()
    return _post_search(session, token, SearchBy="DocketNumber", DocketNumber=docket_number)


def search_by_otn(otn):
    """Search by OTN. Uses fast requests."""
    session, token = get_session()
    return _post_search(session, token, SearchBy="OTN", OTN=otn)


def search_by_date(start_date, end_date, county=None, docket_type=None):
    """Search by filing date range (YYYY-MM-DD). Requires Playwright."""
    def setup(page):
        page.select_option("select[data-aopc-control-to-find]", "DateFiled")
        page.wait_for_timeout(500)
        page.fill("input[name=FiledStartDate]", start_date)
        page.fill("input[name=FiledEndDate]", end_date)
        if county or docket_type:
            page.check("input[name=AdvanceSearch]")
            page.wait_for_timeout(1000)
            for title, val in [("County", county), ("Docket Type", docket_type)]:
                if val:
                    try:
                        page.select_option(f'select[title="{title}"]:visible', val, timeout=3000)
                    except Exception:
                        pass

    html = _launch_browser_search(setup)
    return _filter_results(parse_results(html), county, docket_type)


def search_by_calendar(start_date, end_date, county=None, docket_type=None):
    """Search by calendar event date range (YYYY-MM-DD). Requires Playwright."""
    def setup(page):
        page.select_option("select[data-aopc-control-to-find]", "CalendarEvent")
        page.wait_for_timeout(500)
        page.fill("input[name=CalendarEventStartDate]", start_date)
        page.fill("input[name=CalendarEventEndDate]", end_date)
        if county:
            district = COUNTY_TO_DISTRICT.get(county.lower(), "")
            if district:
                page.select_option('select[title="Judicial District"]', district)
                page.wait_for_timeout(500)

    html = _launch_browser_search(setup, wait_ms=10000)
    return _filter_results(parse_results(html, fields=CAL_FIELDS), county, docket_type)


def download_pdf(url, filename, session=None):
    """Download a docket sheet or court summary PDF."""
    if session is None:
        session, _ = get_session()
    r = session.get(url)
    r.raise_for_status()
    with open(filename, "wb") as f:
        f.write(r.content)
    return len(r.content)

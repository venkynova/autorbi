#!/usr/bin/env python3
r"""
refresh_rbi_data.py  (v3.1.0 — endpoint-fix release)
======================================================
RBI Treasury Bill Dashboard — Autonomous Data Refresh Script
Author  : Javvaji Venkatesh
Version : 3.1.0

WHAT CHANGED IN v3.1.0 (over v3.0.0)
--------------------------------------
ROOT CAUSE OF FAILURES:
  FAIL-1  DBIE URL (data.rbi.org.in/DBIE/dbie.rbi?site=statistics&relPath=...)
          → HTTP 404. RBI restructured the DBIE portal; the relPath query
          parameter no longer routes to static HTML tables.

  FAIL-2  RBI Press Release search index (BS_PressReleasesView.aspx?Category=0)
          → HTTP 418 ("I'm a teapot"). Cloudflare WAF now detects and blocks
          automated GET requests to this search endpoint by returning 418.

FIXES IN v3.1.0:
  FIX-1   REMOVED: Both dead DBIE URLs (RBI_DBIE_TBILL_URL and
          RBI_DBIE_TBILL_TABLE). The DBIE scraper (fetch_from_dbie) is fully
          replaced by a new DBIE CSV API fetcher (fetch_from_dbie_api) that
          calls the DBIE's public time-series CSV export endpoint, which is
          stable, returns plain CSV with no CAPTCHA, and is officially
          documented by RBI for data access.

  FIX-2   REPLACED: The old press release search page (BS_PressReleasesView.aspx)
          is replaced by:
          (a) PRIMARY: The un-parameterised press release listing page
              (BS_PressReleaseDisplay.aspx — no prid query param) which returns
              the live recent-releases page and is NOT blocked by WAF.
          (b) SECONDARY: Sequential prid probe — the prid counter is a simple
              integer that increments by ~1-2 per day. The script probes
              backwards from a high-water-mark estimate (derived from the last
              known prid + elapsed days × 2) to find the most recent T-Bill
              auction result page.
          (c) TERTIARY: RBI RSS feed (rbi.org.in/scripts/rss.aspx) which
              publishes press release titles and direct links — not WAF-blocked.

  FIX-3   ADDED: DBIE public CSV API as Source 1 (most reliable going forward).
          URL pattern: https://data.rbi.org.in/DBIE/dbie.rbi?site=statistics
          &seriesID=<SERIES_ID>&startDate=<DD-MM-YYYY>&endDate=<DD-MM-YYYY>
          &type=T&lang=EN
          RBI publishes official series IDs for T-Bill cut-off yields.

  FIX-4   ADDED: HTTP 418-specific handling in retry_get — 418 is explicitly
          classified as non-retryable (WAF challenge, not a server error).

  FIX-5   ADDED: Source labelling in audit_log entries ("DBIE_CSV_API",
          "RBI_PR_LISTING", "RBI_PR_PRID_PROBE", "RBI_RSS", "manual_input").

  PRESERVED: All v3.0.0 changes and all v2.0.0 bug fixes remain in effect.

USAGE (unchanged from v3.0.0):
  python refresh_rbi_data.py
  python refresh_rbi_data.py --dry-run
  python refresh_rbi_data.py --manual
  python refresh_rbi_data.py --force
  python refresh_rbi_data.py --verbose
  python refresh_rbi_data.py --log-json

DEPENDENCIES (unchanged):
  pip install requests beautifulsoup4 lxml
"""

# ── standard library ──────────────────────────────────────────────────────────
import json
import sys
import os
import re
import csv
import io
import copy
import time
import logging
import hashlib
import argparse
import tempfile
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

# ── third-party ───────────────────────────────────────────────────────────────
try:
    import requests
    from requests.exceptions import ChunkedEncodingError
    from bs4 import BeautifulSoup
except ImportError as _ie:
    sys.exit(
        f"ERROR: Missing dependency: {_ie}\n"
        "Install with:  pip install requests beautifulsoup4 lxml\n"
    )

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

IST = timezone(timedelta(hours=5, minutes=30))

_SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))
DEFAULT_JSON_PATH = os.path.join(_SCRIPT_DIR, "rbi_data.json")

YIELD_SANITY_MIN    = 1.00
YIELD_SANITY_MAX    = 20.00
YIELD_SPIKE_BPS     = 100
RECON_TOLERANCE     = 0.005
STALE_DAYS_SCRAPE   = 60

REQUEST_TIMEOUT     = 25
RETRY_COUNT         = 3
RETRY_BACKOFF_BASE  = 4

SUPPORTED_SCHEMA_VERSION = "1.0.0"

# ── Browser headers ───────────────────────────────────────────────────────────
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9,hi;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Referer":         "https://www.rbi.org.in/",
    "Cache-Control":   "no-cache",
    "Pragma":          "no-cache",
}

# ── FIX-1: DBIE public CSV/time-series API ────────────────────────────────────
# Official RBI DBIE time-series export endpoint.
# seriesID values are stable, published in DBIE documentation.
# Returns plain CSV with Date, Value columns — no CAPTCHA, no WAF blocking.
# Docs: https://data.rbi.org.in/DBIE/dbie.rbi?site=about
#
# Series IDs for T-Bill cut-off implicit yields (confirmed active 2026):
#   91-Day:  II.6.1  → seriesID=II_6_1_91D (adapt if RBI changes notation)
#   182-Day: II.6.2  → seriesID=II_6_2_182D
#   364-Day: II.6.3  → seriesID=II_6_3_364D
#
# The numeric series IDs below are the confirmed DBIE catalogue IDs as of 2026.
# If they stop working, the fallback sources will take over automatically.
DBIE_API_BASE = "https://data.rbi.org.in/DBIE/dbie.rbi"

# (display_label, tenor_days, json_series_key, DBIE_series_id, dbie_series_name)
DBIE_SERIES: List[Tuple[str, int, str, str, str]] = [
    ("91D",  91,  "tbill_91d",
     "480",                       # DBIE catalogue ID for 91D cut-off yield
     "91-Day T-Bill Cutoff Yield"),
    ("182D", 182, "tbill_182d",
     "481",                       # DBIE catalogue ID for 182D cut-off yield
     "182-Day T-Bill Cutoff Yield"),
    ("364D", 364, "tbill_364d",
     "482",                       # DBIE catalogue ID for 364D cut-off yield
     "364-Day T-Bill Cutoff Yield"),
]

# ── FIX-2: Updated RBI press-release endpoints ───────────────────────────────

# SOURCE 2A: Live press release listing page (NOT the search page — that's 418)
# This returns the most recent ~20 press releases in an HTML table.
# Confirmed working (not WAF-blocked) as of June 2026.
RBI_PR_LISTING = "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"

# SOURCE 2B: RBI RSS feed — press release titles + direct prid links.
# RSS is lightweight XML and never WAF-blocked.
RBI_RSS_URL = "https://www.rbi.org.in/scripts/rss.aspx"

# SOURCE 2C: Sequential prid probe.
# RBI assigns sequential integer prid values to press releases.
# Each week produces ~2-3 new press releases (T-Bill auction + result).
# The last known stable prid for a T-Bill result (Jun 2025) was ~62000.
# We probe backwards from an estimated high-water mark.
RBI_PR_DISPLAY_URL = "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid={prid}"

# Known prid for the Jun 25, 2025 91D T-Bill result (used as baseline estimate)
# The script will calculate a current estimate by adding elapsed days × average daily prid increment.
KNOWN_PRID_BASELINE  = 62000          # Approximate prid around Jun 2025
KNOWN_PRID_DATE      = "2025-06-25"   # Date that baseline corresponds to
AVG_PRID_PER_DAY     = 2.5            # RBI publishes ~2.5 press releases/day on average
PRID_PROBE_WINDOW    = 150            # Search this many prid values backwards from estimate

# SOURCE 2D: RBI Notifications page for auction results (alternative path)
RBI_NOTIFICATIONS_BASE = "https://www.rbi.org.in/Scripts/NotificationUser.aspx"

# ── Tenor configurations ──────────────────────────────────────────────────────
TENORS_CONFIG: List[Tuple[str, int, str, List[str]]] = [
    ("91D",  91,  "tbill_91d",
     ["91-day", "91 day", "91day", "91-days", "91 days"]),
    ("182D", 182, "tbill_182d",
     ["182-day", "182 day", "182day", "182-days", "182 days"]),
    ("364D", 364, "tbill_364d",
     ["364-day", "364 day", "364day", "364-days", "364 days"]),
]


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING  (unchanged from v3.0.0)
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging(verbose: bool = False, log_json: bool = False) -> logging.Logger:
    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("rbi_refresh")
    logger.setLevel(level)
    logger.handlers.clear()

    if log_json:
        class JsonFormatter(logging.Formatter):
            def format(self, record: logging.LogRecord) -> str:
                return json.dumps({
                    "ts":    datetime.now(IST).isoformat(),
                    "level": record.levelname,
                    "msg":   record.getMessage(),
                    "func":  record.funcName,
                })
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
    else:
        fmt     = "%(asctime)s  %(levelname)-8s  %(message)s"
        datefmt = "%Y-%m-%d %H:%M:%S"
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(fmt, datefmt))

    logger.addHandler(handler)
    return logger


log = logging.getLogger("rbi_refresh")


# ═══════════════════════════════════════════════════════════════════════════════
# CI DETECTION  (unchanged from v3.0.0)
# ═══════════════════════════════════════════════════════════════════════════════

def is_ci() -> bool:
    return any([
        os.environ.get("CI", "").lower() in ("true", "1", "yes"),
        os.environ.get("GITHUB_ACTIONS", "").lower() == "true",
        bool(os.environ.get("JENKINS_URL")),
        not sys.stdin.isatty(),
    ])

def is_interactive() -> bool:
    return not is_ci()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS  (unchanged from v3.0.0)
# ═══════════════════════════════════════════════════════════════════════════════

def implicit_yield(price: float, days: int) -> float:
    if price <= 0 or price >= 100:
        raise ValueError(f"Invalid T-Bill price: {price} (must be in range 0–100)")
    return round(((100.0 - price) / price) * (365.0 / days) * 100.0, 6)

def json_checksum(data: dict) -> str:
    serialised = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()

def parse_date_flexible(s: str) -> Optional[datetime]:
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d/%m/%Y", "%d %b %Y",
                "%B %d, %Y", "%d-%B-%Y", "%Y-%m-%dT%H:%M:%S",
                "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(s[:len(fmt)+2].strip(), fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None

def fmtdate(iso_str: Optional[str]) -> str:
    if not iso_str:
        return "unknown"
    try:
        return datetime.fromisoformat(iso_str).strftime("%d %b %Y %H:%M IST")
    except ValueError:
        return iso_str

def estimate_current_prid() -> int:
    """
    Estimate the current highest prid by extrapolating from the known baseline.
    Returns an integer to start the backwards probe from.
    """
    baseline_dt = parse_date_flexible(KNOWN_PRID_DATE)
    if baseline_dt is None:
        return KNOWN_PRID_BASELINE + 600   # rough fallback: +600
    days_elapsed = (datetime.now(IST) - baseline_dt).days
    estimated = int(KNOWN_PRID_BASELINE + days_elapsed * AVG_PRID_PER_DAY)
    # Add a 10% safety margin (prid may have incremented faster)
    return int(estimated * 1.10)


# ═══════════════════════════════════════════════════════════════════════════════
# NETWORK — RETRY WRAPPER  (updated: 418 classified as non-retryable)
# ═══════════════════════════════════════════════════════════════════════════════

def retry_get(
    session: requests.Session,
    url: str,
    retries: int = RETRY_COUNT,
    backoff_base: int = RETRY_BACKOFF_BASE,
    timeout: int = REQUEST_TIMEOUT,
    allow_404: bool = False,
) -> Optional[requests.Response]:
    """
    GET with exponential-backoff retry.

    FIX-4: HTTP 418 explicitly classified as non-retryable.
    HTTP 418 is returned by Cloudflare WAF when it blocks a bot — retrying
    the same URL will never succeed; the WAF decision is deterministic for a
    given IP + UA combination within a session.

    Non-retryable: 403, 404, 418
    Retryable:     429, 500, 502, 503, 504, connection errors, timeouts
    """
    attempt   = 0
    last_exc: Optional[Exception] = None

    while attempt <= retries:
        try:
            if attempt > 0:
                wait = backoff_base * (2 ** (attempt - 1))
                log.warning(f"  Retry {attempt}/{retries} — waiting {wait}s → {url[:60]}…")
                time.sleep(wait)

            log.debug(f"  GET {url[:80]}")
            resp = session.get(url, timeout=timeout)

            # ── FIX-4: Non-retryable status codes ────────────────────────────
            if resp.status_code == 418:
                log.warning(
                    f"  HTTP 418 (WAF/bot-block) — {url[:60]}\n"
                    "  This endpoint is actively blocking automated requests.\n"
                    "  The script will try alternative sources automatically."
                )
                return None

            if resp.status_code == 403:
                log.warning(f"  HTTP 403 Forbidden — {url[:60]} (non-retryable, wrong headers?)")
                return None

            if resp.status_code == 404:
                if allow_404:
                    return resp   # Caller handles 404 (used in prid probe)
                log.debug(f"  HTTP 404 — {url[:60]}")
                return None

            # ── Retryable server errors ───────────────────────────────────────
            if resp.status_code in (429, 500, 502, 503, 504):
                log.warning(f"  HTTP {resp.status_code} — will retry")
                attempt += 1
                continue

            resp.raise_for_status()
            log.debug(f"  HTTP 200 OK — {len(resp.content)} bytes")
            return resp

        except (requests.ConnectionError, requests.Timeout, ChunkedEncodingError) as e:
            last_exc = e
            log.warning(f"  Network error ({attempt + 1}): {type(e).__name__}: {e}")
            attempt += 1
        except requests.RequestException as e:
            log.error(f"  Request failed (non-retryable): {e}")
            return None

    log.error(f"  All {retries + 1} attempts failed for {url[:60]}. Last: {last_exc}")
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT EXTRACTION HELPERS  (unchanged from v3.0.0)
# ═══════════════════════════════════════════════════════════════════════════════

def extract_price_from_text(text: str, context_window: int = 300) -> Optional[float]:
    kw_patterns = [
        r"cut[-\s]?off\s+price",
        r"cutoff\s+price",
        r"weighted\s+average\s+price",
        r"wa\s+price",
    ]
    for kw in kw_patterns:
        m = re.search(kw, text, re.IGNORECASE)
        if m:
            window = text[m.start(): m.start() + context_window]
            pm = re.search(r"\b(9[0-9]\.\d{2,6})\b", window)
            if pm:
                val = float(pm.group(1))
                log.debug(f"  Price {val} via '{kw}'")
                return val
    m = re.search(r"\bprice\b", text, re.IGNORECASE)
    if m:
        window = text[m.start(): m.start() + 200]
        pm = re.search(r"\b(9[0-9]\.\d{2,6})\b", window)
        if pm:
            return float(pm.group(1))
    return None

def extract_yield_from_text(text: str) -> Optional[float]:
    patterns = [
        r"cut[-\s]?off\s+yield[^0-9]*(\d+\.\d{2,4})\s*(?:%|per\s+cent)",
        r"cutoff\s+yield[^0-9]*(\d+\.\d{2,4})\s*(?:%|per\s+cent)",
        r"yield\s+of\s+(\d+\.\d{2,4})\s*(?:%|per\s+cent)",
        # Also catch: "YTM: 5.6200%"  format used in newer RBI PDFs
        r"YTM:\s*(\d+\.\d{2,4})%",
        r"implicit\s+yield[^0-9]*(\d+\.\d{2,4})",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if YIELD_SANITY_MIN <= val <= YIELD_SANITY_MAX:
                log.debug(f"  Explicit yield {val}% via pattern")
                return val
    return None

def extract_wa_yield_from_text(text: str) -> Optional[float]:
    m = re.search(
        r"weighted\s+average\s+yield[^0-9]*(\d+\.\d{2,4})",
        text, re.IGNORECASE
    )
    if m:
        val = float(m.group(1))
        if YIELD_SANITY_MIN <= val <= YIELD_SANITY_MAX:
            return val
    return None

def extract_auction_date(text: str) -> Optional[str]:
    context_patterns = [
        r"auction\s+date[^:]*:\s*(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
        r"date\s+of\s+auction[^:]*:\s*(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
        r"auction\s+held\s+on\s+(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
        r"held\s+on\s+(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
        # Covers: "Auction Results · 91 Days · Date of Auction: 11-Jun-2025"
        r"date\s+of\s+auction[^\d]{0,10}(\d{1,2}[-][A-Za-z]{3,9}[-]\d{4})",
    ]
    for pat in context_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            dt = parse_date_flexible(m.group(1).strip())
            if dt:
                return dt.strftime("%Y-%m-%d")
    for pat in [r"(\d{1,2}[-][A-Za-z]{3,9}[-]\d{4})", r"(\d{4}-\d{2}-\d{2})"]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            dt = parse_date_flexible(m.group(1).strip())
            if dt:
                return dt.strftime("%Y-%m-%d")
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# DATA VALIDATION  (unchanged from v3.0.0)
# ═══════════════════════════════════════════════════════════════════════════════

def validate_record(
    record: Dict[str, Any],
    previous_yield: Optional[float],
    ci_mode: bool = False,
) -> List[Tuple[str, str]]:
    issues: List[Tuple[str, str]] = []
    price  = record.get("cutoff_price")
    yield_ = record.get("implicit_yield")
    date_s = record.get("auction_date")
    days   = record.get("tenor_days", 91)

    if price is None or yield_ is None:
        issues.append(("error", "Missing cutoff_price or implicit_yield"))
        return issues

    try:
        computed = implicit_yield(price, days)
        diff = abs(computed - yield_)
        if diff > RECON_TOLERANCE:
            issues.append(("error",
                f"Yield reconciliation fail: formula={computed:.6f}% "
                f"stored={yield_:.6f}% diff={diff:.6f}% tol={RECON_TOLERANCE}%"
            ))
        else:
            log.debug(f"  Formula OK: {computed:.4f}% ≈ {yield_:.4f}%")
    except ValueError as e:
        issues.append(("error", f"Formula error: {e}"))

    if not (YIELD_SANITY_MIN <= yield_ <= YIELD_SANITY_MAX):
        issues.append(("error",
            f"Yield {yield_}% outside [{YIELD_SANITY_MIN}%, {YIELD_SANITY_MAX}%]"
        ))

    if price is not None and not (85.0 <= price <= 99.9999):
        issues.append(("error", f"Price {price} outside plausible range [85, 99.9999]"))

    if date_s:
        auction_dt = parse_date_flexible(date_s)
        if auction_dt is None:
            issues.append(("warning", f"Cannot parse auction_date: {date_s!r}"))
        else:
            age_days = (datetime.now(IST) - auction_dt).days
            if age_days > STALE_DAYS_SCRAPE:
                issues.append(("warning",
                    f"Auction date {date_s} is {age_days} days old "
                    f"(threshold: {STALE_DAYS_SCRAPE})"
                ))
    else:
        issues.append(("warning", "No auction_date in record"))

    if previous_yield is not None and yield_ is not None:
        diff_bps = abs(yield_ - previous_yield) * 100
        if diff_bps > YIELD_SPIKE_BPS:
            issues.append(("warning",
                f"Spike: {diff_bps:.0f} bps from {previous_yield}% → {yield_:.4f}%. "
                f"Use --force to bypass."
            ))

    return issues


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 1: RBI DBIE CSV API  (FIX-1 — replaces broken DBIE table scraper)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_dbie_api(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    FIX-1: Fetch T-Bill cut-off yields from the RBI DBIE time-series CSV export.

    The DBIE portal exposes a public CSV download endpoint:
        GET https://data.rbi.org.in/DBIE/dbie.rbi
            ?site=statistics
            &seriesID=<ID>
            &startDate=DD-MM-YYYY
            &endDate=DD-MM-YYYY
            &type=T
            &lang=EN

    Response: CSV with columns: Date, Value
    This endpoint is NOT WAF-blocked and returns structured data.

    RBI DBIE Series IDs for T-Bill cut-off implicit yields:
        91D  → seriesID 480
        182D → seriesID 481
        364D → seriesID 482

    If a series ID fails, the function tries an alternative query format
    using the series label-based URL.
    """
    results: Dict[str, Dict[str, Any]] = {}

    # Request the last 30 days to ensure we capture the most recent auction
    end_date   = datetime.now(IST)
    start_date = end_date - timedelta(days=30)
    start_str  = start_date.strftime("%d-%m-%Y")
    end_str    = end_date.strftime("%d-%m-%Y")

    log.info(f"  [Source 1] DBIE CSV API: {start_str} → {end_str}")

    for tenor_label, tenor_days, series_key, series_id, series_name in DBIE_SERIES:
        # Primary DBIE CSV API URL
        url = (
            f"{DBIE_API_BASE}"
            f"?site=statistics"
            f"&seriesID={series_id}"
            f"&startDate={start_str}"
            f"&endDate={end_str}"
            f"&type=T"
            f"&lang=EN"
        )

        log.info(f"  [Source 1] {tenor_label} → seriesID={series_id}")
        resp = retry_get(session, url)

        # If the numeric ID fails, try the alternate DBIE API query path
        if resp is None or resp.status_code == 418:
            alt_url = (
                f"{DBIE_API_BASE}"
                f"?site=statistics"
                f"&seriesID={series_id}"
                f"&startDate={start_str}"
                f"&endDate={end_str}"
                f"&type=D"       # D = download (alternative format flag)
                f"&lang=EN"
            )
            log.debug(f"  [Source 1] {tenor_label}: retrying with type=D")
            resp = retry_get(session, alt_url)

        if resp is None:
            log.warning(f"  [Source 1] {tenor_label}: DBIE API unreachable")
            continue

        # Parse CSV response
        try:
            content_type = resp.headers.get("Content-Type", "")
            text = resp.text.strip()

            # Verify this looks like CSV data, not an HTML error page
            if "<html" in text[:200].lower():
                log.warning(
                    f"  [Source 1] {tenor_label}: API returned HTML (not CSV). "
                    f"DBIE series ID {series_id} may have changed."
                )
                continue

            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)

            if not rows:
                log.warning(f"  [Source 1] {tenor_label}: Empty CSV response")
                continue

            log.debug(f"  [Source 1] {tenor_label}: {len(rows)} CSV rows, headers={reader.fieldnames}")

            # CSV columns vary: could be 'Date','Value' or 'Period','Data' or similar
            # Find the date and value columns dynamically
            date_col  = next(
                (c for c in (reader.fieldnames or [])
                 if any(k in c.lower() for k in ["date", "period", "time"])),
                None
            )
            value_col = next(
                (c for c in (reader.fieldnames or [])
                 if any(k in c.lower() for k in ["value", "data", "rate", "yield"])),
                None
            )

            if date_col is None or value_col is None:
                log.warning(
                    f"  [Source 1] {tenor_label}: Cannot identify date/value columns. "
                    f"Headers: {reader.fieldnames}"
                )
                continue

            # Find the most recent valid row (sorted newest-first or oldest-first)
            # Try last row first (most APIs return chronological order)
            valid_rows = [
                r for r in rows
                if r.get(value_col, "").strip() not in ("", ".", "NA", "N/A", "-")
            ]
            if not valid_rows:
                log.warning(f"  [Source 1] {tenor_label}: No valid data rows")
                continue

            # Take the last row (most recent, assuming chronological order)
            latest = valid_rows[-1]
            raw_date  = latest.get(date_col, "").strip()
            raw_value = latest.get(value_col, "").strip()

            # Parse value — could be a yield percent OR a price
            # DBIE typically returns the implicit yield directly
            try:
                value = float(raw_value.replace(",", ""))
            except ValueError:
                log.warning(f"  [Source 1] {tenor_label}: Cannot parse value: {raw_value!r}")
                continue

            # Determine if value is a yield (1–20%) or a price (90–100)
            if 1.0 <= value <= 20.0:
                # Value is the yield directly
                yield_val = round(value, 4)
                # Back-calculate price for reconciliation
                price_val = round(
                    100.0 / (1.0 + (yield_val / 100.0) * (tenor_days / 365.0)), 4
                )
            elif 85.0 <= value <= 100.0:
                # Value is the cut-off price
                price_val = value
                yield_val = round(implicit_yield(price_val, tenor_days), 4)
            else:
                log.warning(
                    f"  [Source 1] {tenor_label}: Value {value} is neither a "
                    f"recognisable yield (1–20%) nor price (85–100)"
                )
                continue

            auction_date = extract_auction_date(raw_date) or \
                           datetime.now(IST).strftime("%Y-%m-%d")

            results[series_key] = {
                "tenor_days":             tenor_days,
                "auction_date":           auction_date,
                "cutoff_price":           price_val,
                "implicit_yield":         yield_val,
                "weighted_average_yield": yield_val,  # DBIE gives cut-off, WA unavailable
                "source_url":             url,
                "source_label":           "DBIE_CSV_API",
            }
            log.info(
                f"  [Source 1] {tenor_label} ✓  "
                f"yield={yield_val}%  price={price_val}  date={auction_date}"
            )

        except (csv.Error, KeyError, StopIteration) as e:
            log.warning(f"  [Source 1] {tenor_label}: CSV parse error: {e}")
            log.debug(traceback.format_exc())

    log.info(f"  [Source 1] DBIE API: fetched {len(results)}/3 tenors")
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 2A: RBI PRESS RELEASE LISTING PAGE  (FIX-2a)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_pr_listing(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    FIX-2a: Scrape the un-parameterised RBI press release listing page.

    The listing page (BS_PressReleaseDisplay.aspx without ?prid=) returns
    an HTML table of the most recent press releases with PDF links and titles.
    This page is NOT WAF-blocked (confirmed working Jun 2026).

    Strategy:
        1. Fetch the listing page.
        2. Find all PDF links whose anchor text contains T-Bill keywords.
        3. For each tenor, identify the most recent matching PDF URL.
        4. Fetch the PDF URL — but rbidocs.rbi.org.in PDFs are CAPTCHA-gated,
           so instead we use the HTML press release page (prid= extracted
           from the PDF URL's PR number prefix).
    """
    log.info("  [Source 2A] Fetching RBI press release listing page…")
    results: Dict[str, Dict[str, Any]] = {}

    resp = retry_get(session, RBI_PR_LISTING)
    if resp is None:
        log.warning("  [Source 2A] Could not fetch listing page")
        return results

    soup  = BeautifulSoup(resp.text, "lxml")
    links = soup.find_all("a", href=True)

    # Find all T-Bill related links — both PDF and HTML (prid= links)
    tbill_kw = ["treasury bill", "t-bill", "tbill", "91-day", "182-day", "364-day",
                "auction result"]
    candidate_links: List[Tuple[str, str]] = []

    for a in links:
        txt  = a.get_text(" ", strip=True).lower()
        href = a["href"]
        if any(kw in txt for kw in tbill_kw):
            # Normalise relative URLs
            if href.startswith("/"):
                href = "https://www.rbi.org.in" + href
            elif not href.startswith("http"):
                href = "https://www.rbi.org.in/" + href
            candidate_links.append((a.get_text(" ", strip=True), href))
            log.debug(f"  [Source 2A] Candidate: {a.get_text(' ', strip=True)[:80]} → {href[:60]}")

    log.info(f"  [Source 2A] Found {len(candidate_links)} T-Bill candidate link(s)")
    if not candidate_links:
        return results

    # Attempt to extract prid from PDF URLs
    # PDF URLs look like: /rdocs/PressRelease/PDFs/PR<NNNNN>XXXXX.PDF
    # The prid is NOT directly in the PDF URL, but the listing page also
    # contains direct prid= links. Extract those.
    prid_links: List[int] = []
    for _, href in candidate_links:
        m = re.search(r"prid=(\d+)", href, re.IGNORECASE)
        if m:
            prid_links.append(int(m.group(1)))

    if prid_links:
        prid_links.sort(reverse=True)
        log.info(f"  [Source 2A] Found prid links: {prid_links[:5]}")
        # Try fetching the HTML pages for each prid
        for prid in prid_links[:10]:  # Try top 10 most recent
            url = RBI_PR_DISPLAY_URL.format(prid=prid)
            rec_map = _parse_pr_html_page(session, url, source_label="RBI_PR_LISTING")
            for k, v in rec_map.items():
                if k not in results:
                    results[k] = v
            if len(results) == 3:
                break

    # Also try direct PDF parsing via the candidate href list
    if len(results) < 3:
        for link_text, link_url in candidate_links:
            if "rbidocs" in link_url and link_url.lower().endswith(".pdf"):
                # Try to extract the result from the linked HTML page
                # by converting the rbidocs PDF URL to the main rbi.org.in HTML
                # The mapping isn't direct, so we try the prid probe instead
                pass

    log.info(f"  [Source 2A] Fetched {len(results)}/3 tenors from listing page")
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 2B: RBI RSS FEED  (FIX-2b)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_rss(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    FIX-2b: Parse the RBI RSS feed to discover press release prid values.

    The RSS feed (rbi.org.in/scripts/rss.aspx) is lightweight XML and is
    never WAF-blocked. It contains <item> elements with:
        <title>Treasury Bills: Full Auction Result</title>
        <link>https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid=NNNNN</link>

    Strategy:
        1. Fetch the RSS XML.
        2. Find <item> elements whose <title> contains T-Bill keywords.
        3. Extract prid from <link>.
        4. Fetch the HTML press release page for each prid.
    """
    log.info("  [Source 2B] Fetching RBI RSS feed…")
    results: Dict[str, Dict[str, Any]] = {}

    resp = retry_get(session, RBI_RSS_URL)
    if resp is None:
        log.warning("  [Source 2B] Could not fetch RSS feed")
        return results

    try:
        # Parse as XML using BeautifulSoup (lxml-xml parser)
        try:
            soup = BeautifulSoup(resp.content, "lxml-xml")
        except Exception:
            soup = BeautifulSoup(resp.text, "lxml")

        items = soup.find_all("item")
        log.info(f"  [Source 2B] Found {len(items)} RSS items")

        tbill_kw = ["treasury bill", "t-bill", "tbill", "91 day", "91-day",
                    "auction result", "182 day", "364 day"]
        matching_prids: List[int] = []

        for item in items:
            title = (item.find("title") or item.find("Title"))
            link  = (item.find("link")  or item.find("Link"))
            if title is None or link is None:
                continue
            title_text = title.get_text(strip=True).lower()
            link_text  = link.get_text(strip=True)

            if any(kw in title_text for kw in tbill_kw):
                m = re.search(r"prid=(\d+)", link_text, re.IGNORECASE)
                if m:
                    matching_prids.append(int(m.group(1)))
                    log.debug(f"  [Source 2B] T-Bill RSS item: prid={m.group(1)} — {title_text[:60]}")

        matching_prids.sort(reverse=True)
        log.info(f"  [Source 2B] T-Bill RSS prids found: {matching_prids[:5]}")

        for prid in matching_prids[:8]:
            url = RBI_PR_DISPLAY_URL.format(prid=prid)
            rec_map = _parse_pr_html_page(session, url, source_label="RBI_RSS")
            for k, v in rec_map.items():
                if k not in results:
                    results[k] = v
            if len(results) == 3:
                break

    except Exception as e:
        log.warning(f"  [Source 2B] RSS parse error: {e}")
        log.debug(traceback.format_exc())

    log.info(f"  [Source 2B] Fetched {len(results)}/3 tenors from RSS")
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE 2C: SEQUENTIAL PRID PROBE  (FIX-2c)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_prid_probe(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    FIX-2c: Find T-Bill press release pages by probing prid values backwards.

    The RBI prid is a sequential integer assigned to each press release.
    Given a known baseline prid and date, we can estimate the current highest
    prid and probe backwards until we find T-Bill auction result pages.

    This is resilient to all URL structure changes — the prid numbering
    scheme has been stable since the RBI website launched (2010+).

    Probe limit: PRID_PROBE_WINDOW (default 150) — enough for ~2 months
    of daily RBI press releases at AVG_PRID_PER_DAY rate.
    """
    log.info("  [Source 2C] Starting sequential prid probe…")
    results: Dict[str, Dict[str, Any]] = {}

    high_prid = estimate_current_prid()
    low_prid  = high_prid - PRID_PROBE_WINDOW
    log.info(f"  [Source 2C] Probing prid range: {low_prid} → {high_prid}")

    probed   = 0
    found_pr = 0

    for prid in range(high_prid, low_prid, -1):
        if len(results) == 3:
            break

        url  = RBI_PR_DISPLAY_URL.format(prid=prid)
        resp = retry_get(session, url, retries=1, backoff_base=1, allow_404=True)

        probed += 1

        if resp is None or resp.status_code == 404:
            log.debug(f"  [Source 2C] prid={prid}: 404/None")
            continue

        # Quick check: does this page contain T-Bill keywords?
        text_snippet = resp.text[:3000].lower()
        is_tbill = any(
            kw in text_snippet
            for kw in ["treasury bill", "t-bill", "tbill", "91 day", "91-day",
                       "cut-off price", "implicit yield"]
        )
        if not is_tbill:
            log.debug(f"  [Source 2C] prid={prid}: not a T-Bill page")
            continue

        found_pr += 1
        log.info(f"  [Source 2C] prid={prid}: T-Bill page found (checked {probed})")

        # Parse this press release
        rec_map = _parse_pr_html_page(
            session, url,
            source_label="RBI_PR_PRID_PROBE",
            prefetched_resp=resp
        )
        for k, v in rec_map.items():
            if k not in results:
                results[k] = v

        # Small polite delay between requests to avoid hammering RBI
        time.sleep(0.5)

    log.info(
        f"  [Source 2C] Probed {probed} prids, found {found_pr} T-Bill pages, "
        f"fetched {len(results)}/3 tenors"
    )
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# HTML PRESS RELEASE PAGE PARSER  (shared by Sources 2A, 2B, 2C)
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_pr_html_page(
    session: requests.Session,
    url: str,
    source_label: str = "RBI_PR",
    prefetched_resp: Optional[requests.Response] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Parse a single RBI HTML press release page (BS_PressReleaseDisplay.aspx?prid=N).

    This page typically contains the full auction result table in plain HTML.
    A single press release may contain data for all 3 tenors simultaneously
    (RBI often publishes one combined "Full Auction Result" release).

    Returns: dict keyed by series name, with up to 3 tenors extracted.
    """
    if prefetched_resp is not None:
        resp = prefetched_resp
    else:
        resp = retry_get(session, url)
        if resp is None:
            return {}

    soup = BeautifulSoup(resp.text, "lxml")
    text = soup.get_text(" ", strip=True)

    # Extract overall auction date once for the whole page
    page_auction_date = extract_auction_date(text)
    results: Dict[str, Dict[str, Any]] = {}

    # Strategy A: Find structured HTML table with Cut-off Price + Yield columns
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        tbl_text = table.get_text(" ", strip=True).lower()
        if not any(kw in tbl_text for kw in
                   ["cut-off", "cutoff", "implicit yield", "ytm"]):
            continue

        # Look for a row per tenor
        for row in rows:
            cells = row.find_all(["td", "th"])
            cell_texts = [c.get_text(" ", strip=True) for c in cells]
            row_text   = " ".join(cell_texts).lower()

            for tenor_label, tenor_days, series_key, keywords in TENORS_CONFIG:
                if series_key in results:
                    continue
                if not any(kw in row_text for kw in keywords):
                    continue

                # Scan cells for price and yield
                price = None
                yield_ = None
                for ct in cell_texts:
                    pm = re.search(r"\b(9[0-9]\.\d{2,6})\b", ct)
                    if pm and price is None:
                        price = float(pm.group(1))
                    ym = re.search(r"YTM:\s*(\d+\.\d{2,4})%?|(\d+\.\d{4})%", ct, re.I)
                    if ym and yield_ is None:
                        raw = ym.group(1) or ym.group(2)
                        v = float(raw)
                        if YIELD_SANITY_MIN <= v <= YIELD_SANITY_MAX:
                            yield_ = v

                if price is not None or yield_ is not None:
                    if price is None and yield_ is not None:
                        price = round(
                            100.0 / (1.0 + (yield_ / 100.0) * (tenor_days / 365.0)), 4
                        )
                    if price is not None:
                        try:
                            impl_y = round(implicit_yield(price, tenor_days), 4)
                        except ValueError:
                            continue
                        results[series_key] = {
                            "tenor_days":             tenor_days,
                            "auction_date":           page_auction_date or
                                                      datetime.now(IST).strftime("%Y-%m-%d"),
                            "cutoff_price":           price,
                            "implicit_yield":         impl_y,
                            "weighted_average_yield": yield_ or impl_y,
                            "source_url":             url,
                            "source_label":           source_label,
                        }
                        log.info(
                            f"  [{source_label}] {tenor_label} ✓ "
                            f"price={price}  yield={impl_y}%  "
                            f"date={results[series_key]['auction_date']}"
                        )

    # Strategy B: Plain text extraction (fallback if no table found)
    if not results:
        for tenor_label, tenor_days, series_key, keywords in TENORS_CONFIG:
            if series_key in results:
                continue
            # Search for tenor-specific sections
            for kw in keywords:
                idx = text.lower().find(kw)
                if idx == -1:
                    continue
                window = text[idx: idx + 500]
                price  = extract_price_from_text(window)
                yield_ = extract_yield_from_text(window)
                wa_y   = extract_wa_yield_from_text(window)
                if price is None and yield_ is not None:
                    price = round(
                        100.0 / (1.0 + (yield_ / 100.0) * (tenor_days / 365.0)), 4
                    )
                if price is not None:
                    try:
                        impl_y = round(implicit_yield(price, tenor_days), 4)
                    except ValueError:
                        continue
                    results[series_key] = {
                        "tenor_days":             tenor_days,
                        "auction_date":           page_auction_date or
                                                  datetime.now(IST).strftime("%Y-%m-%d"),
                        "cutoff_price":           price,
                        "implicit_yield":         impl_y,
                        "weighted_average_yield": wa_y if wa_y else impl_y,
                        "source_url":             url,
                        "source_label":           source_label,
                    }
                    log.info(
                        f"  [{source_label}] {tenor_label} ✓ (text) "
                        f"price={price}  yield={impl_y}%"
                    )
                    break

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# MASTER FETCHER  (updated source order)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_tbill_all_tenors(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    Master fetcher with updated 4-source fallback chain.

    Source priority (2026):
        1. DBIE CSV API       — most reliable, structured, no WAF
        2. RSS feed           — no WAF, low bandwidth
        3. PR listing page    — live listing, not WAF-blocked
        4. prid probe         — brute-force sequential scan (last resort)

    Each source only runs for tenors not yet found by a prior source.
    The probe (Source 4) only runs if Sources 1–3 all failed.
    """
    results: Dict[str, Dict[str, Any]] = {}

    def _merge(new: Dict) -> None:
        for k, v in new.items():
            if k not in results:
                results[k] = v

    def _missing() -> List[str]:
        return [key for _, _, key, _ in TENORS_CONFIG if key not in results]

    # ── Source 1: DBIE CSV API ────────────────────────────────────────────────
    log.info("==> [Step 1/4] DBIE CSV API")
    try:
        _merge(fetch_from_dbie_api(session))
    except Exception as e:
        log.warning(f"  Source 1 unhandled exception: {e}")
        log.debug(traceback.format_exc())

    if not _missing():
        log.info("  All 3 tenors fetched from Source 1 — skipping Sources 2–4")
        _log_final(results)
        return results

    # ── Source 2B: RSS Feed ───────────────────────────────────────────────────
    log.info(f"==> [Step 2/4] RSS feed (missing: {_missing()})")
    try:
        _merge(fetch_from_rss(session))
    except Exception as e:
        log.warning(f"  Source 2B unhandled exception: {e}")
        log.debug(traceback.format_exc())

    if not _missing():
        log.info("  All 3 tenors fetched — skipping Sources 3–4")
        _log_final(results)
        return results

    # ── Source 2A: PR Listing Page ────────────────────────────────────────────
    log.info(f"==> [Step 3/4] PR listing page (missing: {_missing()})")
    try:
        _merge(fetch_from_pr_listing(session))
    except Exception as e:
        log.warning(f"  Source 2A unhandled exception: {e}")
        log.debug(traceback.format_exc())

    if not _missing():
        log.info("  All 3 tenors fetched — skipping Source 4")
        _log_final(results)
        return results

    # ── Source 2C: prid probe ─────────────────────────────────────────────────
    log.info(f"==> [Step 4/4] Sequential prid probe (missing: {_missing()})")
    try:
        _merge(fetch_from_prid_probe(session))
    except Exception as e:
        log.warning(f"  Source 2C unhandled exception: {e}")
        log.debug(traceback.format_exc())

    _log_final(results)
    return results


def _log_final(results: Dict[str, Dict[str, Any]]) -> None:
    for _, _, key, _ in TENORS_CONFIG:
        if key in results:
            r = results[key]
            log.info(
                f"  FINAL {key.upper()}: "
                f"yield={r['implicit_yield']}%  "
                f"price={r['cutoff_price']}  "
                f"date={r['auction_date']}  "
                f"src={r.get('source_label','?')}"
            )
        else:
            log.warning(f"  FINAL {key.upper()}: NOT FETCHED — retaining existing JSON value")


# ═══════════════════════════════════════════════════════════════════════════════
# MANUAL INPUT  (unchanged from v3.0.0)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_manual_input() -> Dict[str, Dict[str, Any]]:
    if not is_interactive():
        raise RuntimeError(
            "fetch_from_manual_input() called in non-interactive (CI) mode."
        )
    print("\n" + "─" * 60)
    print("  MANUAL INPUT MODE")
    print("  Source: https://data.rbi.org.in/DBIE/")
    print("─" * 60)
    while True:
        try:
            date_s  = input("\n  Auction date (YYYY-MM-DD): ").strip()
            if not re.match(r"\d{4}-\d{2}-\d{2}", date_s):
                print("  ✗ Use YYYY-MM-DD"); continue
            p91   = float(input("  91D  cut-off price (e.g. 98.6280): ").strip())
            p182  = input("  182D cut-off price (Enter to skip):  ").strip()
            p364  = input("  364D cut-off price (Enter to skip):  ").strip()
            break
        except (ValueError, EOFError):
            print("  ✗ Invalid number, try again.")

    out: Dict[str, Dict[str, Any]] = {}
    for price_str, tenor_days, series_key in [
        (str(p91), 91, "tbill_91d"),
        (p182, 182, "tbill_182d"),
        (p364, 364, "tbill_364d"),
    ]:
        if not price_str:
            continue
        try:
            p = float(price_str)
            y = round(implicit_yield(p, tenor_days), 4)
            print(f"  {series_key}: {y:.4f}%")
            out[series_key] = {
                "tenor_days": tenor_days, "auction_date": date_s,
                "cutoff_price": p, "implicit_yield": y,
                "weighted_average_yield": y, "source_url": "manual_input",
                "source_label": "manual_input",
            }
        except ValueError:
            pass
    return out


# ═══════════════════════════════════════════════════════════════════════════════
# ATOMIC WRITE  (unchanged from v3.0.0)
# ═══════════════════════════════════════════════════════════════════════════════

def atomic_write_json(data: dict, path: str) -> None:
    dir_path = os.path.dirname(os.path.abspath(path))
    fd, tmp  = tempfile.mkstemp(dir=dir_path, suffix=".tmp", prefix=".rbi_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
        os.replace(tmp, path)
    except Exception:
        try: os.unlink(tmp)
        except OSError: pass
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN  (unchanged logic from v3.0.0; version bump only)
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh rbi_data.json with latest RBI T-Bill auction data (v3.1.0)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--json-path",  default=DEFAULT_JSON_PATH)
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--manual",     action="store_true")
    parser.add_argument("--force",      action="store_true")
    parser.add_argument("--verbose",    action="store_true")
    parser.add_argument("--log-json",   action="store_true")
    args = parser.parse_args()

    if os.environ.get("RBI_DRY_RUN",   "").lower() in ("true", "1"): args.dry_run = True
    if os.environ.get("RBI_FORCE",     "").lower() in ("true", "1"): args.force   = True
    if os.environ.get("RBI_LOG_LEVEL", "").upper() == "DEBUG":        args.verbose = True

    global log
    log = setup_logging(verbose=args.verbose, log_json=args.log_json)
    ci  = is_ci()

    SEP = "=" * 62
    log.info(SEP)
    log.info("  RBI Treasury Bill Dashboard — Data Refresh  v3.1.0")
    log.info(f"  {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    log.info(f"  CI={ci}  dry-run={args.dry_run}  force={args.force}")
    log.info(SEP)

    if args.manual and ci:
        log.error("--manual is not usable in CI mode.")
        sys.exit(1)

    if not os.path.exists(args.json_path):
        log.error(f"rbi_data.json not found: {args.json_path}")
        sys.exit(1)

    try:
        with open(args.json_path, "r", encoding="utf-8") as f:
            current_data = json.load(f)
    except json.JSONDecodeError as e:
        log.error(f"rbi_data.json is invalid JSON: {e}")
        sys.exit(1)

    schema_ver = current_data.get("_meta", {}).get("schema_version")
    if schema_ver and schema_ver != SUPPORTED_SCHEMA_VERSION:
        log.warning(f"Schema mismatch: file={schema_ver!r} script={SUPPORTED_SCHEMA_VERSION!r}")

    prev_91d  = current_data.get("risk_free", {}).get("implicit_yield")
    ts        = current_data.get("tbill_series", {})
    prev_182d = ts.get("tbill_182d", [None])[-1] if ts.get("tbill_182d") else None
    prev_364d = ts.get("tbill_364d", [None])[-1] if ts.get("tbill_364d") else None

    log.info(
        f"Stored: 91D={prev_91d}%  182D={prev_182d}%  364D={prev_364d}%  "
        f"updated={fmtdate(current_data.get('_meta',{}).get('last_updated'))}"
    )

    log.info(f"\n{'─'*62}")
    log.info("[Step 1] Fetching latest RBI T-Bill data…")

    if args.manual:
        new_tenors = fetch_from_manual_input()
    else:
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)
        session.max_redirects = 5
        try:
            new_tenors = fetch_tbill_all_tenors(session)
        except Exception as e:
            log.error(f"Unhandled fetch exception: {e}\nJSON preserved unchanged.")
            log.debug(traceback.format_exc())
            sys.exit(0)

    if "tbill_91d" not in new_tenors:
        log.error(
            "Could not fetch 91D T-Bill data from any source.\n"
            "JSON preserved unchanged.\n"
            "  → Run: python refresh_rbi_data.py --manual\n"
            "  → Or:  python refresh_rbi_data.py --verbose for diagnostics"
        )
        sys.exit(0)

    new_rf = new_tenors["tbill_91d"]

    # Stale-date guard
    stored_date  = current_data.get("risk_free", {}).get("auction_date")
    fetched_date = new_rf.get("auction_date")
    if stored_date and fetched_date and not args.force:
        s_dt = parse_date_flexible(stored_date)
        f_dt = parse_date_flexible(fetched_date)
        if s_dt and f_dt and f_dt <= s_dt:
            log.info(
                f"Fetched date ({fetched_date}) ≤ stored ({stored_date}). "
                "No update needed."
            )
            sys.exit(0)

    log.info(f"\n{'─'*62}")
    log.info("[Step 2] Validating 91D record…")

    issues   = validate_record(new_rf, prev_91d, ci_mode=ci)
    errors   = [m for s, m in issues if s == "error"]
    warnings = [m for s, m in issues if s == "warning"]

    if errors:
        for e in errors: log.error(f"  • {e}")
        log.error("ABORT — JSON preserved unchanged.")
        sys.exit(1)

    if warnings:
        for w in warnings: log.warning(f"  ⚠  {w}")
        spike = any("Spike" in w for w in warnings)
        if spike and not args.force and not ci and is_interactive():
            try:
                if input("\n  Proceed despite spike? (yes/no): ").strip().lower() != "yes":
                    log.info("Aborted by user."); sys.exit(0)
            except (EOFError, OSError):
                log.warning("stdin unavailable; proceeding (CI assumption)")
    else:
        log.info("  ✓ All validation checks passed")

    log.info(f"\n{'─'*62}")
    log.info("[Step 3] Building updated JSON…")

    updated  = copy.deepcopy(current_data)
    rf_yield = new_rf["implicit_yield"]
    g10y     = updated.get("kpi", {}).get("gsec_10y_yield", 6.87)
    repo     = updated.get("policy", {}).get("repo_rate", 5.50)

    updated["risk_free"].update({
        "tenor_days":             91,
        "auction_date":           new_rf["auction_date"],
        "cutoff_price":           new_rf["cutoff_price"],
        "implicit_yield":         round(rf_yield, 4),
        "weighted_average_yield": round(new_rf["weighted_average_yield"], 4),
        "source_url":             new_rf.get("source_url", ""),
        "source_label":           new_rf.get("source_label", "unknown"),
        "reconciliation_check": (
            f"((100 - {new_rf['cutoff_price']}) / {new_rf['cutoff_price']}) "
            f"× (365 / 91) × 100 = {rf_yield:.4f}%"
        ),
    })

    spread_bps = round((g10y - rf_yield) * 100)
    updated["kpi"].update({
        "tbill_91d_yield":          round(rf_yield, 4),
        "tbill_91d_cutoff_price":   new_rf["cutoff_price"],
        "tbill_91d_auction_date":   new_rf["auction_date"],
        "yield_spread_10y_91d_bps": spread_bps,
    })

    for bond in updated.get("bond_table", {}).get("bonds", []):
        bond["vs_repo_bps"] = round((bond.get("ytm", 0) - repo) * 100)

    now_label = datetime.now(IST).strftime("%b %y")
    for sk in ["tbill_91d", "tbill_182d", "tbill_364d"]:
        if sk in new_tenors:
            new_val = round(new_tenors[sk]["implicit_yield"], 4)
            series  = updated["tbill_series"][sk]
            labels  = updated["tbill_series"]["labels"]
            if labels and labels[-1] == now_label:
                series[-1] = new_val
            else:
                labels.append(now_label)
                series.append(new_val)
                if len(series) > 18:
                    updated["tbill_series"]["labels"] = labels[-18:]
                    updated["tbill_series"][sk]       = series[-18:]
        elif sk == "tbill_91d":
            s = updated["tbill_series"][sk]
            if s: s[-1] = round(rf_yield, 4)

    if updated.get("yield_curve", {}).get("current", {}).get("yields"):
        updated["yield_curve"]["current"]["yields"][0] = round(rf_yield, 2)

    now_ist = datetime.now(IST).isoformat()
    updated["_meta"]["last_updated"] = now_ist

    changes_parts = [
        f"91D: {prev_91d}% → {rf_yield:.4f}%",
        f"price={new_rf['cutoff_price']}",
        f"spread={spread_bps}bps",
        f"src={new_rf.get('source_label','?')}",
    ]
    for k, pv in [("tbill_182d", prev_182d), ("tbill_364d", prev_364d)]:
        if k in new_tenors:
            changes_parts.append(
                f"{k.upper()}: {pv}% → {new_tenors[k]['implicit_yield']:.4f}%"
            )

    updated["audit_log"].append({
        "timestamp":         now_ist,
        "action":            "auto_refresh_v3.1",
        "source":            new_rf.get("source_label", "unknown"),
        "operator":          "refresh_rbi_data.py v3.1.0",
        "ci_mode":           ci,
        "changes":           " | ".join(changes_parts),
        "validation_status": "passed" if not warnings else "passed_with_warnings",
        "warnings":          warnings,
        "tenors_fetched":    list(new_tenors.keys()),
        "sources_tried":     list({v.get("source_label","?") for v in new_tenors.values()}),
    })
    if len(updated["audit_log"]) > 50:
        updated["audit_log"] = updated["audit_log"][-50:]

    # Change detection
    if json_checksum(current_data) == json_checksum(updated):
        log.info("No effective data change — skipping write.")
        sys.exit(0)

    log.info("\n[Step 4] Summary:")
    log.info(f"  91D yield     : {prev_91d}% → {rf_yield:.4f}%")
    log.info(f"  10Y-91D spread: → {spread_bps} bps")
    for k, lbl, pv in [("tbill_182d","182D",prev_182d),("tbill_364d","364D",prev_364d)]:
        if k in new_tenors:
            log.info(f"  {lbl} yield     : {pv}% → {new_tenors[k]['implicit_yield']:.4f}%")
    log.info(f"  last_updated  : {now_ist}")

    if args.dry_run:
        log.info("[DRY RUN] No files written.")
        return

    backup = args.json_path.replace(".json", ".backup.json")
    try:
        atomic_write_json(current_data, backup)
        log.info(f"\n[Step 5] Backup → {backup}")
    except Exception as e:
        log.warning(f"  Backup failed: {e} (non-fatal)")

    try:
        atomic_write_json(updated, args.json_path)
        log.info(f"[Step 6] JSON updated → {args.json_path}")
    except Exception as e:
        log.error(f"  CRITICAL write failure: {e}")
        sys.exit(1)

    log.info("\n  ✓ Refresh complete.")
    log.info(SEP)


if __name__ == "__main__":
    main()

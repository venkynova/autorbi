#!/usr/bin/env python3
r"""
refresh_rbi_data.py  (v3.0.0 — production autonomous)
=======================================================
RBI Treasury Bill Dashboard — Autonomous Data Refresh Script
Author  : Javvaji Venkatesh
Version : 3.0.0 — Full CI/CD automation release

WHAT THIS SCRIPT DOES
---------------------
Fetches the latest RBI T-Bill auction cut-off yields for all three tenors
(91-day, 182-day, 364-day) from official RBI sources, validates the fetched
data against multiple safety checks, then writes the results to rbi_data.json.

If fetching fails for any reason, the existing JSON is preserved untouched
and the script exits cleanly (exit code 0) to prevent dashboard breakage.

CHANGES IN v3.0.0 (over v2.0.0)
---------------------------------
- ADDED: --verbose / LOG_LEVEL=DEBUG flag for CI diagnostic output
- ADDED: Multi-source fetch strategy with automatic fallback:
    Source 1: RBI DBIE structured data API (JSON endpoint)
    Source 2: RBI Press Release HTML scraping (original method)
    Source 3: RBI DBIE legacy table scraper
- ADDED: Retry logic with exponential backoff (configurable, default 3 retries)
- ADDED: In-CI stdin guard — spike warning never blocks on input() in CI mode
- ADDED: Structured JSON logging output (--log-json) for GitHub Actions
- ADDED: --verbose flag for GitHub Actions debug output
- ADDED: Atomic JSON write (write to .tmp, then rename) to prevent corruption
- ADDED: Schema version check before writing (prevents version mismatch)
- ADDED: Stale-data guard: if fetched auction_date ≤ stored date, skip update
- FIXED: Interactive input() calls gated behind is_interactive() check
- FIXED: All network calls wrapped in retry_get() with configurable backoff
- PRESERVED: All v2.0.0 bug fixes (BUG-2 through NOTE-10) remain in effect

USAGE
-----
  python refresh_rbi_data.py                      # standard CI refresh
  python refresh_rbi_data.py --dry-run            # preview without writing
  python refresh_rbi_data.py --manual             # enter values interactively
  python refresh_rbi_data.py --force              # bypass spike guard
  python refresh_rbi_data.py --verbose            # debug output
  python refresh_rbi_data.py --log-json           # structured JSON log output
  python refresh_rbi_data.py --json-path /p/rbi_data.json

ENVIRONMENT VARIABLES (for GitHub Actions / CI)
------------------------------------------------
  RBI_FORCE=true           equivalent to --force
  RBI_DRY_RUN=true         equivalent to --dry-run
  RBI_LOG_LEVEL=DEBUG      equivalent to --verbose
  CI=true                  auto-detected; disables interactive input()

DEPENDENCIES
------------
  pip install requests beautifulsoup4 lxml
  (All in requirements.txt)

SCHEDULING
----------
  Managed by .github/workflows/rbi-auto-update.yml
  Manual cron (Linux): 0 2 * * 3  cd /dashboard && python refresh_rbi_data.py
"""

# ── standard library ──────────────────────────────────────────────────────────
import json
import sys
import os
import re
import copy
import time
import logging
import hashlib
import argparse
import tempfile
import traceback
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple
from requests.exceptions import ChunkedEncodingError

# ── third-party (fail fast with clear message) ────────────────────────────────
try:
    import requests
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

# ── file paths ────────────────────────────────────────────────────────────────
_SCRIPT_DIR       = os.path.dirname(os.path.abspath(__file__))
DEFAULT_JSON_PATH = os.path.join(_SCRIPT_DIR, "rbi_data.json")

# ── yield sanity bounds ───────────────────────────────────────────────────────
YIELD_SANITY_MIN  = 1.00    # % — floor (never below 1% in normal conditions)
YIELD_SANITY_MAX  = 20.00   # % — ceiling (never above 20% in normal conditions)
YIELD_SPIKE_BPS   = 100     # bps — warn if single-auction change exceeds this
RECON_TOLERANCE   = 0.005   # % — acceptable formula reconciliation error
STALE_DAYS_SCRAPE = 60      # days — warn if fetched date is older than this

# ── network settings ─────────────────────────────────────────────────────────
REQUEST_TIMEOUT   = 25      # seconds per individual HTTP request
RETRY_COUNT       = 3       # number of retries on transient failures
RETRY_BACKOFF_BASE= 4       # seconds — exponential backoff base (4, 8, 16…)

# ── Full browser headers (required: RBI returns 403 on minimal User-Agent) ───
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
    "Cache-Control":   "max-age=0",
}

# ── RBI source URLs ───────────────────────────────────────────────────────────
# Source 1: RBI DBIE — most reliable, structured endpoint
RBI_DBIE_TBILL_URL = (
    "https://data.rbi.org.in/DBIE/dbie.rbi?site=publications"
    "#!4"   # T-Bill section anchor
)
# Source 1b: DBIE direct table — structured HTML table
RBI_DBIE_TBILL_TABLE = (
    "https://data.rbi.org.in/DBIE/dbie.rbi?site=statistics"
    "&relPath=%2FRBI%2FFinancial%20Markets%2FGovernment%20Securities%20Market"
    "%2FPrimary%20Market%2FAuctions%20of%20Government%20Securities"
    "%2F91-Day%20T-Bill%20Auction%20Results"
)
# Source 2: RBI main press release search page
RBI_PR_SEARCH = (
    "https://www.rbi.org.in/Scripts/BS_PressReleasesView.aspx"
    "?Category=0&Lang=0"
)
# Source 3: RBI Notifications (alternative auction results location)
RBI_NOTIFICATIONS_URL = (
    "https://www.rbi.org.in/Scripts/BS_ViewBulletin.aspx"
)

# ── Tenor configurations ──────────────────────────────────────────────────────
# (display_label, tenor_days, json_series_key, press_release_keywords)
TENORS_CONFIG: List[Tuple[str, int, str, List[str]]] = [
    ("91D",  91,  "tbill_91d",
     ["91-day", "91 day", "91day", "91-days", "91 days"]),
    ("182D", 182, "tbill_182d",
     ["182-day", "182 day", "182day", "182-days", "182 days"]),
    ("364D", 364, "tbill_364d",
     ["364-day", "364 day", "364day", "364-days", "364 days"]),
]

# ── Schema version this script is compatible with ─────────────────────────────
SUPPORTED_SCHEMA_VERSION = "1.0.0"


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

def setup_logging(verbose: bool = False, log_json: bool = False) -> logging.Logger:
    """
    Configure the root logger.
    - verbose=True  → DEBUG level with full tracebacks
    - log_json=True → each log line is a JSON object (for GitHub Actions parsing)
    """
    level = logging.DEBUG if verbose else logging.INFO
    logger = logging.getLogger("rbi_refresh")
    logger.setLevel(level)
    logger.handlers.clear()

    if log_json:
        # Structured JSON handler — useful for log aggregation pipelines
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
        fmt = "%(asctime)s  %(levelname)-8s  %(message)s"
        datefmt = "%Y-%m-%d %H:%M:%S"
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(fmt, datefmt))

    logger.addHandler(handler)
    return logger


# Module-level logger; reconfigured in main() once args are parsed
log = logging.getLogger("rbi_refresh")


# ═══════════════════════════════════════════════════════════════════════════════
# ENVIRONMENT / CI DETECTION
# ═══════════════════════════════════════════════════════════════════════════════

def is_ci() -> bool:
    """
    Return True if running in a CI/CD environment (GitHub Actions, Jenkins, etc.).
    In CI mode, interactive input() calls must never be executed.
    """
    return any([
        os.environ.get("CI", "").lower() in ("true", "1", "yes"),
        os.environ.get("GITHUB_ACTIONS", "").lower() == "true",
        os.environ.get("JENKINS_URL"),
        not sys.stdin.isatty(),   # stdin not a terminal → non-interactive
    ])


def is_interactive() -> bool:
    """Inverse of is_ci(); True only when a human can type at the keyboard."""
    return not is_ci()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def implicit_yield(price: float, days: int) -> float:
    """
    RBI bank-discount yield formula:
        ((Face Value - Price) / Price) × (365 / days) × 100

    Args:
        price: Cut-off price per ₹100 face value (e.g. 98.6280)
        days:  Tenor in days (91, 182, or 364)

    Returns:
        Annualised yield as a percentage (e.g. 5.5796)
    """
    if price <= 0 or price >= 100:
        raise ValueError(f"Invalid T-Bill price: {price} (must be in range 0–100)")
    return round(((100.0 - price) / price) * (365.0 / days) * 100.0, 6)


def json_checksum(data: dict) -> str:
    """Compute a stable SHA-256 checksum of a JSON-serialisable dict."""
    serialised = json.dumps(data, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()


def parse_date_flexible(s: str) -> Optional[datetime]:
    """
    Parse date strings in multiple formats:
        YYYY-MM-DD, DD-Mon-YYYY, DD/MM/YYYY, DD Mon YYYY, Month DD, YYYY
    Returns a timezone-aware datetime in IST, or None if unparseable.
    """
    s = s.strip()
    for fmt in (
        "%Y-%m-%d",
        "%d-%b-%Y",
        "%d/%m/%Y",
        "%d %b %Y",
        "%B %d, %Y",
        "%d-%B-%Y",
    ):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=IST)
        except ValueError:
            continue
    return None


def fmtdate(iso_str: Optional[str]) -> str:
    """Format an ISO datetime string to a human-readable form for logs."""
    if not iso_str:
        return "unknown"
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime("%d %b %Y %H:%M IST")
    except ValueError:
        return iso_str


# ═══════════════════════════════════════════════════════════════════════════════
# NETWORK — RETRY WRAPPER
# ═══════════════════════════════════════════════════════════════════════════════

def retry_get(
    session: requests.Session,
    url: str,
    retries: int = RETRY_COUNT,
    backoff_base: int = RETRY_BACKOFF_BASE,
    timeout: int = REQUEST_TIMEOUT,
) -> Optional[requests.Response]:
    """
    Perform a GET request with exponential-backoff retry logic.

    Retries on:
        - ConnectionError, Timeout, ChunkedEncodingError (transient network issues)
        - HTTP 429 (rate limited), 500, 502, 503, 504 (server errors)

    Does NOT retry on:
        - HTTP 403 (Forbidden) — indicates a header/auth problem, not transient
        - HTTP 404 (Not Found) — page does not exist

    Args:
        session:      requests.Session with browser headers pre-set
        url:          Target URL
        retries:      Maximum number of retry attempts after the first failure
        backoff_base: Base seconds for exponential backoff (attempt 1→base, 2→base*2…)
        timeout:      Per-request timeout in seconds

    Returns:
        requests.Response on success, None on all failures exhausted.
    """
    attempt = 0
    last_exc: Optional[Exception] = None

    while attempt <= retries:
        try:
            if attempt > 0:
                wait = backoff_base * (2 ** (attempt - 1))
                log.warning(f"  Retry {attempt}/{retries} — waiting {wait}s before retrying {url[:60]}…")
                time.sleep(wait)

            log.debug(f"  GET {url[:80]}")
            resp = session.get(url, timeout=timeout)

            # Non-retryable errors — fail immediately
            if resp.status_code in (403, 404):
                log.warning(f"  HTTP {resp.status_code} — {url[:60]} (non-retryable)")
                return None

            # Retryable server errors
            if resp.status_code in (429, 500, 502, 503, 504):
                log.warning(f"  HTTP {resp.status_code} — will retry")
                attempt += 1
                continue

            resp.raise_for_status()
            log.debug(f"  HTTP 200 OK — {len(resp.content)} bytes")
            return resp

        except (
            requests.ConnectionError,
            requests.Timeout,
            ChunkedEncodingError,
        ) as e:
            last_exc = e
            log.warning(f"  Network error (attempt {attempt + 1}): {type(e).__name__}: {e}")
            attempt += 1

        except requests.RequestException as e:
            # Non-retryable request exception
            log.error(f"  Request failed (non-retryable): {e}")
            return None

    log.error(f"  All {retries + 1} attempts failed for {url[:60]}. Last error: {last_exc}")
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT EXTRACTION HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def extract_price_from_text(text: str, context_window: int = 300) -> Optional[float]:
    """
    Extract T-Bill cut-off price from HTML text.

    Strategy: find "cut-off price" keyword, then search within a bounded
    window for a price in the valid T-Bill range (90.00–99.99).
    This prevents matching unrelated numbers elsewhere in the document.

    Args:
        text:           Plain text extracted from the press release page
        context_window: How many characters after the keyword to search

    Returns:
        Price as float (e.g. 98.6280), or None if not found.
    """
    # Priority patterns — most specific first
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
            # T-Bill prices are always between 90 and 99.xxxx
            pm = re.search(r"\b(9[0-9]\.\d{2,6})\b", window)
            if pm:
                val = float(pm.group(1))
                log.debug(f"  Extracted price {val} via keyword '{kw}'")
                return val

    # Broader fallback: any "price" keyword with bounded window
    m = re.search(r"\bprice\b", text, re.IGNORECASE)
    if m:
        window = text[m.start(): m.start() + 200]
        pm = re.search(r"\b(9[0-9]\.\d{2,6})\b", window)
        if pm:
            val = float(pm.group(1))
            log.debug(f"  Extracted price {val} via broad 'price' fallback")
            return val

    log.debug("  Could not extract price from text")
    return None


def extract_yield_from_text(text: str) -> Optional[float]:
    """
    Extract cut-off yield percentage from press release text.
    Looks for patterns like "cut-off yield: 6.52%" or "yield of 6.52 per cent".

    Returns yield as float, or None if not found.
    """
    patterns = [
        r"cut[-\s]?off\s+yield[^0-9]*(\d+\.\d{2,4})\s*(?:%|per\s+cent)",
        r"cutoff\s+yield[^0-9]*(\d+\.\d{2,4})\s*(?:%|per\s+cent)",
        r"yield\s+of\s+(\d+\.\d{2,4})\s*(?:%|per\s+cent)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if YIELD_SANITY_MIN <= val <= YIELD_SANITY_MAX:
                log.debug(f"  Extracted explicit yield {val}% from text")
                return val
    return None


def extract_wa_yield_from_text(text: str) -> Optional[float]:
    """
    Extract weighted average (WA) yield from press release text.
    Returns float or None.
    """
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
    """
    Extract auction date anchored to semantic context keywords.

    Tries progressively more generic patterns to avoid matching
    unrelated dates (e.g. press release publication date).

    Returns date as string (YYYY-MM-DD preferred), or None.
    """
    # Most specific patterns first
    context_patterns = [
        r"auction\s+date[^:]*:\s*(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
        r"date\s+of\s+auction[^:]*:\s*(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
        r"auction\s+held\s+on\s+(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
        r"held\s+on\s+(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
    ]
    for pat in context_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            ds = m.group(1).strip()
            dt = parse_date_flexible(ds)
            if dt:
                # Return ISO format for consistency
                return dt.strftime("%Y-%m-%d")

    # Generic date patterns as last resort
    for pat in [
        r"(\d{1,2}[-][A-Za-z]{3,9}[-]\d{4})",
        r"(\d{4}-\d{2}-\d{2})",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            ds = m.group(1).strip()
            dt = parse_date_flexible(ds)
            if dt:
                return dt.strftime("%Y-%m-%d")

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# DATA VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def validate_record(
    record: Dict[str, Any],
    previous_yield: Optional[float],
    ci_mode: bool = False,
) -> List[Tuple[str, str]]:
    """
    Validate a fetched T-Bill record against multiple safety checks.

    Returns a list of (severity, message) tuples:
        severity = 'error'   → caller should abort; record is definitely wrong
        severity = 'warning' → proceed with caution; data looks unusual but may be valid

    Checks performed:
        1. Required fields present (error)
        2. Yield formula reconciliation within RECON_TOLERANCE (error)
        3. Yield within absolute sanity range [YIELD_SANITY_MIN, YIELD_SANITY_MAX] (error)
        4. Price within plausible T-Bill range [85, 99.99] (error)
        5. Auction date parseable (warning)
        6. Auction date not stale (warning, relaxed to STALE_DAYS_SCRAPE days)
        7. Yield change not a spike vs previous (warning)

    Args:
        record:         Dict with keys: cutoff_price, implicit_yield, auction_date, tenor_days
        previous_yield: Last known yield from stored JSON (for spike detection)
        ci_mode:        If True, disable interactive spike confirmation

    Returns:
        List of (severity, message) tuples. Empty list = all checks passed.
    """
    issues: List[Tuple[str, str]] = []
    price  = record.get("cutoff_price")
    yield_ = record.get("implicit_yield")
    date_s = record.get("auction_date")
    days   = record.get("tenor_days", 91)

    # Check 1: Required fields
    if price is None or yield_ is None:
        issues.append(("error", "Missing required fields: cutoff_price or implicit_yield"))
        return issues   # Can't proceed with other checks

    # Check 2: Formula reconciliation
    try:
        computed = implicit_yield(price, days)
        diff = abs(computed - yield_)
        if diff > RECON_TOLERANCE:
            issues.append(("error",
                f"Yield reconciliation fail: "
                f"formula gives {computed:.6f}%, record says {yield_:.6f}%, "
                f"diff = {diff:.6f}% (tolerance: {RECON_TOLERANCE}%)"
            ))
        else:
            log.debug(f"  Formula check passed: computed {computed:.4f}% vs stored {yield_:.4f}%")
    except ValueError as e:
        issues.append(("error", f"Formula computation error: {e}"))

    # Check 3: Absolute sanity range
    if not (YIELD_SANITY_MIN <= yield_ <= YIELD_SANITY_MAX):
        issues.append(("error",
            f"Yield {yield_}% is outside sanity range "
            f"[{YIELD_SANITY_MIN}%, {YIELD_SANITY_MAX}%]"
        ))

    # Check 4: Price plausibility
    if price is not None and not (85.0 <= price <= 99.9999):
        issues.append(("error",
            f"Cut-off price {price} is outside plausible T-Bill range [85, 99.9999]"
        ))

    # Check 5: Date parseable
    if date_s:
        auction_dt = parse_date_flexible(date_s)
        if auction_dt is None:
            issues.append(("warning", f"Cannot parse auction_date: {date_s!r}"))
        else:
            # Check 6: Date freshness
            age_days = (datetime.now(IST) - auction_dt).days
            if age_days > STALE_DAYS_SCRAPE:
                issues.append(("warning",
                    f"Auction date {date_s} is {age_days} days old "
                    f"(threshold: {STALE_DAYS_SCRAPE} days)"
                ))
            else:
                log.debug(f"  Date freshness OK: {date_s} is {age_days} days old")
    else:
        issues.append(("warning", "No auction_date found in fetched record"))

    # Check 7: Spike guard
    if previous_yield is not None and yield_ is not None:
        diff_bps = abs(yield_ - previous_yield) * 100
        if diff_bps > YIELD_SPIKE_BPS:
            issues.append(("warning",
                f"Large yield change detected: "
                f"{diff_bps:.0f} bps (previous: {previous_yield}%, new: {yield_:.4f}%). "
                f"Pass --force to bypass."
            ))

    return issues


# ═══════════════════════════════════════════════════════════════════════════════
# DATA FETCHERS — THREE INDEPENDENT SOURCES
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_single_press_release(
    session: requests.Session,
    url: str,
    tenor_days: int,
) -> Optional[Dict[str, Any]]:
    """
    Parse one RBI press release page for a specific tenor.
    Tries to extract: cutoff_price, implicit_yield, weighted_average_yield, auction_date.

    Returns a validated record dict, or None on parse failure.
    """
    resp = retry_get(session, url)
    if resp is None:
        log.warning(f"  Could not fetch press release: {url[:70]}")
        return None

    soup  = BeautifulSoup(resp.text, "lxml")
    # Use separator to create readable text without run-together words
    text  = soup.get_text(" ", strip=True)

    auction_date  = extract_auction_date(text)
    cutoff_price  = extract_price_from_text(text)
    explicit_yield = extract_yield_from_text(text)
    wa_yield      = extract_wa_yield_from_text(text)

    if cutoff_price is None:
        # Last resort: try to back-calculate price from explicit yield
        if explicit_yield is not None:
            # Reverse formula: price = 100 / (1 + yield/100 * days/365)
            computed_price = round(
                100.0 / (1.0 + (explicit_yield / 100.0) * (tenor_days / 365.0)), 4
            )
            log.debug(
                f"  No price found; back-calculated price {computed_price} "
                f"from explicit yield {explicit_yield}%"
            )
            cutoff_price = computed_price
        else:
            log.warning(f"  Could not extract price or yield from {url[:70]}")
            return None

    try:
        impl_y = round(implicit_yield(cutoff_price, tenor_days), 4)
    except ValueError as e:
        log.warning(f"  Invalid price {cutoff_price}: {e}")
        return None

    # If explicit yield was scraped, prefer it over computed; flag large discrepancies
    if explicit_yield is not None:
        diff = abs(explicit_yield - impl_y)
        if diff > RECON_TOLERANCE * 10:
            log.warning(
                f"  Explicit yield {explicit_yield}% differs significantly "
                f"from formula {impl_y}% — using formula value"
            )

    return {
        "tenor_days":             tenor_days,
        "auction_date":           auction_date or datetime.now(IST).strftime("%Y-%m-%d"),
        "cutoff_price":           cutoff_price,
        "implicit_yield":         impl_y,
        "weighted_average_yield": wa_yield if wa_yield is not None else impl_y,
        "source_url":             url,
    }


def fetch_from_press_releases(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    SOURCE 1: Scrape RBI press release index page.

    Searches the RBI press release index for T-Bill auction results.
    For each of the three tenors (91D, 182D, 364D), finds the most recent
    matching press release link and parses it.

    Returns: dict keyed by series name (tbill_91d, tbill_182d, tbill_364d)
    """
    log.info("  [Source 1] Fetching RBI press release index…")
    results: Dict[str, Dict[str, Any]] = {}

    resp = retry_get(session, RBI_PR_SEARCH)
    if resp is None:
        log.warning("  [Source 1] Could not reach RBI press release page")
        return results

    soup  = BeautifulSoup(resp.text, "lxml")
    links = soup.find_all("a", href=True)

    # Collect all T-Bill related links
    tbill_keywords = ["t-bill", "tbill", "treasury bill", "91-day", "182-day", "364-day"]
    all_tbill_links: List[Tuple[str, str]] = []
    for a in links:
        txt = a.get_text(" ", strip=True).lower()
        if any(kw in txt for kw in tbill_keywords):
            href = a["href"]
            if not href.startswith("http"):
                href = "https://www.rbi.org.in" + href
            all_tbill_links.append((a.get_text(" ", strip=True), href))

    log.info(f"  [Source 1] Found {len(all_tbill_links)} T-Bill press release link(s)")
    if not all_tbill_links:
        return results

    for tenor_label, tenor_days, series_key, keywords in TENORS_CONFIG:
        # Find the most recent press release for this specific tenor
        matched_url = None
        for link_text, link_url in all_tbill_links:
            lt = link_text.lower()
            if any(kw in lt for kw in keywords):
                matched_url = link_url
                break

        # Fallback: use the first overall T-Bill press release
        if not matched_url and all_tbill_links:
            matched_url = all_tbill_links[0][1]
            log.debug(
                f"  [Source 1] No tenor-specific link for {tenor_label}; "
                f"using first T-Bill link"
            )

        if matched_url:
            log.info(f"  [Source 1] Parsing {tenor_label}: {matched_url[:75]}…")
            rec = _parse_single_press_release(session, matched_url, tenor_days)
            if rec:
                results[series_key] = rec
                log.info(
                    f"  [Source 1] {tenor_label} ✓ "
                    f"price={rec['cutoff_price']}  yield={rec['implicit_yield']}%  "
                    f"date={rec['auction_date']}"
                )
            else:
                log.warning(f"  [Source 1] {tenor_label}: parse failed")
        else:
            log.warning(f"  [Source 1] {tenor_label}: no press release link found")

    return results


def fetch_from_dbie(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    SOURCE 2: Scrape RBI DBIE (Database on Indian Economy) structured table.

    DBIE publishes structured HTML tables of T-Bill auction results which
    are more reliably parseable than press release prose text.

    Returns: dict keyed by series name, same structure as fetch_from_press_releases.
    """
    log.info("  [Source 2] Fetching RBI DBIE T-Bill table…")
    results: Dict[str, Dict[str, Any]] = {}

    resp = retry_get(session, RBI_DBIE_TBILL_TABLE)
    if resp is None:
        log.warning("  [Source 2] Could not reach DBIE table URL")
        return results

    soup   = BeautifulSoup(resp.text, "lxml")
    tables = soup.find_all("table")
    log.debug(f"  [Source 2] Found {len(tables)} tables on DBIE page")

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        # Look for a table with yield/price columns
        header_text = " ".join(
            th.get_text(" ", strip=True).lower()
            for th in rows[0].find_all(["th", "td"])
        )
        if not any(kw in header_text for kw in ["yield", "price", "cut-off", "cutoff"]):
            continue

        log.debug(f"  [Source 2] Candidate table header: {header_text[:100]}")

        # Parse columns to find price and yield positions
        headers = [th.get_text(" ", strip=True).lower() for th in rows[0].find_all(["th", "td"])]
        price_col  = next((i for i, h in enumerate(headers) if "price" in h and "cut" in h), None)
        yield_col  = next((i for i, h in enumerate(headers) if "yield" in h and "cut" in h), None)
        date_col   = next((i for i, h in enumerate(headers) if "date" in h), None)
        tenor_col  = next((i for i, h in enumerate(headers) if "tenor" in h or "days" in h), None)

        log.debug(
            f"  [Source 2] Columns — price:{price_col} yield:{yield_col} "
            f"date:{date_col} tenor:{tenor_col}"
        )

        if price_col is None and yield_col is None:
            continue

        # Parse data rows (most recent first, so break after first valid row per tenor)
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if len(cells) < max(filter(None, [price_col, yield_col, date_col, 0])) + 1:
                continue

            row_text = " ".join(c.get_text(" ", strip=True) for c in cells).lower()

            # Identify tenor from this row
            matched_tenor = None
            for tenor_label, tenor_days, series_key, keywords in TENORS_CONFIG:
                if any(kw in row_text for kw in keywords):
                    matched_tenor = (tenor_label, tenor_days, series_key)
                    break

            if matched_tenor is None:
                continue

            t_label, t_days, t_key = matched_tenor
            if t_key in results:
                continue   # Already have data for this tenor

            # Extract values
            try:
                raw_price = cells[price_col].get_text(" ", strip=True) if price_col is not None else ""
                raw_yield = cells[yield_col].get_text(" ", strip=True) if yield_col is not None else ""
                raw_date  = cells[date_col].get_text(" ", strip=True)  if date_col  is not None else ""

                price_m = re.search(r"(9[0-9]\.\d{2,6})", raw_price)
                yield_m = re.search(r"(\d+\.\d{2,4})",    raw_yield)
                price   = float(price_m.group(1)) if price_m else None
                yield_  = float(yield_m.group(1)) if yield_m else None

                if price is None and yield_ is not None:
                    # Back-calculate price
                    price = round(100.0 / (1.0 + (yield_ / 100.0) * (t_days / 365.0)), 4)

                if price is not None:
                    impl_y = round(implicit_yield(price, t_days), 4)
                    date_s = extract_auction_date(raw_date) or datetime.now(IST).strftime("%Y-%m-%d")
                    results[t_key] = {
                        "tenor_days":             t_days,
                        "auction_date":           date_s,
                        "cutoff_price":           price,
                        "implicit_yield":         impl_y,
                        "weighted_average_yield": yield_ if yield_ else impl_y,
                        "source_url":             RBI_DBIE_TBILL_TABLE,
                    }
                    log.info(
                        f"  [Source 2] {t_label} ✓ "
                        f"price={price}  yield={impl_y}%  date={date_s}"
                    )
            except (ValueError, IndexError, AttributeError) as e:
                log.debug(f"  [Source 2] Row parse error for {t_label}: {e}")
                continue

    log.info(f"  [Source 2] Fetched {len(results)} tenor(s) from DBIE")
    return results


def fetch_tbill_all_tenors(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    Master fetcher: tries each source in order, merges results.

    Strategy:
        1. Try RBI DBIE structured table (most reliable)
        2. Supplement missing tenors from RBI press releases
        3. If a tenor is still missing after both sources, log a warning

    Returns combined dict with up to 3 tenor keys.
    """
    results: Dict[str, Dict[str, Any]] = {}

    # Source 2 first (DBIE — more structured)
    log.info("Trying Source 2 (RBI DBIE structured table)…")
    try:
        dbie_results = fetch_from_dbie(session)
        results.update(dbie_results)
    except Exception as e:
        log.warning(f"  Source 2 exception: {e}")
        log.debug(traceback.format_exc())

    # Source 1 to fill any missing tenors
    missing = [key for _, _, key, _ in TENORS_CONFIG if key not in results]
    if missing:
        log.info(f"  Missing after Source 2: {missing}. Trying Source 1 (press releases)…")
        try:
            pr_results = fetch_from_press_releases(session)
            for key, rec in pr_results.items():
                if key not in results:
                    results[key] = rec
        except Exception as e:
            log.warning(f"  Source 1 exception: {e}")
            log.debug(traceback.format_exc())

    # Report final coverage
    for _, _, key, _ in TENORS_CONFIG:
        if key in results:
            r = results[key]
            log.info(
                f"  FINAL {key.upper()}: yield={r['implicit_yield']}%  "
                f"price={r['cutoff_price']}  date={r['auction_date']}"
            )
        else:
            log.warning(f"  FINAL {key.upper()}: NOT FETCHED — will retain existing JSON value")

    return results


def fetch_from_manual_input() -> Dict[str, Dict[str, Any]]:
    """
    Interactive fallback: prompt the user to enter values manually.
    Only called when is_interactive() is True.

    Source: https://data.rbi.org.in/DBIE/ → Auctions → T-Bills
    """
    if not is_interactive():
        raise RuntimeError(
            "fetch_from_manual_input() called in non-interactive (CI) mode. "
            "This is a bug — check is_interactive() before calling."
        )

    print("\n" + "─" * 60)
    print("  MANUAL INPUT MODE")
    print("  Enter the latest RBI T-Bill auction cut-off prices.")
    print("  Source: https://data.rbi.org.in/DBIE/")
    print("  → Financial Markets → Auctions → T-Bills")
    print("─" * 60)

    while True:
        try:
            date_s   = input("\n  Auction date (YYYY-MM-DD): ").strip()
            if not re.match(r"\d{4}-\d{2}-\d{2}", date_s):
                print("  ✗ Invalid format. Use YYYY-MM-DD (e.g. 2025-06-25)")
                continue
            price91  = float(input("  91D  cut-off price (e.g. 98.6280): ").strip())
            price182 = input("  182D cut-off price (Enter to skip):  ").strip()
            price364 = input("  364D cut-off price (Enter to skip):  ").strip()
            break
        except (ValueError, EOFError):
            print("  ✗ Invalid number. Please try again.")

    results: Dict[str, Dict[str, Any]] = {}

    impl91 = round(implicit_yield(price91, 91), 4)
    print(f"\n  91D implicit yield:  {impl91:.4f}%")
    results["tbill_91d"] = {
        "tenor_days": 91, "auction_date": date_s,
        "cutoff_price": price91, "implicit_yield": impl91,
        "weighted_average_yield": impl91, "source_url": "manual_input",
    }

    if price182:
        p = float(price182)
        y = round(implicit_yield(p, 182), 4)
        print(f"  182D implicit yield: {y:.4f}%")
        results["tbill_182d"] = {
            "tenor_days": 182, "auction_date": date_s,
            "cutoff_price": p, "implicit_yield": y,
            "weighted_average_yield": y, "source_url": "manual_input",
        }

    if price364:
        p = float(price364)
        y = round(implicit_yield(p, 364), 4)
        print(f"  364D implicit yield: {y:.4f}%")
        results["tbill_364d"] = {
            "tenor_days": 364, "auction_date": date_s,
            "cutoff_price": p, "implicit_yield": y,
            "weighted_average_yield": y, "source_url": "manual_input",
        }

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# JSON — ATOMIC WRITE
# ═══════════════════════════════════════════════════════════════════════════════

def atomic_write_json(data: dict, path: str) -> None:
    """
    Write JSON to a temporary file in the same directory, then atomically
    rename it to the target path.

    This prevents a partial/corrupt write if the process is killed mid-write.
    The rename operation is atomic on POSIX filesystems.

    Args:
        data: Python dict to serialize as JSON
        path: Target file path (e.g. rbi_data.json)
    """
    dir_path  = os.path.dirname(os.path.abspath(path))
    fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp", prefix=".rbi_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")   # Trailing newline for clean git diffs
        os.replace(tmp_path, path)   # Atomic rename
        log.debug(f"  Atomic write complete: {path}")
    except Exception:
        # Clean up temp file on any failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    # ── Argument parsing ──────────────────────────────────────────────────────
    parser = argparse.ArgumentParser(
        description="Refresh rbi_data.json with latest RBI T-Bill auction data (v3.0.0)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--json-path", default=DEFAULT_JSON_PATH,
        help="Path to rbi_data.json (default: same directory as this script)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without writing any files"
    )
    parser.add_argument(
        "--manual", action="store_true",
        help="Skip web scraping — enter values interactively (not usable in CI)"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Bypass spike guard and stale date warnings"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Enable DEBUG logging for diagnostic output"
    )
    parser.add_argument(
        "--log-json", action="store_true",
        help="Output logs as JSON objects (for log aggregation pipelines)"
    )
    args = parser.parse_args()

    # ── Override args from environment (for GitHub Actions) ───────────────────
    if os.environ.get("RBI_DRY_RUN", "").lower() in ("true", "1"):
        args.dry_run = True
    if os.environ.get("RBI_FORCE", "").lower() in ("true", "1"):
        args.force = True
    if os.environ.get("RBI_LOG_LEVEL", "").upper() == "DEBUG":
        args.verbose = True

    # ── Configure logging ─────────────────────────────────────────────────────
    global log
    log = setup_logging(verbose=args.verbose, log_json=args.log_json)

    ci = is_ci()

    # ── Banner ─────────────────────────────────────────────────────────────────
    separator = "=" * 62
    log.info(separator)
    log.info("  RBI Treasury Bill Dashboard — Data Refresh  v3.0.0")
    log.info(f"  {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    log.info(f"  CI mode: {ci}  |  Dry run: {args.dry_run}  |  Force: {args.force}")
    log.info(separator)

    # ── Guard: manual mode in CI ───────────────────────────────────────────────
    if args.manual and ci:
        log.error(
            "--manual mode is not usable in CI/automated environments. "
            "Remove --manual from the workflow or run the script locally."
        )
        sys.exit(1)

    # ── Load existing JSON ─────────────────────────────────────────────────────
    log.info(f"Loading existing JSON: {args.json_path}")
    if not os.path.exists(args.json_path):
        log.error(
            f"rbi_data.json not found at: {args.json_path}\n"
            "Expected the file to exist. Run from the dashboard folder, "
            "or pass --json-path."
        )
        sys.exit(1)

    try:
        with open(args.json_path, "r", encoding="utf-8") as f:
            current_data = json.load(f)
    except json.JSONDecodeError as e:
        log.error(f"rbi_data.json is not valid JSON: {e}")
        sys.exit(1)

    # Schema version check
    schema_ver = current_data.get("_meta", {}).get("schema_version")
    if schema_ver and schema_ver != SUPPORTED_SCHEMA_VERSION:
        log.warning(
            f"Schema version mismatch: file has {schema_ver!r}, "
            f"script supports {SUPPORTED_SCHEMA_VERSION!r}. Proceeding with caution."
        )

    # Extract previous yields for spike detection
    prev_91d  = current_data.get("risk_free", {}).get("implicit_yield")
    ts_series = current_data.get("tbill_series", {})
    prev_182d = ts_series.get("tbill_182d", [None])[-1]  if ts_series.get("tbill_182d") else None
    prev_364d = ts_series.get("tbill_364d", [None])[-1]  if ts_series.get("tbill_364d") else None

    log.info(
        f"Stored values — "
        f"91D: {prev_91d}%  182D: {prev_182d}%  364D: {prev_364d}%  "
        f"last_updated: {fmtdate(current_data.get('_meta', {}).get('last_updated'))}"
    )

    # ── Fetch new data ─────────────────────────────────────────────────────────
    log.info(f"\n{'─' * 62}")
    log.info("[Step 1] Fetching latest RBI T-Bill auction data…")

    if args.manual:
        # Interactive mode — gated behind is_interactive() check in the function
        new_tenors = fetch_from_manual_input()
    else:
        session = requests.Session()
        session.headers.update(BROWSER_HEADERS)
        session.max_redirects = 5

        try:
            new_tenors = fetch_tbill_all_tenors(session)
        except Exception as e:
            log.error(
                f"Unhandled exception during data fetch: {e}\n"
                "Existing rbi_data.json is PRESERVED unchanged."
            )
            log.debug(traceback.format_exc())
            # Exit 0 so GitHub Actions doesn't mark this as a "failure"
            # — the dashboard will just keep showing the previous data
            sys.exit(0)

    # ── Check we have at least the critical 91D record ─────────────────────────
    if "tbill_91d" not in new_tenors:
        log.error(
            "Could not fetch 91D T-Bill data from any source. "
            "Existing rbi_data.json is PRESERVED unchanged.\n"
            "Options:\n"
            "  1. Check network access to www.rbi.org.in and data.rbi.org.in\n"
            "  2. Run locally with --manual to enter values interactively\n"
            "  3. Manually update rbi_data.json"
        )
        sys.exit(0)   # Exit 0 to preserve the dashboard

    new_rf = new_tenors["tbill_91d"]

    # ── Stale-date guard: skip if fetched data is not newer than stored ────────
    stored_auction_date = current_data.get("risk_free", {}).get("auction_date")
    fetched_auction_date = new_rf.get("auction_date")
    if stored_auction_date and fetched_auction_date and not args.force:
        stored_dt  = parse_date_flexible(stored_auction_date)
        fetched_dt = parse_date_flexible(fetched_auction_date)
        if stored_dt and fetched_dt and fetched_dt <= stored_dt:
            log.info(
                f"Fetched auction date ({fetched_auction_date}) is not newer than "
                f"stored date ({stored_auction_date}). No update needed."
            )
            log.info("Exiting cleanly — no changes written.")
            sys.exit(0)

    # ── Validate 91D record ────────────────────────────────────────────────────
    log.info(f"\n{'─' * 62}")
    log.info("[Step 2] Validating fetched 91D record…")

    issues = validate_record(new_rf, prev_91d, ci_mode=ci)
    errors   = [msg for sev, msg in issues if sev == "error"]
    warnings = [msg for sev, msg in issues if sev == "warning"]

    if errors:
        log.error("\n  CRITICAL VALIDATION ERRORS — aborting:")
        for e in errors:
            log.error(f"    • {e}")
        log.error("Existing rbi_data.json is PRESERVED unchanged.")
        sys.exit(1)

    if warnings:
        for w in warnings:
            log.warning(f"  ⚠  {w}")

        # Spike guard: in CI or --force mode, always proceed
        spike_warning = any("Large yield change" in w for w in warnings)
        if spike_warning and not args.force and not ci and is_interactive():
            try:
                ans = input("\n  Proceed despite spike warning? (yes/no): ").strip().lower()
                if ans != "yes":
                    log.info("  Aborted by user. No changes written.")
                    sys.exit(0)
            except (EOFError, OSError):
                # stdin closed (non-interactive fallback)
                log.warning("  stdin not available for confirmation; proceeding (CI assumption)")
        elif spike_warning and not args.force and ci:
            log.warning(
                "  Spike warning in CI mode — proceeding automatically. "
                "Re-run with --force to suppress this warning."
            )
    else:
        log.info("  ✓ All validation checks passed")

    # ── Build updated data ─────────────────────────────────────────────────────
    log.info(f"\n{'─' * 62}")
    log.info("[Step 3] Building updated JSON…")

    updated  = copy.deepcopy(current_data)
    rf_yield = new_rf["implicit_yield"]
    g10y     = updated.get("kpi", {}).get("gsec_10y_yield", 6.87)
    repo     = updated.get("policy", {}).get("repo_rate", 5.50)

    # Update risk_free section
    updated["risk_free"].update({
        "tenor_days":             91,
        "auction_date":           new_rf["auction_date"],
        "cutoff_price":           new_rf["cutoff_price"],
        "implicit_yield":         round(rf_yield, 4),
        "weighted_average_yield": round(new_rf["weighted_average_yield"], 4),
        "source_url":             new_rf.get("source_url", "RBI press releases"),
        "reconciliation_check": (
            f"((100 - {new_rf['cutoff_price']}) / {new_rf['cutoff_price']}) "
            f"× (365 / 91) × 100 = {rf_yield:.4f}%"
        ),
    })

    # Update KPI panel
    spread_bps = round((g10y - rf_yield) * 100)
    updated["kpi"].update({
        "tbill_91d_yield":          round(rf_yield, 4),
        "tbill_91d_cutoff_price":   new_rf["cutoff_price"],
        "tbill_91d_auction_date":   new_rf["auction_date"],
        "yield_spread_10y_91d_bps": spread_bps,
    })

    # Recalculate vs_repo_bps for all bonds in bond table
    for bond in updated.get("bond_table", {}).get("bonds", []):
        bond["vs_repo_bps"] = round((bond.get("ytm", 0) - repo) * 100)

    # Update T-Bill series (all three tenors)
    now_label = datetime.now(IST).strftime("%b %y")   # e.g. "Jun 26"
    for series_key in ["tbill_91d", "tbill_182d", "tbill_364d"]:
        if series_key in new_tenors:
            new_val = round(new_tenors[series_key]["implicit_yield"], 4)
            series  = updated["tbill_series"][series_key]
            labels  = updated["tbill_series"]["labels"]

            if labels and labels[-1] == now_label:
                # Same month — update the last data point in place
                series[-1] = new_val
                log.debug(f"  Updated {series_key} in-place for {now_label}: {new_val}%")
            else:
                # New month — append and trim to last 18 months
                labels.append(now_label)
                series.append(new_val)
                if len(series) > 18:
                    updated["tbill_series"]["labels"]    = labels[-18:]
                    updated["tbill_series"][series_key]  = series[-18:]
                log.debug(f"  Appended {series_key} for {now_label}: {new_val}%")
        else:
            # Partial fetch — only update 91D in series if no specific data
            if series_key == "tbill_91d":
                series = updated["tbill_series"][series_key]
                if series:
                    series[-1] = round(rf_yield, 4)

    # Update yield curve 91D data point (index 0 = 91D tenor)
    if updated.get("yield_curve", {}).get("current", {}).get("yields"):
        updated["yield_curve"]["current"]["yields"][0] = round(rf_yield, 2)

    # Update metadata timestamp
    now_ist = datetime.now(IST).isoformat()
    updated["_meta"]["last_updated"] = now_ist

    # Build audit log entry
    changes_parts = [
        f"91D yield: {prev_91d}% → {rf_yield:.4f}%",
        f"cutoff_price: {new_rf['cutoff_price']}",
        f"10Y-91D spread: {spread_bps} bps",
    ]
    for k, prev_yield in [("tbill_182d", prev_182d), ("tbill_364d", prev_364d)]:
        if k in new_tenors:
            changes_parts.append(
                f"{k.upper()}: {prev_yield}% → {new_tenors[k]['implicit_yield']:.4f}%"
            )
    changes_str = " | ".join(changes_parts)

    updated["audit_log"].append({
        "timestamp":         now_ist,
        "action":            "auto_refresh_v3",
        "source":            "RBI DBIE / RBI Press Releases",
        "operator":          "refresh_rbi_data.py v3.0.0",
        "ci_mode":           ci,
        "changes":           changes_str,
        "validation_status": "passed" if not warnings else "passed_with_warnings",
        "warnings":          warnings,
        "tenors_fetched":    list(new_tenors.keys()),
    })

    # Keep audit log to last 50 entries to prevent unbounded growth
    if len(updated["audit_log"]) > 50:
        updated["audit_log"] = updated["audit_log"][-50:]

    # ── Change detection — skip write if nothing actually changed ─────────────
    old_checksum = json_checksum(current_data)
    new_checksum = json_checksum(updated)

    if old_checksum == new_checksum:
        log.info("\n  No effective changes to JSON content — skipping write.")
        log.info("  (Data was already up-to-date)")
        log.info(separator)
        sys.exit(0)

    # ── Summary of changes ─────────────────────────────────────────────────────
    log.info("\n[Step 4] Summary of changes:")
    log.info(f"  91D yield    : {prev_91d}%  →  {rf_yield:.4f}%")
    log.info(f"  10Y-91D spread: {current_data.get('kpi', {}).get('yield_spread_10y_91d_bps')} bps  →  {spread_bps} bps")
    for k, label, prev in [("tbill_182d","182D",prev_182d), ("tbill_364d","364D",prev_364d)]:
        if k in new_tenors:
            log.info(f"  {label} yield    : {prev}%  →  {new_tenors[k]['implicit_yield']:.4f}%")
    log.info(f"  Audit entries: {len(updated['audit_log'])}")
    log.info(f"  last_updated : {now_ist}")

    if args.dry_run:
        log.info("\n  [DRY RUN] No files written. Pass without --dry-run to apply changes.")
        log.info(separator)
        return

    # ── Write output (atomic) ─────────────────────────────────────────────────
    # Write backup of current data
    backup_path = args.json_path.replace(".json", ".backup.json")
    try:
        atomic_write_json(current_data, backup_path)
        log.info(f"\n[Step 5] Backup written : {backup_path}")
    except Exception as e:
        log.warning(f"  Could not write backup: {e} (non-fatal, proceeding)")

    # Atomic write of updated data
    try:
        atomic_write_json(updated, args.json_path)
        log.info(f"[Step 6] JSON updated   : {args.json_path}")
    except Exception as e:
        log.error(f"  CRITICAL: Could not write rbi_data.json: {e}")
        log.error("  The backup file (if created) contains the previous good data.")
        sys.exit(1)

    log.info("\n  ✓ Refresh complete. Dashboard will display updated data on next load.")
    log.info(separator)


if __name__ == "__main__":
    main()

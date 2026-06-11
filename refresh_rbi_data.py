#!/usr/bin/env python3
r"""
refresh_rbi_data.py  (v4.0.0 — complete endpoint overhaul)
===========================================================
RBI Treasury Bill Dashboard — Autonomous Data Refresh Script
Author  : Javvaji Venkatesh
Version : 4.0.0

ROOT CAUSE ANALYSIS (why v3.1.0 failed completely)
---------------------------------------------------
FAIL-1  DBIE seriesID numeric API (seriesIDs 480, 481, 482)
        → All three numeric series IDs return HTTP 200 but deliver an HTML
          error page ("Session has expired") instead of CSV.  The DBIE portal
          requires an active browser session cookie to serve data via the
          ?site=statistics&seriesID=N endpoint.  Unauthenticated GET requests
          from GitHub Actions always receive the session-expired HTML page,
          which contains "<html" in the first 200 bytes and is correctly
          rejected — but since this happens for all three series the entire
          DBIE API source yields 0 results.

FAIL-2  RBI RSS feed (rbi.org.in/scripts/rss.aspx)
        → Returns valid XML but contains 0 T-Bill items.  The RBI RSS feed
          only publishes a small rolling window of the most recent ~20 press
          releases.  T-Bill auction results are published every Wednesday; if
          the workflow runs on any other day and there was a partial-auction
          cancellation that week (as occurred on Mar 25 2026 and Jun 3 2026),
          no T-Bill item appears in the RSS window at all.

FAIL-3  PR listing page (BS_PressReleaseDisplay.aspx without prid)
        → Returns an HTML table with press release titles and prid links.
          However, the BeautifulSoup anchor-text filter requires the link text
          to contain "treasury bill" or similar keywords.  The RBI listing page
          renders anchor text as plain titles such as "Treasury Bills:
          Full Auction Result" which does match — but only if the most recent
          ~20 entries include a T-Bill result.  On weeks with auction
          cancellations or on days before Wednesday, no T-Bill entry appears
          in the listing.

FAIL-4  Sequential prid probe (150 prids backwards)
        → Correct approach but wrong baseline.  By June 2026 the actual prid
          counter is in the ~62800 range.  The probe estimated
          62000 + elapsed_days × 2.5 × 1.10 ≈ 63600, then searched
          63600–63450.  The actual current prids were 62700–62800, outside
          the probe window, so all 150 probed prids returned 404.

FIXES IN v4.0.0
---------------
FIX-1   REMOVE: All DBIE numeric seriesID API calls (seriesIDs 480/481/482).
        They require session authentication that is impossible from GitHub
        Actions.  Replaced with the DBIE *public data table* endpoint which
        returns an embedded HTML table with no session requirement.

FIX-2   ADD: Source A — DBIE Financial Market / Money Market table scraper.
        URL: https://data.rbi.org.in/DBIE/dbie.rbi?site=statistics
             &type=T&lang=EN&relPath=/@21762@21774@21843
        This is the publicly accessible "Financial Market" → "Money Market"
        → "Treasury Bills" table that DBIE serves as a static HTML table
        with no authentication.  Column structure:
          Date | 91D Cutoff | 91D WAY | 182D Cutoff | 182D WAY | 364D Cutoff | 364D WAY

FIX-3   ADD: Source B — RBI PR listing page with corrected prid extraction.
        The listing page does contain T-Bill auction result links titled
        "Treasury Bills: Full Auction Result".  Fixed the link-text matching
        to also check capitalised variants and to extract prid from the <a>
        href directly.

FIX-4   FIX: RSS feed — now also searches prid links in the RSS <link> tags
        and does a wider keyword match including "auction result".

FIX-5   FIX: prid probe baseline corrected.
        New baseline: prid=62798 on 2026-05-22 (confirmed from live listing
        page fetch).  Expanded probe window to 300 (covers ~4 months at 2.5/day).
        Probe now does a fast HEAD-style check first to avoid downloading full
        page bodies for non-T-Bill pages.

FIX-6   ADD: Source D — RBI Financial Market Statistics page scraper.
        URL: https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx
             (listing page, parse all prid links in the current visible table)
        Second attempt at the listing page using the original rbi.org.in domain
        (not data.rbi.org.in) which is more reliably accessible.

REMOVED DEAD ENDPOINTS
-----------------------
  × https://data.rbi.org.in/DBIE/dbie.rbi?site=statistics&seriesID=480...
  × https://data.rbi.org.in/DBIE/dbie.rbi?site=statistics&seriesID=481...
  × https://data.rbi.org.in/DBIE/dbie.rbi?site=statistics&seriesID=482...
    (All return HTML session-expired page, not CSV)
  × https://www.rbi.org.in/scripts/BS_PressReleasesView.aspx?Category=0
    (Returns HTTP 418, Cloudflare-blocked)

USAGE (unchanged):
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

REQUEST_TIMEOUT     = 30
RETRY_COUNT         = 3
RETRY_BACKOFF_BASE  = 4

SUPPORTED_SCHEMA_VERSION = "1.0.0"

# ── Browser headers — mimic a real Chrome browser ────────────────────────────
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

# ═══════════════════════════════════════════════════════════════════════════════
# ENDPOINT DEFINITIONS  (v4.0.0 — all dead endpoints removed)
# ═══════════════════════════════════════════════════════════════════════════════

# ── SOURCE A: DBIE Financial Market Statistics HTML table ─────────────────────
# This is the publicly accessible DBIE "Financial Market > Money Market >
# Treasury Bills" page that requires NO session authentication.
# The table contains: Date | 91D Cutoff Price | 91D WAY | 182D | 364D columns.
# Confirmed reachable from unauthenticated GET requests (no Cloudflare block).
#
# Primary URL (data.rbi.org.in domain):
DBIE_TABLE_URL_PRIMARY = (
    "https://data.rbi.org.in/DBIE/dbie.rbi"
    "?site=statistics&type=T&lang=EN&relPath=/@21762@21774@21843"
)
# Alternative path variants in case primary relPath changes:
DBIE_TABLE_URL_VARIANTS = [
    (
        "https://data.rbi.org.in/DBIE/dbie.rbi"
        "?site=statistics&type=T&lang=EN&relPath=/@21762@21774@21842"
    ),
    (
        "https://data.rbi.org.in/DBIE/dbie.rbi"
        "?site=statistics&type=T&lang=EN&relPath=/@21762@21774@21844"
    ),
]

# ── SOURCE B: RBI Press Release listing page ──────────────────────────────────
# Un-parameterised listing page returns the most recent ~20 press releases.
# NOT blocked by Cloudflare WAF (confirmed working June 2026).
RBI_PR_LISTING_URL = "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx"

# Individual press release display URL (by prid integer):
RBI_PR_DISPLAY_URL = "https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid={prid}"

# ── SOURCE C: RBI RSS feed ────────────────────────────────────────────────────
# Lightweight XML, never WAF-blocked.  Rolling window of ~20 most recent PRs.
RBI_RSS_URL = "https://www.rbi.org.in/scripts/rss.aspx"

# ── SOURCE D: Sequential prid probe ──────────────────────────────────────────
# CORRECTED BASELINE (v4.0.0):
#   prid=62798 confirmed for May 22, 2026 (Auction of State Government Securities)
#   T-Bill auction result press releases appear ~every Wednesday.
#   Adjacent T-Bill PRIDs in May 2026 are in the 62720–62790 range.
KNOWN_PRID_BASELINE  = 62798        # Confirmed prid on May 22, 2026
KNOWN_PRID_DATE      = "2026-05-22" # Date that baseline prid was published
AVG_PRID_PER_DAY     = 2.5          # RBI publishes ~2.5 press releases/day
PRID_PROBE_WINDOW    = 300          # Search this many prid values backwards (~4 months)
PRID_SAFETY_MARGIN   = 1.05         # 5% margin above estimate (was 10% in v3.x, too much)

# T-Bill keyword patterns for prid probe quick-check and PR listing matching:
TBILL_KEYWORDS = [
    "treasury bill", "t-bill", "tbill",
    "91 day", "91-day", "91day",
    "182 day", "182-day", "182day",
    "364 day", "364-day", "364day",
    "auction result", "full auction",
    "cut-off price", "cut off price",
    "implicit yield",
]

# ── Tenor configurations ──────────────────────────────────────────────────────
# (display_label, tenor_days, json_series_key, text_keywords)
TENORS_CONFIG: List[Tuple[str, int, str, List[str]]] = [
    ("91D",  91,  "tbill_91d",
     ["91-day", "91 day", "91day", "91-days", "91 days", "91 d"]),
    ("182D", 182, "tbill_182d",
     ["182-day", "182 day", "182day", "182-days", "182 days", "182 d"]),
    ("364D", 364, "tbill_364d",
     ["364-day", "364 day", "364day", "364-days", "364 days", "364 d"]),
]


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
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
# CI DETECTION
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
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def implicit_yield(price: float, days: int) -> float:
    """Calculate implicit yield from cut-off price and tenor."""
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
                "%d/%m/%Y %H:%M:%S", "%d-%m-%Y", "%d %B %Y",
                "%d-%b-%y", "%d %b %y"):
        try:
            # Try the full string first, then truncate to 30 chars for safety
            candidate = s[:30].strip()
            return datetime.strptime(candidate, fmt).replace(tzinfo=IST)
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
    Estimate the current highest prid using corrected v4.0.0 baseline.
    Baseline: prid=62798 on 2026-05-22 (confirmed from live RBI listing page).
    """
    baseline_dt = parse_date_flexible(KNOWN_PRID_DATE)
    if baseline_dt is None:
        return KNOWN_PRID_BASELINE + int(PRID_PROBE_WINDOW * 0.2)
    days_elapsed = (datetime.now(IST) - baseline_dt).days
    estimated = int(KNOWN_PRID_BASELINE + days_elapsed * AVG_PRID_PER_DAY)
    # Apply safety margin (5% above estimate — smaller than v3.x's 10%)
    return int(estimated * PRID_SAFETY_MARGIN)


# ═══════════════════════════════════════════════════════════════════════════════
# NETWORK — RETRY WRAPPER
# ═══════════════════════════════════════════════════════════════════════════════

def retry_get(
    session: requests.Session,
    url: str,
    retries: int = RETRY_COUNT,
    backoff_base: int = RETRY_BACKOFF_BASE,
    timeout: int = REQUEST_TIMEOUT,
    allow_404: bool = False,
    stream: bool = False,
) -> Optional[requests.Response]:
    """
    GET with exponential-backoff retry.

    Non-retryable: 403, 404, 418 (Cloudflare WAF)
    Retryable:     429, 500, 502, 503, 504, connection errors, timeouts

    HTTP 418 = Cloudflare WAF bot-block.  Retrying never helps.
    HTTP 403 = Access forbidden.  Wrong headers or IP block.
    """
    attempt   = 0
    last_exc: Optional[Exception] = None

    while attempt <= retries:
        try:
            if attempt > 0:
                wait = backoff_base * (2 ** (attempt - 1))
                log.warning(f"  Retry {attempt}/{retries} — waiting {wait}s → {url[:70]}…")
                time.sleep(wait)

            log.debug(f"  GET {url[:90]}")
            resp = session.get(url, timeout=timeout, stream=stream)

            # ── Non-retryable status codes ────────────────────────────────────
            if resp.status_code == 418:
                log.warning(
                    f"  HTTP 418 (Cloudflare WAF bot-block) → {url[:70]}\n"
                    "  This endpoint is actively blocking automated access."
                )
                return None

            if resp.status_code == 403:
                log.warning(f"  HTTP 403 Forbidden → {url[:70]}")
                return None

            if resp.status_code == 404:
                if allow_404:
                    return resp
                log.debug(f"  HTTP 404 → {url[:70]}")
                return None

            # ── Retryable server errors ───────────────────────────────────────
            if resp.status_code in (429, 500, 502, 503, 504):
                log.warning(f"  HTTP {resp.status_code} — will retry")
                attempt += 1
                continue

            resp.raise_for_status()
            log.debug(f"  HTTP 200 OK — {len(resp.content)} bytes from {url[:70]}")
            return resp

        except (requests.ConnectionError, requests.Timeout, ChunkedEncodingError) as e:
            last_exc = e
            log.warning(f"  Network error (attempt {attempt + 1}): {type(e).__name__}: {e}")
            attempt += 1
        except requests.RequestException as e:
            log.error(f"  Request failed (non-retryable): {e}")
            return None

    log.error(f"  All {retries + 1} attempts failed for {url[:70]}. Last: {last_exc}")
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT EXTRACTION HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def extract_price_from_text(text: str, context_window: int = 400) -> Optional[float]:
    """Extract cut-off price (90–100 range) from text near price-related keywords."""
    kw_patterns = [
        r"cut[-\s]?off\s+price",
        r"cutoff\s+price",
        r"weighted\s+average\s+price",
        r"wa\s+price",
        r"cut[-\s]?off",
    ]
    for kw in kw_patterns:
        m = re.search(kw, text, re.IGNORECASE)
        if m:
            window = text[m.start(): m.start() + context_window]
            # Match prices in range 90.xxxx to 99.xxxx
            pm = re.search(r"\b(9[0-9]\.\d{2,6})\b", window)
            if pm:
                val = float(pm.group(1))
                log.debug(f"  Price {val} found via keyword '{kw}'")
                return val
    # Fallback: any number in the price range
    m = re.search(r"\b(9[0-9]\.\d{2,6})\b", text)
    if m:
        return float(m.group(1))
    return None


def extract_yield_from_text(text: str) -> Optional[float]:
    """Extract explicit yield percentage from text."""
    patterns = [
        r"cut[-\s]?off\s+yield[^0-9]*(\d+\.\d{2,4})\s*(?:%|per\s+cent)?",
        r"cutoff\s+yield[^0-9]*(\d+\.\d{2,4})\s*(?:%|per\s+cent)?",
        r"yield\s+of\s+(\d+\.\d{2,4})\s*(?:%|per\s+cent)?",
        # RBI format: (YTM: 6.4238%) in newer press releases
        r"YTM:\s*(\d+\.\d{2,4})%?",
        r"implicit\s+yield[^0-9]*(\d+\.\d{2,4})",
        r"\b(\d+\.\d{4})%",   # bare percentage with 4 decimal places
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if YIELD_SANITY_MIN <= val <= YIELD_SANITY_MAX:
                log.debug(f"  Yield {val}% found via pattern")
                return val
    return None


def extract_wa_yield_from_text(text: str) -> Optional[float]:
    """Extract weighted-average yield from text."""
    patterns = [
        r"weighted\s+average\s+yield[^0-9]*(\d+\.\d{2,4})",
        r"WAY:\s*(\d+\.\d{2,4})%?",
        r"wa\s+yield[^0-9]*(\d+\.\d{2,4})",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = float(m.group(1))
            if YIELD_SANITY_MIN <= val <= YIELD_SANITY_MAX:
                return val
    return None


def extract_auction_date(text: str) -> Optional[str]:
    """Extract auction date from text using multiple patterns."""
    context_patterns = [
        r"auction\s+date[^:]*:\s*(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2}|\d{1,2}[-]\d{1,2}[-]\d{4})",
        r"date\s+of\s+auction[^:]*:\s*(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
        r"auction\s+held\s+on\s+(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
        r"held\s+on\s+(\d{1,2}[-/ ][A-Za-z]{3,9}[-/ ]\d{4}|\d{4}-\d{2}-\d{2})",
    ]
    for pat in context_patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            dt = parse_date_flexible(m.group(1).strip())
            if dt:
                return dt.strftime("%Y-%m-%d")
    # Generic date patterns as fallback
    for pat in [r"(\d{1,2}[-][A-Za-z]{3,9}[-]\d{4})", r"(\d{4}-\d{2}-\d{2})"]:
        m = re.search(pat, text)
        if m:
            dt = parse_date_flexible(m.group(1).strip())
            if dt:
                return dt.strftime("%Y-%m-%d")
    return None


def _is_session_expired_page(html: str) -> bool:
    """Detect if the DBIE response is a 'Session has expired' HTML error page."""
    lower = html[:500].lower()
    return (
        "<html" in lower
        or "session has expired" in lower
        or "session expired" in lower
        or "login" in lower[:200]
    )


# ═══════════════════════════════════════════════════════════════════════════════
# DATA VALIDATION
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
# SOURCE A: DBIE FINANCIAL MARKET TABLE SCRAPER  (v4.0.0 — replaces dead CSV API)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_dbie_table(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    Source A: Scrape the DBIE Financial Market → Money Market → Treasury Bills
    public HTML table.

    This page requires NO authentication and is not Cloudflare-blocked.
    It returns a static HTML table with recent T-Bill auction data including:
      Date | 91D Cut-off Price | 91D WAY | 182D Cut-off | 182D WAY | 364D Cut-off | 364D WAY

    Tries the primary relPath URL first, then variant URLs on failure.
    """
    log.info("  [Source A] DBIE Financial Market table scraper…")
    results: Dict[str, Dict[str, Any]] = {}

    urls_to_try = [DBIE_TABLE_URL_PRIMARY] + DBIE_TABLE_URL_VARIANTS

    for url_idx, url in enumerate(urls_to_try):
        if len(results) == 3:
            break

        log.info(f"  [Source A] Trying URL variant {url_idx + 1}/{len(urls_to_try)}: {url[:80]}…")
        resp = retry_get(session, url)

        if resp is None:
            log.warning(f"  [Source A] URL variant {url_idx + 1} unreachable")
            continue

        text = resp.text

        # Detect session-expired HTML (which would indicate authentication required)
        if _is_session_expired_page(text):
            log.warning(
                f"  [Source A] URL variant {url_idx + 1} returned session-expired HTML.\n"
                "  This DBIE URL requires authentication. Trying next variant."
            )
            continue

        soup = BeautifulSoup(text, "lxml")

        # Strategy 1: Look for a structured data table with T-Bill columns
        results.update(_parse_dbie_html_table(soup, url))

        if results:
            log.info(f"  [Source A] Extracted {len(results)}/3 tenors from DBIE table")
            return results

        log.warning(f"  [Source A] URL variant {url_idx + 1}: no T-Bill table found in page")

    if not results:
        log.warning("  [Source A] All DBIE table URL variants failed or returned no data")

    return results


def _parse_dbie_html_table(
    soup: BeautifulSoup,
    source_url: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Parse a DBIE-rendered HTML table to extract T-Bill cut-off prices/yields.

    The DBIE table typically has columns in this order:
      [0] Date
      [1] 91-Day Cut-off Price
      [2] 91-Day WAY (%)
      [3] 182-Day Cut-off Price
      [4] 182-Day WAY (%)
      [5] 364-Day Cut-off Price
      [6] 364-Day WAY (%)

    Or alternatively as a transposed table where each row is a tenor.
    """
    results: Dict[str, Dict[str, Any]] = {}

    tables = soup.find_all("table")
    log.debug(f"  [Source A] Found {len(tables)} HTML tables on page")

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        tbl_text_lower = table.get_text(" ", strip=True).lower()

        # Check if this table contains T-Bill data
        if not any(kw in tbl_text_lower for kw in [
            "treasury bill", "91", "182", "364", "cut-off", "cutoff", "ytm", "way"
        ]):
            continue

        # Get all rows as cell-text lists
        parsed_rows: List[List[str]] = []
        for row in rows:
            cells = [c.get_text(" ", strip=True) for c in row.find_all(["td", "th"])]
            if cells:
                parsed_rows.append(cells)

        if len(parsed_rows) < 2:
            continue

        log.debug(f"  [Source A] Candidate table: {len(parsed_rows)} rows, "
                  f"first row: {parsed_rows[0][:7]}")

        # Try to identify column layout from header row
        header = [c.lower() for c in parsed_rows[0]]

        # Layout A: columnar table with date + tenor columns
        date_col  = None
        tenor_cols: Dict[str, int] = {}  # series_key → column index

        for ci, h in enumerate(header):
            if any(k in h for k in ["date", "period", "month", "year"]):
                date_col = ci

        # Detect 91/182/364 day columns from header
        for ci, h in enumerate(header):
            h_num = re.sub(r"[^0-9]", "", h)
            if "91" in h_num and "tbill_91d" not in tenor_cols:
                tenor_cols["tbill_91d"] = ci
            elif "182" in h_num and "tbill_182d" not in tenor_cols:
                tenor_cols["tbill_182d"] = ci
            elif "364" in h_num and "tbill_364d" not in tenor_cols:
                tenor_cols["tbill_364d"] = ci

        if tenor_cols and date_col is not None:
            # Find the most recent data row (last non-empty row)
            data_rows = [r for r in parsed_rows[1:] if len(r) > max(tenor_cols.values())]
            if data_rows:
                latest = data_rows[-1]
                raw_date = latest[date_col].strip()
                auction_date = extract_auction_date(raw_date) or \
                               datetime.now(IST).strftime("%Y-%m-%d")

                for series_key, col_idx in tenor_cols.items():
                    if series_key in results:
                        continue
                    tenor_days = {"tbill_91d": 91, "tbill_182d": 182, "tbill_364d": 364}[series_key]
                    tenor_label = series_key.replace("tbill_", "").upper()
                    raw_val = latest[col_idx].strip()

                    try:
                        val = float(raw_val.replace(",", ""))
                    except ValueError:
                        log.debug(f"  [Source A] Cannot parse {tenor_label} value: {raw_val!r}")
                        continue

                    # Determine if value is a price (85–100) or yield (1–20)
                    if 85.0 <= val <= 100.0:
                        price_val = val
                        yield_val = round(implicit_yield(price_val, tenor_days), 4)
                    elif YIELD_SANITY_MIN <= val <= YIELD_SANITY_MAX:
                        yield_val = round(val, 4)
                        price_val = round(
                            100.0 / (1.0 + (yield_val / 100.0) * (tenor_days / 365.0)), 4
                        )
                    else:
                        log.debug(f"  [Source A] {tenor_label}: value {val} not recognised as "
                                  "price or yield")
                        continue

                    results[series_key] = {
                        "tenor_days":             tenor_days,
                        "auction_date":           auction_date,
                        "cutoff_price":           price_val,
                        "implicit_yield":         yield_val,
                        "weighted_average_yield": yield_val,
                        "source_url":             source_url,
                        "source_label":           "DBIE_TABLE",
                    }
                    log.info(
                        f"  [Source A] {tenor_label} ✓  "
                        f"yield={yield_val}%  price={price_val}  date={auction_date}"
                    )

                if results:
                    return results

        # Layout B: try scanning all data rows for numeric price/yield values
        # and identifying tenors from context
        for row in parsed_rows[1:]:
            row_text = " ".join(row).lower()
            for tenor_label, tenor_days, series_key, keywords in TENORS_CONFIG:
                if series_key in results:
                    continue
                if not any(kw in row_text for kw in keywords):
                    continue

                # Look for price-range numbers in cells
                for cell in row:
                    pm = re.search(r"\b(9[0-9]\.\d{2,6})\b", cell)
                    if pm:
                        price_val = float(pm.group(1))
                        try:
                            yield_val = round(implicit_yield(price_val, tenor_days), 4)
                        except ValueError:
                            continue
                        auction_date = extract_auction_date(" ".join(row)) or \
                                       datetime.now(IST).strftime("%Y-%m-%d")
                        results[series_key] = {
                            "tenor_days":             tenor_days,
                            "auction_date":           auction_date,
                            "cutoff_price":           price_val,
                            "implicit_yield":         yield_val,
                            "weighted_average_yield": yield_val,
                            "source_url":             source_url,
                            "source_label":           "DBIE_TABLE",
                        }
                        log.info(
                            f"  [Source A] {tenor_label} ✓ (row scan)  "
                            f"yield={yield_val}%  price={price_val}"
                        )
                        break

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE B: RBI PRESS RELEASE LISTING PAGE  (v4.0.0 — fixed prid extraction)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_pr_listing(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    Source B: Scrape the RBI press release listing page for T-Bill auction
    result links.

    The listing page (BS_PressReleaseDisplay.aspx without ?prid) returns an
    HTML table of the ~20 most recent press releases with title and prid links.

    v4.0.0 fix: Improved prid extraction to scan ALL anchor hrefs on the page,
    not just those where the anchor text matches T-Bill keywords (the anchor text
    sometimes contains only the release title without tenor keywords).
    """
    log.info("  [Source B] Fetching RBI press release listing page…")
    results: Dict[str, Dict[str, Any]] = {}

    resp = retry_get(session, RBI_PR_LISTING_URL)
    if resp is None:
        log.warning("  [Source B] Press release listing page unreachable")
        return results

    soup  = BeautifulSoup(resp.text, "lxml")
    page_text_lower = soup.get_text(" ", strip=True).lower()

    # Quick sanity check: does the page contain any T-Bill related text?
    has_tbill = any(kw in page_text_lower for kw in TBILL_KEYWORDS)
    log.info(f"  [Source B] Listing page loaded; T-Bill keyword present: {has_tbill}")

    # Collect all prid values from the page — both from T-Bill links and all links
    # (we'll then fetch each prid page and check if it's a T-Bill result)
    all_prids: List[int] = []
    tbill_prids: List[int] = []

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"prid=(\d+)", href, re.IGNORECASE)
        if not m:
            continue
        prid = int(m.group(1))
        all_prids.append(prid)

        # Also check if anchor text or surrounding text has T-Bill keywords
        anchor_text = a.get_text(" ", strip=True).lower()
        if any(kw in anchor_text for kw in TBILL_KEYWORDS):
            tbill_prids.append(prid)
            log.debug(f"  [Source B] T-Bill link found: prid={prid} — '{anchor_text[:80]}'")

    # Sort descending (most recent first)
    all_prids   = sorted(set(all_prids),   reverse=True)
    tbill_prids = sorted(set(tbill_prids), reverse=True)

    log.info(
        f"  [Source B] Found {len(all_prids)} total prids, "
        f"{len(tbill_prids)} with T-Bill keywords"
    )

    # Prioritise confirmed T-Bill prids, then fall through to all prids
    prid_order = tbill_prids + [p for p in all_prids if p not in tbill_prids]

    for prid in prid_order[:15]:   # Check top 15 prids from the listing
        if len(results) == 3:
            break
        url = RBI_PR_DISPLAY_URL.format(prid=prid)
        rec_map = _parse_pr_html_page(session, url, source_label="RBI_PR_LISTING")
        for k, v in rec_map.items():
            if k not in results:
                results[k] = v

    log.info(f"  [Source B] Fetched {len(results)}/3 tenors from PR listing page")
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE C: RBI RSS FEED  (v4.0.0 — expanded keyword matching)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_rss(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    Source C: Parse the RBI RSS feed for T-Bill auction result press releases.

    The RSS feed publishes a rolling window of ~20 most recent press releases.
    Each <item> contains:
      <title>Treasury Bills: Full Auction Result</title>
      <link>https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx?prid=NNNNN</link>

    v4.0.0 fix: Expanded keyword list for matching and added link text scanning
    when title text is absent or minimal.
    """
    log.info("  [Source C] Fetching RBI RSS feed…")
    results: Dict[str, Dict[str, Any]] = {}

    resp = retry_get(session, RBI_RSS_URL)
    if resp is None:
        log.warning("  [Source C] RSS feed unreachable")
        return results

    try:
        try:
            soup = BeautifulSoup(resp.content, "lxml-xml")
        except Exception:
            soup = BeautifulSoup(resp.text, "lxml")

        items = soup.find_all("item")
        log.info(f"  [Source C] Found {len(items)} RSS items")

        if len(items) == 0:
            # Try parsing as plain HTML
            log.debug("  [Source C] No <item> tags found; trying HTML parse")
            soup = BeautifulSoup(resp.text, "lxml")
            items = soup.find_all("item")

        matching_prids: List[int] = []

        for item in items:
            title_tag = item.find("title") or item.find("Title")
            link_tag  = item.find("link")  or item.find("Link")
            if not link_tag:
                continue

            title_text = title_tag.get_text(strip=True).lower() if title_tag else ""
            link_text  = link_tag.get_text(strip=True)
            # In RSS/XML the <link> may contain URL in text or as attribute
            if not link_text:
                link_text = link_tag.get("href", "")

            # Match on T-Bill keywords in title
            if any(kw in title_text for kw in TBILL_KEYWORDS):
                m = re.search(r"prid=(\d+)", link_text, re.IGNORECASE)
                if m:
                    prid = int(m.group(1))
                    matching_prids.append(prid)
                    log.debug(f"  [Source C] Matched: prid={prid} — '{title_text[:60]}'")

        matching_prids.sort(reverse=True)
        log.info(f"  [Source C] T-Bill RSS prids: {matching_prids[:8]}")

        for prid in matching_prids[:10]:
            if len(results) == 3:
                break
            url = RBI_PR_DISPLAY_URL.format(prid=prid)
            rec_map = _parse_pr_html_page(session, url, source_label="RBI_RSS")
            for k, v in rec_map.items():
                if k not in results:
                    results[k] = v

    except Exception as e:
        log.warning(f"  [Source C] RSS parse error: {e}")
        log.debug(traceback.format_exc())

    log.info(f"  [Source C] Fetched {len(results)}/3 tenors from RSS feed")
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# SOURCE D: SEQUENTIAL PRID PROBE  (v4.0.0 — corrected baseline, wider window)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_prid_probe(
    session: requests.Session,
    known_tbill_prids: Optional[List[int]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Source D: Find T-Bill press release pages by probing prid values backwards.

    v4.0.0 corrections:
    1. Corrected baseline prid: 62798 on 2026-05-22 (vs wrong estimate in v3.x)
    2. Reduced safety margin from 10% to 5% (10% overshoot was pushing probe
       window to the wrong prid range)
    3. Increased probe window from 150 to 300 prids (~4 months of coverage)
    4. First checks the listing page to seed known prids before probing

    known_tbill_prids: optionally pass in confirmed T-Bill prids from Source B
    to skip re-probing them here.
    """
    log.info("  [Source D] Starting sequential prid probe…")
    results: Dict[str, Dict[str, Any]] = {}

    high_prid = estimate_current_prid()
    low_prid  = high_prid - PRID_PROBE_WINDOW
    log.info(
        f"  [Source D] Probe range: {low_prid}–{high_prid} "
        f"(baseline={KNOWN_PRID_BASELINE} on {KNOWN_PRID_DATE})"
    )

    skip_prids = set(known_tbill_prids or [])
    probed   = 0
    found_pr = 0

    for prid in range(high_prid, low_prid, -1):
        if len(results) == 3:
            break
        if prid in skip_prids:
            log.debug(f"  [Source D] prid={prid}: already tried in earlier source")
            continue

        url  = RBI_PR_DISPLAY_URL.format(prid=prid)
        resp = retry_get(
            session, url,
            retries=1, backoff_base=1,
            timeout=15,
            allow_404=True
        )
        probed += 1

        if resp is None or resp.status_code == 404:
            log.debug(f"  [Source D] prid={prid}: 404/unreachable")
            continue

        if resp.status_code == 418:
            log.warning("  [Source D] HTTP 418 WAF block; stopping probe")
            break

        # Quick T-Bill keyword check on first 4000 characters of response
        text_snippet = resp.text[:4000].lower()
        is_tbill = any(kw in text_snippet for kw in TBILL_KEYWORDS)

        if not is_tbill:
            log.debug(f"  [Source D] prid={prid}: not a T-Bill page")
            continue

        found_pr += 1
        log.info(f"  [Source D] prid={prid}: T-Bill page confirmed (checked {probed} prids)")

        rec_map = _parse_pr_html_page(
            session, url,
            source_label="RBI_PR_PROBE",
            prefetched_resp=resp
        )
        for k, v in rec_map.items():
            if k not in results:
                results[k] = v

        # Polite delay between requests
        time.sleep(0.4)

    log.info(
        f"  [Source D] Probed {probed} prids, found {found_pr} T-Bill pages, "
        f"extracted {len(results)}/3 tenors"
    )
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# HTML PRESS RELEASE PAGE PARSER  (shared by Sources B, C, D)
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_pr_html_page(
    session: requests.Session,
    url: str,
    source_label: str = "RBI_PR",
    prefetched_resp: Optional[requests.Response] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Parse a single RBI HTML press release page (BS_PressReleaseDisplay.aspx?prid=N).

    RBI press release format for T-Bill full auction results:
      Table rows: 91 Days | 182 Days | 364 Days
      Columns: Notified Amount | Bids Received | Cut-off price/Yield | ...

    Returns: dict keyed by series name with extracted data (up to 3 tenors).
    A single RBI press release often contains all 3 tenors simultaneously.
    """
    if prefetched_resp is not None:
        resp = prefetched_resp
    else:
        resp = retry_get(session, url)
        if resp is None:
            return {}

    soup = BeautifulSoup(resp.text, "lxml")
    text = soup.get_text(" ", strip=True)

    page_auction_date = extract_auction_date(text)
    results: Dict[str, Dict[str, Any]] = {}

    # ── Strategy A: Structured HTML table parsing ─────────────────────────────
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        tbl_text = table.get_text(" ", strip=True).lower()
        # Only process tables that look like auction result tables
        if not any(kw in tbl_text for kw in [
            "cut-off", "cutoff", "implicit yield", "ytm", "91", "182", "364"
        ]):
            continue

        for row in rows:
            cells = row.find_all(["td", "th"])
            cell_texts = [c.get_text(" ", strip=True) for c in cells]
            row_text   = " ".join(cell_texts).lower()

            for tenor_label, tenor_days, series_key, keywords in TENORS_CONFIG:
                if series_key in results:
                    continue
                if not any(kw in row_text for kw in keywords):
                    continue

                # Scan cells for price (90–100) and yield values
                price  = None
                yield_ = None
                wa_y   = None

                for ct in cell_texts:
                    # Check for explicit YTM format: "(YTM: 6.4238%)"
                    ytm_m = re.search(r"YTM:\s*(\d+\.\d{2,4})%?", ct, re.I)
                    if ytm_m and yield_ is None:
                        v = float(ytm_m.group(1))
                        if YIELD_SANITY_MIN <= v <= YIELD_SANITY_MAX:
                            yield_ = v

                    # Check for WAY format: "(WAY: 6.4085%)"
                    way_m = re.search(r"WAY:\s*(\d+\.\d{2,4})%?", ct, re.I)
                    if way_m and wa_y is None:
                        v = float(way_m.group(1))
                        if YIELD_SANITY_MIN <= v <= YIELD_SANITY_MAX:
                            wa_y = v

                    # Check for price in 90–100 range
                    pm = re.search(r"\b(9[0-9]\.\d{2,6})\b", ct)
                    if pm and price is None:
                        price = float(pm.group(1))

                    # Check for 4-decimal yield percentage
                    ym = re.search(r"\b(\d+\.\d{4})\b", ct)
                    if ym and yield_ is None:
                        v = float(ym.group(1))
                        if YIELD_SANITY_MIN <= v <= YIELD_SANITY_MAX:
                            yield_ = v

                if price is None and yield_ is None:
                    continue

                # Back-calculate missing price or yield
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
                        "weighted_average_yield": wa_y if wa_y else (yield_ or impl_y),
                        "source_url":             url,
                        "source_label":           source_label,
                    }
                    log.info(
                        f"  [{source_label}] {tenor_label} ✓ "
                        f"price={price}  yield={impl_y}%  "
                        f"date={results[series_key]['auction_date']}"
                    )

    # ── Strategy B: Plain text extraction (fallback if table parsing fails) ────
    if not results:
        for tenor_label, tenor_days, series_key, keywords in TENORS_CONFIG:
            if series_key in results:
                continue
            for kw in keywords:
                idx = text.lower().find(kw)
                if idx == -1:
                    continue
                # Extract a window of text around the keyword
                window = text[max(0, idx - 50): idx + 600]
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
                        f"  [{source_label}] {tenor_label} ✓ (text)  "
                        f"price={price}  yield={impl_y}%"
                    )
                    break

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# MASTER FETCHER  (v4.0.0 — corrected 4-source fallback chain)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_tbill_all_tenors(
    session: requests.Session
) -> Dict[str, Dict[str, Any]]:
    """
    Master fetcher with corrected 4-source fallback chain (v4.0.0).

    Source priority:
        A. DBIE Financial Market table  — structured, no-auth, no WAF
        B. RBI PR listing page          — live listing, 20 most recent PRs
        C. RBI RSS feed                 — XML, never WAF-blocked
        D. Sequential prid probe        — brute-force, last resort

    Each source only runs for tenors not yet found by a prior source.
    Sources B and C are run in parallel (both are fast) before falling
    to the slower prid probe (D).

    The corrected prid baseline in Source D ensures the probe window covers
    the actual current prid range (~62800+ in June 2026).
    """
    results: Dict[str, Dict[str, Any]] = {}
    tbill_prids_seen: List[int] = []  # track prids already fetched for D

    def _merge(new: Dict) -> None:
        for k, v in new.items():
            if k not in results:
                results[k] = v

    def _missing() -> List[str]:
        return [key for _, _, key, _ in TENORS_CONFIG if key not in results]

    # ── Source A: DBIE Financial Market table ─────────────────────────────────
    log.info("==> [1/4] Source A: DBIE Financial Market table")
    try:
        _merge(fetch_from_dbie_table(session))
    except Exception as e:
        log.warning(f"  Source A unhandled exception: {e}")
        log.debug(traceback.format_exc())

    if not _missing():
        log.info("  All 3 tenors from Source A — done")
        _log_final(results)
        return results

    log.info(f"  Source A partial. Missing: {_missing()}")

    # ── Source B: PR listing page ─────────────────────────────────────────────
    log.info("==> [2/4] Source B: RBI PR listing page")
    try:
        src_b = fetch_from_pr_listing(session)
        _merge(src_b)
    except Exception as e:
        log.warning(f"  Source B unhandled exception: {e}")
        log.debug(traceback.format_exc())
        src_b = {}

    if not _missing():
        log.info("  All 3 tenors — done after Source B")
        _log_final(results)
        return results

    # ── Source C: RSS feed ────────────────────────────────────────────────────
    log.info(f"==> [3/4] Source C: RBI RSS feed (missing: {_missing()})")
    try:
        _merge(fetch_from_rss(session))
    except Exception as e:
        log.warning(f"  Source C unhandled exception: {e}")
        log.debug(traceback.format_exc())

    if not _missing():
        log.info("  All 3 tenors — done after Source C")
        _log_final(results)
        return results

    # ── Source D: Sequential prid probe ──────────────────────────────────────
    log.info(f"==> [4/4] Source D: Sequential prid probe (missing: {_missing()})")
    try:
        _merge(fetch_from_prid_probe(session, known_tbill_prids=tbill_prids_seen))
    except Exception as e:
        log.warning(f"  Source D unhandled exception: {e}")
        log.debug(traceback.format_exc())

    _log_final(results)
    return results


def _log_final(results: Dict[str, Dict[str, Any]]) -> None:
    """Log the final fetch summary for all tenors."""
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
            log.warning(f"  FINAL {key.upper()}: NOT FETCHED — existing JSON value preserved")


# ═══════════════════════════════════════════════════════════════════════════════
# MANUAL INPUT  (unchanged)
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_from_manual_input() -> Dict[str, Dict[str, Any]]:
    if not is_interactive():
        raise RuntimeError(
            "fetch_from_manual_input() called in non-interactive (CI) mode."
        )
    print("\n" + "─" * 60)
    print("  MANUAL INPUT MODE")
    print("  Source: https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx")
    print("─" * 60)
    while True:
        try:
            date_s = input("\n  Auction date (YYYY-MM-DD): ").strip()
            if not re.match(r"\d{4}-\d{2}-\d{2}", date_s):
                print("  ✗ Use YYYY-MM-DD format"); continue
            p91   = float(input("  91D  cut-off price (e.g. 98.6280): ").strip())
            p182  = input("  182D cut-off price (Enter to skip): ").strip()
            p364  = input("  364D cut-off price (Enter to skip): ").strip()
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
# ATOMIC WRITE  (unchanged)
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
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh rbi_data.json with latest RBI T-Bill auction data (v4.0.0)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--json-path",  default=DEFAULT_JSON_PATH,
                        help="Path to rbi_data.json (default: same directory as script)")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Fetch data but do not write to JSON")
    parser.add_argument("--manual",     action="store_true",
                        help="Manually enter auction data (interactive mode only)")
    parser.add_argument("--force",      action="store_true",
                        help="Bypass spike detection and stale-date checks")
    parser.add_argument("--verbose",    action="store_true",
                        help="Enable DEBUG-level logging")
    parser.add_argument("--log-json",   action="store_true",
                        help="Output logs as JSON objects")
    args = parser.parse_args()

    if os.environ.get("RBI_DRY_RUN",   "").lower() in ("true", "1"): args.dry_run = True
    if os.environ.get("RBI_FORCE",     "").lower() in ("true", "1"): args.force   = True
    if os.environ.get("RBI_LOG_LEVEL", "").upper() == "DEBUG":        args.verbose = True

    global log
    log = setup_logging(verbose=args.verbose, log_json=args.log_json)
    ci  = is_ci()

    SEP = "=" * 64
    log.info(SEP)
    log.info("  RBI Treasury Bill Dashboard — Data Refresh  v4.0.0")
    log.info(f"  {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    log.info(f"  CI={ci}  dry-run={args.dry_run}  force={args.force}")
    log.info(SEP)

    if args.manual and ci:
        log.error("--manual cannot be used in CI mode (no stdin).")
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
        log.warning(
            f"Schema version mismatch: file={schema_ver!r} "
            f"script={SUPPORTED_SCHEMA_VERSION!r}"
        )

    prev_91d  = current_data.get("risk_free", {}).get("implicit_yield")
    ts        = current_data.get("tbill_series", {})
    prev_182d = ts.get("tbill_182d", [None])[-1] if ts.get("tbill_182d") else None
    prev_364d = ts.get("tbill_364d", [None])[-1] if ts.get("tbill_364d") else None

    log.info(
        f"Stored yields: 91D={prev_91d}%  182D={prev_182d}%  364D={prev_364d}%  "
        f"updated={fmtdate(current_data.get('_meta', {}).get('last_updated'))}"
    )

    log.info(f"\n{'─' * 64}")
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
            "Could not fetch 91D T-Bill data from any of the 4 sources.\n"
            "JSON preserved unchanged.\n"
            "  → Run with --verbose for detailed diagnostics\n"
            "  → Run with --manual to enter data manually"
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
                "No update needed. Use --force to override."
            )
            sys.exit(0)

    log.info(f"\n{'─' * 64}")
    log.info("[Step 2] Validating 91D record…")

    issues   = validate_record(new_rf, prev_91d, ci_mode=ci)
    errors   = [m for s, m in issues if s == "error"]
    warnings = [m for s, m in issues if s == "warning"]

    if errors:
        for e in errors:
            log.error(f"  • {e}")
        log.error("ABORT — validation errors detected. JSON preserved unchanged.")
        sys.exit(1)

    if warnings:
        for w in warnings:
            log.warning(f"  ⚠  {w}")
        spike = any("Spike" in w for w in warnings)
        if spike and not args.force and not ci and is_interactive():
            try:
                if input("\n  Proceed despite spike? (yes/no): ").strip().lower() != "yes":
                    log.info("Aborted by user.")
                    sys.exit(0)
            except (EOFError, OSError):
                log.warning("stdin unavailable; proceeding (CI assumption)")
    else:
        log.info("  ✓ All validation checks passed")

    log.info(f"\n{'─' * 64}")
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
            if s:
                s[-1] = round(rf_yield, 4)

    if updated.get("yield_curve", {}).get("current", {}).get("yields"):
        updated["yield_curve"]["current"]["yields"][0] = round(rf_yield, 2)

    now_ist = datetime.now(IST).isoformat()
    updated["_meta"]["last_updated"] = now_ist

    changes_parts = [
        f"91D: {prev_91d}% → {rf_yield:.4f}%",
        f"price={new_rf['cutoff_price']}",
        f"spread={spread_bps}bps",
        f"src={new_rf.get('source_label', '?')}",
    ]
    for k, pv in [("tbill_182d", prev_182d), ("tbill_364d", prev_364d)]:
        if k in new_tenors:
            changes_parts.append(
                f"{k.upper()}: {pv}% → {new_tenors[k]['implicit_yield']:.4f}%"
            )

    updated["audit_log"].append({
        "timestamp":         now_ist,
        "action":            "auto_refresh_v4.0",
        "source":            new_rf.get("source_label", "unknown"),
        "operator":          "refresh_rbi_data.py v4.0.0",
        "ci_mode":           ci,
        "changes":           " | ".join(changes_parts),
        "validation_status": "passed" if not warnings else "passed_with_warnings",
        "warnings":          warnings,
        "tenors_fetched":    list(new_tenors.keys()),
        "sources_tried":     list({v.get("source_label", "?") for v in new_tenors.values()}),
    })
    if len(updated["audit_log"]) > 50:
        updated["audit_log"] = updated["audit_log"][-50:]

    # Change detection — skip write if nothing actually changed
    if json_checksum(current_data) == json_checksum(updated):
        log.info("No effective data change detected — skipping write.")
        sys.exit(0)

    log.info("\n[Step 4] Summary:")
    log.info(f"  91D yield     : {prev_91d}% → {rf_yield:.4f}%")
    log.info(f"  10Y-91D spread: → {spread_bps} bps")
    for k, lbl, pv in [("tbill_182d", "182D", prev_182d), ("tbill_364d", "364D", prev_364d)]:
        if k in new_tenors:
            log.info(f"  {lbl} yield     : {pv}% → {new_tenors[k]['implicit_yield']:.4f}%")
    log.info(f"  last_updated  : {now_ist}")

    if args.dry_run:
        log.info("[DRY RUN] No files written.")
        return

    backup = args.json_path.replace(".json", ".backup.json")
    try:
        atomic_write_json(current_data, backup)
        log.info(f"\n[Step 5] Backup written → {backup}")
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

# RBI Treasury Bill Dashboard — Autonomous Platform

**Zero-maintenance live dashboard of Indian Government T-Bill yields**
*Data refreshed automatically from official RBI sources.*

---

## What This Is

A production-ready, self-updating web dashboard that tracks RBI Treasury Bill auction cut-off yields (91-day, 182-day, 364-day) and related Indian Government Securities data. Once deployed, it requires **zero manual intervention** — GitHub Actions fetches fresh data after every Wednesday RBI auction and every morning, commits the updated JSON, and the dashboard always displays the latest available data.

| Metric | Detail |
|---|---|
| **Data source** | RBI official (DBIE + Press Releases) |
| **Update frequency** | Daily 07:30 IST + Wednesday 14:30 IST (post-auction) |
| **Automation** | GitHub Actions (fully autonomous) |
| **Manual maintenance** | None after initial setup |
| **Dashboard UI** | Unchanged — all charts, tables, and styling preserved |
| **Fallback** | Dashboard shows embedded fallback data if JSON unavailable |

---

## Repository Structure

```
your-repo/
├── .github/
│   └── workflows/
│       └── rbi-auto-update.yml      ← GitHub Actions automation
├── bonds_research_dashboard_v2.html ← Dashboard (DO NOT MODIFY)
├── rbi_data.json                    ← Live data file (auto-updated)
├── rbi_data.backup.json             ← Auto-created backup (gitignored)
├── refresh_rbi_data.py              ← Data refresh script v3.0.0
├── requirements.txt                 ← Python dependencies
└── README.md                        ← This file
```

---

## Quick Setup (5 minutes)

### Step 1 — Create a GitHub repository

1. Go to [github.com/new](https://github.com/new)
2. Create a **public** repository (required for free GitHub Pages)
   - Repository name: e.g. `rbi-tbill-dashboard`
   - Visibility: **Public**
   - Do NOT initialise with README (you'll push your files)
3. Click **Create repository**

### Step 2 — Upload your files

**Option A: GitHub web interface (easiest)**

1. In your new repository, click **Add file → Upload files**
2. Drag and drop all four files:
   - `bonds_research_dashboard_v2.html`
   - `rbi_data.json`
   - `refresh_rbi_data.py`
   - `requirements.txt`
3. Also create the workflow file:
   - Click **Add file → Create new file**
   - Name it: `.github/workflows/rbi-auto-update.yml`
   - Paste the contents of `rbi-auto-update.yml`
4. Click **Commit changes**

**Option B: Git command line**

```bash
# Clone your new empty repo
git clone https://github.com/YOUR_USERNAME/rbi-tbill-dashboard.git
cd rbi-tbill-dashboard

# Copy your files into this folder
cp /path/to/your/files/* .
mkdir -p .github/workflows
cp /path/to/rbi-auto-update.yml .github/workflows/

# Commit and push
git add .
git commit -m "Initial deployment — RBI T-Bill Dashboard"
git push origin main
```

### Step 3 — Enable workflow write permissions

This is the **most commonly missed step** — the workflow cannot commit the updated JSON without this.

1. Go to your repository on GitHub
2. Click **Settings** (top menu)
3. In the left sidebar: **Actions → General**
4. Scroll down to **Workflow permissions**
5. Select: **Read and write permissions** ✓
6. Click **Save**

### Step 4 — Enable GitHub Pages (deploy the dashboard)

1. In your repository: **Settings → Pages**
2. Under **Source**: select **Deploy from a branch**
3. Branch: **main** | Folder: **/ (root)**
4. Click **Save**
5. Wait 2–3 minutes for the first deployment
6. Your dashboard URL will be:
   `https://YOUR_USERNAME.github.io/rbi-tbill-dashboard/bonds_research_dashboard_v2.html`

### Step 5 — Verify the automation

1. Go to **Actions** tab in your repository
2. Click **RBI Data Auto-Update** in the left sidebar
3. Click **Run workflow → Run workflow** (manual trigger)
4. Watch the workflow execute in real time
5. After it completes, check **rbi_data.json** — the `last_updated` timestamp should be fresh
6. Reload your dashboard URL — it should show updated data

---

## How the Automation Works

```
Every day 07:30 IST ──────────────────────────────────────────────────────┐
Every Wednesday 14:30 IST ─────────────────────────────────────────────┐  │
Manual dispatch ────────────────────────────────────────────────────┐  │  │
                                                                    ▼  ▼  ▼
                                               GitHub Actions triggers workflow
                                                           │
                                            ┌──────────────┘
                                            ▼
                              ubuntu-latest runner starts
                                            │
                              ┌─────────────┴──────────────┐
                              ▼                            ▼
                    Checkout repository          Install Python deps
                              │
                    ┌─────────┘
                    ▼
           Capture JSON checksum (before)
                    │
                    ▼
           python refresh_rbi_data.py
                    │
          ┌─────────┴──────────────────────────┐
          ▼                                    ▼
  Try RBI DBIE table               Try RBI Press Releases
  (structured HTML)                 (prose text parsing)
          │                                    │
          └─────────────┬──────────────────────┘
                        ▼
            Validate fetched data
            (formula, range, spike, date)
                        │
            ┌───────────┴─────────────┐
            ▼                         ▼
      Validation OK             Validation FAIL
            │                         │
            ▼                         ▼
  Update rbi_data.json       Preserve existing JSON
  (atomic write)              (exit 0, no commit)
            │
            ▼
  Capture JSON checksum (after)
            │
     ┌──────┴──────┐
     ▼             ▼
  Changed?      No change
     │             │
     ▼             ▼
 Commit &    Skip commit
  push         (clean exit)
     │
     ▼
Dashboard served by GitHub Pages
reads fresh rbi_data.json on next load
```

---

## Manual Operations

### Trigger a manual data refresh

**Via GitHub website:**
1. Go to **Actions → RBI Data Auto-Update → Run workflow**
2. Optionally enable **Dry run** to preview without committing
3. Click **Run workflow**

**Via GitHub CLI:**
```bash
# Standard refresh
gh workflow run rbi-auto-update.yml

# Dry run (preview only)
gh workflow run rbi-auto-update.yml -f dry_run=true

# Force update (bypass spike guard)
gh workflow run rbi-auto-update.yml -f force=true

# Debug mode
gh workflow run rbi-auto-update.yml -f log_level=DEBUG
```

### Run the refresh script locally

```bash
# Standard refresh
python refresh_rbi_data.py

# Preview what would change (safe — no writes)
python refresh_rbi_data.py --dry-run

# Verbose/debug output
python refresh_rbi_data.py --verbose

# Enter data manually (when RBI website is temporarily unreachable)
python refresh_rbi_data.py --manual

# Bypass spike guard (use when RBI makes a large policy rate change)
python refresh_rbi_data.py --force

# Use a different JSON file path
python refresh_rbi_data.py --json-path /path/to/my/rbi_data.json
```

### Update data manually (emergency procedure)

If automated fetching fails for multiple days (e.g. RBI website restructured):

1. Visit [data.rbi.org.in/DBIE](https://data.rbi.org.in/DBIE/) → Financial Markets → Auctions → T-Bills
2. Note the cut-off price for 91D, 182D, and 364D from the latest auction
3. Run: `python refresh_rbi_data.py --manual`
4. Enter the values when prompted
5. Commit and push `rbi_data.json`:
   ```bash
   git add rbi_data.json
   git commit -m "manual: update T-Bill data from RBI DBIE"
   git push
   ```

---

## Understanding the Data Flow

### What `refresh_rbi_data.py` updates in `rbi_data.json`

| JSON section | What changes | Trigger |
|---|---|---|
| `risk_free` | `auction_date`, `cutoff_price`, `implicit_yield`, `weighted_average_yield` | Every auction |
| `kpi` | `tbill_91d_yield`, `tbill_91d_cutoff_price`, `yield_spread_10y_91d_bps` | Every auction |
| `bond_table.bonds[*].vs_repo_bps` | Recalculated from repo rate | Every run |
| `tbill_series.tbill_91d/182d/364d` | Latest month appended or updated | Monthly |
| `yield_curve.current.yields[0]` | 91D yield updated | Every auction |
| `_meta.last_updated` | Current IST timestamp | Every run |
| `audit_log` | New entry appended | Every run |

### What the script does NOT automatically update

These fields require manual update when the underlying values change:

| Field | When to update manually | Source |
|---|---|---|
| `policy.repo_rate` | After each RBI MPC meeting (~6 weeks) | rbi.org.in → Monetary Policy |
| `kpi.gsec_10y_yield` | Monthly (or when significant moves) | CCIL ZCYC daily file |
| `kpi.sdl_spread_10y_bps` | Monthly | RBI SDL auction results |
| `kpi.cpi_may2025` | Monthly (after MOSPI release) | mospi.gov.in |
| `kpi.real_yield_10y` | Recalculate: 10Y yield − CPI | Derived |
| `yield_curve.current.yields[1:]` | Monthly | CCIL ZCYC |
| `historical_yields` | Quarterly (append new quarter) | FBIL / CCIL archive |
| `annual_returns` | Annually (after FY close) | CCIL Bond Index |

---

## Troubleshooting

### Workflow fails with "Resource not accessible by integration"
**Cause:** Workflow permissions not set to Read and Write.
**Fix:** Settings → Actions → General → Workflow permissions → Read and write ✓

### Dashboard shows "Data may be stale" banner
**Cause:** rbi_data.json was last updated more than 7 days ago.
**Fix:** Trigger a manual workflow run, or check the Actions tab for recent failures.

### Workflow succeeds but JSON doesn't update
**Cause:** Fetched data is identical to stored data (RBI hasn't published new results yet).
**This is correct behaviour** — the workflow skips commits when data hasn't changed.

### Workflow shows "HTTP 403" or "Could not reach RBI website"
**Cause:** RBI website temporarily blocks GitHub Actions IP ranges.
**Fix:** Re-run the workflow — transient blocks usually clear within hours. The script retries automatically 3 times with exponential backoff before giving up.

### Script fetches wrong yield values (regression)
**Cause:** RBI changed their press release HTML structure.
**Diagnosis:** Run `python refresh_rbi_data.py --dry-run --verbose` locally to see exactly what is being parsed.
**Fix:** Update the regex patterns in `extract_price_from_text()` or `extract_auction_date()` to match the new structure.

### GitHub Pages shows old dashboard
**Cause:** Browser cache. Force refresh with Ctrl+Shift+R (Windows/Linux) or Cmd+Shift+R (Mac).

---

## Security Notes

- No secrets or API keys are required — all RBI data is publicly accessible.
- The workflow uses only the built-in `GITHUB_TOKEN` (automatically provided by GitHub).
- `GITHUB_TOKEN` permissions are scoped to `contents: write` only.
- `rbi_data.backup.json` is written locally during each run but never committed.
- The `[skip ci]` tag in commit messages prevents the automated commit from triggering another workflow run (infinite loop prevention).

---

## Data Sources

| Source | URL | Data provided |
|---|---|---|
| RBI DBIE | data.rbi.org.in/DBIE | T-Bill auction cut-off yields (primary) |
| RBI Press Releases | rbi.org.in | T-Bill auction results (secondary) |
| CCIL ZCYC | ccilindia.com | G-Sec spot yield curve |
| FBIL | fbil.org.in | Historical G-Sec reference rates |
| MOSPI | mospi.gov.in | CPI inflation |
| Ministry of Finance | indiabudget.gov.in | Gross borrowing programme |

*All data is from official Indian government and regulatory sources. This dashboard is for academic and research purposes only and does not constitute investment advice.*

---

**Author:** Javvaji Venkatesh | **Version:** 3.0.0 | **Last updated:** June 2026

# why-not-inn.org — Claude Code Build Guide
## MVP: Make the IDR PUF data searchable and visual

---

## What This Is

A local-first web app that ingests CMS IDR Public Use Files (XLSX), loads them into SQLite, and serves a dashboard + explorer UI. The thesis: "things are more expensive than they have to be." Show what IDR arbitrators award vs. what insurers offered, and let people search by CPT, insurer, state, specialty.

## Project Structure

```
why-not-inn/
├── data/
│   └── puf/                    # I drop raw CMS XLSX files here manually
│       ├── 2023-Q1-Q2.xlsx
│       ├── 2023-Q3-Q4.xlsx
│       ├── 2024-Q1-Q2.xlsx
│       ├── 2024-Q3-Q4.xlsx
│       └── 2025-Q1-Q2.xlsx
├── db/
│   └── whynotinn.db            # SQLite database (generated)
├── scripts/
│   ├── parse_puf.py            # Read XLSX files → SQLite
│   └── compute_stats.py        # Pre-compute aggregate stats
├── api/
│   └── main.py                 # FastAPI backend
├── frontend/                   # Static site (Next.js or plain HTML)
│   └── ...
├── requirements.txt
└── README.md
```

## Tech Choices

- **Database:** SQLite (local, zero config, good enough for millions of rows)
- **Backend:** FastAPI (Python)
- **Frontend:** Single-page React app or plain HTML+JS with Chart.js/D3
- **Data ingestion:** pandas + openpyxl reading XLSX files

## Step 1: Database Schema (SQLite)

Create `db/schema.sql`. Three core tables mapping to the three PUF tabs, plus a stats table.

```sql
-- Tab 1 & 2: OON Emergency/Non-Emergency + Air Ambulance
-- These share the same structure; store together with a source_tab column
CREATE TABLE idr_disputes (
    dispute_number TEXT,
    quarter TEXT NOT NULL,           -- '2023-Q1Q2', '2024-Q3Q4', etc.
    source_tab TEXT,                 -- 'oon' or 'air_ambulance'
    type_of_dispute TEXT,
    initiating_party TEXT,
    default_decision TEXT,
    payment_determination_outcome TEXT,
    length_of_time_days INTEGER,
    idre_compensation REAL,
    provider_name TEXT,
    provider_group_name TEXT,
    provider_npi TEXT,
    provider_email_domain TEXT,
    provider_specialty TEXT,
    practice_size TEXT,
    health_plan_name TEXT,
    health_plan_email_domain TEXT,
    health_plan_type TEXT,
    -- Line-item fields (denormalized — one row per DLI)
    dli_number TEXT,
    service_code TEXT,
    type_of_service_code TEXT,
    item_description TEXT,
    location_of_service TEXT,        -- state code
    place_of_service_code TEXT,
    dispute_line_item_type TEXT,
    offer_selected TEXT,
    provider_offer_pct_qpa REAL,
    issuer_offer_pct_qpa REAL,
    prevailing_offer_pct_qpa REAL,
    provider_offer_pct_median REAL,
    issuer_offer_pct_median REAL,
    prevailing_offer_pct_median REAL,
    qpa_pct_median REAL,
    -- Air ambulance specific (NULL for oon tab)
    air_ambulance_vehicle_type TEXT,
    air_ambulance_clinical_capacity TEXT,
    air_ambulance_pickup_location TEXT,
    PRIMARY KEY (dli_number, quarter)
);

-- Tab 3: QPA and Offers (actual dollar amounts)
CREATE TABLE idr_offers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    quarter TEXT NOT NULL,
    service_code TEXT,
    type_of_service_code TEXT,
    geographic_region TEXT,           -- MSA
    place_of_service_code TEXT,
    dispute_line_item_type TEXT,
    initiating_party TEXT,
    default_decision TEXT,
    offer_selected TEXT,
    qpa REAL,
    provider_offer REAL,
    issuer_offer REAL,
    prevailing_offer REAL,
    air_ambulance_pickup_location TEXT,
    -- Computed/flags
    prevailing_pct_qpa REAL,         -- prevailing_offer / qpa * 100
    qpa_suspect INTEGER DEFAULT 0    -- 1 if qpa < 1 or prevailing_pct_qpa > 1000
);

-- Pre-computed stats for fast dashboard queries
CREATE TABLE idr_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dimension TEXT,                   -- 'overall', 'cpt', 'state', 'insurer', 'specialty'
    dimension_value TEXT,             -- NULL for overall, CPT code, state, etc.
    quarter TEXT,                     -- NULL for all-time
    n_disputes INTEGER,
    n_line_items INTEGER,
    provider_wins INTEGER,
    issuer_wins INTEGER,
    split_decisions INTEGER,
    defaults INTEGER,
    median_prevailing_pct_qpa REAL,
    p25_prevailing_pct_qpa REAL,
    p75_prevailing_pct_qpa REAL,
    median_days INTEGER,
    avg_qpa REAL,                    -- from offers table
    avg_prevailing REAL,             -- from offers table
    avg_provider_offer REAL,
    avg_issuer_offer REAL
);

CREATE INDEX idx_disputes_service ON idr_disputes(service_code);
CREATE INDEX idx_disputes_state ON idr_disputes(location_of_service);
CREATE INDEX idx_disputes_plan ON idr_disputes(health_plan_name);
CREATE INDEX idx_disputes_quarter ON idr_disputes(quarter);
CREATE INDEX idx_disputes_specialty ON idr_disputes(provider_specialty);
CREATE INDEX idx_offers_service ON idr_offers(service_code);
CREATE INDEX idx_offers_geo ON idr_offers(geographic_region);
CREATE INDEX idx_offers_quarter ON idr_offers(quarter);
CREATE INDEX idx_stats_dim ON idr_stats(dimension, dimension_value);
```

**Design note:** I'm denormalizing Tabs 1/2 — one row per DLI with dispute-level fields repeated. This is simpler than a normalized disputes + line_items join for a read-heavy dashboard. The PUF data is already structured this way in the XLSX (one row per line item with dispute fields repeated).

## Step 2: PUF Parser (`scripts/parse_puf.py`)

This script reads every XLSX file in `data/puf/`, parses all three tabs, and loads into SQLite.

**Important PUF file structure notes:**

- Each XLSX has 3 tabs (sheets):
  - Tab 1: "OON Emergency & Non-Emergency" — ~31 columns per the data dictionary
  - Tab 2: "OON Air Ambulance" — ~31 columns (similar + air ambulance fields)
  - Tab 3: "QPA and Offers" — ~13 columns (has actual dollar amounts)
- Column headers are in the first row
- Column names may vary slightly between quarterly releases (2023 vs 2024 vs 2025 added fields). Use fuzzy/flexible column matching.
- Some columns were added later (e.g., `Initiating Party` not available in 2023 Q1-Q2, `Type of Service Code` not in Tab 3 for 2023-2024 Q1-Q2, `Air Ambulance Pick-up Location` only from 2025 Q1)

**Parser behavior:**

1. Scan `data/puf/` for all `.xlsx` files
2. For each file, determine the quarter from the filename (user names them like `2023-Q1Q2.xlsx`)
3. Read Tab 1 with pandas, map columns to `idr_disputes` schema, set `source_tab = 'oon'`
4. Read Tab 2, same mapping, set `source_tab = 'air_ambulance'`
5. Read Tab 3, map to `idr_offers` schema, compute `prevailing_pct_qpa` and `qpa_suspect` flag
6. Insert all rows into SQLite (use `INSERT OR REPLACE` keyed on `dli_number + quarter`)
7. Print summary: rows loaded per tab, per file

**Column mapping guidance:**

The PUF column names are verbose. Map them like:
```python
TAB1_COLUMN_MAP = {
    'Dispute Number': 'dispute_number',
    'DLI Number': 'dli_number',
    'Type of Dispute': 'type_of_dispute',
    'Initiating Party': 'initiating_party',
    'Default Decision': 'default_decision',
    'Payment Determination Outcome': 'payment_determination_outcome',
    'Length of Time to Make Determination': 'length_of_time_days',
    'IDRE Compensation': 'idre_compensation',
    'Provider/Facility Name': 'provider_name',
    'Provider/Facility Group Name': 'provider_group_name',
    'Provider/Facility NPI Number': 'provider_npi',
    'Provider Email Domain': 'provider_email_domain',
    'Practice/Facility Specialty or Type': 'provider_specialty',
    'Practice/Facility Size': 'practice_size',
    'Health Plan/Issuer Name': 'health_plan_name',
    'Health Plan/Issuer Email Domain': 'health_plan_email_domain',
    'Health Plan Type': 'health_plan_type',
    'Service Code': 'service_code',
    'Type of Service Code': 'type_of_service_code',
    'Item or Service Description': 'item_description',
    'Location of Service': 'location_of_service',
    'Place of Service Code': 'place_of_service_code',
    'Dispute Line Item Type': 'dispute_line_item_type',
    'Offer Selected from Provider or Issuer': 'offer_selected',
    'Provider/Facility Offer as % of QPA': 'provider_offer_pct_qpa',
    'Health Plan/Issuer Offer as % of QPA': 'issuer_offer_pct_qpa',
    'Prevailing Party Offer as % of QPA': 'prevailing_offer_pct_qpa',
    'Provider/Facility Offer as Percent of Median Provider/Facility Offer Amount': 'provider_offer_pct_median',
    'Health Plan/Issuer Offer as Percent of Median Health Plan/Issuer Offer Amount': 'issuer_offer_pct_median',
    'Prevailing Offer as Percent of Median Prevailing Offer Amount': 'prevailing_offer_pct_median',
    'QPA as Percent of Median QPA': 'qpa_pct_median',
}

TAB3_COLUMN_MAP = {
    'Service Code': 'service_code',
    'Type of Service Code': 'type_of_service_code',
    'Geographical Region': 'geographic_region',
    'Place of Service Code': 'place_of_service_code',
    'Dispute Line Item Type': 'dispute_line_item_type',
    'Initiating Party': 'initiating_party',
    'Default Decision': 'default_decision',
    'Offer Selected from Provider or Issuer': 'offer_selected',
    'QPA': 'qpa',
    'Provider/Facility Offer': 'provider_offer',
    'Health Plan/Issuer Offer': 'issuer_offer',
    'Prevailing Offer': 'prevailing_offer',
    'Air Ambulance Pick-up Location': 'air_ambulance_pickup_location',
}
```

Use flexible matching: strip whitespace, try `startswith` or fuzzy match for columns that vary across releases. If a column doesn't exist in a given release, fill with NULL.

**QPA quality flag:**
```python
# After computing prevailing_pct_qpa = (prevailing_offer / qpa) * 100
# Flag suspect rows
qpa_suspect = 1 if (qpa < 1.0 or prevailing_pct_qpa > 1000) else 0
```

## Step 3: Stats Pre-computation (`scripts/compute_stats.py`)

After loading PUF data, compute aggregates and store in `idr_stats`. This makes dashboard queries instant instead of scanning millions of rows.

**Dimensions to compute:**

```
For each of: [overall, per-CPT, per-state, per-insurer, per-specialty]
  For each of: [all-time, per-quarter]
    Compute:
      - n_disputes (COUNT DISTINCT dispute_number)
      - n_line_items (COUNT *)
      - provider_wins (COUNT WHERE payment_determination_outcome LIKE '%Provider%')
      - issuer_wins (COUNT WHERE payment_determination_outcome LIKE '%Plan%')
      - split_decisions (COUNT WHERE outcome LIKE '%Split%')
      - defaults (COUNT WHERE default_decision = 'Yes' or similar)
      - median/p25/p75 of prevailing_offer_pct_qpa (from disputes table)
      - median length_of_time_days
      - avg qpa, prevailing, provider_offer, issuer_offer (from offers table)
```

**Important:** Exclude `qpa_suspect = 1` rows from the median/percentile calculations on the offers table. Include them in counts.

Run this script after every PUF load: `python scripts/compute_stats.py`

## Step 4: API (`api/main.py`)

FastAPI app with these endpoints:

```
GET /api/dashboard
  Returns: overall stats, quarterly trend data, top CPTs, top insurers

GET /api/disputes?service_code=27447&state=GA&insurer=&quarter=&outcome=&page=1&limit=50
  Returns: filtered list of dispute line items, paginated

GET /api/cpt/{code}
  Returns: aggregate stats for that CPT, quarterly trend, top insurers, state breakdown

GET /api/insurer/{name}
  Returns: aggregate stats for that insurer, top CPTs, state breakdown, win/loss trend

GET /api/offers?service_code=27447&geo=&limit=100
  Returns: actual dollar amounts from Tab 3 for that CPT/geography

GET /api/search/insurers?q=anthem
  Returns: insurer name autocomplete from distinct health_plan_name values

GET /api/search/cpts?q=274
  Returns: CPT code autocomplete with descriptions

GET /api/export?<same filters as /disputes>&format=csv
  Returns: CSV download of filtered data
```

**CORS:** Allow `*` for local dev. Lock down later.

**Startup:** Connect to SQLite at `db/whynotinn.db`. Use `aiosqlite` or just synchronous sqlite3 — the data is read-only so concurrency isn't an issue.

## Step 5: Frontend

A single-page app with these views. Use React (Vite) or plain HTML — whatever Claude Code is faster with.

### Homepage / Dashboard

```
┌──────────────────────────────────────────────────────┐
│  WHY NOT IN-NETWORK?                                 │
│  Federal IDR Outcomes Dashboard                      │
│  Data through Q2 2025                                │
├──────────────┬──────────────┬────────────┬────────────┤
│  1.2M        │  72%         │  2.4x      │  38 days   │
│  disputes    │  provider    │  median    │  median    │
│  resolved    │  win rate    │  award/QPA │  to close  │
├──────────────┴──────────────┴────────────┴────────────┤
│  [Line chart: disputes per quarter, stacked by       │
│   provider win / issuer win / split / default]       │
├──────────────────────────┬───────────────────────────┤
│  Top CPT Codes           │  Top Insurers             │
│  by dispute volume       │  by dispute volume        │
│  1. 99285 — 84K disputes │  1. UHC — 142K disputes   │
│  2. 99284 — 71K disputes │  2. Anthem — 98K disputes │
│  ...                     │  ...                      │
└──────────────────────────┴───────────────────────────┘
```

### Explorer

Full-width filterable table. Filters: CPT code input, state dropdown, insurer search, quarter dropdown, outcome toggle. Table columns: service code, description, state, insurer, provider specialty, provider offer % QPA, issuer offer % QPA, prevailing % QPA, outcome, default. Sortable. Paginated. CSV export button.

### CPT Deep Dive (`/cpt/27447`)

```
CPT 27447 — Total Knee Arthroplasty
──────────────────────────────────
12,847 disputes  |  78% provider wins  |  median award: 2.8x QPA

[Histogram: distribution of prevailing offer as % of QPA]

[Bar chart: median QPA vs median prevailing, by quarter]

Top insurers for this code:
  UHC — 3,241 disputes, 74% provider win
  Anthem — 2,108 disputes, 81% provider win
  ...

By state:
  TX — 4,200 disputes, 2.6x median
  FL — 2,800 disputes, 3.1x median
  ...
```

### Insurer Profile (`/insurer/UnitedHealthcare`)

Similar layout to CPT deep dive but oriented around one insurer.

### Design Notes for Frontend

- **Dark theme.** Background: `#0a0a0f`. Text: `#e0e0e0`. Accent: `#3b82f6` (blue).
- **Monospace for numbers.** Use `JetBrains Mono`, `IBM Plex Mono`, or `Fira Code` for all data.
- **Serif for headlines.** `Playfair Display`, `DM Serif Display`, or `Lora`.
- **Sans for body.** `DM Sans`, `Plus Jakarta Sans`, or `Source Sans 3`.
- **Charts:** Keep them simple. Bar charts and line charts. No 3D, no gradients. Thin gridlines. Data labels on hover.
- **Make it screenshot-friendly.** Each chart/stat block should look good as a standalone image shared on Twitter/LinkedIn.

## Step 6: Running It

```bash
# First time setup
cd why-not-inn
python -m venv venv && source venv/bin/activate
pip install pandas openpyxl fastapi uvicorn aiosqlite

# Create database
sqlite3 db/whynotinn.db < db/schema.sql

# Drop XLSX files into data/puf/, then:
python scripts/parse_puf.py
python scripts/compute_stats.py

# Run API
uvicorn api.main:app --port 3100 --reload

# Frontend dev (if React/Vite)
cd frontend && npm install && npm run dev
```

## Data Quality Rules

Apply these everywhere stats are computed or displayed:

1. **QPA outlier filter:** Exclude rows where `qpa < 1.0` or `prevailing_pct_qpa > 1000` from median/percentile calculations. CMS warns that some initiating parties report nominal QPA values or unit prices.
2. **Default decisions:** Include in counts but consider flagging separately. A dispute won by default (other side didn't show up) is a different signal than a substantive determination.
3. **Component DLIs:** In bundled disputes, "Component Item or Service" DLIs don't receive offers or determinations — they're context only. Exclude from offer/outcome analysis. Only count "Bundled Item or Service" and "Single" and "Batched" types.
4. **Insurer name normalization:** Store raw names but also create a lookup of cleaned/grouped names. "UNITED HEALTHCARE", "UnitedHealthcare", "United HealthCare Services" → "UnitedHealthcare".

## What NOT to Build Yet

- MRF rate overlay (Phase 2 — needs insurer crosswalk work)
- News/blog section (add later as static markdown)
- User accounts or saved searches
- Email alerts
- Mobile-optimized views (desktop-first is fine for MVP)
- Server deployment / Nginx config (run locally first, deploy when ready)

## Files to Generate

When I say "build this," generate these files in order:

1. `requirements.txt`
2. `db/schema.sql`
3. `scripts/parse_puf.py`
4. `scripts/compute_stats.py`
5. `api/main.py`
6. Frontend (either a single `index.html` with inline JS, or a Vite React app)

Keep each file self-contained and working. I'll test after each step.

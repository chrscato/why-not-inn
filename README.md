# Why Not In-Network?

> Local-first explorer for the federal IDR Public Use Files. The thesis: things
> are more expensive than they have to be — show what arbitrators award vs.
> what insurers offered, sliced by CPT, insurer, state, and specialty.

## What's in here

```
why-not-inn/
├── data/puf/                        # raw CMS XLSX/ZIP files (you provide)
├── db/
│   ├── schema.sql                   # SQLite schema
│   └── whynotinn.db                 # generated
├── scripts/
│   ├── parse_puf.py                 # XLSX/ZIP -> SQLite
│   └── compute_stats.py             # roll up aggregates into idr_stats
├── api/main.py                      # FastAPI read-only API + static mount
├── frontend/                        # plain HTML + Chart.js SPA
│   ├── index.html
│   ├── app.js
│   └── style.css
├── requirements.txt
└── README.md
```

## First-time setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Initialize the SQLite database from schema
sqlite3 db/whynotinn.db < db/schema.sql
```

Drop CMS PUF files into `data/puf/`. Both raw `.xlsx` and the `.zip` packages
that CMS releases (containing per-tab CSVs or a single XLSX) are supported.
The data dictionary (`federal-idr-puf-data-dictionary.xlsx`) is auto-skipped.

## Ingest

```bash
python scripts/parse_puf.py                       # ingest anything new
python scripts/parse_puf.py --force               # reload everything
python scripts/parse_puf.py --file 2023-q1.xlsx   # ingest one file
python scripts/compute_stats.py                   # refresh idr_stats
```

Ingest is idempotent (`ingest_log` tracks loaded files; disputes are upserted
on `(dli_number, quarter)`). Re-run `compute_stats.py` after any new ingest.

## Run the app

```bash
uvicorn api.main:app --port 3100 --reload
```

Then open <http://localhost:3100> — the API serves the static frontend
at `/` and the JSON API at `/api/*`.

### Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /api/health` | Row counts |
| `GET /api/dashboard` | Overall stats, quarterly trend, top CPTs/insurers/states/specialties |
| `GET /api/disputes?service_code=&state=&insurer=&quarter=&outcome=&specialty=&page=1&limit=50` | Filtered, paginated line items |
| `GET /api/cpt/{code}` | CPT detail: stats, quarterly trend, distribution histogram, top insurers/states |
| `GET /api/insurer/{name}` | Insurer detail: stats, quarterly win/loss, top CPTs/states |
| `GET /api/state/{code}` | State detail: stats + top CPTs/insurers |
| `GET /api/offers?service_code=&geo=&quarter=&limit=` | Actual dollar amounts (Tab 3) |
| `GET /api/search/insurers?q=` | Insurer autocomplete |
| `GET /api/search/cpts?q=` | CPT autocomplete (matches code or description) |
| `GET /api/search/states` | All states with line counts |
| `GET /api/search/specialties` | All specialties with line counts |
| `GET /api/quarters` | Distinct quarters |
| `GET /api/export?<filters>` | Streaming CSV of filtered disputes |

## Data quality rules

These are applied wherever stats are computed or displayed:

1. **QPA outlier filter.** Rows with `qpa < 1.0` (Tab 3) or
   `prevailing_offer_pct_qpa > 1000` (Tabs 1/2) are excluded from medians and
   percentile calculations. CMS warns some initiating parties report nominal
   QPA values or unit prices.
2. **Default decisions.** Counted, but tagged separately in the explorer.
3. **Component DLIs.** Bundled-dispute "Component Item or Service" line items
   don't receive offers and are excluded from outcome analysis.
4. **Insurer name normalization.** Raw `health_plan_name` is preserved; a
   normalized variant (`health_plan_name_normalized`) collapses common
   spellings (e.g. "UNITED HEALTHCARE", "UnitedHealthcare", "United HealthCare
   Services" -> "UnitedHealthcare"). All insurer-dimension stats use the
   normalized form.

## Notes on the PUF format

- Three tabs per file:
  `OON Emergency and Non-Emergency`, `OON Air Ambulance`, `QPA and Offers`.
  Sheet names vary slightly across releases (e.g. leading whitespace,
  `Air Ambulance` vs `OON Air Ambulance`) — the parser classifies by keyword.
- Column headers vary: `% of QPA` vs `Percent of QPA`, `Initiating Party`
  added in 2023 Q3, `Air Ambulance Pick-up Location` from 2025 Q1, etc.
  The parser uses normalized header matching with multiple aliases per column.
- Tabs 1/2 are denormalized — one row per dispute line item (DLI). The schema
  follows that shape (no separate disputes/line_items tables).

## Not yet implemented

- MRF rate overlay
- News/blog
- Saved searches / accounts
- Email alerts
- Mobile-optimized layout
- Server deployment

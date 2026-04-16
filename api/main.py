"""Why Not In-Network? — read-only FastAPI over the IDR PUF SQLite DB.

All endpoints are GET. Data is never mutated; we use synchronous sqlite3
behind the lock-friendly `aiosqlite`-style helper. Returns JSON, except
``/api/export`` which streams CSV.

Run:
    uvicorn api.main:app --port 3100 --reload
"""

from __future__ import annotations

import csv
import io
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "db" / "whynotinn.db"
FRONTEND_DIR = PROJECT_ROOT / "frontend"

app = FastAPI(
    title="Why Not In-Network?",
    description="Federal IDR Public Use File explorer.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

@contextmanager
def db():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def rows_to_dicts(rows) -> list[dict]:
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Filter assembly for the explorer / disputes endpoints
# ---------------------------------------------------------------------------

SUBSTANTIVE_FILTER = (
    "(d.dispute_line_item_type IS NULL "
    " OR d.dispute_line_item_type NOT LIKE '%Component%')"
)


def build_dispute_filters(
    service_code: Optional[str] = None,
    state: Optional[str] = None,
    insurer: Optional[str] = None,
    quarter: Optional[str] = None,
    outcome: Optional[str] = None,
    specialty: Optional[str] = None,
    initiating_party: Optional[str] = None,
    source_tab: Optional[str] = None,
    include_components: bool = False,
) -> tuple[str, list]:
    """Return (where_sql, params). The result starts with ' AND ' if non-empty."""
    where = []
    params: list = []
    if not include_components:
        where.append(SUBSTANTIVE_FILTER)
    if service_code:
        where.append("d.service_code = ?")
        params.append(service_code)
    if state:
        where.append("d.location_of_service = ?")
        params.append(state)
    if insurer:
        # Match either raw or normalized name to be permissive
        where.append("(d.health_plan_name_normalized = ? OR d.health_plan_name = ?)")
        params.extend([insurer, insurer])
    if quarter:
        where.append("d.quarter = ?")
        params.append(quarter)
    if outcome:
        # 'provider' / 'issuer' / 'split' / 'default'
        outcome_l = outcome.lower()
        if outcome_l == "provider":
            where.append("d.payment_determination_outcome LIKE '%Provider%'")
        elif outcome_l in ("issuer", "plan"):
            where.append("(d.payment_determination_outcome LIKE '%Plan%' OR d.payment_determination_outcome LIKE '%Issuer%')")
        elif outcome_l == "split":
            where.append("d.payment_determination_outcome LIKE '%Split%'")
        elif outcome_l == "default":
            where.append("(d.default_decision LIKE 'Yes%')")
    if specialty:
        where.append("d.provider_specialty = ?")
        params.append(specialty)
    if initiating_party:
        where.append("d.initiating_party = ?")
        params.append(initiating_party)
    if source_tab:
        where.append("d.source_tab = ?")
        params.append(source_tab)
    if not where:
        return "", []
    return "WHERE " + " AND ".join(where), params


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    with db() as conn:
        n = conn.execute("SELECT COUNT(*) FROM idr_disputes").fetchone()[0]
        m = conn.execute("SELECT COUNT(*) FROM idr_offers").fetchone()[0]
    return {"ok": True, "disputes": n, "offers": m}


@app.get("/api/dashboard")
def dashboard():
    """Overall stats, quarterly trend, top CPTs, top insurers."""
    with db() as conn:
        overall = conn.execute("""
            SELECT * FROM idr_stats
            WHERE dimension='overall' AND quarter IS NULL
        """).fetchone()
        overall = dict(overall) if overall else {}

        quarters_rows = conn.execute("""
            SELECT * FROM idr_stats
            WHERE dimension='overall' AND quarter IS NOT NULL
            ORDER BY quarter
        """).fetchall()
        quarters = rows_to_dicts(quarters_rows)

        top_cpts = conn.execute("""
            SELECT s.dimension_value AS service_code,
                   c.description,
                   s.n_line_items,
                   s.n_disputes,
                   s.provider_wins,
                   s.issuer_wins,
                   s.median_prevailing_pct_qpa
            FROM idr_stats s
            LEFT JOIN cpt_descriptions c ON c.service_code = s.dimension_value
            WHERE s.dimension='cpt' AND s.quarter IS NULL
            ORDER BY s.n_line_items DESC
            LIMIT 25
        """).fetchall()

        top_insurers = conn.execute("""
            SELECT dimension_value AS insurer,
                   n_line_items,
                   n_disputes,
                   provider_wins,
                   issuer_wins,
                   median_prevailing_pct_qpa
            FROM idr_stats
            WHERE dimension='insurer' AND quarter IS NULL
            ORDER BY n_line_items DESC
            LIMIT 25
        """).fetchall()

        top_states = conn.execute("""
            SELECT dimension_value AS state,
                   n_line_items,
                   n_disputes,
                   provider_wins,
                   issuer_wins,
                   median_prevailing_pct_qpa
            FROM idr_stats
            WHERE dimension='state' AND quarter IS NULL
            ORDER BY n_line_items DESC
            LIMIT 25
        """).fetchall()

        top_specialties = conn.execute("""
            SELECT dimension_value AS specialty,
                   n_line_items,
                   n_disputes,
                   provider_wins,
                   issuer_wins,
                   median_prevailing_pct_qpa
            FROM idr_stats
            WHERE dimension='specialty' AND quarter IS NULL
            ORDER BY n_line_items DESC
            LIMIT 25
        """).fetchall()

    return {
        "overall": overall,
        "by_quarter": quarters,
        "top_cpts": rows_to_dicts(top_cpts),
        "top_insurers": rows_to_dicts(top_insurers),
        "top_states": rows_to_dicts(top_states),
        "top_specialties": rows_to_dicts(top_specialties),
    }


@app.get("/api/disputes")
def disputes(
    service_code: Optional[str] = None,
    state: Optional[str] = None,
    insurer: Optional[str] = None,
    quarter: Optional[str] = None,
    outcome: Optional[str] = None,
    specialty: Optional[str] = None,
    initiating_party: Optional[str] = None,
    source_tab: Optional[str] = None,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=500),
    sort: str = Query("prevailing_offer_pct_qpa"),
    order: str = Query("desc"),
):
    where_sql, params = build_dispute_filters(
        service_code, state, insurer, quarter, outcome,
        specialty, initiating_party, source_tab,
    )
    sortable = {
        "prevailing_offer_pct_qpa", "provider_offer_pct_qpa",
        "issuer_offer_pct_qpa", "length_of_time_days",
        "service_code", "location_of_service",
        "health_plan_name_normalized", "quarter",
    }
    if sort not in sortable:
        sort = "prevailing_offer_pct_qpa"
    order = "DESC" if order.lower() == "desc" else "ASC"
    offset = (page - 1) * limit

    # CMS stores *_pct_qpa as multipliers (1.0 == 100%). Convert to
    # percentage units on the way out so the frontend can format uniformly.
    select_cols = """
        d.dispute_number, d.dli_number, d.quarter, d.source_tab,
        d.service_code, d.item_description, d.type_of_service_code,
        d.location_of_service, d.place_of_service_code,
        d.provider_specialty, d.provider_group_name, d.provider_name,
        d.health_plan_name, d.health_plan_name_normalized,
        d.initiating_party, d.payment_determination_outcome,
        d.default_decision, d.length_of_time_days,
        d.provider_offer_pct_qpa * 100.0   AS provider_offer_pct_qpa,
        d.issuer_offer_pct_qpa * 100.0     AS issuer_offer_pct_qpa,
        d.prevailing_offer_pct_qpa * 100.0 AS prevailing_offer_pct_qpa,
        d.dispute_line_item_type
    """

    with db() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM idr_disputes d {where_sql}", params
        ).fetchone()[0]
        rows = conn.execute(
            f"""
            SELECT {select_cols}
            FROM idr_disputes d
            {where_sql}
            ORDER BY {sort} {order} NULLS LAST
            LIMIT ? OFFSET ?
            """,
            params + [limit, offset],
        ).fetchall()
    return {
        "total": total,
        "page": page,
        "limit": limit,
        "rows": rows_to_dicts(rows),
    }


@app.get("/api/cpt/{code}")
def cpt(code: str):
    with db() as conn:
        # All-time stats
        overall = conn.execute("""
            SELECT * FROM idr_stats
            WHERE dimension='cpt' AND dimension_value = ? AND quarter IS NULL
        """, (code,)).fetchone()
        if overall is None:
            raise HTTPException(404, f"No data for service code {code}")
        overall = dict(overall)

        description_row = conn.execute(
            "SELECT description FROM cpt_descriptions WHERE service_code = ?",
            (code,),
        ).fetchone()
        description = description_row["description"] if description_row else None

        by_quarter = conn.execute("""
            SELECT * FROM idr_stats
            WHERE dimension='cpt' AND dimension_value = ? AND quarter IS NOT NULL
            ORDER BY quarter
        """, (code,)).fetchall()

        # Top insurers for this code
        top_insurers = conn.execute("""
            SELECT health_plan_name_normalized AS insurer,
                   COUNT(*) AS n,
                   SUM(CASE WHEN payment_determination_outcome LIKE '%Provider%' THEN 1 ELSE 0 END) AS provider_wins,
                   SUM(CASE WHEN payment_determination_outcome LIKE '%Plan%' OR payment_determination_outcome LIKE '%Issuer%' THEN 1 ELSE 0 END) AS issuer_wins
            FROM idr_disputes
            WHERE service_code = ?
              AND health_plan_name_normalized IS NOT NULL
              AND (dispute_line_item_type IS NULL OR dispute_line_item_type NOT LIKE '%Component%')
            GROUP BY health_plan_name_normalized
            ORDER BY n DESC
            LIMIT 15
        """, (code,)).fetchall()

        # Top states (mean %QPA in percentage units)
        top_states = conn.execute("""
            SELECT location_of_service AS state,
                   COUNT(*) AS n,
                   AVG(prevailing_offer_pct_qpa) * 100.0 AS mean_pct
            FROM idr_disputes
            WHERE service_code = ?
              AND location_of_service IS NOT NULL
              AND (dispute_line_item_type IS NULL OR dispute_line_item_type NOT LIKE '%Component%')
              AND prevailing_offer_pct_qpa <= 10
            GROUP BY location_of_service
            ORDER BY n DESC
            LIMIT 15
        """, (code,)).fetchall()

        # Histogram input is converted to percentage units; thresholds in source units.
        hist_rows = conn.execute("""
            SELECT prevailing_offer_pct_qpa * 100.0
            FROM idr_disputes
            WHERE service_code = ?
              AND prevailing_offer_pct_qpa IS NOT NULL
              AND prevailing_offer_pct_qpa <= 10
              AND (dispute_line_item_type IS NULL OR dispute_line_item_type NOT LIKE '%Component%')
        """, (code,)).fetchall()
        buckets = make_pct_histogram([r[0] for r in hist_rows])

    return {
        "service_code": code,
        "description": description,
        "overall": overall,
        "by_quarter": rows_to_dicts(by_quarter),
        "top_insurers": rows_to_dicts(top_insurers),
        "top_states": rows_to_dicts(top_states),
        "histogram": buckets,
    }


@app.get("/api/insurer/{name}")
def insurer(name: str):
    with db() as conn:
        overall = conn.execute("""
            SELECT * FROM idr_stats
            WHERE dimension='insurer' AND dimension_value = ? AND quarter IS NULL
        """, (name,)).fetchone()
        if overall is None:
            raise HTTPException(404, f"No data for insurer {name}")
        overall = dict(overall)

        by_quarter = conn.execute("""
            SELECT * FROM idr_stats
            WHERE dimension='insurer' AND dimension_value = ? AND quarter IS NOT NULL
            ORDER BY quarter
        """, (name,)).fetchall()

        top_cpts = conn.execute("""
            SELECT d.service_code,
                   c.description,
                   COUNT(*) AS n,
                   SUM(CASE WHEN d.payment_determination_outcome LIKE '%Provider%' THEN 1 ELSE 0 END) AS provider_wins
            FROM idr_disputes d
            LEFT JOIN cpt_descriptions c ON c.service_code = d.service_code
            WHERE d.health_plan_name_normalized = ?
              AND d.service_code IS NOT NULL
              AND (d.dispute_line_item_type IS NULL OR d.dispute_line_item_type NOT LIKE '%Component%')
            GROUP BY d.service_code
            ORDER BY n DESC
            LIMIT 25
        """, (name,)).fetchall()

        top_states = conn.execute("""
            SELECT location_of_service AS state, COUNT(*) AS n
            FROM idr_disputes
            WHERE health_plan_name_normalized = ?
              AND location_of_service IS NOT NULL
              AND (dispute_line_item_type IS NULL OR dispute_line_item_type NOT LIKE '%Component%')
            GROUP BY location_of_service
            ORDER BY n DESC
            LIMIT 25
        """, (name,)).fetchall()

    return {
        "insurer": name,
        "overall": overall,
        "by_quarter": rows_to_dicts(by_quarter),
        "top_cpts": rows_to_dicts(top_cpts),
        "top_states": rows_to_dicts(top_states),
    }


@app.get("/api/state/{code}")
def state_view(code: str):
    with db() as conn:
        overall = conn.execute("""
            SELECT * FROM idr_stats
            WHERE dimension='state' AND dimension_value = ? AND quarter IS NULL
        """, (code,)).fetchone()
        if overall is None:
            raise HTTPException(404, f"No data for state {code}")
        overall = dict(overall)

        top_cpts = conn.execute("""
            SELECT d.service_code, c.description, COUNT(*) AS n
            FROM idr_disputes d
            LEFT JOIN cpt_descriptions c ON c.service_code = d.service_code
            WHERE d.location_of_service = ?
              AND (d.dispute_line_item_type IS NULL OR d.dispute_line_item_type NOT LIKE '%Component%')
            GROUP BY d.service_code
            ORDER BY n DESC
            LIMIT 25
        """, (code,)).fetchall()

        top_insurers = conn.execute("""
            SELECT health_plan_name_normalized AS insurer, COUNT(*) AS n
            FROM idr_disputes
            WHERE location_of_service = ?
              AND health_plan_name_normalized IS NOT NULL
              AND (dispute_line_item_type IS NULL OR dispute_line_item_type NOT LIKE '%Component%')
            GROUP BY health_plan_name_normalized
            ORDER BY n DESC
            LIMIT 25
        """, (code,)).fetchall()

    return {
        "state": code,
        "overall": overall,
        "top_cpts": rows_to_dicts(top_cpts),
        "top_insurers": rows_to_dicts(top_insurers),
    }


@app.get("/api/offers")
def offers(
    service_code: Optional[str] = None,
    geo: Optional[str] = None,
    quarter: Optional[str] = None,
    limit: int = Query(200, ge=1, le=1000),
):
    where = ["qpa_suspect = 0"]
    params: list = []
    if service_code:
        where.append("service_code = ?")
        params.append(service_code)
    if geo:
        where.append("geographic_region = ?")
        params.append(geo)
    if quarter:
        where.append("quarter = ?")
        params.append(quarter)
    where_sql = "WHERE " + " AND ".join(where)
    with db() as conn:
        rows = conn.execute(f"""
            SELECT quarter, service_code, geographic_region, place_of_service_code,
                   dispute_line_item_type, initiating_party, default_decision,
                   offer_selected, qpa, provider_offer, issuer_offer,
                   prevailing_offer, prevailing_pct_qpa
            FROM idr_offers {where_sql}
            ORDER BY quarter DESC
            LIMIT ?
        """, params + [limit]).fetchall()

        # Summary statistics for this filter
        summary = conn.execute(f"""
            SELECT
                COUNT(*) AS n,
                AVG(qpa) AS avg_qpa,
                AVG(provider_offer) AS avg_provider_offer,
                AVG(issuer_offer) AS avg_issuer_offer,
                AVG(prevailing_offer) AS avg_prevailing,
                AVG(prevailing_pct_qpa) AS avg_prevailing_pct
            FROM idr_offers {where_sql}
        """, params).fetchone()
    return {
        "summary": dict(summary) if summary else {},
        "rows": rows_to_dicts(rows),
    }


@app.get("/api/search/insurers")
def search_insurers(q: str = Query(..., min_length=1)):
    like = f"%{q}%"
    with db() as conn:
        rows = conn.execute("""
            SELECT health_plan_name_normalized AS insurer, COUNT(*) AS n
            FROM idr_disputes
            WHERE health_plan_name_normalized IS NOT NULL
              AND (health_plan_name_normalized LIKE ?
                   OR health_plan_name LIKE ?)
            GROUP BY health_plan_name_normalized
            ORDER BY n DESC
            LIMIT 20
        """, (like, like)).fetchall()
    return {"results": rows_to_dicts(rows)}


@app.get("/api/search/cpts")
def search_cpts(q: str = Query(..., min_length=1)):
    like = f"{q}%"
    desc_like = f"%{q}%"
    with db() as conn:
        rows = conn.execute("""
            SELECT s.dimension_value AS service_code,
                   c.description,
                   s.n_line_items
            FROM idr_stats s
            LEFT JOIN cpt_descriptions c ON c.service_code = s.dimension_value
            WHERE s.dimension='cpt' AND s.quarter IS NULL
              AND (s.dimension_value LIKE ? OR c.description LIKE ?)
            ORDER BY s.n_line_items DESC
            LIMIT 20
        """, (like, desc_like)).fetchall()
    return {"results": rows_to_dicts(rows)}


@app.get("/api/search/states")
def search_states():
    with db() as conn:
        rows = conn.execute("""
            SELECT dimension_value AS state, n_line_items
            FROM idr_stats
            WHERE dimension='state' AND quarter IS NULL
            ORDER BY n_line_items DESC
        """).fetchall()
    return {"results": rows_to_dicts(rows)}


@app.get("/api/search/specialties")
def search_specialties():
    with db() as conn:
        rows = conn.execute("""
            SELECT dimension_value AS specialty, n_line_items
            FROM idr_stats
            WHERE dimension='specialty' AND quarter IS NULL
            ORDER BY n_line_items DESC
        """).fetchall()
    return {"results": rows_to_dicts(rows)}


@app.get("/api/quarters")
def quarters_list():
    with db() as conn:
        rows = conn.execute("""
            SELECT DISTINCT quarter FROM idr_disputes ORDER BY quarter
        """).fetchall()
    return {"results": [r[0] for r in rows]}


@app.get("/api/export")
def export(
    service_code: Optional[str] = None,
    state: Optional[str] = None,
    insurer: Optional[str] = None,
    quarter: Optional[str] = None,
    outcome: Optional[str] = None,
    specialty: Optional[str] = None,
    initiating_party: Optional[str] = None,
    source_tab: Optional[str] = None,
    limit: int = Query(50_000, ge=1, le=500_000),
):
    where_sql, params = build_dispute_filters(
        service_code, state, insurer, quarter, outcome,
        specialty, initiating_party, source_tab,
    )

    cols = [
        "dispute_number","dli_number","quarter","source_tab","service_code",
        "type_of_service_code","item_description","location_of_service",
        "place_of_service_code","provider_specialty","provider_group_name",
        "provider_name","health_plan_name","health_plan_name_normalized",
        "initiating_party","payment_determination_outcome","default_decision",
        "length_of_time_days","provider_offer_pct_qpa","issuer_offer_pct_qpa",
        "prevailing_offer_pct_qpa","dispute_line_item_type",
    ]
    pct_cols = {"provider_offer_pct_qpa", "issuer_offer_pct_qpa", "prevailing_offer_pct_qpa"}
    select_exprs = [
        f"d.{c} * 100.0" if c in pct_cols else f"d.{c}"
        for c in cols
    ]

    def stream():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(cols)
        yield buf.getvalue()
        with db() as conn:
            rows = conn.execute(
                f"SELECT {','.join(select_exprs)} FROM idr_disputes d {where_sql} LIMIT ?",
                params + [limit],
            )
            buf.seek(0); buf.truncate()
            for r in rows:
                writer.writerow(r)
                if buf.tell() > 64 * 1024:
                    yield buf.getvalue()
                    buf.seek(0); buf.truncate()
            if buf.tell():
                yield buf.getvalue()

    headers = {"Content-Disposition": 'attachment; filename="idr_disputes.csv"'}
    return StreamingResponse(stream(), media_type="text/csv", headers=headers)


# ---------------------------------------------------------------------------
# Histogram helper
# ---------------------------------------------------------------------------

def make_pct_histogram(values: list[float]) -> list[dict]:
    """Bucket prevailing-offer-as-%-of-QPA values for charting."""
    edges = [0, 25, 50, 75, 100, 125, 150, 200, 300, 500, 1001]
    labels = [
        "0-25%", "25-50%", "50-75%", "75-100%", "100-125%",
        "125-150%", "150-200%", "200-300%", "300-500%", "500%+",
    ]
    counts = [0] * (len(edges) - 1)
    for v in values:
        if v is None:
            continue
        for i in range(len(edges) - 1):
            if edges[i] <= v < edges[i + 1]:
                counts[i] += 1
                break
    return [{"bucket": labels[i], "count": counts[i]} for i in range(len(counts))]


# ---------------------------------------------------------------------------
# Static frontend mount
# ---------------------------------------------------------------------------

if FRONTEND_DIR.exists():
    app.mount("/", StaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")


@app.exception_handler(sqlite3.OperationalError)
async def db_error_handler(request, exc):
    return JSONResponse(status_code=500, content={"error": str(exc)})

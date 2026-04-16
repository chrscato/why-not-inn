"""Pre-compute aggregate stats into ``idr_stats``.

Run after every ``parse_puf.py`` ingest. Emits one row per
(dimension, dimension_value, quarter) with counts, win/loss splits, percentile
of prevailing_offer_pct_qpa, median time-to-close, and dollar averages from
``idr_offers`` (where applicable).

Performance approach: every dimension is computed with at most a handful of
SQL queries that aggregate or window over the full disputes/offers tables.
Per-dimension-value percentiles use ``NTILE(100)`` to bucket sorted values,
then ``MIN`` of the appropriate buckets — that's an approximation but it's
within a percent or two and runs in seconds instead of hours.

Data quality rules applied:
  - Component DLIs excluded from outcome counts and percentile inputs
  - prevailing_offer_pct_qpa percentiles exclude NULL and >1000 (suspect)
  - offers averages exclude qpa_suspect=1
"""

from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "db" / "whynotinn.db"

SUBSTANTIVE_FILTER = (
    "(dispute_line_item_type IS NULL "
    " OR dispute_line_item_type NOT LIKE '%Component%')"
)


# ---------------------------------------------------------------------------
# Per-dimension batched computation
# ---------------------------------------------------------------------------

DIMENSIONS = [
    # (label, dispute_column, min_count_for_inclusion, offer_column_or_None)
    ("cpt",       "service_code",                 50,  "service_code"),
    ("state",     "location_of_service",          1,   None),
    ("insurer",   "health_plan_name_normalized", 30,   None),
    ("specialty", "provider_specialty",          100,  None),
]


def fetch_count_stats(conn: sqlite3.Connection, dim_col: str | None,
                      quarter_filter: str | None,
                      min_count: int) -> dict[tuple, dict]:
    """Counts + win/loss/split/default per dimension value.

    Returns ``{(dim_value,): {...}}``. When ``dim_col`` is None,
    a single key ``(None,)`` is returned (overall).
    """
    select_dim = dim_col if dim_col else "NULL"
    group_by = f"GROUP BY {dim_col}" if dim_col else ""
    where_extras = []
    if dim_col:
        where_extras.append(f"{dim_col} IS NOT NULL AND {dim_col} != ''")
    if quarter_filter:
        where_extras.append(f"quarter = '{quarter_filter}'")
    where_extras.append(SUBSTANTIVE_FILTER)
    where = "WHERE " + " AND ".join(where_extras)

    having = f"HAVING COUNT(*) >= {min_count}" if dim_col else ""

    sql = f"""
        SELECT
            {select_dim} AS dim_value,
            COUNT(DISTINCT dispute_number) AS n_disputes,
            COUNT(*) AS n_line_items,
            SUM(CASE WHEN payment_determination_outcome LIKE '%Provider%' THEN 1 ELSE 0 END) AS provider_wins,
            SUM(CASE WHEN payment_determination_outcome LIKE '%Plan%'
                       OR payment_determination_outcome LIKE '%Issuer%' THEN 1 ELSE 0 END) AS issuer_wins,
            SUM(CASE WHEN payment_determination_outcome LIKE '%Split%' THEN 1 ELSE 0 END) AS split_decisions,
            SUM(CASE WHEN default_decision LIKE 'Yes%' OR default_decision = '1' THEN 1 ELSE 0 END) AS defaults
        FROM idr_disputes
        {where}
        {group_by}
        {having}
    """
    out: dict[tuple, dict] = {}
    for row in conn.execute(sql):
        v = row[0]
        out[(v,)] = {
            "n_disputes": row[1],
            "n_line_items": row[2],
            "provider_wins": row[3],
            "issuer_wins": row[4],
            "split_decisions": row[5],
            "defaults": row[6],
        }
    return out


def fetch_percentile_stats(conn: sqlite3.Connection, dim_col: str | None,
                           quarter_filter: str | None) -> dict[tuple, dict]:
    """Approximate p25/p50/p75 of prevailing offer (as % of QPA) via NTILE(100).

    Stored values are multipliers (1.0 == 100% of QPA per CMS convention).
    We multiply by 100 in the SELECT so downstream consumers see percentages.
    The WHERE excludes >1000% (i.e. >10x QPA) — CMS warns of nominal-QPA
    outliers in that range. NTILE(100) within each partition gives a fast
    approximation; for tiny partitions some buckets are NULL (acceptable).
    """
    dim_select = dim_col if dim_col else "NULL"
    partition = f"PARTITION BY {dim_col}" if dim_col else ""
    where_extras = [
        SUBSTANTIVE_FILTER,
        "prevailing_offer_pct_qpa IS NOT NULL",
        "prevailing_offer_pct_qpa <= 10",
    ]
    if dim_col:
        where_extras.append(f"{dim_col} IS NOT NULL AND {dim_col} != ''")
    if quarter_filter:
        where_extras.append(f"quarter = '{quarter_filter}'")
    where = "WHERE " + " AND ".join(where_extras)

    sql = f"""
        WITH ranked AS (
            SELECT
                {dim_select} AS dim_value,
                prevailing_offer_pct_qpa * 100.0 AS v,
                NTILE(100) OVER ({partition} ORDER BY prevailing_offer_pct_qpa) AS pct_bucket
            FROM idr_disputes
            {where}
        )
        SELECT
            dim_value,
            MIN(CASE WHEN pct_bucket = 26 THEN v END) AS p25,
            MIN(CASE WHEN pct_bucket = 51 THEN v END) AS p50,
            MIN(CASE WHEN pct_bucket = 76 THEN v END) AS p75
        FROM ranked
        GROUP BY dim_value
    """
    out: dict[tuple, dict] = {}
    for row in conn.execute(sql):
        out[(row[0],)] = {
            "p25_prevailing_pct_qpa": row[1],
            "median_prevailing_pct_qpa": row[2],
            "p75_prevailing_pct_qpa": row[3],
        }
    return out


def fetch_median_days(conn: sqlite3.Connection, dim_col: str | None,
                      quarter_filter: str | None) -> dict[tuple, int]:
    dim_select = dim_col if dim_col else "NULL"
    partition = f"PARTITION BY {dim_col}" if dim_col else ""
    where_extras = [SUBSTANTIVE_FILTER, "length_of_time_days IS NOT NULL"]
    if dim_col:
        where_extras.append(f"{dim_col} IS NOT NULL AND {dim_col} != ''")
    if quarter_filter:
        where_extras.append(f"quarter = '{quarter_filter}'")
    where = "WHERE " + " AND ".join(where_extras)
    sql = f"""
        WITH ranked AS (
            SELECT {dim_select} AS dim_value, length_of_time_days AS v,
                   NTILE(100) OVER ({partition} ORDER BY length_of_time_days) AS b
            FROM idr_disputes
            {where}
        )
        SELECT dim_value, MIN(CASE WHEN b = 51 THEN v END) FROM ranked GROUP BY dim_value
    """
    out: dict[tuple, int] = {}
    for row in conn.execute(sql):
        out[(row[0],)] = int(row[1]) if row[1] is not None else None
    return out


def fetch_offer_avgs(conn: sqlite3.Connection, dim_col: str | None,
                     quarter_filter: str | None) -> dict[tuple, dict]:
    dim_select = dim_col if dim_col else "NULL"
    group_by = f"GROUP BY {dim_col}" if dim_col else ""
    where_extras = ["qpa_suspect = 0"]
    if dim_col:
        where_extras.append(f"{dim_col} IS NOT NULL AND {dim_col} != ''")
    if quarter_filter:
        where_extras.append(f"quarter = '{quarter_filter}'")
    where = "WHERE " + " AND ".join(where_extras)
    sql = f"""
        SELECT {dim_select} AS dim_value,
               AVG(qpa) AS avg_qpa,
               AVG(prevailing_offer) AS avg_prevailing,
               AVG(provider_offer) AS avg_provider_offer,
               AVG(issuer_offer) AS avg_issuer_offer
        FROM idr_offers
        {where}
        {group_by}
    """
    out: dict[tuple, dict] = {}
    for row in conn.execute(sql):
        out[(row[0],)] = {
            "avg_qpa": row[1],
            "avg_prevailing": row[2],
            "avg_provider_offer": row[3],
            "avg_issuer_offer": row[4],
        }
    return out


def insert_rows(conn: sqlite3.Connection, dimension: str, quarter: str | None,
                count_stats: dict, pct_stats: dict, day_stats: dict,
                offer_stats: dict | None) -> int:
    """Merge dicts keyed by (dim_value,) and write to idr_stats."""
    if not count_stats:
        return 0
    rows = []
    for key, base in count_stats.items():
        merged = dict(base)
        merged.update(pct_stats.get(key, {}))
        merged["median_days"] = day_stats.get(key)
        avgs = offer_stats.get(key) if offer_stats is not None else None
        for k in ("avg_qpa", "avg_prevailing", "avg_provider_offer", "avg_issuer_offer"):
            merged[k] = avgs.get(k) if avgs else None
        merged["dimension"] = dimension
        merged["dimension_value"] = key[0]
        merged["quarter"] = quarter
        rows.append(merged)
    cols = [
        "dimension", "dimension_value", "quarter",
        "n_disputes", "n_line_items", "provider_wins", "issuer_wins",
        "split_decisions", "defaults",
        "median_prevailing_pct_qpa", "p25_prevailing_pct_qpa",
        "p75_prevailing_pct_qpa", "median_days",
        "avg_qpa", "avg_prevailing", "avg_provider_offer", "avg_issuer_offer",
    ]
    payload = [tuple(r.get(c) for c in cols) for r in rows]
    conn.executemany(
        f"INSERT INTO idr_stats ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})",
        payload,
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def list_quarters(conn: sqlite3.Connection) -> list[str]:
    return [r[0] for r in conn.execute(
        "SELECT DISTINCT quarter FROM idr_disputes ORDER BY quarter"
    )]


def main() -> int:
    if not DB_PATH.exists():
        print(f"DB missing: {DB_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA cache_size = -200000")  # 200 MB
    print("Clearing idr_stats ...", flush=True)
    conn.execute("DELETE FROM idr_stats")
    conn.commit()

    quarters = list_quarters(conn)
    print(f"Quarters: {quarters}", flush=True)

    def go(label: str, dim_col: str | None, dimension: str,
           min_count: int, offer_col: str | None, want_offers: bool,
           quarter_filter: str | None):
        t0 = time.time()
        counts = fetch_count_stats(conn, dim_col, quarter_filter, min_count)
        if not counts:
            print(f"  {label}: no rows", flush=True)
            return
        keep = set(counts.keys())
        pct = fetch_percentile_stats(conn, dim_col, quarter_filter)
        days = fetch_median_days(conn, dim_col, quarter_filter)
        if want_offers:
            offers = fetch_offer_avgs(conn, offer_col, quarter_filter)
        else:
            offers = None
        pct = {k: v for k, v in pct.items() if k in keep}
        days = {k: v for k, v in days.items() if k in keep}
        if offers is not None:
            offers = {k: v for k, v in offers.items() if k in keep}
        n = insert_rows(conn, dimension, quarter_filter, counts, pct, days, offers)
        print(f"  {label}: {n} rows in {time.time()-t0:.1f}s", flush=True)
        conn.commit()

    print("- overall", flush=True)
    go("overall all-time", None, "overall", 0, None, True, None)
    for q in quarters:
        go(f"overall {q}", None, "overall", 0, None, True, q)

    for dimension, dim_col, min_count, offer_col in DIMENSIONS:
        print(f"- {dimension}", flush=True)
        want = offer_col is not None
        go(f"{dimension} all-time", dim_col, dimension, min_count, offer_col, want, None)
        for q in quarters:
            go(f"{dimension} {q}", dim_col, dimension, min_count, offer_col, want, q)

    n = conn.execute("SELECT COUNT(*) FROM idr_stats").fetchone()[0]
    print(f"\nDone — idr_stats rows: {n:,}", flush=True)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

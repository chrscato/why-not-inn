"""Fetch NPPES/NPI Registry data for provider NPIs and persist a local cache.

The CMS IDR PUF exposes a mixed field, ``Practice/Facility Specialty or Type``.
For rows with a valid 10-digit provider NPI, we can use the NPPES registry to
recover provider taxonomy and build a stronger specialty normalization layer.

This script does three things:

1. Pull distinct valid NPIs from ``idr_disputes``.
2. Fetch registry results from ``https://npiregistry.cms.hhs.gov/api/``.
3. Store a raw cache, flattened taxonomy rows, and recommendation seeds that
   compare raw CMS specialty values with NPPES taxonomy descriptions.

Usage:
    python scripts/enrich_nppes.py --limit 100
    python scripts/enrich_nppes.py --npi 1245251222
    python scripts/enrich_nppes.py --refresh
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import random
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DB_PATH = PROJECT_ROOT / "db" / "whynotinn.db"
TARGET_DB_PATH = PROJECT_ROOT / "db" / "normalization.db"

NPPES_API_URL = "https://npiregistry.cms.hhs.gov/api/"
DEFAULT_SLEEP_SECONDS = 0.25
SCHEMA_RETRY_ATTEMPTS = 10
SCHEMA_RETRY_SLEEP_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_FAILURES_AFTER_HOURS = 24.0
DEFAULT_COMMIT_EVERY = 25

SUPPORTING_DDL = """
CREATE TABLE IF NOT EXISTS specialty_map (
    raw_value TEXT PRIMARY KEY,
    clean_value TEXT,
    specialty_kind TEXT,
    canonical_specialty TEXT,
    specialty_rollup TEXT,
    mapping_source TEXT,
    confidence REAL,
    status TEXT DEFAULT 'pending',
    notes TEXT
);

CREATE TABLE IF NOT EXISTS insurer_map (
    raw_value TEXT PRIMARY KEY,
    clean_value TEXT,
    entity_type TEXT,
    canonical_entity TEXT,
    parent_family TEXT,
    mapping_source TEXT,
    confidence REAL,
    status TEXT DEFAULT 'pending',
    notes TEXT
);

CREATE TABLE IF NOT EXISTS nppes_provider_cache (
    provider_npi TEXT PRIMARY KEY,
    enumeration_type TEXT,
    provider_status TEXT,
    organization_name TEXT,
    first_name TEXT,
    last_name TEXT,
    credential TEXT,
    primary_taxonomy_code TEXT,
    primary_taxonomy_desc TEXT,
    primary_taxonomy_classification TEXT,
    primary_taxonomy_specialization TEXT,
    primary_state TEXT,
    primary_postal_code TEXT,
    last_updated TEXT,
    fetched_at TEXT NOT NULL,
    raw_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nppes_provider_taxonomies (
    provider_npi TEXT NOT NULL,
    taxonomy_code TEXT NOT NULL,
    taxonomy_desc TEXT,
    taxonomy_classification TEXT,
    taxonomy_specialization TEXT,
    is_primary INTEGER DEFAULT 0,
    state TEXT,
    license TEXT,
    PRIMARY KEY (provider_npi, taxonomy_code, state, license)
);

CREATE TABLE IF NOT EXISTS provider_specialty_recommendations (
    provider_npi TEXT NOT NULL,
    raw_specialty TEXT NOT NULL,
    raw_specialty_count INTEGER NOT NULL,
    recommended_kind TEXT,
    recommended_canonical_specialty TEXT,
    recommended_rollup TEXT,
    recommendation_source TEXT,
    confidence REAL,
    rationale TEXT,
    PRIMARY KEY (provider_npi, raw_specialty)
);

CREATE TABLE IF NOT EXISTS nppes_fetch_log (
    provider_npi TEXT PRIMARY KEY,
    fetch_status TEXT NOT NULL,           -- success|missing|failed
    http_status INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    fetched_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_specialty_map_kind ON specialty_map(specialty_kind, canonical_specialty);
CREATE INDEX IF NOT EXISTS idx_insurer_map_parent ON insurer_map(parent_family, canonical_entity);
CREATE INDEX IF NOT EXISTS idx_nppes_primary_taxonomy ON nppes_provider_cache(primary_taxonomy_code);
CREATE INDEX IF NOT EXISTS idx_nppes_taxonomy_desc ON nppes_provider_taxonomies(taxonomy_desc);
CREATE INDEX IF NOT EXISTS idx_nppes_fetch_status ON nppes_fetch_log(fetch_status, fetched_at);
"""


def source_db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{SOURCE_DB_PATH}?mode=ro", uri=True, timeout=30.0)
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def target_db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(TARGET_DB_PATH, timeout=30.0)
    conn.execute("PRAGMA busy_timeout = 30000")
    ensure_supporting_tables(conn)
    return conn


def ensure_supporting_tables(conn: sqlite3.Connection) -> None:
    for attempt in range(1, SCHEMA_RETRY_ATTEMPTS + 1):
        try:
            conn.executescript(SUPPORTING_DDL)
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == SCHEMA_RETRY_ATTEMPTS:
                raise
            time.sleep(SCHEMA_RETRY_SLEEP_SECONDS)


def is_valid_npi(value: str | None) -> bool:
    return bool(value) and len(value) == 10 and value.isdigit()


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def fetch_json(url: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "why-not-inn-nppes-enrichment/0.1",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_nppes_record(
    npi: str,
    timeout: float,
    max_retries: int,
) -> tuple[str, dict | None, int | None, str | None, int]:
    params = urllib.parse.urlencode({"version": "2.1", "number": npi})
    url = f"{NPPES_API_URL}?{params}"
    last_error = None
    last_status = None
    for attempt in range(1, max_retries + 1):
        try:
            payload = fetch_json(url, timeout=timeout)
            results = payload.get("results") or []
            if not results:
                return "missing", None, 200, None, attempt
            return "success", payload, 200, None, attempt
        except urllib.error.HTTPError as exc:
            last_status = exc.code
            body = exc.read().decode("utf-8", errors="replace")
            last_error = f"HTTP {exc.code}: {body[:300]}"
            if exc.code == 404:
                return "missing", None, exc.code, None, attempt
            if exc.code == 429:
                retry_after = exc.headers.get("Retry-After")
                sleep_seconds = float(retry_after) if retry_after else (2 ** attempt) + random.random()
                time.sleep(sleep_seconds)
                continue
            if 500 <= exc.code < 600 and attempt < max_retries:
                time.sleep((2 ** attempt) + random.random())
                continue
            return "failed", None, exc.code, last_error, attempt
        except urllib.error.URLError as exc:
            last_error = str(exc)
            if attempt < max_retries:
                time.sleep((2 ** attempt) + random.random())
                continue
            return "failed", None, last_status, last_error, attempt
    return "failed", None, last_status, last_error, max_retries


def parse_primary_address(result: dict) -> tuple[str | None, str | None]:
    addresses = result.get("addresses") or []
    for addr in addresses:
        if addr.get("address_purpose") == "LOCATION":
            return addr.get("state"), addr.get("postal_code")
    if addresses:
        return addresses[0].get("state"), addresses[0].get("postal_code")
    return None, None


def pick_primary_taxonomy(taxonomies: list[dict]) -> dict | None:
    if not taxonomies:
        return None
    for tax in taxonomies:
        if str(tax.get("primary", "")).upper() == "Y":
            return tax
    return taxonomies[0]


def split_taxonomy_desc(desc: str | None) -> tuple[str | None, str | None]:
    if not desc:
        return None, None
    parts = [p.strip() for p in str(desc).split("|", 1)]
    if len(parts) == 2:
        return parts[0] or None, parts[1] or None
    return parts[0] or None, None


def specialty_kind_from_text(value: str | None) -> str:
    if value is None:
        return "unknown"
    s = value.strip().lower()
    if not s or s in {"nr", "unknown", "n/a", "na"}:
        return "unknown"
    if any(token in s for token in ("llc", "inc", "associates", "physicians", "medical group", "p.a.")):
        return "organization"
    if any(token in s for token in ("hospital", "facility", "center", "room", "services", "department")):
        return "facility_setting"
    return "clinical"


def rollup_from_taxonomy(desc: str | None) -> str | None:
    if not desc:
        return None
    s = desc.lower()
    if "emergency medicine" in s:
        return "Emergency Medicine"
    if "anesthes" in s:
        return "Anesthesiology"
    if "radiolog" in s:
        return "Radiology"
    if "patholog" in s:
        return "Pathology"
    if "neurolog" in s or "neurophysi" in s or "neuromonitor" in s:
        return "Neurology / Neurodiagnostics"
    if "surgery" in s:
        return "Surgery"
    return None


def upsert_cache(conn: sqlite3.Connection, payload: dict) -> None:
    results = payload.get("results") or []
    if not results:
        return

    result = results[0]
    npi = result.get("number")
    basic = result.get("basic") or {}
    taxonomies = result.get("taxonomies") or []
    primary_tax = pick_primary_taxonomy(taxonomies) or {}
    primary_state, primary_postal = parse_primary_address(result)
    primary_desc = primary_tax.get("desc")
    primary_classification, primary_specialization = split_taxonomy_desc(primary_desc)

    conn.execute(
        """
        INSERT OR REPLACE INTO nppes_provider_cache (
            provider_npi, enumeration_type, provider_status, organization_name,
            first_name, last_name, credential, primary_taxonomy_code,
            primary_taxonomy_desc, primary_taxonomy_classification,
            primary_taxonomy_specialization, primary_state, primary_postal_code,
            last_updated, fetched_at, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            npi,
            result.get("enumeration_type"),
            basic.get("status"),
            basic.get("organization_name"),
            basic.get("first_name"),
            basic.get("last_name"),
            basic.get("credential"),
            primary_tax.get("code"),
            primary_desc,
            primary_classification,
            primary_specialization,
            primary_state,
            primary_postal,
            basic.get("last_updated"),
            utc_now_iso(),
            json.dumps(payload, sort_keys=True),
        ),
    )

    conn.execute("DELETE FROM nppes_provider_taxonomies WHERE provider_npi = ?", (npi,))
    rows = []
    primary_code = primary_tax.get("code")
    for idx, tax in enumerate(taxonomies):
        desc = tax.get("desc")
        classification, specialization = split_taxonomy_desc(desc)
        is_primary = str(tax.get("primary", "")).upper() == "Y"
        if not is_primary and len(taxonomies) == 1 and idx == 0:
            is_primary = True
        if not is_primary and primary_code and tax.get("code") == primary_code:
            is_primary = True
        rows.append(
            (
                npi,
                tax.get("code"),
                desc,
                classification,
                specialization,
                1 if is_primary else 0,
                tax.get("state"),
                tax.get("license"),
            )
        )
    if rows:
        conn.executemany(
            """
            INSERT OR REPLACE INTO nppes_provider_taxonomies (
                provider_npi, taxonomy_code, taxonomy_desc,
                taxonomy_classification, taxonomy_specialization,
                is_primary, state, license
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def refresh_recommendations(
    source_conn: sqlite3.Connection,
    target_conn: sqlite3.Connection,
    npi: str,
) -> None:
    cache = target_conn.execute(
        """
        SELECT primary_taxonomy_desc, primary_taxonomy_classification
        FROM nppes_provider_cache
        WHERE provider_npi = ?
        """,
        (npi,),
    ).fetchone()
    if cache is None:
        return

    taxonomy_desc = cache[0]
    taxonomy_classification = cache[1]
    rollup = rollup_from_taxonomy(taxonomy_desc or taxonomy_classification)
    recommended_specialty = taxonomy_classification or taxonomy_desc

    raw_rows = source_conn.execute(
        """
        SELECT provider_specialty, COUNT(*) AS n
        FROM idr_disputes
        WHERE provider_npi = ?
        GROUP BY provider_specialty
        ORDER BY n DESC
        """,
        (npi,),
    ).fetchall()

    target_conn.execute(
        "DELETE FROM provider_specialty_recommendations WHERE provider_npi = ?",
        (npi,),
    )
    rows = []
    for raw_specialty, count in raw_rows:
        kind = specialty_kind_from_text(raw_specialty)
        if kind == "clinical" and recommended_specialty:
            confidence = 0.9
            source = "nppes"
        elif kind == "unknown" and recommended_specialty:
            confidence = 0.7
            source = "hybrid"
        else:
            confidence = 0.5 if recommended_specialty else 0.2
            source = "heuristic" if recommended_specialty is None else "hybrid"
        rationale = (
            f"raw={raw_specialty or '<blank>'}; "
            f"nppes_primary={taxonomy_desc or '<none>'}"
        )
        rows.append(
            (
                npi,
                raw_specialty or "",
                count,
                kind,
                recommended_specialty,
                rollup,
                source,
                confidence,
                rationale,
            )
        )
    if rows:
        target_conn.executemany(
            """
            INSERT OR REPLACE INTO provider_specialty_recommendations (
                provider_npi, raw_specialty, raw_specialty_count,
                recommended_kind, recommended_canonical_specialty,
                recommended_rollup, recommendation_source, confidence, rationale
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def record_fetch_log(
    conn: sqlite3.Connection,
    npi: str,
    fetch_status: str,
    http_status: int | None,
    attempts: int,
    last_error: str | None,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO nppes_fetch_log (
            provider_npi, fetch_status, http_status, attempts, last_error, fetched_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (npi, fetch_status, http_status, attempts, last_error, utc_now_iso()),
    )


def load_target_npis(
    source_conn: sqlite3.Connection,
    target_conn: sqlite3.Connection,
    limit: int | None,
    refresh: bool,
    specific_npi: str | None,
    retry_failures_after_hours: float,
) -> list[str]:
    if specific_npi:
        return [specific_npi]

    where = [
        "provider_npi GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'"
    ]
    if not refresh:
        where.append(
            "provider_npi NOT IN (SELECT provider_npi FROM cached_npis)"
        )
        where.append(
            "provider_npi NOT IN (SELECT provider_npi FROM recently_failed_npis)"
        )
    sql = f"""
        SELECT DISTINCT provider_npi
        FROM idr_disputes
        WHERE {" AND ".join(where)}
        ORDER BY provider_npi
    """
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    source_conn.execute("DROP TABLE IF EXISTS temp.cached_npis")
    source_conn.execute("CREATE TEMP TABLE cached_npis (provider_npi TEXT PRIMARY KEY)")
    source_conn.execute("DROP TABLE IF EXISTS temp.recently_failed_npis")
    source_conn.execute("CREATE TEMP TABLE recently_failed_npis (provider_npi TEXT PRIMARY KEY)")
    source_conn.executemany(
        "INSERT OR IGNORE INTO cached_npis (provider_npi) VALUES (?)",
        [(row[0],) for row in target_conn.execute("SELECT provider_npi FROM nppes_provider_cache")],
    )
    cutoff = (
        dt.datetime.now(dt.UTC) - dt.timedelta(hours=retry_failures_after_hours)
    ).isoformat(timespec="seconds").replace("+00:00", "Z")
    source_conn.executemany(
        "INSERT OR IGNORE INTO recently_failed_npis (provider_npi) VALUES (?)",
        [
            (row[0],)
            for row in target_conn.execute(
                """
                SELECT provider_npi
                FROM nppes_fetch_log
                WHERE fetch_status = 'failed' AND fetched_at >= ?
                """,
                (cutoff,),
            )
        ],
    )
    return [row[0] for row in source_conn.execute(sql)]


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--npi", help="Fetch a single 10-digit NPI.")
    ap.add_argument("--limit", type=int, help="Limit number of distinct NPIs fetched.")
    ap.add_argument("--refresh", action="store_true", help="Re-fetch NPIs already cached.")
    ap.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="Delay between successful requests. Keep this at 0.25s or higher.",
    )
    ap.add_argument(
        "--timeout-seconds",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Per-request timeout.",
    )
    ap.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="Retries for transient failures and 429/5xx responses.",
    )
    ap.add_argument(
        "--retry-failures-after-hours",
        type=float,
        default=DEFAULT_RETRY_FAILURES_AFTER_HOURS,
        help="Skip recently failed NPIs until this many hours have passed.",
    )
    ap.add_argument(
        "--commit-every",
        type=int,
        default=DEFAULT_COMMIT_EVERY,
        help="Commit target DB every N successful or terminal fetches.",
    )
    ap.add_argument(
        "--source-db",
        default=str(SOURCE_DB_PATH),
        help="Read-only source SQLite DB with idr_disputes.",
    )
    ap.add_argument(
        "--target-db",
        default=str(TARGET_DB_PATH),
        help="Writable SQLite DB for normalization and NPPES cache tables.",
    )
    return ap.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.npi and not is_valid_npi(args.npi):
        raise SystemExit("--npi must be a 10-digit value")

    global SOURCE_DB_PATH, TARGET_DB_PATH
    SOURCE_DB_PATH = Path(args.source_db)
    TARGET_DB_PATH = Path(args.target_db)

    with source_db_connect() as source_conn, target_db_connect() as target_conn:
        npis = load_target_npis(
            source_conn,
            target_conn,
            args.limit,
            args.refresh,
            args.npi,
            args.retry_failures_after_hours,
        )
        if not npis:
            print("No NPIs to enrich.")
            return 0

        fetched = 0
        missing = 0
        failed = 0
        dirty = 0
        for idx, npi in enumerate(npis, start=1):
            status, payload, http_status, last_error, attempts = fetch_nppes_record(
                npi,
                timeout=args.timeout_seconds,
                max_retries=args.max_retries,
            )
            record_fetch_log(
                target_conn,
                npi,
                status,
                http_status,
                attempts,
                last_error,
            )
            dirty += 1

            if status == "missing":
                missing += 1
                print(f"[{idx}/{len(npis)}] {npi} missing")
            elif status == "success" and payload is not None:
                upsert_cache(target_conn, payload)
                refresh_recommendations(source_conn, target_conn, npi)
                fetched += 1
                dirty += 1
                print(f"[{idx}/{len(npis)}] {npi} cached")
            else:
                failed += 1
                print(f"[{idx}/{len(npis)}] {npi} failed: {last_error}", file=sys.stderr)

            if dirty >= args.commit_every:
                target_conn.commit()
                dirty = 0

            if idx < len(npis) and args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

        if dirty:
            target_conn.commit()

    print(
        f"Done. fetched={fetched} missing={missing} failed={failed} total={len(npis)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

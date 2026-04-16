"""Profile raw normalization gaps and seed safe specialty mappings.

This script reads raw values from the CMS source DB and enrichment artifacts
from the sidecar normalization DB, then:

1. Reports the highest-volume unmapped specialty and insurer values.
2. Surfaces NPPES-backed specialty candidates for review.
3. Optionally inserts conservative seed rows into ``specialty_map``.

Usage:
    python scripts/profile_normalization.py
    python scripts/profile_normalization.py --top 50 --out-dir tmp/profile
    python scripts/profile_normalization.py --seed-specialties
"""

from __future__ import annotations

import argparse
import csv
import re
import sqlite3
from pathlib import Path

from enrich_nppes import SOURCE_DB_PATH, TARGET_DB_PATH, ensure_supporting_tables

UNKNOWN_VALUES = {"", "NR", "UNKNOWN", "N/A", "NA", "NONE", "NULL", "-", "REDACTED"}
SEEDABLE_UNKNOWN_VALUES = {"", "NR", "UNKNOWN", "N/A", "NA", "NONE", "NULL", "-", "N/R"}
PROCEDURE_TOKENS = {
    "exam", "mammo", "mri", "ct", "scan", "echo", "view", "xray", "x-ray",
    "ultrasound", "abdomen", "chest", "cad", "biopsy", "screening",
}
INSURER_UNKNOWN_VALUES = {"", "UNKNOWN", "NR", "N/A", "NA"}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=50, help="Rows to show per profile section.")
    ap.add_argument("--source-db", default=str(SOURCE_DB_PATH), help="Source DB path.")
    ap.add_argument("--target-db", default=str(TARGET_DB_PATH), help="Normalization DB path.")
    ap.add_argument("--seed-specialties", action="store_true", help="Insert safe specialty seed rows into specialty_map.")
    ap.add_argument("--seed-min-rows", type=int, default=25, help="Minimum supporting raw rows for a seed.")
    ap.add_argument("--seed-min-dominance", type=float, default=0.8, help="Minimum NPPES recommendation dominance ratio.")
    ap.add_argument("--out-dir", help="Optional directory for CSV exports.")
    return ap.parse_args()


def source_conn(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def target_conn(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, timeout=30.0)
    conn.execute("PRAGMA busy_timeout = 30000")
    conn.row_factory = sqlite3.Row
    ensure_supporting_tables(conn)
    return conn


def norm_text(value: str | None) -> str:
    if value is None:
        return ""
    s = str(value).strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_label(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.upper() in UNKNOWN_VALUES:
        return None
    s = re.sub(r"\s+", " ", s)
    if s.isupper() and len(s) > 4:
        s = s.title()
    return s


def specialty_kind_from_raw(value: str | None) -> str:
    if value is None or not str(value).strip() or str(value).strip().upper() in UNKNOWN_VALUES:
        return "unknown"
    s = norm_text(value)
    if any(token in s for token in ("llc", "inc", "associates", "physicians", "medical group", " pa ")):
        return "organization"
    if any(token in s for token in ("hospital", "facility", "center", "room", "services", "department")):
        return "facility_setting"
    return "clinical"


def looks_like_procedure_text(value: str | None) -> bool:
    s = norm_text(value)
    return any(token in s.split() for token in PROCEDURE_TOKENS)


def choose_rollup(raw_value: str | None, recommendation: dict) -> str | None:
    if recommendation.get("recommended_rollup"):
        return recommendation["recommended_rollup"]
    clean = clean_label(raw_value)
    if specialty_kind_from_raw(raw_value) == "clinical":
        return clean
    return None


def insurer_entity_type(raw_value: str | None) -> str:
    if raw_value is None or not str(raw_value).strip() or str(raw_value).strip().upper() in INSURER_UNKNOWN_VALUES:
        return "unknown"
    s = norm_text(raw_value)
    if any(token in s for token in ("benefit", "welfare", "employee", "health plan", "group health plan")):
        return "self_funded_plan"
    if any(token in s for token in ("administrator", "administrators", "admin", "tpa", "services inc", "benefit systems")):
        return "tpa"
    if any(token in s for token in ("bcbs", "blue cross", "aetna", "cigna", "united", "anthem", "humana", "kaiser", "molina", "medica")):
        return "major_carrier"
    return "local_plan"


def fetch_specialty_counts(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT provider_specialty AS raw_value, COUNT(*) AS n_rows
        FROM idr_disputes
        GROUP BY provider_specialty
        ORDER BY n_rows DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_insurer_counts(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            health_plan_name AS raw_value,
            MAX(health_plan_name_normalized) AS normalized_hint,
            COUNT(*) AS n_rows
        FROM idr_disputes
        GROUP BY health_plan_name
        ORDER BY n_rows DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def fetch_mapped_values(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[0] for row in conn.execute(f"SELECT raw_value FROM {table_name}")}


def fetch_specialty_recommendation_summary(conn: sqlite3.Connection) -> dict[str, dict]:
    rows = conn.execute(
        """
        WITH recs AS (
            SELECT
                raw_specialty,
                recommended_kind,
                recommended_canonical_specialty,
                recommended_rollup,
                recommendation_source,
                SUM(raw_specialty_count) AS support_rows,
                COUNT(DISTINCT provider_npi) AS support_npis,
                AVG(confidence) AS avg_confidence
            FROM provider_specialty_recommendations
            GROUP BY
                raw_specialty,
                recommended_kind,
                recommended_canonical_specialty,
                recommended_rollup,
                recommendation_source
        ),
        ranked AS (
            SELECT
                raw_specialty,
                recommended_kind,
                recommended_canonical_specialty,
                recommended_rollup,
                recommendation_source,
                support_rows,
                support_npis,
                avg_confidence,
                SUM(support_rows) OVER (PARTITION BY raw_specialty) AS total_support_rows,
                ROW_NUMBER() OVER (
                    PARTITION BY raw_specialty
                    ORDER BY support_rows DESC, avg_confidence DESC, recommended_canonical_specialty
                ) AS rn
            FROM recs
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        """
    ).fetchall()
    out = {}
    for row in rows:
        d = dict(row)
        total = d["total_support_rows"] or 0
        d["dominance"] = (d["support_rows"] / total) if total else 0.0
        out[d["raw_specialty"] or ""] = d
    return out


def build_specialty_profile(
    specialty_counts: list[dict],
    mapped_values: set[str],
    recommendation_summary: dict[str, dict],
) -> tuple[list[dict], dict]:
    rows = []
    mapped_row_count = 0
    total_rows = 0
    for item in specialty_counts:
        raw_value = item["raw_value"] or ""
        n_rows = item["n_rows"]
        total_rows += n_rows
        mapped = raw_value in mapped_values
        if mapped:
            mapped_row_count += n_rows
        rec = recommendation_summary.get(raw_value, {})
        rows.append(
            {
                "raw_value": raw_value,
                "n_rows": n_rows,
                "mapped": mapped,
                "kind_hint": specialty_kind_from_raw(raw_value),
                "top_recommended_kind": rec.get("recommended_kind"),
                "top_recommended_canonical": rec.get("recommended_canonical_specialty"),
                "top_recommended_rollup": rec.get("recommended_rollup"),
                "recommendation_support_rows": rec.get("support_rows"),
                "recommendation_support_npis": rec.get("support_npis"),
                "recommendation_dominance": rec.get("dominance"),
                "recommendation_confidence": rec.get("avg_confidence"),
            }
        )
    summary = {
        "distinct_values": len(rows),
        "mapped_values": sum(1 for row in rows if row["mapped"]),
        "unmapped_values": sum(1 for row in rows if not row["mapped"]),
        "total_rows": total_rows,
        "mapped_rows": mapped_row_count,
        "mapped_row_share": (mapped_row_count / total_rows) if total_rows else 0.0,
    }
    return rows, summary


def build_insurer_profile(insurer_counts: list[dict], mapped_values: set[str]) -> tuple[list[dict], dict]:
    rows = []
    mapped_row_count = 0
    total_rows = 0
    for item in insurer_counts:
        raw_value = item["raw_value"] or ""
        n_rows = item["n_rows"]
        total_rows += n_rows
        mapped = raw_value in mapped_values
        if mapped:
            mapped_row_count += n_rows
        rows.append(
            {
                "raw_value": raw_value,
                "n_rows": n_rows,
                "mapped": mapped,
                "normalized_hint": item["normalized_hint"],
                "entity_type_hint": insurer_entity_type(raw_value),
            }
        )
    summary = {
        "distinct_values": len(rows),
        "mapped_values": sum(1 for row in rows if row["mapped"]),
        "unmapped_values": sum(1 for row in rows if not row["mapped"]),
        "total_rows": total_rows,
        "mapped_rows": mapped_row_count,
        "mapped_row_share": (mapped_row_count / total_rows) if total_rows else 0.0,
    }
    return rows, summary


def specialty_seed_candidates(
    specialty_profile: list[dict],
    min_rows: int,
    min_dominance: float,
) -> list[dict]:
    candidates = []
    for row in specialty_profile:
        raw_value = row["raw_value"]
        raw_norm = norm_text(raw_value)
        canonical = row["top_recommended_canonical"]
        rollup = row["top_recommended_rollup"]
        canonical_norm = norm_text(canonical)
        rollup_norm = norm_text(rollup)
        raw_upper = str(raw_value).strip().upper()
        is_unknown = not raw_norm or raw_upper in UNKNOWN_VALUES
        seedable_unknown = not raw_norm or raw_upper in SEEDABLE_UNKNOWN_VALUES
        close_match = bool(
            raw_norm
            and (
                raw_norm == canonical_norm
                or raw_norm == rollup_norm
                or canonical_norm == raw_norm
                or rollup_norm == raw_norm
            )
        )
        if row["mapped"]:
            continue
        if row["n_rows"] < min_rows:
            continue
        if not canonical:
            continue
        if not str(raw_value).strip():
            continue
        if (row["recommendation_dominance"] or 0.0) < min_dominance:
            continue
        if is_unknown and (row["recommendation_support_npis"] or 0) < 2:
            continue
        if looks_like_procedure_text(raw_value):
            continue
        if not ((seedable_unknown and is_unknown) or close_match):
            continue

        raw_kind = specialty_kind_from_raw(raw_value)
        kind = raw_kind if raw_kind != "clinical" else (row["top_recommended_kind"] or raw_kind)
        clean = clean_label(raw_value)
        seed_confidence = min(
            0.99,
            ((row["recommendation_confidence"] or 0.0) * 0.6) + ((row["recommendation_dominance"] or 0.0) * 0.4),
        )
        notes = (
            "auto-seeded from provider_specialty_recommendations; "
            f"support_rows={row['recommendation_support_rows']}; "
            f"support_npis={row['recommendation_support_npis']}; "
            f"dominance={row['recommendation_dominance']:.3f}"
        )
        candidates.append(
            {
                "raw_value": raw_value,
                "clean_value": clean,
                "specialty_kind": kind,
                "canonical_specialty": canonical,
                "specialty_rollup": choose_rollup(raw_value, row),
                "mapping_source": "nppes_seed",
                "confidence": round(seed_confidence, 4),
                "status": "pending",
                "notes": notes,
            }
        )
    return sorted(candidates, key=lambda row: row["confidence"], reverse=True)


def insert_specialty_seeds(conn: sqlite3.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    before = conn.total_changes
    conn.executemany(
        """
        INSERT OR IGNORE INTO specialty_map (
            raw_value, clean_value, specialty_kind, canonical_specialty,
            specialty_rollup, mapping_source, confidence, status, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row["raw_value"],
                row["clean_value"],
                row["specialty_kind"],
                row["canonical_specialty"],
                row["specialty_rollup"],
                row["mapping_source"],
                row["confidence"],
                row["status"],
                row["notes"],
            )
            for row in rows
        ],
    )
    conn.commit()
    return conn.total_changes - before


def print_summary(title: str, summary: dict) -> None:
    print(title)
    print(
        f"  distinct={summary['distinct_values']} mapped_values={summary['mapped_values']} "
        f"unmapped_values={summary['unmapped_values']}"
    )
    print(
        f"  total_rows={summary['total_rows']} mapped_rows={summary['mapped_rows']} "
        f"mapped_row_share={summary['mapped_row_share']:.1%}"
    )


def print_top_rows(title: str, rows: list[dict], columns: list[str], top: int) -> None:
    print(title)
    for row in rows[:top]:
        parts = [f"{col}={row.get(col)}" for col in columns]
        print("  " + " | ".join(parts))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)


def main() -> int:
    args = parse_args()
    with source_conn(args.source_db) as source, target_conn(args.target_db) as target:
        specialty_counts = fetch_specialty_counts(source)
        insurer_counts = fetch_insurer_counts(source)
        specialty_mapped = fetch_mapped_values(target, "specialty_map")
        insurer_mapped = fetch_mapped_values(target, "insurer_map")
        rec_summary = fetch_specialty_recommendation_summary(target)

        specialty_profile, specialty_summary = build_specialty_profile(
            specialty_counts, specialty_mapped, rec_summary
        )
        insurer_profile, insurer_summary = build_insurer_profile(
            insurer_counts, insurer_mapped
        )
        candidates = specialty_seed_candidates(
            specialty_profile,
            min_rows=args.seed_min_rows,
            min_dominance=args.seed_min_dominance,
        )

        print_summary("Specialty Coverage", specialty_summary)
        print_top_rows(
            "Top Unmapped Specialties",
            [row for row in specialty_profile if not row["mapped"]],
            [
                "raw_value", "n_rows", "kind_hint", "top_recommended_canonical",
                "top_recommended_rollup", "recommendation_dominance",
            ],
            args.top,
        )
        print_summary("Insurer Coverage", insurer_summary)
        print_top_rows(
            "Top Unmapped Insurers",
            [row for row in insurer_profile if not row["mapped"]],
            ["raw_value", "n_rows", "normalized_hint", "entity_type_hint"],
            args.top,
        )
        print_top_rows(
            "Specialty Seed Candidates",
            candidates,
            [
                "raw_value", "canonical_specialty", "specialty_rollup",
                "specialty_kind", "confidence",
            ],
            args.top,
        )

        if args.out_dir:
            out_dir = Path(args.out_dir)
            write_csv(out_dir / "unmapped_specialties.csv", [row for row in specialty_profile if not row["mapped"]])
            write_csv(out_dir / "unmapped_insurers.csv", [row for row in insurer_profile if not row["mapped"]])
            write_csv(out_dir / "specialty_seed_candidates.csv", candidates)
            print(f"Wrote CSVs to {out_dir}")

    if args.seed_specialties:
        with target_conn(args.target_db) as seed_target:
            inserted = insert_specialty_seeds(seed_target, candidates)
        print(f"Inserted specialty seeds: {inserted}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

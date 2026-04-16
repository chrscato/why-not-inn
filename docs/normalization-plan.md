# Normalization Plan

This dataset needs a normalization system, not just prettier strings.

The core problem is that the raw CMS fields already mix multiple concepts:

- `provider_specialty` comes from `Practice/Facility Specialty or Type`, so one column contains clinical specialties, facility/service settings, unknown placeholders, and some organization-like values.
- `health_plan_name` mixes carrier brands, local Blue plans, TPAs, employer/self-funded plans, and spelling noise.

## Design Principles

1. Preserve raw values.
2. Normalize in layers.
3. Separate identity from rollup.
4. Keep low-confidence rows explicit instead of forcing fake precision.
5. Make every mapping reviewable in SQL.

## Target Model

For specialties:

- `raw_value`
- `clean_value`
- `specialty_kind`: `clinical`, `facility_setting`, `organization`, `unknown`, `mixed`
- `canonical_specialty`
- `specialty_rollup`
- `mapping_source`
- `confidence`

For insurers:

- `raw_value`
- `clean_value`
- `entity_type`: `major_carrier`, `local_plan`, `tpa`, `self_funded_plan`, `unknown`
- `canonical_entity`
- `parent_family`
- `mapping_source`
- `confidence`

For NPI/NPPES:

- Cache raw registry responses locally.
- Flatten all returned taxonomies.
- Use primary taxonomy as a recommendation input, not as unquestioned truth.
- Join NPPES back to all rows sharing the same valid 10-digit `provider_npi`.

## Why NPPES Helps

NPPES gives us an external provider taxonomy anchor for rows with a valid NPI.

In this database:

- `269,975` dispute rows have a valid 10-digit NPI.
- `6,698` distinct valid NPIs exist.
- That covers most of the row volume, so NPPES can materially improve specialty standardization.

NPPES is most valuable for:

- collapsing specialty synonyms for the same provider/NPI
- replacing `NR` or blank specialty values when the NPI is valid
- distinguishing provider taxonomy from facility/service-setting labels

NPPES is not a full solution for:

- rows with invalid or missing NPIs
- facility rows whose CMS label is a setting rather than a provider specialty
- insurer normalization

## Recommended Processing Order

1. Ingest raw CMS files exactly as today.
2. Run `scripts/enrich_nppes.py` to cache NPPES records for valid NPIs.
3. Generate provider specialty recommendations from NPPES taxonomy plus raw CMS values.
4. Curate `specialty_map` for high-volume raw values.
5. Curate `insurer_map` separately with a parent-family hierarchy.
6. Build downstream views or materialized outputs that use approved mappings only.

## Governance Rules

- Never overwrite raw CMS values.
- Never replace insurer entities with broad parent families unless the analysis explicitly wants the rollup.
- Never treat NPPES taxonomy as certain when the row clearly reflects a facility/service setting.
- Review high-volume unmapped values first.
- Track mapping coverage and confidence over time.

## Immediate Repo Additions

The repo now has:

- normalization crosswalk tables in `db/schema.sql`
- NPPES cache and taxonomy tables in `db/schema.sql`
- `scripts/enrich_nppes.py` to fetch and persist NPPES provider data
- a sidecar write target, `db/normalization.db`, so enrichment work does not contend with the live app database

## Running Enrichment

Use the sidecar DB as the writable target. The script is resumable and skips
already-cached NPIs unless `--refresh` is passed.

Examples:

```bash
python scripts/enrich_nppes.py --limit 25 --sleep-seconds 0.5
python scripts/enrich_nppes.py --limit 250 --sleep-seconds 1.0 --commit-every 25
python scripts/enrich_nppes.py --npi 1538107875
python scripts/enrich_nppes.py --refresh --limit 100 --retry-failures-after-hours 0
```

Operational notes:

- Default behavior skips NPIs already cached in `db/normalization.db`.
- Failed NPIs are logged and skipped for 24 hours unless you override with
  `--retry-failures-after-hours`.
- The script uses retries with backoff for transient errors and 429 responses.
- Keep `--sleep-seconds` at `0.25` or higher to stay gentle with the API.

## Next Implementation Steps

1. Add a profiling script that ranks unmapped specialty and insurer values by row share.
2. Add SQL views for `normalized_specialty` and `normalized_insurer`.
3. Update `compute_stats.py` to aggregate on normalized dimensions once the crosswalks are approved.
4. Update the API/frontend to filter by normalized specialty rollups instead of raw `provider_specialty`.

## Profiling And Seeding

Use `scripts/profile_normalization.py` after enrichment runs.

Examples:

```bash
python scripts/profile_normalization.py --top 25
python scripts/profile_normalization.py --top 50 --out-dir tmp/normalization-profile
python scripts/profile_normalization.py --seed-specialties
```

What it does:

- profiles highest-volume unmapped `provider_specialty` values
- profiles highest-volume unmapped `health_plan_name` values
- summarizes NPPES-backed specialty recommendations
- optionally inserts conservative `specialty_map` seed rows with `status='pending'`

Current behavior:

- only seeds specialties when the raw value is either a safe unknown placeholder
  or an exact/near-exact text match to the dominant NPPES recommendation
- skips obvious procedure-description leakage
- writes into `db/normalization.db`, not the main app DB

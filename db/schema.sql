-- Why Not In-Network? — SQLite schema
-- Mirrors the three CMS IDR PUF tabs (OON Emergency/Non-Emergency, OON Air
-- Ambulance, QPA and Offers) plus a pre-computed stats table for fast
-- dashboard queries.

PRAGMA foreign_keys = OFF;
PRAGMA journal_mode = WAL;

-- Tabs 1 & 2: OON Emergency/Non-Emergency + Air Ambulance
-- Same shape; source_tab discriminates.
CREATE TABLE IF NOT EXISTS idr_disputes (
    dispute_number TEXT,
    quarter TEXT NOT NULL,                  -- '2023-Q1', '2024-Q3Q4', etc.
    source_tab TEXT,                        -- 'oon' | 'air_ambulance'
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
    health_plan_name_normalized TEXT,
    health_plan_email_domain TEXT,
    health_plan_type TEXT,
    -- Line-item fields (denormalized — one row per DLI)
    dli_number TEXT,
    service_code TEXT,
    type_of_service_code TEXT,
    item_description TEXT,
    location_of_service TEXT,               -- state code (or country/region)
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

-- Tab 3: QPA and Offers — actual dollar amounts
CREATE TABLE IF NOT EXISTS idr_offers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    quarter TEXT NOT NULL,
    service_code TEXT,
    type_of_service_code TEXT,
    geographic_region TEXT,                 -- MSA
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
    -- Computed
    prevailing_pct_qpa REAL,
    qpa_suspect INTEGER DEFAULT 0           -- 1 if qpa<1 or prevailing_pct_qpa>1000
);

-- Pre-computed aggregates for fast dashboard queries
CREATE TABLE IF NOT EXISTS idr_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dimension TEXT,                         -- overall|cpt|state|insurer|specialty
    dimension_value TEXT,                   -- NULL for overall
    quarter TEXT,                           -- NULL for all-time
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
    avg_qpa REAL,
    avg_prevailing REAL,
    avg_provider_offer REAL,
    avg_issuer_offer REAL
);

-- Optional CPT description lookup (populated opportunistically from
-- item_description fields seen during ingest)
CREATE TABLE IF NOT EXISTS cpt_descriptions (
    service_code TEXT PRIMARY KEY,
    description TEXT,
    n_seen INTEGER DEFAULT 0
);

-- Ingestion bookkeeping so re-runs are idempotent
CREATE TABLE IF NOT EXISTS ingest_log (
    file_name TEXT PRIMARY KEY,
    quarter TEXT,
    rows_disputes INTEGER,
    rows_air_ambulance INTEGER,
    rows_offers INTEGER,
    loaded_at TEXT
);

-- Normalization crosswalks and enrichment cache.
-- These tables let us preserve raw CMS values while layering deterministic
-- cleanup, curated mappings, and third-party enrichment.

CREATE TABLE IF NOT EXISTS specialty_map (
    raw_value TEXT PRIMARY KEY,
    clean_value TEXT,
    specialty_kind TEXT,                  -- clinical|facility_setting|organization|unknown|mixed
    canonical_specialty TEXT,
    specialty_rollup TEXT,
    mapping_source TEXT,                  -- rule|manual|nppes|hybrid
    confidence REAL,                      -- 0.0 - 1.0
    status TEXT DEFAULT 'pending',        -- pending|approved|rejected
    notes TEXT
);

CREATE TABLE IF NOT EXISTS insurer_map (
    raw_value TEXT PRIMARY KEY,
    clean_value TEXT,
    entity_type TEXT,                     -- major_carrier|local_plan|tpa|self_funded_plan|unknown
    canonical_entity TEXT,
    parent_family TEXT,
    mapping_source TEXT,                  -- rule|manual|hybrid
    confidence REAL,                      -- 0.0 - 1.0
    status TEXT DEFAULT 'pending',        -- pending|approved|rejected
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
    PRIMARY KEY (provider_npi, taxonomy_code, state, license),
    FOREIGN KEY (provider_npi) REFERENCES nppes_provider_cache(provider_npi)
);

CREATE TABLE IF NOT EXISTS provider_specialty_recommendations (
    provider_npi TEXT NOT NULL,
    raw_specialty TEXT NOT NULL,
    raw_specialty_count INTEGER NOT NULL,
    recommended_kind TEXT,
    recommended_canonical_specialty TEXT,
    recommended_rollup TEXT,
    recommendation_source TEXT,           -- nppes|heuristic|hybrid
    confidence REAL,
    rationale TEXT,
    PRIMARY KEY (provider_npi, raw_specialty)
);

CREATE INDEX IF NOT EXISTS idx_disputes_service       ON idr_disputes(service_code);
CREATE INDEX IF NOT EXISTS idx_disputes_state         ON idr_disputes(location_of_service);
CREATE INDEX IF NOT EXISTS idx_disputes_plan          ON idr_disputes(health_plan_name);
CREATE INDEX IF NOT EXISTS idx_disputes_plan_norm     ON idr_disputes(health_plan_name_normalized);
CREATE INDEX IF NOT EXISTS idx_disputes_quarter       ON idr_disputes(quarter);
CREATE INDEX IF NOT EXISTS idx_disputes_specialty     ON idr_disputes(provider_specialty);
CREATE INDEX IF NOT EXISTS idx_disputes_outcome       ON idr_disputes(payment_determination_outcome);
CREATE INDEX IF NOT EXISTS idx_disputes_dli_type      ON idr_disputes(dispute_line_item_type);
CREATE INDEX IF NOT EXISTS idx_offers_service         ON idr_offers(service_code);
CREATE INDEX IF NOT EXISTS idx_offers_geo             ON idr_offers(geographic_region);
CREATE INDEX IF NOT EXISTS idx_offers_quarter         ON idr_offers(quarter);
CREATE INDEX IF NOT EXISTS idx_stats_dim              ON idr_stats(dimension, dimension_value);
CREATE INDEX IF NOT EXISTS idx_stats_dim_quarter      ON idr_stats(dimension, dimension_value, quarter);
CREATE INDEX IF NOT EXISTS idx_specialty_map_kind     ON specialty_map(specialty_kind, canonical_specialty);
CREATE INDEX IF NOT EXISTS idx_insurer_map_parent     ON insurer_map(parent_family, canonical_entity);
CREATE INDEX IF NOT EXISTS idx_nppes_primary_taxonomy ON nppes_provider_cache(primary_taxonomy_code);
CREATE INDEX IF NOT EXISTS idx_nppes_taxonomy_desc    ON nppes_provider_taxonomies(taxonomy_desc);

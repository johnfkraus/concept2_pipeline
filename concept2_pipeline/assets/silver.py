"""
concept2_pipeline.assets.silver
================================
Silver layer — cleaned, typed, and enriched workout data.

Reads from ``concept2_bronze.results`` (written by the bronze dlt asset)
and produces ``concept2_silver.workouts`` via a CREATE TABLE AS … SELECT
statement executed directly against Postgres.

Cleaning steps applied
----------------------
1. Parse `date` → proper TIMESTAMPTZ and extract date parts for partitioning.
2. Derive `pace_seconds_per_500m` from raw distance + time fields.
3. Normalise `type` to lowercase and map known aliases
   (e.g. "bike" / "bikeerg" → "bikeerg").
4. Cast `time_tenths` to a human-readable INTERVAL.
5. Drop internal dlt metadata columns (_dlt_id, _dlt_load_id, etc.).
6. Add a `loaded_at` timestamp for lineage tracking.

The asset declares a dependency on `concept2_bronze_assets` so Dagster
always runs bronze first.
"""

from __future__ import annotations

from dagster import asset

from concept2_pipeline.resources.postgres_resource import PostgresResource
from concept2_pipeline.assets.bronze import concept2_bronze_assets

# ── SQL that builds the silver table ─────────────────────────────────────────

_CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS concept2_silver;"

_DROP_TABLE    = "DROP TABLE IF EXISTS concept2_silver.workouts;"

# Columns that are always present (dlt always writes these when non-null
# values are returned by the API for at least one row).
_REQUIRED_COLS = {
    "id", "date", "type", "workout_type", "distance_m",
    "time_tenths", "time_formatted", "stroke_rate", "calories",
    "watts", "source",
}

# Optional columns — only selected when they exist in the bronze table.
# Maps bronze column name → silver SELECT expression.
_OPTIONAL_COLS = {
    "heart_rate_avg": "heart_rate_avg::INT  AS heart_rate_avg",
    "heart_rate_max": "heart_rate_max::INT  AS heart_rate_max",
    "heart_rate_min": "heart_rate_min::INT  AS heart_rate_min",
    "weight_class":   "weight_class",
    "verified":       "verified::BOOLEAN    AS verified",
    "ranked":         "ranked::BOOLEAN      AS ranked",
    "comments":       "comments",
}


def _build_create_table_sql(bronze_table: str, bronze_cols: set) -> str:
    """Build the CREATE TABLE AS SELECT dynamically based on available columns."""
    optional_fragments = [
        f"    {expr}"
        for col, expr in _OPTIONAL_COLS.items()
        if col in bronze_cols
    ]
    optional_sql = ("\n" + ",\n".join(optional_fragments) + ",") if optional_fragments else ""

    return f"""
CREATE TABLE concept2_silver.workouts AS
SELECT
    -- identifiers
    id::BIGINT                                          AS workout_id,

    -- time dimension
    date::TIMESTAMPTZ                                   AS workout_at,
    date::TIMESTAMPTZ::DATE                             AS workout_date,
    EXTRACT(YEAR  FROM date::TIMESTAMPTZ)::INT          AS year,
    EXTRACT(MONTH FROM date::TIMESTAMPTZ)::INT          AS month,
    EXTRACT(DOW   FROM date::TIMESTAMPTZ)::INT          AS day_of_week,

    -- activity type (normalised)
    LOWER(COALESCE(type, 'unknown'))                    AS activity_type,
    CASE
        WHEN LOWER(type) IN ('bike', 'bikeerg') THEN 'bikeerg'
        WHEN LOWER(type) = 'skierg'             THEN 'skierg'
        WHEN LOWER(type) = 'rower'              THEN 'rower'
        ELSE LOWER(COALESCE(type, 'unknown'))
    END                                                 AS activity_type_clean,
    {'workout_type,' if 'workout_type' in bronze_cols else "NULL::TEXT AS workout_type,"}

    -- distance
    {'distance_m::DOUBLE PRECISION AS distance_m,' if 'distance_m' in bronze_cols else 'NULL::DOUBLE PRECISION AS distance_m,'}
    {'ROUND((distance_m / 1000.0)::NUMERIC, 3) AS distance_km,' if 'distance_m' in bronze_cols else 'NULL::NUMERIC AS distance_km,'}

    -- time
    {'time_tenths::BIGINT AS time_tenths,' if 'time_tenths' in bronze_cols else 'NULL::BIGINT AS time_tenths,'}
    {'time_formatted,' if 'time_formatted' in bronze_cols else 'NULL::TEXT AS time_formatted,'}
    {'MAKE_INTERVAL(secs => (time_tenths::DOUBLE PRECISION / 10.0)) AS duration,' if 'time_tenths' in bronze_cols else 'NULL::INTERVAL AS duration,'}

    -- pace (seconds per 500 m)
    CASE
        WHEN {'distance_m > 0' if 'distance_m' in bronze_cols else 'FALSE'}
         AND {'time_tenths IS NOT NULL' if 'time_tenths' in bronze_cols else 'FALSE'}
        THEN ROUND(
            ((time_tenths::DOUBLE PRECISION / 10.0) / distance_m * 500)::NUMERIC, 2
        )
        ELSE NULL
    END                                                 AS pace_seconds_per_500m,

    -- effort metrics
    {'stroke_rate::INT AS stroke_rate,' if 'stroke_rate' in bronze_cols else 'NULL::INT AS stroke_rate,'}
    {'calories::INT    AS calories,'     if 'calories'    in bronze_cols else 'NULL::INT AS calories,'}
    {'watts::INT       AS watts,'        if 'watts'       in bronze_cols else 'NULL::INT AS watts,'}

    -- optional columns (present only when the API returned data for them)
    {optional_sql}

    -- source & lineage
    {'source,' if 'source' in bronze_cols else 'NULL::TEXT AS source,'}
    NOW()  AS loaded_at

FROM concept2_bronze."{bronze_table}"
WHERE id IS NOT NULL
ORDER BY date::TIMESTAMPTZ ASC;
"""

_ADD_PK = """
ALTER TABLE concept2_silver.workouts
    ADD PRIMARY KEY (workout_id);
"""

_ADD_INDEXES = """
CREATE INDEX IF NOT EXISTS ix_silver_workouts_date
    ON concept2_silver.workouts (workout_date);
CREATE INDEX IF NOT EXISTS ix_silver_workouts_type
    ON concept2_silver.workouts (activity_type_clean);
CREATE INDEX IF NOT EXISTS ix_silver_workouts_year_month
    ON concept2_silver.workouts (year, month);
"""


# ── Asset definition ──────────────────────────────────────────────────────────

@asset(
    name="silver_workouts",
    group_name="silver",
    deps=[concept2_bronze_assets],
    description=(
        "Cleaned and typed workout data derived from the bronze raw ingest. "
        "Lives in concept2_silver.workouts."
    ),
    metadata={
        "schema":      "concept2_silver",
        "table":       "workouts",
        "destination": "postgres",
    },
)
def silver_workouts(
    context,
    postgres: PostgresResource,
) -> None:
    """
    Rebuild concept2_silver.workouts from the bronze results table.

    This is a full-replace transform: the old table is dropped and recreated
    from scratch on every run.  Because the bronze layer is the source of
    truth (and handles incremental state), silver can afford to be a cheap
    full SELECT transform — the bronze table is already small enough that
    a full rebuild is fast.

    If the dataset grows very large, this can be changed to an incremental
    INSERT … WHERE workout_id NOT IN (SELECT workout_id FROM silver_workouts)
    pattern with a date watermark.
    """
    # ── Discover the bronze results table (dlt may normalise the name) ────────
    tables_df = postgres.query_df("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'concept2_bronze'
          AND table_type   = 'BASE TABLE'
        ORDER BY table_name
    """)
    all_tables = tables_df["table_name"].tolist()
    context.log.info(f"Tables in concept2_bronze schema: {all_tables}")

    # Accept any table whose name contains 'result' (handles dlt normalisation
    # variants like 'results', 'concept2__results', etc.)
    result_tables = [t for t in all_tables if "result" in t.lower()]
    if not result_tables:
        raise RuntimeError(
            f"No results table found in concept2_bronze schema. "
            f"Available tables: {all_tables}. "
            "Materialize the bronze asset first."
        )

    bronze_table = result_tables[0]   # use the first match
    context.log.info(f"Using bronze table: concept2_bronze.{bronze_table}")

    bronze_count_df = postgres.query_df(
        f'SELECT COUNT(*) AS n FROM concept2_bronze."{bronze_table}";'
    )
    bronze_rows = int(bronze_count_df["n"].iloc[0])
    context.log.info(f"concept2_bronze.{bronze_table} has {bronze_rows:,} rows — proceeding.")
    if bronze_rows == 0:
        context.log.warning("Bronze table is empty — run the bronze asset first.")

    # ── Build silver ──────────────────────────────────────────────────────────
    context.log.info("Creating concept2_silver schema if not exists…")
    postgres.execute(_CREATE_SCHEMA)

    context.log.info("Dropping existing silver table…")
    postgres.execute(_DROP_TABLE)

    # Fetch the actual columns present in the bronze table
    cols_df = postgres.query_df("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'concept2_bronze'
          AND table_name   = :tbl
        ORDER BY ordinal_position
    """, params={"tbl": bronze_table})
    bronze_cols = set(cols_df["column_name"].tolist())
    context.log.info(f"Columns in concept2_bronze.{bronze_table}: {sorted(bronze_cols)}")

    context.log.info(f"Building silver.workouts from concept2_bronze.{bronze_table}…")
    create_sql = _build_create_table_sql(bronze_table, bronze_cols)
    postgres.execute(create_sql)

    context.log.info("Adding primary key and indexes…")
    postgres.execute(_ADD_PK)
    postgres.execute(_ADD_INDEXES)

    # Surface row count as Dagster metadata
    df = postgres.query_df("SELECT COUNT(*) AS n FROM concept2_silver.workouts;")
    row_count = int(df["n"].iloc[0])
    context.log.info(f"silver.workouts contains {row_count:,} rows.")
    context.add_output_metadata({"row_count": row_count, "bronze_source_rows": bronze_rows})

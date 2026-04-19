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

from dagster import AssetExecutionContext, AssetKey, asset

from concept2_pipeline.resources.postgres_resource import PostgresResource

# ── SQL that builds the silver table ─────────────────────────────────────────

_CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS concept2_silver;"

_DROP_TABLE    = "DROP TABLE IF EXISTS concept2_silver.workouts;"

_CREATE_TABLE  = """
CREATE TABLE concept2_silver.workouts AS
SELECT
    -- identifiers
    id::BIGINT                                          AS workout_id,

    -- time dimension
    date::TIMESTAMPTZ                                   AS workout_at,
    date::TIMESTAMPTZ::DATE                             AS workout_date,
    EXTRACT(YEAR  FROM date::TIMESTAMPTZ)::INT          AS year,
    EXTRACT(MONTH FROM date::TIMESTAMPTZ)::INT          AS month,
    EXTRACT(DOW   FROM date::TIMESTAMPTZ)::INT          AS day_of_week,  -- 0=Sun

    -- activity type (normalised)
    LOWER(COALESCE(type, 'unknown'))                    AS activity_type,
    CASE
        WHEN LOWER(type) IN ('bike', 'bikeerg') THEN 'bikeerg'
        WHEN LOWER(type) = 'skierg'             THEN 'skierg'
        WHEN LOWER(type) = 'rower'              THEN 'rower'
        ELSE LOWER(COALESCE(type, 'unknown'))
    END                                                 AS activity_type_clean,
    workout_type,

    -- distance
    distance_m::DOUBLE PRECISION                        AS distance_m,
    ROUND((distance_m / 1000.0)::NUMERIC, 3)            AS distance_km,

    -- time
    time_tenths::BIGINT                                 AS time_tenths,
    time_formatted,
    -- convert tenths-of-seconds to a Postgres INTERVAL
    MAKE_INTERVAL(
        secs => (time_tenths::DOUBLE PRECISION / 10.0)
    )                                                   AS duration,

    -- pace (seconds per 500 m) — NULL-safe
    CASE
        WHEN distance_m > 0 AND time_tenths IS NOT NULL
        THEN ROUND(
            ((time_tenths::DOUBLE PRECISION / 10.0) / distance_m * 500)::NUMERIC, 2
        )
        ELSE NULL
    END                                                 AS pace_seconds_per_500m,

    -- effort metrics
    stroke_rate::INT                                    AS stroke_rate,
    calories::INT                                       AS calories,
    watts::INT                                          AS watts,

    -- heart rate
    heart_rate_avg::INT                                 AS heart_rate_avg,
    heart_rate_max::INT                                 AS heart_rate_max,
    heart_rate_min::INT                                 AS heart_rate_min,

    -- metadata
    source,
    weight_class,
    verified::BOOLEAN                                   AS verified,
    ranked::BOOLEAN                                     AS ranked,
    comments,

    -- lineage
    NOW()                                               AS loaded_at

FROM concept2_bronze.results
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
    deps=[AssetKey(["concept2_bronze", "results"])],
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
    context: AssetExecutionContext,
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
    context.log.info("Creating concept2_silver schema if not exists…")
    postgres.execute(_CREATE_SCHEMA)

    context.log.info("Dropping existing silver table…")
    postgres.execute(_DROP_TABLE)

    context.log.info("Building silver.workouts from bronze.results…")
    postgres.execute(_CREATE_TABLE)

    context.log.info("Adding primary key and indexes…")
    postgres.execute(_ADD_PK)
    postgres.execute(_ADD_INDEXES)

    # Surface row count as Dagster metadata
    df = postgres.query_df("SELECT COUNT(*) AS n FROM concept2_silver.workouts;")
    row_count = int(df["n"].iloc[0])
    context.log.info(f"silver.workouts contains {row_count:,} rows.")

    from dagster import Output
    # Emit metadata visible in the Dagster UI asset panel
    context.add_output_metadata({"row_count": row_count})

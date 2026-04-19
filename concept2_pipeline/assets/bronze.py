"""
concept2_pipeline.assets.bronze
================================
Bronze layer — raw ingest from the Concept2 Logbook API into Postgres.

Uses the @dlt_assets decorator so that each dlt resource (results, splits)
becomes a tracked Dagster asset in the "bronze" group.  dlt handles:
  - OAuth Bearer auth via the access token
  - Pagination
  - Incremental state (cursor on `date` field)
  - Schema inference + Postgres table creation
  - Merge upsert on primary key `id`

The resulting Postgres tables live in the `concept2_bronze` schema:
  concept2_bronze.results          — one row per workout
  concept2_bronze.results__splits  — child rows (one per split interval)
"""

from __future__ import annotations

import os

import dlt
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
from sqlalchemy import create_engine, text

from concept2_pipeline.sources.concept2_source import concept2_source

# ── DLT pipeline definition ──────────────────────────────────────────────────
# The pipeline is defined at module level so Dagster can inspect it to build
# asset specs before any run starts.

_bronze_pipeline = dlt.pipeline(
    pipeline_name="concept2_bronze",
    destination="postgres",
    dataset_name="concept2_bronze",
    # dlt stores incremental cursor state in Postgres (same DB, _dlt_* tables)
)


@dlt_assets(
    dlt_source=concept2_source(
        access_token=os.getenv("C2_ACCESS_TOKEN", "placeholder"),
    ),
    dlt_pipeline=_bronze_pipeline,
    name="concept2_bronze",
    group_name="bronze",
)
def concept2_bronze_assets(
    context,
    dlt: DagsterDltResource,
) -> None:
    """
    Ingest Concept2 workout data into the bronze Postgres schema.

    Incremental behaviour
    ---------------------
    On first run: fetches all pages from the API (no date filter).
    On subsequent runs: queries concept2_bronze.results for the most
    recent `date` value and passes it as `updated_after` to the API,
    so only new workouts are fetched.
    """
    access_token = os.environ.get("C2_ACCESS_TOKEN", "")
    if not access_token:
        raise RuntimeError(
            "C2_ACCESS_TOKEN environment variable is not set. "
            "Run `python -m concept2_pipeline.auth` to obtain a token."
        )

    # ── Determine incremental cursor ───────────────────────────────────────
    updated_after: str | None = None
    pg_conn = os.environ.get("PG_CONN", "")
    if pg_conn:
        try:
            engine = create_engine(pg_conn, pool_pre_ping=True)
            with engine.connect() as conn:
                # Check table exists first
                exists = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'concept2_bronze'
                          AND table_name   = 'results'
                    )
                """)).scalar()
                if exists:
                    # Discover the actual table name dlt used
                    row = conn.execute(text("""
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'concept2_bronze'
                          AND table_type   = 'BASE TABLE'
                          AND table_name LIKE '%result%'
                        ORDER BY table_name LIMIT 1
                    """)).fetchone()
                    bronze_tbl = row[0] if row else None
                    max_date = None
                    if bronze_tbl:
                        max_date = conn.execute(
                            text(f'SELECT MAX(date) FROM concept2_bronze."{bronze_tbl}"')
                        ).scalar()
                    if max_date is not None:
                        # Format as ISO date string for the API filter
                        updated_after = str(max_date)[:10]  # YYYY-MM-DD
                        context.log.info(
                            f"Incremental run: fetching workouts after {updated_after}"
                        )
                    else:
                        context.log.info("Bronze table exists but is empty — full load.")
                else:
                    context.log.info("First run — performing full load from Concept2 API.")
            engine.dispose()
        except Exception as e:
            context.log.warning(f"Could not determine incremental cursor: {e}. Doing full load.")
    else:
        context.log.warning("PG_CONN not set — cannot determine cursor, doing full load.")

    # ── Run dlt pipeline ───────────────────────────────────────────────────
    yield from dlt.run(
        context=context,
        dlt_source=concept2_source(
            access_token=access_token,
            updated_after=updated_after,
        ),
        dlt_pipeline=_bronze_pipeline,
    )

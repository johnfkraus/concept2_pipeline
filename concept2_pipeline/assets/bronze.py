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
from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets

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
    context: AssetExecutionContext,
    dlt: DagsterDltResource,
) -> None:
    """
    Ingest Concept2 workout data into the bronze Postgres schema.

    Incremental behaviour
    ---------------------
    dlt persists the highest `date` value seen across runs in its internal
    state table (``concept2_bronze._dlt_pipeline_state``).  On every
    subsequent materialization only workouts newer than that cursor are
    fetched from the API, keeping API calls and data transfer minimal.

    To force a full reload (e.g. after a schema change), delete the dlt
    state row for this pipeline or run with ``--full-refresh`` via the CLI.
    """
    # Re-instantiate the source at runtime so the real token is used
    # (avoids the placeholder used at import time for asset-spec generation)
    access_token = os.environ.get("C2_ACCESS_TOKEN", "")
    if not access_token:
        raise RuntimeError(
            "C2_ACCESS_TOKEN environment variable is not set. "
            "Run `python -m concept2_pipeline.auth` to obtain a token."
        )

    yield from dlt.run(
        context=context,
        dlt_source=concept2_source(access_token=access_token),
        dlt_pipeline=_bronze_pipeline,
    )

"""
concept2_pipeline.definitions
==============================
Top-level Dagster Definitions object — the single entry point that
dagster-webserver and dagster-daemon read from workspace.yaml.

Asset groups
------------
  bronze  — raw dlt ingest from Concept2 API → concept2_bronze schema
  silver  — cleaned/typed transform          → concept2_silver schema
  gold    — Plotly charts                    → charts_output/

Schedule
--------
  concept2_daily_refresh  — runs the full bronze → silver → gold pipeline
  every day at 06:00 UTC.  dlt's incremental state ensures only new API
  records are fetched on each run.

Resources
---------
  dlt       — DagsterDltResource (manages dlt pipeline execution)
  postgres  — PostgresResource   (SQLAlchemy engine for silver + gold)
"""

from __future__ import annotations

import os

import dagster as dg
from dagster_embedded_elt.dlt import DagsterDltResource

from concept2_pipeline.assets.bronze import concept2_bronze_assets
from concept2_pipeline.assets.silver import silver_workouts
from concept2_pipeline.assets.gold   import gold_charts
from concept2_pipeline.resources.postgres_resource import PostgresResource


# ── Resources ─────────────────────────────────────────────────────────────────

resources = {
    "dlt": DagsterDltResource(),
    "postgres": PostgresResource(
        connection_string=os.getenv("PG_CONN", "")
        # Falls back to PG_CONN env var inside the resource if left empty
    ),
}


# ── Job (manual materialize-all) ──────────────────────────────────────────────

concept2_full_pipeline_job = dg.define_asset_job(
    name="concept2_full_pipeline",
    selection=dg.AssetSelection.groups("bronze", "silver", "gold"),
    description="Ingest → clean → chart the full Concept2 workout history.",
)

silver_gold_refresh_job = dg.define_asset_job(
    name="silver_gold_refresh_job",
    selection=dg.AssetSelection.groups("silver", "gold"),
    description="Re-build silver + gold whenever bronze is updated.",
)


# ── Schedule ──────────────────────────────────────────────────────────────────
# Runs daily at 06:00 UTC.  Because the bronze layer is incremental, only
# workouts logged since the last successful run are fetched from the API.

concept2_daily_schedule = dg.ScheduleDefinition(
    name="concept2_daily_refresh",
    cron_schedule="0 6 * * *",
    job=concept2_full_pipeline_job,
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description=(
        "Daily incremental refresh: fetches new Concept2 workouts, "
        "rebuilds the silver layer, and regenerates all gold charts."
    ),
)


# ── Sensor: detect new bronze rows and trigger silver+gold ───────────────────
# Optional lightweight alternative to the schedule: triggers silver+gold
# whenever the bronze asset is freshly materialized (e.g. via manual run).

@dg.asset_sensor(
    asset_key=dg.AssetKey(["concept2_bronze", "results"]),
    job=silver_gold_refresh_job,
    name="bronze_results_sensor",
    default_status=dg.DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
)
def bronze_results_sensor(context: dg.SensorEvaluationContext):
    """Fire the silver+gold job whenever bronze results is materialized."""
    return dg.RunRequest(run_key=context.cursor)


# ── Definitions ───────────────────────────────────────────────────────────────

defs = dg.Definitions(
    assets=[
        concept2_bronze_assets,
        silver_workouts,
        gold_charts,
    ],
    resources=resources,
    jobs=[
        concept2_full_pipeline_job,
        silver_gold_refresh_job,
    ],
    schedules=[concept2_daily_schedule],
    sensors=[bronze_results_sensor],
)

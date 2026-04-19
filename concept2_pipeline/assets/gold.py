"""
concept2_pipeline.assets.gold
===============================
Gold layer — publication-ready charts generated from silver data.

Charts produced
---------------
1. monthly_distance_bar      — total metres rowed per calendar month (bar)
2. cumulative_distance_line  — running total distance over time (line)
3. pace_trend_scatter        — pace per 500m per workout (scatter + 30-day MA)
4. heart_rate_trend          — avg HR per workout over time (line, HR workouts only)
5. workout_type_breakdown    — pie/donut of workout count by activity type
6. distance_histogram        — distribution of individual workout distances

Each chart is saved as a PNG to ``charts_output/`` (configurable) AND as a
Plotly interactive HTML.  The PNG paths are surfaced as Dagster asset metadata
so they appear in the asset panel.

All charts read from ``concept2_silver.workouts`` via the PostgresResource.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from dagster import AssetExecutionContext, AssetKey, asset

from concept2_pipeline.resources.postgres_resource import PostgresResource

# Output directory (override via CHARTS_OUTPUT_DIR env var)
CHARTS_DIR = Path(os.getenv("CHARTS_OUTPUT_DIR", "charts_output"))

# Nexus palette (matches the Excel workbook)
TEAL       = "#20808D"
RUST       = "#A84B2F"
DARK_TEAL  = "#1B474D"
MAUVE      = "#944454"
GOLD_COL   = "#FFC553"
COLORS     = [TEAL, RUST, DARK_TEAL, MAUVE, GOLD_COL, "#848456", "#6E522B", "#BCE2E7"]

PLOTLY_TEMPLATE = "plotly_white"


def _save(fig: go.Figure, name: str) -> tuple[str, str]:
    """Save a Plotly figure as both PNG and HTML. Returns (png_path, html_path)."""
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    png  = str(CHARTS_DIR / f"{name}.png")
    html = str(CHARTS_DIR / f"{name}.html")
    fig.write_image(png, width=1200, height=600, scale=2)
    fig.write_html(html)
    return png, html


# ── individual chart functions ────────────────────────────────────────────────

def chart_monthly_distance(df: pd.DataFrame) -> tuple[str, str]:
    monthly = (
        df.groupby(["year", "month"])["distance_m"]
          .sum()
          .reset_index()
    )
    monthly["period"] = pd.to_datetime(
        monthly[["year", "month"]].assign(day=1)
    ).dt.strftime("%Y-%m")
    monthly = monthly.sort_values("period")

    fig = px.bar(
        monthly,
        x="period",
        y="distance_m",
        title="Total Distance per Month",
        labels={"period": "Month", "distance_m": "Distance (m)"},
        color_discrete_sequence=[TEAL],
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        yaxis_title="Distance (m)",
        bargap=0.2,
    )
    return _save(fig, "monthly_distance_bar")


def chart_cumulative_distance(df: pd.DataFrame) -> tuple[str, str]:
    daily = (
        df.groupby("workout_date")["distance_m"]
          .sum()
          .reset_index()
          .sort_values("workout_date")
    )
    daily["cumulative_m"]  = daily["distance_m"].cumsum()
    daily["cumulative_km"] = daily["cumulative_m"] / 1000

    fig = px.area(
        daily,
        x="workout_date",
        y="cumulative_km",
        title="Cumulative Distance Over Time",
        labels={"workout_date": "Date", "cumulative_km": "Cumulative Distance (km)"},
        color_discrete_sequence=[TEAL],
        template=PLOTLY_TEMPLATE,
    )
    fig.update_traces(line_color=DARK_TEAL, fillcolor=f"rgba(32,128,141,0.15)")
    return _save(fig, "cumulative_distance_line")


def chart_pace_trend(df: pd.DataFrame) -> tuple[str, str]:
    pace_df = df[df["pace_seconds_per_500m"].notna()].copy()
    pace_df = pace_df.sort_values("workout_date")
    pace_df["ma30"] = pace_df["pace_seconds_per_500m"].rolling(30, min_periods=5).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=pace_df["workout_date"],
        y=pace_df["pace_seconds_per_500m"],
        mode="markers",
        name="Workout Pace",
        marker=dict(color=TEAL, size=5, opacity=0.5),
    ))
    fig.add_trace(go.Scatter(
        x=pace_df["workout_date"],
        y=pace_df["ma30"],
        mode="lines",
        name="30-workout Moving Avg",
        line=dict(color=RUST, width=2),
    ))
    fig.update_layout(
        title="Pace per 500m Over Time (lower = faster)",
        xaxis_title="Date",
        yaxis_title="Seconds per 500m",
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        yaxis_autorange="reversed",   # lower is better
    )
    return _save(fig, "pace_trend_scatter")


def chart_heart_rate(df: pd.DataFrame) -> tuple[str, str] | None:
    hr_df = df[df["heart_rate_avg"].notna()].copy().sort_values("workout_date")
    if len(hr_df) < 2:
        return None

    fig = go.Figure()
    if hr_df["heart_rate_max"].notna().any():
        fig.add_trace(go.Scatter(
            x=hr_df["workout_date"],
            y=hr_df["heart_rate_max"],
            mode="lines",
            name="Max HR",
            line=dict(color=RUST, width=1, dash="dot"),
            opacity=0.6,
        ))
    fig.add_trace(go.Scatter(
        x=hr_df["workout_date"],
        y=hr_df["heart_rate_avg"],
        mode="lines+markers",
        name="Avg HR",
        line=dict(color=TEAL, width=2),
        marker=dict(size=4),
    ))
    fig.update_layout(
        title="Heart Rate per Workout",
        xaxis_title="Date",
        yaxis_title="BPM",
        template=PLOTLY_TEMPLATE,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    return _save(fig, "heart_rate_trend")


def chart_workout_type_breakdown(df: pd.DataFrame) -> tuple[str, str]:
    counts = df["activity_type_clean"].value_counts().reset_index()
    counts.columns = ["type", "count"]

    fig = px.pie(
        counts,
        names="type",
        values="count",
        title="Workout Count by Activity Type",
        color_discrete_sequence=COLORS,
        hole=0.4,
        template=PLOTLY_TEMPLATE,
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    return _save(fig, "workout_type_breakdown")


def chart_distance_histogram(df: pd.DataFrame) -> tuple[str, str]:
    fig = px.histogram(
        df[df["distance_m"].notna()],
        x="distance_m",
        nbins=40,
        title="Distribution of Workout Distances",
        labels={"distance_m": "Distance (m)", "count": "Workouts"},
        color_discrete_sequence=[TEAL],
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(yaxis_title="Number of Workouts", bargap=0.05)
    return _save(fig, "distance_histogram")


def chart_weekly_volume(df: pd.DataFrame) -> tuple[str, str]:
    """Stacked bar: weekly volume split by activity type."""
    df2 = df.copy()
    df2["workout_date"] = pd.to_datetime(df2["workout_date"])
    df2["week"] = df2["workout_date"].dt.to_period("W").dt.start_time

    weekly = (
        df2.groupby(["week", "activity_type_clean"])["distance_m"]
           .sum()
           .reset_index()
    )

    fig = px.bar(
        weekly,
        x="week",
        y="distance_m",
        color="activity_type_clean",
        title="Weekly Volume by Activity Type",
        labels={"week": "Week", "distance_m": "Distance (m)", "activity_type_clean": "Type"},
        color_discrete_sequence=COLORS,
        template=PLOTLY_TEMPLATE,
    )
    fig.update_layout(
        xaxis_tickangle=-45,
        bargap=0.1,
        legend_title_text="Activity",
    )
    return _save(fig, "weekly_volume_stacked")


# ── Dagster asset ─────────────────────────────────────────────────────────────

@asset(
    name="gold_charts",
    group_name="gold",
    deps=[AssetKey(["silver_workouts"])],
    description=(
        "Publication-ready Plotly charts (PNG + interactive HTML) generated "
        "from silver workout data. Saved to charts_output/."
    ),
    metadata={
        "output_dir": str(CHARTS_DIR),
    },
)
def gold_charts(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> None:
    """
    Generate all gold-layer charts from concept2_silver.workouts.

    Each chart is written as:
      - A high-resolution PNG (for embedding in reports / emails)
      - An interactive Plotly HTML (for sharing / exploration)

    Chart paths are surfaced in the Dagster asset panel metadata.
    """
    context.log.info("Loading silver data…")
    df = postgres.query_df("""
        SELECT
            workout_id,
            workout_date,
            workout_at,
            year,
            month,
            activity_type_clean,
            distance_m,
            distance_km,
            time_tenths,
            pace_seconds_per_500m,
            stroke_rate,
            calories,
            watts,
            heart_rate_avg,
            heart_rate_max,
            heart_rate_min,
            verified
        FROM concept2_silver.workouts
        ORDER BY workout_date ASC
    """)

    if df.empty:
        context.log.warning("No rows in silver_workouts — skipping chart generation.")
        return

    context.log.info(f"Loaded {len(df):,} rows. Generating charts…")

    generated: dict[str, str] = {}

    def _run(fn, *args):
        result = fn(*args)
        if result:
            png, html = result
            name = Path(png).stem
            generated[name] = png
            context.log.info(f"  ✓ {Path(png).name}")

    _run(chart_monthly_distance,        df)
    _run(chart_cumulative_distance,     df)
    _run(chart_pace_trend,              df)
    _run(chart_heart_rate,              df)
    _run(chart_workout_type_breakdown,  df)
    _run(chart_distance_histogram,      df)
    _run(chart_weekly_volume,           df)

    context.log.info(f"Generated {len(generated)} charts → {CHARTS_DIR}/")

    # Surface chart paths in the Dagster UI
    context.add_output_metadata({
        "charts_generated": len(generated),
        "output_dir": str(CHARTS_DIR.resolve()),
        **{name: path for name, path in generated.items()},
    })

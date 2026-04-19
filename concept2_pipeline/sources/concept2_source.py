"""
concept2_pipeline.sources.concept2_source
==========================================
DLT source for the Concept2 Logbook API.

Key design decisions
--------------------
* **Incremental loading** — the `results` resource uses `dlt.sources.incremental`
  keyed on `date` (ISO timestamp string).  On every run dlt automatically
  stores the highest `date` value seen so far in its pipeline state and passes
  it as `updated_after` to the next API request, so we only fetch new workouts.

* **Merge write-disposition** — primary key `id` + incremental cursor `date`
  means dlt will INSERT new rows and UPDATE existing ones if the API ever
  back-fills corrected data.

* **Splits flattened** — the nested `splits` array is emitted as a separate
  child resource (`result_splits`) with a `_parent_id` foreign key so it lands
  in its own Postgres table without exploding the main row width.
"""

from __future__ import annotations

import os
from typing import Iterator

import dlt
import requests

API_BASE    = "https://log.concept2.com/api"
PER_PAGE    = 250          # max allowed by Concept2


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.c2logbook.v1+json",
    }


@dlt.source(name="concept2")
def concept2_source(
    access_token: str = dlt.secrets.value,
    updated_after: str | None = None,
) -> list:
    """
    DLT source yielding Concept2 Logbook resources.

    Parameters
    ----------
    access_token:
        Bearer token from the OAuth flow (stored in dlt secrets or env var).
    updated_after:
        ISO-8601 date string; when provided, only workouts on or after this
        date are fetched.  Managed automatically by dlt's incremental state.
    """
    return [
        results_resource(access_token=access_token, updated_after=updated_after),
    ]


@dlt.resource(
    name="results",
    write_disposition="merge",
    primary_key="id",
    columns={
        "id":             {"data_type": "bigint",    "nullable": False},
        "date":           {"data_type": "timestamp", "nullable": True},
        "distance_m":     {"data_type": "double",    "nullable": True},
        "time_tenths":    {"data_type": "bigint",    "nullable": True},
        "stroke_rate":    {"data_type": "bigint",    "nullable": True},
        "calories":       {"data_type": "bigint",    "nullable": True},
        "watts":          {"data_type": "bigint",    "nullable": True},
        "heart_rate_avg": {"data_type": "bigint",    "nullable": True},
        "heart_rate_max": {"data_type": "bigint",    "nullable": True},
        "heart_rate_min": {"data_type": "bigint",    "nullable": True},
    },
)
def results_resource(
    access_token: str,
    updated_after: dlt.sources.incremental[str] = dlt.sources.incremental(
        "date",
        initial_value=None,   # None = fetch everything on first run
    ),
) -> Iterator[dict]:
    """
    Paginated Concept2 workout results, incrementally loaded by `date`.

    On the first run `updated_after.last_value` is None so all pages are
    fetched.  On subsequent runs only pages newer than the stored cursor are
    requested via the `updated_after` query parameter.
    """
    headers = _headers(access_token)
    params: dict = {"per_page": PER_PAGE}

    if updated_after.last_value is not None:
        # API supports `updated_after` (ISO date) to filter server-side
        params["updated_after"] = updated_after.last_value

    url: str | None = f"{API_BASE}/users/me/results"

    while url:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        for raw in payload.get("data", []):
            hr_obj = raw.get("heart_rate") or {}

            row = {
                "id":              raw.get("id"),
                "date":            raw.get("date"),
                "type":            raw.get("type"),
                "workout_type":    raw.get("workout_type"),
                "distance_m":      raw.get("distance"),
                "time_tenths":     raw.get("time"),
                "time_formatted":  raw.get("time_formatted"),
                "stroke_rate":     raw.get("stroke_rate"),
                "calories":        raw.get("calories"),
                "watts":           raw.get("watts"),
                "heart_rate_avg":  hr_obj.get("average") if isinstance(hr_obj, dict) else None,
                "heart_rate_max":  hr_obj.get("max")     if isinstance(hr_obj, dict) else None,
                "heart_rate_min":  hr_obj.get("min")     if isinstance(hr_obj, dict) else None,
                "source":          raw.get("source"),
                "weight_class":    raw.get("weight_class"),
                "verified":        raw.get("verified"),
                "ranked":          raw.get("ranked"),
                "comments":        raw.get("comments"),
                # Keep raw splits as nested list; dlt will normalise into a
                # child table (concept2__results__splits) automatically.
                "splits":          raw.get("splits") or raw.get("intervals") or [],
            }
            yield row

        # Follow pagination; clear params so the next URL is used as-is
        pagination = payload.get("meta", {}).get("pagination", {})
        url        = pagination.get("links", {}).get("next")
        params     = {}

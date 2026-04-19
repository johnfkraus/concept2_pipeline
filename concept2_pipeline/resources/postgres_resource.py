"""
concept2_pipeline.resources.postgres_resource
==============================================
Dagster resource wrapping a SQLAlchemy engine pointed at Postgres.
Used by the silver and gold layers to run SQL transforms.
"""

from __future__ import annotations

import os

from dagster import ConfigurableResource
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


class PostgresResource(ConfigurableResource):
    """
    Thin SQLAlchemy wrapper.

    Configuration
    -------------
    connection_string : str
        A full SQLAlchemy Postgres URL, e.g.
        ``postgresql://user:pass@host:5432/dbname``
        Defaults to the ``PG_CONN`` environment variable.
    """

    connection_string: str = ""

    def _conn_str(self) -> str:
        return self.connection_string or os.environ["PG_CONN"]

    def get_engine(self) -> Engine:
        return create_engine(self._conn_str(), pool_pre_ping=True)

    def execute(self, sql: str, params: dict | None = None) -> None:
        """Execute a DDL or DML statement."""
        engine = self.get_engine()
        with engine.begin() as conn:
            conn.execute(text(sql), params or {})

    def query_df(self, sql: str, params: dict | None = None):
        """Run a SELECT and return a pandas DataFrame."""
        import pandas as pd
        engine = self.get_engine()
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params or {})

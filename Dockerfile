FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY concept2_pipeline/ ./concept2_pipeline/
COPY workspace.yaml .

RUN pip install --no-cache-dir -e ".[postgres]" 2>/dev/null || pip install --no-cache-dir -e .

RUN pip install --no-cache-dir \
    dagster dagster-webserver dagster-postgres dagster-embedded-elt \
    "dlt[postgres]" requests requests-oauthlib pandas sqlalchemy \
    psycopg2-binary matplotlib plotly kaleido

EXPOSE 3000

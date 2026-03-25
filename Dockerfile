FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1     PYTHONUNBUFFERED=1     PIP_DISABLE_PIP_VERSION_CHECK=1     DBT_PROJECT_DIR=/app/dbt     DBT_PROFILES_DIR=/app/dbt     DUCKDB_PATH=/data/warehouse.duckdb

WORKDIR /app

# OS deps (git is handy for dbt deps; curl for debugging)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# default command: run the Prefect flow
CMD ["python", "-m", "flows.dbt_flow"]

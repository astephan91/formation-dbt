from __future__ import annotations

import os
import subprocess
from pathlib import Path

from prefect import flow, task, get_run_logger


PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/app/dbt"))
PROFILES_DIR = Path(os.getenv("DBT_PROFILES_DIR", "/app/dbt"))


def _run(cmd: list[str], cwd: Path | None = None) -> None:
    logger = get_run_logger()
    logger.info("Running: %s", " ".join(cmd))
    p = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        env={**os.environ},
        text=True,
        capture_output=True,
    )
    if p.stdout:
        logger.info(p.stdout.strip())
    if p.stderr:
        # dbt often writes to stderr even on success; keep it visible
        logger.warning(p.stderr.strip())
    if p.returncode != 0:
        raise RuntimeError(f"Command failed with code {p.returncode}: {' '.join(cmd)}")


@task
def generate_fake_data() -> None:
    _run(["python", "scripts/generate_fake_data.py"], cwd=Path("/app"))


@task
def dbt_debug() -> None:
    _run(["dbt", "debug"], cwd=PROJECT_DIR)


@task
def dbt_deps() -> None:
    # Optional: only useful if you add packages.yml dependencies
    if (PROJECT_DIR / "packages.yml").exists():
        _run(["dbt", "deps"], cwd=PROJECT_DIR)


@task
def dbt_run() -> None:
    _run(["dbt", "run"], cwd=PROJECT_DIR)


@task
def dbt_test() -> None:
    _run(["dbt", "test"], cwd=PROJECT_DIR)


@flow(name="dbt-duckdb-prefect-demo")
def pipeline() -> None:
    generate_fake_data()
    dbt_debug()
    dbt_deps()
    dbt_run()
    dbt_test()


if __name__ == "__main__":
    pipeline()

"""Airflow DAG for the sample data engineering workflow.

This DAG is intentionally simple: it runs the local pipeline script with a
representative set of arguments and assumes the workspace dependencies are
available in the Airflow runtime.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
PYTHON_BIN = WORKSPACE_ROOT / ".venv" / "bin" / "python"
SCRIPT_PATH = WORKSPACE_ROOT / "script.py"
INPUT_PATH = WORKSPACE_ROOT / "data" / "raw" / "orders.csv"
OUTPUT_DIR = WORKSPACE_ROOT / "data" / "processed"
WAREHOUSE_PATH = OUTPUT_DIR / "warehouse" / "warehouse.db"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="data_engineering_pipeline",
    default_args=default_args,
    description="Run the sample CSV-to-Parquet-to-SQL data pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data-engineering", "sample"],
) as dag:
    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command=(
            f'"{PYTHON_BIN}" "{SCRIPT_PATH}" '
            f'--input "{INPUT_PATH}" '
            f'--output-dir "{OUTPUT_DIR}" '
            f'--warehouse-path "{WAREHOUSE_PATH}" '
            f'--primary-key "Order ID" '
            f'--date-column "Order Date" '
            f'--mandatory-columns "Order ID,Order Date"'
        ),
    )

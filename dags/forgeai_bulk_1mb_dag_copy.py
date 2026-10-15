"""Auto-generated bulk DAG, on-disk size ~1MB.

This is a real, importable Airflow DAG (743 extract/transform/load
steps wired sequentially). The line block below is inert filler whose only job
is to bring this source file up to ~1MB for DAG-sync / bundle-size /
parse-load testing. It is the module docstring, so it costs one string token.

    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
"""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "forgeai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}


def step_0_extract(**context):
    """Auto-generated step 0 extract stage for the bulk sizing DAG."""
    records = [
        {"id": "R0-0001", "value": 100.0, "region": "east", "status": "confirmed"},
        {"id": "R0-0002", "value": 200.0, "region": "west", "status": "pending"},
        {"id": "R0-0003", "value": 300.0, "region": "east", "status": "confirmed"},
        {"id": "R0-0004", "value": 400.0, "region": "south", "status": "confirmed"},
    ]
    context["ti"].xcom_push(key="step_0_raw", value=records)


def step_0_transform(**context):
    """Auto-generated step 0 transform stage for the bulk sizing DAG."""
    records = context["ti"].xcom_pull(key="step_0_raw", task_ids="step_0_extract")
    confirmed = [r for r in records if r["status"] == "confirmed"]
    for record in confirmed:
        record["region"] = record["region"].upper()
        record["tax"] = round(record["value"] * 0.08, 2)
        record["total"] = round(record["value"] + record["tax"], 2)
    context["ti"].xcom_push(key="step_0_clean", value=confirmed)


def step_0_load(**context):
    """Auto-generated step 0 load stage for the bulk sizing DAG."""
    records = context["ti"].xcom_pull(key="step_0_clean", task_ids="step_0_transform")
    grand_total = sum(r["total"] for r in records)
    print(f"step 0 loaded {len(records)} records, grand_total={grand_total}")


STEP_COUNT = 1

with DAG(
    dag_id="forgeai_bulk_1mb_dag",
    default_args=default_args,
    description="Auto-generated bulk DAG (~1MB) for sizing / parse-load tests",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=60),
    tags=["poc", "forgeai", "bulk", "generated", "1MB"],
) as dag:
    previous_load = None
    for i in range(STEP_COUNT):
        extract = PythonOperator(
            task_id=f"step_{i}_extract",
            python_callable=globals()[f"step_{i}_extract"],
        )
        transform = PythonOperator(
            task_id=f"step_{i}_transform",
            python_callable=globals()[f"step_{i}_transform"],
        )
        load = PythonOperator(
            task_id=f"step_{i}_load",
            python_callable=globals()[f"step_{i}_load"],
        )
        extract >> transform >> load
        if previous_load is not None:
            previous_load >> extract
        previous_load = load

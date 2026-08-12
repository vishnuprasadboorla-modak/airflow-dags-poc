from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "forgeai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract(**context):
    rows = [{"id": 1, "value": 10}, {"id": 2, "value": 20}]
    context["ti"].xcom_push(key="rows", value=rows)


def transform(**context):
    rows = context["ti"].xcom_pull(key="rows", task_ids="extract")
    total = sum(row["value"] for row in rows)
    context["ti"].xcom_push(key="total", value=total)


def load(**context):
    total = context["ti"].xcom_pull(key="total", task_ids="transform")
    print(f"Loaded total value: {total}")


with DAG(
    dag_id="forgeai_python_etl_dag",
    default_args=default_args,
    description="Simple extract -> transform -> load DAG using XCom",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["poc", "forgeai", "etl"],
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task

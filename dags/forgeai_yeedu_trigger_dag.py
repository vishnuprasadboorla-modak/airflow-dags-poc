from datetime import datetime, timezone

from airflow import DAG
from yeedu.operators.yeedu import YeeduOperator

with DAG(
    dag_id="forgeai_yeedu_trigger_dag",
    description="GitOps-deployed DAG triggering a Yeedu notebook",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["poc", "yeedu", "forgeai"],
) as dag:
    YeeduOperator(
        task_id="run_notebook",
        job_url="https://dev-onprem-008.yeedu.io:8080/tenant/3337654a-ec94-4f4f-9eac-5907d8dae9ed/workspace/18/notebook/4299",
        connection_id="yeedu_login",   # must already exist as an Airflow Connection — never hardcode credentials
    )

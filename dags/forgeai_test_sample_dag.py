"""forgeai_test_sample_dag - two-task chained smoke test DAG (ForgeAI POC)."""

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

DAG_ID = "forgeai_test_sample_dag"

default_args = {
    "owner": "forgeai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def report_result(ti=None, **context):
    """Task 2: pull task 1's XCom and print a run summary."""
    echoed = ti.xcom_pull(task_ids="run_echo") or "<empty>"
    print(f"{DAG_ID} run_id={context.get('run_id')}")
    print(f"run_echo stdout: {echoed.strip()}")


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="POC DAG with two chained tasks - deploy + MCP control smoke test",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,          # manual/MCP-triggered only
    catchup=False,
    tags=["poc", "forgeai", "test"],
) as dag:
    run_echo = BashOperator(
        task_id="run_echo",
        bash_command=f'echo "hello from {DAG_ID}"',
    )
    finish = PythonOperator(
        task_id="finish",
        python_callable=report_result,
    )

    run_echo >> finish

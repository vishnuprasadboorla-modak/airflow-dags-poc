from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="forgeai_hello_world_dag",
    description="Minimal single-task DAG — smoke test for CI/CD deployment",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["poc", "forgeai", "vishnu", "test"],
) as dag:
    BashOperator(task_id="hello", bash_command='echo "Hello from forgeai_hello_world_dag!"')

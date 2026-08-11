from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "forgeai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def print_execution_context(**context):
    print(f"forgeai_poc_dag ran at {context['logical_date']}")


with DAG(
    dag_id="forgeai_poc_dag",
    default_args=default_args,
    description="POC DAG proving CI/CD deployment + Airflow MCP control",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,          # manual/MCP-triggered only, no auto schedule during POC
    catchup=False,
    tags=["poc", "mcp", "forgeai"],
) as dag:
    start = BashOperator(task_id="start", bash_command='echo "starting"')
    process = PythonOperator(task_id="process", python_callable=print_execution_context)
    end = BashOperator(task_id="end", bash_command='echo "done"')

    start >> process >> end

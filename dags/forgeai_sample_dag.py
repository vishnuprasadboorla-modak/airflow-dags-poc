from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "forgeai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract(**context):
    data = {"records": [1, 2, 3, 4, 5]}
    context["ti"].xcom_push(key="sample_data", value=data)
    print(f"extract: produced {len(data['records'])} records")


def transform(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="extract", key="sample_data")
    transformed = [r * 10 for r in data["records"]]
    ti.xcom_push(key="transformed_data", value=transformed)
    print(f"transform: {transformed}")


def load(**context):
    ti = context["ti"]
    transformed = ti.xcom_pull(task_ids="transform", key="transformed_data")
    print(f"load: writing {len(transformed)} transformed records")
    for value in transformed:
        print(f"load: {value}")


with DAG(
    dag_id="forgeai_sample_dag",
    default_args=default_args,
    description="Sample ETL DAG: extract -> transform -> load, for CI/CD smoke test",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,          # manual/MCP-triggered only, no auto schedule during POC
    catchup=False,
    tags=["poc", "forgeai", "sample", "etl"],
) as dag:
    start = BashOperator(task_id="start", bash_command='echo "starting sample ETL"')
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)
    end = BashOperator(task_id="end", bash_command='echo "sample ETL complete"')

    start >> extract_task >> transform_task >> load_task >> end

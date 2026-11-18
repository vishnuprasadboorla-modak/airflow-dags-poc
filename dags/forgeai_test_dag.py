from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    print("Hello from forgeai_test_dag")


with DAG(
    dag_id="forgeai_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["forgeai", "test"],
) as dag:
    hello_task = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello,
    )

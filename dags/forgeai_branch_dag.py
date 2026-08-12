from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    dag_id="forgeai_branch_dag",
    description="Simple branching DAG — picks a path based on a Python callable",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["poc", "forgeai", "branch"],
) as dag:

    def choose_branch():
        return "even_path" if datetime.now(timezone.utc).second % 2 == 0 else "odd_path"

    branch = BranchPythonOperator(task_id="branch", python_callable=choose_branch)
    even_path = BashOperator(task_id="even_path", bash_command='echo "even"')
    odd_path = BashOperator(task_id="odd_path", bash_command='echo "odd"')

    branch >> [even_path, odd_path]

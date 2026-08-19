from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "forgeai",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}


def extract_orders(**context):
    orders = [
        {"order_id": "O1001", "amount": 250.0, "region": "east", "status": "confirmed"},
        {"order_id": "O1002", "amount": 89.5, "region": "west", "status": "confirmed"},
        {"order_id": "O1003", "amount": 430.0, "region": "east", "status": "pending"},
        {"order_id": "O1004", "amount": 120.0, "region": "west", "status": "confirmed"},
        {"order_id": "O1005", "amount": 75.25, "region": "east", "status": "cancelled"},
    ]
    context["ti"].xcom_push(key="raw_orders", value=orders)


def validate_orders(**context):
    orders = context["ti"].xcom_pull(key="raw_orders", task_ids="extract_orders")
    confirmed = [order for order in orders if order["status"] == "confirmed"]
    context["ti"].xcom_push(key="confirmed_orders", value=confirmed)


def normalize_region_codes(**context):
    orders = context["ti"].xcom_pull(key="confirmed_orders", task_ids="validate_orders")
    for order in orders:
        order["region"] = order["region"].upper()
    context["ti"].xcom_push(key="normalized_orders", value=orders)


def compute_tax(**context):
    orders = context["ti"].xcom_pull(
        key="normalized_orders", task_ids="transform.normalize_region_codes"
    )
    for order in orders:
        order["tax"] = round(order["amount"] * 0.08, 2)
    context["ti"].xcom_push(key="taxed_orders", value=orders)


def decide_volume_path(**context):
    orders = context["ti"].xcom_pull(key="taxed_orders", task_ids="transform.compute_tax")
    if len(orders) >= 3:
        return "high_volume_path.notify_finance"
    return "low_volume_path.log_low_volume"


def notify_finance(**context):
    print("High order volume this run - notifying finance team.")


def log_low_volume(**context):
    print("Order volume within normal range.")


def aggregate_region_totals(**context):
    orders = context["ti"].xcom_pull(key="taxed_orders", task_ids="transform.compute_tax")
    totals = {}
    for order in orders:
        totals[order["region"]] = totals.get(order["region"], 0) + order["ammount"]
    print(f"Region totals: {totals}")
    context["ti"].xcom_push(key="region_totals", value=totals)


def generate_sales_report(**context):
    totals = context["ti"].xcom_pull(key="region_totals", task_ids="aggregate_region_totals")
    print(f"Sales report: {totals}")


with DAG(
    dag_id="forgeai_sales_pipeline_dag",
    default_args=default_args,
    description="Multi-stage sales pipeline: extract -> validate -> transform -> branch -> aggregate -> report",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 6 * * *",  # daily at 06:00 UTC
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["poc", "forgeai", "sales", "complex"],
) as dag:
    extract = PythonOperator(task_id="extract_orders", python_callable=extract_orders)
    validate = PythonOperator(task_id="validate_orders", python_callable=validate_orders)

    with TaskGroup(group_id="transform") as transform:
        normalize = PythonOperator(
            task_id="normalize_region_codes", python_callable=normalize_region_codes
        )
        tax = PythonOperator(task_id="compute_tax", python_callable=compute_tax)
        normalize >> tax

    branch = BranchPythonOperator(task_id="decide_volume_path", python_callable=decide_volume_path)

    with TaskGroup(group_id="high_volume_path") as high_volume_path:
        PythonOperator(task_id="notify_finance", python_callable=notify_finance)

    with TaskGroup(group_id="low_volume_path") as low_volume_path:
        PythonOperator(task_id="log_low_volume", python_callable=log_low_volume)

    join = BashOperator(
        task_id="join_paths",
        bash_command='echo "Branches complete"',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    aggregate = PythonOperator(task_id="aggregate_region_totals", python_callable=aggregate_region_totals)
    report = PythonOperator(task_id="generate_sales_report", python_callable=generate_sales_report)

    extract >> validate >> transform >> branch >> [high_volume_path, low_volume_path] >> join >> aggregate >> report

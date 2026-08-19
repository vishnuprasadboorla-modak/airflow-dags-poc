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
    "retry_delay": timedelta(minutes=5),
}


def extract_inventory(**context):
    items = [
        {"sku": "A100", "warehouse": "east", "quantity": 42, "reorder_level": 20},
        {"sku": "B200", "warehouse": "east", "quantity": 5, "reorder_level": 15},
        {"sku": "C300", "warehouse": "west", "quantity": 80, "reorder_level": 30},
        {"sku": "D400", "warehouse": "west", "quantity": 3, "reorder_level": 10},
    ]
    context["ti"].xcom_push(key="raw_items", value=items)


def validate_inventory(**context):
    items = context["ti"].xcom_pull(key="raw_items", task_ids="extract_inventory")
    valid_items = [item for item in items if item["quantity"] >= 0]
    if len(valid_items) != len(items):
        raise ValueError("Inventory validation failed: negative quantity detected")
    context["ti"].xcom_push(key="valid_items", value=valid_items)


def normalize_warehouse_codes(**context):
    items = context["ti"].xcom_pull(key="valid_items", task_ids="validate_inventory")
    for item in items:
        item["warehouse"] = item["warehouse"].upper()
    context["ti"].xcom_push(key="normalized_items", value=items)


def enrich_with_status(**context):
    items = context["ti"].xcom_pull(
        key="normalized_items", task_ids="transform.normalize_warehouse_codes"
    )
    for item in items:
        item["status"] = "low_stock" if item["quantity"] < item["reorder_level"] else "sufficient"
    context["ti"].xcom_push(key="enriched_items", value=items)


def decide_restock_path(**context):
    items = context["ti"].xcom_pull(key="enriched_items", task_ids="transform.enrich_with_status")
    low_stock = [item for item in items if item["status"] == "low_stock"]
    if low_stock:
        return "restock_path.flag_low_stock_items"
    return "sufficient_path.skip_restock"


def flag_low_stock_items(**context):
    items = context["ti"].xcom_pull(key="enriched_items", task_ids="transform.enrich_with_status")
    low_stock_skus = [item["sku"] for item in items if item["status"] == "low_stock"]
    print(f"SKUs flagged for restock: {low_stock_skus}")


def aggregate_by_warehouse(**context):
    items = context["ti"].xcom_pull(key="enriched_items", task_ids="transform.enrich_with_status")
    totals = {}
    for item in items:
        totals[item["warehouse"]] = totals.get(item["warehouse"], 0) + item["quantity"]
    print(f"Warehouse totals: {totals}")
    context["ti"].xcom_push(key="warehouse_totals", value=totals)


def send_summary_report(**context):
    totals = context["ti"].xcom_pull(key="warehouse_totals", task_ids="aggregate_by_warehouse")
    print(f"Inventory summary report: {totals}")


with DAG(
    dag_id="forgeai_inventory_pipeline_dag",
    default_args=default_args,
    description="Multi-stage inventory pipeline: extract -> validate -> transform -> branch -> aggregate -> report",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["poc", "forgeai", "inventory", "complex"],
) as dag:
    extract = PythonOperator(task_id="extract_inventory", python_callable=extract_inventory)
    validate = PythonOperator(task_id="validate_inventory", python_callable=validate_inventory)

    with TaskGroup(group_id="transform") as transform:
        normalize = PythonOperator(
            task_id="normalize_warehouse_codes", python_callable=normalize_warehouse_codes
        )
        enrich = PythonOperator(task_id="enrich_with_status", python_callable=enrich_with_status)
        normalize >> enrich

    branch = BranchPythonOperator(task_id="decide_restock_path", python_callable=decide_restock_path)

    with TaskGroup(group_id="restock_path") as restock_path:
        PythonOperator(task_id="flag_low_stock_items", python_callable=flag_low_stock_items)

    with TaskGroup(group_id="sufficient_path") as sufficient_path:
        BashOperator(task_id="skip_restock", bash_command='echo "No restock needed"')

    join = BashOperator(
        task_id="join_paths",
        bash_command='echo "Branches complete"',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    aggregate = PythonOperator(task_id="aggregate_by_warehouse", python_callable=aggregate_by_warehouse)
    report = PythonOperator(task_id="send_summary_report", python_callable=send_summary_report)

    extract >> validate >> transform >> branch >> [restock_path, sufficient_path] >> join >> aggregate >> report

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


def extract_campaigns(**context):
    campaigns = [
        {"campaign_id": "M1001", "responses": 320, "region": "east", "status": "delivered"},
        {"campaign_id": "M1002", "responses": 85, "region": "west", "status": "delivered"},
        {"campaign_id": "M1003", "responses": 540, "region": "east", "status": "pending"},
        {"campaign_id": "M1004", "responses": 150, "region": "west", "status": "delivered"},
        {"campaign_id": "M1005", "responses": 40, "region": "east", "status": "cancelled"},
    ]
    context["ti"].xcom_push(key="raw_campaigns", value=campaigns)


def validate_campaigns(**context):
    campaigns = context["ti"].xcom_pull(key="raw_campaigns", task_ids="extract_campaigns")
    delivered = [campaign for campaign in campaigns if campaign["status"] == "delivered"]
    context["ti"].xcom_push(key="delivered_campaigns", value=delivered)


def normalize_region_codes(**context):
    campaigns = context["ti"].xcom_pull(key="delivered_campaigns", task_ids="validate_campaigns")
    for campaign in campaigns:
        campaign["region"] = campaign["region"].upper()
    context["ti"].xcom_push(key="normalized_campaigns", value=campaigns)


def compute_response_rate(**context):
    campaigns = context["ti"].xcom_pull(
        key="normalized_campaigns", task_ids="transform.normalize_region_codes"
    )
    for campaign in campaigns:
        campaign["response_rate"] = round(campaign["responses"] / 1000.0, 4)
    context["ti"].xcom_push(key="enriched_campaigns", value=campaigns)


def decide_volume_path(**context):
    campaigns = context["ti"].xcom_pull(
        key="enriched_campaigns", task_ids="transform.compute_response_rate"
    )
    if len(campaigns) >= 3:
        return "high_volume_path.notify_marketing"
    return "low_volume_path.log_low_volume"


def notify_marketing(**context):
    print("High campaign volume this run - notifying marketing team.")


def log_low_volume(**context):
    print("Campaign volume within normal range.")


def aggregate_region_totals(**context):
    campaigns = context["ti"].xcom_pull(
        key="enriched_campaigns", task_ids="transform.compute_response_rate"
    )
    totals = {}
    for campaign in campaigns:
        totals[campaign["region"]] = totals.get(campaign["region"], 0) + campaign["responses"]
    print(f"Region totals: {totals}")
    context["ti"].xcom_push(key="region_totals", value=totals)


def generate_campaign_report(**context):
    totals = context["ti"].xcom_pull(
        key="region_totals", task_ids="aggregate_region_totals"
    )
    print(f"Campaign report: {totals}")


with DAG(
    dag_id="forgeai_marketing_campaign_dag",
    default_args=default_args,
    description="Multi-stage marketing campaign pipeline: extract -> validate -> transform -> branch -> aggregate -> report",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 7 * * *",  # daily at 07:00 UTC
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    tags=["poc", "forgeai", "marketing", "complex"],
) as dag:
    extract = PythonOperator(task_id="extract_campaigns", python_callable=extract_campaigns)
    validate = PythonOperator(task_id="validate_campaigns", python_callable=validate_campaigns)

    with TaskGroup(group_id="transform") as transform:
        normalize = PythonOperator(
            task_id="normalize_region_codes", python_callable=normalize_region_codes
        )
        rate = PythonOperator(
            task_id="compute_response_rate", python_callable=compute_response_rate
        )
        normalize >> rate

    branch = BranchPythonOperator(task_id="decide_volume_path", python_callable=decide_volume_path)

    with TaskGroup(group_id="high_volume_path") as high_volume_path:
        PythonOperator(task_id="notify_marketing", python_callable=notify_marketing)

    with TaskGroup(group_id="low_volume_path") as low_volume_path:
        PythonOperator(task_id="log_low_volume", python_callable=log_low_volume)

    join = BashOperator(
        task_id="join_paths",
        bash_command='echo "Branches complete"',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    aggregate = PythonOperator(
        task_id="aggregate_region_totals", python_callable=aggregate_region_totals
    )
    report = PythonOperator(
        task_id="generate_campaign_report", python_callable=generate_campaign_report
    )

    extract >> validate >> transform >> branch >> [high_volume_path, low_volume_path] >> join >> aggregate >> report

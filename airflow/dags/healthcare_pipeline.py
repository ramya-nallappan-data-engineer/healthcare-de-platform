from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DATABRICKS_CONN_ID = "databricks_default"

# Databricks job IDs (identifiers, not secrets)
JOB_IDS = {
    "bronze": "103976395809527",
    "silver": "982116541770189",
    "dq": "878248122550059",
    "gold": "502995445662100",
    "snowflake_load": "834363963835090",
}

default_args = {
    "owner": "ramya",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def run_job(task_id, job_key, **kwargs):
    """One Databricks job as one Airflow task (start, poll, report result)."""
    return DatabricksRunNowOperator(
        task_id=task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=JOB_IDS[job_key],
        polling_period_seconds=30,
        **kwargs,
    )


with DAG(
    dag_id="healthcare_pipeline",
    description="Bronze -> Silver -> DQ -> Gold -> DQ -> Snowflake",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["healthcare", "databricks"],
) as dag:

    bronze = run_job("bronze_ingest", "bronze")
    silver = run_job("silver_transform", "silver")
    dq_silver = run_job("dq_silver", "dq", notebook_params={"stage": "silver"}, retries=0)
    gold = run_job("gold_build", "gold")
    dq_gold = run_job("dq_gold", "dq", notebook_params={"stage": "gold"}, retries=0)
    snowflake_load = run_job("snowflake_load", "snowflake_load")

    bronze >> silver >> dq_silver >> gold >> dq_gold >> snowflake_load

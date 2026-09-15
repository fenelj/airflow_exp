"""
Demonstrates the "happy path" for an org-scoped DAG under config/airflow_local_settings.py.

Because this file lives in dags/finance/, dag_policy() auto-detects org
"finance" and sets access_control so only Finance_Admin, Finance_User, and
the built-in Op role can see/touch it (Admin always bypasses this). Every
task_type here is on RESTRICTED_ORG_ALLOWLIST, and the Glue job's
iam_role_name / aws_conn_id both match the "finance-" boundary that
task_policy() derives from the folder name -- so this DAG parses cleanly.

The 'pool:finance_etl' tag also exercises the pre-existing pool-routing
half of task_policy() (paired with dags/sync_pool_tags.py, which
auto-creates the pool if it doesn't exist yet).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.operators.empty import EmptyOperator

default_args = {
    "owner": "finance-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finance_etl_pipeline",
    default_args=default_args,
    description="Finance org ETL pipeline -- allowed operator + correct IAM boundary",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "pool:finance_etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_glue_job = GlueJobOperator(
        task_id="run_finance_glue_job",
        job_name="finance-monthly-close-etl",
        iam_role_name="finance-glue-role",  # matches the "finance-" IAM boundary
        aws_conn_id="aws_finance_account",  # matches ORG_BOUNDARIES-derived aws_conn_id
    )

    end = EmptyOperator(task_id="end")

    start >> run_glue_job >> end

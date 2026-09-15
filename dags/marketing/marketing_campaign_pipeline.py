"""
"Happy path" DAG for the marketing org, mirroring
dags/finance/finance_etl_pipeline.py to show that org boundaries are
independent: this file's IAM role / connection id are scoped to
"marketing-" / "aws_marketing_account" (derived from the dags/marketing/
folder name), and Finance_Admin/Finance_User have no access_control entry
here -- only Marketing_Admin, Marketing_User, and the built-in Op role do.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.standard.operators.empty import EmptyOperator

default_args = {
    "owner": "marketing-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="marketing_campaign_pipeline",
    default_args=default_args,
    description="Marketing org campaign trigger -- allowed operator + correct IAM boundary",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketing"],
) as dag:

    start = EmptyOperator(task_id="start")

    send_campaign = LambdaInvokeFunctionOperator(
        task_id="send_campaign_email",
        function_name="marketing-ds-send_campaign",  # matches the "marketing-ds-" lambda boundary
        aws_conn_id="aws_marketing_account",  # matches ORG_BOUNDARIES-derived aws_conn_id
    )

    end = EmptyOperator(task_id="end")

    start >> send_campaign >> end

"""
INTENTIONALLY BROKEN -- demonstrates the *other* half of task_policy():
deep recursive IAM inspection, not just the operator allowlist.

GlueJobOperator IS on the allowlist, so this passes the first check. But
iam_role_name below is "finance-glue-role" while this file lives under
dags/marketing/ -- a cross-org privilege escalation attempt (a Marketing
DAG trying to assume a Finance IAM role). _inspect_for_unauthorized_roles()
walks every argument on the task (not just the obvious ones) looking for
IAM-role-shaped keys and catches it, raising
AirflowClusterPolicyViolation at parse time.

Expect this DAG under Browse -> DAG Import Errors. Delete this file once
you've seen it, it isn't meant to run.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

with DAG(
    dag_id="marketing_cross_org_iam_demo",
    description="Demo: an allowed operator using another org's IAM role is still blocked",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketing", "demo"],
) as dag:

    not_allowed = GlueJobOperator(
        task_id="not_allowed",
        job_name="marketing-attribution-etl",
        iam_role_name="finance-glue-role",  # wrong org's IAM role -- should be blocked
        aws_conn_id="aws_marketing_account",
    )

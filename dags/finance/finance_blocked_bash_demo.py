"""
INTENTIONALLY BROKEN -- demonstrates task_policy()'s operator allowlist
actively rejecting a DAG, rather than just describing the rule.

BashOperator runs arbitrary shell code directly on the shared Airflow
worker, which is exactly what org DAGs are barred from doing (they must
offload execution to a managed AWS service instead). Because this file
lives under dags/finance/, task_policy() detects org "finance" and raises
AirflowClusterPolicyViolation the moment the dag-processor parses it.

Expect this DAG to show up under Browse -> DAG Import Errors (or
`airflow dags list-import-errors`), NOT in the normal DAG list. That's the
policy working as intended -- delete this file once you've seen it, it
isn't meant to run.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="finance_blocked_bash_demo",
    description="Demo: BashOperator is not on the finance org's allowlist",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["finance", "demo"],
) as dag:

    not_allowed = BashOperator(
        task_id="not_allowed",
        bash_command="echo 'this should never parse successfully'",
    )

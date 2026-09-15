# config/airflow_local_settings.py

import logging
from airflow.models import DAG
from airflow.exceptions import AirflowClusterPolicyViolation

log = logging.getLogger(__name__)

# 1. Define the exact Operator class names that are permitted.
# Note: Always include 'EmptyOperator' so they can structure their DAG flow.
RESTRICTED_ORG_ALLOWLIST = {
    "LambdaInvokeFunctionOperator",
    "GlueJobOperator",
    "GlueDataBrewJobOperator",
    "EcsRunTaskOperator",
    "EmptyOperator" 
}

def dag_policy(dag: DAG):
    """
    (Your existing DAG policy code for RBAC goes here)
    """
    pass

def task_policy(task):
    """
    Cluster Policy to enforce operator restrictions on a per-task level.
    This protects the Airflow ECS worker by offloading execution to AWS services.
    """
    # Safety check: Ensure the task is attached to a DAG
    if not task.dag or not hasattr(task.dag, 'fileloc'):
        return

    file_path = task.dag.fileloc.lower()
    
    # Check if the task originates from the finance or marketing folders/bundles
    if 'finance' in file_path or 'marketing' in file_path:
        
        # task.task_type returns the class name of the operator (e.g., 'BashOperator')
        if task.task_type not in RESTRICTED_ORG_ALLOWLIST:
            
            error_msg = (
                f"Security Violation in DAG '{task.dag.dag_id}', Task '{task.task_id}': "
                f"The '{task.task_type}' is not permitted. "
                f"Your organization is restricted to off-worker execution using: "
                f"{', '.join(RESTRICTED_ORG_ALLOWLIST)}."
            )
            
            log.warning(error_msg)
            
            # This exception stops the DAG from parsing and displays the error in the UI
            raise AirflowClusterPolicyViolation(error_msg)

# config/airflow_local_settings.py

import logging
from airflow.models import DAG
from airflow.exceptions import AirflowClusterPolicyViolation

log = logging.getLogger(__name__)

RESTRICTED_ORG_ALLOWLIST = {
    # Core
    "EmptyOperator",
    
    # Existing AWS Execution Allowlist
    "LambdaInvokeFunctionOperator",
    "GlueJobOperator",
    "EcsRunTaskOperator",
    
    # --- ADDED: EMR on EC2 (Standard EMR) ---
    "EmrCreateJobFlowOperator",
    "EmrAddStepsOperator",
    "EmrTerminateJobFlowOperator",
    "EmrModifyClusterOperator",
    "EmrJobFlowSensor",
    "EmrStepSensor",
    
    # --- ADDED: EMR Serverless ---
    "EmrServerlessCreateApplicationOperator",
    "EmrServerlessStartJobOperator",
    "EmrServerlessDeleteApplicationOperator",
    "EmrServerlessJobSensor",
    "EmrServerlessApplicationSensor",

    # --- ADDED: EMR on EKS (Containers) ---
    "EmrEksCreateClusterOperator",
    "EmrContainerOperator",
    "EmrContainerSensor"
}

def task_policy(task):
    if not task.dag or not hasattr(task.dag, 'fileloc'):
        return

    file_path = task.dag.fileloc.lower()
    
    if 'finance' in file_path or 'marketing' in file_path:
        # 1. Base Operator Check
        if task.task_type not in RESTRICTED_ORG_ALLOWLIST:
            raise AirflowClusterPolicyViolation(
                f"Operator '{task.task_type}' is not permitted. "
                f"Your organization is restricted to off-worker execution."
            )
            
        # 2. (Optional) Deep Parameter Inspection for EMR
        # Example: Enforcing an IAM role boundary for EMR Serverless
        if task.task_type == "EmrServerlessStartJobOperator":
            execution_role = getattr(task, 'execution_role_arn', '')
            if not execution_role.startswith("arn:aws:iam::123456789012:role/finance-emr-"):
                raise AirflowClusterPolicyViolation(
                    f"Security Violation: You must use an approved Finance EMR IAM role. "
                    f"Attempted to use '{execution_role}'."
                )

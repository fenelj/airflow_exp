# config/airflow_local_settings.py

import logging
from airflow.models import DAG
from airflow.exceptions import AirflowClusterPolicyViolation

log = logging.getLogger(__name__)

# Define exact boundaries for the Finance team
FINANCE_IAM_PREFIX = "arn:aws:iam::123456789012:role/finance-"
FINANCE_AWS_CONN_ID = "aws_finance_account"
FINANCE_LAMBDA_PREFIX = "finance-ds-"

def task_policy(task):
    if not task.dag or not hasattr(task.dag, 'fileloc'):
        return

    file_path = task.dag.fileloc.lower()
    
    if 'finance' in file_path:
        
        # --- 1. Global AWS Connection Check ---
        # Prevent the Finance team from using another team's Airflow AWS connection (which might assume a different role)
        aws_conn = getattr(task, 'aws_conn_id', None)
        if aws_conn and aws_conn != FINANCE_AWS_CONN_ID:
            raise AirflowClusterPolicyViolation(
                f"Security Violation: You must use '{FINANCE_AWS_CONN_ID}'. Attempted to use '{aws_conn}'."
            )

        # --- 2. AWS Glue Job Operator ---
        if task.task_type == "GlueJobOperator":
            iam_role_arn = getattr(task, 'iam_role_arn', None)
            iam_role_name = getattr(task, 'iam_role_name', None)
            
            if iam_role_arn and not iam_role_arn.startswith(FINANCE_IAM_PREFIX):
                raise AirflowClusterPolicyViolation(f"Unauthorized Glue IAM Role ARN: {iam_role_arn}")
            
            if iam_role_name and not iam_role_name.startswith("finance-"):
                raise AirflowClusterPolicyViolation(f"Unauthorized Glue IAM Role Name: {iam_role_name}")

        # --- 3. EMR on EC2 (Standard EMR) ---
        elif task.task_type == "EmrCreateJobFlowOperator":
            overrides = getattr(task, 'job_flow_overrides', {})
            
            job_flow_role = overrides.get('JobFlowRole', '')
            service_role = overrides.get('ServiceRole', '')
            
            if not job_flow_role.startswith("finance-") or not service_role.startswith("finance-"):
                raise AirflowClusterPolicyViolation(
                    f"Security Violation: EMR JobFlowRole and ServiceRole must start with 'finance-'. "
                    f"Got JobFlowRole: '{job_flow_role}', ServiceRole: '{service_role}'."
                )

        # --- 4. AWS Lambda Invoke ---
        elif task.task_type == "LambdaInvokeFunctionOperator":
            # Lambda execution roles are bound to the function in AWS, not passed at runtime.
            # Therefore, we restrict which Lambda functions they are allowed to invoke.
            function_name = getattr(task, 'function_name', '')
            
            if not function_name.startswith(FINANCE_LAMBDA_PREFIX):
                raise AirflowClusterPolicyViolation(
                    f"Security Violation: Finance can only invoke Lambdas prefixed with '{FINANCE_LAMBDA_PREFIX}'. "
                    f"Attempted to invoke '{function_name}'."
                )

# config/airflow_local_settings.py

import logging
from airflow.models import DAG
from airflow.exceptions import AirflowClusterPolicyViolation

log = logging.getLogger(__name__)

RESTRICTED_ORG_ALLOWLIST = {
    "LambdaInvokeFunctionOperator",
    "GlueJobOperator",
    "EcsRunTaskOperator",
    "EmptyOperator" 
}

# Define your secure infrastructure boundaries
APPROVED_ECR_REGISTRY = "123456789012.dkr.ecr.us-east-1.amazonaws.com/finance-approved/"
FINANCE_CLUSTER = "finance-production-cluster"

def task_policy(task):
    if not task.dag or not hasattr(task.dag, 'fileloc'):
        return

    file_path = task.dag.fileloc.lower()
    
    # 1. Base Operator Check
    if 'finance' in file_path:
        if task.task_type not in RESTRICTED_ORG_ALLOWLIST:
            raise AirflowClusterPolicyViolation(
                f"Operator '{task.task_type}' is not permitted."
            )
            
        # 2. Deep Parameter Inspection for ECS
        if task.task_type == "EcsRunTaskOperator":
            
            # --- Check the ECS Cluster ---
            cluster = getattr(task, 'cluster', None)
            if cluster != FINANCE_CLUSTER:
                raise AirflowClusterPolicyViolation(
                    f"Security Violation: Finance DAGs must run on '{FINANCE_CLUSTER}'. "
                    f"Attempted to use '{cluster}'."
                )

            # --- Check the Task Definition Naming Convention ---
            # Ensures they are only invoking Task Definitions provisioned for their role boundary
            task_definition = getattr(task, 'task_definition', '')
            if not task_definition.startswith("finance-ds-"):
                raise AirflowClusterPolicyViolation(
                    f"Security Violation: Finance task definitions must start with 'finance-ds-'. "
                    f"Attempted to use '{task_definition}'."
                )

            # --- Inspect Container Overrides for Rogue Images ---
            # EcsRunTaskOperator allows users to override the image at runtime. We must catch this.
            overrides = getattr(task, 'overrides', {})
            container_overrides = overrides.get('containerOverrides', [])
            
            for container in container_overrides:
                image = container.get('image')
                
                # If they supplied an image override, force it to be from the approved private ECR
                if image and not image.startswith(APPROVED_ECR_REGISTRY):
                    raise AirflowClusterPolicyViolation(
                        f"Security Violation: Docker image '{image}' is not permitted. "
                        f"All images must be pulled from '{APPROVED_ECR_REGISTRY}'."
                    )

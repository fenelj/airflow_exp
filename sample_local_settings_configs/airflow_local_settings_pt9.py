# config/airflow_local_settings.py

import logging
from airflow.models import DAG
from airflow.exceptions import AirflowClusterPolicyViolation

log = logging.getLogger(__name__)

# ==========================================
# 1. GLOBAL CONSTANTS & ALLOWLISTS
# ==========================================

# Operators that the Finance and Marketing orgs are permitted to use
RESTRICTED_ORG_ALLOWLIST = {
    # Core
    "EmptyOperator",
    
    # ECS (Run & Provisioning)
    "EcsRunTaskOperator",
    "EcsCreateClusterOperator",
    "EcsRegisterTaskDefinitionOperator",
    "EcsDeregisterTaskDefinitionOperator",
    "EcsDeleteClusterOperator",
    
    # Lambda (Invoke & Provisioning)
    "LambdaInvokeFunctionOperator",
    "LambdaCreateFunctionOperator",
    "LambdaDeleteFunctionOperator",
    
    # Glue
    "GlueJobOperator",
    "GlueDataBrewStartJobOperator",
    
    # EMR on EC2
    "EmrCreateJobFlowOperator",
    "EmrAddStepsOperator",
    "EmrTerminateJobFlowOperator",
    "EmrModifyClusterOperator",
    "EmrJobFlowSensor",
    "EmrStepSensor",
    
    # EMR Serverless
    "EmrServerlessCreateApplicationOperator",
    "EmrServerlessStartJobOperator",
    "EmrServerlessDeleteApplicationOperator",
    "EmrServerlessJobSensor",
    "EmrServerlessApplicationSensor",

    # EMR on EKS
    "EmrEksCreateClusterOperator",
    "EmrContainerOperator",
    "EmrContainerSensor"
}

# --- Finance Boundaries ---
FINANCE_IAM_ARN_PREFIX = "arn:aws:iam::123456789012:role/finance-"
FINANCE_ROLE_NAME_PREFIX = "finance-"
FINANCE_CLUSTER_PREFIX = "finance-"
FINANCE_AWS_CONN_ID = "aws_finance_account"
FINANCE_LAMBDA_PREFIX = "finance-ds-"
FINANCE_TASK_DEF_PREFIX = "finance-ds-"
APPROVED_ECR_REGISTRY = "123456789012.dkr.ecr.us-east-1.amazonaws.com/finance-approved/"

# Restricted IAM keys that could be hidden in nested JSON/Dicts
RESTRICTED_ROLE_KEYS = {
    "role", 
    "jobflowrole", 
    "servicerole", 
    "executionrolearn", 
    "execution_role_arn",
    "rolearn",
    "iamrolename",
    "iam_role_name",
    "iam_role_arn"
}

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def inspect_for_unauthorized_roles(data, org_arn_prefix, org_name_prefix):
    """
    Recursively scans nested dictionaries and lists for any AWS role keys.
    Returns (True, None) if safe, or (False, error_message) if a violation is found.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if str(key).lower() in RESTRICTED_ROLE_KEYS:
                if isinstance(value, str):
                    if not (value.startswith(org_arn_prefix) or value.startswith(org_name_prefix)):
                        return False, f"Unauthorized role '{value}' found in nested key '{key}'."
            
            is_valid, error_msg = inspect_for_unauthorized_roles(value, org_arn_prefix, org_name_prefix)
            if not is_valid:
                return False, error_msg

    elif isinstance(data, (list, tuple)):
        for item in data:
            is_valid, error_msg = inspect_for_unauthorized_roles(item, org_arn_prefix, org_name_prefix)
            if not is_valid:
                return False, error_msg

    return True, None

# ==========================================
# 3. DAG POLICY (RBAC ENFORCEMENT)
# ==========================================

def dag_policy(dag: DAG):
    """
    Automatically assigns DAG-level access control based on the file's location.
    """
    file_path = dag.fileloc.lower()
    bundle_name = getattr(dag, 'bundle_name', '')
    
    if 'finance' in file_path or bundle_name == 'finance_team_bundle':
        dag.access_control = {
            "Finance_Admin": {"can_read", "can_edit", "can_delete"},
            "Finance_Viewer": {"can_read"},
            "Orchestration_Admin": {"can_read", "can_edit", "can_delete"}
        }
        log.info(f"Enforced Finance RBAC on DAG: {dag.dag_id}")

    elif 'marketing' in file_path or bundle_name == 'marketing_team_bundle':
        dag.access_control = {
            "Marketing_Admin": {"can_read", "can_edit", "can_delete"},
            "Marketing_Viewer": {"can_read"},
            "Orchestration_Admin": {"can_read", "can_edit", "can_delete"}
        }
        log.info(f"Enforced Marketing RBAC on DAG: {dag.dag_id}")
        
    else:
        dag.access_control = {
            "Orchestration_Admin": {"can_read", "can_edit", "can_delete"}
        }
        log.warning(f"Uncategorized DAG {dag.dag_id} found. Restricted to Admins only.")

# ==========================================
# 4. TASK POLICY (INFRASTRUCTURE SECURITY)
# ==========================================

def task_policy(task):
    """
    Enforces operator allowlists and deep parameter inspection for AWS infrastructure.
    """
    if not task.dag or not hasattr(task.dag, 'fileloc'):
        return

    file_path = task.dag.fileloc.lower()
    
    if 'finance' in file_path:
        
        # --- A. BASE OPERATOR CHECK ---
        if task.task_type not in RESTRICTED_ORG_ALLOWLIST:
            raise AirflowClusterPolicyViolation(
                f"Security Violation in DAG '{task.dag.dag_id}', Task '{task.task_id}': "
                f"The '{task.task_type}' is not permitted."
            )
            
        # --- B. DEEP RECURSIVE IAM ROLE INSPECTION ---
        is_valid, error_msg = inspect_for_unauthorized_roles(
            task.__dict__, 
            FINANCE_IAM_ARN_PREFIX, 
            FINANCE_ROLE_NAME_PREFIX
        )
        if not is_valid:
            raise AirflowClusterPolicyViolation(
                f"Security Violation in DAG '{task.dag.dag_id}', Task '{task.task_id}': {error_msg}"
            )

        # --- C. GLOBAL AWS CONNECTION ID CHECK ---
        aws_conn = getattr(task, 'aws_conn_id', None)
        if aws_conn and aws_conn != FINANCE_AWS_CONN_ID:
            raise AirflowClusterPolicyViolation(
                f"Security Violation: You must use '{FINANCE_AWS_CONN_ID}'. Attempted to use '{aws_conn}'."
            )

        # --- D. ECS RUN & PROVISIONING INSPECTIONS ---
        if task.task_type.startswith("Ecs"):
            
            # Check Cluster Name (For Run, Create, Delete operators)
            cluster = getattr(task, 'cluster', getattr(task, 'cluster_name', ''))
            if cluster and not cluster.startswith(FINANCE_CLUSTER_PREFIX):
                raise AirflowClusterPolicyViolation(
                    f"Security Violation: Finance clusters must start with '{FINANCE_CLUSTER_PREFIX}'. "
                    f"Attempted to use '{cluster}'."
                )

            # Check Task Definition Family (For Run, Register, Deregister operators)
            task_definition = getattr(task, 'task_definition', getattr(task, 'family', ''))
            if task_definition and not task_definition.startswith(FINANCE_TASK_DEF_PREFIX):
                raise AirflowClusterPolicyViolation(
                    f"Security Violation: Finance task definitions must start with '{FINANCE_TASK_DEF_PREFIX}'. "
                    f"Attempted to use '{task_definition}'."
                )

            # Inspect Container Image Overrides (For EcsRunTaskOperator)
            overrides = getattr(task, 'overrides', {})
            container_overrides = overrides.get('containerOverrides', [])
            for container in container_overrides:
                image = container.get('image')
                if image and not image.startswith(APPROVED_ECR_REGISTRY):
                    raise AirflowClusterPolicyViolation(
                        f"Security Violation: Docker image '{image}' is not permitted. "
                        f"All images must be pulled from '{APPROVED_ECR_REGISTRY}'."
                    )

        # --- E. LAMBDA INVOKE & PROVISIONING INSPECTIONS ---
        elif task.task_type.startswith("Lambda"):
            function_name = getattr(task, 'function_name', '')
            if function_name and not function_name.startswith(FINANCE_LAMBDA_PREFIX):
                raise AirflowClusterPolicyViolation(
                    f"Security Violation: Finance Lambda functions must be prefixed with '{FINANCE_LAMBDA_PREFIX}'. "
                    f"Attempted to use '{function_name}'."
                )

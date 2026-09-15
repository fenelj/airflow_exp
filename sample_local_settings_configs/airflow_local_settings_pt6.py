# config/airflow_local_settings.py

import logging
from airflow.models import DAG
from airflow.exceptions import AirflowClusterPolicyViolation

log = logging.getLogger(__name__)

FINANCE_IAM_ARN_PREFIX = "arn:aws:iam::123456789012:role/finance-"
FINANCE_ROLE_NAME_PREFIX = "finance-"

# A lowercase set of all common keys AWS uses to define IAM roles.
# This prevents users from trying different casing like "Role" vs "role".
RESTRICTED_ROLE_KEYS = {
    "role", 
    "jobflowrole", 
    "servicerole", 
    "executionrolearn", 
    "rolearn",
    "iamrolename",
    "iam_role_name",
    "iam_role_arn"
}

def inspect_for_unauthorized_roles(data):
    """
    Recursively scans nested dictionaries and lists for any AWS role keys.
    Returns (True, None) if safe, or (False, error_message) if a violation is found.
    """
    if isinstance(data, dict):
        for key, value in data.items():
            
            # Case-insensitive check to catch the restricted keys
            if str(key).lower() in RESTRICTED_ROLE_KEYS:
                if isinstance(value, str):
                    # Check if the role violates both the ARN prefix and the Name prefix
                    if not (value.startswith(FINANCE_IAM_ARN_PREFIX) or value.startswith(FINANCE_ROLE_NAME_PREFIX)):
                        return False, f"Unauthorized role '{value}' found in nested key '{key}'."
            
            # Recurse into nested dictionaries
            is_valid, error_msg = inspect_for_unauthorized_roles(value)
            if not is_valid:
                return False, error_msg

    # Also recurse into lists and tuples, as dictionaries can be hidden inside them
    elif isinstance(data, (list, tuple)):
        for item in data:
            is_valid, error_msg = inspect_for_unauthorized_roles(item)
            if not is_valid:
                return False, error_msg

    return True, None

# config/airflow_local_settings.py

import logging
from airflow.models import DAG

log = logging.getLogger(__name__)

def dag_policy(dag: DAG):
    """
    Cluster Policy to enforce DAG-level access control based on file path or bundle.
    This guarantees users cannot accidentally or maliciously expose their DAGs.
    """
    # 1. Identify where the DAG came from
    # In Airflow 3 with bundles, dag.bundle_name tells you the origin.
    # In Airflow 2 with subfolders, use dag.fileloc (e.g., '/opt/airflow/dags/finance/...')
    
    file_path = dag.fileloc.lower()
    
    # 2. Apply rules for Finance Org
    if 'finance' in file_path or getattr(dag, 'bundle_name', '') == 'finance_team_bundle':
        dag.access_control = {
            "Finance_Admin": {"can_read", "can_edit", "can_delete"},
            "Finance_Viewer": {"can_read"},
            "Orchestration_Admin": {"can_read", "can_edit", "can_delete"} # Give Class 2 visibility
        }
        log.info(f"Enforced Finance RBAC on DAG: {dag.dag_id}")

    # 3. Apply rules for Marketing Org
    elif 'marketing' in file_path or getattr(dag, 'bundle_name', '') == 'marketing_team_bundle':
        dag.access_control = {
            "Marketing_Admin": {"can_read", "can_edit", "can_delete"},
            "Marketing_Viewer": {"can_read"},
            "Orchestration_Admin": {"can_read", "can_edit", "can_delete"} # Give Class 2 visibility
        }
        log.info(f"Enforced Marketing RBAC on DAG: {dag.dag_id}")
        
    # 4. Fail-safe: If a DAG is dropped outside a designated org folder, hide it from everyone
    # except System Admins (Class 1) and Orchestration Admins (Class 2).
    else:
        dag.access_control = {
            "Orchestration_Admin": {"can_read", "can_edit", "can_delete"}
        }
        log.warning(f"Uncategorized DAG {dag.dag_id} found. Restricted to Admins only.")

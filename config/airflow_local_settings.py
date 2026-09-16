"""
Airflow cluster policies: DAG-level RBAC + task-level infrastructure governance.

Implements the 4-role access model for this deployment:
  1. Full Admin - FAB built-in "Admin" role. Not referenced below: Admin
     bypasses per-DAG access_control entirely and can edit Configurations.
  2. Operator   - FAB built-in "Op" role. dag_policy() grants it read/edit/
     delete on every DAG; Op has no Configurations-edit permission by
     default, so it can run/manage everything without touching core config.
  3. Org Admin  - "<Org>_Admin". Full read/edit/delete, but only on DAGs
     that live in that org's subfolder.
  4. Org User   - "<Org>_User". Read + edit (view/trigger, no delete),
     only on DAGs in that org's boundary.

Orgs are NOT explicitly configured here. Each DAG's org is whatever
top-level subfolder of dags/ it lives in (dags/finance/... -> org
"finance"; a DAG directly in dags/ has no org and is treated as a
platform/core DAG). Drop a new subfolder in and it becomes a fully governed
org automatically -- role names and AWS boundary strings (IAM prefix,
connection id, cluster/lambda/task-def prefixes) are all derived from the
folder name (see _org_boundary()). Folders starting with "_" or "." are
skipped (e.g. use dags/_lambda_scripts/ for support code that isn't itself
a DAG, so it isn't mistaken for an org).

IMPORTANT: dag_policy() only *assigns* access_control -- it does not, and
cannot, create the "<Org>_Admin"/"<Org>_User" roles it references. FAB's
access_control sync (FabAirflowSecurityManagerOverride._sync_dag_view_permissions)
raises if a referenced role doesn't already exist; it never auto-creates
one. dags/sync_org_roles.py is the other half of this: a periodic
maintenance DAG that scans the same dags/ subfolders and provisions any
missing org role before dag_policy() ever references it. If you add a new
org folder, either wait for that DAG's next run or trigger it manually
(`airflow dags test admin_sync_org_roles`) before expecting DAGs in the new
folder to parse successfully.

Supersedes dags/sync_tags_to_roles.py's tag-based '<Org>_Editor' role sync,
which has been removed: bundle/path-scoped access_control below is the
single source of truth for org RBAC now.
"""

import logging
import os
from functools import lru_cache

from airflow.configuration import conf
from airflow.exceptions import AirflowClusterPolicyViolation
from airflow.models import DAG

log = logging.getLogger(__name__)

# Without a named DAG bundle, dag.relative_fileloc is actually absolute
# (equal to dag.fileloc) -- so subfolder detection strips this configured
# root itself rather than trusting relative_fileloc to already be relative.
DAGS_FOLDER = os.path.abspath(conf.get("core", "dags_folder"))

# ==========================================
# 1. GLOBAL CONSTANTS & ALLOWLISTS
# ==========================================

# FAB built-in role reused for the "Operator" tier (see module docstring).
OPERATOR_ROLE = "Op"

# Operators every org is permitted to use. Deliberately excludes anything
# that executes arbitrary code on the shared Airflow worker (BashOperator,
# PythonOperator, DockerOperator, ...) -- org DAGs must offload execution to
# a managed AWS service instead. EmptyOperator is kept for DAG structuring.
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
    "EmrContainerSensor",

    # Step Functions (all operators, plus the sensor for the execution they start)
    "StepFunctionStartExecutionOperator",
    "StepFunctionGetExecutionOutputOperator",
    "StepFunctionExecutionSensor",

    # Neptune (all available operators -- no sensor module exists for this
    # provider version)
    "NeptuneStartDbClusterOperator",
    "NeptuneStopDbClusterOperator",

    # Glue Data Catalog (create-only -- deletes intentionally excluded:
    # task_policy has no resource-name-prefix check for catalog databases/
    # tables, so an org DAG could otherwise delete another org's catalog
    # entries by name)
    "GlueCatalogCreateDatabaseOperator",
    "GlueCatalogCreateTableOperator",
    "GlueCatalogCreatePartitionOperator",
    "GlueCrawlerOperator",

    # Glue Data Quality
    "GlueDataQualityOperator",
    "GlueDataQualityRuleRecommendationRunOperator",
    "GlueDataQualityRuleSetEvaluationRunOperator",

    # RDS (lifecycle start/create only -- deletes intentionally excluded:
    # task_policy has no resource-name-prefix check for RDS instance/
    # snapshot ids, so an org DAG could otherwise delete another org's
    # database by name)
    "RdsCreateDbInstanceOperator",
    "RdsStartDbOperator",
    "RdsStopDbOperator",
    "RdsCreateDbSnapshotOperator",
    "RdsStartExportTaskOperator",
    "RdsSnapshotExistenceSensor",
    "RdsExportTaskExistenceSensor",
}

# Lowercase set of all common keys AWS operators use to carry an IAM role,
# so a nested/renamed kwarg can't smuggle an out-of-boundary role past the
# top-level checks below.
RESTRICTED_ROLE_KEYS = {
    "role",
    "jobflowrole",
    "servicerole",
    "executionrolearn",
    "execution_role_arn",
    "rolearn",
    "iamrolename",
    "iam_role_name",
    "iam_role_arn",
}

# AWS account/partition these ARNs live under. Set via AWS_ACCOUNT_ID /
# AWS_PARTITION env vars (see .env) -- partition defaults to "aws"
# (commercial); use "aws-us-gov" or "aws-cn" for GovCloud/China. If
# AWS_ACCOUNT_ID isn't set, every org's IAM prefix fails closed (matches
# no real ARN) rather than silently accepting any account.
AWS_ACCOUNT_ID = os.environ.get("AWS_ACCOUNT_ID", "")
AWS_PARTITION = os.environ.get("AWS_PARTITION", "aws")


@lru_cache(maxsize=None)
def _org_boundary(org_key):
    """Derives an org's role names and IAM/cluster/lambda boundary purely
    from its key (the dags/ subfolder name) -- '<org>-' prefixes,
    'aws_<org>_account' connection, '<org>-ds-' names, 'Org_Admin'/
    'Org_User' roles. Cached since it's rebuilt on every task/DAG parse."""
    title = org_key.capitalize()
    return {
        "admin_role": f"{title}_Admin",
        "user_role": f"{title}_User",
        "iam_arn_prefix": f"arn:{AWS_PARTITION}:iam::{AWS_ACCOUNT_ID}:role/{org_key}-",
        "iam_name_prefix": f"{org_key}-",
        "cluster_prefix": f"{org_key}-",
        "aws_conn_id": f"aws_{org_key}_account",
        "lambda_prefix": f"{org_key}-ds-",
        "task_def_prefix": f"{org_key}-ds-",
    }


# ==========================================
# 2. HELPERS
# ==========================================

def _get_org_for_dag(dag):
    """Returns the org key this DAG belongs to -- the top-level subfolder
    of dags/ its file lives in -- or None if it's directly in dags/ (i.e.
    a platform/core DAG). No org list to maintain: any subfolder works,
    except ones starting with "_" or "." (for non-DAG support code)."""
    fileloc = getattr(dag, "relative_fileloc", None) or dag.fileloc or ""
    if not fileloc:
        return None

    # relative_fileloc is only actually relative under a named DAG bundle;
    # without one it equals the absolute fileloc, so normalize both cases
    # by stripping the configured dags_folder root ourselves.
    abs_fileloc = os.path.abspath(fileloc)
    if abs_fileloc.startswith(DAGS_FOLDER + os.sep):
        rel = abs_fileloc[len(DAGS_FOLDER) + 1 :]
    else:
        rel = fileloc.lstrip("/\\")

    rel = rel.replace(os.sep, "/")
    if "/" not in rel:
        return None

    top_level = rel.split("/", 1)[0].strip().lower()
    if not top_level or top_level.startswith(("_", ".")):
        return None
    return top_level


def _inspect_for_unauthorized_roles(data, org_arn_prefix, org_name_prefix):
    """Recursively scans nested dicts/lists for any AWS IAM role key whose
    value doesn't match this org's ARN/name prefix. Returns (True, None) if
    safe, or (False, error_message) on the first violation found."""
    if isinstance(data, dict):
        for key, value in data.items():
            if str(key).lower() in RESTRICTED_ROLE_KEYS:
                if isinstance(value, str) and not (
                    value.startswith(org_arn_prefix) or value.startswith(org_name_prefix)
                ):
                    return False, f"Unauthorized role '{value}' found in nested key '{key}'."

            is_valid, error_msg = _inspect_for_unauthorized_roles(value, org_arn_prefix, org_name_prefix)
            if not is_valid:
                return False, error_msg

    elif isinstance(data, (list, tuple)):
        for item in data:
            is_valid, error_msg = _inspect_for_unauthorized_roles(item, org_arn_prefix, org_name_prefix)
            if not is_valid:
                return False, error_msg

    return True, None


# ==========================================
# 3. DAG POLICY (RBAC ENFORCEMENT)
# ==========================================

def dag_policy(dag: DAG):
    """Assigns DAG-level access_control based on which org's boundary the
    DAG falls in. The built-in Operator role always gets full access;
    each org's Admin/User roles are scoped to that org's own DAGs only.
    Full Admins need no entry here -- the built-in Admin role always
    bypasses access_control."""
    org_key = _get_org_for_dag(dag)

    if org_key:
        boundary = _org_boundary(org_key)
        dag.access_control = {
            boundary["admin_role"]: {"can_read", "can_edit", "can_delete"},
            boundary["user_role"]: {"can_read", "can_edit"},  # view + trigger, no delete
            OPERATOR_ROLE: {"can_read", "can_edit", "can_delete"},
        }
        log.info(f"Enforced {org_key} RBAC on DAG: {dag.dag_id}")
    else:
        # Platform/core DAG outside any org boundary: only Operators (and
        # Admins, who bypass this check) can see or touch it.
        dag.access_control = {
            OPERATOR_ROLE: {"can_read", "can_edit", "can_delete"},
        }
        log.warning(f"Uncategorized DAG {dag.dag_id} found outside any org boundary; restricted to Admin/Operator.")


# ==========================================
# 4. TASK POLICY (POOL ROUTING + INFRASTRUCTURE SECURITY)
# ==========================================

def task_policy(task):
    """Runs on every task at DAG-parse time.
      1. Pool auto-routing from 'pool:<name>' DAG tags (applies everywhere).
      2. For tasks inside an org boundary: restrict to the off-worker AWS
         operator allowlist and deep-inspect every argument for IAM roles,
         clusters, and task definitions outside that org's boundary --
         this is what keeps an Org Admin's DAGs sandboxed to their own
         infrastructure instead of reaching into core systems or another
         org's resources.
    Tasks outside any org boundary (platform/core DAGs) are left alone.
    """
    # 1. Pool auto-routing
    if task.dag and task.dag.tags:
        for tag in task.dag.tags:
            if tag.startswith("pool:"):
                task.pool = tag.split("pool:")[1]

    if not task.dag:
        return

    org_key = _get_org_for_dag(task.dag)
    if not org_key:
        return

    boundary = _org_boundary(org_key)

    # 2a. Base operator allowlist
    if task.task_type not in RESTRICTED_ORG_ALLOWLIST:
        raise AirflowClusterPolicyViolation(
            f"Security Violation in DAG '{task.dag.dag_id}', Task '{task.task_id}': "
            f"The '{task.task_type}' is not permitted. The '{org_key}' org is restricted "
            f"to off-worker execution using: {', '.join(sorted(RESTRICTED_ORG_ALLOWLIST))}."
        )

    # 2b. Deep recursive IAM role inspection (catches roles buried in kwargs)
    is_valid, error_msg = _inspect_for_unauthorized_roles(
        task.__dict__, boundary["iam_arn_prefix"], boundary["iam_name_prefix"]
    )
    if not is_valid:
        raise AirflowClusterPolicyViolation(
            f"Security Violation in DAG '{task.dag.dag_id}', Task '{task.task_id}': {error_msg}"
        )

    # 2c. Global AWS connection id check
    aws_conn = getattr(task, "aws_conn_id", None)
    if aws_conn and aws_conn != boundary["aws_conn_id"]:
        raise AirflowClusterPolicyViolation(
            f"Security Violation: '{org_key}' DAGs must use '{boundary['aws_conn_id']}'. "
            f"Attempted to use '{aws_conn}'."
        )

    # 2d. ECS run & provisioning inspections
    if task.task_type.startswith("Ecs"):
        cluster = getattr(task, "cluster", getattr(task, "cluster_name", ""))
        if cluster and not cluster.startswith(boundary["cluster_prefix"]):
            raise AirflowClusterPolicyViolation(
                f"Security Violation: '{org_key}' clusters must start with '{boundary['cluster_prefix']}'. "
                f"Attempted to use '{cluster}'."
            )

        task_definition = getattr(task, "task_definition", getattr(task, "family", ""))
        if task_definition and not task_definition.startswith(boundary["task_def_prefix"]):
            raise AirflowClusterPolicyViolation(
                f"Security Violation: '{org_key}' task definitions must start with "
                f"'{boundary['task_def_prefix']}'. Attempted to use '{task_definition}'."
            )

    # 2e. Lambda invoke & provisioning inspections
    elif task.task_type.startswith("Lambda"):
        function_name = getattr(task, "function_name", "")
        if function_name and not function_name.startswith(boundary["lambda_prefix"]):
            raise AirflowClusterPolicyViolation(
                f"Security Violation: '{org_key}' Lambda functions must be prefixed with "
                f"'{boundary['lambda_prefix']}'. Attempted to use '{function_name}'."
            )

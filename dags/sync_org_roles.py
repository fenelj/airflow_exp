"""
Auto-provisions the FAB roles config/airflow_local_settings.py's dag_policy()
references in access_control.

FAB's access_control sync (FabAirflowSecurityManagerOverride._sync_dag_view_permissions)
requires every role it's given to already exist -- it raises FabException and
fails the whole DAG's parse otherwise. It does NOT auto-create roles, despite
what the docstring in an earlier revision of this file's sibling DAGs assumed.
So each org's "<Org>_Admin"/"<Org>_User" role has to be provisioned somewhere
before dag_policy() ever references it for a DAG in that org's folder.

This mirrors dags/sync_pool_tags.py's pattern (and the repo's original
sync_tags_to_roles.py) but for roles: scan dags/ subfolders using the exact
same rules as airflow_local_settings._get_org_for_dag() (skip "_"/"."-prefixed
folders), and create any missing org role with a base permission set that
lets it actually use the UI.

That base set deliberately EXCLUDES can_read/can_edit/can_delete on the
generic "DAGs" resource -- FabAuthManager._is_authorized_dag() checks that
blanket permission first and, if present, bypasses per-DAG access_control
entirely. Granting it here would let every org's role see every org's DAGs,
defeating the whole point of the per-folder boundary. (menu_access on "DAGs"
is a different, safe action -- it just shows the nav item.)
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Same safe base a Viewer gets, minus the "DAGs" resource's own read/edit/
# delete actions (see module docstring for why those are excluded), and
# minus self-service password management -- login is PKI SSO only
# (config/webserver_config.py); only the break-glass account's password is
# ever managed, directly via `airflow users reset-password`, not the UI.
BASE_PERMISSIONS = [
    ("menu_access", "Website"),
    ("can_read", "Website"),
    ("menu_access", "DAGs"),
    ("can_read", "My Profile"),
    ("can_edit", "My Profile"),
    ("can_read", "DAG Runs"),
    ("can_read", "DAG Code"),
    ("can_read", "DAG Dependencies"),
    ("can_read", "Task Instances"),
    ("can_read", "Task Logs"),
    ("can_read", "HITL Detail"),  # the UI's /ui/dags list call 403s the whole
    # page without this -- it's one of four resources requires_access_dag()
    # checks for that endpoint, and failing any one denies the request
    ("can_read", "XComs"),
    ("can_read", "Jobs"),
]

# Explicitly stripped from every org role even if already granted (e.g. by
# an earlier run of this DAG, before this permission was excluded above).
REVOKED_PERMISSIONS = [
    ("can_read", "My Password"),
    ("can_edit", "My Password"),
]


def sync_org_roles_logic():
    import airflow_local_settings as local_settings
    from airflow.providers.fab.www.app import create_app

    dags_folder = local_settings.DAGS_FOLDER
    orgs = set()
    for entry in os.scandir(dags_folder):
        if entry.is_dir() and not entry.name.startswith(("_", ".")):
            orgs.add(entry.name.lower())

    # Airflow 3's FAB provider moved the Flask app builder here from the old
    # airflow.www.app (which no longer exists); enable_plugins=False since
    # this only needs appbuilder.sm, not the full webserver plugin set.
    app = create_app(enable_plugins=False)
    sm = app.appbuilder.sm

    for org in orgs:
        boundary = local_settings._org_boundary(org)
        for role_name in (boundary["admin_role"], boundary["user_role"]):
            role = sm.find_role(role_name)
            if not role:
                role = sm.add_role(role_name)
                print(f"Created role: {role_name}")

            for action_name, resource_name in BASE_PERMISSIONS:
                perm = sm.get_permission(action_name, resource_name)
                if not perm:
                    perm = sm.create_permission(action_name, resource_name)
                if perm not in role.permissions:
                    sm.add_permission_to_role(role, perm)

            for action_name, resource_name in REVOKED_PERMISSIONS:
                perm = sm.get_permission(action_name, resource_name)
                if perm and perm in role.permissions:
                    sm.remove_permission_from_role(role, perm)
                    print(f"Revoked {action_name}/{resource_name} from role: {role_name}")


with DAG(
    dag_id="admin_sync_org_roles",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["admin", "rbac"],
    description="Auto-creates <Org>_Admin/<Org>_User FAB roles for each dags/ subfolder",
) as dag:
    sync_task = PythonOperator(
        task_id="sync_org_roles_logic",
        python_callable=sync_org_roles_logic,
    )

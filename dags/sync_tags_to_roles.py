import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagModel, Pool
from airflow.utils.session import provide_session

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
}

@provide_session
def sync_tags_logic(session=None):
    """
    Queries all active DAGs to:
    1. Map 'org:xyz' tags to FAB Roles and grant permissions.
    2. Map 'pool:xyz' tags and automatically create Airflow Pools.
    """
    from airflow.www.app import cached_app
    app = cached_app(testing=True)
    sm = app.appbuilder.sm

    dags = session.query(DagModel).filter(DagModel.is_active == True).all()
    
    role_to_dags = {}
    discovered_pools = set()
    
    # 1. Parse all tags
    for dag in dags:
        if dag.tags:
            for tag_obj in dag.tags:
                tag = tag_obj.name
                
                # Role logic
                if tag.startswith('org:'):
                    org_name = tag.split('org:')[1]
                    role_name = f"{org_name.capitalize()}_Editor"
                    if role_name not in role_to_dags:
                        role_to_dags[role_name] = []
                    role_to_dags[role_name].append(dag.dag_id)
                
                # Pool logic
                elif tag.startswith('pool:'):
                    pool_name = tag.split('pool:')[1]
                    discovered_pools.add(pool_name)
                    
    # 2. Auto-create missing Pools
    existing_pools = {p.pool for p in session.query(Pool).all()}
    for pool_name in discovered_pools:
        if pool_name not in existing_pools:
            print(f"Auto-creating missing pool: {pool_name}")
            new_pool = Pool(
                pool=pool_name,
                slots=10, # Default slot count, can be adjusted in UI later
                description=f"Auto-created by sync_tags_to_roles DAG"
            )
            session.add(new_pool)
            
    # 3. Create roles and assign DAG-specific permissions
    for role_name, dag_ids in role_to_dags.items():
        role = sm.find_role(role_name)
        if not role:
            role = sm.add_role(role_name)
            
        website_perm = sm.get_permission('can_read', 'Website')
        if website_perm: sm.add_permission_to_role(role, website_perm)
        
        dags_perm = sm.get_permission('can_read', 'DAGs')
        if dags_perm: sm.add_permission_to_role(role, dags_perm)
        
        for dag_id in dag_ids:
            view_menu_name = f"DAG:{dag_id}"
            sm.create_dag_specific_permissions(dag_id)
            edit_perm = sm.get_permission('can_edit', view_menu_name)
            if edit_perm and edit_perm not in role.permissions:
                sm.add_permission_to_role(role, edit_perm)

with DAG(
    dag_id='admin_sync_tags_logic',
    default_args=default_args,
    schedule='*/5 * * * *',
    catchup=False,
    tags=['admin', 'security', 'pools'],
    description='Automatically maps DAG tags to RBAC roles and creates Airflow Pools'
) as dag:

    sync_task = PythonOperator(
        task_id='sync_tags_logic',
        python_callable=sync_tags_logic,
    )

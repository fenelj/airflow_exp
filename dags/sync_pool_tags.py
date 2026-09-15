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
def sync_pools_logic(session=None):
    """
    Queries all active DAGs and auto-creates any Airflow Pool referenced by
    a 'pool:xyz' tag but missing from the metadata DB. Pairs with the
    pool-routing half of config/airflow_local_settings.py's task_policy,
    which assigns tasks to these pools at parse time.
    """
    dags = session.query(DagModel).filter(DagModel.is_active == True).all()

    discovered_pools = set()
    for dag in dags:
        if dag.tags:
            for tag_obj in dag.tags:
                tag = tag_obj.name
                if tag.startswith('pool:'):
                    discovered_pools.add(tag.split('pool:')[1])

    existing_pools = {p.pool for p in session.query(Pool).all()}
    for pool_name in discovered_pools:
        if pool_name not in existing_pools:
            print(f"Auto-creating missing pool: {pool_name}")
            session.add(Pool(
                pool=pool_name,
                slots=10,  # Default slot count, can be adjusted in UI later
                description="Auto-created by sync_pool_tags DAG",
            ))

with DAG(
    dag_id='admin_sync_pool_tags',
    default_args=default_args,
    schedule='*/5 * * * *',
    catchup=False,
    tags=['admin', 'pools'],
    description='Auto-creates Airflow Pools referenced by pool:<name> DAG tags',
) as dag:

    sync_task = PythonOperator(
        task_id='sync_pools_logic',
        python_callable=sync_pools_logic,
    )

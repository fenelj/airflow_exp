def task_policy(task):
    """
    Airflow Cluster Policy: Runs every time a DAG is parsed or loaded.
    This intercepts the task definition and can dynamically modify its properties.
    """
    # 1. Check if the task's DAG has any tags
    if task.dag and task.dag.tags:
        for tag in task.dag.tags:
            
            # 2. If a tag specifies a pool (e.g., 'pool:high_priority')
            if tag.startswith('pool:'):
                
                # 3. Extract the pool name and assign it to the task automatically
                pool_name = tag.split('pool:')[1]
                task.pool = pool_name
                
                # Note: The pool must exist in the Airflow UI (Admin -> Pools).
                # If it doesn't exist, tasks assigned to it will remain stuck in 'queued' state!

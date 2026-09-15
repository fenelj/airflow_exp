def task_policy(task):
    if not task.dag or not hasattr(task.dag, 'fileloc'):
        return

    file_path = task.dag.fileloc.lower()
    
    if 'finance' in file_path:
        
        # 1. Base Operator Allowance Check
        # (Your existing logic restricting them to EMR, ECS, Lambda, Glue, etc.)
        
        # 2. Deep Dictionary IAM Inspection
        # We pass the entire task object's internal dictionary to scan every 
        # argument, kwarg, and override passed to the operator.
        is_valid, error_message = inspect_for_unauthorized_roles(task.__dict__)
        
        if not is_valid:
            # Rejects the DAG completely before it enters the database
            raise AirflowClusterPolicyViolation(
                f"Security Violation in DAG '{task.dag.dag_id}', Task '{task.task_id}': {error_message}"
            )

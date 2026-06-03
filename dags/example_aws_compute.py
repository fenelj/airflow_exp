from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.empty import EmptyOperator

# ------------------------------------------------------------------------------
# DAG Documentation & Default Arguments
# ------------------------------------------------------------------------------
# default_args are applied to all tasks in this DAG unless overridden at the task level.
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'on_failure_callback': my_custom_slack_alert_function,
}

with DAG(
    dag_id='example_aws_compute_workflows',
    default_args=default_args,
    description='A reference DAG for triggering AWS Compute (ECS & Lambda) tasks',
    schedule=None, # Triggered manually or externally
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aws', 'reference', 'compute', 'ecs', 'lambda'],
    doc_md="""
    # AWS Compute Reference DAG
    This DAG provides examples of how to invoke standard AWS compute workloads.
    - **Lambda**: Great for short-lived, lightweight Python/Node execution.
    - **ECS Fargate**: Perfect for heavy workloads, containerized batch processing, and ML training.
    
    Make sure you have an AWS Connection configured in Airflow (default connection ID is `aws_default`).
    """
) as dag:

    start_task = EmptyOperator(task_id='start')

    # ------------------------------------------------------------------------------
    # 1. Lambda Invoke Operator
    # ------------------------------------------------------------------------------
    # Triggers an existing AWS Lambda function synchronously.
    # We pass a JSON payload and wait for the response.
    invoke_lambda_task = LambdaInvokeFunctionOperator(
        task_id='invoke_lambda_calculation',
        function_name='my_data_processing_lambda',
        payload='{"target_date": "{{ ds }}", "run_mode": "batch"}', # {{ ds }} is an Airflow macro for execution date
        log_type='Tail', # Brings Lambda logs into Airflow task logs
        aws_conn_id='aws_default', # Connection setup in Airflow UI
    )

    # ------------------------------------------------------------------------------
    # 2. ECS Run Task Operator (Fargate)
    # ------------------------------------------------------------------------------
    # Spins up a container on ECS Fargate, runs it to completion, and monitors the logs.
    # This is effectively "Serverless Batch Processing".
    run_ecs_fargate_task = EcsRunTaskOperator(
        task_id='run_heavy_etl_container',
        cluster='my-fargate-cluster',
        task_definition='my-heavy-etl-task-def', # Name of the Task Definition in AWS
        launch_type='FARGATE',
        overrides={
            'containerOverrides': [
                {
                    'name': 'my-etl-container',
                    # Injecting Airflow parameters dynamically into the container!
                    'command': ['python', 'run_etl.py', '--date', '{{ ds }}'],
                    'environment': [
                        {'name': 'ENV_VAR_NAME', 'value': 'ENV_VAR_VALUE'},
                    ],
                },
            ],
        },
        network_configuration={
            'awsvpcConfiguration': {
                'subnets': ['subnet-12345678', 'subnet-87654321'],
                'securityGroups': ['sg-12345678'],
                'assignPublicIp': 'ENABLED',
            },
        },
        awslogs_group='/ecs/my-heavy-etl-task-def', # Streams the container logs directly into Airflow UI!
        awslogs_stream_prefix='ecs',
        aws_conn_id='aws_default',
    )

    end_task = EmptyOperator(task_id='end')

    # ------------------------------------------------------------------------------
    # Define the DAG structure (Dependencies)
    # ------------------------------------------------------------------------------
    # This dictates the order of execution: Start -> Lambda -> ECS -> End
    start_task >> invoke_lambda_task >> run_ecs_fargate_task >> end_task

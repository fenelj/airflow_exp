from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.providers.amazon.aws.operators.step_function import StepFunctionStartExecutionOperator
from airflow.providers.amazon.aws.sensors.step_function import StepFunctionExecutionSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'retries': 0,
}

with DAG(
    dag_id='example_aws_triggers_and_sensors',
    default_args=default_args,
    description='A reference DAG for EventBridge, SQS, and Step Function Triggers',
    schedule=None, # Typically driven by external events
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aws', 'reference', 'sqs', 'stepfunctions', 'eventbridge'],
    doc_md="""
    # AWS Triggers & Sensors Reference DAG
    
    ## 1. EventBridge & API Gateway Triggers
    You cannot natively "listen" to EventBridge inside Airflow. Instead, the paradigm is inverted:
    You configure EventBridge (or API Gateway) to call the Airflow REST API to trigger a DAG.
    
    **To trigger this DAG from EventBridge via API Target:**
    `POST /api/v1/dags/example_aws_triggers_and_sensors/dagRuns`
    Payload: `{"conf": {"my_custom_event_data": "value"}}`

    ## 2. SQS Sensor
    Airflow can poll an SQS queue continuously until a specific message arrives, then continue execution.

    ## 3. Step Functions
    Airflow can trigger a complex AWS Step Function state machine and wait for it to complete.
    """
) as dag:

    # ------------------------------------------------------------------------------
    # 0. Parsing REST API (EventBridge) Payloads
    # ------------------------------------------------------------------------------
    # If this DAG was triggered by EventBridge via the Airflow REST API, the payload
    # can be accessed via the `dag_run.conf` dictionary.
    def print_eventbridge_payload(**kwargs):
        payload = kwargs['dag_run'].conf
        print(f"Triggered externally. Payload received: {payload}")

    parse_event_payload = PythonOperator(
        task_id='parse_eventbridge_payload',
        python_callable=print_eventbridge_payload,
    )

    # ------------------------------------------------------------------------------
    # 1. SQS Sensor
    # ------------------------------------------------------------------------------
    # The DAG will pause here and poll the queue every `poke_interval` seconds.
    # It only progresses if a message is found in the queue.
    # *Note: In production, consider using Airflow Deferrable Operators to save compute!
    wait_for_sqs_message = SqsSensor(
        task_id='wait_for_sqs_message',
        sqs_queue='my-trigger-queue-name',
        aws_conn_id='aws_default',
        max_messages=1, # Number of messages to pull
        wait_time_seconds=10, # SQS long-polling wait time
        poke_interval=30, # Airflow polling interval
        timeout=60 * 60 * 2, # Timeout after 2 hours
    )

    # ------------------------------------------------------------------------------
    # 2. Step Functions - Start Execution
    # ------------------------------------------------------------------------------
    # Triggers an AWS Step Function state machine.
    trigger_step_function = StepFunctionStartExecutionOperator(
        task_id='trigger_step_function',
        state_machine_arn='arn:aws:states:us-east-1:123456789012:stateMachine:MyStateMachine',
        name='AirflowTriggeredExecution-{{ ds_nodash }}',
        state_machine_input='{"run_date": "{{ ds }}"}',
        aws_conn_id='aws_default',
    )

    # ------------------------------------------------------------------------------
    # 3. Step Functions - Sensor (Wait for Completion)
    # ------------------------------------------------------------------------------
    # Waits for the Step Function we just triggered to reach SUCCEEDED status.
    wait_for_step_function = StepFunctionExecutionSensor(
        task_id='wait_for_step_function',
        execution_arn="{{ task_instance.xcom_pull(task_ids='trigger_step_function', key='return_value') }}",
        aws_conn_id='aws_default',
        poke_interval=15,
    )

    end_task = EmptyOperator(task_id='end')

    # ------------------------------------------------------------------------------
    # Define the DAG structure
    # ------------------------------------------------------------------------------
    parse_event_payload >> wait_for_sqs_message >> trigger_step_function >> wait_for_step_function >> end_task

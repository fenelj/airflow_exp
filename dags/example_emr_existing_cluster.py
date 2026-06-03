from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# ------------------------------------------------------------------------------
# Default Arguments & Configuration
# ------------------------------------------------------------------------------
default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

# Best Practice: Store your long-running cluster ID in Airflow Variables (Admin -> Variables) 
# instead of hardcoding it. We provide a default fallback here for local testing.
EMR_CLUSTER_ID = Variable.get("EMR_CLUSTER_ID", default_var="j-1234567890ABC")

# The Spark Step we want to execute on the cluster
SPARK_STEPS = [
    {
        'Name': 'execute_spark_etl',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'cluster',
                '--class', 'org.apache.spark.examples.SparkPi',
                # This would be your custom ETL PySpark script stored in S3
                's3://my-emr-scripts-bucket/spark-examples.jar',
                '10' # Example argument passed to the spark script
            ]
        }
    }
]

with DAG(
    dag_id='emr_existing_cluster_pipeline',
    default_args=default_args,
    schedule=None, # Trigger manually or via external schedule
    catchup=False,
    tags=['emr', 'spark', 'aws'],
    doc_md="""
    # Existing EMR Cluster Pipeline
    
    This DAG demonstrates how to interact with an EMR cluster that is already running 24/7.
    It simply:
    1. Submits a new Spark step to the cluster via the EMR API.
    2. Uses a Sensor to poll AWS every 15 seconds until the step completes successfully.
    """
) as dag:

    start = EmptyOperator(task_id='start')

    # 1. Add our Spark Step to the existing cluster
    add_step = EmrAddStepsOperator(
        task_id='add_spark_step',
        job_flow_id=EMR_CLUSTER_ID,
        aws_conn_id='aws_default',
        steps=SPARK_STEPS,
    )

    # 2. Wait for the Spark Step to complete successfully
    # The step_id is dynamically pulled from the previous task using Airflow XComs!
    wait_for_step = EmrStepSensor(
        task_id='wait_for_spark_step',
        job_flow_id=EMR_CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
        poke_interval=15, # Check AWS every 15 seconds
    )

    end = EmptyOperator(task_id='end')

    # Define Workflow
    start >> add_step >> wait_for_step >> end

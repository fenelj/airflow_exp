from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='example_aws_data_processing_workflows',
    default_args=default_args,
    description='A reference DAG for triggering AWS Data (Glue, Athena, EMR) tasks',
    schedule='@daily', # Runs once a day automatically
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aws', 'reference', 'data', 'glue', 'emr', 'athena'],
    doc_md="""
    # AWS Data Processing Reference DAG
    Demonstrates how to invoke and monitor standard big data workloads.
    - **Athena**: Run SQL queries directly against S3 data lakes.
    - **Glue**: Serverless PySpark jobs.
    - **EMR**: Submit big data steps to an existing EMR cluster.
    """
) as dag:

    start_task = EmptyOperator(task_id='start')

    # ------------------------------------------------------------------------------
    # 1. Athena Operator
    # ------------------------------------------------------------------------------
    # Executes a SQL query in Athena to transform or analyze data in S3
    run_athena_query = AthenaOperator(
        task_id='run_athena_query',
        query='''
            CREATE TABLE IF NOT EXISTS analytics_db.daily_summary AS
            SELECT category, count(*) as total 
            FROM raw_db.events 
            WHERE event_date = '{{ ds }}'
            GROUP BY category;
        ''',
        database='analytics_db',
        output_location='s3://my-athena-query-results-bucket/daily_summaries/',
        aws_conn_id='aws_default',
        sleep_time=10, # How often to poll Athena for completion status
    )

    # ------------------------------------------------------------------------------
    # 2. Glue Job Operator
    # ------------------------------------------------------------------------------
    # Triggers an AWS Glue PySpark job. It automatically waits for the job to finish.
    run_glue_job = GlueJobOperator(
        task_id='run_glue_etl_job',
        job_name='my_pyspark_etl_job',
        script_location='s3://my-scripts-bucket/glue/etl_script.py',
        s3_bucket='my-glue-artifacts-bucket', # Where Airflow outputs glue logs locally
        iam_role_name='my-glue-service-role',
        create_job_kwargs={
            'GlueVersion': '3.0',
            'NumberOfWorkers': 5,
            'WorkerType': 'G.1X'
        },
        script_args={
            '--input_path': 's3://my-data/raw/{{ ds }}/',
            '--output_path': 's3://my-data/processed/{{ ds }}/',
        },
        aws_conn_id='aws_default',
    )

    # ------------------------------------------------------------------------------
    # 3. EMR Add Steps Operator
    # ------------------------------------------------------------------------------
    # Adds a Spark Step to an EXISTING EMR cluster. 
    # Notice that unlike Glue/Athena, adding a step is an asynchronous API call!
    # Therefore, we MUST use a Sensor to wait for it to finish.
    SPARK_STEP = [
        {
            'Name': 'calculate_pi',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    '--class', 'org.apache.spark.examples.SparkPi',
                    's3://my-emr-scripts/spark-examples.jar',
                    '10'
                ]
            }
        }
    ]

    add_emr_step = EmrAddStepsOperator(
        task_id='add_emr_spark_step',
        job_flow_id='j-1234567890ABC', # The ID of the running EMR Cluster
        aws_conn_id='aws_default',
        steps=SPARK_STEP,
    )

    # ------------------------------------------------------------------------------
    # 4. EMR Step Sensor
    # ------------------------------------------------------------------------------
    # The Sensor sits in a polling loop, checking AWS every few seconds to see if 
    # the step added in the previous task has completed successfully.
    wait_for_emr_step = EmrStepSensor(
        task_id='wait_for_emr_spark_step',
        job_flow_id='j-1234567890ABC',
        step_id="{{ task_instance.xcom_pull(task_ids='add_emr_spark_step', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    end_task = EmptyOperator(task_id='end')

    # ------------------------------------------------------------------------------
    # Define the DAG structure
    # ------------------------------------------------------------------------------
    # Athena and Glue run in parallel. When both are done, EMR step is submitted.
    start_task >> [run_athena_query, run_glue_job] >> add_emr_step >> wait_for_emr_step >> end_task

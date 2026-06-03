from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='medallion_bronze_to_silver_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['ingestion', 'ecs', 'glue', 'iceberg', 'medallion'],
    doc_md="""
    # Medallion Architecture: Bronze to Silver (Iceberg)
    
    1. **Ingestion (ECS Fargate)**: Triggers a containerized app to pull data from an API/Database and dumps it raw into the Bronze S3 bucket.
    2. **ETL (AWS Glue)**: Runs a Serverless PySpark job to read the Bronze data, perform deduplication, and upsert it into an Apache Iceberg table in the Silver bucket.
    
    *Note: The Iceberg deduplication logic (MERGE INTO) happens inside the Glue PySpark script, which is passed dynamically via `script_args`.*
    """
) as dag:

    start = EmptyOperator(task_id='start')

    # ------------------------------------------------------------------------------
    # 1. Lambda Ingestion Task (API to Bronze S3)
    # ------------------------------------------------------------------------------
    # Invokes a serverless Lambda function to handle the secure mTLS API request.
    ingest_to_bronze = LambdaInvokeFunctionOperator(
        task_id='ingest_api_to_bronze',
        function_name='my_api_ingestion_lambda',
        # We pass the Airflow execution date dynamically so the Lambda knows where to save the data!
        payload='{"execution_date": "{{ ds }}", "destination_bucket": "s3://my-bronze-bucket/raw_data/{{ ds }}/"}',
        log_type='Tail', # Brings the Lambda execution logs into the Airflow UI
        aws_conn_id='aws_default'
    )

    # ------------------------------------------------------------------------------
    # 2. Glue ETL Task (Bronze S3 to Silver Iceberg)
    # ------------------------------------------------------------------------------
    glue_bronze_to_silver = GlueJobOperator(
        task_id='etl_bronze_to_silver_iceberg',
        job_name='bronze_to_silver_iceberg_upsert',
        script_location='s3://my-glue-scripts-bucket/glue_bronze_to_silver.py',
        s3_bucket='my-airflow-glue-logs-bucket',
        iam_role_name='GlueDataLakeRole',
        create_job_kwargs={
            'GlueVersion': '4.0',
            'NumberOfWorkers': 10,
            'WorkerType': 'G.1X',
            'DefaultArguments': {
                # CRITICAL: These configurations enable Apache Iceberg support natively in AWS Glue!
                '--datalake-formats': 'iceberg',
                '--conf': (
                    'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions '
                    '--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog '
                    '--conf spark.sql.catalog.glue_catalog.warehouse=s3://my-silver-bucket/iceberg/ '
                    '--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog '
                    '--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO'
                )
            }
        },
        script_args={
            '--BRONZE_PATH': 's3://my-bronze-bucket/raw_data/{{ ds }}/',
            '--SILVER_TABLE': 'glue_catalog.silver_db.my_iceberg_table',
            # Pass the composite primary key to Glue so it knows how to deduplicate!
            '--PRIMARY_KEY_COLS': 'user_id,event_timestamp', 
        },
        aws_conn_id='aws_default',
    )

    end = EmptyOperator(task_id='end')

    # Define Workflow
    start >> ingest_to_bronze >> glue_bronze_to_silver >> end

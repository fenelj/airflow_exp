"""
Produces the "other dataset" that example_complex_pipeline.py's normalized
sales table forms a relationship with (see that Lambda's RELATED_TABLE
constant). Independent, on-demand DAG -- not asset-scheduled off anything,
since it's the upstream/reference side of the relationship rather than a
consumer.

Deliberately uses Airflow-native Glue Catalog operators instead of a Lambda,
as a second example of "create if not exists": GlueCatalogCreateDatabaseOperator
and GlueCatalogCreateTableOperator both take if_exists="skip", so re-running
this DAG is a no-op against an already-provisioned catalog entry rather than
raising or duplicating anything. Compare with the Lambda-driven approach in
_lambda_scripts/normalize_and_catalog_lambda.py, which does the equivalent
check itself via a get_table/create_table try-except.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue_catalog import (
    GlueCatalogCreateDatabaseOperator,
    GlueCatalogCreateTableOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset

# Declared purely for catalog lineage/discovery (visible under the Assets
# tab) -- nothing currently schedules off this asset.
customer_dimension_asset = Asset(
    name="customer_dimension_dataset",
    uri="s3://data-lake/curated/customer_dimension/",
    group="curated",
)

with DAG(
    dag_id="example_customer_dimension_pipeline",
    description="Creates the customer dimension table referenced by the normalized sales pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reference", "glue", "catalog"],
) as dag:

    start = EmptyOperator(task_id="start")

    create_database = GlueCatalogCreateDatabaseOperator(
        task_id="create_database_if_not_exists",
        database_name="curated",
        description="Curated, compliance-reviewed datasets",
        if_exists="skip",
    )

    create_table = GlueCatalogCreateTableOperator(
        task_id="create_customer_dimension_table",
        database_name="curated",
        table_name="customer_dimension",
        if_exists="skip",
        table_input={
            "Name": "customer_dimension",
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": "s3://data-lake/curated/customer_dimension/",
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"},
                "Columns": [
                    {"Name": "customer_id", "Type": "string"},
                    {"Name": "customer_name", "Type": "string"},
                    {"Name": "country", "Type": "string"},
                    {"Name": "signup_date", "Type": "timestamp"},
                ],
            },
            "Parameters": {
                "data_classification": "confidential",
                "pii_flag": "true",
                "compliance_owner": "finance-data-governance",
            },
        },
        outlets=[customer_dimension_asset],
    )

    end = EmptyOperator(task_id="end")

    start >> create_database >> create_table >> end

"""
Reference DAG showcasing a multi-stage, asset-driven pipeline:

  1. generate_raw_dataset   -- a task that fabricates a raw JSON dataset and
     pushes it to XCom (return value auto-pushed).
  2. normalize_and_catalog  -- invokes a Lambda (see
     _lambda_scripts/normalize_and_catalog_lambda.py) that normalizes the
     raw XCom'd JSON, does a "create if not exists" against the Glue Data
     Catalog, populates compliance table Parameters, and records a
     relationship to another dataset (example_customer_dimension_pipeline.py's
     customer_dimension table). Declares `outlets=[normalized_sales_asset]`
     -- that's what lets Airflow trigger downstream DAGs automatically when
     this task succeeds, without either DAG naming the other directly.

Two more DAGs pick up from there, both scheduled off an Asset rather than a
cron/interval:
  - example_materialized_view_builder.py: schedule=[normalized_sales_asset]
  - example_view_subset_builder.py:       schedule=[sales_mv_asset]

That second one is chained off the *materialized view's own* asset rather
than fanning out off normalized_sales_asset directly -- "a subset of the
first [view]" read most naturally as sitting downstream of the view, not
as a sibling reading the same raw table. If you actually meant both
follow-on DAGs to trigger independently off the normalized table, swap
example_view_subset_builder.py's `schedule=` to `[normalized_sales_asset]`
-- it's a one-line change, the rest of that DAG is unaffected either way.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

# The asset this pipeline produces. Its `uri` matches the S3 location the
# Lambda writes to and the Glue table's StorageDescriptor.Location -- kept
# in sync by convention, not enforced (see the cross-DAG consistency note
# in an earlier conversation about this repo's asset usage).
normalized_sales_asset = Asset(
    name="normalized_sales_dataset",
    uri="s3://data-lake/normalized/sales/",
    group="curated",
)

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def generate_raw_dataset(**context):
    """Stands in for a real extraction step (an API pull, a CDC stream,
    a file drop) -- deliberately messy: string-typed amount, a nested
    address blob, non-UTC timestamps -- exactly what normalize_and_catalog
    exists to clean up."""
    raw_records = [
        {
            "order_id": 1001,
            "customer_id": "CUST-42",
            "amount": "129.99",
            "order_timestamp": "2024-06-01T14:30:00-04:00",
            "address": {"country": "US"},
        },
        {
            "order_id": 1002,
            "customer_id": "CUST-07",
            "amount": "58.50",
            "order_timestamp": "2024-06-01T09:15:00+00:00",
            "address": {"country": "GB"},
        },
        {
            "order_id": 1003,
            "customer_id": "CUST-42",
            "amount": "2400.00",
            "order_timestamp": "2024-06-02T22:05:00-04:00",
            "address": {"country": "US"},
        },
    ]
    return raw_records  # auto-pushed to XCom under key "return_value"


with DAG(
    dag_id="example_complex_pipeline",
    default_args=default_args,
    description="Raw JSON -> Lambda-normalized Glue Catalog table (with compliance fields + a dataset relationship)",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reference", "complex-pipeline", "glue", "lambda"],
    doc_md=__doc__,
) as dag:

    generate_raw = PythonOperator(
        task_id="generate_raw_dataset",
        python_callable=generate_raw_dataset,
    )

    normalize_and_catalog = LambdaInvokeFunctionOperator(
        task_id="normalize_and_catalog",
        function_name="platform-ds-normalize_and_catalog",
        payload="{{ ti.xcom_pull(task_ids='generate_raw_dataset') | tojson }}",
        outlets=[normalized_sales_asset],
    )

    generate_raw >> normalize_and_catalog

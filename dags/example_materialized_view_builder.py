"""
Asset-triggered follow-on to example_complex_pipeline.py: runs automatically
whenever normalized_sales_asset is updated (i.e. every time that DAG's
normalize_and_catalog task succeeds) -- no cron schedule, no explicit
trigger, and neither DAG names the other anywhere except through the
shared Asset object.

Builds a Redshift Spectrum materialized view over the Glue Catalog table so
downstream BI/analytics queries hit precomputed aggregates instead of
scanning raw S3 objects on every query.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.sdk import Asset

normalized_sales_asset = Asset(name="normalized_sales_dataset", uri="s3://data-lake/normalized/sales/", group="curated")

# Produced here so example_view_subset_builder.py can chain off *this*
# view specifically, rather than off normalized_sales_asset again.
# Amazon provider's redshift:// asset scheme requires host, database,
# schema, and table in the URI (airflow.providers.amazon.aws.assets.redshift
# .sanitize_uri) -- a bare "redshift://analytics/curated/sales_mv" fails to
# parse at DAG-load time.
REDSHIFT_HOST = "analytics-workgroup.redshift-serverless.amazonaws.com:5439"
sales_mv_asset = Asset(name="sales_mv", uri=f"redshift://{REDSHIFT_HOST}/analytics/curated/sales_mv", group="curated")

with DAG(
    dag_id="example_materialized_view_builder",
    description="Builds a Redshift materialized view over the normalized sales Glue table",
    schedule=[normalized_sales_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reference", "asset-triggered", "redshift"],
) as dag:

    build_materialized_view = RedshiftDataOperator(
        task_id="build_sales_materialized_view",
        workgroup_name="analytics-workgroup",  # Redshift Serverless; use cluster_identifier= for a provisioned cluster
        database="analytics",
        sql="""
            CREATE MATERIALIZED VIEW curated.sales_mv AUTO REFRESH YES AS
            SELECT
                customer_id,
                date_trunc('day', order_ts) AS order_date,
                country,
                COUNT(*) AS order_count,
                SUM(amount_usd) AS total_amount_usd
            FROM spectrum.curated_normalized_sales
            GROUP BY customer_id, date_trunc('day', order_ts), country;
        """,
        outlets=[sales_mv_asset],
    )

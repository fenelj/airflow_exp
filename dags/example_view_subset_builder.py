"""
Asset-triggered follow-on to example_materialized_view_builder.py: runs
automatically whenever sales_mv is rebuilt -- chained off the materialized
view's own asset (not normalized_sales_asset) since a "subset of the first
view" is naturally downstream of that view, not a sibling reading the raw
table again. See example_complex_pipeline.py's docstring if you actually
want this to fan out off normalized_sales_asset directly instead --
swapping the `schedule=` below is the only change needed.

Creates a plain (non-materialized) view filtering the materialized view
down to high-value orders -- a much cheaper object than another materialized
view for a subset that a handful of dashboards query directly.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.sdk import Asset

# Must match example_materialized_view_builder.py's sales_mv_asset exactly
# (same name, uri, group) -- Airflow matches assets across DAGs by uri, not
# by the Python object, so any drift here creates a second, disconnected
# asset instead of chaining off the real one.
REDSHIFT_HOST = "analytics-workgroup.redshift-serverless.amazonaws.com:5439"
sales_mv_asset = Asset(name="sales_mv", uri=f"redshift://{REDSHIFT_HOST}/analytics/curated/sales_mv", group="curated")

high_value_sales_view_asset = Asset(
    name="high_value_sales_view",
    uri=f"redshift://{REDSHIFT_HOST}/analytics/curated/high_value_sales",
    group="curated",
)

with DAG(
    dag_id="example_view_subset_builder",
    description="Builds a plain view of high-value orders, subset from the sales materialized view",
    schedule=[sales_mv_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reference", "asset-triggered", "redshift"],
) as dag:

    build_subset_view = RedshiftDataOperator(
        task_id="build_high_value_sales_view",
        workgroup_name="analytics-workgroup",
        database="analytics",
        sql="""
            CREATE OR REPLACE VIEW curated.high_value_sales AS
            SELECT *
            FROM curated.sales_mv
            WHERE total_amount_usd > 1000;
        """,
        outlets=[high_value_sales_view_asset],
    )

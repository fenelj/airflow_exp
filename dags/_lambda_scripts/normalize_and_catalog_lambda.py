"""
Reference implementation for the Lambda function invoked by
dags/example_complex_pipeline.py's `normalize_and_catalog` task.

Deploy this as its own Lambda (see README.md's CI/CD section for the
S3-sync pattern used to ship scripts from this repo) -- Airflow only holds
a LambdaInvokeFunctionOperator pointing at whatever function_name you
deploy this under.

Responsibilities:
  1. Normalize the raw JSON records passed in as the invocation payload.
  2. Write the normalized rows to the table's data location in S3.
  3. Create the Glue Data Catalog table if it doesn't exist yet ("create
     if not exists"), or leave its schema alone if it does.
  4. Unconditionally update the table's Parameters with compliance fields
     and a relationship pointer to another dataset (its own table, so
     downstream consumers can walk the catalog to find related data
     without needing an external metadata store).
"""

import json
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

GLUE_DATABASE = "curated"
GLUE_TABLE = "normalized_sales"
TABLE_S3_LOCATION = "s3://data-lake/normalized/sales/"
RELATED_TABLE = "curated.customer_dimension"  # produced by example_customer_dimension_pipeline.py

# Lazy-initialized rather than created at import time: this file lives
# under dags/ and Airflow's dag-processor can import it while scanning for
# DAGs, and building a boto3 client requires a resolvable AWS region --
# not guaranteed at parse time in every environment this repo runs in.
# Still cached per warm Lambda execution environment like a module-level
# client would be, just initialized on first use instead of at import.
_clients: dict[str, "boto3.client"] = {}


def _client(service: str):
    if service not in _clients:
        _clients[service] = boto3.client(service)
    return _clients[service]


def normalize_records(raw_records: list[dict]) -> list[dict]:
    """Coerce the messy raw JSON (string amounts, flat address blob, ad-hoc
    timestamp formats) into a normalized schema."""
    normalized = []
    for record in raw_records:
        normalized.append(
            {
                "order_id": str(record["order_id"]),
                "customer_id": str(record["customer_id"]),
                "amount_usd": round(float(record["amount"]), 2),
                "order_ts": datetime.fromisoformat(record["order_timestamp"]).astimezone(timezone.utc).isoformat(),
                "country": record.get("address", {}).get("country", "UNKNOWN"),
            }
        )
    return normalized


def write_records_to_s3(records: list[dict]) -> str:
    """Writes one newline-delimited JSON object per line -- the layout
    Redshift Spectrum / Athena expect for a JSON-backed external table."""
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    key = f"normalized/sales/dt={datetime.now(timezone.utc):%Y-%m-%d}/part-{int(time.time())}.json"
    bucket = TABLE_S3_LOCATION.removeprefix("s3://").split("/", 1)[0]
    _client("s3").put_object(Bucket=bucket, Key=key, Body=body)
    return f"s3://{bucket}/{key}"


def ensure_table_exists():
    """Create-if-not-exists: leaves an existing table's schema untouched so
    a hand-tuned partition/column change isn't clobbered by every run."""
    try:
        _client("glue").get_table(DatabaseName=GLUE_DATABASE, Name=GLUE_TABLE)
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise

    _client("glue").create_table(
        DatabaseName=GLUE_DATABASE,
        TableInput={
            "Name": GLUE_TABLE,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Location": TABLE_S3_LOCATION,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                },
                "Columns": [
                    {"Name": "order_id", "Type": "string"},
                    {"Name": "customer_id", "Type": "string"},
                    {"Name": "amount_usd", "Type": "double"},
                    {"Name": "order_ts", "Type": "timestamp"},
                    {"Name": "country", "Type": "string"},
                ],
            },
            "PartitionKeys": [{"Name": "dt", "Type": "string"}],
        },
    )


def update_table_properties(row_count: int, output_path: str):
    """Runs on every invocation regardless of whether the table was just
    created -- compliance fields and the dataset relationship need to stay
    current even when the schema itself hasn't changed."""
    _client("glue").update_table(
        DatabaseName=GLUE_DATABASE,
        TableInput={
            "Name": GLUE_TABLE,
            "Parameters": {
                # Compliance fields
                "data_classification": "confidential",
                "pii_flag": "true",  # customer_id is joinable back to PII in the dimension table
                "retention_days": "2555",  # 7 years
                "compliance_owner": "finance-data-governance",
                "compliance_reviewed_at": datetime.now(timezone.utc).isoformat(),
                # Relationship to another dataset
                "related_table": RELATED_TABLE,
                "relationship_type": "many_to_one_via_customer_id",
                # Operational metadata
                "last_updated_row_count": str(row_count),
                "last_updated_path": output_path,
            },
        },
    )


def handler(event, context):
    raw_records = event if isinstance(event, list) else event.get("records", [])
    normalized = normalize_records(raw_records)

    output_path = write_records_to_s3(normalized)
    ensure_table_exists()
    update_table_properties(row_count=len(normalized), output_path=output_path)

    return {
        "database": GLUE_DATABASE,
        "table": GLUE_TABLE,
        "row_count": len(normalized),
        "output_path": output_path,
    }

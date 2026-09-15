import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# ------------------------------------------------------------------------------
# 1. Initialization and Argument Parsing
# ------------------------------------------------------------------------------
# Airflow passes these arguments dynamically via `script_args`
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_PATH', 'SILVER_TABLE', 'PRIMARY_KEY_COLS'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bronze_path = args['BRONZE_PATH']
silver_table = args['SILVER_TABLE']
pk_cols = [col.strip() for col in args['PRIMARY_KEY_COLS'].split(',')]

# ------------------------------------------------------------------------------
# 2. Read Bronze Data & Intra-batch Deduplication
# ------------------------------------------------------------------------------
# Read the raw data dumped by the ECS task
df_bronze = spark.read.json(bronze_path) # Assumes JSON, can be changed to parquet/csv

# Drop exact duplicates within the current batch itself using the composite primary key
df_deduped = df_bronze.dropDuplicates(subset=pk_cols)

# Create a temporary view to use in our Iceberg SQL MERGE statement
df_deduped.createOrReplaceTempView("bronze_updates")

# ------------------------------------------------------------------------------
# 3. Iceberg Setup & Inter-batch Deduplication (Upsert)
# ------------------------------------------------------------------------------
# Build the dynamic ON condition for the MERGE statement based on the composite keys
# E.g., "target.user_id = source.user_id AND target.event_timestamp = source.event_timestamp"
merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_cols])

# Ensure the Iceberg table actually exists before we try to MERGE into it
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_table}
USING iceberg
AS SELECT * FROM bronze_updates WHERE 1=0
""")

# Perform an ACID Upsert! 
# This guarantees no duplicates ever enter the Silver table across historical batches.
# If the composite primary key exists, it updates the record. If it doesn't, it inserts it.
spark.sql(f"""
MERGE INTO {silver_table} target
USING bronze_updates source
ON {merge_condition}
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN
    INSERT *
""")

job.commit()

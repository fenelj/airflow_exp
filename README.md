# Enterprise Airflow Deployment (ECS-Ready)

This repository contains a modern, production-grade Apache Airflow 3.x deployment designed specifically to run on AWS ECS Fargate, featuring zero-fragmentation CI/CD patterns, API Gateway mTLS Single Sign-On (SSO), and fully automated Role-Based Access Control (RBAC).

## Architecture Highlights

*   **Header-Based SSO (mTLS):** Airflow sits behind a Reverse Proxy (API Gateway/ALB). The `config/webserver_config.py` intercepts `X-Forwarded-User` and `X-Forwarded-Roles` headers to seamlessly log users in, mapping them natively to Airflow's internal RBAC roles. Unauthenticated traffic is blocked at the edge.
*   **Automated Tag-Based Security:** Developers simply add tags like `tags=['org:finance']` to their DAGs. A background maintenance DAG (`dags/sync_tags_to_roles.py`) automatically generates the `Finance_Editor` role and securely locks down edit access dynamically.
*   **Automated Pool Routing:** A Cluster Policy in `config/airflow_local_settings.py` reads tags like `tags=['pool:high_priority']` and dynamically routes tasks to the correct Airflow pool at parse time. The maintenance DAG auto-creates the pools if they are missing.
*   **Zero-Fragmentation Deployments:** The `Dockerfile` and `docker-compose.yaml` are environment-agnostic. All secrets and endpoints are injected via the `.env` file (locally) or ECS Task Definitions (in production).
*   **EFS DAG Syncing:** DAGs are not baked into the Docker image. In production, an S3 Sync Sidecar container pulls DAGs from S3 into an EFS volume mounted at `/opt/airflow/dags`, allowing real-time DAG updates without restarting containers.

## Included Sample Workflows

The `dags/` folder comes pre-loaded with highly-documented reference blueprints for enterprise AWS operations:

1.  **Medallion Architecture (`example_medallion_architecture.py`)**: Demonstrates a complete Bronze-to-Silver ETL pipeline. Uses a Lambda function (script included in `dags/lambda_scripts/`) to pull external API data via mTLS into S3, and triggers an AWS Glue PySpark job (script included in `dags/glue_scripts/`) to perform composite-key deduplication via an Apache Iceberg `MERGE INTO` statement.
2.  **Existing EMR (`example_emr_existing_cluster.py`)**: Demonstrates submitting jobs to an active 24/7 EMR cluster using dynamic XCom step polling.
3.  **Compute & Triggers**: Extensive examples of `EcsRunTaskOperator`, EventBridge API triggers, SQS Sensors, and AWS Step Functions in `example_aws_compute.py` and `example_aws_triggers_and_sensors.py`.

## Local Development Instructions

### Prerequisites
*   Docker & Docker Compose installed.
*   Local AWS credentials configured (`~/.aws/credentials`).

### Running the Stack
1.  **Environment Variables**: Ensure your `.env` file contains the `AIRFLOW_UID=50000` and the `AIRFLOW_CONN_AWS_DEFAULT` JSON configuration for your specific VPC endpoints.
2.  **Start Airflow**: Run the following command in the root of the project:
    ```bash
    docker compose up -d --build
    ```
    *This will build the custom Python 3.14 image and start the Postgres backend, Scheduler, Webserver, and Triggerer.*
3.  **Access the UI**: Navigate to `http://localhost:8080`.
    *Note: Because we are testing locally without the API Gateway, the custom `webserver_config.py` allows fallback to the default login screen so you can continue iterating locally.*

### Connecting to AWS Services
Airflow automatically assumes your local AWS SSO profile (via `boto3`). In production, you leave the username/password blank in the Airflow AWS Connection, and it will automatically assume the IAM Role attached to the ECS Task. 

Custom VPC endpoints or GovCloud configurations are defined centrally in the `.env` file's `AIRFLOW_CONN_AWS_DEFAULT` JSON string under the `service_config` dictionary.

## CI/CD Deployment Strategy

To deploy this repository to an AWS ECS cluster:
1.  **Docker Image**: CI/CD runs `docker build .` and pushes the exact same image to AWS ECR for all environments.
2.  **DAG Syncing**: CI/CD runs `aws s3 sync ./dags/ s3://my-dags-bucket/ --delete`. A sidecar container in the ECS Task Definition continuously syncs this bucket to your EFS mount.
3.  **Script Syncing**: CI/CD runs `aws s3 sync ./dags/glue_scripts/ s3://my-glue-scripts-bucket/ --delete` ensuring your Spark scripts are version controlled exactly alongside your DAGs.

import json
import boto3
import requests
import tempfile

def lambda_handler(event, context):
    """
    AWS Lambda function triggered by Airflow to ingest API data securely.
    Expects event: {"execution_date": "2024-01-01", "destination_bucket": "s3://my-bronze-bucket/..."}
    """
    execution_date = event.get('execution_date')
    destination_path = event.get('destination_bucket')
    
    # 1. Fetch mTLS Client Certificate from AWS Secrets Manager
    # Note: Standard ACM certificates cannot be exported for client-side mTLS.
    # You must store the client cert/key in AWS Secrets Manager (or use ACM Private CA).
    secrets_client = boto3.client('secretsmanager')
    secret_response = secrets_client.get_secret_value(SecretId='my-api-mtls-cert')
    secret_data = json.loads(secret_response['SecretString'])
    
    cert_body = secret_data['certificate']
    key_body = secret_data['private_key']
    
    # Write certs to temporary files for the requests library
    with tempfile.NamedTemporaryFile(delete=False) as cert_file, \
         tempfile.NamedTemporaryFile(delete=False) as key_file:
        cert_file.write(cert_body.encode())
        key_file.write(key_body.encode())
        cert_path = cert_file.name
        key_path = key_file.name

    # 2. Invoke External API securely using mTLS
    print(f"Fetching data for {execution_date} using mTLS...")
    response = requests.get(
        'https://external-secure-api.example.com/data',
        cert=(cert_path, key_path) # Presenting our client certificate
    )
    response.raise_for_status()
    
    # 3. Upload raw JSON to Bronze S3 Bucket
    s3_client = boto3.client('s3')
    
    # Extract bucket and key from the destination path (s3://bucket/key)
    path_parts = destination_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key_prefix = "/".join(path_parts[1:])
    
    file_key = f"{key_prefix}raw_export.json"
    
    print(f"Uploading data to s3://{bucket_name}/{file_key}")
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_key,
        Body=response.content
    )
    
    return {
        'statusCode': 200,
        'body': f"Successfully ingested {len(response.content)} bytes to Bronze layer."
    }

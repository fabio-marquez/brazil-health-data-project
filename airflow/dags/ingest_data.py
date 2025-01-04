from airflow import DAG
import zipfile
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
from botocore.client import Config
import os
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# MinIO connection details
BUCKET_NAME = 'landing'
OBJECT_NAME = 'cnes202411.zip'
LANDING_BUCKET_NAME = 'landing'
PROCESSED_BUCKET_NAME = 'bronze'

# Function to download the file from a URL
def download_zip_file(url, download_path):

    logging.info(f"Starting download from {url}")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for HTTP errors

        with open(download_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        logging.info(f"File downloaded successfully and saved to {download_path}")

    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download file from {url}: {e}")
        raise

# Function to upload a file to MinIO
def upload_file_to_minio(file_path, bucket_name, object_name):

    logging.info(f"Starting upload of {file_path} to bucket {bucket_name} as {object_name}")

    # Initialize MinIO client
    s3_client = boto3.client(
        's3',
        endpoint_url = os.environ.get('MINIO_ENDPOINT'),
        aws_access_key_impid = os.environ.get('MINIO_ACCESS_KEY'),
        aws_secret_access_key = os.environ.get('MINIO_SECRET_KEY'),
        region_name = 'us-east-1',
    )

    # Ensure the bucket exists
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)

    # Upload the file
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        logging.info(f"File {file_path} uploaded successfully to {bucket_name}/{object_name}")
    except Exception as e:
        logging.error(f"Failed to upload file {file_path} to bucket {bucket_name}")
        raise

# Function to extract and upload files to MinIO
def extract_and_upload_zip(zip_path, bucket_name):
    # Initialize MinIO client
    s3_client = boto3.client(
        's3',
        endpoint_url = os.environ.get('MINIO_ENDPOINT'),
        aws_access_key_id = os.environ.get('MINIO_ACCESS_KEY'),
        aws_secret_access_key = os.environ.get('MINIO_SECRET_KEY'),
        region_name = 'us-east-1',
    )

    # Extract ZIP contents
    extract_path = '/tmp/extracted_files'
    os.makedirs(extract_path, exist_ok=True)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    # Upload each extracted file
    for root, _, files in os.walk(extract_path):
        for file in files:
            file_path = os.path.join(root, file)
            object_name = os.path.relpath(file_path, extract_path)
            s3_client.upload_file(file_path, bucket_name, object_name)

# Define the task to handle the ZIP file
def download_extract_and_store_zip(**kwargs):
    url = kwargs.get('url', 'https://example.com/sample.zip')
    local_zip_path = '/tmp/example.zip'

    # Step 1: Download the ZIP file
    download_zip_file(url, local_zip_path)

    # Step 2: Upload ZIP to landing bucket
    upload_file_to_minio(local_zip_path, LANDING_BUCKET_NAME, os.path.basename(local_zip_path))

    # Step 3: Extract and upload contents to processed bucket
    extract_and_upload_zip(local_zip_path, PROCESSED_BUCKET_NAME)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'process_zip_file_and_store_extracted',
    default_args=default_args,
    description='Download a ZIP file, save it, extract contents, and store them in MinIO',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id='download_extract_and_store_zip',
        python_callable=download_extract_and_store_zip,
        op_kwargs={
            'url': 'https://cnes.datasus.gov.br/EstatisticasServlet?path=BASE_DE_DADOS_CNES_202411.ZIP',  # Replace with your ZIP file URL
        }
    )

task

from airflow import DAG
import zipfile
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import boto3
from botocore.client import Config
import os
import logging
from utils.storage import Storage

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

    # # Initialize MinIO client
    # s3_client = boto3.client(
    #     's3',
    #     endpoint_url = os.environ.get('MINIO_ENDPOINT'),
    #     aws_access_key_id = os.environ.get('MINIO_ACCESS_KEY'),
    #     aws_secret_access_key = os.environ.get('MINIO_SECRET_KEY'),
    #     region_name = 'us-east-1',
    # )

    storage = Storage()

    # Ensure the bucket exists
    try:
        storage.head_bucket(Bucket=bucket_name)
    except Exception:
        storage.create_bucket(Bucket=bucket_name)

    # Upload the file
    try:
        storage.upload_file(file_path, bucket_name, object_name)
        logging.info(f"File {file_path} uploaded successfully to {bucket_name}/{object_name}")
    except Exception as e:
        logging.error(f"Failed to upload file {file_path} to bucket {bucket_name}")
        raise

# Function to extract and upload files to MinIO
def extract_and_upload_zip(zip_path, bucket_name):
    # # Initialize MinIO client
    # s3_client = boto3.client(
    #     's3',
    #     endpoint_url = os.environ.get('MINIO_ENDPOINT'),
    #     aws_access_key_id = os.environ.get('MINIO_ACCESS_KEY'),
    #     aws_secret_access_key = os.environ.get('MINIO_SECRET_KEY'),
    #     region_name = 'us-east-1',
    # )

    storage = Storage()

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
            storage.upload_file(file_path, bucket_name, object_name)


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

     # Task 1: Download ZIP file
    download_zip_task = PythonOperator(
        task_id='download_zip',
        python_callable=download_zip_file,
        op_kwargs={
            'url': 'https://github.com//fabio-marquez/learning-ai/archive/refs/heads/main.zip',
            'download_path': '/tmp/example.zip',
        },
    )

    # Task 2: Upload ZIP to landing bucket
    upload_zip_task = PythonOperator(
        task_id='upload_zip_to_landing',
        python_callable=upload_file_to_minio,
        op_kwargs={
            'file_path': '/tmp/example.zip',
            'bucket_name': LANDING_BUCKET_NAME,
            'object_name': 'example.zip',
        },
    )

    # Task 3: Extract ZIP contents and upload to processed bucket
    extract_and_upload_task = PythonOperator(
        task_id='extract_and_upload_contents',
        python_callable=extract_and_upload_zip,
        op_kwargs={
            'zip_path': '/tmp/example.zip',
            'bucket_name': PROCESSED_BUCKET_NAME,
        },
    )

    # Define task dependencies
    download_zip_task >> upload_zip_task >> extract_and_upload_task
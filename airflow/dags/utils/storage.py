import boto3
import os
import logging


class Storage:
    def __init__(self):
        self.s3_client = boto3.client(
        's3',
        endpoint_url = os.environ.get('MINIO_ENDPOINT'),
        aws_access_key_id = os.environ.get('MINIO_ACCESS_KEY'),
        aws_secret_access_key = os.environ.get('MINIO_SECRET_KEY'),
        region_name = 'us-east-1',
    )

    logging.info("Storage object created")   
        
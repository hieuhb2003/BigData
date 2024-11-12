from minio import Minio
import yaml
import json
from pathlib import Path

class MinioUploader:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            minio_config = config['minio']

        self.client = Minio(
            minio_config['endpoint'],
            access_key=minio_config['access_key'],
            secret_key=minio_config['secret_key'],
            secure=minio_config['secure']
        )
        self.bucket_name = minio_config['bucket_name']

    def ensure_bucket_exists(self):
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    def upload_json(self, file_path, object_name):
        self.ensure_bucket_exists()
        self.client.fput_object(
            self.bucket_name,
            object_name,
            file_path
        )

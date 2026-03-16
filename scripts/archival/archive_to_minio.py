import os
import sys
import glob

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import boto3
from botocore.client import Config

from app.config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
from app.logger import get_logger

logger = get_logger("archive_to_minio")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

existing_buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
if MINIO_BUCKET not in existing_buckets:
    s3.create_bucket(Bucket=MINIO_BUCKET)
    logger.info("Created bucket: %s", MINIO_BUCKET)
else:
    logger.info("Bucket already exists: %s", MINIO_BUCKET)

DATA_BASE_PATH = os.environ.get("DATA_BASE_PATH", "data")
LAYERS = ["bronze", "silver", "gold"]

uploaded = 0
for layer in LAYERS:
    layer_path = os.path.join(DATA_BASE_PATH, layer)
    files = glob.glob(os.path.join(layer_path, "**", "*.parquet"), recursive=True)

    if not files:
        logger.warning("No parquet files found in %s", layer_path)
        continue

    for local_path in files:
        key = os.path.relpath(local_path, DATA_BASE_PATH)
        s3.upload_file(local_path, MINIO_BUCKET, key)
        logger.info("Uploaded %s → s3://%s/%s", local_path, MINIO_BUCKET, key)
        uploaded += 1

logger.info("Archive complete — %d files uploaded to bucket '%s'", uploaded, MINIO_BUCKET)

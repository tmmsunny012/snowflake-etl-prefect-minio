"""
MinIO Tasks for Prefect ETL Flow

Handles file upload and download operations with MinIO (S3-compatible storage).
"""

import os
from pathlib import Path
from minio import Minio
from minio.error import S3Error
from prefect import task, get_run_logger


def get_minio_client() -> Minio:
    """Create MinIO client from environment variables."""
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,  # Use HTTP for local development
    )


@task(name="Upload to MinIO", retries=2, retry_delay_seconds=5)
def upload_to_minio(
    local_file_path: str,
    bucket_name: str = None,
    object_name: str = None,
) -> str:
    """
    Upload a file to MinIO bucket.
    
    Args:
        local_file_path: Path to the local file to upload
        bucket_name: Target bucket name (default from env)
        object_name: Object name in bucket (default: filename)
    
    Returns:
        The object path in MinIO (bucket/object_name)
    """
    logger = get_run_logger()
    
    bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "etl-bucket")
    object_name = object_name or Path(local_file_path).name
    
    client = get_minio_client()
    
    # Ensure bucket exists
    if not client.bucket_exists(bucket_name):
        logger.info(f"Creating bucket: {bucket_name}")
        client.make_bucket(bucket_name)
    
    # Upload file
    logger.info(f"Uploading {local_file_path} to {bucket_name}/{object_name}")
    
    try:
        client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=local_file_path,
            content_type="text/csv",
        )
        logger.info(f"Successfully uploaded to {bucket_name}/{object_name}")
        return f"{bucket_name}/{object_name}"
    
    except S3Error as e:
        logger.error(f"Failed to upload: {e}")
        raise


@task(name="Download from MinIO", retries=2, retry_delay_seconds=5)
def download_from_minio(
    object_name: str,
    local_file_path: str,
    bucket_name: str = None,
) -> str:
    """
    Download a file from MinIO bucket.
    
    Args:
        object_name: Object name in the bucket
        local_file_path: Local path to save the file
        bucket_name: Source bucket name (default from env)
    
    Returns:
        The local file path where the file was saved
    """
    logger = get_run_logger()
    
    bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "etl-bucket")
    
    client = get_minio_client()
    
    logger.info(f"Downloading {bucket_name}/{object_name} to {local_file_path}")
    
    try:
        client.fget_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=local_file_path,
        )
        logger.info(f"Successfully downloaded to {local_file_path}")
        return local_file_path
    
    except S3Error as e:
        logger.error(f"Failed to download: {e}")
        raise


@task(name="List MinIO Objects")
def list_minio_objects(bucket_name: str = None, prefix: str = "") -> list[str]:
    """
    List objects in a MinIO bucket.
    
    Args:
        bucket_name: Bucket to list (default from env)
        prefix: Filter objects by prefix
    
    Returns:
        List of object names
    """
    logger = get_run_logger()
    
    bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "etl-bucket")
    client = get_minio_client()
    
    try:
        objects = client.list_objects(bucket_name, prefix=prefix)
        object_names = [obj.object_name for obj in objects]
        logger.info(f"Found {len(object_names)} objects in {bucket_name}")
        return object_names
    
    except S3Error as e:
        logger.error(f"Failed to list objects: {e}")
        raise

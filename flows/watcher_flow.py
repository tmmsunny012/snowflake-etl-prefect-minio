"""
MinIO File Watcher Flow

Scheduled flow that polls MinIO for new files and triggers the ETL pipeline.
Runs on a configurable interval (default: every 60 seconds).

Persistence:
- Processed files tracker stored in MinIO (metadata/processed_files.json)
- ETL run logs stored in MinIO (logs/run_YYYY-MM-DD_HH-MM-SS.json)

Usage:
    python flows/watcher_flow.py
    python flows/watcher_flow.py --once  # Single check
    python flows/watcher_flow.py --interval 30  # 30 second polling
"""

import os
import json
import io
from pathlib import Path
from datetime import datetime
from typing import Optional

from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

# Add parent directory to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from flows.tasks.minio_tasks import get_minio_client
from flows.etl_flow import etl_pipeline


# MinIO paths for metadata
METADATA_BUCKET = "etl-bucket"
PROCESSED_FILES_KEY = "metadata/processed_files.json"
LOGS_PREFIX = "logs/"


def load_processed_files_from_minio() -> set:
    """Load the set of already processed file names from MinIO."""
    logger_name = "watcher"
    
    try:
        client = get_minio_client()
        
        # Check if file exists
        try:
            response = client.get_object(METADATA_BUCKET, PROCESSED_FILES_KEY)
            data = json.loads(response.read().decode('utf-8'))
            response.close()
            response.release_conn()
            return set(data)
        except Exception:
            # File doesn't exist yet
            return set()
            
    except Exception as e:
        print(f"[!] Could not load processed files from MinIO: {e}")
        return set()


def save_processed_files_to_minio(processed: set):
    """Save the set of processed file names to MinIO."""
    try:
        client = get_minio_client()
        
        # Convert to JSON
        data = json.dumps(list(processed), indent=2)
        data_bytes = data.encode('utf-8')
        
        # Upload to MinIO
        client.put_object(
            bucket_name=METADATA_BUCKET,
            object_name=PROCESSED_FILES_KEY,
            data=io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="application/json"
        )
        print(f"[OK] Saved processed files list to MinIO: {PROCESSED_FILES_KEY}")
        
    except Exception as e:
        print(f"[!] Could not save processed files to MinIO: {e}")


def save_run_log_to_minio(log_data: dict):
    """Save ETL run log to MinIO for tracking."""
    try:
        client = get_minio_client()
        
        # Generate log filename with timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_key = f"{LOGS_PREFIX}run_{timestamp}.json"
        
        # Convert to JSON
        data = json.dumps(log_data, indent=2, default=str)
        data_bytes = data.encode('utf-8')
        
        # Upload to MinIO
        client.put_object(
            bucket_name=METADATA_BUCKET,
            object_name=log_key,
            data=io.BytesIO(data_bytes),
            length=len(data_bytes),
            content_type="application/json"
        )
        print(f"[OK] Saved run log to MinIO: {log_key}")
        
    except Exception as e:
        print(f"[!] Could not save run log to MinIO: {e}")


@task(name="Check for New Files")
def check_for_new_files(bucket_name: Optional[str] = None, prefix: str = "") -> list[str]:
    """
    Check MinIO bucket for new files that haven't been processed yet.
    
    Returns:
        List of new file names to process
    """
    logger = get_run_logger()
    
    bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "etl-bucket")
    
    # Get current files in bucket (exclude metadata and logs folders)
    client = get_minio_client()
    try:
        objects = client.list_objects(bucket_name, prefix=prefix)
        current_files = {
            obj.object_name for obj in objects 
            if not obj.object_name.startswith('metadata/') 
            and not obj.object_name.startswith('logs/')
        }
    except Exception as e:
        logger.error(f"Failed to list MinIO objects: {e}")
        return []
    
    # Load already processed files from MinIO
    processed_files = load_processed_files_from_minio()
    
    # Find new files (CSV only)
    new_files = [
        f for f in current_files 
        if f not in processed_files and f.endswith('.csv')
    ]
    
    if new_files:
        logger.info(f"Found {len(new_files)} new file(s) to process: {new_files}")
    else:
        logger.info("No new files to process")
    
    return new_files


@task(name="Mark File as Processed")
def mark_file_processed(file_name: str):
    """Mark a file as processed so it won't be picked up again."""
    logger = get_run_logger()
    
    processed = load_processed_files_from_minio()
    processed.add(file_name)
    save_processed_files_to_minio(processed)
    
    logger.info(f"Marked '{file_name}' as processed")


@flow(name="MinIO File Watcher")
def watch_minio_for_files(
    bucket_name: Optional[str] = None,
    prefix: str = "",
    run_validation: bool = True,
):
    """
    Watch MinIO bucket for new files and trigger ETL pipeline.
    
    This flow checks for new CSV files in the specified bucket
    and processes each one through the ETL pipeline.
    
    Args:
        bucket_name: MinIO bucket to watch (default from env)
        prefix: Optional prefix to filter files
        run_validation: Whether to run validation after ETL
    """
    logger = get_run_logger()
    
    logger.info("=" * 60)
    logger.info("MinIO File Watcher - Checking for new files...")
    logger.info("=" * 60)
    
    # Check for new files
    new_files = check_for_new_files(bucket_name=bucket_name, prefix=prefix)
    
    if not new_files:
        logger.info("No new files found. Watcher will check again on next run.")
        return {"files_processed": 0, "status": "idle"}
    
    # Process each new file
    results = []
    bucket_name = bucket_name or os.getenv("MINIO_BUCKET", "etl-bucket")
    
    for file_name in new_files:
        logger.info(f"\n>> Processing file: {file_name}")
        
        run_result = {
            "file": file_name,
            "started_at": datetime.now().isoformat(),
            "status": "pending"
        }
        
        try:
            # Download file from MinIO to local temp path first
            client = get_minio_client()
            local_path = f"/tmp/{file_name}"
            client.fget_object(bucket_name, file_name, local_path)
            logger.info(f"Downloaded {file_name} from MinIO to {local_path}")
            
            # Run ETL pipeline for this file
            # skip_minio=True because file is already in MinIO (we just downloaded it for processing)
            result = etl_pipeline(
                csv_file_path=local_path,
                skip_minio=True,  # File is already in MinIO
                run_validation=run_validation,
            )
            
            # Mark as processed only if successful
            mark_file_processed(file_name)
            
            run_result["status"] = "success"
            run_result["completed_at"] = datetime.now().isoformat()
            run_result["etl_result"] = result
            
        except Exception as e:
            logger.error(f"Failed to process {file_name}: {e}")
            run_result["status"] = "error"
            run_result["error"] = str(e)
            run_result["completed_at"] = datetime.now().isoformat()
        
        results.append(run_result)
        
        # Save individual run log to MinIO
        save_run_log_to_minio(run_result)
    
    logger.info("=" * 60)
    logger.info(f"Watcher complete. Processed {len(results)} file(s)")
    logger.info("=" * 60)
    
    return {"files_processed": len(results), "results": results}


@flow(name="Scheduled Watcher", log_prints=True)
def scheduled_watcher(interval_seconds: int = 60):
    """
    Continuously watch MinIO with a polling interval.
    
    Args:
        interval_seconds: How often to check for new files (default: 60)
    """
    import time
    
    logger = get_run_logger()
    logger.info(f"Starting scheduled watcher (polling every {interval_seconds} seconds)")
    logger.info("Press Ctrl+C to stop")
    
    while True:
        try:
            watch_minio_for_files()
        except Exception as e:
            logger.error(f"Watcher error: {e}")
        
        logger.info(f"Sleeping for {interval_seconds} seconds...")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    import argparse
    
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="MinIO File Watcher")
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=60,
        help="Polling interval in seconds (default: 60)"
    )
    parser.add_argument(
        "--once", "-o",
        action="store_true",
        help="Run once and exit (don't loop)"
    )
    
    args = parser.parse_args()
    
    if args.once:
        # Run single check
        result = watch_minio_for_files()
        print(f"\nResult: {result}")
    else:
        # Run continuous polling
        scheduled_watcher(interval_seconds=args.interval)

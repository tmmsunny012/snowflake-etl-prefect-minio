"""
Main ETL Flow - Snowflake Pipeline with Prefect

This flow orchestrates the complete ETL process:
1. Upload CSV to MinIO
2. Download and analyze schema
3. Create parent table in Snowflake
4. Load data via staging table
5. MERGE for incremental updates
6. Create secure views
7. Validate results

Run locally:
    python flows/etl_flow.py

Run via Prefect:
    prefect deployment build flows/etl_flow.py:etl_pipeline -n main
    prefect deployment apply etl_pipeline-deployment.yaml
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

from prefect import flow, get_run_logger
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from flows.tasks.minio_tasks import upload_to_minio, download_from_minio, list_minio_objects
from flows.tasks.schema_detector import detect_schema, generate_ddl
from flows.tasks.snowflake_tasks import (
    create_parent_table,
    load_to_staging,
    merge_to_parent,
    create_secure_views,
    cleanup_staging,
)
from flows.tasks.validation import (
    validate_row_counts,
    validate_sample_data,
    validate_variant_column,
    generate_validation_report,
)


@flow(name="ETL Pipeline", description="Snowflake ETL with MinIO and Prefect")
def etl_pipeline(
    csv_file_path: Optional[str] = None,
    parent_table_name: str = "PARENT_EVENTS",
    skip_minio: bool = False,
    run_validation: bool = True,
):
    """
    Main ETL pipeline flow.
    
    Args:
        csv_file_path: Path to the CSV file to process
                       (default: data/sample_events.csv)
        parent_table_name: Name for the parent table in Snowflake
        skip_minio: If True, skip MinIO upload/download (use local file)
        run_validation: If True, run validation tasks at the end
    
    Returns:
        Dictionary with pipeline results
    """
    logger = get_run_logger()
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info("Starting ETL Pipeline")
    logger.info("=" * 60)
    
    # Default CSV path
    if csv_file_path is None:
        csv_file_path = str(Path(__file__).parent.parent / "data" / "sample_events.csv")
    
    logger.info(f"Input file: {csv_file_path}")
    logger.info(f"Parent table: {parent_table_name}")
    
    results = {
        "start_time": start_time.isoformat(),
        "csv_file": csv_file_path,
        "parent_table": parent_table_name,
    }
    
    # -------------------------------------------
    # Step 1: MinIO Operations (optional)
    # -------------------------------------------
    if not skip_minio:
        logger.info("\n>> Step 1: Upload to MinIO")
        minio_path = upload_to_minio(csv_file_path)
        results["minio_path"] = minio_path
        
        # Download to temp location (simulates real workflow)
        temp_file = f"/tmp/{Path(csv_file_path).name}"
        local_file = download_from_minio(
            object_name=Path(csv_file_path).name,
            local_file_path=temp_file,
        )
        csv_file_path = local_file
    else:
        logger.info("\n>> Step 1: Skipping MinIO (using local file)")
    
    # -------------------------------------------
    # Step 2: Schema Detection
    # -------------------------------------------
    logger.info("\n>> Step 2: Detecting Schema")
    schema = detect_schema(csv_file_path)
    results["schema"] = schema
    
    # -------------------------------------------
    # Step 3: Generate and Execute DDL
    # -------------------------------------------
    logger.info("\n>> Step 3: Creating Parent Table")
    ddl = generate_ddl(
        schema=schema,
        table_name=parent_table_name,
        if_not_exists=True,
    )
    create_parent_table(ddl)
    
    # -------------------------------------------
    # Step 4: Load to Staging
    # -------------------------------------------
    logger.info("\n>> Step 4: Loading to Staging Table")
    rows_loaded = load_to_staging(
        csv_path=csv_file_path,
        staging_table="STAGING_EVENTS",
    )
    results["rows_staged"] = rows_loaded
    
    # -------------------------------------------
    # Step 5: MERGE to Parent
    # -------------------------------------------
    logger.info("\n>> Step 5: Merging to Parent Table")
    merge_result = merge_to_parent(
        staging_table="STAGING_EVENTS",
        parent_table=parent_table_name,
        key_column="ID",
    )
    results["merge_result"] = merge_result
    
    # -------------------------------------------
    # Step 6: Create Secure Views
    # -------------------------------------------
    logger.info("\n>> Step 6: Creating Secure Views")
    views_created = create_secure_views()
    results["views_created"] = views_created
    
    # -------------------------------------------
    # Step 7: Cleanup Staging
    # -------------------------------------------
    logger.info("\n>> Step 7: Cleaning Up Staging")
    cleanup_staging("STAGING_EVENTS")
    
    # -------------------------------------------
    # Step 8: Validation (optional)
    # -------------------------------------------
    if run_validation:
        logger.info("\n>> Step 8: Running Validation")
        row_counts = validate_row_counts()
        sample_data = validate_sample_data(parent_table_name)
        variant_validation = validate_variant_column(parent_table_name)
        
        report = generate_validation_report(
            row_counts=row_counts,
            sample_data=sample_data,
            variant_validation=variant_validation,
        )
        results["validation_report"] = report
    
    # -------------------------------------------
    # Complete
    # -------------------------------------------
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    results["end_time"] = end_time.isoformat()
    results["duration_seconds"] = duration
    
    logger.info("\n" + "=" * 60)
    logger.info(f"ETL Pipeline Complete!")
    logger.info(f"Duration: {duration:.2f} seconds")
    logger.info(f"Rows processed: {merge_result.get('total_rows', 'N/A')}")
    logger.info(f"Views created: {', '.join(views_created)}")
    logger.info("=" * 60)
    
    return results


@flow(name="Setup Infrastructure", description="Create Snowflake database, warehouse, and stage")
def setup_infrastructure():
    """
    Setup flow to create Snowflake infrastructure.
    
    This is run once before the main ETL pipeline.
    Uses the Snowflake CLI commands via SQL execution.
    """
    logger = get_run_logger()
    
    logger.info("Setting up Snowflake infrastructure...")
    logger.info("Please run the following commands with Snowflake CLI:")
    logger.info("")
    logger.info("  snow sql -f snowflake/setup/01_create_database.sql")
    logger.info("  snow sql -f snowflake/setup/02_create_warehouse.sql")
    logger.info("  snow sql -f snowflake/setup/03_create_stage.sql")
    logger.info("")
    logger.info("Or run the setup script: ./scripts/setup.ps1")
    
    return {"status": "manual_setup_required"}


# Entry point for direct execution
if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()
    
    # Check for required env vars
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"❌ Missing required environment variables: {missing}")
        print("Please copy .env.example to .env and fill in your values.")
        sys.exit(1)
    
    # Run the pipeline
    result = etl_pipeline(
        skip_minio=False,  # Use MinIO for file storage (default for Docker)
        run_validation=True,
    )
    
    print("\n📊 Pipeline Result:")
    for key, value in result.items():
        if key != "validation_report":
            print(f"  {key}: {value}")

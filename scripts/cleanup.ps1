<#
.SYNOPSIS
    Cleanup/Teardown Script - Remove all resources

.DESCRIPTION
    This script removes all resources created by the ETL pipeline:
    - Stops and removes Docker containers
    - Removes Docker volumes (MinIO data, Prefect data)
    - Drops Snowflake database, warehouse, and stage
    
    Use with caution! This is destructive.

.EXAMPLE
    .\scripts\cleanup.ps1
    .\scripts\cleanup.ps1 -SkipSnowflake  # Keep Snowflake resources
    .\scripts\cleanup.ps1 -SkipDocker     # Keep Docker resources
#>

param(
    [switch]$SkipDocker,
    [switch]$SkipSnowflake,
    [switch]$Force  # Skip confirmation prompt
)

$ErrorActionPreference = "Stop"

# Colors
function Write-Info { param($msg) Write-Host "  $msg" -ForegroundColor Cyan }
function Write-Success { param($msg) Write-Host "[OK] $msg" -ForegroundColor Green }
function Write-Warn { param($msg) Write-Host "[!] $msg" -ForegroundColor Yellow }

# Get project root
$ProjectRoot = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "==============================================================" -ForegroundColor Red
Write-Host "                    CLEANUP / TEARDOWN                        " -ForegroundColor Red
Write-Host "==============================================================" -ForegroundColor Red
Write-Host ""
Write-Host "This will remove:" -ForegroundColor Yellow
if (-not $SkipDocker) {
    Write-Host "  - Docker containers (etl-minio, etl-prefect-server, etl-prefect-worker, etl-file-watcher)" -ForegroundColor White
    Write-Host "  - Docker volumes (minio-data, prefect-data)" -ForegroundColor White
    Write-Host "  - Docker images" -ForegroundColor White
}
if (-not $SkipSnowflake) {
    Write-Host "  - Snowflake database: STAGING_DB" -ForegroundColor White
    Write-Host "  - Snowflake warehouse: ETL_WAREHOUSE" -ForegroundColor White
    Write-Host "  - Snowflake stage: ETL_STAGE" -ForegroundColor White
}
Write-Host ""

# Confirmation prompt
if (-not $Force) {
    $confirm = Read-Host "Are you sure you want to proceed? (yes/no)"
    if ($confirm -ne "yes") {
        Write-Host "Cleanup cancelled." -ForegroundColor Yellow
        exit 0
    }
}

Write-Host ""

# ===========================================================================
# STEP 1: Stop and Remove Docker Resources
# ===========================================================================
if (-not $SkipDocker) {
    Write-Host "[1/2] Cleaning up Docker resources..." -ForegroundColor Yellow
    
    Push-Location $ProjectRoot
    try {
        # Clear MinIO tracker first (if containers are running)
        Write-Info "Clearing file tracker..."
        $ErrorActionPreference = "SilentlyContinue"
        docker-compose exec -T prefect-worker python -c "from flows.tasks.minio_tasks import get_minio_client; c = get_minio_client(); c.remove_object('etl-bucket', 'metadata/processed_files.json')" 2>&1 | Out-Null
        $ErrorActionPreference = "Stop"
        
        # Stop containers AND remove volumes
        Write-Info "Stopping containers and removing volumes..."
        docker-compose down -v 2>&1 | Out-Null
        
        # Remove images (optional)
        Write-Info "Removing images..."
        docker rmi snowflake-etl-prefect-minio-prefect-worker 2>&1 | Out-Null
        docker rmi snowflake-etl-prefect-minio-file-watcher 2>&1 | Out-Null
        
        Write-Success "Docker resources removed"
    }
    catch {
        Write-Warn "Some Docker resources may not have been removed: $_"
    }
    finally {
        Pop-Location
    }
}
else {
    Write-Host "[1/2] Skipping Docker cleanup" -ForegroundColor Yellow
}

# ===========================================================================
# STEP 2: Drop Snowflake Resources
# ===========================================================================
if (-not $SkipSnowflake) {
    Write-Host ""
    Write-Host "[2/2] Cleaning up Snowflake resources..." -ForegroundColor Yellow
    
    # Check for Snowflake CLI
    $snowCli = Get-Command snow -ErrorAction SilentlyContinue
    if (-not $snowCli) {
        Write-Warn "Snowflake CLI not found. Skipping Snowflake cleanup."
        Write-Host "  You can manually drop resources with:" -ForegroundColor Gray
        Write-Host "    DROP DATABASE IF EXISTS STAGING_DB;" -ForegroundColor Gray
        Write-Host "    DROP WAREHOUSE IF EXISTS ETL_WAREHOUSE;" -ForegroundColor Gray
    }
    else {
        try {
            # Drop views first
            Write-Info "Dropping views..."
            snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.GERMANY_EVENTS" 2>&1 | Out-Null
            snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.RECENT_SIGNUPS" 2>&1 | Out-Null
            snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.US_EVENTS" 2>&1 | Out-Null
            snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.HIGH_VALUE_PURCHASES" 2>&1 | Out-Null
            snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.FR_SIGNUPS" 2>&1 | Out-Null
            snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.HIGH_VALUE" 2>&1 | Out-Null
            snow sql -q "DROP VIEW IF EXISTS STAGING_DB.PUBLIC.CUSTOM_VIEW" 2>&1 | Out-Null
            
            # Drop tables
            Write-Info "Dropping tables..."
            snow sql -q "DROP TABLE IF EXISTS STAGING_DB.PUBLIC.PARENT_EVENTS" 2>&1 | Out-Null
            snow sql -q "DROP TABLE IF EXISTS STAGING_DB.PUBLIC.STAGING_EVENTS" 2>&1 | Out-Null
            
            # Drop stage
            Write-Info "Dropping stage..."
            snow sql -q "DROP STAGE IF EXISTS STAGING_DB.PUBLIC.ETL_STAGE" 2>&1 | Out-Null
            
            # Drop database
            Write-Info "Dropping database..."
            snow sql -q "DROP DATABASE IF EXISTS STAGING_DB" 2>&1 | Out-Null
            
            # Drop warehouse
            Write-Info "Dropping warehouse..."
            snow sql -q "DROP WAREHOUSE IF EXISTS ETL_WAREHOUSE" 2>&1 | Out-Null
            
            Write-Success "Snowflake resources removed"
        }
        catch {
            Write-Warn "Some Snowflake resources may not have been removed: $_"
        }
    }
}
else {
    Write-Host ""
    Write-Host "[2/2] Skipping Snowflake cleanup" -ForegroundColor Yellow
}

# ===========================================================================
# SUMMARY
# ===========================================================================
Write-Host ""
Write-Host "==============================================================" -ForegroundColor Green
Write-Host "                    Cleanup Complete!                         " -ForegroundColor Green
Write-Host "==============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  To set up again from scratch, run:" -ForegroundColor Yellow
Write-Host "    powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1" -ForegroundColor Cyan
Write-Host ""

<#
.SYNOPSIS
    Run the ETL Flow via Docker

.DESCRIPTION
    This script runs the Snowflake ETL pipeline inside the Docker container.
    No local Python dependencies required!

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\run_flow.ps1
    powershell -ExecutionPolicy Bypass -File .\scripts\run_flow.ps1 -Local
#>

param(
    [switch]$Local  # Run locally instead of Docker (requires pip install)
)

$ErrorActionPreference = "Stop"

# Get project root directory
$ProjectRoot = Split-Path -Parent $PSScriptRoot

Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "   Snowflake ETL Pipeline - Run Flow       " -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

# Load .env file
$EnvFile = Join-Path $ProjectRoot ".env"
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        if ($_ -match "^([A-Za-z_][A-Za-z0-9_]*)=(.*)$") {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [Environment]::SetEnvironmentVariable($name, $value, "Process")
        }
    }
    Write-Host "[OK] Loaded environment from .env" -ForegroundColor Green
}
else {
    Write-Host "[ERROR] No .env file found. Run setup.ps1 first!" -ForegroundColor Red
    exit 1
}

Push-Location $ProjectRoot

if ($Local) {
    # -----------------------------------------
    # Run locally with Python (requires dependencies)
    # -----------------------------------------
    Write-Host ""
    Write-Host "Running locally with Python..." -ForegroundColor Yellow
    Write-Host "(This requires: pip install -r requirements.txt)" -ForegroundColor Gray
    Write-Host ""
    
    $Env:PYTHONPATH = $ProjectRoot
    python flows/etl_flow.py
    
}
else {
    # -----------------------------------------
    # Run via Docker (default - no local deps needed)
    # -----------------------------------------
    Write-Host "Running via Docker container..." -ForegroundColor Yellow
    Write-Host ""
    
    # Check if Docker is running
    docker ps 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] Docker is not running. Please start Docker Desktop." -ForegroundColor Red
        exit 1
    }
    
    # Check if containers are running
    $containers = docker ps --format "{{.Names}}" 2>$null
    if (-not ($containers -match "etl-prefect-worker")) {
        Write-Host "Docker containers not running. Starting..." -ForegroundColor Yellow
        docker-compose up -d 2>&1 | Out-Null
        Write-Host "Waiting for services to be healthy..."
        Start-Sleep -Seconds 15
    }
    
    Write-Host "Executing ETL flow inside container..." -ForegroundColor Cyan
    Write-Host ""
    
    # Execute flow in the worker container
    docker-compose exec -T prefect-worker python /app/flows/etl_flow.py
}

Pop-Location

Write-Host ""
Write-Host "============================================" -ForegroundColor Green
Write-Host "       Flow Execution Complete!            " -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Green
Write-Host ""
Write-Host "Verify results with:" -ForegroundColor Yellow
Write-Host "  snow sql -q `"SELECT * FROM STAGING_DB.PUBLIC.PARENT_EVENTS`"" -ForegroundColor Gray
Write-Host "  snow sql -q `"SELECT * FROM STAGING_DB.PUBLIC.GERMANY_EVENTS`"" -ForegroundColor Gray
Write-Host "  snow sql -q `"SELECT * FROM STAGING_DB.PUBLIC.RECENT_SIGNUPS`"" -ForegroundColor Gray
Write-Host ""

<#
.SYNOPSIS
    Setup script for Snowflake ETL Pipeline

.DESCRIPTION
    This script sets up everything needed to run the ETL pipeline:
    1. Creates/configures .env file
    2. Configures Snowflake CLI connection
    3. Creates Snowflake infrastructure (database, warehouse, stage)
    4. Optionally starts Docker services

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
    powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1 -SkipDocker
#>

param(
    [switch]$SkipDocker,
    [switch]$SkipSnowflake
)

$ErrorActionPreference = "Stop"

# Colors for output
function Write-Success { param($msg) Write-Host "[OK] $msg" -ForegroundColor Green }
function Write-Info { param($msg) Write-Host "    $msg" -ForegroundColor Cyan }
function Write-Warn { param($msg) Write-Host "[WARN] $msg" -ForegroundColor Yellow }
function Write-Err { param($msg) Write-Host "[ERROR] $msg" -ForegroundColor Red }

Write-Host ""
Write-Host "==============================================================" -ForegroundColor Cyan
Write-Host "       Snowflake ETL Pipeline - Setup Script                  " -ForegroundColor Cyan
Write-Host "==============================================================" -ForegroundColor Cyan
Write-Host ""

# Get project root directory
$ProjectRoot = Split-Path -Parent $PSScriptRoot

# ===========================================================================
# STEP 0: Check Prerequisites
# ===========================================================================
Write-Host "[0/4] Checking prerequisites..." -ForegroundColor Yellow

# Check Docker (unless skipping)
if (-not $SkipDocker) {
    $docker = Get-Command docker -ErrorAction SilentlyContinue
    if (-not $docker) {
        Write-Host ""
        Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Red
        Write-Host "  |                    DOCKER NOT FOUND                         |" -ForegroundColor Red
        Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Red
        Write-Host ""
        Write-Host "  Docker Desktop is required for MinIO and Prefect services." -ForegroundColor Yellow
        Write-Host ""
        Write-Host "  To install Docker Desktop:" -ForegroundColor Cyan
        Write-Host "    1. Download from: https://www.docker.com/products/docker-desktop/" -ForegroundColor White
        Write-Host "    2. Run the installer" -ForegroundColor White
        Write-Host "    3. Restart your computer" -ForegroundColor White
        Write-Host "    4. Start Docker Desktop" -ForegroundColor White
        Write-Host "    5. Run this script again" -ForegroundColor White
        Write-Host ""
        Write-Host "  Alternatively, run with -SkipDocker to set up Snowflake only:" -ForegroundColor Gray
        Write-Host "    powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1 -SkipDocker" -ForegroundColor Gray
        Write-Host ""
        exit 1
    }
    
    # Check if Docker daemon is running
    & docker info 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Red
        Write-Host "  |               DOCKER IS NOT RUNNING                         |" -ForegroundColor Red
        Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Red
        Write-Host ""
        Write-Host "  Please start Docker Desktop and try again." -ForegroundColor Yellow
        Write-Host ""
        exit 1
    }
    
    Write-Success "Docker is installed and running"
}

# Check Snowflake CLI
$snowCli = Get-Command snow -ErrorAction SilentlyContinue
if (-not $snowCli) {
    Write-Host ""
    Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Red
    Write-Host "  |              SNOWFLAKE CLI NOT FOUND                        |" -ForegroundColor Red
    Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Install with: pip install snowflake-cli-labs" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}
Write-Success "Snowflake CLI found"

# Test Snowflake CLI connection
Write-Info "Testing Snowflake CLI connection..."
snow sql -q "SELECT 1" 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Red
    Write-Host "  |          SNOWFLAKE CLI NOT CONFIGURED                       |" -ForegroundColor Red
    Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Red
    Write-Host ""
    Write-Host "  The Snowflake CLI is installed but not configured." -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Please create ~/.snowflake/config.toml with your credentials:" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "    [connections.default]" -ForegroundColor White
    Write-Host "    account = `"your_account.region`"" -ForegroundColor White
    Write-Host "    user = `"your_username`"" -ForegroundColor White
    Write-Host "    password = `"your_password`"" -ForegroundColor White
    Write-Host "    warehouse = `"ETL_WAREHOUSE`"" -ForegroundColor White
    Write-Host "    database = `"STAGING_DB`"" -ForegroundColor White
    Write-Host "    role = `"ACCOUNTADMIN`"" -ForegroundColor White
    Write-Host ""
    Write-Host "  Or copy the example:" -ForegroundColor Cyan
    Write-Host "    copy snowflake\connections\config.toml.example ~/.snowflake/config.toml" -ForegroundColor Gray
    Write-Host ""
    exit 1
}
Write-Success "Snowflake CLI connection verified"

# ===========================================================================
# STEP 1: Environment File
# ===========================================================================
Write-Host ""
Write-Host "[1/4] Checking environment configuration..." -ForegroundColor Yellow

$EnvFile = Join-Path $ProjectRoot ".env"
$EnvExample = Join-Path $ProjectRoot ".env.example"

if (-not (Test-Path $EnvFile)) {
    Write-Warn "No .env file found. Creating from template..."
    Copy-Item $EnvExample $EnvFile
    Write-Host ""
    Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Yellow
    Write-Host "  |  IMPORTANT: You need to edit .env with your credentials!   |" -ForegroundColor Yellow
    Write-Host "  |                                                             |" -ForegroundColor Yellow
    Write-Host "  |  Required values:                                           |" -ForegroundColor Yellow
    Write-Host "  |    - SNOWFLAKE_ACCOUNT  (e.g., abc12345.us-east-1)         |" -ForegroundColor Yellow
    Write-Host "  |    - SNOWFLAKE_USER     (your username)                    |" -ForegroundColor Yellow
    Write-Host "  |    - SNOWFLAKE_PASSWORD (your password)                    |" -ForegroundColor Yellow
    Write-Host "  +-------------------------------------------------------------+" -ForegroundColor Yellow
    Write-Host ""
    
    # Open in default editor
    Start-Process notepad $EnvFile -Wait
    Write-Host ""
}

# Load and validate .env file
Write-Info "Loading .env file..."
$envVars = @{}
Get-Content $EnvFile | ForEach-Object {
    $line = $_
    if ($line -match '^([A-Za-z_][A-Za-z0-9_]*)=(.*)$') {
        $name = $matches[1].Trim()
        $value = $matches[2].Trim()
        $envVars[$name] = $value
        [Environment]::SetEnvironmentVariable($name, $value, "Process")
    }
}

# Validate required variables
$requiredVars = @("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
$missing = @()
foreach ($var in $requiredVars) {
    $val = $envVars[$var]
    if (-not $val -or $val -match "your-" -or $val -match "<") {
        $missing += $var
    }
}

if ($missing.Count -gt 0) {
    Write-Err "Missing or invalid credentials in .env file:"
    foreach ($m in $missing) {
        Write-Host "    - $m" -ForegroundColor Red
    }
    Write-Host ""
    Write-Host "Please edit .env with your actual Snowflake credentials and run again."
    exit 1
}

Write-Success ".env file configured"
Write-Info ("Account: " + $envVars["SNOWFLAKE_ACCOUNT"])
Write-Info ("User: " + $envVars["SNOWFLAKE_USER"])

# ===========================================================================
# STEP 2: Snowflake CLI Configuration
# ===========================================================================
Write-Host ""
Write-Host "[2/4] Configuring Snowflake CLI..." -ForegroundColor Yellow

# Check if snow CLI is installed
$snowCli = Get-Command snow -ErrorAction SilentlyContinue
if (-not $snowCli) {
    Write-Err "Snowflake CLI not found!"
    Write-Host ""
    Write-Host "  Install with: pip install snowflake-cli-labs" -ForegroundColor Yellow
    Write-Host "  Then run this script again."
    exit 1
}

$snowVersion = & snow --version 2>&1
Write-Info "Found: $snowVersion"

# Create config directory
$configDir = Join-Path $env:USERPROFILE ".snowflake"
$configFile = Join-Path $configDir "config.toml"

if (-not (Test-Path $configDir)) {
    New-Item -ItemType Directory -Path $configDir -Force | Out-Null
}

# Get values with defaults
$warehouse = $envVars["SNOWFLAKE_WAREHOUSE"]
if (-not $warehouse) { $warehouse = "ETL_WAREHOUSE" }

$database = $envVars["SNOWFLAKE_DATABASE"]
if (-not $database) { $database = "STAGING_DB" }

$schema = $envVars["SNOWFLAKE_SCHEMA"]
if (-not $schema) { $schema = "PUBLIC" }

$role = $envVars["SNOWFLAKE_ROLE"]
if (-not $role) { $role = "ACCOUNTADMIN" }

# Build config content with [connections.default] block
$configContent = @"
[connections.default]
account = "$($envVars['SNOWFLAKE_ACCOUNT'])"
user = "$($envVars['SNOWFLAKE_USER'])"
password = "$($envVars['SNOWFLAKE_PASSWORD'])"
warehouse = "$warehouse"
database = "$database"
schema = "$schema"
role = "$role"
"@

# Write without BOM (critical for Snowflake CLI)
$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText($configFile, $configContent, $utf8NoBom)
Write-Success "Snowflake CLI configured at: $configFile"

# ===========================================================================
# STEP 3: Create Snowflake Infrastructure
# ===========================================================================
if (-not $SkipSnowflake) {
    Write-Host ""
    Write-Host "[3/4] Creating Snowflake infrastructure..." -ForegroundColor Yellow
    
    # Directly try to create database - this implicitly tests connection
    # and handles 'database does not exist' gracefully
    
    # Create database
    Write-Info "Creating database STAGING_DB..."
    # Using 2>&1 | Out-Null to suppress native errors from stderr output
    & snow sql -q "CREATE DATABASE IF NOT EXISTS STAGING_DB" 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { 
        Write-Success "Database created" 
    }
    else {
        Write-Err "Failed to create database. Check your credentials."
        exit 1
    }
    
    # Create warehouse
    Write-Info "Creating warehouse ETL_WAREHOUSE..."
    $whSql = "CREATE WAREHOUSE IF NOT EXISTS ETL_WAREHOUSE WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE"
    & snow sql -q $whSql 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { Write-Success "Warehouse created" }
    
    # Create stage
    Write-Info "Creating internal stage ETL_STAGE..."
    & snow sql -q "USE DATABASE STAGING_DB; CREATE STAGE IF NOT EXISTS ETL_STAGE" 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { Write-Success "Stage created" }
    
    # Status
    Write-Host ""
    Write-Info "Infrastructure status:"
    & snow connection test

}
else {
    Write-Host ""
    Write-Host "[3/4] Skipping Snowflake infrastructure" -ForegroundColor Yellow
}

# ===========================================================================
# STEP 4: Docker Services
# ===========================================================================
if (-not $SkipDocker) {
    Write-Host ""
    Write-Host "[4/4] Starting Docker services..." -ForegroundColor Yellow
    
    Push-Location $ProjectRoot
    try {
        Write-Info "Building and starting containers (this may take a few minutes on first run)..."
        Write-Info "Pulling images: MinIO, Prefect Server, Prefect Worker..."
        
        # Run docker-compose (suppress stderr which contains progress info)
        # Using cmd to avoid PowerShell treating stderr as error
        $env:DOCKER_CLI_HINTS = "false"
        $oldErrorActionPreference = $ErrorActionPreference
        $ErrorActionPreference = "SilentlyContinue"
        cmd /c "docker-compose up -d --build 2>nul"
        $ErrorActionPreference = $oldErrorActionPreference
        
        # Wait for containers to start
        Write-Info "Waiting for services to be healthy..."
        Start-Sleep -Seconds 5
        $containers = docker ps --format "{{.Names}}" 2>$null
        
        if ($containers -match "etl-minio" -and $containers -match "etl-prefect") {
            Write-Success "Docker services started successfully!"
            Write-Host ""
            Write-Host "  Services available at:" -ForegroundColor Cyan
            Write-Host "    - MinIO Console:  http://localhost:9001  (user: minioadmin / pass: minioadmin)" -ForegroundColor White
            Write-Host "    - Prefect UI:     http://localhost:4200" -ForegroundColor White
        }
        else {
            Write-Warn "Docker containers may still be starting..."
            Write-Host "  Check status with: docker ps" -ForegroundColor Gray
        }
    }
    finally {
        Pop-Location
    }
}
else {
    Write-Host ""
    Write-Host "[4/4] Skipping Docker" -ForegroundColor Yellow
}

# ===========================================================================
# SUMMARY
# ===========================================================================
Write-Host ""
Write-Host "==============================================================" -ForegroundColor Green
Write-Host "                    Setup Complete!                           " -ForegroundColor Green
Write-Host "==============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  NEXT STEP: Run the ETL pipeline:" -ForegroundColor Yellow
Write-Host ""
Write-Host "    powershell -ExecutionPolicy Bypass -File .\scripts\run_flow.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "  This will (inside Docker, no local Python required):" -ForegroundColor White
Write-Host "    1. Upload sample CSV to MinIO" -ForegroundColor Gray
Write-Host "    2. Detect schema (including VARIANT for JSON)" -ForegroundColor Gray
Write-Host "    3. Create PARENT_EVENTS table in Snowflake" -ForegroundColor Gray
Write-Host "    4. Load data via staging + MERGE" -ForegroundColor Gray
Write-Host "    5. Create secure views (GERMANY_EVENTS, RECENT_SIGNUPS)" -ForegroundColor Gray
Write-Host ""
Write-Host "  File Watcher is enabled by default:" -ForegroundColor Yellow
Write-Host "    - Polls MinIO every 60s for new CSV files" -ForegroundColor Gray
Write-Host "    - Automatically triggers ETL pipeline" -ForegroundColor Gray
Write-Host "    - View logs: docker-compose logs -f file-watcher" -ForegroundColor Gray
Write-Host ""
Write-Host "  After running, verify with:" -ForegroundColor Yellow
Write-Host "    snow sql -q `"SELECT * FROM STAGING_DB.PUBLIC.PARENT_EVENTS`"" -ForegroundColor Gray
Write-Host ""



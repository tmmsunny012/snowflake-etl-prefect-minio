# Snowflake ETL Pipeline with Prefect & MinIO

A production-ready ETL pipeline demonstrating Snowflake expertise, including dynamic schema detection, VARIANT/JSON handling, incremental MERGE updates, and secure views.

![Architecture](https://img.shields.io/badge/Architecture-Prefect%20%2B%20MinIO%20%2B%20Snowflake-blue)
![Python](https://img.shields.io/badge/Python-3.11+-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

## Quick Start

```powershell
# 1. Clone and setup
git clone <repository>
cd snowflake-etl-prefect-minio

# 2. Configure credentials
copy .env.example .env
# Edit .env with your Snowflake credentials

# 3. Run setup (creates DB, warehouse, stage + starts Docker)
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1

# 4. Run the ETL pipeline (inside Docker)
docker-compose exec prefect-worker python /app/flows/etl_flow.py
```

### Quick Test Commands
```powershell
# Check row count
snow sql -q "SELECT COUNT(*) FROM PARENT_EVENTS"

# View data with JSON extraction
snow sql -q "SELECT id, name, country, event_metadata:user_id::INT AS user_id FROM PARENT_EVENTS"

# Check secure views
snow sql -q "SELECT * FROM GERMANY_EVENTS"

# List files in Snowflake stage
snow sql -q "LIST @ETL_STAGE"

# Create a custom view on-the-fly
docker-compose exec prefect-worker python /app/scripts/create_view.py --name US_EVENTS --filter "country='US'"
```

## Prerequisites

- **Python 3.11+**
- **Docker Desktop** (for MinIO and Prefect)
- **Snowflake Account** ([Free trial](https://signup.snowflake.com/))
- **Snowflake CLI** (`pip install snowflake-cli-labs`)

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Local CSV     │────▶│     MinIO       │────▶│   Snowflake     │
│   (source)      │     │  (S3 storage)   │     │   (warehouse)   │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                              │                        │
                              ▼                        ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │    Prefect      │     │  Secure Views   │
                        │  (orchestration)│     │ (filtered data) │
                        └─────────────────┘     └─────────────────┘
```

### Data Flow

1. **Upload**: CSV uploaded to MinIO bucket
2. **Schema Detection**: Python analyzes columns (INT, FLOAT, DATE, VARIANT)
3. **Create Table**: Dynamic DDL generation with correct types
4. **Staging + MERGE**: Incremental upserts (no duplicates on re-run)
5. **Secure Views**: Filtered subsets with flattened JSON

## Configuration

### Snowflake Credentials

####  Environment Variables (Docker) - Please setup this file in the root directory of the project

Edit `.env` file:

```ini
SNOWFLAKE_ACCOUNT=abc12345.us-east-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=ETL_WAREHOUSE
SNOWFLAKE_DATABASE=STAGING_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

####  Snowflake CLI (Local) - Please setup to use cli command.

Create `~/.snowflake/config.toml`:

```toml
[connections.default]
account = "abc12345.us-east-1"
user = "your_username"
password = "your_password"
warehouse = "ETL_WAREHOUSE"
database = "STAGING_DB"
schema = "PUBLIC"
role = "ACCOUNTADMIN"
```

### MinIO Credentials

Default credentials (for local development):
- **Username**: `minioadmin`
- **Password**: `minioadmin`
- **Console**: http://localhost:9001

## Manual Setup Steps

### 1. Create Snowflake Infrastructure

```powershell
# Using Snowflake CLI
snow sql -f snowflake/setup/01_create_database.sql
snow sql -f snowflake/setup/02_create_warehouse.sql
snow sql -f snowflake/setup/03_create_stage.sql
```

### 2. Start Docker Services

```powershell
docker-compose up -d --build

# Verify services
docker-compose ps

# View logs
docker-compose logs -f file-watcher
```

### 3. Run the ETL Pipeline

```powershell
# Run via Docker
docker-compose exec prefect-worker python /app/flows/etl_flow.py
```

## Verify Results

### Query Parent Table

```powershell
snow sql -q "SELECT * FROM STAGING_DB.PUBLIC.PARENT_EVENTS LIMIT 5"
```

### Query VARIANT Column

```powershell
snow sql -q "SELECT id, name, event_metadata:user_id::INT as user_id FROM PARENT_EVENTS"
```

### Query Secure Views

```powershell
# German events only
snow sql -q "SELECT * FROM GERMANY_EVENTS"

# Signup events only
snow sql -q "SELECT * FROM RECENT_SIGNUPS"
```

## Incremental Updates (MERGE)

The pipeline uses `MERGE` for idempotent updates:

```sql
MERGE INTO PARENT_EVENTS AS target
USING staging AS source
ON target.ID = source.ID
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

**Test incremental behavior:**

```powershell
# Run with original data (inside Docker)
docker-compose exec prefect-worker python /app/flows/etl_flow.py

# Copy updated data (has 1 modified + 2 new rows)
copy test_data\sample_events_updated.csv data\sample_events.csv

# Re-run ETL (MERGE will update + insert, not duplicate)
docker-compose exec prefect-worker python /app/flows/etl_flow.py

# Verify: should have 10 rows (8 original + 2 new), 
# with id=1 updated (session_duration: 45 -> 90)
snow sql -q "SELECT COUNT(*) FROM PARENT_EVENTS"
snow sql -q "SELECT * FROM PARENT_EVENTS WHERE id = 1"
```

## Project Structure

```
snowflake-etl-prefect-minio/
├── docker-compose.yml        # Prefect + MinIO services
├── Dockerfile                # Python worker container
├── requirements.txt          # Python dependencies
├── .env.example              # Credentials template
├── snowflake/
│   ├── connections/
│   │   └── config.toml.example  # Snowflake CLI config template
│   ├── setup/
│   │   ├── 01_create_database.sql
│   │   ├── 02_create_warehouse.sql
│   │   └── 03_create_stage.sql
│   └── views/
│       ├── germany_events.sql
│       └── recent_signups.sql
├── data/
│   └── sample_events.csv         # Sample data (8 rows)
├── test_data/                     # Test files for watcher demo
│   ├── sample_events_updated.csv  # For testing MERGE
│   ├── test_batch_1.csv
│   ├── test_batch_2.csv
│   └── test_batch_3.csv
├── flows/
│   ├── etl_flow.py           # Main Prefect orchestration
│   ├── watcher_flow.py       # MinIO file watcher (auto-trigger)
│   └── tasks/
│       ├── minio_tasks.py    # Upload/download files
│       ├── schema_detector.py # Dynamic type detection
│       ├── snowflake_tasks.py # Create, load, merge
│       └── validation.py      # Data quality checks
└── scripts/
    ├── setup.ps1             # Windows setup
    ├── setup.sh              # Linux/Mac setup
    ├── run_flow.ps1          # Run the pipeline (Windows)
    ├── run_flow.sh           # Run the pipeline (Linux/Mac)
    ├── cleanup.ps1           # Teardown/cleanup (Windows)
    ├── cleanup.sh            # Teardown/cleanup (Linux/Mac)
    └── create_view.py        # Dynamic secure view generator CLI
```

## File Watcher (Auto-Trigger ETL)

The file watcher polls MinIO for new CSV files and automatically triggers the ETL pipeline.

### Start the Watcher
The file watcher starts automatically with Docker services.

```powershell
# All services including watcher start together
docker-compose up -d --build

# View watcher logs
docker-compose logs -f file-watcher
```

### How It Works
1. Watcher checks MinIO every 60 seconds for new `.csv` files
2. New files trigger the ETL pipeline automatically
3. Processed files are tracked in `metadata/processed_files.json`
4. Run logs are saved to `logs/run_*.json`

### Test the Watcher
Sample test files are included in `test_data/` folder. To see the watcher in action:

1. Open MinIO Console: http://localhost:9001 (login: `minioadmin` / `minioadmin`)
2. Navigate to `etl-bucket`
3. Drag and drop any CSV from `test_data/` folder into the bucket:
   - `test_batch_1.csv` (US/UK/CA events)
   - `test_batch_2.csv` (JP/AU events)
   - `test_batch_3.csv` (BR/MX events)
4. Watch the watcher logs to see auto-trigger:
   ```powershell
   docker-compose logs -f file-watcher
   ```
5. Check the new rows in Snowflake:
   ```powershell
   snow sql -q "SELECT COUNT(*) FROM PARENT_EVENTS"
   ```

### MinIO Storage Structure
```
etl-bucket/
├── sample_events.csv          # Data files
├── metadata/
│   └── processed_files.json   # Tracker (persistent)
└── logs/
    ├── run_2026-01-15_02-04-30.json
    └── ...                    # ETL run logs
```

### Run Once (Manual Check)
```powershell
docker-compose exec prefect-worker python /app/flows/watcher_flow.py --once
```

## Cleanup / Teardown

Remove all resources (Docker + Snowflake) for a fresh start:

```powershell
# Windows
powershell -ExecutionPolicy Bypass -File .\scripts\cleanup.ps1

# Linux/Mac
./scripts/cleanup.sh
```

### Options:
```powershell
# Skip confirmation prompt
.\scripts\cleanup.ps1 -Force

# Keep Snowflake, only remove Docker
.\scripts\cleanup.ps1 -SkipSnowflake

# Keep Docker, only remove Snowflake
.\scripts\cleanup.ps1 -SkipDocker
```

### What Gets Removed:
- Docker containers, volumes, and images
- Snowflake: STAGING_DB, ETL_WAREHOUSE, all tables and views

## Key Features

| Feature | Description |
|---------|-------------|
| **VARIANT Detection** | Automatically detects JSON columns and uses VARIANT type |
| **Dynamic DDL** | Generates CREATE TABLE from CSV with correct types |
| **Incremental MERGE** | Upserts without duplicates on re-runs |
| **Secure Views** |  filtered views with flattened JSON |
| **QUERY_TAG** | Cost tracking with session tags |
| **Validation** | Row counts, sample data, VARIANT verification |

### Cost Tracking with QUERY_TAG
All ETL queries are tagged with `prefect_etl_pipeline` for cost tracking and auditing.

```powershell
# View recent queries from the ETL pipeline
snow sql -q "SELECT QUERY_ID, QUERY_TEXT, START_TIME, TOTAL_ELAPSED_TIME/1000 AS SECONDS FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) WHERE QUERY_TAG = 'prefect_etl_pipeline' ORDER BY START_TIME DESC LIMIT 10"

# Cost analysis (requires ACCOUNTADMIN role)
snow sql -q "SELECT QUERY_TAG, COUNT(*) AS QUERY_COUNT, ROUND(SUM(TOTAL_ELAPSED_TIME)/1000/60, 2) AS TOTAL_MINUTES FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE QUERY_TAG = 'prefect_etl_pipeline' AND START_TIME > DATEADD('day', -7, CURRENT_TIMESTAMP()) GROUP BY QUERY_TAG"
```

## Demo Commands

### 1. Setup and Run (Start Fresh)
```powershell
# Run the full setup (creates Snowflake infra + starts Docker)
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1

# Run the ETL pipeline (inside Docker)
powershell -ExecutionPolicy Bypass -File .\scripts\run_flow.ps1
```

### 2. Verify Data in Snowflake
```powershell
# Check the parent table (8 rows with VARIANT column)
snow sql -q "SELECT * FROM STAGING_DB.PUBLIC.PARENT_EVENTS"

# Query JSON fields from VARIANT column  
snow sql -q "SELECT id, name, event_metadata:user_id::INT AS user_id, event_metadata:amount::FLOAT AS amount FROM PARENT_EVENTS"

# Check pre-built secure views
snow sql -q "SELECT * FROM GERMANY_EVENTS"
snow sql -q "SELECT * FROM RECENT_SIGNUPS"
```

### 3. Dynamic View Creation (Interactive Demo)
```powershell
# Show all available examples
docker-compose exec prefect-worker python /app/scripts/create_view.py --examples

# Create a US events view  
docker-compose exec prefect-worker python /app/scripts/create_view.py --name US_EVENTS --filter "country='US'"

# Create a high-value purchases view (JSON filter)
docker-compose exec prefect-worker python /app/scripts/create_view.py --name HIGH_VALUE --filter "event_metadata:amount > 100"

# Create your own custom filter
docker-compose exec prefect-worker python /app/scripts/create_view.py --name CUSTOM_VIEW --filter "event_type='purchase'"

# List all views
docker-compose exec prefect-worker python /app/scripts/create_view.py --list
```

### 4. Demo Incremental MERGE
```powershell
# Copy the updated CSV (has 1 modified + 2 new rows)
copy test_data\sample_events_updated.csv data\sample_events.csv

# Re-run ETL (MERGE will update + insert, not duplicate)
powershell -ExecutionPolicy Bypass -File .\scripts\run_flow.ps1

# Verify: Should now have 10 rows (8 + 2 new)
snow sql -q "SELECT COUNT(*) FROM PARENT_EVENTS"

# Show the updated row (id=1, session_duration changed from 45 to 90)
snow sql -q "SELECT * FROM PARENT_EVENTS WHERE id=1"
```

### 5. Docker Status Check
```powershell
# Show running containers
docker ps

# View Prefect flow logs
docker-compose logs prefect-worker --tail 50
```

### 6. Access UIs
- **Prefect Dashboard**: http://localhost:4200
- **MinIO Console**: http://localhost:9001 (user: minioadmin / pass: minioadmin)

## Troubleshooting

### "Connection refused" to MinIO

```powershell
# Ensure Docker services are running
docker-compose ps
docker-compose up -d minio
```

### "Authentication failed" to Snowflake

```powershell
# Test CLI connection
snow connection test

# Check credentials
cat ~/.snowflake/config.toml
```

### Prefect worker not executing flows

```powershell
# Check worker logs
docker-compose logs prefect-worker

# Restart worker
docker-compose restart prefect-worker
```

## Sample Data

The sample CSV follows this structure:

```csv
id,name,country,event_type,event_date,event_metadata
1,John,DE,signup,2025-01-01,"{""user_id"": 123, ""session_duration"": 45}"
2,Maria,FR,purchase,2025-01-02,"{""user_id"": 456, ""amount"": 120.5}"
```

### Filtered Views

**GERMANY_EVENTS** (country = 'DE'):
- Flattens: `user_id`, `session_duration`, `amount`

**RECENT_SIGNUPS** (event_type = 'signup'):
- Flattens: `user_id`, `session_duration`

### 🔧 Dynamic View Generator CLI

Create custom secure views on-the-fly with any filter:

```powershell
# Create a view for US events
python scripts/create_view.py --name US_EVENTS --filter "country='US'"

# Create a view for high-value transactions
python scripts/create_view.py --name HIGH_VALUE --filter "event_metadata:amount > 100"

# Create a view with date filter
python scripts/create_view.py --name RECENT --filter "event_date >= '2025-01-05'"

# Combined filters
python scripts/create_view.py --name DE_SIGNUPS --filter "country='DE' AND event_type='signup'"

# List all views
python scripts/create_view.py --list

# Drop a view
python scripts/create_view.py --drop US_EVENTS

# Show examples
python scripts/create_view.py --examples
```

## License

MIT License - feel free to use this as a template for your projects.

---

*Built with ❄️ Snowflake, 🐍 Python, 🔄 Prefect, and 📦 MinIO*

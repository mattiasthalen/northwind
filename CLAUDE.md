# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data pipeline project that extracts Northwind database data and loads it into Microsoft Fabric using dlt (data load tool) and SQLMesh for data transformations. The project consists of two main components:

1. **dlt pipeline**: Extracts data from Northwind API and loads it into Microsoft Fabric OneLake as Delta tables
2. **SQLMesh models**: Transforms raw data through various dimensional modeling patterns

## Key Commands

### Running the dlt Pipeline
```bash
# Run in development mode (creates branch-specific dataset)
cd dlt
python northwind.py dev

# Run in production mode
cd dlt
python northwind.py prod
```

### Running SQLMesh
```bash
cd sqlmesh

# Plan and apply changes interactively
sqlmesh plan

# Plan and apply changes automatically (for production)
sqlmesh plan prod --run --auto-apply --no-prompts

# Run specific environment
sqlmesh plan dev__main --run --auto-apply --no-prompts
```

### Environment Setup
```bash
# Install dependencies using uv (recommended)
uv pip install -r requirements.txt

# Or install with pip
pip install -r requirements.txt
```

## Architecture

### Directory Structure
- `dlt/`: Data extraction pipeline
  - `northwind.py`: Main pipeline script that loads data to OneLake
  - `northwind.json`: REST API configuration for source endpoints
  - `schemas/`: Import/export schema definitions
  
- `sqlmesh/`: Data transformation layer
  - `config.py`: SQLMesh configuration with Fabric connections
  - `models/`: SQL and Python transformation models
    - `das__*`: Data Access Store (raw layer)
    - `dar__*`: Data Access Refined (transformed layer)
    - `dab__*`: Data Access Bridge (bridge tables)
  - `macros/`: Custom SQLMesh macros
  
- `notebooks/`: Fabric notebook for running pipelines
  - `runner.ipynb`: Orchestration notebook for Fabric environment

### Data Flow
1. dlt extracts data from Northwind REST API
2. Data is loaded to OneLake as Delta tables in branch-specific datasets
3. SQLMesh models transform data through layers:
   - Raw layer (`das__raw__*`): Direct copies of source data
   - SCD layer (`das__scd__*`): Slowly changing dimensions
   - Bridge layer (`dar__bridge__*`): Bridge tables with temporal support
   - Peripheral layer (`dar__peripheral__*`): Supporting dimension tables

### Environment Variables Required
All credentials are loaded from Azure KeyVault and Fabric Variable Library in production. For local development:

```bash
# Azure Service Principal
CREDENTIALS__AZURE_TENANT_ID
CREDENTIALS__AZURE_CLIENT_ID  
CREDENTIALS__AZURE_CLIENT_SECRET

# Fabric Configuration
FABRIC__WORKSPACE_ID
FABRIC__WAREHOUSE_ENDPOINT
FABRIC__STATE_ENDPOINT
FABRIC__STATE_DATABASE
DESTINATION__BUCKET_URL
```

### SQLMesh Model Patterns

The project uses several modeling patterns:

1. **Blueprint Models** (`*__blueprint`): Template models for generating entity-specific transformations
2. **Hook-based Joins**: Uses `frames.yml` and `keysets.yml` to define relationships between entities
3. **Temporal Models**: Support for as-of date queries with `_record__is_current` flags
4. **Branch-based Environments**: Automatically creates environment per git branch

### Important Conventions

- Branch names are automatically incorporated into dataset/environment names (e.g., `dev__feature_branch`)
- All models use case-sensitive DuckDB dialect for parsing
- Models are scheduled to run every 5 minutes (cron: `*/5 * * * *`)
- Default start date for models is 2025-08-20
- Fabric connections use Service Principal authentication with ODBC Driver 18

### Working with Models

When creating new SQLMesh models:
1. Follow naming convention: `{layer}__{type}__{entity}` (e.g., `dar__bridge__customer`)
2. Define hooks in `frames.yml` for entity relationships
3. Use macros from `macros/` for common operations
4. Models should specify appropriate `kind` (VIEW, TABLE, INCREMENTAL, etc.)

### Debugging

- SQLMesh logs are stored in `sqlmesh/logs/` and `logs/`
- Use `sqlmesh plan --no-auto-categorization` to manually review changes
- Check connection with `sqlmesh test connection`
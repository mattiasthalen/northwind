# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data pipeline project implementing the **ADSS-HOOK-USS** architecture pattern, extracting Northwind database data and loading it into Microsoft Fabric using dlt (data load tool) and SQLMesh for data transformations.

### Architecture Patterns

The project follows three core architectural patterns:

1. **ADSS (Analytical Data Storage System)**: Three-layer architecture (DAS, DAB, DAR) addressing dependencies, change, and complexity
2. **HOOK Methodology**: Standardized approach for business entity integration with immutable identifiers and temporal tracking
3. **USS (Unified Star Schema)**: Consolidated analytical layer for BI consumption

### Components

1. **dlt pipeline**: Extracts data from Northwind API and loads it into Microsoft Fabric OneLake as Delta tables
2. **SQLMesh models**: Transforms raw data through ADSS layers using HOOK methodology

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
    - `das__*`: Data According to System (raw layer)
    - `dab__*`: Data According to Business (HOOK integration layer)
    - `dar__*`: Data According to Requirements (analytical layer)
  - `models.yml`: HOOK entity configuration
  - `keysets.yml`: HOOK keyset definitions
  - `macros/`: Custom SQLMesh macros including USS helpers
  
- `notebooks/`: Fabric notebook for running pipelines
  - `runner.ipynb`: Orchestration notebook for Fabric environment

### Data Flow

```
Source → DAS (Raw/SCD) → DAB (HOOK Integration) → DAR (USS Analytics) → BI Tools
```

1. **DAS Layer**: dlt extracts data from Northwind REST API into OneLake
   - `das.raw.*`: Direct copies of source data
   - `das.scd.*`: Historized version of raw

2. **DAB Layer**: HOOK integration for business entities
   - `dab.hook.*`: HOOK-enabled business entities
   - Primary hooks with PIT (point-in-time) markers
   - Composite hooks for relationships

3. **DAR Layer**: Analytical consumption through USS
   - `dar.uss._bridge*`: Complex relationship models using HOOK joins
   - `dar.uss.*`: Supporting dimension tables

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

The project implements ADSS-HOOK-USS patterns:

1. **ADSS Layers**:
   - **DAS**: Data According to System - raw data capture
   - **DAB**: Data According to Business - HOOK integration
   - **DAR**: Data According to Requirements - USS analytics

2. **HOOK Methodology**:
   - **Primary Hooks**: Unique entity identifiers (e.g., `_hook__customer__id`)
   - **Composite Hooks**: Multi-entity relationships (e.g., `_hook__order__product`)
   - **PIT Hooks**: Point-in-time identifiers (e.g., `_pithook__customer__id`)
   - **Keyset Format**: `namespace.concept.qualifier|value`
   - **Temporal Format**: `hook~epoch__valid_from|timestamp`

3. **USS (Unified Star Schema)**:
   - Consolidated analytical models
   - Leverages HOOK identifiers for consistent joins
   - Supports As-Is (current) and As-Of (historical) queries

4. **Blueprint Models** (`*__blueprint`): Template models for generating entity-specific transformations

5. **Temporal Models**: Support for as-of date queries with `_record__is_current` flags

6. **Branch-based Environments**: Automatically creates environment per git branch

### Important Conventions

- Branch names are automatically incorporated into dataset/environment names (e.g., `dev__feature_branch`)
- All models use case-sensitive DuckDB dialect for parsing
- Models are scheduled to run every 5 minutes (cron: `*/5 * * * *`)
- Default start date for models is 2025-08-20
- Fabric connections use Service Principal authentication with ODBC Driver 18

### Working with Models

When creating new SQLMesh models:

1. **Follow ADSS naming convention**:
   - DAS layer: `das__raw__[entity]` or `das__scd__[entity]`
   - DAB layer: `dab__hook__frame__[entity]`
   - DAR layer: `dar__bridge__[entity]`, `dar__peripheral__[entity]`, `dar__star__[schema]`

2. **Configure HOOK entities in `models.yml`**:
   ```yaml
   - name: entity_name
     hooks:
     - name: _hook__concept__qualifier
       primary: true/false
       keyset: namespace.concept.qualifier
       expression: source_column
   ```

3. **Use HOOK identifiers for joins**:
   - Join through HOOK columns (e.g., `_hook__customer__id`)
   - Use PIT hooks for temporal queries
   - Leverage composite hooks for multi-entity relationships

4. **Apply USS patterns**:
   - Use `star__list` macro for column selection
   - Create unified views across multiple star schemas
   - Optimize for analytical query patterns

5. **Specify appropriate model kinds** (VIEW, TABLE, INCREMENTAL, etc.)

### Debugging

- SQLMesh logs are stored in `sqlmesh/logs/` and `logs/`
- Use `sqlmesh plan --no-auto-categorization` to manually review changes
- Check connection with `sqlmesh test connection`
- Validate HOOK format in DAB layer outputs
- Verify USS joins are using correct HOOK identifiers

## HOOK Examples

### Primary Hook Example
```
_hook__customer__id = "northwind.customer.id|ALFKI"
```

### Composite Hook Example
```
_hook__order__product = "northwind.order.id|10248~northwind.product.id|11"
```

### PIT Hook Example
```
_pithook__customer__id = "northwind.customer.id|ALFKI~epoch__valid_from|2025-08-20T10:00:00"
```

### Temporal Query Examples
```sql
-- Current state query (As-Is)
SELECT * FROM dar.bridge.as_is
WHERE _record__is_current = TRUE

-- Historical query (As-Of)
SELECT * FROM dab.hook.frame__orders
WHERE _pithook__order__id LIKE '%~epoch__valid_from|2025-01-01%'

-- Join using HOOKs
SELECT *
FROM dab.hook.frame__orders o
JOIN dab.hook.frame__customers c 
  ON o._hook__customer__id = c._hook__customer__id
WHERE o._record__is_current = TRUE
```

## Architecture Benefits

### ADSS Benefits
- **Macro Agility**: Source system changes only affect DAS→DAB transformations
- **Micro Agility**: New attributes can be added without disrupting existing code
- **Maintainability**: Clear separation of concerns across layers
- **Longevity**: Designed for 10-20 year system lifecycle

### HOOK Benefits
- **Immutable Identifiers**: HOOKs never change, ensuring referential integrity
- **Temporal Consistency**: All relationships tracked through time
- **Flexible Joins**: Any entity can join with any other through HOOKs
- **Complete Audit Trail**: Full history of all entity relationships

### USS Benefits
- **Single Source of Truth**: One unified model for all analytics
- **Reduced Complexity**: Fewer models to maintain
- **Optimized Performance**: Designed for analytical query patterns
- **Tool Flexibility**: Supports various BI tool requirements
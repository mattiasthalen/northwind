import getpass
import os
import subprocess

from sqlmesh.core.config import (
    Config,
    ModelDefaultsConfig,
    GatewayConfig,
    DuckDBConnectionConfig,
    FabricConnectionConfig,
    MSSQLConnectionConfig,
    NameInferenceConfig,
    CategorizerConfig,
    PlanConfig,
    AutoCategorizationMode
)

from sqlmesh.core.config.connection import DuckDBAttachOptions

def get_current_branch():
    try:
        branch_name = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode('utf-8')
        return branch_name
        
    except Exception as e:
        print(f"Error getting current branch: {e}")
        return None

branch = get_current_branch()
default_environment = f"dev__{branch}".replace('-', '_') if branch else "dev"

print(f"Environment is set to: {default_environment}.")

config = Config(
    project="northwind",
    default_target_environment=default_environment,
    gateways={
        "local": GatewayConfig(
            connection=DuckDBConnectionConfig(
                catalogs={
                    "ducklake": DuckDBAttachOptions(
                        type="ducklake",
                        path="data/catalog.ducklake",
                        data_path="data",
                        encrypted=True,
                        data_inlining_row_limit=10,
                    ),
                }
            )
        ),
        "fabric": GatewayConfig(
            connection=FabricConnectionConfig(
                concurrent_tasks=1,
                host=os.getenv("FABRIC__WAREHOUSE_ENDPOINT", ""),
                user=os.getenv("CREDENTIALS__AZURE_CLIENT_ID", ""),
                password=os.getenv("CREDENTIALS__AZURE_CLIENT_SECRET", ""),
                database="das",
                timeout=120,
                login_timeout=120,
                driver="pyodbc",
                driver_name="ODBC Driver 18 for SQL Server",
                tenant_id=os.getenv("CREDENTIALS__AZURE_TENANT_ID", ""),
                workspace_id=os.getenv("FABRIC__WORKSPACE_ID", ""),
                odbc_properties={
                    "Authentication": "ActiveDirectoryServicePrincipal",
                    "RetryExec": "{40613:3,5}" # Retry connection
                }
            ),
            state_connection=MSSQLConnectionConfig(
                host=os.getenv("FABRIC__STATE_ENDPOINT", ""),
                user= os.getenv("CREDENTIALS__AZURE_CLIENT_ID", ""),
                password=os.getenv("CREDENTIALS__AZURE_CLIENT_SECRET", ""),
                database=os.getenv("FABRIC__STATE_DATABASE", ""),
                timeout=120,
                login_timeout=120,
                driver="pyodbc",
                driver_name="ODBC Driver 18 for SQL Server",
                odbc_properties={
                    "Authentication": "ActiveDirectoryServicePrincipal",
                    "RetryExec": "{40613:3,5}" # Retry connection
                }
                
            )
        )
    },
    default_gateway="local",
    model_defaults=ModelDefaultsConfig(
        dialect="duckdb,normalization_strategy=case_sensitive",
        start="2025-08-20",
        cron="*/5 * * * *"
    ),
    model_naming=NameInferenceConfig(
        infer_names=True
    ),
    plan=PlanConfig(
        auto_categorize_changes=CategorizerConfig(
            external=AutoCategorizationMode.FULL,
            python=AutoCategorizationMode.FULL,
            sql=AutoCategorizationMode.FULL,
            seed=AutoCategorizationMode.FULL
        )
    ),
    variables = {
        "project_path": os.path.abspath(".").lstrip('/'),
        "min_date": "1970-01-01",
        "max_date": "9999-12-31",
        "min_ts": "1970-01-01 00:00:00",
        "max_ts": "9999-12-31 23:59:59"
    }
)
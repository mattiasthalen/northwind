import getpass
import os
import subprocess

from sqlmesh.core.config import (
    Config,
    ModelDefaultsConfig,
    GatewayConfig,
    FabricConnectionConfig,
    MSSQLConnectionConfig,
    NameInferenceConfig,
    CategorizerConfig,
    PlanConfig,
    AutoCategorizationMode
)

azure__tenant_id = os.getenv("CREDENTIALS__AZURE_TENANT_ID")
azure__client_id = os.getenv("CREDENTIALS__AZURE_CLIENT_ID")
azure__client_secret = os.getenv("CREDENTIALS__AZURE_CLIENT_SECRET")
fabric__workspace_id = os.getenv("FABRIC__WORKSPACE_ID")
fabric__warehouse_endpoint = os.getenv("FABRIC__WAREHOUSE_ENDPOINT")
fabric__state_endpoint = os.getenv("FABRIC__STATE_ENDPOINT")
fabric__state_database = os.getenv("FABRIC__STATE_DATABASE")

assert azure__tenant_id, "Azure tenant ID is not set in environment variables."
assert azure__client_id, "Azure client ID is not set in environment variables."
assert azure__client_secret, "Azure client secret is not set in environment variables."
assert fabric__workspace_id, "Fabric workspace ID is not set in environment variables."
assert fabric__warehouse_endpoint, "Fabric warehouse endpoint is not set in environment variables."
assert fabric__state_endpoint, "Fabric state endpoint is not set in environment variables."
assert fabric__state_database, "Fabric state database is not set in environment variables."

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
    project="northwind-to-fabric",
    default_target_environment=default_environment,
    gateways={
        "fabric": GatewayConfig(
            connection=FabricConnectionConfig(
                concurrent_tasks=1,
                host=fabric__warehouse_endpoint,
                user=azure__client_id,
                password=azure__client_secret,
                database="das",
                timeout=120,
                login_timeout=120,
                driver="pyodbc",
                driver_name="ODBC Driver 18 for SQL Server",
                tenant_id=azure__tenant_id,
                workspace_id=fabric__workspace_id,
                odbc_properties={
                    "Authentication": "ActiveDirectoryServicePrincipal"
                }
            ),
            state_connection=MSSQLConnectionConfig(
                host=fabric__state_endpoint,
                user=azure__client_id,
                password=azure__client_secret,
                database=fabric__state_database,
                timeout=120,
                login_timeout=120,
                driver="pyodbc",
                driver_name="ODBC Driver 18 for SQL Server",
                odbc_properties={
                    "Authentication": "ActiveDirectoryServicePrincipal"
                }
                
            )
        )
    },
    default_gateway="fabric",
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
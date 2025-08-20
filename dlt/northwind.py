import dlt
import json
import os
import subprocess
import sys
import typing as t

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.rest_api import rest_api_resources

@dlt.source(name="northwind")
def northwind_source() -> t.Any:
    source_config: RESTAPIConfig = json.load(open("./dlt/northwind.json", "r"))
    yield from rest_api_resources(source_config)

def load_northwind(env) -> None:
    dev_mode = env != "prod"
    print(f"Running in {'dev' if dev_mode else 'prod'} mode")

    dataset_name = "northwind"

    if dev_mode:
        branch_name = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip().decode('utf-8')
        dataset_name = f"{dataset_name}_{branch_name.replace('-', '_')}"

    schema_path = "./dlt/schemas"
    export_schema_path = os.path.join(schema_path, "export")
    import_schema_path = os.path.join(schema_path, "import")

    # Set env vars for connecting to One Lake, no secret :)
    os.environ["CREDENTIALS__AZURE_STORAGE_ACCOUNT_NAME"] = "onelake"
    os.environ["CREDENTIALS__AZURE_ACCOUNT_HOST"] = "onelake.blob.fabric.microsoft.com"

    pipeline = dlt.pipeline(
        pipeline_name="northwind",
        destination="filesystem",
        dataset_name=dataset_name,
        progress="enlighten",
        export_schema_path=export_schema_path,
        import_schema_path=import_schema_path,
        dev_mode=dev_mode
    )

    source = northwind_source()
    
    load_info = pipeline.run(source, table_format="delta")
    print(load_info)

if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "dev"
    load_northwind(env=env)
import dlt
import json
import sys
import typing as t

from loader import load_data_pipeline

from dlt.sources.rest_api.typing import RESTAPIConfig
from dlt.sources.rest_api import rest_api_resources
@dlt.source(name="northwind")
def northwind_source() -> t.Any:
    source_config: RESTAPIConfig = {
        "client": {
            "base_url": "https://demodata.grapecity.com/northwind/odata/v1/"
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "max_table_nesting": 0,
            "endpoint": {
                "data_selector": "value",
                "paginator": {
                    "type": "json_link",
                    "next_url_path": "['@odata.nextLink']",
                },
                "params": {
                    "$count": "false",
                },
            },
        },
        "resources": [
            {
                "name": "northwind__categories",
                "table_name": "raw__northwind__categories",
                "primary_key": "CategoryId",
                "endpoint": {
                    "path": "Categories",
                }
            },

            {
                "name": "northwind__customers",
                "table_name": "raw__northwind__customers",
                "primary_key": "CustomerId",
                "endpoint": {
                    "path": "Customers",
                }
            },

            {
                "name": "northwind__employees",
                "table_name": "raw__northwind__employees",
                "primary_key": "EmployeeId",
                "endpoint": {
                    "path": "Employees",
                }
            },

            {
                "name": "northwind__employee_territories",
                "table_name": "raw__northwind__employee_territories",
                "primary_key": ["EmployeeId", "TerritoryId"],
                "endpoint": {
                    "path": "Employees({id})/EmployeeTerritories",
                    "params": {
                        "id": {
                            "type": "resolve",
                            "resource": "northwind__employees",
                            "field": "EmployeeId"
                        }
                    },
                }
            },

            {
                "name": "northwind__order_details",
                "table_name": "raw__northwind__order_details",
                "primary_key": ["OrderId", "ProductId"],
                "endpoint": {
                    "path": "OrderDetails",
                }
            },

            {
                "name": "northwind__orders",
                "table_name": "raw__northwind__orders",
                "primary_key": "OrderId",
                "endpoint": {
                    "path": "Orders",
                }
            },

            {
                "name": "northwind__products",
                "table_name": "raw__northwind__products",
                "primary_key": "ProductId",
                "endpoint": {
                    "path": "Products",
                }
            },

            {
                "name": "northwind__regions",
                "table_name": "raw__northwind__regions",
                "primary_key": "RegionId",
                "endpoint": {
                    "path": "Regions",
                }
            },

            {
                "name": "northwind__shippers",
                "table_name": "raw__northwind__shippers",
                "primary_key": "ShipperId",
                "endpoint": {
                    "path": "Shippers",
                }
            },

            {
                "name": "northwind__suppliers",
                "table_name": "raw__northwind__suppliers",
                "primary_key": "SupplierId",
                "endpoint": {
                    "path": "Suppliers",
                }
            },

            {
                "name": "northwind__territories",
                "table_name": "raw__northwind__territories",
                "primary_key": "TerritoryId",
                "endpoint": {
                    "path": "Territories",
                }
            },

        ]
    }

    yield from rest_api_resources(source_config)

if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "dev"

    load_data_pipeline(northwind_source, env=env)
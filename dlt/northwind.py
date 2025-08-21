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
            "base_url": "https://demodata.grapecity.com/northwind/api/v1/"
        },
        "resource_defaults": {
            "write_disposition": "replace",
            "max_table_nesting": 0
        },
        "resources": [
            {
                "name": "get_northwindapiv_1_categories",
                "table_name": "raw__northwind__categories",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Categories",
                    "paginator": "single_page"
                }
            },

            {
                "name": "get_northwindapiv_1_categoriesid_details",
                "table_name": "raw__northwind__category_details",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Categories/{id}/Details",
                    "params": {
                        "id": {
                            "type": "resolve",
                            "resource": "get_northwindapiv_1_categories",
                            "field": "categoryId"
                        }
                    },
                    "paginator": "single_page"
                }
            },

            {
                "name": "get_northwindapiv_1_customers",
                "table_name": "raw__northwind__customers",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Customers",
                    "paginator": "single_page"
                }
            },

            {
                "name": "get_northwindapiv_1_employees",
                "table_name": "raw__northwind__employees",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Employees",
                    "paginator": "single_page"
                }
            },

            {
                "name": "get_northwindapiv_1_employee_territories",
                "table_name": "raw__northwind__employee_territories",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Employees/{id}/Territories",
                    "params": {
                        "id": {
                            "type": "resolve",
                            "resource": "get_northwindapiv_1_employees",
                            "field": "employeeId"
                        }
                    },
                    "paginator": "single_page"
                },
                "include_from_parent": ["employeeId"]
            },

            {
                "name": "get_northwindapiv_1_ordersid_order_details",
                "table_name": "raw__northwind__order_details",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Orders/{id}/OrderDetails",
                    "params": {
                        "id": {
                            "type": "resolve",
                            "resource": "get_northwindapiv_1_orders",
                            "field": "orderId"
                        }
                    },
                    "paginator": "single_page"
                }
            },
            {
                "name": "get_northwindapiv_1_orders",
                "table_name": "raw__northwind__orders",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Orders",
                    "paginator": "single_page"
                }
            },

            {
                "name": "get_northwindapiv_1_products",
                "table_name": "raw__northwind__products",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Products",
                    "paginator": "single_page"
                }
            },

            {
                "name": "get_northwindapiv_1_regions",
                "table_name": "raw__northwind__regions",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Regions",
                    "paginator": "single_page"
                }
            },

            {
                "name": "get_northwindapiv_1_shippers",
                "table_name": "raw__northwind__shippers",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Shippers",
                    "paginator": "single_page"
                }
            },

            {
                "name": "get_northwindapiv_1_suppliers",
                "table_name": "raw__northwind__suppliers",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Suppliers",
                    "paginator": "single_page"
                }
            },

            {
                "name": "get_northwindapiv_1_territories",
                "table_name": "raw__northwind__territories",
                "endpoint": {
                    "data_selector": "$",
                    "path": "Territories",
                    "paginator": "single_page"
                }
            }
        ]
    }

    yield from rest_api_resources(source_config)

if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "dev"

    load_data_pipeline(northwind_source, env=env)
import os
import yaml
import typing as t

from sqlglot import exp
from sqlmesh.core.model import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName
from sqlmesh.core import macros

# --- Functional helpers for blueprint generation ---
def _normalize_schemas(schemas: str | list[str]) -> list[str]:
    return [schemas] if isinstance(schemas, str) else schemas

def _schema_path(script_dir: str, schema: str) -> str:
    rel_path = f"../../dlt/schemas/export/{schema}.schema.yaml"
    return os.path.normpath(os.path.join(script_dir, rel_path))

def _load_schema_dict(schema_path: str) -> dict:
    with open(schema_path, 'r') as f:
        return yaml.safe_load(f)

def _extract_blueprints(
        schema: str,
        schema_dict: dict
) -> list[dict[str, t.Any]]:

    return [
        {
            "name": key,
            "schema": schema,
            "columns": value["columns"]
        }
        for key, value in schema_dict["tables"].items()
        if not key.startswith("_dlt")
    ]

def generate_blueprints(schemas) -> list[dict[str, t.Any]]:
    """Generates a blueprint for the DAS raw models based on the dlt schema(s)."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    blueprints = []
    for schema in _normalize_schemas(schemas):
        path = _schema_path(script_dir, schema)
        schema_dict = _load_schema_dict(path)
        blueprints.extend(_extract_blueprints(schema, schema_dict))
    return blueprints

# --- Functional helpers for SQL construction ---
def build_source_columns(columns: dict | None) -> list[exp.Expression]:
    if not columns:
        return []
    return [
        exp.cast(
            exp.column(key),
            # SQLGlot translates JSON to varchar in Fabric, need to set it as varchar(max)
            exp.DataType.build("varchar(max)") if value["data_type"].lower() == "json" 
            else exp.DataType.build(value["data_type"])
        ).as_(key)
        for key, value in columns.items()
    ]

def get_columns_to_hash(source_columns: list[exp.Expression]) -> list[str]:
    return [col.alias for col in source_columns if not col.alias.startswith("_dlt")]

def hash_columns(*fields: exp.Expression) -> exp.Func:
    string_fields: t.List[exp.Expression] = []
    for i, field in enumerate(fields):
        if i > 0:
            string_fields.append(exp.Literal.string("|"))
        string_fields.append(
            exp.func(
                "COALESCE",
                exp.cast(field, exp.DataType.build("text")),
                exp.Literal.string("_sqlmesh_surrogate_key_null_"),
            )
        )
    func = exp.func(
        "SHA256",
        exp.func("CONCAT", *string_fields)
    )
    return func

def to_timestamp(column: exp.Expression) -> exp.Expression:
    seconds_since_epoch = exp.cast(
        column,
        exp.DataType.build("float")
    )
    microseconds = exp.Cast(
        this=exp.Round(
            this=exp.Mul(this=seconds_since_epoch, expression=exp.Literal.number("1e6")),
            decimals=exp.Literal.number("0"),
        ),
        to=exp.DataType(this=exp.DataType.Type.BIGINT),
    )
    epoch_start = exp.Cast(
        this=exp.Literal.string("1970-01-01"),
        to=exp.DataType(this=exp.DataType.Type.TIMESTAMP)
    )
    date_add = exp.func(
        "DATEADD",
        exp.Identifier(this="MICROSECOND"),
        microseconds,
        epoch_start,
    )
    cast = exp.Cast(
        this=date_add,
        to=exp.DataType(this=exp.DataType.Type.TIMESTAMP)
    )
    return cast

def build_sql_select(
        name: str,
        source_table: str,
        source_columns: list[exp.Expression],
        columns_to_hash: list[str]
) -> exp.Expression:

    hashed_columns = hash_columns(*[exp.column(alias) for alias in columns_to_hash]).as_("_record__hash")
    loaded_at = to_timestamp(exp.column("_dlt_load_id")).as_("_record__loaded_at")

    cte__source = (
        exp.select(
            *source_columns,
            hashed_columns,
            loaded_at
        )
        .from_(source_table)
    )

    cte__deduplicated = (
        exp.select(
            exp.Star(),
            exp.Window(
                this=exp.RowNumber(),
                partition_by=[exp.column("_record__hash")],
                order=exp.Order(expressions=[exp.column("_record__loaded_at")])
            ).as_("_record__hash__version")
        )
        .from_("cte__source")
    )

    sql = (
        exp.select(
            *source_columns,
            exp.cast(exp.column("_record__hash"), "varbinary(max)").as_("_record__hash"),
            exp.cast(exp.column("_record__loaded_at"), "timestamp").as_("_record__loaded_at")
        )
        .from_("cte__deduplicated")
        .with_("cte__source", as_=cte__source)
        .with_("cte__deduplicated", as_=cte__deduplicated)
        .where(
            exp.EQ(
                this=exp.column("_record__hash__version"),
                expression=exp.Literal.number("1")
            )
        )
        .join(
            exp.Join(
                this=f"das.raw.{name}",
                on=exp.EQ(
                    this=exp.column("_record__hash", table=f"{name}"),
                    expression=exp.column("_record__hash", table="cte__deduplicated")
                ),
                kind="ANTI"
            )
        )
    )

    return sql

# --- Model Entrypoint ---
@model(
    "das.raw.@{name}",
    is_sql=True,
    kind=dict(
        name=ModelKindName.INCREMENTAL_UNMANAGED,
        disable_restatement=True
    ),
    blueprints=generate_blueprints("northwind"),
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:

    name = evaluator.blueprint_var("name")

    if not isinstance(name, str) or not name:
        raise ValueError("Blueprint variable 'name' must be a non-empty string")

    schema = evaluator.blueprint_var("schema")
    columns = evaluator.blueprint_var("columns")
    source_table = f"landing_zone.{schema}.{name}"
    source_columns = build_source_columns(columns)
    columns_to_hash = get_columns_to_hash(source_columns)

    sql = build_sql_select(name, source_table, source_columns, columns_to_hash)

    return sql
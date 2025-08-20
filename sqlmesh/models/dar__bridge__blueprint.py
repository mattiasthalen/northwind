import os
import yaml
from typing import Any, Dict, List, Optional

from sqlglot import exp
from sqlmesh.core.model import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName

# --- File and Frame Utilities ---
def get_frames_path() -> str:
    return os.path.join(os.path.dirname(__file__), "frames.yml")

def load_frames(path: str) -> List[Dict[str, Any]]:
    with open(path, 'r') as f:
        return yaml.safe_load(f)

# --- Primary Hook Finder ---
def find_primary_hook(frame: Dict[str, Any]) -> Optional[str]:
    hooks = frame.get("hooks", [])
    for hook in hooks:
        if hook.get("primary", False):
            return hook.get("name")
    for hook in frame.get("composite_hooks", []) or []:
        if hook.get("primary", False):
            return hook.get("name")
    return None


# --- Foreign Hook Helpers ---
def get_foreign_frame_name(hook_name: str, all_frames: List[Dict[str, Any]]) -> str | None:
    for f in all_frames:
        for h in f.get("hooks", []) + (f.get("composite_hooks", []) or []):
            if h.get("name") == hook_name and h.get("primary", False):
                return f.get("name")
    return None


def get_foreign_hooks(frame: Dict[str, Any], primary_hook: str, all_frames: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def is_primary_in_own_frame(hook_name: str) -> bool:
        return get_foreign_frame_name(hook_name, all_frames) is not None

    all_hooks = frame.get("hooks", []) + (frame.get("composite_hooks", []) or [])
    return [
        hook for hook in all_hooks
        if hook["name"] != primary_hook and is_primary_in_own_frame(hook["name"])
    ]


# --- SQL Generation ---
def build_join_for_hook(hook: Dict[str, Any], left_table: str, all_frames: List[Dict[str, Any]], evaluator: MacroEvaluator) -> Dict[str, Any]:
    fk = hook["name"]
    foreign_frame_name = get_foreign_frame_name(fk, all_frames)
    if not foreign_frame_name:
        return {}

    right_table_name = f"_bridge__{foreign_frame_name}"
    right_table = f"dar.uss__staging.{right_table_name}"
    right_columns = evaluator.columns_to_types(right_table).keys()

    foreign_pit_hooks = [exp.column(column, table=right_table_name) for column in right_columns if column.startswith("_pit")]

    join = exp.Join(
        this=right_table,
        on=exp.and_(
            exp.EQ(
                this=exp.column(fk, table=left_table),
                expression=exp.column(fk, table=right_table_name)
            ),
            exp.LT(
                this=exp.column("_record__valid_from", table=left_table),
                expression=exp.column("_record__valid_to", table=right_table_name)
            ),
            exp.GT(
                this=exp.column("_record__valid_to", table=left_table),
                expression=exp.column("_record__valid_from", table=right_table_name)
            )
        ),
        kind="LEFT"
    )

    return {
        "join": join,
        "pit_hooks": foreign_pit_hooks,
        "right_table_name": right_table_name,
    }


def build_joins(foreign_hooks: List[Dict[str, Any]], left_table: str, all_frames: List[Dict[str, Any]], evaluator: MacroEvaluator) -> Dict[str, Any]:
    joins = []
    foreign_hook_columns = []
    record_metadata_tables = [left_table]

    for hook in foreign_hooks:
        join_data = build_join_for_hook(hook, left_table, all_frames, evaluator)
        if join_data:
            joins.append(join_data["join"])
            foreign_hook_columns.extend(join_data["pit_hooks"])
            record_metadata_tables.append(join_data["right_table_name"])
    
    return {
        "joins": joins,
        "foreign_hook_columns": foreign_hook_columns,
        "record_metadata_tables": record_metadata_tables,
    }


def build_validity_expressions(tables: List[str]) -> Dict[str, exp.Expression]:
    def validity_expression(column_name: str, func_name: str) -> exp.Expression:
        columns = [exp.column(column_name, table=t) for t in tables]
        return exp.func(func_name, *columns) if len(columns) > 1 else columns[0]

    return {
        "record__updated_at": validity_expression("_record__updated_at", "GREATEST"),
        "record__valid_from": validity_expression("_record__valid_from", "GREATEST"),
        "record__valid_to": validity_expression("_record__valid_to", "LEAST"),
        "record__is_current": validity_expression("_record__is_current", "LEAST"),
    }


# --- Main Entrypoint ---
frames_path = get_frames_path()
frames = load_frames(frames_path)

@model(
    "dar.uss__staging._bridge__@{name}",
    enabled=True,
    is_sql=True,
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column="_record__updated_at",
    ),
    cron="*/5 * * * *",  # Run every 5 min, the smallest cron supported by SQLMesh
    blueprints=frames,
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    name = evaluator.blueprint_var("name")
    hooks = evaluator.blueprint_var("hooks") or []
    composite_hooks = evaluator.blueprint_var("composite_hooks") or []
    frame = {"hooks": hooks, "composite_hooks": composite_hooks}

    primary_hook = find_primary_hook(frame)
    if not primary_hook:
        raise ValueError(f"No primary hook found for frame {name}")

    foreign_hooks = get_foreign_hooks(frame, primary_hook, frames)
    
    left_table = f"frame__{name}"
    join_data = build_joins(foreign_hooks, left_table, frames, evaluator)
    
    validity_expressions = build_validity_expressions(join_data["record_metadata_tables"])

    sql = (
        exp.select(
            exp.cast(exp.Literal.string(name), exp.DataType.build("text")).as_("peripheral"),
            exp.column(f"_pit{primary_hook}", table=left_table),
            exp.column(primary_hook, table=left_table),
            *join_data["foreign_hook_columns"],
            validity_expressions["record__updated_at"].as_("_record__updated_at"),
            validity_expressions["record__valid_from"].as_("_record__valid_from"),
            validity_expressions["record__valid_to"].as_("_record__valid_to"),
            validity_expressions["record__is_current"].as_("_record__is_current")
        )
        .from_(f"dab.hook.frame__{name}")
    )

    for join in join_data["joins"]:
        sql = sql.join(join)
    
    sql = sql.where(
        validity_expressions["record__updated_at"].between(
            low=evaluator.locals["start_ts"],
            high=evaluator.locals["end_ts"]
        )
    )

    return sql

import os
import yaml
from typing import Any, Dict, List, Optional

from sqlglot import exp
from sqlmesh.core.model import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName

# --- File and Frame Utilities ---
def load_model_yaml() -> List[Dict[str, Any]]:
    """Loads models from a YAML file."""
    path = "sqlmesh/models/models.yml"

    with open(path, 'r') as f:
        return yaml.safe_load(f)

def filter_frames(frames: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [frame for frame in frames if not frame.get("skip_generation", False)]

# --- Hook Expression Builders ---
def build_hook_expression(hook: Dict[str, Any]) -> Optional[exp.Expression]:
    name = hook.get("name")
    keyset = hook.get("keyset", "")
    expression = hook.get("expression")
    if not name or expression is None:
        return None
    return exp.func(
        "CONCAT",
        exp.Literal.string(f"{keyset}|"),
        exp.cast(expression, exp.DataType.build("text"))
    ).as_(name)

def build_composite_hook_expression(hook: Dict[str, Any]) -> Optional[exp.Expression]:
    name = hook.get("name")
    child_hooks = hook.get("hooks") or []
    if not name or not isinstance(child_hooks, list) or not child_hooks:
        return None
    return exp.func(
        "CONCAT_WS",
        exp.Literal.string("~"),
        *child_hooks
    ).as_(name)

# --- Primary Hook Expression ---
def build_primary_hook_expression(primary_hook: Optional[str]) -> Optional[exp.Expression]:
    if not primary_hook:
        return None
    return exp.func(
        "CONCAT",
        exp.column(primary_hook),
        exp.Literal.string("~epoch__valid_from|"),
        exp.cast(exp.column("_record__valid_from"), exp.DataType.build("text"))
    ).as_(f"_pit{primary_hook}")

# --- Hook Processing Functions ---
def process_hooks(hooks: Any) -> tuple[list, Optional[str], list]:
    all_hooks = []
    primary_hook = None
    hook_expressions = []
    if isinstance(hooks, list):
        for hook in hooks:
            name = hook.get("name")
            if not name:
                continue
            all_hooks.append(name)
            if hook.get("primary", False):
                primary_hook = name
            expr = build_hook_expression(hook)
            if expr is not None:
                hook_expressions.append(expr)
    return all_hooks, primary_hook, hook_expressions

def process_composite_hooks(composite_hooks: Any) -> tuple[list, Optional[str], list]:
    all_hooks = []
    primary_hook = None
    composite_hook_expressions = []
    if isinstance(composite_hooks, list):
        for hook in composite_hooks:
            name = hook.get("name")
            if not name:
                continue
            all_hooks.append(name)
            if hook.get("primary", False):
                primary_hook = name
            expr = build_composite_hook_expression(hook)
            if expr is not None:
                composite_hook_expressions.append(expr)
    return all_hooks, primary_hook, composite_hook_expressions

# --- CTE Builders ---
def build_cte_source(source_table: str) -> exp.Expression:
    return exp.select(exp.Star()).from_(source_table)

def build_cte_hooks(hook_expressions: List[exp.Expression]) -> exp.Expression:
    return exp.select(*hook_expressions, exp.Star()).from_("cte__source")

def build_cte_composite_hooks(composite_hook_expressions: List[exp.Expression]) -> exp.Expression:
    return exp.select(*composite_hook_expressions, exp.Star()).from_("cte__hooks")

def build_cte_primary_hook(primary_hook_expression: Optional[exp.Expression]) -> Optional[exp.Expression]:
    if not primary_hook_expression:
        return None
    return exp.select(primary_hook_expression, exp.Star()).from_("cte__composite_hooks")

# --- Main Entrypoint ---
models = load_model_yaml()
models_to_generate = filter_frames(models)

@model(
    "dab.hook.@{name}",
    enabled=True,
    is_sql=True,
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column="_record__updated_at",
    ),
    blueprints=models_to_generate,
)
def entrypoint(evaluator: MacroEvaluator) -> str | exp.Expression:
    name = evaluator.blueprint_var("name")
    hooks = evaluator.blueprint_var("hooks")
    composite_hooks = evaluator.blueprint_var("composite_hooks")
    source_table = f"das.scd.{name}"
    source_columns = evaluator.columns_to_types(source_table).keys()

    all_hooks, primary_hook, hook_expressions = process_hooks(hooks)
    composite_hook_names, composite_primary_hook, composite_hook_expressions = process_composite_hooks(composite_hooks)

    all_hooks += composite_hook_names
    if composite_primary_hook:
        primary_hook = composite_primary_hook

    # Filter out None expressions
    hook_expressions = [e for e in hook_expressions if e is not None]
    composite_hook_expressions = [e for e in composite_hook_expressions if e is not None]

    primary_hook_expression = build_primary_hook_expression(primary_hook)
    cte__source = build_cte_source(source_table)
    cte__hooks = build_cte_hooks(hook_expressions)
    cte__composite_hooks = build_cte_composite_hooks(composite_hook_expressions)
    cte__primary_hook = build_cte_primary_hook(primary_hook_expression)

    if not primary_hook or not primary_hook_expression or not cte__primary_hook:
        raise ValueError("Primary hook is missing or invalid.")

    sql = (
        exp.select(
            f"_pit{primary_hook}",
            *all_hooks,
            *source_columns
        )
        .from_("cte__primary_hooks")
        .where(
            exp.column("_record__updated_at").between(
                low=evaluator.locals["start_ts"],
                high=evaluator.locals["end_ts"]
            )
        )
        .with_("cte__source", as_=cte__source)
        .with_("cte__hooks", as_=cte__hooks)
        .with_("cte__composite_hooks", as_=cte__composite_hooks)
        .with_("cte__primary_hooks", as_=cte__primary_hook)
    )
    return sql
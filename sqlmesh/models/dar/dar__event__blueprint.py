import yaml
from typing import Any, Dict, List

from sqlglot import exp
from sqlmesh.core.model import model
from sqlmesh.core.macros import MacroEvaluator
from sqlmesh.core.model.kind import ModelKindName

# --- File and Frame Utilities ---
def load_model_yaml() -> List[Dict[str, Any]]:
    """Loads models from a YAML file."""
    path = "sqlmesh/models/models.yml"

    with open(path, 'r') as f:
        data = yaml.safe_load(f)
        # Filter out models without events
        return [m for m in data if m.get("events")]


# --- SQL Generation ---
def build_event_bridge_sql(
    name: str,
    events: List[Dict[str, Any]],
    evaluator: MacroEvaluator
) -> exp.Expression:
    """Build the complete SQL for an event bridge model using sqlglot."""
    
    bridge_table = f"dar.uss__staging._bridge__{name}"
    
    # Get columns from bridge table
    bridge_columns = list(evaluator.columns_to_types(bridge_table).keys())
    
    # Build event columns and collect unique sources for joins
    event_columns = []
    event_names = []
    sources_to_join = {}  # source -> join_on mapping
    
    for event in events:
        event_name = event.get("name")
        source = event.get("source", f"dab.hook.{name}")
        join_on = event.get("join_on")
        expression = event.get("expression")
        
        if not event_name or not expression:
            continue
        
        # Extract table name from source
        source_table = source.split(".")[-1]
        
        # Track sources that need joins (not the bridge table itself)
        if source != bridge_table and join_on:
            sources_to_join[source] = join_on
        
        # Build the event column expression
        event_col = exp.cast(
            exp.column(expression, table=source_table),
            exp.DataType.build("TIMESTAMP")
        ).as_(event_name)
        
        event_columns.append(event_col)
        event_names.append(event_name)
    
    if not event_names:
        # No valid events, return empty query
        return exp.select(exp.Literal.number(1)).where(exp.false())
    
    # Build CTE for source
    cte__source = exp.select("*").from_(bridge_table)
    
    # Build CTE for events with joins
    cte__events = exp.select(
        *[exp.column(col, table="cte__source") for col in bridge_columns],
        *event_columns
    ).from_("cte__source")
    
    # Add joins for each unique source
    for source, join_col in sources_to_join.items():
        source_table = source.split(".")[-1]
        # Create proper join expression with explicit ON condition
        join = exp.Join(
            this=source,
            on=exp.EQ(
                this=exp.column(join_col, table="cte__source"),
                expression=exp.column(join_col, table=source_table)
            ),
            kind="LEFT"
        )
        cte__events = cte__events.join(join)
    
    # Build CTE for unpivot
    unpivot_sql = f"""
    SELECT * FROM cte__events
    UNPIVOT(event_occurred_at FOR event IN ({', '.join(event_names)})) AS long
    """
    cte__unpivot = evaluator.parse_one(unpivot_sql)
    
    # Build the final SELECT
    record_columns = [exp.column(col) for col in bridge_columns if col.startswith("_record__")]
    non_record_columns = [exp.column(col) for col in bridge_columns if not col.startswith("_record__")]

    event = exp.column("event")

    event_occurred_on = exp.cast(
        exp.column("event_occurred_at"),
        exp.DataType.build("DATE")
    ).as_("event_occurred_on")

    event_occurred_at = exp.cast(
        exp.column("event_occurred_at"),
        exp.DataType(this=exp.DataType.Type.TIME, expressions=[exp.Literal.number(0)])
    ).as_("event_occurred_at")

    final_fields = [
        *non_record_columns,
        event,
        event_occurred_on,
        event_occurred_at,
        *record_columns
    ]

    final_select = exp.select(
        *final_fields
    ).from_("cte__unpivot")
    
    # Add WHERE clause
    final_select = final_select.where(
        exp.and_(
            exp.EQ(this=exp.Literal.number(1), expression=exp.Literal.number(1)),
            exp.column("_record__updated_at").between(
                low=evaluator.locals["start_ts"],
                high=evaluator.locals["end_ts"]
            )
        )
    )
    
    # Add CTEs using .with_() method
    sql = (
        final_select
        .with_("cte__source", as_=cte__source)
        .with_("cte__events", as_=cte__events)
        .with_("cte__unpivot", as_=cte__unpivot)
    )
    
    return sql


# --- Main Entrypoint ---
models = load_model_yaml()

@model(
    "dar.uss__staging._event_bridge__@{name}",
    enabled=True,
    cron="@daily",
    is_sql=True,
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
        time_column="_record__updated_at",
    ),
    blueprints=models,  # Already filtered to only include models with events
    # Partition by event date and event type for efficient event-based queries
    partitioned_by=["event_occurred_on", "event"],
    # Cluster by peripheral, event, and current flag for optimal query performance
    clustered_by=["peripheral", "event", "_record__is_current"],
)
def entrypoint(evaluator: MacroEvaluator) -> exp.Expression:
    name = evaluator.blueprint_var("name")
    
    if not isinstance(name, str) or not name:
        raise ValueError("Blueprint variable 'name' must be a non-empty string")
    
    events = evaluator.blueprint_var("events") or []
    
    # Build and return the SQL
    sql = build_event_bridge_sql(name, events, evaluator)
    
    return sql
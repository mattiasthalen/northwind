from __future__ import annotations

import typing as t
from sqlglot import exp
from sqlmesh.core.macros import macro

@macro()
def star__list(
    evaluator,
    table_name: exp.Table,
    exclude: t.Union[exp.Array, exp.Tuple] = exp.Tuple(expressions=[])
    ) -> t.List[exp.Column]:
    """
    Returns a list of exp.Column objects for the given table, excluding any specified fields.
    Args:
        evaluator: The macro evaluator (provided by SQLMesh).
        table_name (exp.Table): The table expression.
        exclude (exp.Array|exp.Tuple, optional): Columns to exclude.
    Returns:
        list[exp.Column]: List of exp.Column objects.
    """
    excluded_names = {e.name for e in exclude.expressions}
    columns = [
        exp.column(col) for col in evaluator.columns_to_types(table_name).keys()
        if col not in excluded_names
    ]
    return columns

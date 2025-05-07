from typing import List, Dict, Any
from cmr_connectors_lib.database_connectors.utils.enums import SelectType, AggregationFunction, ColumnType, JoinType, ComparisonType, QueryOperator, DateUnit


def _build_select_clause(selected_fields: List[Dict[str, Any]]) -> str:
    """Build the SELECT clause for PostgreSQL"""
    if not selected_fields:
        return "SELECT *"

    select_parts = []
    for field in selected_fields:
        field_name = field.get('field', '')
        table = field.get('table', '')
        alias = (field.get('alias') or '').strip().replace(' ', '_')
        select_type = field.get('selectType')
        field_type = field.get('type')
        aggregate = (field.get('aggregate') or '').upper()
        is_count_all = field.get('isCountAll', False)

        # Basic column reference
        column_expr = f"{table}.{field_name}" if table else field_name

        # Aggregations
        if select_type == SelectType.aggregate.value and aggregate:
            if aggregate == AggregationFunction.COUNT_DISTINCT.value:
                column_expr = f"COUNT(DISTINCT {column_expr})"
            elif aggregate == AggregationFunction.DISTINCT.value:
                column_expr = f"DISTINCT {column_expr}"
            elif is_count_all:
                column_expr = "COUNT(*)"
            else:
                column_expr = f"{aggregate}({column_expr})"

        # Cast array/list types to TEXT
        if field_type in (
            ColumnType.List.value,
            ColumnType.Set.value,
            ColumnType.MultiSet.value
        ) and select_type == SelectType.Normal.value:
            column_expr = f"{column_expr}::TEXT"

        # Add alias
        if alias:
            column_expr = f"{column_expr} AS {alias}"

        select_parts.append(column_expr)

    return "SELECT " + ", ".join(select_parts)


def _build_joins_clause(base_table: str, joins: List[Dict[str, Any]]) -> str:
    """Build JOINs for PostgreSQL (syntax is identical here)"""
    join_map = {
        'INNER': "INNER JOIN",
        'LEFT': "LEFT JOIN",
        'RIGHT': "RIGHT JOIN"
    }
    parts = []
    for join in joins:
        jt = join.get('joinType', JoinType.INNER.value).upper()
        target = join.get('targetTable')
        conds = join.get('conditions', [])
        if not target or not conds:
            continue

        join_kw = join_map.get(jt, "INNER JOIN")
        clauses = []
        for idx, c in enumerate(conds):
            connector = f" {conds[idx-1].get('connector', 'AND').upper()} " if idx > 0 else ""
            src = c.get('sourceField')
            tgt = c.get('targetField')
            op = c.get('operator', '=')
            if src and tgt:
                clauses.append(f"{connector}{base_table}.{src} {op} {target}.{tgt}")
        parts.append(f"{join_kw} {target} ON {''.join(clauses).lstrip()}")
    return " ".join(parts)


def _build_where_clause(conditions: List[Dict[str, Any]], invert: bool = False) -> str:
    """Build WHERE clause for PostgreSQL"""
    if not conditions:
        return ""

    parts = []
    for i, cond in enumerate(conditions):
        connector = f"{conditions[i - 1].get('connector', 'AND')} " if i > 0 else ''
        field = cond.get('field')
        tbl = cond.get('table')
        expr = f"{tbl}.{field}" if tbl else field

        comp_type = cond.get('comparisonType', ComparisonType.Value.value)
        if comp_type == ComparisonType.Column.value:
            clause = _build_column_condition(cond)
        else:
            clause = _build_value_condition(
                expr,
                cond.get('operator', '='),
                cond.get('value'),
                cond.get('secondValue'),
                cond.get('valueType', 'string')
            )
        if not clause:
            continue

        parts.append(f"{connector}{clause}")

    where = " ".join(parts)
    if invert:
        return f"WHERE NOT ({where})"
    return f"WHERE {where}" if where else ""


def _build_group_by(group_by_fields: List[Dict[str, Any]]) -> str:
    """Postgres GROUP BY (same as Informix)"""
    if not group_by_fields:
        return ""
    cols = []
    for f in group_by_fields:
        tbl = f.get('table')
        name = f.get('field')
        if name:
            cols.append(f"{tbl}.{name}" if tbl else name)
    return "GROUP BY " + ", ".join(cols)


def _build_having_clause(having_fields: List[Dict[str, Any]]) -> str:
    if not having_fields:
        return ""
    conds = []
    for i, h in enumerate(having_fields):
        conn = f" {having_fields[i - 1].get('connector', 'AND')} " if i > 0 else ""
        c = format_having_condition(h, conn)
        if c:
            conds.append(c)
    return "HAVING " + "".join(conds) if conds else ""


def _build_value_condition(
    field_expr: str,
    operator: str,
    value: Any,
    second_value: Any,
    value_type: str
) -> str:
    """Build column vs. literal for Postgres"""

    formatted = _format_value(value, value_type)

    # BETWEEN / NOT BETWEEN
    if operator in (QueryOperator.BETWEEN.value, QueryOperator.NOT_BETWEEN.value):
        sec = _format_value(second_value, value_type)
        op = "BETWEEN" if operator == QueryOperator.BETWEEN.value else "NOT BETWEEN"
        return f"{field_expr} {op} {formatted} AND {sec}"

    # Array/list contains
    if operator == QueryOperator.LIST_CONTAINS.value:
        # e.g. 'foo' = ANY(arr_col)
        return f"{formatted} = ANY({field_expr})"
    elif operator == QueryOperator.LIST_NOT_CONTAINS.value:
        return f"NOT ({formatted} = ANY({field_expr}))"

    # LIKE / NOT LIKE
    if operator == QueryOperator.CONTAINS.value:
        return f"{field_expr} LIKE '%{value}%'"
    if operator == QueryOperator.NOT_CONTAINS.value:
        return f"{field_expr} NOT LIKE '%{value}%'"
    if operator == QueryOperator.STARTS_WITH.value:
        return f"{field_expr} LIKE '{value}%'"
    if operator == QueryOperator.ENDS_WITH.value:
        return f"{field_expr} LIKE '%{value}'"

    # Regex
    if operator in (QueryOperator.MATCHES.value, QueryOperator.NOT_MATCHES.value):
        return f"{field_expr} {operator} '{value}'"

    # IN / NOT IN
    if operator in (QueryOperator.IN.value, QueryOperator.NOT_IN.value):
        return f"{field_expr} {operator} ({formatted})"

    # Fallback (=, !=, >, <, >=, <=)
    return f"{field_expr} {operator} {formatted}"


def _build_column_condition(cond: Dict[str, Any]) -> str:
    """
    Column vs. column comparison for Postgres, with EXTRACT() for dates.
    """
    tbl = cond.get('table')
    fld = cond.get('field')
    tgt_tbl = cond.get('targetTable')
    tgt_fld = cond.get('targetField')
    op = cond.get('operator')
    cmp_op = cond.get('compareOperator', '=')
    unit = cond.get('dateUnit')
    val = cond.get('value')
    ftype = cond.get('valueType', 'string')

    left = f"{tbl}.{fld}" if tbl else fld
    right = f"{tgt_tbl}.{tgt_fld}" if tgt_tbl else tgt_fld

    if ftype != ColumnType.Date.value:
        base = f"{left} {cmp_op} {right}"
        return f"{base} {op} {val}" if op else base

    # Date difference
    if unit == DateUnit.Year.value:
        expr = f"(EXTRACT(YEAR FROM {left}) - EXTRACT(YEAR FROM {right}))"
    elif unit == DateUnit.Month.value:
        expr = (
            f"((EXTRACT(YEAR FROM {left}) - EXTRACT(YEAR FROM {right})) * 12 + "
            f"(EXTRACT(MONTH FROM {left}) - EXTRACT(MONTH FROM {right})))"
        )
    else:
        expr = f"({left} - {right})"

    return f"{expr} {op} {val}" if op else expr


def format_having_condition(having: Dict[str, Any], connector: str) -> str:
    op = having.get('operator')
    if not op:
        return None

    tbl = having.get('table')
    fld = having.get('field')
    expr = f"{tbl}.{fld}" if tbl else fld
    agg = having.get('aggregate')
    is_agg = having.get('isAggregation', False)
    is_cnt_all = having.get('isCountAll', False)

    if is_agg and agg:
        if agg == AggregationFunction.COUNT_DISTINCT.value:
            expr = f"COUNT(DISTINCT {expr})"
        elif is_cnt_all:
            expr = "COUNT(*)"
        else:
            expr = f"{agg}({expr})"
    value = having.get('value')
    second_value = having.get('secondValue')
    value_type = having.get('valueType')
    condition_str = _build_value_condition(expr, op, value, second_value, value_type)
    return f"{connector}{condition_str}"


def _format_value(value: Any, field_type: str) -> str:
    """Format a literal for PostgreSQL."""
    if value is None:
        return ''

    if field_type == ColumnType.Date.value:
        return f"'{value}'::DATE"
    if field_type == ColumnType.Datetime.value:
        return f"'{value}'::TIMESTAMP"
    if field_type == ColumnType.Boolean.value:
        return "TRUE" if value else "FALSE"
    if field_type == ColumnType.Number.value:
        return str(value)
    if field_type in (
        ColumnType.List.value,
        ColumnType.MultiSet.value,
        ColumnType.Set.value
    ):
        # e.g. ARRAY['a','b','c']
        items = ", ".join(f"'{v}'" for v in value)
        return f"ARRAY[{items}]"

    # Default: quote and escape strings
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return f"'{value}'"

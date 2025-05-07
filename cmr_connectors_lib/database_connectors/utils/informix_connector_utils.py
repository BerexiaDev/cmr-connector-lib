from typing import List, Dict, Any
from enums import SelectType, AggregationFunction, ColumnType, JoinType, ComparisonType, QueryOperator, DateUnit


def _build_select_clause(selected_fields: List[Dict[str, Any]]) -> str:
    """Build the SELECT clause for Informix"""
    if not selected_fields:
        return "SELECT *"

    select_parts = []
    for field in selected_fields:
        field_expr = field.get('field', '')
        table = field.get('table', '')
        alias = field.get('alias', '')
        select_type = field.get('selectType')
        field_type = field.get('type')
        aggregate = field.get('aggregate', '').upper()
        is_count_all = field.get('isCountAll', False)

        # Build the column expression
        if table:
            column_expr = f"{table}.{field_expr}"
        else:
            column_expr = field_expr

        # Apply aggregation if specified
        if select_type == SelectType.aggregate.value and aggregate:
            if aggregate == AggregationFunction.COUNT_DISTINCT.value:
                column_expr = f"COUNT(DISTINCT {column_expr})"
            elif aggregate == AggregationFunction.DISTINCT.value:
                column_expr = f"DISTINCT {column_expr}"
            elif is_count_all:
                column_expr = f"COUNT(*)"
            else:
                column_expr = f"{aggregate}({column_expr})"

        if field_type in [ColumnType.List.value, ColumnType.Set.value,
                          ColumnType.MultiSet.value] and select_type == SelectType.Normal.value:
            column_expr = f"{column_expr}::LVARCHAR"

        # Apply alias if specified
        alias = alias.strip()
        if alias:
            # replace spaces with underscores
            alias = alias.replace(' ', '_')
            column_expr = f"{column_expr} AS {alias}"

        select_parts.append(column_expr)

    return "SELECT " + ", ".join(select_parts)


def _build_joins_clause(base_table: str, joins: List[Dict[str, Any]]) -> str:
    """Build JOINs for Informix"""

    join_parts = []

    # Map join types to Informix syntax.
    join_map = {
        'INNER': "INNER JOIN",
        'LEFT': "LEFT JOIN",
        'RIGHT': "RIGHT JOIN"
    }

    for join in joins:
        join_type = join.get('joinType', JoinType.INNER.value).upper()
        target_table = join.get('targetTable')
        conditions = join.get('conditions', [])

        # Skip this join if there's no target table or no conditions.
        if not target_table or not conditions:
            continue

        join_keyword = join_map.get(join_type, "INNER JOIN")

        # Build the join conditions string.
        condition_clauses = []
        for idx, condition in enumerate(conditions):
            # For additional conditions, prepend the connector from the previous condition.
            connector = f" {conditions[idx - 1].get('connector', 'AND').upper()} " if idx > 0 else ""

            source_field = condition.get('sourceField')
            target_field = condition.get('targetField')
            operator = condition.get('operator', '=')

            if not source_field or not target_field:
                continue

            clause = f"{connector}{base_table}.{source_field} {operator} {target_table}.{target_field}"
            condition_clauses.append(clause)

        join_condition = "".join(condition_clauses)
        # Ensure a space after ON.
        join_expr = f"{join_keyword} {target_table} ON {join_condition.lstrip()}"
        join_parts.append(join_expr)

    return " ".join(join_parts)


def _build_where_clause(conditions: List[Dict[str, Any]], invert: bool = False) -> str:
    """Build the WHERE clause for Informix, with optional NOT(...) inversion."""
    if not conditions:
        return ""

    where_parts = []
    for i, condition in enumerate(conditions):
        # Determine connector (AND / OR, etc.)
        connector = "AND"
        if i > 0:
            connector = conditions[i - 1].get('connector', 'AND')

        field = condition.get('field')
        operator = condition.get('operator', '=')
        table = condition.get('table')
        field_type = condition.get('valueType', 'string')
        value = condition.get('value')
        second_value = condition.get('secondValue')

        field_expr = f"{table}.{field}" if table else field

        comparison_type = condition.get('comparisonType', ComparisonType.Value.value)  # e.g. 'VALUE' or 'COLUMN'

        # If we are comparing column-to-column
        if comparison_type == ComparisonType.Column.value:
            condition_part = _build_column_condition(condition)

        # Otherwise, default to "compare column vs. a literal value"
        else:
            condition_part = _build_value_condition(field_expr, operator, value, second_value, field_type)

        if not condition_part:
            continue

        # For the first condition, no connector prefix
        if i == 0:
            where_parts.append(condition_part)
        else:
            where_parts.append(f"{connector} {condition_part}")

    where_clause = " ".join(where_parts)

    if invert and where_clause:
        return f"WHERE NOT ({where_clause})"
    return f"WHERE {where_clause}" if where_clause else ""


def _build_group_by(group_by_fields: List[Dict[str, Any]]) -> str:
    """Build the GROUP BY clause for Informix"""
    if not group_by_fields:
        return ""

    group_by_parts = []
    for field in group_by_fields:
        table = field.get('table')
        field_name = field.get('field')

        if not field_name:
            continue

        # Add table prefix if provided
        field_expr = f"{table}.{field_name}" if table else field_name
        group_by_parts.append(field_expr)

    return "GROUP BY " + ", ".join(group_by_parts)


def _build_having_clause(having_fields: List[Dict[str, Any]]) -> str:
    if not having_fields:
        return ""

    conditions = []
    # Use the previous field's 'connector' for non-first conditions.
    for i, having in enumerate(having_fields):
        connector = f" {having_fields[i - 1].get('connector', 'AND')} " if i > 0 else ""
        cond = format_having_condition(having, connector)
        if cond:
            conditions.append(cond)

    if not conditions:
        return ""

    return "HAVING " + "".join(conditions)


def _build_value_condition(
        field_expr: str,
        operator: str,
        value: Any,
        second_value: Any,
        value_type: str
) -> str:
    """Handles comparison between a column and a literal value (BETWEEN, LIKE, etc.)."""

    formatted_val = _format_value(value, value_type)

    # BETWEEN / NOT BETWEEN
    if operator in (QueryOperator.BETWEEN.value, QueryOperator.NOT_BETWEEN.value):
        formated_second_value = _format_value(second_value, value_type)
        between_op = "BETWEEN" if operator == QueryOperator.BETWEEN.value else "NOT BETWEEN"
        return f"{field_expr} {between_op} {formatted_val} AND {formated_second_value}"

    is_list_type = value_type in [ColumnType.List.value, ColumnType.Set.value, ColumnType.MultiSet.value]

    # LIKE / NOT LIKE
    if operator == QueryOperator.LIST_CONTAINS.value:
        return f"'{value}' IN {field_expr}"
    elif operator == QueryOperator.LIST_NOT_CONTAINS.value:
        return f"'{value}' NOT IN {field_expr}"

    if operator == QueryOperator.CONTAINS.value:
        return f"{field_expr} LIKE '%{value}%'"
    elif operator == QueryOperator.NOT_CONTAINS.value:
        return f"{field_expr} NOT LIKE '%{value}%'"

    # STARTS_WITH / ENDS_WITH
    elif operator == QueryOperator.STARTS_WITH.value:
        return f"{field_expr} LIKE '{value}%'"
    elif operator == QueryOperator.ENDS_WITH.value:
        return f"{field_expr} LIKE '%{value}'"

    # REGEX (MATCHES in Informix)
    elif operator in [QueryOperator.MATCHES.value, QueryOperator.NOT_MATCHES.value]:
        return f"{field_expr} {operator} '{value}'"

    elif operator in [QueryOperator.IN.value, QueryOperator.NOT_IN.value]:
        # quoted_values = ', '.join(f"'{v}'" for v in value)
        return f"{field_expr} {operator} ({formatted_val})"

    elif is_list_type and (operator in [QueryOperator.EQUALS.value, QueryOperator.NOT_EQUALS.value]):
        # value = ', '.join(f"'{item}'" for item in value)
        return f"{field_expr} {operator} {value_type.upper()}{{{formatted_val}}}"

    # Handle basic operators (=, !=, >, <, >=, <=)
    return f"{field_expr} {operator} {formatted_val}"



def _build_column_condition(condition: Dict[str, Any]) -> str:
    """
    Handles comparison between two columns. Optionally calculates
    difference in YEAR, MONTH, or DAY for Informix, e.g.:
      (YEAR(col1) - YEAR(col2))
      ((YEAR(col1) - YEAR(col2))*12 + (MONTH(col1) - MONTH(col2)))
      (col1 - col2)
    If `compareOperator` == '-', treat difference as absolute and compare to `= value`.
    """
    table = condition.get('table')
    field = condition.get('field')
    target_table = condition.get('targetTable')
    target_field = condition.get('targetField')
    operator = condition.get('operator')
    compare_op = condition.get('compareOperator', '=')
    date_unit = condition.get('dateUnit')
    value = condition.get('value')
    field_type = condition.get('valueType', 'string')

    left_expr = f"{table}.{field}" if table else field
    right_expr = f"{target_table}.{target_field}" if target_table else target_field

    # For non-date fields, perform a direct comparison.
    if field_type != ColumnType.Date.value:
        base_expr = f"{left_expr} {compare_op} {right_expr}"
        return f"{base_expr} {operator} {value}" if operator else base_expr

    # For date fields, build the expression based on the specified date unit.
    if date_unit == DateUnit.Year.value:
        expr = f"(YEAR({left_expr}) - YEAR({right_expr}))"
    elif date_unit == DateUnit.Month.value:
        expr = (
            f"((YEAR({left_expr}) - YEAR({right_expr})) * 12 + "
            f"(MONTH({left_expr}) - MONTH({right_expr})))"
        )
    else:
        expr = f"({left_expr} {compare_op} {right_expr})"

    return f"{expr} {operator} {value}" if operator is not None else expr



def format_having_condition(having: Dict[str, Any], connector: str):
    operator = having.get('operator')
    value = having.get('value')
    is_aggregation = having.get('isAggregation', False)
    aggregate = having.get('aggregate')
    field = having.get('field')
    table = having.get('table')
    second_value = having.get('secondValue')
    value_type = having.get('valueType')
    is_count_all = having.get('isCountAll', False)
    # Skip condition if required components are missing
    if not operator:
        return None

    field_expr = f"{table}.{field}" if table else field

    if is_aggregation and aggregate:
        if aggregate == AggregationFunction.COUNT_DISTINCT.value:
            field_expr = f"COUNT(DISTINCT {field_expr})"
        elif is_count_all:
            field_expr = f"COUNT(*)"
        else:
            field_expr = f"{aggregate}({field_expr})"

    condition_str = _build_value_condition(field_expr, operator, value, second_value, value_type)

    return f"{connector}{condition_str}"


def _format_value(value, field_type):
    """Format a value based on its type for SQL compatibility."""
    if value is None:
        return ''

    if field_type == ColumnType.Date.value:
        return f"TO_DATE('{value}', '{'%Y-%m-%d'}')"
    if field_type == ColumnType.Datetime.value:
        return f"'{value}'"
    elif field_type == ColumnType.Boolean.value:
        return "'t'" if value else "'f'"
    elif field_type == ColumnType.Number.value:
        return value
    elif field_type in [ColumnType.List.value, ColumnType.MultiSet.value, ColumnType.Set.value]:
        return ', '.join(f"'{item}'" for item in value)

    else:  # Default to string
        if isinstance(value, str):
            # Escape single quotes
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        return f"'{value}'"

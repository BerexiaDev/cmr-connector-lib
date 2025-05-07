from typing import Dict, Any, List

from .enums import QueryOperator, ComparisonType, HavingConditionType, DateUnit, ColumnType, \
    AggregationFunction
from loguru import logger
from sqlalchemy import MetaData, func, distinct, select, Select, and_, or_, Table, Column, not_, cast, String

from enums import SelectType, AggregationFunction, ColumnType, QueryOperator, JoinType


def process_select_fields(
    data: Dict[str, Any],
    connection,
    base_table_name: str,
    schema: str
) -> List:
    """
    Turn your JSON “selectedFields” spec into a list of SQLAlchemy ColumnElement
    (with aggregates, casts, and labels applied).
    """
    selected_columns = []

    for sel in data.get('selectedFields', []):
        # 1. resolve table & column
        table_name = sel.get('table') or base_table_name
        field_name = sel.get('field')
        if not field_name:
            raise ValueError(f"Missing field in {sel!r}")

        tbl = load_single_table(connection, table_name, schema)
        col = validate_field(tbl, field_name, table_name)

        # 2. aggregation?
        if sel.get('type') == SelectType.aggregate.value and sel.get('aggregate'):
            agg = sel['aggregate'].upper()
            if agg == AggregationFunction.COUNT.value:
                col = func.count(col)
            elif agg == AggregationFunction.SUM.value:
                col = func.sum(col)
            elif agg == AggregationFunction.AVERAGE.value:
                col = func.avg(col)
            elif agg == AggregationFunction.MINIMUM.value:
                col = func.min(col)
            elif agg == AggregationFunction.MAXIMUM.value:
                col = func.max(col)
            elif agg == AggregationFunction.DISTINCT.value:
                col = col.distinct()
            elif agg == AggregationFunction.COUNT_DISTINCT.value:
                col = func.count(distinct(col))
            elif sel.get('isCountAll', False):
                col = func.count()
            else:
                raise ValueError(f"Unsupported aggregate: {agg}")

        # 4. aliasing
        alias = (sel.get('alias') or "").strip().replace(" ", "_")
        if alias:
            col = col.label(alias)

        selected_columns.append(col)

    return selected_columns


def process_joins(
    connection,
    schema: str,
    data: Dict[str, Any],
    stmt: Select,
    base_table_name: str,
    base_table: Table
) -> Select:
    """
    Process and apply join definitions to the given Select statement.
    Expects each join_def in data['joins'] to have:
      - 'targetTable'
      - either a flat 'sourceField' & 'targetField', or a list of 'conditions',
        where each condition is a dict with:
          'sourceField', 'targetField', optional 'operator', optional 'connector'
    """
    try:
        for join_def in data.get('joins', []):
            tgt_name = join_def.get('targetTable')
            if not tgt_name:
                raise ValueError("join definition missing 'targetTable'")

            # load the target table
            target_table = load_single_table(connection, tgt_name, schema)
            conditions = join_def.get('conditions', [])
            # build the ON clause
            on_clause = None
            for idx, cond in enumerate(conditions):
                sf = cond.get('sourceField')
                tf = cond.get('targetField')
                if not sf or not tf:
                    raise ValueError(f"invalid join condition: {cond!r}")

                left_col  = base_table.c[sf]
                right_col = target_table.c[tf]

                op = cond.get('operator', '=')
                if   op == '=':  expr = left_col == right_col
                elif op == '!=': expr = left_col != right_col
                elif op == '<':  expr = left_col <  right_col
                elif op == '>':  expr = left_col >  right_col
                elif op == '<=': expr = left_col <= right_col
                elif op == '>=': expr = left_col >= right_col
                else:
                    raise ValueError(f"Unsupported operator {op!r}")

                if idx == 0:
                    on_clause = expr
                else:
                    prev_conn = conditions[idx - 1].get('connector', 'AND').upper()
                    if prev_conn == 'AND':
                        on_clause = and_(on_clause, expr)
                    elif prev_conn == 'OR':
                        on_clause = or_(on_clause, expr)
                    else:
                        raise ValueError(f"Unsupported connector {prev_conn!r}")

            # apply the join
            jt = join_def.get('joinType', JoinType.INNER.value).upper()
            if jt == JoinType.INNER.value:
                stmt = stmt.join(target_table, on_clause)
            elif jt == JoinType.LEFT.value:
                stmt = stmt.outerjoin(target_table, on_clause)
            elif jt == JoinType.RIGHT.value:
                # SQLAlchemy has no direct right‐join, so swap sides
                stmt = stmt.select_from(target_table.outerjoin(base_table, on_clause))
            else:
                raise ValueError(f"Unsupported join type: {jt!r}")

    except Exception as err:
        logger.error(f"Error processing joins: {err}")
        raise ValueError(f"Error in joins: {err}")

    return stmt


def process_where_conditions(connection, schema, data: Dict[str, Any]) -> List:
    """Process and validate WHERE conditions."""
    where_expressions = []
    try:
        for cond in data.get('whereConditions', []):
            expr = create_where_expression(connection, schema, cond)
            where_expressions.append(expr)
    except Exception as where_err:
        logger.error(f"Error processing where conditions: {where_err}")
        raise ValueError(f"Error in whereConditions: {str(where_err)}")

    return where_expressions


def create_where_expression(connection, schema, condition: Dict[str, Any]) -> Any:
    # This function creates SQLAlchemy expression for a WHERE condition.
    table_name = condition.get('table')
    if not table_name:
        raise ValueError("Table missing in where condition")
    table = load_single_table(connection, table_name, schema)

    field_name = condition.get('field')
    if not field_name:
        raise ValueError("Field missing in where condition")
    column = validate_field(table, field_name, table_name, "in where condition ")

    op = condition.get('operator')
    if not op:
        raise ValueError("Operator missing in where condition")

    val = condition.get('value')
    second_value = condition.get('secondValue')
    value_required_ops = {
        QueryOperator.EQUALS.value,
        QueryOperator.NOT_EQUALS.value,
        QueryOperator.GREATER_THAN.value,
        QueryOperator.LESS_THAN.value,
        QueryOperator.GREATER_THAN_OR_EQUAL.value,
        QueryOperator.LESS_THAN_OR_EQUAL.value,
        QueryOperator.STARTS_WITH.value,
        QueryOperator.ENDS_WITH.value,
        QueryOperator.CONTAINS.value,
        QueryOperator.NOT_CONTAINS.value,
        QueryOperator.IN.value,
        QueryOperator.NOT_IN.value,
        QueryOperator.BETWEEN.value,
        QueryOperator.NOT_BETWEEN.value,
        QueryOperator.MATCHES.value,
    }

    if op in value_required_ops and val is None:
        raise ValueError(f"Value required for operator '{op}'")
    if op in ['BETWEEN', 'NOT BETWEEN'] and second_value is None:
        raise ValueError(f"Second value required for operator '{op}'")

    if op == QueryOperator.EQUALS.value:
        return column == val
    elif op == QueryOperator.NOT_EQUALS.value:
        return column != val
    elif op == QueryOperator.GREATER_THAN.value:
        return column > val
    elif op == QueryOperator.LESS_THAN.value:
        return column < val
    elif op == QueryOperator.GREATER_THAN_OR_EQUAL.value:
        return column >= val
    elif op == QueryOperator.LESS_THAN_OR_EQUAL.value:
        return column <= val
    elif op == QueryOperator.CONTAINS.value:
        return column.ilike('%' + val + '%')
    elif op == QueryOperator.ENDS_WITH.value:
        return column.ilike('%' + val)
    elif op == QueryOperator.STARTS_WITH.value:
        return column.ilike(val + '%')
    elif op == QueryOperator.NOT_CONTAINS.value:
        return column.not_ilike('%' + val + '%')
    elif op == QueryOperator.IN.value:
        if not isinstance(val, list):
            raise ValueError("Value for 'IN' operator must be a list")
        return column.in_(val)
    elif op == QueryOperator.NOT_IN.value:
        if not isinstance(val, list):
            raise ValueError("Value for 'NOT IN' operator must be a list")
        return ~column.in_(val)
    elif op == QueryOperator.BETWEEN.value:
        return column.between(val, second_value)
    elif op == QueryOperator.NOT_BETWEEN.value:
        return ~column.between(val, second_value)
    elif op == QueryOperator.IS_NULL.value:
        return column.is_(None)
    elif op == QueryOperator.IS_NOT_NULL.value:
        return column.isnot(None)
    elif op == QueryOperator.MATCHES.value:
        return cast(column, String).regexp_match(val)
    else:
        raise ValueError(f"Unsupported operator: {op.value}")


def validate_base_data(data: Dict[str, Any]) -> bool:
    return bool(data and data.get('baseTable') and data.get('connector_id'))



def validate_field(table: Table, field_name: str, table_name: str, context: str = "") -> Column:
    if field_name not in table.c:
        raise ValueError(f"Field '{field_name}' {context}not found in table '{table_name}'")
    return table.c[field_name]

def load_single_table(connection, table_name: str, schema) -> Table:
    metadata = MetaData()
    tbl = Table(
        table_name,
        metadata,
        autoload_with=connection,
        schema=schema,
    )
    return tbl
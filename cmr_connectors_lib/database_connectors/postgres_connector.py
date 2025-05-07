#!/usr/bin/python
# -*- coding: utf-8 -*-
from asyncio.log import logger
from typing import Dict, Any

from .sql_connector import SqlConnector
import psycopg2

from ..connectors_factory import ConnectorFactory
from utils.postgres_connector_utils import load_single_table, process_select_fields, process_joins, \
    process_where_conditions

from sqlalchemy import MetaData, func, distinct, select, Select, and_, or_, Table, Column, not_, cast, String



class PostgresConnector(SqlConnector):

    def __init__(self, host, user, password, port, database):
        super().__init__(host, user, password, port, database)
        self.driver = "postgresql+psycopg2"

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            query = query.replace(";", " ")
            query += " limit " + str(rows)
        return query
    
    def get_connection(self):
        return psycopg2.connect(host=self.host, user=self.user, password=self.password, port=self.port, dbname=self.database)
    

    
    def create_schema_if_missing(self, schema_name: str):
        """Creates a schema in PostgreSQL if it doesn't exist."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
            conn.commit()
            logger.info(f"Schema {schema_name} created or already exists.")
            
    def create_table_if_missing(self, table_name:str, create_table_statement: str):
        """Creates a table in PostgreSQL if it doesn't exist."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_statement)
            conn.commit()
            logger.info(f"Table {table_name} created or already exists.")
    
    def  build_create_table_statement(self, table_name: str, schema_name: str = 'public', columns = []) -> str:
        """Generates a PostgreSQL CREATE TABLE statement from column metadata."""
        column_defs = []
        primary_keys = []
        for col in columns:
            col_name = col["name"]
            col_type = col["type"].upper()
            length = col.get("length")
            nullable = col["nullable"].strip() == "YES"
            default = col["default"]
            is_pk = col["primary_key"].strip() == "YES"

            # Handle types with length
            if col_type in ("VARCHAR", "CHAR") and length:
                col_type_str = f"{col_type}({length})"
            else:
                col_type_str = col_type

            # Build column definition
            col_def_parts = [f'"{col_name}"', col_type_str]

            if not nullable:
                col_def_parts.append("NOT NULL")

            if default is not None:
                if isinstance(default, str):
                    col_def_parts.append(f"DEFAULT '{default}'")
                else:
                    col_def_parts.append(f"DEFAULT {default}")

            column_defs.append(" ".join(col_def_parts))

            if is_pk:
                primary_keys.append(f'"{col_name}"')

        # Append primary key constraint
        if primary_keys:
            pk_def = f"PRIMARY KEY ({', '.join(primary_keys)})"
            column_defs.append(pk_def)

        columns_sql = ",\n  ".join(column_defs)
        create_stmt = f'CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (\n  {columns_sql}\n);'
        return create_stmt


    def build_query(data: Dict[str, Any], invert_where: bool = False, connector=None):
        """
        Parse the JSON definition and create a SQLAlchemy selectable (query).
        """
        if connector is None:
            connector = {}
        try:
            # Step 1: Validate input data
            if not bool(data and data.get('baseTable') and connector):
                return None

            # Step 2: Connect to database
            factory = ConnectorFactory()
            connection = factory.create_connector(connector.type, vars(connector))

            # Step 4: Get and validate base table
            base_table_name = data.get('baseTable')
            base_table = load_single_table(connection, base_table_name, connector.database)

            # Step 5: Process SELECT fields
            selected_columns = process_select_fields(data, connection, base_table_name, connector.database)

            # Step 6: Start building the SELECT statement
            stmt = select(*selected_columns if selected_columns else '*').select_from(base_table)

            # Step 7: Process JOINs
            stmt = process_joins(connection, connector.database, data, stmt, base_table_name, base_table)

            # Step 8: Process WHERE conditions
            where_expressions = process_where_conditions(connection, connector.database, data)

            if where_expressions:
                if invert_where:
                    # Opposite: NOT( condition1 AND condition2 AND ... )
                    stmt = stmt.where(not_(and_(*where_expressions)))
                else:
                    # Exact: condition1 AND condition2 AND ...
                    stmt = stmt.where(and_(*where_expressions))

            # Step 9: Compile final query
            compiled_stmt = stmt.compile(bind=connection.engine, compile_kwargs={"literal_binds": True})
            query = str(compiled_stmt)
            return query
        except Exception as e:
            logger.error(f"An error occurred while building query : {str(e)}")
            return None

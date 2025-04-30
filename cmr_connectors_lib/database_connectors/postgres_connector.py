#!/usr/bin/python
# -*- coding: utf-8 -*-
from asyncio.log import logger
from .sql_connector import SqlConnector
import psycopg2


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
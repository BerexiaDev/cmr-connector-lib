#!/usr/bin/python
# -*- coding: utf-8 -*-
from loguru import logger
from typing import Dict, Any

from pyodbc import Cursor

from .sql_connector import SqlConnector
import psycopg2

from cmr_connectors_lib.database_connectors.utils.postgres_connector_utils import _build_select_clause, _build_joins_clause, _build_where_clause, _build_group_by, \
    _build_having_clause

from cmr_connectors_lib.database_connectors.sql_connector_utils import cast_postgres_to_typescript

class PostgresConnector(SqlConnector):

    def __init__(self, host, user, password, port, database, schema):
        super().__init__(host, user, password, port, database)
        self.driver = "postgresql+psycopg2"
        self.schema = schema

    def construct_query(self, query, preview, rows):
        if preview:
            query = query.lower()
            query = query.replace(";", " ")
            query += " limit " + str(rows)
        return query
    
    def get_connection(self):
        """Open a new psycopg2 connection.
            If `schema` is provided, sets the search_path to '<schema>'.
            """
        conn_params = {
            "host": self.host,
            "user": self.user,
            "password": self.password,
            "port": self.port,
            "dbname": self.database,
            "options" : f"-c search_path={self.schema}"
        }

        return psycopg2.connect(**conn_params)
    

    
    def create_schema_if_missing(self, schema_name: str):
        """Creates a schema in PostgreSQL if it doesn't exist."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
            conn.commit()
            logger.info(f"Schema {schema_name} created or already exists.")
            
    def create_table_if_missing(self, table_name:str, create_table_statement: str, index_table_statement:str = None):
        """Creates a table in PostgreSQL if it doesn't exist."""
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_statement)
                if index_table_statement:
                    cursor.execute(index_table_statement)
            conn.commit()
            logger.info(f"Table {table_name} created or already exists.")
    
    def  build_create_table_statement(self, table_name: str, schema_name: str = 'public', columns = []):
        """
        Generates a PostgreSQL CREATE TABLE statement along with a CREATE INDEX statement
        (for indexed columns) using the provided column metadata.
        """
        column_defs = []
        primary_keys = []
        index_keys = []
        for col in columns:
            col_name = col["name"]
            col_type = col["type"].upper()
            length = col.get("length")
            nullable = col["nullable"].strip() == "YES"
            default = col["default"]
            is_pk = col["primary_key"].strip() == "YES"
            if col['is_index'] == "YES":
                index_keys.append(col_name)

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
        index_stmt = None
        if index_keys:
            index_name = f"idx_{schema_name}_{table_name}_{'_'.join(index_keys)}"
            cols_sql = ", ".join(primary_keys)
            index_stmt = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema_name}.{table_name} ({cols_sql});"

        return create_stmt, index_stmt


    def ping(self):
        """Returns True if the connection is successful, False otherwise."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()  # Ensure the query runs
            logger.info("Database connection is active.")
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False


    def get_connection_tables(self):
        """
        Returns a list of all table names in the given PostgreSQL schema.
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_type   = 'BASE TABLE';
                    """,
                    (self.schema,),
                )
                # fetchall() returns list of tuples [(table1,), (table2,), …]
                return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()


    def get_connection_columns(
            self,
            table_name: str,
            schema: str = "public"
    ):
        """
        Returns a list of dicts with column names and mapped TypeScript types
        for the given Postgres table in the given schema.
        
        Args:
            table_name: The name of the table to get columns for
            schema: Schema name, defaults to "public"
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                      column_name,
                      data_type,
                      udt_name
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name   = %s
                    ORDER BY ordinal_position;
                    """,
                    (schema, table_name),
                )
                rows = cur.fetchall()

            columns: list[dict[str, str]] = []
            for column_name, data_type, udt_name in rows:
                ts_type = cast_postgres_to_typescript(data_type, udt_name)
                columns.append({"name": column_name, "type": ts_type})
            return columns

        finally:
            conn.close()


    def count_table_rows(self, table_name: str) -> int:
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count_result = cursor.fetchone()
            total_count = int(count_result[0]) if count_result else 0
            cursor.close()
            return total_count
        except Exception as e:
            logger.error(f"Error getting table total rows: {str(e)}")
            raise ValueError(f"Failed to get table total rows: {str(e)}")

    def build_query(self, data: Dict[str, Any], invert_where: bool = False):
        """
        Build an Informix SQL query based on the provided JSON definition.
        """
        try:
            # Step 1: Validate input data
            base_table = data.get('baseTable')
            if not base_table:
                logger.error("Base table is required")
                return None

            # Step 4: Build the SELECT clause
            select_clause = _build_select_clause(data.get('selectedFields', []))

            # Step 5: Build the FROM
            from_clause = f"FROM {base_table}"
            joins_clause = _build_joins_clause(base_table, data.get('joins', []))

            # Step 6: Build the WHERE clause
            where_clause = _build_where_clause(data.get('whereConditions', []), invert_where)

            # Step 7: Build the GROUP BY clause
            group_by_clause = _build_group_by(data.get('groupByFields', []))
            having_clause = _build_having_clause(data.get('having', []))

            # Combine clauses into a list
            clauses = [
                select_clause,
                from_clause,
                joins_clause,
                where_clause,
                group_by_clause,
                having_clause
            ]

            # Join the clauses with a newline if the clause is not empty.
            query = "\n".join(clause for clause in clauses if clause.strip())
            return query

        except Exception as e:
            logger.error(f"Error building query: {str(e)}")
            return None

    def fetch_batch(self, cursor: Cursor, table_name, offset: int, limit: int = 100):
        """
          Fetch up to `limit` rows from `table`, skipping the first `offset` rows.

        Args:
            table_name (str):       Name of the Informix table.
            offset (int):      Number of rows to skip.
            limit (int):       Maximum rows to return.
            cursor:            An Informix cursor.

        Returns:
            list of tuple:     The fetched rows, empty if none remain.
        """
        try:
            query = f'SELECT * FROM {table_name} OFFSET {offset} LIMIT {limit}'
            cursor.execute(query)
            results = cursor.fetchall()
            return results
        except Exception as e:
            logger.error(f"Error fetching batch from {table_name}: {str(e)}")
            return None